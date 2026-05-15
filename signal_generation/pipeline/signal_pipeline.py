import asyncio
import uuid
import json
import time
from typing import TypedDict, Any
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

from loguru import logger
from langgraph.graph import StateGraph, END
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import redis

from config.settings import settings
from alpha_research.storage.research_models import ResearchHypothesis
from signal_generation.storage.signal_models import TradingSignal, SignalGenerationRun

# Import Agents
from signal_generation.agents.strategy_coder_agent import StrategyCoderAgent
from signal_generation.agents.backtester_agent import BacktesterAgent
from signal_generation.agents.walk_forward_agent import WalkForwardAgent
from signal_generation.agents.optimizer_agent import OptimizerAgent
from signal_generation.agents.signal_scorer_agent import SignalScorerAgent

# ==========================================
# STATE & DATACLASSES
# ==========================================
class SignalPipelineState(TypedDict):
    hypotheses: list[dict]
    current_index: int
    current_hypothesis: dict | None
    
    generated_signals: list[dict]
    backtested_signals: list[dict]
    validated_signals: list[dict]
    rejected_signals: list[dict]
    optimized_signals: list[dict]
    
    top_signals: list[dict]
    run_id: str
    stats: dict
    error: str | None

@dataclass
class PipelineResult:
    run_id: str
    hypotheses_processed: int
    signals_generated: int
    signals_validated: int
    signals_rejected: int
    top_signals: list[dict]
    best_sharpe: float
    duration_seconds: float

# ==========================================
# PIPELINE ORCHESTRATOR
# ==========================================
class SignalPipeline:
    def __init__(self):
        # Instantiate Agents
        self.coder = StrategyCoderAgent()
        self.backtester = BacktesterAgent()
        self.validator = WalkForwardAgent()
        self.optimizer = OptimizerAgent()
        self.scorer = SignalScorerAgent()
        
        # Build Graph
        workflow = StateGraph(SignalPipelineState)
        
        workflow.add_node("fetch", self.fetch_hypotheses_node)
        workflow.add_node("generate", self.generate_strategy_node)
        workflow.add_node("backtest", self.backtest_strategy_node)
        workflow.add_node("walk_forward", self.walk_forward_validate_node)
        workflow.add_node("optimize", self.optimize_parameters_node)
        workflow.add_node("score", self.score_and_rank_node)
        workflow.add_node("finalize", self.finalize_run_node)
        workflow.add_node("next", self.next_hypothesis_node)
        
        workflow.add_edge("fetch", "generate")
        
        # Routing from generation
        workflow.add_conditional_edges("generate", self.route_from_generate)
        workflow.add_conditional_edges("backtest", self.route_from_backtest)
        workflow.add_conditional_edges("walk_forward", self.route_from_walk_forward)
        
        # End of successful chain goes to next
        workflow.add_edge("optimize", "next")
        
        # Next loops back or scores
        workflow.add_conditional_edges("next", self.route_loop)
        
        workflow.add_edge("score", "finalize")
        workflow.add_edge("finalize", END)
        
        workflow.set_entry_point("fetch")
        self.app = workflow.compile()
        logger.info("SignalPipeline orchestrator initialised")

    # --- NODES ---
    
    def fetch_hypotheses_node(self, state: SignalPipelineState) -> dict[str, Any]:
        try:
            # If user provided hypotheses explicitly, use them
            if state.get("hypotheses"):
                logger.info(f"Using {len(state['hypotheses'])} provided hypotheses.")
                return {"current_index": 0}
                
            engine = create_engine(settings.postgres_url)
            Session = sessionmaker(bind=engine)
            
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            
            with Session() as session:
                records = session.query(ResearchHypothesis).filter(
                    ResearchHypothesis.status == 'pending',
                    ResearchHypothesis.conviction_score >= 0.6,
                    ResearchHypothesis.created_at > yesterday
                ).order_by(ResearchHypothesis.conviction_score.desc()).limit(20).all()
                
                hypos = []
                for r in records:
                    hypos.append({
                        "id": str(r.id),
                        "ticker": r.ticker,
                        "hypothesis_type": r.hypothesis_type,
                        "title": r.title,
                        "expected_direction": r.expected_direction,
                        "expected_timeframe": r.expected_timeframe,
                        "description": r.description
                    })
                    
            logger.info(f"Found {len(hypos)} hypotheses to process")
            return {"hypotheses": hypos, "current_index": 0}
        except Exception as e:
            logger.error(f"Failed to fetch hypotheses: {e}")
            return {"error": str(e), "current_index": 0}

    async def generate_strategy_node(self, state: SignalPipelineState) -> dict[str, Any]:
        idx = state.get("current_index", 0)
        hypos = state.get("hypotheses", [])
        
        if idx >= len(hypos):
            return {} # Loop end
            
        hypo = hypos[idx]
        logger.info(f"Processing hypothesis {idx+1}/{len(hypos)}: {hypo['title']}")
        
        try:
            signal = await asyncio.wait_for(self.coder.generate(hypo), timeout=60.0)
            
            if not signal or signal.get("status") == "rejected":
                return {"rejected_signals": [hypo.get("id")]}
                
            return {"current_hypothesis": hypo, "generated_signals": [signal]}
        except asyncio.TimeoutError:
            logger.warning(f"Generation timed out for {hypo['id']}")
            return {"rejected_signals": [hypo.get("id")]}
        except Exception as e:
            logger.error(f"Generation failed: {e}")
            return {"rejected_signals": [hypo.get("id")]}

    async def backtest_strategy_node(self, state: SignalPipelineState) -> dict[str, Any]:
        signals = state.get("generated_signals", [])
        if not signals:
            return {}
            
        signal = signals[-1] # The one just generated
        try:
            res = await asyncio.wait_for(self.backtester.backtest(signal), timeout=120.0)
            
            if res.get("status") == "rejected" or res.get("error"):
                return {"rejected_signals": [signal["id"]]}
                
            signal["status"] = "backtested"
            return {"backtested_signals": [signal]}
        except asyncio.TimeoutError:
            logger.warning(f"Backtest timed out for {signal['id']}")
            return {"rejected_signals": [signal["id"]]}
        except Exception as e:
            logger.error(f"Backtest failed: {e}")
            return {"rejected_signals": [signal["id"]]}

    async def walk_forward_validate_node(self, state: SignalPipelineState) -> dict[str, Any]:
        signals = state.get("backtested_signals", [])
        if not signals:
            return {}
            
        signal = signals[-1]
        try:
            res = await asyncio.wait_for(self.validator.validate(signal), timeout=180.0)
            
            if not res or not res.get("passed") or res.get("error"):
                return {"rejected_signals": [signal["id"]]}
                
            signal["status"] = "validated"
            return {"validated_signals": [signal]}
        except asyncio.TimeoutError:
            logger.warning(f"WF Validate timed out for {signal['id']}")
            return {"rejected_signals": [signal["id"]]}
        except Exception as e:
            logger.error(f"WF Validate failed: {e}")
            return {"rejected_signals": [signal["id"]]}

    async def optimize_parameters_node(self, state: SignalPipelineState) -> dict[str, Any]:
        signals = state.get("validated_signals", [])
        if not signals:
            return {"current_index": state["current_index"] + 1}
            
        signal = signals[-1]
        try:
            # Need param ranges. For quick pipeline, we'll use auto-grid generation or DB values.
            # In Phase 3, WF agent saves the initial bounds to SignalParameters.
            # We'll pass an empty dict and let Optimizer infer ranges if missing, or fetch from DB.
            engine = create_engine(settings.postgres_url)
            Session = sessionmaker(bind=engine)
            param_ranges = {}
            with Session() as session:
                from signal_generation.storage.signal_models import SignalParameter
                params = session.query(SignalParameter).filter_by(signal_id=uuid.UUID(signal["id"])).all()
                for p in params:
                    param_ranges[p.parameter_name] = p.search_range
                    
            if not param_ranges:
                logger.warning(f"No param ranges found for {signal['id']}, skipping optimize")
                return {"current_index": state["current_index"] + 1}
                
            # Quick optimize with 20 trials max
            res = await asyncio.wait_for(self.optimizer.quick_optimize(signal, param_ranges), timeout=300.0)
            
            if res.get("error") or res.get("overfit_warning"):
                return {"rejected_signals": [signal["id"]], "current_index": state["current_index"] + 1}
                
            return {"optimized_signals": [signal], "current_index": state["current_index"] + 1}
        except asyncio.TimeoutError:
            logger.warning(f"Optimize timed out for {signal['id']}")
            return {"current_index": state["current_index"] + 1}
        except Exception as e:
            logger.error(f"Optimize failed: {e}")
            return {"current_index": state["current_index"] + 1}

    async def score_and_rank_node(self, state: SignalPipelineState) -> dict[str, Any]:
        try:
            top_signals_obj = await self.scorer.get_top_signals(10)
            
            top_dicts = []
            for s in top_signals_obj:
                top_dicts.append({
                    "id": str(s.signal_id),
                    "ticker": s.ticker,
                    "score": s.composite_score,
                    "sharpe": s.sharpe_ratio
                })
                
            return {"top_signals": top_dicts}
        except Exception as e:
            logger.error(f"Score and rank failed: {e}")
            return {"error": str(e)}

    def finalize_run_node(self, state: SignalPipelineState) -> dict[str, Any]:
        try:
            run_id = state.get("run_id", str(uuid.uuid4()))
            
            stats = {
                "hypotheses_processed": len(state.get("hypotheses", [])),
                "signals_generated": len(state.get("generated_signals", [])),
                "signals_backtested": len(state.get("backtested_signals", [])),
                "signals_validated": len(state.get("validated_signals", [])),
                "signals_rejected": len(state.get("rejected_signals", [])),
                "signals_optimized": len(state.get("optimized_signals", [])),
                "top_signals_ready": len(state.get("top_signals", [])),
                "best_sharpe": 0.0
            }
            
            if state.get("top_signals"):
                stats["best_sharpe"] = max(s["sharpe"] for s in state["top_signals"])
                
            best_sharpe = stats["best_sharpe"]
                
            logger.info(f"Signal pipeline complete:\n"
                        f"   Hypotheses: {stats['hypotheses_processed']}\n"
                        f"   Generated: {stats['signals_generated']}\n"
                        f"   Backtested: {stats['signals_backtested']}\n"
                        f"   Walk-forward passed: {stats['signals_validated']}\n"
                        f"   Optimized: {stats['signals_optimized']}\n"
                        f"   Top signals ready: {stats['top_signals_ready']}")
                        
            # Publish
            try:
                r = redis.from_url(settings.redis_url, decode_responses=True)
                r.publish("signals.pipeline.completed", json.dumps({
                    "run_id": run_id,
                    "top_signals_count": stats["top_signals_ready"],
                    "best_sharpe": best_sharpe
                }))
                r.close()
            except Exception as e:
                logger.warning(f"Failed to publish pipeline completion: {e}")
                
            return {"stats": stats}
        except Exception as e:
            logger.error(f"Finalize failed: {e}")
            return {"error": str(e)}

    def next_hypothesis_node(self, state: SignalPipelineState) -> dict[str, Any]:
        return {"current_index": state.get("current_index", 0) + 1}

    # --- CONDITIONAL ROUTING ---
    def route_from_generate(self, state: SignalPipelineState) -> str:
        # Check if the last generated signal matches the current hypothesis
        gen_sigs = state.get("generated_signals", [])
        curr_hypo = state.get("current_hypothesis", {})
        if gen_sigs and curr_hypo and gen_sigs[-1].get("hypothesis_id") == curr_hypo.get("id"):
            return "backtest"
        return "next"

    def route_from_backtest(self, state: SignalPipelineState) -> str:
        bt_sigs = state.get("backtested_signals", [])
        gen_sigs = state.get("generated_signals", [])
        if bt_sigs and gen_sigs and bt_sigs[-1]["id"] == gen_sigs[-1]["id"]:
            return "walk_forward"
        return "next"

    def route_from_walk_forward(self, state: SignalPipelineState) -> str:
        val_sigs = state.get("validated_signals", [])
        bt_sigs = state.get("backtested_signals", [])
        if val_sigs and bt_sigs and val_sigs[-1]["id"] == bt_sigs[-1]["id"]:
            return "optimize"
        return "next"

    def route_loop(self, state: SignalPipelineState) -> str:
        idx = state.get("current_index", 0)
        hypos = state.get("hypotheses", [])
        if idx < len(hypos):
            return "generate"
        return "score"

    async def run(self, hypotheses: list[dict] = None) -> PipelineResult:
        run_id = str(uuid.uuid4())
        start_time = time.time()
        
        state: SignalPipelineState = {
            "hypotheses": hypotheses or [],
            "current_index": 0,
            "current_hypothesis": None,
            "generated_signals": [],
            "backtested_signals": [],
            "validated_signals": [],
            "rejected_signals": [],
            "optimized_signals": [],
            "top_signals": [],
            "run_id": run_id,
            "stats": {},
            "error": None
        }
        
        final_state = await self.app.ainvoke(state, config={"recursion_limit": 150})
        
        duration = time.time() - start_time
        
        return PipelineResult(
            run_id=run_id,
            hypotheses_processed=len(final_state.get("hypotheses", [])),
            signals_generated=len(final_state.get("generated_signals", [])),
            signals_validated=len(final_state.get("validated_signals", [])),
            signals_rejected=len(final_state.get("rejected_signals", [])),
            top_signals=final_state.get("top_signals", []),
            best_sharpe=final_state.get("stats", {}).get("best_sharpe", 0.0),
            duration_seconds=duration
        )

    async def run_single(self, hypothesis: dict) -> dict | None:
        result = await self.run([hypothesis])
        if result.top_signals:
            return result.top_signals[0]
        return None

    def get_ready_signals(self) -> list[dict]:
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        # We can fetch ready signals from Redis rankings, or DB directly.
        # For this pipeline, we will return empty list as mock unless requested.
        return []

    def get_pipeline_status(self) -> dict:
        return {"status": "ready"}

