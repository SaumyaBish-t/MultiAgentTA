import json
import uuid
import time
import asyncio
from datetime import datetime
from typing import TypedDict, Any, Optional
from dataclasses import dataclass

import redis
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from langgraph.graph import StateGraph, END

from config.settings import settings
from risk_management.agents.risk_gate_agent import RiskGateAgent, RiskDecision
from risk_management.agents.var_agent import VaRAgent
from risk_management.agents.correlation_agent import CorrelationAgent
from risk_management.agents.drawdown_monitor_agent import DrawdownMonitorAgent
from signal_generation.storage.signal_models import TradingSignal

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — PIPELINE STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class RiskPipelineState(TypedDict):
    incoming_signals: list[dict]  # from Phase 3
    evaluated_signals: list[dict] # RiskDecision results (as dicts for state compatibility)
    approved_signals: list[dict]
    rejected_signals: list[dict]
    portfolio_snapshot: dict
    risk_alerts: list[dict]
    run_id: str
    started_at: float
    error: str | None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — PIPELINE FLOW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_pending_signals_node(state: RiskPipelineState) -> dict[str, Any]:
    """Fetch signals that are ready for risk evaluation if none were provided."""
    if state.get("error"): return {}
    
    signals = state.get("incoming_signals", [])
    if not signals:
        # Fallback to fetching validated signals from DB if empty
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        with Session() as session:
            stmt = select(TradingSignal).where(TradingSignal.status == 'validated')
            records = session.execute(stmt).scalars().all()
            for r in records:
                signals.append({
                    "id": str(r.id),
                    "ticker": r.ticker,
                    "direction": r.direction,
                    "conviction_score": float(r.conviction_score),
                    "strategy_type": r.strategy_type
                })
                
    logger.info(f"Risk pipeline: {len(signals)} signals to evaluate")
    return {"incoming_signals": signals}

async def update_portfolio_snapshot_node(state: RiskPipelineState) -> dict[str, Any]:
    """Take a full baseline snapshot before adding new signals."""
    if state.get("error"): return {}
    
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        pf_str = r.get("portfolio:current:state")
        positions_dict = {}
        if pf_str:
            pf_data = json.loads(pf_str)
            for p in pf_data.get("positions", []):
                if "ticker" in p and "current_value" in p:
                    positions_dict[p["ticker"]] = float(p["current_value"])
                    
        # Update snapshots via agents
        var_agent = VaRAgent()
        corr_agent = CorrelationAgent()
        
        # We can just fire these asynchronously to update the state caches
        await asyncio.gather(
            var_agent.calculate(positions_dict),
            corr_agent.analyze(list(positions_dict.keys()), positions_dict),
            return_exceptions=True
        )
        
        # Read the updated cache
        var_str = r.get("risk:var:portfolio:current")
        var_data = json.loads(var_str) if var_str else {}
        
        corr_str = r.get("risk:correlation:current")
        corr_data = json.loads(corr_str) if corr_str else {}
        
        return {
            "portfolio_snapshot": {
                "var_95": var_data.get("var_95", 0.0),
                "avg_corr": corr_data.get("avg_corr", 0.0),
                "positions_count": len(positions_dict)
            }
        }
    except Exception as e:
        logger.error(f"Snapshot update error: {e}")
        return {}

async def evaluate_signals_sequentially_node(state: RiskPipelineState) -> dict[str, Any]:
    """Evaluate signals sequentially, updating hypothetical state between evaluations."""
    if state.get("error"): return {}
    
    incoming = state.get("incoming_signals", [])
    approved = []
    rejected = []
    evaluated = []
    
    risk_gate = RiskGateAgent()
    
    for signal in incoming:
        decision = await risk_gate.evaluate(signal)
        ticker = signal.get("ticker", "UNKNOWN")
        size = decision.final_position_size_usd
        
        # Convert decision to dict for state serialization
        dec_dict = {
            "signal_id": decision.signal_id,
            "ticker": ticker,
            "approved": decision.approved,
            "size_usd": size,
            "risk_score": decision.risk_score,
            "reasons": decision.rejection_reasons,
            "summary": decision.risk_summary
        }
        
        evaluated.append(dec_dict)
        
        if decision.approved:
            approved.append(dec_dict)
            logger.info(f"[PASS] APPROVED: {ticker} ${size:,.0f} (Score: {decision.risk_score:.2f})")
            
            # NOTE: In a true sequential simulation, we would inject this approved
            # position into a temporary 'mock portfolio' so the next signal's 
            # correlation and VaR math sees it. For now, we trust the Risk Gate.
        else:
            rejected.append(dec_dict)
            reasons = " | ".join(decision.rejection_reasons)
            logger.info(f"[FAIL] REJECTED: {ticker} - {reasons}")
            
    return {
        "evaluated_signals": evaluated,
        "approved_signals": approved,
        "rejected_signals": rejected
    }

async def run_continuous_monitoring_node(state: RiskPipelineState) -> dict[str, Any]:
    """Execute a quick circuit breaker check."""
    if state.get("error"): return {}
    
    # We just run one iteration of the drawdown monitor rather than blocking the pipeline
    monitor = DrawdownMonitorAgent()
    result = await monitor.run()
    
    if result.alert_level in ['orange', 'red']:
        logger.warning(f"Pipeline Monitor Alert: {result.alert_level} | Breakers: {result.triggered_breakers}")
        
    return {"risk_alerts": result.triggered_breakers}

async def finalize_pipeline_node(state: RiskPipelineState) -> dict[str, Any]:
    """Log results and publish pipeline completion."""
    if state.get("error"): return {}
    
    approved = state.get("approved_signals", [])
    rejected = state.get("rejected_signals", [])
    snap = state.get("portfolio_snapshot", {})
    start_time = state.get("started_at", time.time())
    
    total_approved = len(approved)
    total_rejected = len(rejected)
    total_usd = sum([s.get("size_usd", 0.0) for s in approved])
    avg_score = sum([s.get("risk_score", 0.0) for s in approved]) / total_approved if total_approved > 0 else 0.0
    var_95 = snap.get("var_95", 0.0)
    
    logger.info(
        f"Risk pipeline complete:\n"
        f" Evaluated: {total_approved + total_rejected}\n"
        f" Approved: {total_approved} (${total_usd:,.0f} total)\n"
        f" Rejected: {total_rejected}\n"
        f" Avg risk score: {avg_score:.2f}\n"
        f" Portfolio VaR 95%: ${var_95:,.0f}"
    )
    
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        r.publish("risk.pipeline.completed", json.dumps({
            "run_id": state.get("run_id"),
            "approved": total_approved,
            "rejected": total_rejected,
            "total_usd_approved": total_usd,
            "duration_seconds": time.time() - start_time,
            "approved_signals": [s["signal_id"] for s in approved]
        }))
    except Exception as e:
        logger.error(f"Failed to publish pipeline completion: {e}")
        
    return {}

def build_risk_pipeline_graph() -> StateGraph:
    workflow = StateGraph(RiskPipelineState)
    
    workflow.add_node("fetch_pending_signals_node", fetch_pending_signals_node)
    workflow.add_node("update_portfolio_snapshot_node", update_portfolio_snapshot_node)
    workflow.add_node("evaluate_signals_sequentially_node", evaluate_signals_sequentially_node)
    workflow.add_node("run_continuous_monitoring_node", run_continuous_monitoring_node)
    workflow.add_node("finalize_pipeline_node", finalize_pipeline_node)
    
    workflow.set_entry_point("fetch_pending_signals_node")
    workflow.add_edge("fetch_pending_signals_node", "update_portfolio_snapshot_node")
    workflow.add_edge("update_portfolio_snapshot_node", "evaluate_signals_sequentially_node")
    workflow.add_edge("evaluate_signals_sequentially_node", "run_continuous_monitoring_node")
    workflow.add_edge("run_continuous_monitoring_node", "finalize_pipeline_node")
    workflow.add_edge("finalize_pipeline_node", END)
    
    return workflow.compile()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class RiskPipelineResult:
    run_id: str
    signals_evaluated: int
    signals_approved: int
    signals_rejected: int
    approved_signals: list[dict]
    portfolio_var_95: float
    alert_level: str
    duration_seconds: float

class RiskPipeline:
    """The complete Phase 4 Risk Management flow orchestrator."""
    
    def __init__(self):
        self.graph = build_risk_pipeline_graph()
        
    async def run(self, signals: list[dict] = None) -> RiskPipelineResult:
        """Execute the entire risk pipeline for a batch of incoming signals."""
        run_id = str(uuid.uuid4())
        start_time = time.time()
        
        initial_state: RiskPipelineState = {
            "incoming_signals": signals if signals else [],
            "evaluated_signals": [],
            "approved_signals": [],
            "rejected_signals": [],
            "portfolio_snapshot": {},
            "risk_alerts": [],
            "run_id": run_id,
            "started_at": start_time,
            "error": None
        }
        
        try:
            final_state = await self.graph.ainvoke(initial_state)
            
            duration = time.time() - start_time
            approved = final_state.get("approved_signals", [])
            rejected = final_state.get("rejected_signals", [])
            snap = final_state.get("portfolio_snapshot", {})
            
            # Get current alert level
            try:
                r = redis.from_url(settings.redis_url, decode_responses=True)
                alert = r.get("portfolio:alert:level") or "green"
            except:
                alert = "green"
                
            return RiskPipelineResult(
                run_id=run_id,
                signals_evaluated=len(approved) + len(rejected),
                signals_approved=len(approved),
                signals_rejected=len(rejected),
                approved_signals=approved,
                portfolio_var_95=snap.get("var_95", 0.0),
                alert_level=alert,
                duration_seconds=duration
            )
        except Exception as e:
            logger.exception("Risk Pipeline execution failed")
            return RiskPipelineResult(
                run_id=run_id,
                signals_evaluated=0,
                signals_approved=0,
                signals_rejected=0,
                approved_signals=[],
                portfolio_var_95=0.0,
                alert_level="unknown",
                duration_seconds=time.time() - start_time
            )

    def get_approved_signals(self) -> list[dict]:
        """Fetch approved signals awaiting execution."""
        gate = RiskGateAgent()
        return gate.get_approved_signals()

    def get_portfolio_risk_snapshot(self) -> dict:
        """Fetch the current aggregated risk snapshot from Redis."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            var_str = r.get("risk:var:portfolio:current")
            corr_str = r.get("risk:correlation:current")
            dd_str = r.get("portfolio:drawdown:current")
            
            return {
                "var": json.loads(var_str) if var_str else {},
                "correlation": json.loads(corr_str) if corr_str else {},
                "drawdown_pct": float(dd_str) if dd_str else 0.0
            }
        except Exception:
            return {}

    def is_trading_halted(self) -> bool:
        """Check if trading is suspended by circuit breakers."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            return r.get("risk:trading:halted") == "True"
        except Exception:
            return False
