import asyncio
import json
import uuid
from typing import TypedDict, Any
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

import pandas as pd
from loguru import logger
from langgraph.graph import StateGraph, END
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import redis

from config.settings import settings
from config.llm_config import document_llm
from signal_generation.storage.signal_models import (
    TradingSignal, 
    BacktestResult, 
    WalkForwardResult, 
    SignalPerformanceLive
)

# ==========================================
# STATE & DATACLASSES
# ==========================================
class ScorerState(TypedDict):
    signals: list[dict]
    backtest_results: dict
    wf_results: dict
    composite_scores: dict
    rankings: list[dict]
    top_signals: list[dict]
    decay_flags: list[str]
    report: str
    error: str | None

@dataclass
class ScoredSignal:
    signal_id: uuid.UUID
    ticker: str
    strategy_type: str
    composite_score: float
    sharpe_ratio: float
    annualized_return: float
    max_drawdown: float
    consistency_score: float
    status: str
    rank: int

# ==========================================
# GRAPH NODES
# ==========================================
def fetch_all_validated_signals_node(state: ScorerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        signals_list = []
        backtest_dict = {}
        wf_dict = {}
        
        ninety_days_ago = datetime.now(timezone.utc) - timedelta(days=90)
        
        with Session() as session:
            # Query signals
            db_signals = session.query(TradingSignal).filter(
                TradingSignal.status.in_(["validated", "live"]),
                TradingSignal.created_at >= ninety_days_ago
            ).all()
            
            for sig in db_signals:
                sig_id = str(sig.id)
                signals_list.append({
                    "id": sig_id,
                    "ticker": sig.ticker,
                    "strategy_type": sig.signal_type,
                    "status": sig.status,
                    "created_at": sig.created_at
                })
                
                # Fetch latest backtest
                bt = session.query(BacktestResult).filter_by(signal_id=sig.id).order_by(BacktestResult.backtested_at.desc()).first()
                if bt:
                    backtest_dict[sig_id] = {
                        "sharpe_ratio": bt.sharpe_ratio,
                        "annualized_return_pct": bt.annualized_return_pct,
                        "max_drawdown_pct": bt.max_drawdown_pct
                    }
                    
                # Fetch latest walk forward
                wf = session.query(WalkForwardResult).filter_by(signal_id=sig.id, passed=True).order_by(WalkForwardResult.tested_at.desc()).first()
                if wf:
                    wf_dict[sig_id] = {
                        "consistency_score": wf.consistency_score,
                        "overfit_score": wf.overfit_score
                    }
                    
        return {
            "signals": signals_list,
            "backtest_results": backtest_dict,
            "wf_results": wf_dict
        }
    except Exception as e:
        logger.error(f"Failed to fetch signals: {e}")
        return {"error": str(e)}

def compute_composite_score_node(state: ScorerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        signals = state["signals"]
        backtest_results = state["backtest_results"]
        wf_results = state["wf_results"]
        
        composite_scores = {}
        
        for sig in signals:
            sig_id = sig["id"]
            
            # Skip if missing required results
            if sig_id not in backtest_results or sig_id not in wf_results:
                composite_scores[sig_id] = 0.0
                continue
                
            bt = backtest_results[sig_id]
            wf = wf_results[sig_id]
            
            # Sub-scores
            sharpe_score = min(max(bt["sharpe_ratio"] / 3.0, 0.0), 1.0)
            ret_score = min(max(bt["annualized_return_pct"] / 50.0, 0.0), 1.0)
            
            # Drawdown is typically a negative number (e.g. -15.0), so higher is worse
            dd = bt["max_drawdown_pct"]
            if dd > 0:
                dd = -dd # Ensure negative
            dd_score = max(0.0, 1.0 + (dd / 25.0))
            
            consistency = wf["consistency_score"]
            overfit_penalty = max(0.0, 1.0 - (wf["overfit_score"] - 1.0) / 2.0)
            
            # Composite
            composite = (
                sharpe_score * 0.30 +
                ret_score * 0.20 +
                dd_score * 0.20 +
                consistency * 0.20 +
                overfit_penalty * 0.10
            )
            
            composite_scores[sig_id] = float(composite)
            
        return {"composite_scores": composite_scores}
    except Exception as e:
        logger.error(f"Failed to compute composite scores: {e}")
        return {"error": str(e)}

def check_signal_decay_node(state: ScorerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        signals = state["signals"]
        decay_flags = []
        
        thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
        
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        with Session() as session:
            for sig in signals:
                if sig["status"] == "live" and sig["created_at"] < thirty_days_ago:
                    sig_id_uuid = uuid.UUID(sig["id"])
                    
                    # Fetch last 20 performance records
                    recent_perfs = session.query(SignalPerformanceLive).filter_by(
                        signal_id=sig_id_uuid
                    ).order_by(SignalPerformanceLive.recorded_at.desc()).limit(20).all()
                    
                    if not recent_perfs:
                        continue
                        
                    hits = sum(1 for p in recent_perfs if p.hit)
                    hit_rate = hits / len(recent_perfs)
                    
                    if hit_rate < 0.45:
                        decay_flags.append(sig["id"])
                        
                        # Update DB
                        db_sig = session.query(TradingSignal).filter_by(id=sig_id_uuid).first()
                        if db_sig:
                            db_sig.status = "retired"
                            
                        # Publish
                        try:
                            r = redis.from_url(settings.redis_url, decode_responses=True)
                            r.publish("signals.decay.detected", json.dumps({"signal_id": str(sig["id"])}))
                            r.close()
                        except Exception as redis_e:
                            logger.warning(f"Failed to publish decay event: {redis_e}")
                            
            session.commit()
            
        return {"decay_flags": decay_flags}
    except Exception as e:
        logger.error(f"Failed to check signal decay: {e}")
        return {"error": str(e)}

def rank_and_select_node(state: ScorerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        signals = state["signals"]
        scores = state["composite_scores"]
        decay_flags = state["decay_flags"]
        bt_results = state["backtest_results"]
        wf_results = state["wf_results"]
        
        # Filter out decayed or un-scored signals
        valid_signals = [s for s in signals if s["id"] not in decay_flags and scores.get(s["id"], 0) > 0]
        
        # Build enriched list for sorting
        enriched = []
        for s in valid_signals:
            s_id = s["id"]
            enriched.append({
                "signal_id": s_id,
                "ticker": s["ticker"],
                "strategy_type": s["strategy_type"],
                "status": s["status"],
                "composite_score": scores[s_id],
                "sharpe_ratio": bt_results[s_id]["sharpe_ratio"],
                "annualized_return": bt_results[s_id]["annualized_return_pct"],
                "max_drawdown": bt_results[s_id]["max_drawdown_pct"],
                "consistency_score": wf_results[s_id]["consistency_score"]
            })
            
        # Sort descending by score
        rankings = sorted(enriched, key=lambda x: x["composite_score"], reverse=True)
        
        # Apply diversification rules
        top_signals = []
        ticker_counts = {}
        strategy_counts = {}
        
        for sig in rankings:
            t = sig["ticker"]
            st = sig["strategy_type"]
            
            if ticker_counts.get(t, 0) >= 3:
                continue
            if strategy_counts.get(st, 0) >= 2:
                continue
                
            top_signals.append(sig)
            ticker_counts[t] = ticker_counts.get(t, 0) + 1
            strategy_counts[st] = strategy_counts.get(st, 0) + 1
            
            if len(top_signals) >= 10:
                break
                
        return {"rankings": rankings, "top_signals": top_signals}
    except Exception as e:
        logger.error(f"Failed to rank and select: {e}")
        return {"error": str(e)}

async def generate_signal_report_node(state: ScorerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        top_signals = state["top_signals"]
        if not top_signals:
            return {"report": "No valid signals found for report generation."}
            
        summary_data = []
        for s in top_signals:
            summary_data.append(
                f"{s['ticker']} ({s['strategy_type']}) - Score: {s['composite_score']:.2f}, Sharpe: {s['sharpe_ratio']:.2f}"
            )
            
        prompt = f"""Given these top trading signals with performance metrics:
{json.dumps(summary_data, indent=2)}

Write a 3-sentence summary of what's working, what timeframes or tickers dominate, and overall signal quality.
Be direct and quantitative."""

        response = await document_llm.ainvoke(prompt)
        report = response.content.strip()
        
        return {"report": report}
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        return {"report": "Failed to generate report due to LLM error."}

def store_rankings_node(state: ScorerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        top_signals = state["top_signals"]
        
        if not top_signals:
            return {}
            
        # Cache in Redis
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            cache_data = [s["signal_id"] for s in top_signals]
            r.setex("signals:rankings:current", 3600, json.dumps(cache_data))
            
            avg_sharpe = sum(s["sharpe_ratio"] for s in top_signals) / len(top_signals)
            top_ticker = top_signals[0]["ticker"]
            
            r.publish("signals.ranked.updated", json.dumps({
                "count": len(top_signals),
                "top_ticker": top_ticker,
                "avg_sharpe": avg_sharpe
            }))
            r.close()
        except Exception as redis_e:
            logger.warning(f"Failed to cache rankings in Redis: {redis_e}")
            
        return {}
    except Exception as e:
        logger.error(f"Failed to store rankings: {e}")
        return {"error": str(e)}

# ==========================================
# PUBLIC INTERFACE
# ==========================================
class SignalScorerAgent:
    def __init__(self):
        workflow = StateGraph(ScorerState)
        
        workflow.add_node("fetch", fetch_all_validated_signals_node)
        workflow.add_node("score", compute_composite_score_node)
        workflow.add_node("decay", check_signal_decay_node)
        workflow.add_node("rank", rank_and_select_node)
        workflow.add_node("report", generate_signal_report_node)
        workflow.add_node("store", store_rankings_node)
        
        workflow.add_edge("fetch", "score")
        workflow.add_edge("score", "decay")
        workflow.add_edge("decay", "rank")
        workflow.add_edge("rank", "report")
        workflow.add_edge("report", "store")
        workflow.add_edge("store", END)
        
        workflow.set_entry_point("fetch")
        self.app = workflow.compile()
        logger.info("SignalScorer agent initialised")
        
    async def _run_graph(self) -> ScorerState:
        state: ScorerState = {
            "signals": [],
            "backtest_results": {},
            "wf_results": {},
            "composite_scores": {},
            "rankings": [],
            "top_signals": [],
            "decay_flags": [],
            "report": "",
            "error": None
        }
        return await self.app.ainvoke(state)
        
    async def score_all(self) -> list[ScoredSignal]:
        final_state = await self._run_graph()
        if final_state.get("error"):
            logger.error(f"Score all failed: {final_state['error']}")
            return []
            
        results = []
        for i, sig in enumerate(final_state["rankings"]):
            results.append(ScoredSignal(
                signal_id=uuid.UUID(sig["signal_id"]),
                ticker=sig["ticker"],
                strategy_type=sig["strategy_type"],
                composite_score=sig["composite_score"],
                sharpe_ratio=sig["sharpe_ratio"],
                annualized_return=sig["annualized_return"],
                max_drawdown=sig["max_drawdown"],
                consistency_score=sig["consistency_score"],
                status=sig["status"],
                rank=i + 1
            ))
        return results

    async def get_top_signals(self, n: int = 10) -> list[ScoredSignal]:
        final_state = await self._run_graph()
        if final_state.get("error"):
            return []
            
        results = []
        for i, sig in enumerate(final_state["top_signals"][:n]):
            results.append(ScoredSignal(
                signal_id=uuid.UUID(sig["signal_id"]),
                ticker=sig["ticker"],
                strategy_type=sig["strategy_type"],
                composite_score=sig["composite_score"],
                sharpe_ratio=sig["sharpe_ratio"],
                annualized_return=sig["annualized_return"],
                max_drawdown=sig["max_drawdown"],
                consistency_score=sig["consistency_score"],
                status=sig["status"],
                rank=i + 1
            ))
        return results

    async def get_best_for_ticker(self, ticker: str) -> ScoredSignal | None:
        final_state = await self._run_graph()
        if final_state.get("error"):
            return None
            
        for i, sig in enumerate(final_state["rankings"]):
            if sig["ticker"] == ticker:
                return ScoredSignal(
                    signal_id=uuid.UUID(sig["signal_id"]),
                    ticker=sig["ticker"],
                    strategy_type=sig["strategy_type"],
                    composite_score=sig["composite_score"],
                    sharpe_ratio=sig["sharpe_ratio"],
                    annualized_return=sig["annualized_return"],
                    max_drawdown=sig["max_drawdown"],
                    consistency_score=sig["consistency_score"],
                    status=sig["status"],
                    rank=i + 1
                )
        return None

    async def get_signal_report(self) -> str:
        final_state = await self._run_graph()
        return final_state.get("report", "No report available.")
