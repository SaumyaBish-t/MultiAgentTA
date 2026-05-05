import asyncio
import json
import uuid
from typing import TypedDict, Any
from datetime import datetime, timezone, date
from dataclasses import dataclass

from loguru import logger
from langgraph.graph import StateGraph, END
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import httpx
import redis

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal, SignalPerformanceLive

# ==========================================
# STATE & DATACLASSES
# ==========================================
class DecayState(TypedDict):
    signal_id: str
    ticker: str
    live_predictions: list[dict]
    last_20_hit_rate: float
    last_20_avg_return: float
    last_60_avg_return: float
    decay_detected: bool
    decay_types: list[str]
    severity: str           # none/low/medium/high/critical
    recommendation: str     # none/monitor/warn/retire
    error: str | None

@dataclass
class DecayResult:
    signal_id: uuid.UUID
    ticker: str
    decay_detected: bool
    decay_types: list[str]
    severity: str
    recommendation: str

# ==========================================
# GRAPH NODES
# ==========================================
def fetch_live_performance_node(state: DecayState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        signal_id = uuid.UUID(state["signal_id"])
        
        with Session() as session:
            # Fetch last 60 records
            records = session.query(SignalPerformanceLive).filter_by(
                signal_id=signal_id
            ).order_by(SignalPerformanceLive.date.desc()).limit(60).all()
            
            if not records:
                return {
                    "live_predictions": [],
                    "last_20_hit_rate": 0.0,
                    "last_20_avg_return": 0.0,
                    "last_60_avg_return": 0.0
                }
                
            predictions = []
            for r in records:
                predictions.append({
                    "hit": r.hit,
                    "actual_return": r.actual_return,
                    "date": r.date.isoformat()
                })
                
        # Calculate metrics
        last_20 = predictions[:20]
        
        last_20_hit_rate = 0.0
        last_20_avg_return = 0.0
        last_60_avg_return = 0.0
        
        if last_20:
            hits = sum(1 for p in last_20 if p["hit"])
            last_20_hit_rate = hits / len(last_20)
            last_20_avg_return = sum(p["actual_return"] for p in last_20) / len(last_20)
            
        if predictions:
            last_60_avg_return = sum(p["actual_return"] for p in predictions) / len(predictions)
            
        return {
            "live_predictions": predictions,
            "last_20_hit_rate": last_20_hit_rate,
            "last_20_avg_return": last_20_avg_return,
            "last_60_avg_return": last_60_avg_return
        }
    except Exception as e:
        logger.error(f"Failed to fetch live performance: {e}")
        return {"error": str(e)}

async def detect_decay_patterns_node(state: DecayState) -> dict[str, Any]:
    if state.get("error") or not state.get("live_predictions"):
        return {"decay_detected": False, "decay_types": [], "severity": "none", "recommendation": "none"}
        
    try:
        decay_types = []
        
        last_20_hit = state["last_20_hit_rate"]
        last_20_ret = state["last_20_avg_return"]
        last_60_ret = state["last_60_avg_return"]
        
        # PATTERN 1: Hit rate decay
        if last_20_hit < 0.45:
            decay_types.append("HIT_RATE_DECAY")
            
        # PATTERN 2: Return decay
        if last_20_ret < 0 and last_60_ret > 0:
            decay_types.append("RETURN_DECAY")
            
        # Fetch current external regime data (mocking the "creation" data as typical for now, 
        # since DB doesn't store regime at creation time).
        current_vix = 15.0
        original_vix = 15.0
        current_regime = "bullish"
        original_regime = "bullish"
        
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            # Try to fetch current macro regime if alpha_research agent published it
            cached_regime = r.get("macro:current_regime")
            if cached_regime:
                current_regime = cached_regime
            r.close()
        except Exception as e:
            logger.warning(f"Failed to fetch regime from Redis: {e}")
            
        # PATTERN 3: Regime change
        if current_regime != original_regime:
            decay_types.append("REGIME_CHANGE_DECAY")
            
        # PATTERN 4: Volatility regime
        if current_vix > 2 * original_vix:
            decay_types.append("VOLATILITY_REGIME_DECAY")
            
        # Calculate severity
        num_patterns = len(decay_types)
        if num_patterns == 0:
            severity = "none"
        elif num_patterns == 1:
            severity = "low"
        elif num_patterns == 2:
            severity = "medium"
        else:
            severity = "high"
            
        # Override
        if last_20_hit < 0.35:
            severity = "critical"
            
        return {
            "decay_detected": num_patterns > 0,
            "decay_types": decay_types,
            "severity": severity
        }
    except Exception as e:
        logger.error(f"Failed to detect patterns: {e}")
        return {"error": str(e)}

def recommend_action_node(state: DecayState) -> dict[str, Any]:
    if state.get("error") or not state.get("decay_detected"):
        return {"recommendation": "none"}
        
    severity = state["severity"]
    
    if severity == "low":
        rec = "monitor"
    elif severity == "medium":
        rec = "warn"
    elif severity in ["high", "critical"]:
        rec = "retire"
    else:
        rec = "none"
        
    return {"recommendation": rec}

def execute_recommendation_node(state: DecayState) -> dict[str, Any]:
    if state.get("error") or state.get("recommendation") == "none":
        return {}
        
    try:
        rec = state["recommendation"]
        severity = state["severity"]
        signal_id_str = state["signal_id"]
        
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        r = redis.from_url(settings.redis_url, decode_responses=True)
        
        with Session() as session:
            if rec == "retire":
                sig = session.query(TradingSignal).filter_by(id=uuid.UUID(signal_id_str)).first()
                if sig:
                    sig.status = "retired"
                    
                r.publish("signals.retired", json.dumps({
                    "signal_id": signal_id_str,
                    "reason": state["decay_types"]
                }))
                
            elif rec == "warn":
                r.publish("signals.warning", json.dumps({
                    "signal_id": signal_id_str,
                    "severity": severity
                }))
                
            # Always write a decay event row
            decay_event = SignalPerformanceLive(
                id=uuid.uuid4(),
                signal_id=uuid.UUID(signal_id_str),
                ticker=state["ticker"],
                date=datetime.now(timezone.utc).date(),
                predicted_direction="DECAY",
                actual_direction=severity,
                predicted_return=0.0,
                actual_return=0.0,
                hit=False,
                cumulative_hit_rate=state["last_20_hit_rate"]
            )
            session.add(decay_event)
            session.commit()
            
        r.close()
        
        logger.info(f"Decay Agent executed {rec} for signal {signal_id_str}. Severity: {severity}")
        return {}
    except Exception as e:
        logger.error(f"Failed to execute recommendation: {e}")
        return {"error": str(e)}

# ==========================================
# PUBLIC INTERFACE
# ==========================================
class DecayMonitor:
    def __init__(self):
        workflow = StateGraph(DecayState)
        
        workflow.add_node("fetch", fetch_live_performance_node)
        workflow.add_node("detect", detect_decay_patterns_node)
        workflow.add_node("recommend", recommend_action_node)
        workflow.add_node("execute", execute_recommendation_node)
        
        workflow.add_edge("fetch", "detect")
        workflow.add_edge("detect", "recommend")
        workflow.add_edge("recommend", "execute")
        workflow.add_edge("execute", END)
        
        workflow.set_entry_point("fetch")
        self.app = workflow.compile()
        logger.info("DecayMonitor agent initialised")
        
    async def check_signal(self, signal_id: uuid.UUID, ticker: str = "UNKNOWN") -> DecayResult:
        state: DecayState = {
            "signal_id": str(signal_id),
            "ticker": ticker,
            "live_predictions": [],
            "last_20_hit_rate": 0.0,
            "last_20_avg_return": 0.0,
            "last_60_avg_return": 0.0,
            "decay_detected": False,
            "decay_types": [],
            "severity": "none",
            "recommendation": "none",
            "error": None
        }
        
        final_state = await self.app.ainvoke(state)
        
        return DecayResult(
            signal_id=signal_id,
            ticker=ticker,
            decay_detected=final_state.get("decay_detected", False),
            decay_types=final_state.get("decay_types", []),
            severity=final_state.get("severity", "none"),
            recommendation=final_state.get("recommendation", "none")
        )
        
    async def check_all_live_signals(self) -> list[DecayResult]:
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        live_signals = []
        with Session() as session:
            db_signals = session.query(TradingSignal).filter_by(status="live").all()
            for s in db_signals:
                live_signals.append({"id": s.id, "ticker": s.ticker})
                
        results = []
        for s in live_signals:
            res = await self.check_signal(s["id"], s["ticker"])
            results.append(res)
            
        return results

    async def get_health_report(self) -> dict:
        results = await self.check_all_live_signals()
        
        total = len(results)
        decaying = sum(1 for r in results if r.decay_detected)
        retired = sum(1 for r in results if r.recommendation == "retire")
        
        return {
            "total_live_signals": total,
            "decaying_signals": decaying,
            "recently_retired": retired,
            "health_ratio": 1.0 - (decaying / total) if total > 0 else 1.0
        }
