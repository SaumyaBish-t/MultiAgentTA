from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Any, Optional
from loguru import logger
from sqlalchemy import create_engine, text
from config.settings import settings
import datetime

router = APIRouter(prefix="/signals", tags=["Signals"])
engine = create_engine(settings.postgres_url)

@router.get("/full")
async def get_signals_full():
    try:
        live_feed = []
        signal_registry = []
        graveyard = []
        decay_metrics = []
        
        with engine.connect() as conn:
            # Live Feed (Hypotheses)
            res_hypo = conn.execute(text("SELECT * FROM research_hypotheses ORDER BY created_at DESC LIMIT 50")).fetchall()
            for r in res_hypo:
                m = dict(r._mapping)
                if m.get("status") == "rejected":
                    graveyard.append({
                        "hypothesis_id": m.get("id"),
                        "ticker": m.get("ticker"),
                        "direction": m.get("direction"),
                        "rejection_reason": m.get("rejection_reason", "CONVICTION_BELOW_MINIMUM"),
                        "rejected_at": m.get("updated_at").isoformat() if m.get("updated_at") else None,
                        "conviction_at_rejection": float(m.get("conviction_score", 0)),
                    })
                else:
                    live_feed.append({
                        "hypothesis_id": m.get("id"),
                        "ticker": m.get("ticker"),
                        "direction": m.get("direction"),
                        "conviction_score": float(m.get("conviction_score", 0)),
                        "status": m.get("status"),
                        "created_at": m.get("created_at").isoformat() if m.get("created_at") else None,
                        "agent_consensus": {
                            "fundamental": 0.5, # Placeholder, would join tables
                            "technical": "neutral",
                            "sentiment": 0.0,
                            "macro": "neutral",
                        },
                        "rejection_reason": m.get("rejection_reason"),
                        "hypothesis_text": m.get("hypothesis", ""),
                        "timeframe": m.get("timeframe", "1d"),
                        "signal_id": m.get("signal_id"),
                    })
            
            # Signal Registry
            res_sig = conn.execute(text("SELECT * FROM trading_signals ORDER BY created_at DESC LIMIT 50")).fetchall()
            for r in res_sig:
                m = dict(r._mapping)
                created_days_ago = (datetime.datetime.now(datetime.timezone.utc) - m.get("created_at")).days if m.get("created_at") else 0
                signal_registry.append({
                    "signal_id": m.get("id"),
                    "ticker": m.get("ticker"),
                    "strategy_type": m.get("strategy_type", "Unknown"),
                    "conviction_score": float(m.get("conviction_score", 0)),
                    "sharpe_ratio": float(m.get("sharpe_ratio", 0)),
                    "hit_rate": float(m.get("hit_rate", 0)),
                    "decay_status": m.get("decay_status", "healthy"),
                    "created_days_ago": created_days_ago,
                    "status": m.get("status"),
                    "last_validated": m.get("updated_at").isoformat() if m.get("updated_at") else None,
                })
                
                decay_metrics.append({
                    "signal_id": m.get("id"),
                    "ticker": m.get("ticker"),
                    "created_days_ago": created_days_ago,
                    "rolling_hit_rate_20": float(m.get("hit_rate", 0)) * 0.9,
                    "rolling_hit_rate_60": float(m.get("hit_rate", 0)),
                    "decay_severity": m.get("decay_status", "healthy"),
                    "last_checked": m.get("updated_at").isoformat() if m.get("updated_at") else None,
                })
                
        return {
            "live_feed": live_feed,
            "signal_registry": signal_registry,
            "graveyard": graveyard,
            "decay_metrics": decay_metrics
        }
    except Exception as e:
        logger.error(f"Failed to get signals full: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/agent-consensus/{ticker}")
async def get_agent_consensus(ticker: str):
    return {
        "ticker": ticker,
        "agents": {
            "fundamental": {
                "score": 0.72,
                "label": "bullish",
                "details": {"pe_score": 0.8, "growth_score": 0.7, "quality_score": 0.6}
            },
            "technical": {
                "score": 0.35,
                "label": "bearish",
                "signals": [{"type": "oscillator", "indicator": "RSI", "direction": "oversold", "strength": "strong"}]
            },
            "sentiment": {
                "score": 0.15,
                "label": "neutral",
                "magnitude": 0.4,
                "sample_count": 45,
            },
            "macro": {
                "score": 0.85,
                "label": "bullish",
                "regime_impact": "positive",
            },
            "document": {
                "score": 0.6,
                "label": "bullish",
                "management_tone": "cautiously optimistic",
            }
        },
        "overall_alignment": "mixed",
        "hypothesis_count": 3,
    }

@router.get("/feed")
async def get_signal_feed(status: str = "all", limit: int = 50, direction: str = "all", min_conviction: float = 0.0):
    # This would normally query the DB with filters
    # Delegated to the full endpoint for now in this iteration
    return []
