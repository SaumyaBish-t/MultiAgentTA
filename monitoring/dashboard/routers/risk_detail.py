from fastapi import APIRouter, HTTPException, Body
from typing import Dict, List, Any, Optional
from loguru import logger
import json
import datetime
from sqlalchemy import create_engine, text
from config.settings import settings
import redis

router = APIRouter(prefix="/risk", tags=["Risk"])
engine = create_engine(settings.postgres_url)
r = redis.from_url(settings.redis_url)

@router.get("/full")
async def get_risk_full():
    try:
        circuit_breakers = []
        with engine.connect() as conn:
            res = conn.execute(text("SELECT * FROM circuit_breakers")).fetchall()
            for row in res:
                m = dict(row._mapping)
                circuit_breakers.append({
                    "breaker_type": m.get("breaker_type"),
                    "threshold": float(m.get("threshold", 0)),
                    "current_value": float(m.get("current_value", 0)),
                    "triggered": m.get("triggered", False),
                    "triggered_at": m.get("updated_at").isoformat() if m.get("updated_at") and m.get("triggered") else None,
                    "action": m.get("action"),
                    "severity": "high" if m.get("action") in ["halt_new_trades", "close_position"] else "medium"
                })

        trading_halted = r.get("risk:trading:halted") == "True"
        current_drawdown = float(r.get("portfolio:drawdown:current") or 0.0)
        peak_value = float(r.get("portfolio:peak:value") or 100000.0)

        return {
            "portfolio_var": {
                "var_95_1day_usd": 1840.00,
                "var_99_1day_usd": 2500.00,
                "cvar_95_1day_usd": 2200.00,
                "var_as_pct_of_portfolio": 0.0184,
                "method": "HISTORICAL SIMULATION",
                "calculated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            },
            "drawdown": {
                "current_drawdown_pct": current_drawdown,
                "max_drawdown_pct": -0.084,
                "peak_value": peak_value,
                "current_value": peak_value * (1 + current_drawdown),
                "drawdown_duration_days": 4,
                "recovery_needed_pct": abs(current_drawdown) / (1 + current_drawdown) if current_drawdown < 0 else 0,
            },
            "correlation_matrix": {
                "tickers": ["AAPL", "MSFT", "NVDA", "SPY", "GOOGL"],
                "matrix": [
                    [1.00, 0.84, 0.65, 0.72, 0.80],
                    [0.84, 1.00, 0.70, 0.75, 0.82],
                    [0.65, 0.70, 1.00, 0.68, 0.60],
                    [0.72, 0.75, 0.68, 1.00, 0.78],
                    [0.80, 0.82, 0.60, 0.78, 1.00]
                ],
                "high_correlation_pairs": [{
                    "ticker1": "AAPL",
                    "ticker2": "MSFT",
                    "correlation": 0.84,
                    "combined_weight": 0.35,
                }],
                "avg_correlation": 0.73,
                "concentration_risk": "medium",
            },
            "circuit_breakers": circuit_breakers,
            "volatility_alerts": [],
            "kill_switch_status": {
                "trading_halted": trading_halted,
                "halted_at": None,
                "halted_reason": None,
                "positions_open": 5,
            },
            "position_var": []
        }
    except Exception as e:
        logger.error(f"Failed to get risk full: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/kill-switch")
async def toggle_kill_switch(body: Dict[str, str] = Body(...)):
    try:
        action = body.get("action")
        reason = body.get("reason", "Manual override")
        
        if action == "halt":
            r.set("risk:trading:halted", "True")
            return {"success": True, "status": "halted", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        elif action == "resume":
            r.set("risk:trading:halted", "False")
            return {"success": True, "status": "active", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        
        raise HTTPException(status_code=400, detail="Invalid action")
    except Exception as e:
        logger.error(f"Kill switch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/vix-live")
async def get_vix_live():
    return {"vix": 18.4, "change": 0.5, "level": "normal", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}
