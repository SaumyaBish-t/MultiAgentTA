"""
Phase 8: Monitoring Dashboard API
================================
FastAPI application serving real-time monitoring data for the trading system.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from sqlalchemy import create_engine, text, select, desc
from sqlalchemy.orm import Session

from config.settings import settings
from monitoring.alerts.alert_manager import alert_manager
from monitoring.agents.health_monitor_agent import SystemHealthMonitor
from monitoring.storage.monitoring_models import Alert, SystemHealthSnapshot, PerformanceMetrics, RegimeDetection, FeedbackAction
from monitoring.dashboard.routers import strategy_comparison
from monitoring.dashboard.routers import realtime
from monitoring.dashboard.routers import pipeline_trigger
from monitoring.dashboard.routers import portfolio_detail
from monitoring.dashboard.routers import signals_detail
from monitoring.dashboard.routers import risk_detail
from monitoring.dashboard.routers import audit_detail
# 14-layer upgrade — new routers (additive)
from monitoring.api.new_endpoints import router as new_endpoints_router
from review_gate.api.review_endpoints import router as review_router

app = FastAPI(title="MultiModelTA Monitoring API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(strategy_comparison.router)
app.include_router(realtime.router, prefix='/realtime')
app.include_router(pipeline_trigger.router)
app.include_router(portfolio_detail.router)
app.include_router(signals_detail.router)
app.include_router(risk_detail.router)
app.include_router(audit_detail.router)
# 14-layer upgrade — new routes: /meta/summary, /account/status, /review/*
app.include_router(new_endpoints_router)
app.include_router(review_router, prefix="/review", tags=["review"])

# Shared components
r = redis.from_url(settings.redis_url, decode_responses=True)
engine = create_engine(settings.postgres_url)
health_monitor = SystemHealthMonitor()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ENDPOINTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/")
async def root():
    """Dashboard API landing page — lists all available endpoints."""
    return {
        "service": "MultiModelTA Monitoring Dashboard",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "GET /status": "Overall system health summary",
            "GET /portfolio": "Current portfolio state & positions",
            "GET /signals": "Active signals with performance",
            "GET /alerts": "Recent alerts (query: ?severity=critical&limit=20)",
            "POST /alerts/{id}/acknowledge": "Acknowledge an alert",
            "GET /performance": "Performance metrics (query: ?period=30d)",
            "GET /regime": "Current market regime",
            "GET /audit": "Recent audit events",
            "GET /health/detailed": "Detailed phase-by-phase health",
            "GET /feedback": "Recent feedback actions",
            "WS /ws/live": "WebSocket for live updates (5s interval)",
            "GET /docs": "Interactive Swagger UI",
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/status")
async def get_status():
    """Returns overall system health summary."""
    try:
        # Try Redis first
        data = r.get("monitoring:health:latest")
        if data:
            try:
                health = json.loads(data)
                # Ensure health is a dict, not a double-encoded string
                if isinstance(health, str):
                    health = json.loads(health)
            except (json.JSONDecodeError, TypeError):
                health = {}
        else:
            # Fallback to health monitor run (might be slow)
            report = await health_monitor.run_full_health_check()
            health = report.dict()
            
        return {
            "overall_status": health.get("overall", "UNKNOWN") if isinstance(health, dict) else "UNKNOWN",
            "phases": {k: v.get("status") for k, v in health.get("phases", {}).items()} if isinstance(health, dict) and "phases" in health else {},
            "portfolio_value": float(r.get("portfolio:current:value") or 100000.0),
            "daily_pnl_pct": float(r.get("portfolio:current:daily_pnl_pct") or 0.0),
            "drawdown": float(r.get("portfolio:drawdown:current") or 0.0),
            "alert_count": int(r.get("monitoring:alerts:unacknowledged") or 0),
            "regime": r.get("research:regime:current") or "UNKNOWN",
            "trading_halted": r.get("risk:trading:halted") == "True",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Replaced by portfolio_detail.py and signals_detail.py routers

@app.get("/alerts")
async def get_alerts(severity: Optional[str] = None, limit: int = 20):
    """Returns recent alerts."""
    try:
        with engine.connect() as conn:
            query = select(Alert).order_by(desc(Alert.created_at))
            if severity:
                query = query.where(Alert.severity == severity)
            query = query.limit(limit)
            
            res = conn.execute(query).fetchall()
            alerts = [dict(r._mapping) for r in res]
            
            unack = conn.execute(select(text("COUNT(*)")).select_from(text("alerts")).where(text("acknowledged = False"))).scalar()
            
            return {
                "total": len(alerts),
                "unacknowledged": unack,
                "alerts": alerts
            }
    except Exception as e:
        logger.error(f"Failed to get alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: uuid.UUID, body: Dict[str, str] = Body(...)):
    """Marks an alert as acknowledged."""
    try:
        ack_by = body.get("acknowledged_by", "dashboard_user")
        success = alert_manager.acknowledge_alert(alert_id, acknowledged_by=ack_by)
        return {"success": success}
    except Exception as e:
        logger.error(f"Failed to acknowledge alert: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/performance")
async def get_performance(period: str = "30d"):
    """Returns performance metrics for a specific period."""
    try:
        with engine.connect() as conn:
            query = select(PerformanceMetrics).order_by(desc(PerformanceMetrics.metric_date)).limit(1)
            # In real system, would filter by period. For now, get latest.
            res = conn.execute(query).fetchone()
            if not res:
                return {}
            
            m = dict(res._mapping)
            return {
                "return": m.get("total_return"),
                "annualized_return": m.get("annualized_return"),
                "sharpe": m.get("sharpe_ratio"),
                "max_drawdown": m.get("max_drawdown"),
                "vs_benchmark": m.get("excess_return"),
                "best_day": m.get("best_day"),
                "worst_day": m.get("worst_day"),
                "win_rate": m.get("win_day_rate")
            }
    except Exception as e:
        logger.error(f"Failed to get performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/regime")
async def get_regime():
    """Returns current market regime."""
    try:
        data = r.get("research:regime:current")
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                pass
        
        with engine.connect() as conn:
            query = select(RegimeDetection).order_by(desc(RegimeDetection.detection_date)).limit(1)
            res = conn.execute(query).fetchone()
            if res:
                return dict(res._mapping)
        return {}
    except Exception as e:
        logger.error(f"Failed to get regime: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Replaced by audit_detail.py router

@app.get("/health/detailed")
async def get_detailed_health():
    """Returns detailed phase-by-phase health."""
    try:
        data = r.get("monitoring:health:latest")
        if data:
            return json.loads(data)
        
        report = await health_monitor.run_full_health_check()
        return report.dict()
    except Exception as e:
        logger.error(f"Failed to get detailed health: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/feedback")
async def get_feedback():
    """Returns recent feedback actions."""
    try:
        with engine.connect() as conn:
            query = select(FeedbackAction).order_by(desc(FeedbackAction.created_at)).limit(20)
            res = conn.execute(query).fetchall()
            return {"actions": [dict(r._mapping) for r in res]}
    except Exception as e:
        logger.error(f"Failed to get feedback: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/account/status")
async def get_account_status():
    """Returns Alpaca account info with clear paper trading labeling."""
    try:
        from alpaca.trading.client import TradingClient
        from config.settings import settings
        client = TradingClient(
            settings.alpaca_api_key.get_secret_value() if hasattr(settings.alpaca_api_key, 'get_secret_value') else settings.alpaca_api_key, 
            settings.alpaca_secret_key.get_secret_value() if hasattr(settings.alpaca_secret_key, 'get_secret_value') else settings.alpaca_secret_key, 
            paper=True
        )
        acct = client.get_account()
        return {
            "mode": "PAPER TRADING",
            "explanation": "All trades are 100% simulated. No real money.",
            "cash_balance": f"${float(acct.cash):,.2f} from Alpaca paper account",
            "portfolio_value": f"${float(acct.portfolio_value):,.2f}",
            "is_real_money": False
        }
    except Exception as e:
        logger.error(f"Failed to get account status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WEBSOCKET
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                pass

manager = ConnectionManager()

@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Fetch latest snapshot
            data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "portfolio_value": float(r.get("portfolio:current:value") or 100000.0),
                "daily_pnl_pct": float(r.get("portfolio:current:daily_pnl_pct") or 0.0),
                "drawdown": float(r.get("portfolio:drawdown:current") or 0.0),
                "alerts_count": int(r.get("monitoring:alerts:unacknowledged") or 0),
                "new_alerts": [] # Could check for very recent alerts
            }
            await websocket.send_text(json.dumps(data))
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
