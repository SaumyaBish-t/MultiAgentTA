"""
Phase 8: Monitoring API & Dashboard
==================================
FastAPI server to expose system health, alerts, and dashboard snapshots.
"""

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import redis
import json
from typing import Dict, Any, List
from sqlalchemy import create_engine, text
from config.settings import settings

app = FastAPI(title="MultiModelTA Monitoring Dashboard")

# CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DB & Redis connection
engine = create_engine(settings.postgres_url)
r = redis.from_url(settings.redis_url)

@app.get("/")
async def root():
    return {"message": "MultiModelTA Monitoring API is live"}

@app.get("/api/dashboard/overview")
async def get_dashboard_overview():
    """Returns the latest aggregated dashboard snapshot."""
    snapshot = r.get("dashboard:snapshot:main")
    if snapshot:
        return json.loads(snapshot)
    
    # Fallback: Fetch from DB if Redis is empty
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT data FROM dashboard_snapshots 
            ORDER BY created_at DESC LIMIT 1
        """)).fetchone()
        if result:
            return result[0]
            
    raise HTTPException(status_code=404, detail="No snapshot found")

@app.get("/api/alerts")
async def get_alerts(limit: int = 20, severity: str = None):
    """Fetches recent alert history."""
    query = "SELECT id, alert_type, severity, message, acknowledged, created_at FROM alert_history"
    if severity:
        query += f" WHERE severity = '{severity.upper()}'"
    query += f" ORDER BY created_at DESC LIMIT {limit}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [
            {
                "id": str(r[0]), "type": r[1], "severity": r[2],
                "message": r[3], "acknowledged": r[4], "timestamp": r[5]
            } for r in result
        ]

@app.get("/api/health")
async def get_system_health():
    """Fetches the latest health status for all components."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT ON (component_name) component_name, status, latency_ms, cpu_usage_pct, memory_usage_mb, timestamp
            FROM system_health_metrics
            ORDER BY component_name, timestamp DESC
        """))
        return [
            {
                "component": r[0], "status": r[1], "latency": r[2],
                "cpu": r[3], "mem": r[4], "timestamp": r[5]
            } for r in result
        ]

@app.get("/api/drift")
async def get_drift_reports(limit: int = 10):
    """Fetches recent model drift reports."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT feature_name, drift_score, is_drifting, p_value, detected_at
            FROM feature_performance_drift
            ORDER BY detected_at DESC LIMIT {limit}
        """))
        return [
            {
                "feature": r[0], "score": r[1], "is_drifting": r[2],
                "p_value": r[3], "timestamp": r[4]
            } for r in result
        ]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
