"""
Phase 8: Dashboard Aggregator Agent
==================================
Aggregates data from across the system into fast-loading JSON snapshots.
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import redis

from config.settings import settings
from monitoring.storage.monitoring_models import DashboardSnapshot

class DashboardAggregator:
    
    def __init__(self):
        self.engine = create_engine(settings.postgres_url)
        self.redis = redis.from_url(settings.redis_url)

    async def fetch_portfolio_summary(self) -> Dict[str, Any]:
        """Fetches current portfolio state from Redis."""
        state = self.redis.get("portfolio:current:state")
        if state:
            return json.loads(state)
        return {"total_value": 0, "positions": []}

    async def fetch_compliance_status(self) -> Dict[str, Any]:
        """Fetches latest compliance status."""
        status = self.redis.get("compliance:position:status")
        if status:
            return json.loads(status)
        return {"overall_status": "UNKNOWN", "breaches": []}

    async def fetch_recent_alerts(self, limit: int = 5) -> list:
        """Fetches recent alerts from DB."""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT alert_type, severity, message, created_at 
                FROM alert_history 
                ORDER BY created_at DESC LIMIT {limit}
            """))
            return [
                {
                    "type": r[0], "severity": r[1], 
                    "message": r[2], "timestamp": r[3].isoformat()
                } for r in result
            ]

    async def generate_main_overview(self):
        """Aggregates all top-level metrics into a single snapshot."""
        portfolio = await self.fetch_portfolio_summary()
        compliance = await self.fetch_compliance_status()
        alerts = await self.fetch_recent_alerts()
        
        # System Health (Latest per component)
        with self.engine.connect() as conn:
            health_res = conn.execute(text("""
                SELECT DISTINCT ON (component_name) component_name, status, latency_ms, timestamp
                FROM system_health_metrics
                ORDER BY component_name, timestamp DESC
            """))
            health = {
                r[0]: {"status": r[1], "latency": r[2], "ts": r[3].isoformat()}
                for r in health_res
            }

        snapshot_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "portfolio": {
                "total_value": portfolio.get("total_value", 0),
                "daily_pnl": portfolio.get("daily_pnl", 0),
                "position_count": len(portfolio.get("positions", []))
            },
            "compliance": {
                "status": compliance.get("overall_status", "UNKNOWN"),
                "open_breaches": len(compliance.get("breaches", []))
            },
            "system_health": health,
            "recent_alerts": alerts
        }
        
        # Save to DB
        with Session(self.engine) as session:
            snapshot = DashboardSnapshot(
                snapshot_type="main_overview",
                data=snapshot_data,
                created_at=datetime.now(timezone.utc)
            )
            session.add(snapshot)
            session.commit()
            
        # Cache in Redis for instant UI access
        self.redis.set("dashboard:snapshot:main", json.dumps(snapshot_data), ex=300)
        logger.info("DASHBOARD | Main overview snapshot generated.")

    async def run_aggregator(self, interval_sec: int = 30):
        """Continuous aggregation loop."""
        logger.info(f"Dashboard Aggregator started (interval: {interval_sec}s)")
        while True:
            try:
                await self.generate_main_overview()
            except Exception as e:
                logger.error(f"Dashboard aggregation failed: {e}")
            await asyncio.sleep(interval_sec)

if __name__ == "__main__":
    agg = DashboardAggregator()
    asyncio.run(agg.generate_main_overview())
