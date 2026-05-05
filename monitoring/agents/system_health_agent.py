"""
Phase 8: System Health Agent
===========================
Monitors system heartbeats, database connectivity, and resource usage.
"""

import asyncio
import time
import psutil
import socket
from datetime import datetime, timezone
from typing import Dict, Any
from loguru import logger
from sqlalchemy import create_engine, text
import redis

from config.settings import settings
from monitoring.storage.monitoring_models import SystemHealthMetric

class SystemHealthAgent:
    
    def __init__(self):
        self.engine = create_engine(settings.postgres_url)
        self.redis = redis.from_url(settings.redis_url)
        self.components = [
            "MarketDataAPI", "FeatureEngine", "RiskManager", 
            "ExecutionRouter", "ComplianceEngine", "RedisCache", "PostgreSQL"
        ]

    async def check_database(self) -> Dict[str, Any]:
        """Checks PostgreSQL connectivity and latency."""
        start = time.time()
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            latency = (time.time() - start) * 1000
            return {"status": "healthy", "latency": latency}
        except Exception as e:
            return {"status": "down", "error": str(e)}

    async def check_redis(self) -> Dict[str, Any]:
        """Checks Redis connectivity and latency."""
        start = time.time()
        try:
            self.redis.ping()
            latency = (time.time() - start) * 1000
            return {"status": "healthy", "latency": latency}
        except Exception as e:
            return {"status": "down", "error": str(e)}

    async def check_resource_usage(self) -> Dict[str, Any]:
        """Checks local CPU and Memory usage."""
        return {
            "cpu_pct": psutil.cpu_percent(interval=None),
            "memory_mb": psutil.virtual_memory().used / (1024 * 1024),
            "hostname": socket.gethostname()
        }

    async def run_checks(self):
        """Runs all health checks and persists to DB."""
        now = datetime.now(timezone.utc)
        
        # 1. DB Check
        db_res = await self.check_database()
        self._save_metric("PostgreSQL", db_res["status"], latency=db_res.get("latency"), timestamp=now)
        
        # 2. Redis Check
        redis_res = await self.check_redis()
        self._save_metric("RedisCache", redis_res["status"], latency=redis_res.get("latency"), timestamp=now)
        
        # 3. System Resources
        resources = await self.check_resource_usage()
        self._save_metric(
            "SystemResources", "healthy", 
            cpu=resources["cpu_pct"], mem=resources["memory_mb"], 
            details=resources, timestamp=now
        )
        
        logger.info(f"HEALTH CHECK | CPU: {resources['cpu_pct']}% | Mem: {resources['memory_mb']:.1f}MB | DB: {db_res['status']} ({db_res.get('latency', 0):.1f}ms)")

    def _save_metric(self, component, status, latency=None, cpu=None, mem=None, details=None, timestamp=None):
        """Helper to write to DB."""
        from sqlalchemy.orm import Session
        with Session(self.engine) as session:
            metric = SystemHealthMetric(
                component_name=component,
                status=status,
                latency_ms=latency,
                cpu_usage_pct=cpu,
                memory_usage_mb=mem,
                details=details,
                timestamp=timestamp or datetime.now(timezone.utc)
            )
            session.add(metric)
            session.commit()

    async def start_monitoring(self, interval_sec: int = 60):
        """Continuous monitoring loop."""
        logger.info(f"System Health Agent started (interval: {interval_sec}s)")
        while True:
            try:
                await self.run_checks()
            except Exception as e:
                logger.error(f"Health check failed: {e}")
            await asyncio.sleep(interval_sec)

if __name__ == "__main__":
    agent = SystemHealthAgent()
    asyncio.run(agent.run_checks())
