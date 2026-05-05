"""
Phase 8: Monitoring Pipeline
===========================
Orchestrates health checks, drift detection, and dashboard updates.
"""

import asyncio
from loguru import logger
from monitoring.agents.system_health_agent import SystemHealthAgent
from monitoring.agents.alert_manager import AlertManager
from monitoring.agents.drift_detection_agent import DriftDetectionAgent
from monitoring.agents.dashboard_aggregator import DashboardAggregator

class MonitoringPipeline:
    
    def __init__(self):
        self.health = SystemHealthAgent()
        self.alerts = AlertManager()
        self.drift = DriftDetectionAgent()
        self.dashboard = DashboardAggregator()

    async def run_heartbeat(self):
        """Runs the 1-minute heartbeat tasks."""
        logger.info("PIPELINE | Running Monitoring Heartbeat...")
        
        # 1. System Health
        await self.health.run_checks()
        
        # 2. Dashboard Update
        await self.dashboard.generate_main_overview()
        
        logger.success("PIPELINE | Monitoring Heartbeat Complete.")

    async def run_deep_check(self):
        """Runs the deeper analysis tasks (e.g., drift)."""
        logger.info("PIPELINE | Running Monitoring Deep Check...")
        
        # Drift check for main prediction features
        # Note: In a real system, we'd fetch these feature names from the Model Registry (Phase 2)
        features_to_check = ["rsi_14", "macd_diff", "volatility_20"]
        
        for feature in features_to_check:
            # For now, we mock the baseline (0 mean, 1 std)
            import numpy as np
            baseline = np.random.normal(0, 1, 500).tolist()
            await self.drift.check_feature_drift(feature, baseline)
            
        logger.success("PIPELINE | Monitoring Deep Check Complete.")

if __name__ == "__main__":
    pipeline = MonitoringPipeline()
    asyncio.run(pipeline.run_heartbeat())
    asyncio.run(pipeline.run_deep_check())
