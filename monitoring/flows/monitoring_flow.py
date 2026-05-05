"""
Phase 8: Monitoring Flow
========================
Prefect flows for automated system monitoring and reporting.
"""

from prefect import flow, task
from loguru import logger
import asyncio
from monitoring.pipeline.monitoring_pipeline import MonitoringPipeline

@task(name="System Heartbeat")
def run_system_heartbeat():
    """Runs high-frequency health checks."""
    pipeline = MonitoringPipeline()
    asyncio.run(pipeline.run_heartbeat())

@task(name="Drift and Performance Analysis")
def run_deep_analysis():
    """Runs statistical drift checks."""
    pipeline = MonitoringPipeline()
    asyncio.run(pipeline.run_deep_check())

@flow(name="System Monitoring Flow")
def monitoring_flow():
    """Main monitoring flow scheduled every minute."""
    run_system_heartbeat()
    
@flow(name="Deep Performance Analysis Flow")
def deep_analysis_flow():
    """Deeper analysis run every hour."""
    run_deep_analysis()

if __name__ == "__main__":
    # To run locally:
    monitoring_flow()
    deep_analysis_flow()
