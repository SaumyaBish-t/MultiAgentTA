"""
Phase 8: Master Trading System Prefect Flow
==========================================
Orchestrates the daily execution of the entire system.
"""

import asyncio
from prefect import flow, task
from monitoring.pipeline.master_orchestrator import master_orchestrator
from loguru import logger

@task(name="Run Master Orchestrator")
async def run_orchestrator_task(run_type: str = "scheduled"):
    """Task to execute the master orchestrator."""
    result = await master_orchestrator.run(run_type=run_type)
    logger.info(f"Master run {result.run_id} finished: {len(result.phases_completed)} phases OK.")
    return result

@flow(name="Daily Trading Cycle")
async def daily_trading_cycle():
    """Daily flow scheduled for market sessions."""
    logger.info("Starting Daily Trading Cycle...")
    result = await run_orchestrator_task(run_type="scheduled")
    return result

@flow(name="System Startup")
async def system_startup():
    """Flow triggered on system initialization."""
    logger.info("Starting System Startup sequence...")
    await master_orchestrator.startup()

if __name__ == "__main__":
    # Test run
    asyncio.run(daily_trading_cycle())
