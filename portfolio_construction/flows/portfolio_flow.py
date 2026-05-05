import asyncio
from datetime import datetime, timezone, time as dt_time
from prefect import flow, task
from loguru import logger
import redis
import json

from portfolio_construction.pipeline.portfolio_pipeline import PortfolioPipeline
from portfolio_construction.agents.rebalancing_agent import RebalancingAgent
from config.settings import settings

@task(retries=2, retry_delay_seconds=60)
async def run_portfolio_pipeline_task():
    """Task to execute the full construction pipeline."""
    logger.info("Starting Portfolio Construction Pipeline Task")
    pipeline = PortfolioPipeline()
    result = await pipeline.run()
    
    if result.get("error"):
        logger.error(f"Pipeline failed: {result['error']}")
        return False
        
    logger.info("Pipeline task completed successfully")
    return True

@task
async def update_performance_task():
    """Task to calculate daily P&L and update performance metrics."""
    logger.info("Updating portfolio performance metrics")
    # This would typically fetch latest prices and compare with previous snapshot
    # and update the portfolio_performance table.
    # For now, we'll log a placeholder.
    pass

@task
async def check_drift_task():
    """Monitor drift and trigger immediate rebalance if necessary."""
    logger.info("Checking portfolio drift")
    agent = RebalancingAgent()
    plan = await agent.check_and_plan()
    
    if plan and plan.needed and plan.trigger_type == "drift":
        logger.warning(f"High drift detected ({plan.max_drift:.2%}). Triggering immediate rebalance.")
        pipeline = PortfolioPipeline()
        await pipeline.run()
    else:
        logger.info("Drift within acceptable limits.")

# flows
@flow(name="Portfolio Construction")
async def portfolio_construction_flow():
    """Triggered daily or by events to rebuild the portfolio."""
    await run_portfolio_pipeline_task()

@flow(name="Daily Performance Update")
async def daily_performance_flow():
    """Runs after market close to record daily P&L."""
    await update_performance_task()

@flow(name="Intraday Drift Monitor")
async def drift_monitor_flow():
    """Periodic check for target weight violations."""
    await check_drift_task()

if __name__ == "__main__":
    # Local test
    asyncio.run(portfolio_construction_flow())
