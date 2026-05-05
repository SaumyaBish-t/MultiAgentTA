import asyncio
import json
from datetime import datetime, timezone
from prefect import flow, task
from loguru import logger
import redis

from config.settings import settings
from execution.pipeline.execution_pipeline import ExecutionPipeline
from execution.agents.smart_order_router_agent import SmartOrderRouter
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PREFECT TASKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@task(retries=1, retry_delay_seconds=60, name="Run Execution Pipeline Task")
async def run_execution_pipeline_task(rebalance_plan: dict):
    """Executes a rebalance plan through the full pipeline."""
    pipeline = ExecutionPipeline()
    return await pipeline.run(rebalance_plan)

@task(name="Sync Alpaca Account Task")
def sync_alpaca_account_task():
    """Syncs live broker data with Redis and DB."""
    adapter = AlpacaBrokerAdapter()
    account = adapter.get_account()
    positions = adapter.get_positions()
    
    r = redis.from_url(settings.redis_url, decode_responses=True)
    portfolio_state = {
        "total_value": account["portfolio_value"],
        "cash": account["cash"],
        "positions": positions,
        "last_updated": datetime.now(timezone.utc).isoformat()
    }
    r.set("portfolio:current:state", json.dumps(portfolio_state))
    logger.info(f"Account synced. Total Value: ${account['portfolio_value']:,.2f}")

@task(name="Execute Queued Orders Task")
async def execute_queued_orders_task():
    """Submits orders that were queued while market was closed."""
    r = redis.from_url(settings.redis_url, decode_responses=True)
    queued = r.get("execution:queued:orders")
    
    if queued:
        plans = json.loads(queued)
        logger.info(f"Executing {len(plans)} queued rebalance plans at market open.")
        pipeline = ExecutionPipeline()
        for plan in plans:
            await pipeline.run(plan)
        r.delete("execution:queued:orders")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PREFECT FLOWS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@flow(name="Execution Pipeline Flow")
async def execution_flow(rebalance_plan: dict):
    """Orchestrates on-demand execution of a rebalance plan."""
    return await run_execution_pipeline_task(rebalance_plan)

@flow(name="Market Open Execution Flow")
async def market_open_flow():
    """Flow scheduled to run at market open."""
    await execute_queued_orders_task()

@flow(name="Account Sync Flow")
def account_sync_flow():
    """Periodic account synchronization flow."""
    sync_alpaca_account_task()

@flow(name="End of Day Execution Report Flow")
def eod_execution_report_flow():
    """Generates summary of today's execution performance."""
    logger.info("Generating EOD Execution Report...")
    # This would aggregate execution_performance table data
    pass

if __name__ == "__main__":
    # For testing:
    # asyncio.run(execution_flow({"trades": []}))
    pass
