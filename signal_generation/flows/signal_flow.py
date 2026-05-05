import asyncio
from prefect import flow, task
from loguru import logger

from signal_generation.pipeline.signal_pipeline import SignalPipeline
from signal_generation.agents.decay_monitor_agent import DecayMonitor

@task(retries=2, retry_delay_seconds=60)
async def run_signal_pipeline_task():
    logger.info("Starting Signal Generation Pipeline via Prefect Task")
    pipeline = SignalPipeline()
    result = await pipeline.run()
    logger.info(f"Pipeline finished with run_id {result.run_id}")
    return result

@task(retries=1, retry_delay_seconds=30)
async def run_decay_monitor_task():
    logger.info("Starting Signal Decay Monitor via Prefect Task")
    monitor = DecayMonitor()
    results = await monitor.check_all_live_signals()
    logger.info(f"Decay monitor checked {len(results)} signals")
    return results

@flow(name="Signal Generation Pipeline")
async def signal_generation_flow():
    """
    Main scheduled flow for Phase 3 Signal Generation.
    Intended to be scheduled every day at 08:00 UTC (after pre-market research).
    """
    logger.info("Triggering signal_generation_flow")
    
    # 1. Check for decay in existing live signals
    await run_decay_monitor_task()
    
    # 2. Process pending hypotheses into new signals
    await run_signal_pipeline_task()

if __name__ == "__main__":
    # For local testing
    asyncio.run(signal_generation_flow())
