"""
Prefect flow definition for the Alpha Research Pipeline.
Schedules the LangGraph orchestration pipeline to run
pre-market and post-market automatically.
"""

from typing import Any

from loguru import logger
from prefect import flow, task

from alpha_research.pipeline.research_pipeline import ResearchPipeline


@task(name="Run Research Pipeline", retries=2, retry_delay_seconds=60)
async def run_research_pipeline_task(tickers: list[str] | None = None) -> Any:
    """Executes the LangGraph ResearchPipeline."""
    logger.info("Prefect Task: Triggering LangGraph Research Pipeline")
    pipeline = ResearchPipeline()
    result = await pipeline.run(tickers)
    return result


@flow(name="Research Pipeline", log_prints=True)
async def research_pipeline_flow(tickers: list[str] | None = None) -> Any:
    """
    Main flow for Alpha Discovery Research.
    
    Scheduled executions:
      - 06:00 UTC (Pre-market)
      - 20:30 UTC (Post-market, after close)
    """
    logger.info("Starting Research Pipeline Flow")
    result = await run_research_pipeline_task(tickers)
    logger.info(f"Flow completed with status: {result.status}")
    return result


if __name__ == "__main__":
    # For local testing
    import asyncio
    asyncio.run(research_pipeline_flow())
