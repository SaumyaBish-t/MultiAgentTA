import asyncio
import sys
from loguru import logger
from data_ingestion.agent.data_ingestion_agent import DataIngestionCoordinator

# Configure logger
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

async def test_agent():
    logger.info("Initializing Data Ingestion Coordinator...")
    coordinator = DataIngestionCoordinator()
    
    logger.info("Running Pipeline Health Check...")
    # This will trigger the LangGraph agent
    # Nodes: Freshness -> Quality -> Decision (LLM) -> Action
    status = await coordinator.run_health_check(thread_id="smoke_test_123")
    
    logger.info("--- AGENT RUN RESULTS ---")
    logger.info(f"Is Healthy: {status.is_healthy}")
    logger.info(f"Active Issues: {status.active_issues}")
    
    # Get more detailed status
    pipeline_status = await coordinator.get_status(thread_id="smoke_test_123")
    logger.info(f"Pipeline State: {pipeline_status.state}")
    logger.info("Actions Taken:")
    for action in pipeline_status.last_actions:
        logger.info(f" - {action}")

if __name__ == "__main__":
    asyncio.run(test_agent())
