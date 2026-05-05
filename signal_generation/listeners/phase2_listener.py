import asyncio
import json
import threading
from loguru import logger
import redis

from config.settings import settings
from signal_generation.pipeline.signal_pipeline import SignalPipeline

async def async_run_pipeline():
    logger.info("Triggered SignalPipeline from Redis event")
    try:
        pipeline = SignalPipeline()
        result = await pipeline.run()
        logger.info(f"SignalPipeline completed. Run ID: {result.run_id}, Top Signals: {len(result.top_signals)}")
    except Exception as e:
        logger.error(f"SignalPipeline failed during Redis event trigger: {e}")

def run_pipeline_thread():
    # Run the async pipeline in a new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_run_pipeline())
    loop.close()

def phase2_listener():
    """
    Subscribes to 'research.pipeline.completed' Redis channel.
    When Phase 2 (Alpha Research) finishes, it automatically triggers
    Phase 3 (Signal Generation).
    """
    logger.info("Starting Phase 2 Listener for 'research.pipeline.completed' events...")
    
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe("research.pipeline.completed")
        
        for message in pubsub.listen():
            if message["type"] == "message":
                logger.info(f"Received Phase 2 completion event: {message['data']}")
                # Launch pipeline in a background thread so we don't block the listener
                t = threading.Thread(target=run_pipeline_thread)
                t.start()
                
    except Exception as e:
        logger.error(f"Phase 2 Listener encountered an error: {e}")

if __name__ == "__main__":
    phase2_listener()
