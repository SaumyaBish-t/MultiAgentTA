import asyncio
import json
import redis
from loguru import logger
from datetime import datetime, timezone

from config.settings import settings
from execution.pipeline.execution_pipeline import ExecutionPipeline
from execution.agents.emergency_handler import EmergencyHandler
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter

async def start_phase5_listener():
    """
    Subscribes to Phase 5 allocation events and triggers execution.
    Also starts the Emergency Handler listener.
    """
    r = redis.from_url(settings.redis_url, decode_responses=True)
    pubsub = r.pubsub()
    
    # 1. Subscribe to relevant channels
    pubsub.subscribe("portfolio.allocation.final", "portfolio.rebalance.approved")
    
    # 2. Start Emergency Handler in background
    emergency = EmergencyHandler()
    emergency.start_listener()
    
    logger.info("Phase 5 Listener active. Waiting for allocations...")
    
    pipeline = ExecutionPipeline()
    adapter = AlpacaBrokerAdapter()
    
    try:
        while True:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message:
                channel = message["channel"]
                data = json.loads(message["data"])
                
                logger.info(f"Received event on {channel}")
                
                # Check Market State
                clock = adapter.get_market_clock()
                
                if clock["is_open"]:
                    # Immediate Execution
                    logger.info("Market is open. Triggering immediate execution pipeline.")
                    asyncio.create_task(pipeline.run(data))
                else:
                    # Queue for next open
                    logger.warning("Market is closed. Queuing rebalance for next open.")
                    queued = r.get("execution:queued:orders")
                    plans = json.loads(queued) if queued else []
                    plans.append(data)
                    r.set("execution:queued:orders", json.dumps(plans))
                    
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        logger.info("Phase 5 Listener task cancelled.")
    except Exception as e:
        logger.error(f"Phase 5 Listener error: {e}")
        # Auto-restart
        await asyncio.sleep(5)
        await start_phase5_listener()

if __name__ == "__main__":
    asyncio.run(start_phase5_listener())
