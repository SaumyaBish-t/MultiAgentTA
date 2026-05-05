import json
import asyncio
import signal
from loguru import logger
import redis.asyncio as redis

from config.settings import settings
from portfolio_construction.pipeline.portfolio_pipeline import PortfolioPipeline

class Phase4Listener:
    """Listens for Phase 4 Risk signals and triggers Portfolio Construction."""
    
    def __init__(self):
        self.redis_url = settings.redis_url
        self.pipeline = PortfolioPipeline()
        self._stop_event = asyncio.Event()

    async def start(self):
        """Subscribe to Redis channels and handle events."""
        logger.info("Starting Phase 4 Listener for Portfolio Construction...")
        r = redis.from_url(self.redis_url, decode_responses=True)
        pubsub = r.pubsub()
        
        await pubsub.subscribe("risk.pipeline.completed", "risk.circuit_breaker.emergency")
        
        try:
            while not self._stop_event.is_set():
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    channel = message["channel"]
                    data = json.loads(message["data"])
                    
                    if channel == "risk.pipeline.completed":
                        logger.info(f"Phase 4 completed event received. Triggering Portfolio Pipeline.")
                        # Trigger pipeline in the background
                        asyncio.create_task(self.pipeline.run())
                        
                    elif channel == "risk.circuit_breaker.emergency":
                        logger.warning(f"EMERGENCY CIRCUIT BREAKER triggered. Initiating liquidation plan.")
                        # Emergency logic: set target weights to 0 for all and rebalance
                        # This would be handled by the AllocationAgent circuit breaker check
                        asyncio.create_task(self.pipeline.run())
                        
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Listener error: {e}")
        finally:
            await pubsub.unsubscribe()
            await r.close()

    def stop(self):
        self._stop_event.set()

async def main():
    listener = Phase4Listener()
    
    # Handle shutdown signals
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, listener.stop)
        
    await listener.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
