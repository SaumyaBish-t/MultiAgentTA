import json
import asyncio
from loguru import logger

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal
from risk_management.pipeline.risk_pipeline import RiskPipeline

# If using Prefect directly, you could trigger the flow:
# from risk_management.flows.risk_flow import risk_evaluation_flow

async def fetch_signals_from_db(signal_ids: list[str]) -> list[dict]:
    """Fetch validated signals from PostgreSQL by their UUIDs."""
    # Note: We use synchronous engine in other agents, but standardizing to async if needed
    # For simplicity and to match the other agents, we'll use standard sync SQLAlchemy wrapped in a thread if needed,
    # or just simple sync call since the volume of ids is small.
    from sqlalchemy import create_engine
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    signals = []
    try:
        with Session() as session:
            stmt = select(TradingSignal).where(TradingSignal.id.in_(signal_ids))
            records = session.execute(stmt).scalars().all()
            for r in records:
                signals.append({
                    "id": str(r.id),
                    "ticker": r.ticker,
                    "direction": r.direction,
                    "conviction_score": float(r.conviction_score),
                    "strategy_type": r.strategy_type
                })
    except Exception as e:
        logger.error(f"Failed to fetch signals from DB: {e}")
        
    return signals

async def listen_for_phase3_completion():
    """Subscribe to the Redis pubsub channel and trigger Phase 4 Pipeline."""
    logger.info("Starting Phase 3 Listener (Subscribing to 'signals.pipeline.completed')")
    
    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("signals.pipeline.completed")
    
    pipeline = RiskPipeline()
    
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    payload = json.loads(message["data"])
                    logger.info(f"Received Phase 3 completion event. Run ID: {payload.get('run_id')}")
                    
                    # 1. Parse signal IDs
                    signal_ids = payload.get("validated_signals", [])
                    
                    if not signal_ids:
                        logger.warning("Event contained no validated signals. Skipping Phase 4.")
                        continue
                        
                    # 2. Fetch signals from DB
                    logger.info(f"Fetching {len(signal_ids)} signals from database...")
                    signals = await fetch_signals_from_db(signal_ids)
                    
                    if not signals:
                        logger.error("Failed to fetch any of the requested signals from DB.")
                        continue
                        
                    # 3. Trigger Phase 4 Pipeline
                    logger.info(f"Triggering Phase 4 Risk Pipeline for {len(signals)} signals...")
                    
                    # You could optionally trigger the Prefect flow here:
                    # await risk_evaluation_flow(signals)
                    
                    result = await pipeline.run(signals)
                    logger.info(f"Phase 4 Run Complete: {result.signals_approved} approved, {result.signals_rejected} rejected.")
                    
                except json.JSONDecodeError:
                    logger.error("Failed to parse message payload as JSON.")
                except Exception as e:
                    logger.exception(f"Error processing Phase 3 event: {e}")
                    
    except asyncio.CancelledError:
        logger.info("Phase 3 Listener shutting down.")
    finally:
        await pubsub.unsubscribe("signals.pipeline.completed")
        await redis_client.aclose()

if __name__ == "__main__":
    try:
        asyncio.run(listen_for_phase3_completion())
    except KeyboardInterrupt:
        logger.info("Listener stopped manually.")
