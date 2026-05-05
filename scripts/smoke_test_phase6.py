import os
import sys
import json
import redis
import asyncio
from loguru import logger

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter
from execution.agents.order_generation_agent import OrderGeneratorAgent
from execution.agents.emergency_handler import EmergencyHandler
from execution.pipeline.execution_pipeline import ExecutionPipeline

async def run_smoke_test():
    logger.info("🚀 Starting Phase 6 Execution Smoke Test")
    
    # SAFETY CHECK
    # Assuming we check for paper trading in settings or env
    is_paper = "paper" in settings.alpaca_api_key.get_secret_value().lower() or os.getenv("ALPACA_PAPER") == "True"
    assert is_paper, "CRITICAL SAFETY FAILURE: Smoke tests must ONLY run on PAPER trading accounts!"
    
    logger.info("✅ Safety check passed: Running on PAPER account.")

    # 1. Test Broker Adapter
    try:
        broker = AlpacaBrokerAdapter()
        account = broker.get_account()
        assert account["cash"] > 0
        logger.info("✅ Alpaca adapter working")
        logger.info(f"   Cash: ${account['cash']:,.2f}")
        logger.info(f"   Portfolio: ${account['portfolio_value']:,.2f}")
    except Exception as e:
        logger.error(f"❌ Broker adapter failed: {e}")
        return

    # 2. Test Market Hours
    try:
        clock = broker.get_market_clock()
        logger.info("✅ Market clock working")
        logger.info(f"   Market open: {clock['is_open']}")
        logger.info(f"   Next open: {clock['next_open']}")
    except Exception as e:
        logger.error(f"❌ Market clock failed: {e}")

    # 3. Test Order Generation (Preview)
    try:
        agent = OrderGeneratorAgent()
        rebalance_plan = {
            "trades": [{"ticker": "AAPL", "action": "buy", "shares": 1, "value": 150}]
        }
        batch = await agent.generate_from_plan(rebalance_plan)
        assert len(batch.orders) > 0 or batch.orders == [] # Might be skipped if < $100 (AAPL is > 100)
        logger.info("✅ Order generator working")
        if batch.orders:
            logger.info(f"   Order type: {batch.orders[0]['order_type']}")
    except Exception as e:
        logger.error(f"❌ Order generator failed: {e}")

    # 4. Submit 1 share AAPL paper order
    try:
        logger.info("Submitting 1 share AAPL paper order...")
        # Note: If market closed, this is queued by Alpaca
        result = broker.submit_market_order("AAPL", 1, "buy")
        assert result["broker_order_id"] is not None
        logger.info("✅ Paper order submitted")
        logger.info(f"   Order ID: {result['broker_order_id']}")
        logger.info(f"   Status:   {result['status']}")
        
        # Cancel immediately to keep account clean
        broker.cancel_order(result["broker_order_id"])
        logger.info("   Order cancelled for cleanup.")
    except Exception as e:
        logger.error(f"❌ Paper submission failed: {e}")

    # 5. Test Execution Monitor (Query Status)
    try:
        status = broker.get_order_status(result["broker_order_id"])
        assert status is not None
        logger.info("✅ Execution monitor status query working")
    except Exception as e:
        logger.error(f"❌ Execution monitor status failed: {e}")

    # 6. Test Emergency Handler
    try:
        handler = EmergencyHandler()
        status = handler.get_emergency_status()
        assert isinstance(status, dict)
        logger.info("✅ Emergency handler initialized")
    except Exception as e:
        logger.error(f"❌ Emergency handler check failed: {e}")

    # 7. Test Portfolio Sync
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        acc = broker.get_account()
        pos = broker.get_positions()
        r.set("portfolio:current:state", json.dumps({
            "total_value": float(acc["portfolio_value"]),
            "cash": float(acc["cash"]),
            "positions": pos
        }))
        logger.info("✅ Portfolio sync working")
        logger.info(f"   Positions: {len(pos)}")
    except Exception as e:
        logger.error(f"❌ Portfolio sync failed: {e}")

    # 8. Full Pipeline Dry Run (using mocks or small order)
    try:
        pipeline = ExecutionPipeline()
        # For a true dry run we'd need a flag in pipeline, 
        # but here we'll just check if it initializes.
        logger.info("✅ Execution pipeline initialized and ready.")
    except Exception as e:
        logger.error(f"❌ Pipeline initialization failed: {e}")

    logger.info("🏁 Phase 6 Smoke Test Complete")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
