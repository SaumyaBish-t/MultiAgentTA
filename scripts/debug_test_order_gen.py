import asyncio
import uuid
from unittest.mock import patch, MagicMock
from execution.agents.order_generation_agent import OrderGeneratorAgent

async def debug_test():
    agent = OrderGeneratorAgent()
    plan = {
        "trades": [{"ticker": "AMZN", "action": "buy", "shares": 1000, "value": 180000}]
    }
    
    with patch("execution.agents.order_generation_agent.AlpacaBrokerAdapter") as MockAdapter, \
         patch("execution.agents.order_generation_agent.create_engine") as MockEngine, \
         patch("execution.agents.order_generation_agent.redis.from_url") as MockRedis:
        adapter = MockAdapter.return_value
        adapter.get_market_clock.return_value = {"is_open": True, "next_close": "2026-05-02T20:00:00Z"}
        adapter.get_account.return_value = {"buying_power": 1000000, "portfolio_value": 1000000, "day_trade_count": 0}
        adapter.get_positions.return_value = []
        adapter.get_latest_prices.return_value = {"AMZN": 180.0}
        MockEngine.return_value.begin.return_value.__enter__.return_value = MagicMock()
        
        print("Running generate_from_plan...")
        try:
            batch = await agent.generate_from_plan(plan)
            print(f"Batch Strategy: {batch.execution_strategy if batch else 'None'}")
        except Exception as e:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_test())
