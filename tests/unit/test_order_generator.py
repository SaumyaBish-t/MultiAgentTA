import pytest
import uuid
from unittest.mock import patch, MagicMock
from execution.agents.order_generation_agent import OrderGeneratorAgent

@pytest.mark.asyncio
async def test_sells_before_buys():
    """Verify that orders are sequenced: sells first, then buys."""
    agent = OrderGeneratorAgent()
    
    # 1. Mock state with mixed trades
    state = {
        "rebalance_plan": {
            "trades": [
                {"ticker": "AAPL", "action": "buy", "shares": 10, "value": 2000},
                {"ticker": "MSFT", "action": "sell", "shares": 5, "value": 2100}
            ]
        },
        "market_state": {"prices": {"AAPL": 200, "MSFT": 420}},
        "existing_positions": {},
        "account_state": {"buying_power": 100000},
        "batch_id": uuid.uuid4(),
        "trigger_type": "rebalance",
        "execution_strategy": "immediate"
    }
    
    # We test the generate_orders_node logic indirectly or by calling the node
    from execution.agents.order_generation_agent import generate_orders_node
    result = await generate_orders_node(state)
    
    orders = result["generated_orders"]
    assert len(orders) == 2
    # Sells should be first
    assert orders[0]["action"] == "sell"
    assert orders[1]["action"] == "buy"

@pytest.mark.asyncio
async def test_large_trade_split_into_twap():
    """Verify trades > $100k use staged strategy."""
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
        
        # Mock DB connection context manager
        MockEngine.return_value.begin.return_value.__enter__.return_value = MagicMock()
        
        batch = await agent.generate_from_plan(plan)
        assert batch.execution_strategy == "twap"

@pytest.mark.asyncio
async def test_small_trade_is_market_order():
    """Verify trades < $5k use market orders."""
    agent = OrderGeneratorAgent()
    
    plan = {
        "trades": [{"ticker": "TSLA", "action": "buy", "shares": 10, "value": 1500}]
    }
    
    with patch("execution.agents.order_generation_agent.AlpacaBrokerAdapter") as MockAdapter, \
         patch("execution.agents.order_generation_agent.create_engine") as MockEngine, \
         patch("execution.agents.order_generation_agent.redis.from_url") as MockRedis:
        adapter = MockAdapter.return_value
        adapter.get_market_clock.return_value = {"is_open": True, "next_close": "2026-05-02T20:00:00Z"}
        adapter.get_account.return_value = {"buying_power": 1000000, "portfolio_value": 1000000, "day_trade_count": 0}
        adapter.get_positions.return_value = []
        adapter.get_latest_prices.return_value = {"TSLA": 150.0}
        
        # Mock DB connection context manager
        MockEngine.return_value.begin.return_value.__enter__.return_value = MagicMock()
        
        batch = await agent.generate_from_plan(plan)
        assert batch.orders[0]["order_type"] == "market"

@pytest.mark.asyncio
async def test_medium_trade_is_limit_order():
    """Verify trades between $5k and $100k use limit orders."""
    agent = OrderGeneratorAgent()
    
    plan = {
        "trades": [{"ticker": "GOOGL", "action": "buy", "shares": 100, "value": 15000}]
    }
    
    with patch("execution.agents.order_generation_agent.AlpacaBrokerAdapter") as MockAdapter, \
         patch("execution.agents.order_generation_agent.create_engine") as MockEngine, \
         patch("execution.agents.order_generation_agent.redis.from_url") as MockRedis:
        adapter = MockAdapter.return_value
        adapter.get_market_clock.return_value = {"is_open": True, "next_close": "2026-05-02T20:00:00Z"}
        adapter.get_account.return_value = {"buying_power": 1000000, "portfolio_value": 1000000, "day_trade_count": 0}
        adapter.get_positions.return_value = []
        adapter.get_latest_prices.return_value = {"GOOGL": 150.0}
        
        # Mock DB connection context manager
        MockEngine.return_value.begin.return_value.__enter__.return_value = MagicMock()
        
        batch = await agent.generate_from_plan(plan)
        assert batch.orders[0]["order_type"] == "limit"

@pytest.mark.asyncio
async def test_minimum_trade_100_usd_enforced():
    """Verify trades < $100 are skipped."""
    agent = OrderGeneratorAgent()
    
    plan = {
        "trades": [{"ticker": "PENN", "action": "buy", "shares": 1, "value": 20}]
    }
    
    with patch("execution.agents.order_generation_agent.AlpacaBrokerAdapter") as MockAdapter, \
         patch("execution.agents.order_generation_agent.create_engine") as MockEngine, \
         patch("execution.agents.order_generation_agent.redis.from_url") as MockRedis:
        adapter = MockAdapter.return_value
        adapter.get_market_clock.return_value = {"is_open": True, "next_close": "2026-05-02T20:00:00Z"}
        adapter.get_account.return_value = {"buying_power": 1000000, "portfolio_value": 1000000, "day_trade_count": 0}
        adapter.get_positions.return_value = []
        adapter.get_latest_prices.return_value = {"PENN": 20.0}
        
        # Mock DB connection context manager
        MockEngine.return_value.begin.return_value.__enter__.return_value = MagicMock()
        
        batch = await agent.generate_from_plan(plan)
        assert len(batch.orders) == 0

@pytest.mark.asyncio
async def test_emergency_always_market_order():
    """Verify emergency liquidations always use market orders."""
    agent = OrderGeneratorAgent()
    
    with patch("execution.agents.order_generation_agent.AlpacaBrokerAdapter") as MockAdapter, \
         patch("execution.agents.order_generation_agent.create_engine") as MockEngine, \
         patch("execution.agents.order_generation_agent.redis.from_url") as MockRedis:
        adapter = MockAdapter.return_value
        adapter.get_market_clock.return_value = {"is_open": True, "next_close": "2026-05-02T20:00:00Z"}
        adapter.get_account.return_value = {"buying_power": 1000000, "portfolio_value": 1000000, "day_trade_count": 0}
        adapter.get_positions.return_value = [{"ticker": "AAPL", "shares": 10}]
        adapter.get_latest_prices.return_value = {"AAPL": 150.0}
        
        # Mock DB connection context manager
        MockEngine.return_value.begin.return_value.__enter__.return_value = MagicMock()
        
        batch = await agent.generate_emergency_close(["AAPL"])
        assert batch.orders[0]["order_type"] == "market"
        assert batch.batch_type == "emergency"
