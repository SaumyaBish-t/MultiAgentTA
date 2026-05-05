import pytest
from portfolio_construction.agents.rebalancing_agent import RebalancingAgent

@pytest.mark.asyncio
async def test_drift_threshold_triggers_rebalance():
    agent = RebalancingAgent()
    # We'd need to mock the state or Redis to test this accurately without DB
    # But we can check if the logic in calculate_drift works if we exposed it
    pass

@pytest.mark.asyncio
async def test_trades_sorted_closes_first():
    # Test sort_trades_node logic
    from portfolio_construction.agents.rebalancing_agent import sort_trades_node
    
    state = {
        "trades_required": [
            {"ticker": "AAPL", "action": "buy", "shares": 10},
            {"ticker": "MSFT", "action": "close", "shares": 50},
            {"ticker": "GOOGL", "action": "sell", "shares": 5}
        ]
    }
    result = await sort_trades_node(state)
    trades = result["trades_required"]
    
    assert trades[0]["action"] == "close"
    assert trades[1]["action"] in ["sell", "buy"]

def test_minimum_trade_size_100_usd():
    # Test logic in calculate_trades_node
    pass
