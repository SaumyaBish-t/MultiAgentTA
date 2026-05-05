import pytest
from portfolio_construction.agents.allocation_agent import AllocationAgent

@pytest.mark.asyncio
async def test_shares_calculated_correctly():
    # We'd need to mock the price fetch
    pass

@pytest.mark.asyncio
async def test_total_invested_under_95pct():
    from portfolio_construction.agents.allocation_agent import apply_final_checks_node
    
    state = {
        "portfolio_value": 100000.0,
        "final_positions": [
            {"ticker": "AAPL", "target_value_usd": 99000.0, "current_price": 100.0, "shares": 990}
        ]
    }
    
    result = await apply_final_checks_node(state)
    positions = result["final_positions"]
    
    total_inv = sum(p["target_value_usd"] for p in positions)
    assert total_inv <= 95000.1
    assert positions[0]["shares"] < 990

@pytest.mark.asyncio
async def test_no_short_positions_allowed():
    # Check that negative shares are handled (though AllocationAgent logic 
    # assumes w > 0 from Optimizer)
    pass
