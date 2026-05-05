import pytest
import math
from portfolio_construction.agents.cost_estimator_agent import CostEstimatorAgent

@pytest.mark.asyncio
async def test_sec_fee_applied_to_sells_only():
    from portfolio_construction.agents.cost_estimator_agent import estimate_commission_node
    
    state = {
        "trades": [
            {"ticker": "AAPL", "action": "buy", "shares": 100},
            {"ticker": "MSFT", "action": "sell", "shares": 100}
        ],
        "ticker_data": {
            "AAPL": {"current_price": 150.0},
            "MSFT": {"current_price": 300.0}
        }
    }
    
    result = await estimate_commission_node(state)
    breakdown = result["cost_breakdown"]
    
    # AAPL (buy) should have 0 SEC fee
    aapl = next(x for x in breakdown if x["ticker"] == "AAPL")
    assert aapl["sec_fee"] == 0.0
    
    # MSFT (sell) should have > 0 SEC fee
    msft = next(x for x in breakdown if x["ticker"] == "MSFT")
    assert msft["sec_fee"] > 0.0

@pytest.mark.asyncio
async def test_market_impact_calculation():
    from portfolio_construction.agents.cost_estimator_agent import estimate_market_impact_node
    
    state = {
        "cost_breakdown": [
            {"ticker": "AAPL", "trade_value": 100000.0}
        ],
        "ticker_data": {
            "AAPL": {
                "realized_vol_30d": 0.20,
                "avg_30d_dollar_volume": 1000000.0 # 10% participation
            }
        }
    }
    
    result = await estimate_market_impact_node(state)
    impact = result["cost_breakdown"][0]["market_impact_usd"]
    
    # impact_pct = 0.1 * (0.2/sqrt(252)) * sqrt(0.1)
    expected_pct = 0.1 * (0.2 / math.sqrt(252)) * math.sqrt(0.1)
    expected_usd = expected_pct * 100000.0
    
    assert math.isclose(impact, expected_usd, rel_tol=1e-5)
