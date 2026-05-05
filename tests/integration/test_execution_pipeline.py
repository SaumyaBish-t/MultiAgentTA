import pytest
import asyncio
import os
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter

@pytest.fixture
def broker():
    # Safety check
    assert os.getenv("ALPACA_PAPER") == "True" or "paper" in settings.alpaca_api_key.get_secret_value().lower(), "MUST use paper trading!"
    return AlpacaBrokerAdapter()

def test_account_sync_paper(broker):
    """Verify we can fetch live paper account data."""
    acc = broker.get_account()
    assert acc["cash"] > 0
    assert "portfolio_value" in acc
    
    pos = broker.get_positions()
    assert isinstance(pos, list)

@pytest.mark.asyncio
async def test_submit_limit_order_paper(broker):
    """Submit a limit order far from market and cancel it."""
    # Market must be open for this to work normally, 
    # but Alpaca allows submitting even when closed (they queue it).
    
    ticker = "AAPL"
    # Price $1.00 is far from market
    res = broker.submit_limit_order(ticker, 1, "buy", 1.00)
    
    assert res["broker_order_id"] is not None
    assert res["status"] in ["accepted", "pending", "new"]
    
    # Cancel it
    success = broker.cancel_order(res["broker_order_id"])
    assert success is True

@pytest.mark.asyncio
async def test_submit_and_fill_market_order_paper(broker):
    """
    Submit small market order for AAPL (1 share).
    ONLY RUN THIS IF MARKET IS OPEN.
    """
    clock = broker.get_market_clock()
    if not clock["is_open"]:
        pytest.skip("Market is closed, cannot test market order fill.")
        
    res = broker.submit_market_order("AAPL", 1, "buy")
    assert res["broker_order_id"] is not None
    
    # Wait up to 30s for fill
    filled = False
    for _ in range(6):
        await asyncio.sleep(5)
        status = broker.get_order_status(res["broker_order_id"])
        if status["status"] == "filled":
            filled = True
            break
            
    assert filled is True
    assert status["filled_shares"] == 1
    
    # Liquidate immediately to keep account clean
    broker.submit_market_order("AAPL", 1, "sell")
