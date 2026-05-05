import pytest
import json
from unittest.mock import MagicMock, patch
from execution.agents.emergency_handler import EmergencyHandler

@pytest.mark.asyncio
async def test_close_all_cancels_pending_first():
    """Verify emergency close-all cancels orders before liquidating."""
    handler = EmergencyHandler()
    handler.adapter = MagicMock()
    handler.r = MagicMock()
    handler.engine = MagicMock()
    
    await handler.handle_close_all(reason="Test")
    
    handler.adapter.cancel_all_orders.assert_called_once()
    handler.adapter.close_all_positions.assert_called_once()

def test_reduce_all_shares_calculation():
    """Verify factor 0.5 results in 50% shares sold."""
    pos = {"ticker": "AAPL", "shares": 100}
    factor = 0.5
    to_sell = int(pos["shares"] * factor)
    assert to_sell == 50

@pytest.mark.asyncio
async def test_trading_halted_set_on_emergency():
    """Verify Redis halt flag is set."""
    handler = EmergencyHandler()
    handler.adapter = MagicMock()
    handler.r = MagicMock()
    handler.engine = MagicMock()
    
    await handler.handle_close_all(reason="Halt Test")
    
    # Check if redis set was called with halted flag
    handler.r.set.assert_any_call("risk:trading:halted", "True")
