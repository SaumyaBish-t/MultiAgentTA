import pytest
from unittest.mock import MagicMock, patch
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter, InsufficientFundsError

def test_format_order_correctly():
    adapter = AlpacaBrokerAdapter()
    
    # Mock Alpaca Order object
    mock_order = MagicMock()
    mock_order.id = "alpaca-123"
    mock_order.symbol = "AAPL"
    mock_order.qty = "10"
    mock_order.filled_qty = "5"
    mock_order.filled_avg_price = "150.50"
    mock_order.status = "partially_filled"
    mock_order.side = "buy"
    mock_order.submitted_at.isoformat.return_value = "2026-05-01T10:00:00Z"
    
    result = adapter._format_order(mock_order)
    
    assert result["broker_order_id"] == "alpaca-123"
    assert result["ticker"] == "AAPL"
    assert result["requested_shares"] == 10
    assert result["filled_shares"] == 5
    assert result["filled_avg_price"] == 150.50
    assert result["status"] == "partially_filled"
    assert result["action"] == "buy"

@patch("alpaca.trading.client.TradingClient.submit_order")
def test_market_order_request_built_correctly(mock_submit):
    adapter = AlpacaBrokerAdapter()
    
    # Setup mock return
    mock_res = MagicMock()
    mock_res.id = "b-1"
    mock_res.symbol = "AAPL"
    mock_res.qty = "1"
    mock_res.filled_qty = "0"
    mock_res.filled_avg_price = None
    mock_res.status = "pending"
    mock_res.side = "buy"
    mock_res.submitted_at.isoformat.return_value = "now"
    mock_submit.return_value = mock_res
    
    adapter.submit_market_order("AAPL", 1, "buy")
    
    # Check if correct request was sent to Alpaca
    args, kwargs = mock_submit.call_args
    req = args[0]
    assert req.symbol == "AAPL"
    assert req.qty == 1
    assert str(req.side) == "OrderSide.BUY"
    assert str(req.type) == "OrderType.MARKET"

@patch("alpaca.trading.client.TradingClient.submit_order")
def test_limit_order_request_built_correctly(mock_submit):
    adapter = AlpacaBrokerAdapter()
    
    mock_res = MagicMock()
    mock_res.id = "b-2"
    mock_res.symbol = "MSFT"
    mock_res.qty = "5"
    mock_res.filled_qty = "0"
    mock_res.filled_avg_price = None
    mock_res.status = "pending"
    mock_res.side = "buy"
    mock_res.submitted_at.isoformat.return_value = "now"
    mock_submit.return_value = mock_res
    
    adapter.submit_limit_order("MSFT", 5, "buy", 420.50)
    
    args, kwargs = mock_submit.call_args
    req = args[0]
    assert req.symbol == "MSFT"
    assert req.limit_price == 420.50
    assert str(req.type) == "OrderType.LIMIT"

@patch("alpaca.trading.client.TradingClient.submit_order")
def test_order_submission_error_raised_on_failure(mock_submit):
    adapter = AlpacaBrokerAdapter()
    
    # Simulate API error
    mock_submit.side_effect = Exception("Insufficient buying power")
    
    with pytest.raises(InsufficientFundsError):
        adapter.submit_market_order("AAPL", 10000, "buy")

@patch("alpaca.trading.client.TradingClient.close_all_positions")
def test_close_all_positions_calls_broker(mock_close):
    adapter = AlpacaBrokerAdapter()
    mock_close.return_value = []
    
    adapter.close_all_positions()
    mock_close.assert_called_once()

@patch("alpaca.trading.client.TradingClient.cancel_order_by_id")
def test_cancel_order_returns_bool(mock_cancel):
    adapter = AlpacaBrokerAdapter()
    
    # Success case
    mock_cancel.return_value = None
    assert adapter.cancel_order("b-1") is True
    
    # Failure case
    mock_cancel.side_effect = Exception("Not found")
    assert adapter.cancel_order("b-2") is False
