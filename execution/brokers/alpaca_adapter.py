from datetime import date, datetime
from typing import Dict, List, Any, Optional
import json

from loguru import logger
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.requests import (
    MarketOrderRequest, LimitOrderRequest, GetCalendarRequest, GetOrdersRequest
)
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus, QueryOrderStatus
from alpaca.data.requests import StockLatestQuoteRequest

from config.settings import settings
from execution.brokers.base_broker import BaseBroker

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CUSTOM EXCEPTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class BrokerError(Exception): pass
class OrderSubmissionError(BrokerError): pass
class InsufficientFundsError(BrokerError): pass
class MarketClosedError(BrokerError): pass
class InvalidTickerError(BrokerError): pass
class PositionNotFoundError(BrokerError): pass

class AlpacaBrokerAdapter(BaseBroker):
    """
    Adapter for Alpaca Markets using alpaca-py.
    Standardizes responses into internal dictionaries.
    Always uses Paper Trading mode for development safety.
    """
    
    def __init__(self):
        """Initialize Alpaca clients using settings API keys."""
        api_key = settings.alpaca_api_key.get_secret_value()
        secret_key = settings.alpaca_secret_key.get_secret_value()
        
        self.trading_client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=True  # FORCE PAPER FOR SAFETY
        )
        
        self.data_client = StockHistoricalDataClient(
            api_key=api_key,
            secret_key=secret_key
        )
        logger.info("Alpaca Broker Adapter initialized (Paper Mode)")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SECTION 2 — ACCOUNT METHODS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_account(self) -> Dict[str, Any]:
        """Fetch account summary and balances."""
        try:
            account = self.trading_client.get_account()
            return {
                "account_number": account.account_number,
                "cash": float(account.cash),
                "portfolio_value": float(account.portfolio_value),
                "buying_power": float(account.buying_power),
                "day_trade_count": account.daytrade_count,
                "pattern_day_trader": account.pattern_day_trader,
                "trading_blocked": account.trading_blocked,
                "account_blocked": account.account_blocked
            }
        except Exception as e:
            logger.error(f"Failed to fetch account: {e}")
            raise BrokerError(str(e))

    def get_positions(self) -> List[Dict[str, Any]]:
        """Fetch all open positions."""
        try:
            positions = self.trading_client.get_all_positions()
            return [{
                "ticker": p.symbol,
                "shares": int(p.qty),
                "current_price": float(p.current_price),
                "market_value": float(p.market_value),
                "cost_basis": float(p.cost_basis),
                "unrealized_pnl": float(p.unrealized_pl),
                "unrealized_pnl_pct": float(p.unrealized_plpc),
                "side": p.side.value if hasattr(p.side, 'value') else str(p.side)
            } for p in positions]
        except Exception as e:
            logger.error(f"Failed to fetch positions: {e}")
            return []

    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Fetch all open orders."""
        try:
            # Fetch only open orders by default
            request = GetOrdersRequest(status=QueryOrderStatus.OPEN)
            orders = self.trading_client.get_orders(filter=request)
            return [self._format_order(o) for o in orders]
        except Exception as e:
            logger.error(f"Failed to fetch open orders: {e}")
            return []

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SECTION 3 — ORDER METHODS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def submit_market_order(
        self,
        ticker: str,
        shares: int,
        action: str,  # buy/sell
        time_in_force: str = "day"
    ) -> Dict[str, Any]:
        """Submit a market order."""
        tif_map = {
            "day": TimeInForce.DAY,
            "gtc": TimeInForce.GTC,
            "opg": TimeInForce.OPG,
            "cls": TimeInForce.CLS,
            "ioc": TimeInForce.IOC,
            "fok": TimeInForce.FOK
        }
        
        request = MarketOrderRequest(
            symbol=ticker,
            qty=shares,
            side=OrderSide.BUY if action.lower() == "buy" else OrderSide.SELL,
            time_in_force=tif_map.get(time_in_force.lower(), TimeInForce.DAY)
        )
        
        try:
            order = self.trading_client.submit_order(request)
            logger.info(f"Market Order Submitted: {action} {shares} {ticker}")
            return self._format_order(order)
        except Exception as e:
            logger.error(f"Order failed for {ticker}: {e}")
            # Check for specific errors like insufficient funds
            err_msg = str(e).lower()
            if "insufficient" in err_msg or "buying power" in err_msg:
                raise InsufficientFundsError(str(e))
            raise OrderSubmissionError(str(e))

    def submit_limit_order(
        self,
        ticker: str,
        shares: int,
        action: str,
        limit_price: float,
        time_in_force: str = "day"
    ) -> Dict[str, Any]:
        """Submit a limit order."""
        tif_map = {
            "day": TimeInForce.DAY,
            "gtc": TimeInForce.GTC,
            "opg": TimeInForce.OPG,
            "cls": TimeInForce.CLS,
            "ioc": TimeInForce.IOC,
            "fok": TimeInForce.FOK
        }
        
        request = LimitOrderRequest(
            symbol=ticker,
            qty=shares,
            side=OrderSide.BUY if action.lower() == "buy" else OrderSide.SELL,
            limit_price=round(limit_price, 2),
            time_in_force=tif_map.get(time_in_force.lower(), TimeInForce.DAY)
        )
        
        try:
            order = self.trading_client.submit_order(request)
            logger.info(f"Limit Order Submitted: {action} {shares} {ticker} @ ${limit_price:.2f}")
            return self._format_order(order)
        except Exception as e:
            logger.error(f"Limit order failed for {ticker}: {e}")
            raise OrderSubmissionError(str(e))

    def cancel_order(self, broker_order_id: str) -> bool:
        """Cancel a specific order."""
        try:
            self.trading_client.cancel_order_by_id(broker_order_id)
            logger.info(f"Order Cancelled: {broker_order_id}")
            return True
        except Exception as e:
            logger.error(f"Cancel failed for {broker_order_id}: {e}")
            return False

    def cancel_all_orders(self) -> int:
        """Cancel all open orders."""
        try:
            cancelled = self.trading_client.cancel_orders()
            logger.info(f"Cancelled {len(cancelled)} open orders")
            return len(cancelled)
        except Exception as e:
            logger.error(f"Cancel all failed: {e}")
            return 0

    def close_position(self, ticker: str) -> Dict[str, Any]:
        """Close a specific position."""
        try:
            order = self.trading_client.close_position(ticker)
            logger.info(f"Position Closed: {ticker}")
            return self._format_order(order)
        except Exception as e:
            logger.error(f"Close position failed for {ticker}: {e}")
            raise PositionNotFoundError(str(e))

    def close_all_positions(self) -> List[Dict[str, Any]]:
        """Close all positions (liquidate)."""
        try:
            orders = self.trading_client.close_all_positions(cancel_orders=True)
            logger.info(f"Liquidating all positions ({len(orders)} orders generated)")
            return [self._format_order(o) for o in orders]
        except Exception as e:
            logger.error(f"Close all positions failed: {e}")
            return []

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SECTION 4 — MARKET DATA METHODS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_latest_price(self, ticker: str) -> float:
        """Fetch latest ask price."""
        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=ticker)
            quote = self.data_client.get_stock_latest_quote(request)
            return float(quote[ticker].ask_price)
        except Exception as e:
            logger.error(f"Failed to fetch price for {ticker}: {e}")
            raise BrokerError(f"Price unavailable for {ticker}")

    def get_latest_prices(self, tickers: List[str]) -> Dict[str, float]:
        """Fetch latest ask prices for multiple tickers."""
        if not tickers: return {}
        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=tickers)
            quotes = self.data_client.get_stock_latest_quote(request)
            return {t: float(q.ask_price) for t, q in quotes.items()}
        except Exception as e:
            logger.error(f"Failed to fetch batch prices: {e}")
            return {}

    def get_order_status(self, broker_order_id: str) -> Dict[str, Any]:
        """Fetch latest order status."""
        try:
            order = self.trading_client.get_order_by_id(broker_order_id)
            return self._format_order(order)
        except Exception as e:
            logger.error(f"Failed to fetch order status for {broker_order_id}: {e}")
            raise BrokerError(str(e))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SECTION 5 — MARKET HOURS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def is_market_open(self) -> bool:
        """Check if market is currently open."""
        try:
            clock = self.trading_client.get_clock()
            return clock.is_open
        except Exception as e:
            logger.error(f"Clock fetch failed: {e}")
            return False

    def get_market_clock(self) -> Dict[str, Any]:
        """Fetch market clock details."""
        try:
            clock = self.trading_client.get_clock()
            return {
                "is_open": clock.is_open,
                "next_open": clock.next_open.isoformat(),
                "next_close": clock.next_close.isoformat(),
                "timestamp": clock.timestamp.isoformat()
            }
        except Exception as e:
            logger.error(f"Market clock error: {e}")
            raise BrokerError(str(e))

    def get_calendar(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch market calendar."""
        try:
            request = GetCalendarRequest(start=start_date, end=end_date)
            calendar = self.trading_client.get_calendar(request)
            return [{
                "date": c.date.isoformat(),
                "open": c.open.isoformat(),
                "close": c.close.isoformat()
            } for c in calendar]
        except Exception as e:
            logger.error(f"Calendar fetch failed: {e}")
            return []

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SECTION 6 — HELPER METHODS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _format_order(self, order) -> Dict[str, Any]:
        """Convert Alpaca order object to internal dictionary format."""
        return {
            "broker_order_id": str(order.id),
            "ticker": order.symbol,
            "order_type": order.order_type.value if hasattr(order.order_type, 'value') else str(order.order_type),
            "action": order.side.value if hasattr(order.side, 'value') else str(order.side),
            "requested_shares": int(order.qty or 0),
            "filled_shares": int(order.filled_qty or 0),
            "filled_avg_price": float(order.filled_avg_price or 0.0),
            "status": order.status.value if hasattr(order.status, 'value') else str(order.status),
            "submitted_at": order.submitted_at.isoformat() if order.submitted_at else None,
            "filled_at": order.filled_at.isoformat() if order.filled_at else None,
            "time_in_force": order.time_in_force.value if hasattr(order.time_in_force, 'value') else str(order.time_in_force)
        }
