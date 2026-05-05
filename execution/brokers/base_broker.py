from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import date

class BaseBroker(ABC):
    """
    Abstract Base Class for Broker Adapters.
    Defines the standard interface for all brokers (Alpaca, IBKR, etc.)
    """

    @abstractmethod
    def get_account(self) -> Dict[str, Any]:
        """Fetch account details (cash, equity, buying power)."""
        pass

    @abstractmethod
    def get_positions(self) -> List[Dict[str, Any]]:
        """Fetch current open positions."""
        pass

    @abstractmethod
    def get_open_orders(self) -> List[Dict[str, Any]]:
        """Fetch all currently open orders."""
        pass

    @abstractmethod
    def submit_market_order(
        self,
        ticker: str,
        shares: int,
        action: str,  # buy/sell
        time_in_force: str = "day"
    ) -> Dict[str, Any]:
        """Submit a market order."""
        pass

    @abstractmethod
    def submit_limit_order(
        self,
        ticker: str,
        shares: int,
        action: str,
        limit_price: float,
        time_in_force: str = "day"
    ) -> Dict[str, Any]:
        """Submit a limit order."""
        pass

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> bool:
        """Cancel a specific order by broker ID."""
        pass

    @abstractmethod
    def cancel_all_orders(self) -> int:
        """Cancel all open orders for the account."""
        pass

    @abstractmethod
    def close_position(self, ticker: str) -> Dict[str, Any]:
        """Close a specific position."""
        pass

    @abstractmethod
    def close_all_positions(self) -> List[Dict[str, Any]]:
        """Liquidate the entire portfolio."""
        pass

    @abstractmethod
    def get_latest_price(self, ticker: str) -> float:
        """Fetch the latest quote price (ask) for a ticker."""
        pass

    @abstractmethod
    def get_latest_prices(self, tickers: List[str]) -> Dict[str, float]:
        """Fetch the latest quotes for multiple tickers."""
        pass

    @abstractmethod
    def get_order_status(self, broker_order_id: str) -> Dict[str, Any]:
        """Fetch the latest status of an order."""
        pass

    @abstractmethod
    def is_market_open(self) -> bool:
        """Check if the market is currently open for regular session."""
        pass

    @abstractmethod
    def get_market_clock(self) -> Dict[str, Any]:
        """Fetch the current market clock details."""
        pass

    @abstractmethod
    def get_calendar(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Fetch market calendar for a specific date range."""
        pass
