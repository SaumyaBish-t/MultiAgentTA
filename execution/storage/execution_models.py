from datetime import date, datetime
import uuid
from typing import Optional, List

from sqlalchemy import (
    DateTime, Date, Float, Integer, String, Text, Boolean, ForeignKey, Index, func
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_ingestion.storage.models import FundamentalBase
import portfolio_construction.storage.portfolio_models
import signal_generation.storage.signal_models

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 1: orders
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class Order(FundamentalBase):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    portfolio_position_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("portfolio_positions.id"), nullable=True)
    rebalance_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("rebalance_events.id"), nullable=True)
    signal_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=True)
    
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    broker_order_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    order_type: Mapped[str] = mapped_column(String(50), nullable=False) # market/limit/stop/twap/vwap
    action: Mapped[str] = mapped_column(String(20), nullable=False)      # buy/sell/close
    
    requested_shares: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    
    filled_shares: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    filled_avg_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    filled_total_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    
    status: Mapped[str] = mapped_column(String(50), nullable=False) # pending/submitted/partial/filled/cancelled/rejected/expired
    
    submitted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    filled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    cancelled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    time_in_force: Mapped[str] = mapped_column(String(20), nullable=False) # day/gtc/opg/cls/ioc/fok
    extended_hours: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    
    slippage_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    commission_paid: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    executions: Mapped[List["Execution"]] = relationship(back_populates="order")
    performance: Mapped[Optional["ExecutionPerformance"]] = relationship(back_populates="order")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 2: executions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class Execution(FundamentalBase):
    __tablename__ = "executions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    
    execution_price: Mapped[float] = mapped_column(Float, nullable=False)
    execution_shares: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_value: Mapped[float] = mapped_column(Float, nullable=False)
    
    side: Mapped[str] = mapped_column(String(20), nullable=False)        # buy/sell
    venue: Mapped[Optional[str]] = mapped_column(String(100), nullable=True) # exchange/dark_pool/otc
    
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    settlement_date: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    order: Mapped["Order"] = relationship(back_populates="executions")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 3: order_batches
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class OrderBatch(FundamentalBase):
    __tablename__ = "order_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rebalance_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("rebalance_events.id"), nullable=True)
    batch_type: Mapped[str] = mapped_column(String(50), nullable=False) # open/close/rebalance/emergency
    
    total_orders: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    filled_orders: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_orders: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    
    status: Mapped[str] = mapped_column(String(50), nullable=False) # pending/running/completed/failed
    
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 4: execution_performance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ExecutionPerformance(FundamentalBase):
    __tablename__ = "execution_performance"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    
    implementation_shortfall: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    arrival_price: Mapped[float] = mapped_column(Float, nullable=False)
    execution_price: Mapped[float] = mapped_column(Float, nullable=False)
    
    slippage_bps: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    market_impact_bps: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    timing_cost_bps: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_cost_bps: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    
    benchmark: Mapped[Optional[str]] = mapped_column(String(50), nullable=True) # vwap/twap/arrival
    benchmark_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    vs_benchmark_bps: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    
    measured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    order: Mapped["Order"] = relationship(back_populates="performance")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 5: broker_connections
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class BrokerConnection(FundamentalBase):
    __tablename__ = "broker_connections"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    broker_name: Mapped[str] = mapped_column(String(50), nullable=False) # alpaca/ibkr
    account_number: Mapped[str] = mapped_column(String(100), nullable=False)
    account_type: Mapped[str] = mapped_column(String(20), nullable=False) # paper/live
    
    cash_balance: Mapped[float] = mapped_column(Float, nullable=False)
    portfolio_value: Mapped[float] = mapped_column(Float, nullable=False)
    buying_power: Mapped[float] = mapped_column(Float, nullable=False)
    
    day_trade_count: Mapped[int] = mapped_column(Integer, default=0)
    pattern_day_trader: Mapped[bool] = mapped_column(Boolean, default=False)
    trading_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    
    last_synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 6: market_hours
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class MarketHour(FundamentalBase):
    __tablename__ = "market_hours"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True, unique=True)
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False)
    
    open_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    close_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    early_close: Mapped[bool] = mapped_column(Boolean, default=False)
    session_type: Mapped[str] = mapped_column(String(50), nullable=False) # regular/extended/closed
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
