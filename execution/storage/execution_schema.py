from sqlalchemy import (
    Table, Column, String, Float, Integer, Boolean, DateTime, Date, ForeignKey, Index, Text, MetaData
)
from sqlalchemy.dialects.postgresql import UUID
import uuid

metadata = MetaData()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 1: orders
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
orders = Table(
    "orders",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("portfolio_position_id", UUID(as_uuid=True), ForeignKey("portfolio_positions.id"), nullable=True),
    Column("rebalance_id", UUID(as_uuid=True), ForeignKey("rebalance_events.id"), nullable=True),
    Column("signal_id", UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=True),
    Column("ticker", String(20), nullable=False, index=True),
    Column("broker_order_id", String(255), nullable=True),
    Column("order_type", String(50), nullable=False),  # market/limit/stop/twap/vwap
    Column("action", String(20), nullable=False),      # buy/sell/close
    Column("requested_shares", Integer, nullable=False),
    Column("requested_price", Float, nullable=True),
    Column("filled_shares", Integer, default=0, nullable=False),
    Column("filled_avg_price", Float, nullable=True),
    Column("filled_total_value", Float, nullable=True),
    Column("status", String(50), nullable=False),     # pending/submitted/partial/filled/cancelled/rejected/expired
    Column("submitted_at", DateTime(timezone=True), nullable=True),
    Column("filled_at", DateTime(timezone=True), nullable=True),
    Column("cancelled_at", DateTime(timezone=True), nullable=True),
    Column("time_in_force", String(20), nullable=False), # day/gtc/opg/cls/ioc/fok
    Column("extended_hours", Boolean, default=False, nullable=False),
    Column("slippage_pct", Float, nullable=True),
    Column("commission_paid", Float, default=0, nullable=False),
    Column("error_message", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 2: executions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
executions = Table(
    "executions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("order_id", UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False),
    Column("ticker", String(20), nullable=False, index=True),
    Column("execution_price", Float, nullable=False),
    Column("execution_shares", Integer, nullable=False),
    Column("execution_value", Float, nullable=False),
    Column("side", String(20), nullable=False),        # buy/sell
    Column("venue", String(100), nullable=True),       # exchange/dark_pool/otc
    Column("executed_at", DateTime(timezone=True), nullable=False),
    Column("settlement_date", Date, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 3: order_batches
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
order_batches = Table(
    "order_batches",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("rebalance_id", UUID(as_uuid=True), ForeignKey("rebalance_events.id"), nullable=True),
    Column("batch_type", String(50), nullable=False), # open/close/rebalance/emergency
    Column("total_orders", Integer, nullable=False, default=0),
    Column("filled_orders", Integer, nullable=False, default=0),
    Column("failed_orders", Integer, nullable=False, default=0),
    Column("total_value", Float, nullable=False, default=0.0),
    Column("status", String(50), nullable=False),     # pending/running/completed/failed
    Column("started_at", DateTime(timezone=True), nullable=True),
    Column("completed_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 4: execution_performance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
execution_performance = Table(
    "execution_performance",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("order_id", UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False),
    Column("ticker", String(20), nullable=False),
    Column("implementation_shortfall", Float, nullable=True),
    Column("arrival_price", Float, nullable=False),
    Column("execution_price", Float, nullable=False),
    Column("slippage_bps", Float, nullable=True),
    Column("market_impact_bps", Float, nullable=True),
    Column("timing_cost_bps", Float, nullable=True),
    Column("total_cost_bps", Float, nullable=True),
    Column("benchmark", String(50), nullable=True),    # vwap/twap/arrival
    Column("benchmark_price", Float, nullable=True),
    Column("vs_benchmark_bps", Float, nullable=True),
    Column("measured_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 5: broker_connections
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
broker_connections = Table(
    "broker_connections",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("broker_name", String(50), nullable=False), # alpaca/ibkr
    Column("account_number", String(100), nullable=False),
    Column("account_type", String(20), nullable=False),   # paper/live
    Column("cash_balance", Float, nullable=False),
    Column("portfolio_value", Float, nullable=False),
    Column("buying_power", Float, nullable=False),
    Column("day_trade_count", Integer, default=0),
    Column("pattern_day_trader", Boolean, default=False),
    Column("trading_blocked", Boolean, default=False),
    Column("last_synced_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 6: market_hours
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
market_hours = Table(
    "market_hours",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("date", Date, nullable=False, index=True, unique=True),
    Column("is_open", Boolean, nullable=False),
    Column("open_time", DateTime(timezone=True), nullable=True),
    Column("close_time", DateTime(timezone=True), nullable=True),
    Column("early_close", Boolean, default=False),
    Column("session_type", String(50), nullable=False), # regular/extended/closed
    Column("created_at", DateTime(timezone=True), nullable=False),
)
