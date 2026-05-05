from datetime import date, datetime
import uuid
from typing import Any

from sqlalchemy import (
    DateTime,
    Date,
    Float,
    Index,
    Integer,
    String,
    Text,
    Boolean,
    ForeignKey,
    func,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from data_ingestion.storage.models import FundamentalBase

# Ensure risk_management metadata is loaded so ForeignKey references work
import risk_management.storage.risk_models

class Portfolio(FundamentalBase):
    __tablename__ = "portfolios"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    strategy: Mapped[str] = mapped_column(String(50), nullable=False)
    total_capital: Mapped[float] = mapped_column(Float, nullable=False)
    invested_capital: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cash: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

class PortfolioPosition(FundamentalBase):
    __tablename__ = "portfolio_positions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    portfolio_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("portfolios.id"), nullable=False)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("approved_signals.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    target_weight: Mapped[float] = mapped_column(Float, nullable=False)
    current_weight: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    target_shares: Mapped[int] = mapped_column(Integer, nullable=False)
    current_shares: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    target_value_usd: Mapped[float] = mapped_column(Float, nullable=False)
    current_value_usd: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    entry_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    current_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    unrealized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    unrealized_pnl_pct: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

class PortfolioWeight(FundamentalBase):
    __tablename__ = "portfolio_weights"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    portfolio_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("portfolios.id"), nullable=False)
    optimization_method: Mapped[str] = mapped_column(String(50), nullable=False)
    weights: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    expected_return: Mapped[float] = mapped_column(Float, nullable=False)
    expected_volatility: Mapped[float] = mapped_column(Float, nullable=False)
    expected_sharpe: Mapped[float] = mapped_column(Float, nullable=False)
    optimization_inputs: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    constraints_applied: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class RebalanceEvent(FundamentalBase):
    __tablename__ = "rebalance_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    portfolio_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("portfolios.id"), nullable=False)
    trigger_type: Mapped[str] = mapped_column(String(50), nullable=False)
    trigger_reason: Mapped[str] = mapped_column(Text, nullable=False)
    positions_before: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    positions_after: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    trades_required: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    estimated_cost: Mapped[float] = mapped_column(Float, nullable=False)
    estimated_tax_impact: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    approved: Mapped[bool] = mapped_column(Boolean, default=False)
    executed: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

class FactorExposure(FundamentalBase):
    __tablename__ = "factor_exposures"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    portfolio_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("portfolios.id"), nullable=False)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    market_beta: Mapped[float] = mapped_column(Float, nullable=False)
    size_factor: Mapped[float] = mapped_column(Float, nullable=False)
    value_factor: Mapped[float] = mapped_column(Float, nullable=False)
    momentum_factor: Mapped[float] = mapped_column(Float, nullable=False)
    quality_factor: Mapped[float] = mapped_column(Float, nullable=False)
    volatility_factor: Mapped[float] = mapped_column(Float, nullable=False)
    sector_weights: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    geographic_weights: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class PortfolioPerformance(FundamentalBase):
    __tablename__ = "portfolio_performance"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    portfolio_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("portfolios.id"), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    portfolio_value: Mapped[float] = mapped_column(Float, nullable=False)
    daily_return: Mapped[float] = mapped_column(Float, nullable=False)
    cumulative_return: Mapped[float] = mapped_column(Float, nullable=False)
    benchmark_return: Mapped[float] = mapped_column(Float, nullable=False)
    excess_return: Mapped[float] = mapped_column(Float, nullable=False)
    rolling_sharpe_30d: Mapped[float] = mapped_column(Float, nullable=False)
    rolling_volatility_30d: Mapped[float] = mapped_column(Float, nullable=False)
    rolling_max_drawdown: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class CostEstimate(FundamentalBase):
    __tablename__ = "cost_estimates"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rebalance_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("rebalance_events.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    action: Mapped[str] = mapped_column(String(10), nullable=False)
    shares: Mapped[int] = mapped_column(Integer, nullable=False)
    estimated_price: Mapped[float] = mapped_column(Float, nullable=False)
    commission: Mapped[float] = mapped_column(Float, nullable=False)
    market_impact: Mapped[float] = mapped_column(Float, nullable=False)
    spread_cost: Mapped[float] = mapped_column(Float, nullable=False)
    total_cost: Mapped[float] = mapped_column(Float, nullable=False)
    cost_as_pct_of_trade: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
