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

# Ensure trading_signals metadata is loaded so ForeignKey references work
import signal_generation.storage.signal_models 

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 1: position_limits
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class PositionLimit(FundamentalBase):
    __tablename__ = "position_limits"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    max_position_size_pct: Mapped[float] = mapped_column(Float, nullable=False)
    max_position_size_usd: Mapped[float] = mapped_column(Float, nullable=False)
    max_shares: Mapped[int] = mapped_column(Integer, nullable=True)
    sizing_method: Mapped[str] = mapped_column(String(50), nullable=False)
    kelly_fraction: Mapped[float] = mapped_column(Float, nullable=True)
    volatility_scalar: Mapped[float] = mapped_column(Float, nullable=True)
    approved: Mapped[bool] = mapped_column(Boolean, default=False)
    approved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 2: portfolio_risk_snapshots
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class PortfolioRiskSnapshot(FundamentalBase):
    __tablename__ = "portfolio_risk_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    total_portfolio_value: Mapped[float] = mapped_column(Float, nullable=False)
    cash_pct: Mapped[float] = mapped_column(Float, nullable=False)
    invested_pct: Mapped[float] = mapped_column(Float, nullable=False)
    long_exposure_pct: Mapped[float] = mapped_column(Float, nullable=False)
    short_exposure_pct: Mapped[float] = mapped_column(Float, nullable=False)
    net_exposure_pct: Mapped[float] = mapped_column(Float, nullable=False)
    gross_exposure_pct: Mapped[float] = mapped_column(Float, nullable=False)
    current_drawdown_pct: Mapped[float] = mapped_column(Float, nullable=False)
    peak_portfolio_value: Mapped[float] = mapped_column(Float, nullable=False)
    var_95_1day: Mapped[float] = mapped_column(Float, nullable=False)
    var_99_1day: Mapped[float] = mapped_column(Float, nullable=False)
    cvar_95_1day: Mapped[float] = mapped_column(Float, nullable=False)
    portfolio_beta: Mapped[float] = mapped_column(Float, nullable=False)
    portfolio_sharpe_rolling: Mapped[float] = mapped_column(Float, nullable=False)
    sector_exposures: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    top_positions: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 3: risk_events
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class RiskEvent(FundamentalBase):
    __tablename__ = "risk_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    ticker: Mapped[str] = mapped_column(String(20), nullable=True)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    current_value: Mapped[float] = mapped_column(Float, nullable=False)
    threshold_value: Mapped[float] = mapped_column(Float, nullable=False)
    action_taken: Mapped[str] = mapped_column(String(50), nullable=False)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 4: var_calculations
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class VarCalculation(FundamentalBase):
    __tablename__ = "var_calculations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    ticker: Mapped[str] = mapped_column(String(20), nullable=True)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=True)
    calculation_method: Mapped[str] = mapped_column(String(50), nullable=False)
    confidence_level: Mapped[float] = mapped_column(Float, nullable=False)
    horizon_days: Mapped[int] = mapped_column(Integer, nullable=False)
    var_value: Mapped[float] = mapped_column(Float, nullable=False)
    cvar_value: Mapped[float] = mapped_column(Float, nullable=False)
    position_size_usd: Mapped[float] = mapped_column(Float, nullable=False)
    returns_window_days: Mapped[int] = mapped_column(Integer, nullable=False)
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 5: correlation_matrix_snapshots
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class CorrelationMatrixSnapshot(FundamentalBase):
    __tablename__ = "correlation_matrix_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tickers: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    correlation_matrix: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    avg_correlation: Mapped[float] = mapped_column(Float, nullable=False)
    max_correlation: Mapped[float] = mapped_column(Float, nullable=False)
    high_correlation_pairs: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 6: circuit_breakers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class CircuitBreaker(FundamentalBase):
    __tablename__ = "circuit_breakers"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    breaker_type: Mapped[str] = mapped_column(String(50), nullable=False)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)
    current_value: Mapped[float] = mapped_column(Float, nullable=False)
    triggered: Mapped[bool] = mapped_column(Boolean, default=False)
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    auto_reset: Mapped[bool] = mapped_column(Boolean, default=True)
    reset_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    action: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 7: approved_signals
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ApprovedSignal(FundamentalBase):
    __tablename__ = "approved_signals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    approved_position_size_pct: Mapped[float] = mapped_column(Float, nullable=False)
    approved_position_size_usd: Mapped[float] = mapped_column(Float, nullable=False)
    risk_score: Mapped[float] = mapped_column(Float, nullable=False)
    approval_reason: Mapped[str] = mapped_column(Text, nullable=False)
    rejection_reason: Mapped[str] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    conditions: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=True)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    approved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
