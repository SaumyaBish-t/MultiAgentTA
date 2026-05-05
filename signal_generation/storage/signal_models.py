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
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_ingestion.storage.models import FundamentalBase
import alpha_research.storage.research_models  # Ensure ResearchHypothesis metadata is loaded

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 1: trading_signals
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TradingSignal(FundamentalBase):
    __tablename__ = "trading_signals"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hypothesis_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("research_hypotheses.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    signal_name: Mapped[str] = mapped_column(String(255), nullable=False)
    signal_type: Mapped[str] = mapped_column(String(50), nullable=False, comment="momentum/mean_reversion/breakout/trend/pairs/event_driven")
    entry_condition: Mapped[str] = mapped_column(Text, nullable=False)
    exit_condition: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_code: Mapped[str] = mapped_column(Text, nullable=False)
    timeframe: Mapped[str] = mapped_column(String(20), nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, comment="draft/backtested/validated/rejected/live/retired")
    created_by: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_trading_signals_ticker_status", "ticker", "status"),
        Index("ix_trading_signals_created_at", "created_at"),
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 2: backtest_results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class BacktestResult(FundamentalBase):
    __tablename__ = "backtest_results"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    initial_capital: Mapped[float] = mapped_column(Float, default=100000.0, nullable=False)
    final_capital: Mapped[float] = mapped_column(Float, nullable=False)
    total_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    annualized_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    sharpe_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    sortino_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    calmar_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    max_drawdown_pct: Mapped[float] = mapped_column(Float, nullable=False)
    max_drawdown_duration_days: Mapped[int] = mapped_column(Integer, nullable=False)
    win_rate: Mapped[float] = mapped_column(Float, nullable=False)
    profit_factor: Mapped[float] = mapped_column(Float, nullable=False)
    total_trades: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_trade_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    avg_holding_days: Mapped[float] = mapped_column(Float, nullable=False)
    best_trade_pct: Mapped[float] = mapped_column(Float, nullable=False)
    worst_trade_pct: Mapped[float] = mapped_column(Float, nullable=False)
    volatility_annualized: Mapped[float] = mapped_column(Float, nullable=False)
    benchmark_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    alpha: Mapped[float] = mapped_column(Float, nullable=False)
    beta: Mapped[float] = mapped_column(Float, nullable=False)
    equity_curve: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    monthly_returns: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    trade_log: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    backtested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    engine: Mapped[str] = mapped_column(String(50), nullable=False)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 3: walk_forward_results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class WalkForwardResult(FundamentalBase):
    __tablename__ = "walk_forward_results"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    n_splits: Mapped[int] = mapped_column(Integer, nullable=False)
    train_pct: Mapped[float] = mapped_column(Float, nullable=False)
    in_sample_sharpe: Mapped[float] = mapped_column(Float, nullable=False)
    out_sample_sharpe: Mapped[float] = mapped_column(Float, nullable=False)
    consistency_score: Mapped[float] = mapped_column(Float, nullable=False)
    overfit_score: Mapped[float] = mapped_column(Float, nullable=False)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    splits_detail: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    tested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 4: signal_parameters
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class SignalParameter(FundamentalBase):
    __tablename__ = "signal_parameters"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=False)
    parameter_name: Mapped[str] = mapped_column(String(100), nullable=False)
    optimal_value: Mapped[float] = mapped_column(Float, nullable=False)
    search_range: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    optimization_method: Mapped[str] = mapped_column(String(50), nullable=False)
    stability_score: Mapped[float] = mapped_column(Float, nullable=False)
    optimized_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 5: signal_performance_live
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class SignalPerformanceLive(FundamentalBase):
    __tablename__ = "signal_performance_live"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    predicted_direction: Mapped[str] = mapped_column(String(10), nullable=False)
    actual_direction: Mapped[str] = mapped_column(String(10), nullable=False)
    predicted_return: Mapped[float] = mapped_column(Float, nullable=False)
    actual_return: Mapped[float] = mapped_column(Float, nullable=False)
    hit: Mapped[bool] = mapped_column(Boolean, nullable=False)
    cumulative_hit_rate: Mapped[float] = mapped_column(Float, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 6: signal_generation_runs
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class SignalGenerationRun(FundamentalBase):
    __tablename__ = "signal_generation_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hypothesis_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    signals_generated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    signals_backtested: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    signals_passed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    signals_rejected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    best_sharpe: Mapped[float] = mapped_column(Float, nullable=True)
    best_signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trading_signals.id"), nullable=True)
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_signal_runs_created_at", "started_at"),
    )
