from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID
from pydantic import BaseModel, Field

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Trading Signal Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TradingSignalBase(BaseModel):
    hypothesis_id: UUID
    ticker: str
    signal_name: str
    signal_type: str
    entry_condition: str
    exit_condition: str
    strategy_code: str
    timeframe: str
    parameters: dict[str, Any]
    status: str
    created_by: str

class TradingSignalCreate(TradingSignalBase):
    pass

class TradingSignalResponse(TradingSignalBase):
    id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Backtest Result Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class BacktestResultBase(BaseModel):
    signal_id: UUID
    ticker: str
    start_date: date
    end_date: date
    initial_capital: float = 100000.0
    final_capital: float
    total_return_pct: float
    annualized_return_pct: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    max_drawdown_pct: float
    max_drawdown_duration_days: int
    win_rate: float
    profit_factor: float
    total_trades: int
    avg_trade_return_pct: float
    avg_holding_days: float
    best_trade_pct: float
    worst_trade_pct: float
    volatility_annualized: float
    benchmark_return_pct: float
    alpha: float
    beta: float
    equity_curve: list[dict[str, Any]]
    monthly_returns: dict[str, Any]
    trade_log: list[dict[str, Any]]
    engine: str

class BacktestResultCreate(BacktestResultBase):
    pass

class BacktestResultResponse(BacktestResultBase):
    id: UUID
    backtested_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Walk Forward Result Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class WalkForwardResultBase(BaseModel):
    signal_id: UUID
    ticker: str
    n_splits: int
    train_pct: float
    in_sample_sharpe: float
    out_sample_sharpe: float
    consistency_score: float
    overfit_score: float
    passed: bool
    splits_detail: list[dict[str, Any]]

class WalkForwardResultCreate(WalkForwardResultBase):
    pass

class WalkForwardResultResponse(WalkForwardResultBase):
    id: UUID
    tested_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Signal Parameter Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class SignalParameterBase(BaseModel):
    signal_id: UUID
    parameter_name: str
    optimal_value: float
    search_range: dict[str, Any]
    optimization_method: str
    stability_score: float

class SignalParameterCreate(SignalParameterBase):
    pass

class SignalParameterResponse(SignalParameterBase):
    id: UUID
    optimized_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Signal Performance Live Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class SignalPerformanceLiveBase(BaseModel):
    signal_id: UUID
    ticker: str
    date: date
    predicted_direction: str
    actual_direction: str
    predicted_return: float
    actual_return: float
    hit: bool
    cumulative_hit_rate: float

class SignalPerformanceLiveCreate(SignalPerformanceLiveBase):
    pass

class SignalPerformanceLiveResponse(SignalPerformanceLiveBase):
    id: UUID
    recorded_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Signal Generation Run Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class SignalGenerationRunBase(BaseModel):
    hypothesis_id: UUID
    signals_generated: int = 0
    signals_backtested: int = 0
    signals_passed: int = 0
    signals_rejected: int = 0
    best_sharpe: Optional[float] = None
    best_signal_id: Optional[UUID] = None
    duration_seconds: float
    status: str
    started_at: datetime
    completed_at: datetime

class SignalGenerationRunCreate(SignalGenerationRunBase):
    pass

class SignalGenerationRunResponse(SignalGenerationRunBase):
    id: UUID

    class Config:
        from_attributes = True
