from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Position Limit Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class PositionLimitBase(BaseModel):
    signal_id: UUID
    ticker: str
    max_position_size_pct: float
    max_position_size_usd: float
    max_shares: Optional[int] = None
    sizing_method: str
    kelly_fraction: Optional[float] = None
    volatility_scalar: Optional[float] = None
    approved: bool = False
    approved_at: Optional[datetime] = None

class PositionLimitCreate(PositionLimitBase):
    pass

class PositionLimitResponse(PositionLimitBase):
    id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Portfolio Risk Snapshot Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class PortfolioRiskSnapshotBase(BaseModel):
    snapshot_time: datetime
    total_portfolio_value: float
    cash_pct: float
    invested_pct: float
    long_exposure_pct: float
    short_exposure_pct: float
    net_exposure_pct: float
    gross_exposure_pct: float
    current_drawdown_pct: float
    peak_portfolio_value: float
    var_95_1day: float
    var_99_1day: float
    cvar_95_1day: float
    portfolio_beta: float
    portfolio_sharpe_rolling: float
    sector_exposures: dict[str, Any]
    top_positions: list[dict[str, Any]]

class PortfolioRiskSnapshotCreate(PortfolioRiskSnapshotBase):
    pass

class PortfolioRiskSnapshotResponse(PortfolioRiskSnapshotBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Risk Event Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class RiskEventBase(BaseModel):
    event_type: str
    severity: str
    ticker: Optional[str] = None
    signal_id: Optional[UUID] = None
    description: str
    current_value: float
    threshold_value: float
    action_taken: str
    resolved: bool = False
    resolved_at: Optional[datetime] = None

class RiskEventCreate(RiskEventBase):
    pass

class RiskEventResponse(RiskEventBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# VaR Calculation Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class VarCalculationBase(BaseModel):
    ticker: Optional[str] = None
    signal_id: Optional[UUID] = None
    calculation_method: str
    confidence_level: float
    horizon_days: int
    var_value: float
    cvar_value: float
    position_size_usd: float
    returns_window_days: int

class VarCalculationCreate(VarCalculationBase):
    pass

class VarCalculationResponse(VarCalculationBase):
    id: UUID
    calculated_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Correlation Matrix Snapshot Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class CorrelationMatrixSnapshotBase(BaseModel):
    tickers: list[str]
    correlation_matrix: dict[str, Any]
    avg_correlation: float
    max_correlation: float
    high_correlation_pairs: list[dict[str, Any]]
    snapshot_date: date

class CorrelationMatrixSnapshotCreate(CorrelationMatrixSnapshotBase):
    pass

class CorrelationMatrixSnapshotResponse(CorrelationMatrixSnapshotBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Circuit Breaker Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class CircuitBreakerBase(BaseModel):
    breaker_type: str
    threshold: float
    current_value: float
    triggered: bool = False
    triggered_at: Optional[datetime] = None
    auto_reset: bool = True
    reset_at: Optional[datetime] = None
    action: str

class CircuitBreakerCreate(CircuitBreakerBase):
    pass

class CircuitBreakerResponse(CircuitBreakerBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Approved Signal Schemas
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class ApprovedSignalBase(BaseModel):
    signal_id: UUID
    ticker: str
    approved_position_size_pct: float
    approved_position_size_usd: float
    risk_score: float
    approval_reason: str
    rejection_reason: Optional[str] = None
    status: str
    conditions: Optional[dict[str, Any]] = None
    valid_until: datetime
    approved_at: datetime

class ApprovedSignalCreate(ApprovedSignalBase):
    pass

class ApprovedSignalResponse(ApprovedSignalBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True
