from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


# ── Prices ────────────────────────────────────────────────────────
class PriceBar(BaseModel):
    ticker: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None
    timeframe: str
    is_adjusted: bool

    model_config = ConfigDict(from_attributes=True)


class BatchPriceRequest(BaseModel):
    tickers: List[str]
    timeframe: str
    start: Optional[date] = None
    end: Optional[date] = None


# ── Fundamentals ──────────────────────────────────────────────────
class IncomeStatementResponse(BaseModel):
    ticker: str
    fiscal_date: datetime
    period_type: str
    revenue: Optional[float]
    gross_profit: Optional[float]
    operating_income: Optional[float]
    net_income: Optional[float]
    ebitda: Optional[float]

    model_config = ConfigDict(from_attributes=True)


class BalanceSheetResponse(BaseModel):
    ticker: str
    fiscal_date: datetime
    period_type: str
    total_assets: Optional[float]
    total_liabilities: Optional[float]
    total_debt: Optional[float]
    cash: Optional[float]
    equity: Optional[float]

    model_config = ConfigDict(from_attributes=True)


class FundamentalSummaryResponse(BaseModel):
    ticker: str
    pe_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None


# ── News ──────────────────────────────────────────────────────────
class NewsArticleResponse(BaseModel):
    tickers: Optional[List[str]] = None
    headline: str
    summary: Optional[str] = None
    source: str
    url: Optional[str] = None
    published_at: datetime
    sentiment_score: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class NewsSearchRequest(BaseModel):
    query: str
    tickers: Optional[List[str]] = None
    limit: int = Field(default=10, ge=1, le=50)


class NewsSearchHit(BaseModel):
    headline: str
    url: str
    source: str
    published_at: str
    similarity: float


class NewsSearchResponse(BaseModel):
    query: str
    results: List[NewsSearchHit]


# ── Macro ─────────────────────────────────────────────────────────
class MacroObservation(BaseModel):
    series_id: str
    observation_date: datetime
    value: float
    source: str

    model_config = ConfigDict(from_attributes=True)


# ── Health ────────────────────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str
    databases: Dict[str, bool]
    cache: Dict[str, Any]
    last_updates: Dict[str, Optional[datetime]]
