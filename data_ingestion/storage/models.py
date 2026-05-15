"""
Trading System — SQLAlchemy ORM Models
=======================================

Two separate databases, two separate ``DeclarativeBase`` hierarchies:

* **TimescaleBase** → ``market_data`` (TimescaleDB)
    - ``OHLCVBar``   — aggregated price bars at multiple timeframes
    - ``RawTick``    — individual trade ticks

* **FundamentalBase** → ``fundamentals`` (PostgreSQL)
    - ``Company``          — static reference / profile data
    - ``IncomeStatement``  — quarterly / annual income statements
    - ``BalanceSheet``     — quarterly / annual balance sheets
    - ``NewsArticle``      — headlines with sentiment scores
    - ``MacroSeries``      — FRED economic indicator observations

All timestamps are stored as ``TIMESTAMP WITH TIME ZONE`` and
defaulted to UTC via ``func.now()``.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Base Classes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TimescaleBase(DeclarativeBase):
    """Base for all TimescaleDB (market_data) models."""
    pass


class FundamentalBase(DeclarativeBase):
    """Base for all PostgreSQL (fundamentals) models."""
    pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TimescaleDB — market_data
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class OhlcvBar(TimescaleBase):
    """
    Aggregated OHLCV price bars.

    The combination of (ticker, timestamp, timeframe) is unique — this
    prevents duplicate bars from overlapping collector runs.  The table
    is converted to a TimescaleDB hypertable partitioned on ``timestamp``
    during database initialisation.
    """

    __tablename__ = "ohlcv_bars"

    ticker: Mapped[str] = mapped_column(
        String(20), primary_key=True, nullable=False, index=True,
        comment="Equity / ETF symbol, e.g. AAPL",
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, nullable=False, index=True,
        comment="Bar open time in UTC",
    )
    open: Mapped[float] = mapped_column(
        Numeric(14, 4), nullable=False, comment="Open price",
    )
    high: Mapped[float] = mapped_column(
        Numeric(14, 4), nullable=False, comment="High price",
    )
    low: Mapped[float] = mapped_column(
        Numeric(14, 4), nullable=False, comment="Low price",
    )
    close: Mapped[float] = mapped_column(
        Numeric(14, 4), nullable=False, comment="Close price",
    )
    volume: Mapped[int] = mapped_column(
        BigInteger, nullable=False, comment="Trade volume",
    )
    vwap: Mapped[float | None] = mapped_column(
        Numeric(14, 4), nullable=True,
        comment="Volume-weighted average price",
    )
    transactions: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True,
        comment="Number of transactions in the bar",
    )
    timeframe: Mapped[str] = mapped_column(
        String(20), primary_key=True, nullable=False, index=True,
        comment="Bar resolution: 1min, 5min, 15min, 1h, 4h, 1d, 1w",
    )
    source: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="polygon",
        comment="Data provider that supplied this bar",
    )
    is_adjusted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false",
        comment="Whether prices are split/dividend adjusted",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
        comment="Row insertion timestamp (UTC)",
    )

    __table_args__ = (
        Index("ix_ohlcv_ticker_ts", "ticker", "timestamp"),
        Index("ix_ohlcv_tf_ts", "timeframe", "timestamp"),
        {"comment": "Aggregated OHLCV bars — hypertable on timestamp"},
    )

    def __repr__(self) -> str:
        return (
            f"<OhlcvBar {self.ticker} {self.timeframe} "
            f"{self.timestamp:%Y-%m-%d %H:%M} "
            f"O={self.open} H={self.high} L={self.low} C={self.close}>"
        )


class RawTick(TimescaleBase):
    """
    Individual trade ticks.

    Stored for short-term analysis (configurable retention) and later
    aggregated into ``OHLCVBar`` records by the normalisation pipeline.
    """

    __tablename__ = "raw_ticks"

    ticker: Mapped[str] = mapped_column(
        String(20), primary_key=True, nullable=False, index=True,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, nullable=False, index=True,
        comment="Trade execution time in UTC",
    )
    price: Mapped[float] = mapped_column(
        Numeric(14, 4), nullable=False,
    )
    size: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="Trade size (shares)",
    )
    conditions: Mapped[str | None] = mapped_column(
        String(50), nullable=True,
        comment="Trade condition codes (SIP/exchange specific)",
    )
    exchange: Mapped[str | None] = mapped_column(
        String(20), nullable=True,
        comment="Exchange or venue code",
    )

    __table_args__ = (
        Index("ix_tick_ticker_ts", "ticker", "timestamp"),
        {"comment": "Raw tick-level trade data — hypertable on timestamp"},
    )

    def __repr__(self) -> str:
        return (
            f"<RawTick {self.ticker} {self.timestamp:%H:%M:%S.%f} "
            f"px={self.price} sz={self.size}>"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PostgreSQL — fundamentals
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class Company(FundamentalBase):
    """Static company profile / reference data."""

    __tablename__ = "companies"

    ticker: Mapped[str] = mapped_column(
        String(20), primary_key=True,
        comment="Primary equity symbol",
    )
    name: Mapped[str] = mapped_column(
        String(200), nullable=False,
    )
    sector: Mapped[str | None] = mapped_column(
        String(100), nullable=True,
    )
    industry: Mapped[str | None] = mapped_column(
        String(100), nullable=True,
    )
    exchange: Mapped[str | None] = mapped_column(
        String(20), nullable=True,
        comment="Primary listing exchange, e.g. NASDAQ",
    )
    market_cap: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True,
    )
    shares_outstanding: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True,
    )
    currency: Mapped[str] = mapped_column(
        String(3), nullable=False, server_default="USD",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        return f"<Company {self.ticker} — {self.name}>"


class IncomeStatement(FundamentalBase):
    """Quarterly or annual income statement snapshots."""

    __tablename__ = "income_statements"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True,
    )
    ticker: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True,
    )
    period_type: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="'annual' or 'quarterly'",
    )
    fiscal_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        comment="End of the fiscal period",
    )
    revenue: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    gross_profit: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    operating_income: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    net_income: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    eps: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    ebitda: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="fmp",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "ticker", "fiscal_date", "period_type",
            name="uq_income_ticker_date_period",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<IncomeStatement {self.ticker} {self.period_type} "
            f"{self.fiscal_date:%Y-%m-%d}>"
        )


class BalanceSheet(FundamentalBase):
    """Quarterly or annual balance sheet snapshots."""

    __tablename__ = "balance_sheets"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True,
    )
    ticker: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True,
    )
    fiscal_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )
    period_type: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="'annual' or 'quarterly'",
    )
    total_assets: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    total_liabilities: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    equity: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    cash: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    total_debt: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="fmp",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "ticker", "fiscal_date", "period_type",
            name="uq_balance_ticker_date_period",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<BalanceSheet {self.ticker} {self.period_type} "
            f"{self.fiscal_date:%Y-%m-%d}>"
        )


class NewsArticle(FundamentalBase):
    """
    News articles with associated tickers and sentiment score.

    A generated ``tsvector`` column (``search_vector``) enables fast
    full-text search over headline + summary via a GIN index.
    """

    __tablename__ = "news_articles"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True,
    )
    tickers: Mapped[list[str] | None] = mapped_column(
        ARRAY(String(20)), nullable=True,
        comment="Related equity symbols",
    )
    headline: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(
        String(50), nullable=False,
    )
    url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True,
    )
    sentiment_score: Mapped[float | None] = mapped_column(
        Float, nullable=True,
        comment="Sentiment score: -1.0 (bearish) → +1.0 (bullish)",
    )
    # Auto-maintained tsvector for full-text search — populated by trigger
    search_vector: Mapped[str | None] = mapped_column(
        TSVECTOR, nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index(
            "ix_news_search_vector",
            "search_vector",
            postgresql_using="gin",
        ),
        Index("ix_news_tickers", "tickers", postgresql_using="gin"),
        UniqueConstraint("url", name="uq_news_url"),
    )

    def __repr__(self) -> str:
        return f"<NewsArticle {self.published_at:%Y-%m-%d} — {self.headline[:60]}>"


class MacroSeries(FundamentalBase):
    """FRED economic indicator observations."""

    __tablename__ = "macro_series"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True,
    )
    series_id: Mapped[str] = mapped_column(
        String(30), nullable=False, index=True,
        comment="FRED series identifier, e.g. UNRATE, GDP",
    )
    series_name: Mapped[str | None] = mapped_column(
        String(200), nullable=True,
        comment="Human-readable name of the series",
    )
    value: Mapped[float] = mapped_column(
        Numeric(18, 6), nullable=False,
    )
    observation_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )
    source: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="fred",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint(
            "series_id", "observation_date",
            name="uq_macro_series_date",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<MacroSeries {self.series_id} "
            f"{self.observation_date:%Y-%m-%d} = {self.value}>"
        )


class DataAnomaly(FundamentalBase):
    """
    Anomalies detected during the data cleaning phase.
    """

    __tablename__ = "data_anomalies"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True,
    )
    ticker: Mapped[str | None] = mapped_column(
        String(20), nullable=True, index=True,
        comment="Ticker associated with the anomaly (if applicable)",
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True,
        comment="When the anomaly occurred or was detected",
    )
    anomaly_type: Mapped[str] = mapped_column(
        String(50), nullable=False,
    )
    value: Mapped[float | None] = mapped_column(
        Numeric(18, 6), nullable=True,
        comment="The anomalous value",
    )
    expected_range: Mapped[str | None] = mapped_column(
        String(200), nullable=True,
        comment="Description of what was expected",
    )
    source: Mapped[str] = mapped_column(
        String(50), nullable=False,
        comment="Data source where the anomaly originated",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    def __repr__(self) -> str:
        return f"<DataAnomaly {self.anomaly_type} for {self.ticker}>"


class DataQualityReport(FundamentalBase):
    """
    Summary metrics for a data cleaning run.
    """

    __tablename__ = "data_quality_reports"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True,
    )
    source: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True,
    )
    run_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    records_received: Mapped[int] = mapped_column(
        BigInteger, nullable=False,
    )
    records_passed: Mapped[int] = mapped_column(
        BigInteger, nullable=False,
    )
    records_failed: Mapped[int] = mapped_column(
        BigInteger, nullable=False,
    )
    failure_rate: Mapped[float] = mapped_column(
        Numeric(5, 4), nullable=False,
    )
    anomalies_detected: Mapped[int] = mapped_column(
        BigInteger, nullable=False,
    )
    specific_failures: Mapped[list[str] | None] = mapped_column(
        ARRAY(String), nullable=True,
        comment="List of specific failure reasons or IDs",
    )

    def __repr__(self) -> str:
        return (
            f"<DataQualityReport {self.source} - rate: "
            f"{self.failure_rate:.2%} failures>"
        )

