"""
Alpha Research — SQLAlchemy ORM Models (Phase 2)
==================================================

Six tables added to the PostgreSQL ``fundamentals`` database:

* **ResearchHypothesis** — agent-generated trading hypotheses
* **SentimentScore**     — aggregated sentiment per ticker
* **TechnicalSignal**    — indicator-based signals
* **FundamentalScore**   — multi-factor fundamental scores
* **MacroSignal**        — macro-economic regime signals
* **ResearchRun**        — audit trail for research runs

All models inherit from ``FundamentalBase`` (defined in
``data_ingestion.storage.models``) so they share the same
metadata registry and can be created alongside Phase 1
tables with a single ``metadata.create_all()`` call.

All primary keys are UUID v4 for distributed-safe generation.
All timestamps are ``TIMESTAMP WITH TIME ZONE`` defaulting to UTC.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    Float,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

# Reuse the same base so these tables live alongside Phase 1
from data_ingestion.storage.models import FundamentalBase


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TABLE 1: research_hypotheses
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ResearchHypothesis(FundamentalBase):
    """
    Agent-generated trading hypothesis.

    Each hypothesis captures a directional view on a ticker, the
    signals that support or contradict it, and a conviction score
    assigned by the generating agent.  Hypotheses expire after a
    configurable validity window.
    """

    __tablename__ = "research_hypotheses"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
        comment="Unique hypothesis identifier (UUID v4)",
    )
    ticker: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True,
        comment="Equity / ETF symbol, e.g. AAPL",
    )
    hypothesis_type: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="fundamental | technical | sentiment | macro | composite",
    )
    title: Mapped[str] = mapped_column(
        String(300), nullable=False,
        comment="Short human-readable description",
    )
    description: Mapped[str] = mapped_column(
        Text, nullable=False,
        comment="Full hypothesis narrative",
    )
    conviction_score: Mapped[float] = mapped_column(
        Numeric(3, 2), nullable=False,
        comment="Agent confidence 0.00 → 1.00",
    )
    expected_direction: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="long | short | neutral",
    )
    expected_timeframe: Mapped[str] = mapped_column(
        String(15), nullable=False,
        comment="intraday | swing | position",
    )
    supporting_signals: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="List of signals supporting the hypothesis",
    )
    contradicting_signals: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="List of signals contradicting the hypothesis",
    )
    data_sources_used: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="Data sources that informed this hypothesis",
    )
    status: Mapped[str] = mapped_column(
        String(15), nullable=False, server_default="pending",
        comment="pending | validated | rejected | expired",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(), onupdate=func.now(),
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Hypothesis validity window end",
    )
    created_by_agent: Mapped[str] = mapped_column(
        String(50), nullable=False,
        comment="Agent that generated this hypothesis",
    )

    __table_args__ = (
        Index("ix_hyp_ticker_status", "ticker", "status"),
        Index("ix_hyp_type_created", "hypothesis_type", "created_at"),
        {"comment": "Agent-generated trading hypotheses"},
    )

    def __repr__(self) -> str:
        return (
            f"<ResearchHypothesis {self.ticker} {self.hypothesis_type} "
            f"conv={self.conviction_score} [{self.status}]>"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TABLE 2: sentiment_scores
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SentimentScore(FundamentalBase):
    """
    Aggregated sentiment measurement for a ticker from a single source.

    Scores are computed over a rolling time window and may come from
    news articles, Reddit posts, Twitter / X, or earnings-call
    transcripts.
    """

    __tablename__ = "sentiment_scores"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    ticker: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True,
    )
    source: Mapped[str] = mapped_column(
        String(30), nullable=False,
        comment="news | reddit | twitter | earnings_call",
    )
    score: Mapped[float] = mapped_column(
        Numeric(4, 3), nullable=False,
        comment="Sentiment -1.000 (bearish) → +1.000 (bullish)",
    )
    magnitude: Mapped[float] = mapped_column(
        Numeric(4, 3), nullable=False,
        comment="Signal strength 0.000 → 1.000",
    )
    sample_count: Mapped[int] = mapped_column(
        Integer, nullable=False,
        comment="Number of items scored in this window",
    )
    time_window_hours: Mapped[int] = mapped_column(
        Integer, nullable=False,
        comment="Lookback window in hours",
    )
    raw_scores: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="Per-source breakdown of scores",
    )
    scored_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        comment="When the score was computed",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_sent_ticker_source", "ticker", "source"),
        {"comment": "Aggregated sentiment measurements per ticker"},
    )

    def __repr__(self) -> str:
        return (
            f"<SentimentScore {self.ticker} {self.source} "
            f"score={self.score} mag={self.magnitude}>"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TABLE 3: technical_signals
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TechnicalSignal(FundamentalBase):
    """
    Indicator-based technical signal detected on a price series.

    Each row captures the signal type (momentum, breakout, etc.),
    the specific indicator that fired, direction, and the market
    context at detection time.
    """

    __tablename__ = "technical_signals"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    ticker: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True,
    )
    timeframe: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="Bar resolution: 1min, 5min, 1h, 1d, etc.",
    )
    signal_type: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="momentum | mean_reversion | breakout | trend | volatility | volume",
    )
    indicator_name: Mapped[str] = mapped_column(
        String(30), nullable=False,
        comment="RSI, MACD, BB, EMA_cross, etc.",
    )
    signal_value: Mapped[float] = mapped_column(
        Float, nullable=False,
        comment="Raw indicator reading at detection",
    )
    signal_direction: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="bullish | bearish | neutral",
    )
    strength: Mapped[float] = mapped_column(
        Numeric(3, 2), nullable=False,
        comment="Signal strength 0.00 → 1.00",
    )
    price_at_signal: Mapped[float] = mapped_column(
        Numeric(14, 4), nullable=False,
        comment="Close price when signal fired",
    )
    volume_at_signal: Mapped[float] = mapped_column(
        Float, nullable=False,
        comment="Volume when signal fired",
    )
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        comment="When the signal was detected",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_tech_ticker_type", "ticker", "signal_type"),
        Index("ix_tech_indicator", "indicator_name", "detected_at"),
        {"comment": "Indicator-based technical signals"},
    )

    def __repr__(self) -> str:
        return (
            f"<TechnicalSignal {self.ticker} {self.indicator_name} "
            f"{self.signal_direction} str={self.strength}>"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TABLE 4: fundamental_scores
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FundamentalScore(FundamentalBase):
    """
    Multi-factor fundamental score for a ticker.

    Each row captures a composite quality / value / growth score
    along with its sub-component breakdowns.  Scores are recomputed
    after every new filing ingestion.
    """

    __tablename__ = "fundamental_scores"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    ticker: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True,
    )
    score_type: Mapped[str] = mapped_column(
        String(15), nullable=False,
        comment="value | growth | quality | momentum",
    )
    overall_score: Mapped[float] = mapped_column(
        Numeric(3, 2), nullable=False,
        comment="Composite score 0.00 → 1.00",
    )
    pe_score: Mapped[float | None] = mapped_column(
        Numeric(3, 2), nullable=True,
        comment="P/E relative score",
    )
    growth_score: Mapped[float | None] = mapped_column(
        Numeric(3, 2), nullable=True,
        comment="Revenue / earnings growth score",
    )
    margin_score: Mapped[float | None] = mapped_column(
        Numeric(3, 2), nullable=True,
        comment="Profit margin quality score",
    )
    debt_score: Mapped[float | None] = mapped_column(
        Numeric(3, 2), nullable=True,
        comment="Debt / leverage health score",
    )
    roe_score: Mapped[float | None] = mapped_column(
        Numeric(3, 2), nullable=True,
        comment="Return on equity score",
    )
    details: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="Full breakdown of all sub-scores",
    )
    fiscal_period: Mapped[str | None] = mapped_column(
        String(20), nullable=True,
        comment="Fiscal period this score covers, e.g. Q3-2025",
    )
    scored_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        comment="When the score was computed",
    )

    __table_args__ = (
        Index("ix_fund_ticker_type", "ticker", "score_type"),
        {"comment": "Multi-factor fundamental scores per ticker"},
    )

    def __repr__(self) -> str:
        return (
            f"<FundamentalScore {self.ticker} {self.score_type} "
            f"overall={self.overall_score}>"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TABLE 5: macro_signals
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MacroSignal(FundamentalBase):
    """
    Macro-economic regime signal.

    Detects structural shifts like yield-curve inversions, Fed pivots,
    or inflation regime changes and maps them to affected sectors and
    tickers.
    """

    __tablename__ = "macro_signals"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    signal_name: Mapped[str] = mapped_column(
        String(60), nullable=False, index=True,
        comment="yield_curve_inversion | fed_pivot | inflation_regime etc.",
    )
    signal_value: Mapped[float] = mapped_column(
        Float, nullable=False,
        comment="Quantified signal reading",
    )
    signal_direction: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="bullish | bearish | neutral",
    )
    affected_sectors: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="List of sectors affected by this signal",
    )
    affected_tickers: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="Specific tickers affected",
    )
    severity: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="low | medium | high | critical",
    )
    description: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        comment="Narrative explanation of the signal",
    )
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        comment="When the signal was detected",
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="When the signal is expected to lose relevance",
    )

    __table_args__ = (
        Index("ix_macro_severity", "severity", "detected_at"),
        {"comment": "Macro-economic regime signals"},
    )

    def __repr__(self) -> str:
        return (
            f"<MacroSignal {self.signal_name} "
            f"{self.signal_direction} [{self.severity}]>"
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TABLE 6: research_runs
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ResearchRun(FundamentalBase):
    """
    Audit record for a single research pipeline execution.

    Tracks which tickers were analysed, which agents participated,
    how many hypotheses were generated vs rejected, and the overall
    run duration and status.
    """

    __tablename__ = "research_runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4,
    )
    run_type: Mapped[str] = mapped_column(
        String(15), nullable=False,
        comment="scheduled | triggered | manual",
    )
    tickers_analyzed: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="List of tickers included in this run",
    )
    agents_used: Mapped[dict | None] = mapped_column(
        JSON, nullable=True,
        comment="List of agent names that participated",
    )
    hypotheses_generated: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0",
    )
    hypotheses_rejected: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0",
    )
    duration_seconds: Mapped[float | None] = mapped_column(
        Float, nullable=True,
        comment="Total wall-clock time in seconds",
    )
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, server_default="running",
        comment="running | completed | failed | completed_with_errors",
    )
    error_message: Mapped[str | None] = mapped_column(
        Text, nullable=True,
        comment="Error details if status == failed",
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    __table_args__ = (
        Index("ix_runs_type_started", "run_type", "started_at"),
        {"comment": "Audit trail for research pipeline executions"},
    )

    def __repr__(self) -> str:
        return (
            f"<ResearchRun {self.run_type} [{self.status}] "
            f"hyp={self.hypotheses_generated}>"
        )
