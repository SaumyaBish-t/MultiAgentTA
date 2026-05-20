"""
review_gate.models
==================

Pydantic models for the review gate. These map 1:1 onto the
``trade_reviews`` table created by ``core.safe_migrations``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


Recommendation = Literal["APPROVE", "REDUCE_SIZE", "REJECT"]
HumanDecision = Literal["pending", "approved", "reduced", "rejected", "expired"]


class PriceCheckResult(BaseModel):
    price_at_signal: float
    current_price: float
    pct_move: float
    still_valid: bool


class NewsCheckResult(BaseModel):
    has_earnings_within_48h: bool = False
    has_fed_event_within_48h: bool = False
    upcoming_events: list[str] = Field(default_factory=list)


class OptionsCheckResult(BaseModel):
    put_call_ratio: Optional[float] = None
    sentiment: Optional[str] = None
    aligned_with_signal: Optional[bool] = None


class MemoryResult(BaseModel):
    similar_setups_found: int = 0
    historical_win_rate: Optional[float] = None
    notes: list[str] = Field(default_factory=list)


class ReviewRecommendation(BaseModel):
    recommendation: Recommendation
    score: int
    headline: str
    key_concern: str = ""
    key_support: str = ""
    confidence: float = 0.0


class ReviewDecision(BaseModel):
    review_id: str
    decision: HumanDecision
    notes: str = ""
    final_position_usd: Optional[float] = None
    decided_at: datetime = Field(default_factory=datetime.utcnow)


class TradeReviewRecord(BaseModel):
    """Full review row — mirrors trade_reviews table."""

    review_id: str
    signal_id: Optional[str] = None
    ticker: str
    direction: str
    strategy_type: str
    recommendation: Recommendation
    recommendation_confidence: float
    headline: str
    key_concern: str = ""
    key_support: str = ""
    price_check: PriceCheckResult
    news_check: NewsCheckResult
    options_check: OptionsCheckResult
    memory_check: MemoryResult
    proposed_position_usd: float
    signal_valid_hours: float
    vault_note_path: str = ""
    thread_id: str = ""
    status: str = "pending"
