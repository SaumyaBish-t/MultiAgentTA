"""
Alpha Research — Storage Layer
===============================

SQLAlchemy ORM models and database initialisation for the
Research & Alpha Discovery pipeline (Phase 2).

All tables live in the PostgreSQL ``fundamentals`` database
(port 5434) alongside the Phase 1 reference data.
"""

from alpha_research.storage.research_models import (
    FundamentalScore,
    MacroSignal,
    ResearchHypothesis,
    ResearchRun,
    SentimentScore,
    TechnicalSignal,
)

__all__ = [
    "ResearchHypothesis",
    "SentimentScore",
    "TechnicalSignal",
    "FundamentalScore",
    "MacroSignal",
    "ResearchRun",
]
