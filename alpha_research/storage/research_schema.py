"""
Alpha Research — Raw SQL DDL Extensions
=========================================

Post-creation SQL for the research tables that cannot be
expressed through the SQLAlchemy ORM:

* Composite indexes for common query patterns
* Partial indexes on ``status`` for active hypotheses
* Check constraints enforcing enum-like value domains

Executed *after* ``metadata.create_all()`` in
``init_research_db.py``.
"""

from __future__ import annotations


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Partial index: only active hypotheses (pending / validated)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE_ACTIVE_HYPOTHESES_INDEX = """
CREATE INDEX IF NOT EXISTS ix_hypotheses_active
ON research_hypotheses (ticker, conviction_score DESC)
WHERE status IN ('pending', 'validated');
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Composite indexes for time-series lookups
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE_SENTIMENT_LOOKUP_INDEX = """
CREATE INDEX IF NOT EXISTS ix_sentiment_ticker_scored
ON sentiment_scores (ticker, scored_at DESC);
"""

CREATE_TECHNICAL_LOOKUP_INDEX = """
CREATE INDEX IF NOT EXISTS ix_technical_ticker_detected
ON technical_signals (ticker, detected_at DESC);
"""

CREATE_FUNDAMENTAL_LOOKUP_INDEX = """
CREATE INDEX IF NOT EXISTS ix_fundamental_ticker_scored
ON fundamental_scores (ticker, scored_at DESC);
"""

CREATE_MACRO_LOOKUP_INDEX = """
CREATE INDEX IF NOT EXISTS ix_macro_signal_detected
ON macro_signals (signal_name, detected_at DESC);
"""

CREATE_RUNS_STATUS_INDEX = """
CREATE INDEX IF NOT EXISTS ix_runs_status_started
ON research_runs (status, started_at DESC);
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Check constraints for enum-like columns
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADD_HYPOTHESIS_TYPE_CHECK = """
DO $$
BEGIN
    ALTER TABLE research_hypotheses
        ADD CONSTRAINT chk_hypothesis_type
        CHECK (hypothesis_type IN (
            'fundamental', 'technical', 'sentiment', 'macro', 'composite'
        ));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;
"""

ADD_HYPOTHESIS_STATUS_CHECK = """
DO $$
BEGIN
    ALTER TABLE research_hypotheses
        ADD CONSTRAINT chk_hypothesis_status
        CHECK (status IN ('pending', 'validated', 'rejected', 'expired'));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;
"""

ADD_DIRECTION_CHECK = """
DO $$
BEGIN
    ALTER TABLE research_hypotheses
        ADD CONSTRAINT chk_expected_direction
        CHECK (expected_direction IN ('long', 'short', 'neutral'));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;
"""

ADD_TIMEFRAME_CHECK = """
DO $$
BEGIN
    ALTER TABLE research_hypotheses
        ADD CONSTRAINT chk_expected_timeframe
        CHECK (expected_timeframe IN ('intraday', 'swing', 'position'));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;
"""

ADD_SIGNAL_TYPE_CHECK = """
DO $$
BEGIN
    ALTER TABLE technical_signals
        ADD CONSTRAINT chk_signal_type
        CHECK (signal_type IN (
            'momentum', 'mean_reversion', 'breakout',
            'trend', 'volatility', 'volume'
        ));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;
"""

ADD_SEVERITY_CHECK = """
DO $$
BEGIN
    ALTER TABLE macro_signals
        ADD CONSTRAINT chk_severity
        CHECK (severity IN ('low', 'medium', 'high', 'critical'));
EXCEPTION WHEN duplicate_object THEN
    NULL;
END $$;
"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Ordered execution list (used by init_research_db.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RESEARCH_POST_CREATE_SQL: list[str] = [
    CREATE_ACTIVE_HYPOTHESES_INDEX,
    CREATE_SENTIMENT_LOOKUP_INDEX,
    CREATE_TECHNICAL_LOOKUP_INDEX,
    CREATE_FUNDAMENTAL_LOOKUP_INDEX,
    CREATE_MACRO_LOOKUP_INDEX,
    CREATE_RUNS_STATUS_INDEX,
    ADD_HYPOTHESIS_TYPE_CHECK,
    ADD_HYPOTHESIS_STATUS_CHECK,
    ADD_DIRECTION_CHECK,
    ADD_TIMEFRAME_CHECK,
    ADD_SIGNAL_TYPE_CHECK,
    ADD_SEVERITY_CHECK,
]
"""SQL to run after research tables are created via ORM."""
