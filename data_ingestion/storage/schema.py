"""
Trading System — Raw SQL DDL & TimescaleDB Extensions
======================================================

This module contains raw SQL statements that **cannot** be expressed
through SQLAlchemy's ORM layer:

* TimescaleDB ``create_hypertable`` calls
* PostgreSQL full-text-search triggers (``tsvector_update_trigger``)
* Retention policies via TimescaleDB automation

These are executed *after* ``metadata.create_all()`` in ``init_db.py``.
"""

from __future__ import annotations

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TimescaleDB — Hypertable Conversion
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ENABLE_TIMESCALEDB = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"

CREATE_HYPERTABLE_OHLCV = """
SELECT create_hypertable(
    'ohlcv_bars',
    by_range('timestamp'),
    if_not_exists => TRUE,
    migrate_data  => TRUE
);
"""

CREATE_HYPERTABLE_TICKS = """
SELECT create_hypertable(
    'raw_ticks',
    by_range('timestamp'),
    if_not_exists => TRUE,
    migrate_data  => TRUE
);
"""

# Chunk interval: 1 day for ticks (high cardinality), 7 days for bars
SET_CHUNK_INTERVAL_OHLCV = """
SELECT set_chunk_time_interval('ohlcv_bars', INTERVAL '7 days');
"""

SET_CHUNK_INTERVAL_TICKS = """
SELECT set_chunk_time_interval('raw_ticks', INTERVAL '1 day');
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TimescaleDB — Retention Policies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADD_RETENTION_OHLCV = """
SELECT add_retention_policy(
    'ohlcv_bars',
    drop_after => INTERVAL '{retention_days} days',
    if_not_exists => TRUE
);
"""

ADD_RETENTION_TICKS = """
SELECT add_retention_policy(
    'raw_ticks',
    drop_after => INTERVAL '{retention_days} days',
    if_not_exists => TRUE
);
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TimescaleDB — Compression (optional, for cost savings)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ENABLE_COMPRESSION_OHLCV = """
ALTER TABLE ohlcv_bars SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ticker, timeframe',
    timescaledb.compress_orderby = 'timestamp DESC'
);
"""

ADD_COMPRESSION_POLICY_OHLCV = """
SELECT add_compression_policy(
    'ohlcv_bars',
    compress_after => INTERVAL '30 days',
    if_not_exists => TRUE
);
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PostgreSQL — Full-Text Search Trigger (news_articles)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CREATE_NEWS_FTS_TRIGGER = """
DO $$
BEGIN
    -- Create trigger function if it doesn't exist
    CREATE OR REPLACE FUNCTION news_search_vector_update() RETURNS trigger AS $trig$
    BEGIN
        NEW.search_vector :=
            setweight(to_tsvector('english', COALESCE(NEW.headline, '')), 'A') ||
            setweight(to_tsvector('english', COALESCE(NEW.summary, '')),  'B');
        RETURN NEW;
    END;
    $trig$ LANGUAGE plpgsql;

    -- Drop and recreate trigger to ensure it's current
    DROP TRIGGER IF EXISTS trg_news_search_vector ON news_articles;

    CREATE TRIGGER trg_news_search_vector
        BEFORE INSERT OR UPDATE ON news_articles
        FOR EACH ROW
        EXECUTE FUNCTION news_search_vector_update();
END
$$;
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Ordered execution lists (used by init_db.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TIMESCALE_POST_CREATE_SQL: list[str] = [
    ENABLE_TIMESCALEDB,
    CREATE_HYPERTABLE_OHLCV,
    CREATE_HYPERTABLE_TICKS,
    SET_CHUNK_INTERVAL_OHLCV,
    SET_CHUNK_INTERVAL_TICKS,
]
"""SQL to run after TimescaleDB tables are created via ORM."""

TIMESCALE_POLICY_SQL: list[str] = [
    # Retention days are injected at runtime via .format()
    ADD_RETENTION_OHLCV,
    ADD_RETENTION_TICKS,
    ENABLE_COMPRESSION_OHLCV,
    ADD_COMPRESSION_POLICY_OHLCV,
]
"""SQL for retention & compression policies (run after hypertables exist)."""

FUNDAMENTAL_POST_CREATE_SQL: list[str] = [
    CREATE_NEWS_FTS_TRIGGER,
]
"""SQL to run after fundamentals tables are created via ORM."""
