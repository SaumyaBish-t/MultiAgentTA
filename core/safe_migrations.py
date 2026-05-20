"""
core.safe_migrations
====================

Idempotent, *additive-only* migrations for the 14-layer upgrade.

Rules
-----
* Only ``CREATE TABLE IF NOT EXISTS`` and ``ALTER TABLE ... ADD COLUMN
  IF NOT EXISTS``.  Never DROP, RENAME, or ALTER COLUMN TYPE on an
  existing table.
* Re-running this script must be a no-op once tables exist.
* Connects to the **PostgreSQL fundamentals DB** (port 5434) — the same
  database all existing scripts target.  TimescaleDB (5435) is untouched.

Invocation
----------
$ python -m core.safe_migrations
        # or
$ python scripts/run_safe_migrations.py
"""

from __future__ import annotations

import sys
from typing import Iterable

import psycopg2
from loguru import logger

from config.settings import settings


# Each entry is (table_name, CREATE statement).  Statements are taken
# verbatim from section 6 of FORGE_PROJECT_CONTEXT.md.
NEW_TABLES: list[tuple[str, str]] = [
    (
        "stock_profiles",
        """
        CREATE TABLE IF NOT EXISTS stock_profiles (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            ticker VARCHAR(20) NOT NULL UNIQUE,
            hurst_exponent FLOAT,
            hurst_class VARCHAR(20),
            preferred_strategy VARCHAR(50),
            avg_daily_volume FLOAT,
            beta FLOAT,
            avg_spread_bps FLOAT,
            last_computed_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
    ),
    (
        "computed_features",
        """
        CREATE TABLE IF NOT EXISTS computed_features (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            ticker VARCHAR(20) NOT NULL,
            feature_date DATE NOT NULL,
            hurst_exponent FLOAT,
            market_breadth FLOAT,
            sector_rotation_score FLOAT,
            liquidity_score FLOAT,
            realized_vol FLOAT,
            put_call_ratio FLOAT,
            insider_signal FLOAT,
            etf_flow_direction FLOAT,
            computed_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(ticker, feature_date)
        );
        """,
    ),
    (
        "insider_transactions",
        """
        CREATE TABLE IF NOT EXISTS insider_transactions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            ticker VARCHAR(20) NOT NULL,
            insider_name VARCHAR(200),
            insider_title VARCHAR(200),
            transaction_type VARCHAR(10),
            shares INT,
            price_per_share FLOAT,
            total_value FLOAT,
            transaction_date DATE,
            filed_at DATE,
            signal_strength FLOAT,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(ticker, insider_name, transaction_date)
        );
        """,
    ),
    (
        "trade_reviews",
        """
        CREATE TABLE IF NOT EXISTS trade_reviews (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            review_id VARCHAR(20) NOT NULL UNIQUE,
            signal_id UUID,
            ticker VARCHAR(20) NOT NULL,
            direction VARCHAR(10),
            strategy_type VARCHAR(50),
            recommendation VARCHAR(20),
            recommendation_confidence FLOAT,
            headline TEXT,
            key_concern TEXT,
            key_support TEXT,
            price_check_json JSONB,
            news_check_json JSONB,
            options_check_json JSONB,
            memory_check_json JSONB,
            human_decision VARCHAR(20) DEFAULT 'pending',
            human_notes TEXT,
            proposed_position_usd FLOAT,
            final_position_usd FLOAT,
            signal_valid_hours FLOAT,
            vault_note_path TEXT,
            thread_id VARCHAR(100),
            status VARCHAR(20) DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            decided_at TIMESTAMPTZ
        );
        """,
    ),
    (
        "agent_calibration",
        """
        CREATE TABLE IF NOT EXISTS agent_calibration (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            agent_name VARCHAR(50) NOT NULL,
            measurement_date DATE NOT NULL,
            confidence_bucket FLOAT,
            actual_accuracy FLOAT,
            sample_count INT,
            regime VARCHAR(30),
            UNIQUE(agent_name, measurement_date, confidence_bucket)
        );
        """,
    ),
    (
        "human_override_tracking",
        """
        CREATE TABLE IF NOT EXISTS human_override_tracking (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            review_id VARCHAR(20),
            ticker VARCHAR(20),
            ai_recommendation VARCHAR(20),
            human_decision VARCHAR(20),
            agreed BOOL,
            outcome_pct FLOAT,
            outcome_recorded_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
    ),
    (
        "llm_usage_log",
        """
        CREATE TABLE IF NOT EXISTS llm_usage_log (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            provider VARCHAR(50) NOT NULL,
            model VARCHAR(100),
            call_type VARCHAR(50),
            input_tokens INT DEFAULT 0,
            output_tokens INT DEFAULT 0,
            estimated_cost_usd FLOAT DEFAULT 0,
            agent_name VARCHAR(100),
            ticker VARCHAR(20),
            success BOOL DEFAULT TRUE,
            latency_ms INT,
            called_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
    ),
    (
        "strategy_experiments",
        """
        CREATE TABLE IF NOT EXISTS strategy_experiments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            signal_id UUID,
            ticker VARCHAR(20),
            mlflow_run_id VARCHAR(100),
            mlflow_experiment_id VARCHAR(100),
            strategy_type VARCHAR(50),
            params JSONB,
            sharpe FLOAT,
            total_return FLOAT,
            max_drawdown FLOAT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """,
    ),
]


def _ensure_pgcrypto(cur) -> None:
    """gen_random_uuid() requires the pgcrypto extension."""
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")


def _run(cur, statements: Iterable[tuple[str, str]]) -> list[str]:
    created: list[str] = []
    for name, sql in statements:
        cur.execute(sql)
        created.append(name)
        logger.info("✓ ensured table: {}", name)
    return created


def run_migrations(dsn: str | None = None) -> list[str]:
    """Apply all new-layer table migrations.

    Parameters
    ----------
    dsn : optional connection string; defaults to ``settings.postgres_url``.

    Returns
    -------
    list[str]
        Names of tables that were ensured (created or already existing).
    """
    target_dsn = dsn or settings.postgres_url
    logger.info("Applying safe migrations against {}", target_dsn)

    conn = psycopg2.connect(target_dsn, connect_timeout=5)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            _ensure_pgcrypto(cur)
            created = _run(cur, NEW_TABLES)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.success("Safe migrations complete ({} tables)", len(created))
    return created


def main() -> int:
    try:
        run_migrations()
    except Exception as exc:
        logger.error("Safe migrations FAILED: {}", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
