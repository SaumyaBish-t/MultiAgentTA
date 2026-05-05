"""
Alpha Research — Database Initialisation
==========================================

Idempotent bootstrap for the Phase 2 research tables.

Creates all six research tables in the PostgreSQL ``fundamentals``
database (port 5434) and applies post-creation DDL (indexes,
check constraints).  Inserts a verification ``ResearchRun`` record
to confirm write access.

Usage
-----
::

    # From the trading-system root:
    python -m alpha_research.storage.init_research_db

    # Or import and call programmatically:
    from alpha_research.storage.init_research_db import init_research_tables
    init_research_tables()
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from config.settings import settings

# ── Import models so SQLAlchemy registers them on FundamentalBase ──
from data_ingestion.storage.models import FundamentalBase

# These imports trigger model registration on FundamentalBase.metadata
from alpha_research.storage.research_models import (  # noqa: F401
    FundamentalScore,
    MacroSignal,
    ResearchHypothesis,
    ResearchRun,
    SentimentScore,
    TechnicalSignal,
)
from alpha_research.storage.research_schema import RESEARCH_POST_CREATE_SQL


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _create_engine() -> Engine:
    """Create a SQLAlchemy engine for the fundamentals database."""
    return create_engine(
        settings.postgres_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        echo=False,
        future=True,
    )


def _exec_sql_list(engine: Engine, statements: list[str], label: str) -> None:
    """Execute a list of raw SQL statements inside a single transaction."""
    with engine.begin() as conn:
        for stmt in statements:
            try:
                conn.execute(text(stmt))
            except Exception as exc:
                logger.warning(
                    "{} — statement failed (may be idempotent): {}",
                    label, exc,
                )


def _insert_verification_record(engine: Engine) -> None:
    """Insert a test ResearchRun record to verify write access."""
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    session: Session = session_factory()

    try:
        now = datetime.now(tz=timezone.utc)
        test_run = ResearchRun(
            id=uuid.uuid4(),
            run_type="manual",
            tickers_analyzed=["AAPL"],
            agents_used=["init_verification"],
            hypotheses_generated=0,
            hypotheses_rejected=0,
            duration_seconds=0.0,
            status="completed",
            error_message=None,
            started_at=now,
            completed_at=now,
        )
        session.add(test_run)
        session.commit()
        logger.success(
            "✓ Verification record inserted → research_runs.id = {}",
            test_run.id,
        )
    except Exception as exc:
        session.rollback()
        logger.error("✗ Verification INSERT failed: {}", exc)
        raise
    finally:
        session.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Main initialisation function
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def init_research_tables() -> None:
    """
    Idempotent bootstrap for Phase 2 research tables.

    Steps
    -----
    1. Create all ORM tables that belong to ``FundamentalBase``
       (this is additive — existing Phase 1 tables are untouched).
    2. Apply post-creation DDL: partial indexes and CHECK constraints.
    3. Insert a verification ``ResearchRun`` record.
    4. Print a summary of all research tables.
    """
    logger.info("═══ Phase 2: Research Schema Initialisation ═══")
    logger.info("Target database: {} (PostgreSQL)", settings.postgres_url.split("@")[-1])

    engine = _create_engine()

    # ── 1. Verify database connectivity ────────────────────────
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ PostgreSQL connection OK")
    except Exception as exc:
        logger.critical("✗ Cannot connect to PostgreSQL: {}", exc)
        sys.exit(1)

    # ── 2. Create ORM tables ──────────────────────────────────
    logger.info("Creating research tables (CREATE TABLE IF NOT EXISTS)…")
    FundamentalBase.metadata.create_all(bind=engine)

    # Count only the Phase 2 tables we care about
    research_tables = [
        "research_hypotheses",
        "sentiment_scores",
        "technical_signals",
        "fundamental_scores",
        "macro_signals",
        "research_runs",
    ]
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = ANY(:names)"
        ), {"names": research_tables})
        found = [row[0] for row in result]

    for tbl in research_tables:
        status = "✓" if tbl in found else "✗ MISSING"
        logger.info("  {} {}", status, tbl)

    if len(found) != len(research_tables):
        logger.critical("Not all research tables were created!")
        sys.exit(1)

    # ── 3. Post-creation DDL ──────────────────────────────────
    logger.info("Applying indexes and CHECK constraints…")
    _exec_sql_list(engine, RESEARCH_POST_CREATE_SQL, "Research-DDL")

    # ── 4. Verification INSERT ────────────────────────────────
    logger.info("Inserting verification record…")
    _insert_verification_record(engine)

    # ── 5. Summary ────────────────────────────────────────────
    with engine.connect() as conn:
        for tbl in research_tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}"))
            count = result.scalar()
            logger.info("  {} → {} rows", tbl, count)

    logger.success("═══ Phase 2 research schema initialisation complete ✓ ═══")
    engine.dispose()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CLI entry-point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    init_research_tables()
