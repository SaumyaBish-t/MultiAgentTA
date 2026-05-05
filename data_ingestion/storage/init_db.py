"""
Trading System — Database Manager & Initialisation
====================================================

Provides:

* ``DatabaseManager``
    Central gateway to both databases.  Manages SQLAlchemy engines with
    connection pooling, exposes scoped-session factories, and handles
    graceful shutdown.

* ``init_databases()``
    Idempotent bootstrap function — creates all tables (ORM), converts
    TimescaleDB tables to hypertables, sets up retention / compression
    policies, and installs the full-text search trigger for news.

Usage
-----
::

    from data_ingestion.storage.init_db import db_manager, init_databases

    # At startup
    init_databases()

    # Anywhere you need a session
    with db_manager.timescale_session() as session:
        session.execute(...)

    # At shutdown
    db_manager.dispose()
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from typing import Generator

from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

# ── Internal imports ────────────────────────────────────────────
from data_ingestion.storage.models import (
    FundamentalBase,
    TimescaleBase,
)
from data_ingestion.storage.schema import (
    FUNDAMENTAL_POST_CREATE_SQL,
    TIMESCALE_POLICY_SQL,
    TIMESCALE_POST_CREATE_SQL,
)

# Settings are loaded lazily to avoid circular-import issues when
# this module is imported during Alembic migrations.
_settings = None


def _get_settings():
    """Lazy-load the settings singleton."""
    global _settings
    if _settings is None:
        from config.settings import settings
        _settings = settings
    return _settings


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Database Manager
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DatabaseManager:
    """
    Manages SQLAlchemy engines and session factories for both databases.

    Parameters
    ----------
    timescale_url : str
        Connection string for the TimescaleDB ``market_data`` database.
    postgres_url : str
        Connection string for the PostgreSQL ``fundamentals`` database.
    pool_min : int
        Minimum number of connections to keep in each pool.
    pool_max : int
        Maximum number of connections each pool may grow to.

    Notes
    -----
    * Uses ``QueuePool`` (SQLAlchemy default) with ``pool_pre_ping=True``
      so stale connections are recycled transparently.
    * ``pool_recycle=1800`` ensures connections are refreshed every
      30 minutes to avoid server-side timeouts.
    """

    def __init__(
        self,
        timescale_url: str,
        postgres_url: str,
        pool_min: int = 2,
        pool_max: int = 10,
    ) -> None:
        self._timescale_url = timescale_url
        self._postgres_url = postgres_url

        # ── Engines ─────────────────────────────────────────────
        self._ts_engine: Engine = create_engine(
            timescale_url,
            pool_size=pool_min,
            max_overflow=pool_max - pool_min,
            pool_pre_ping=True,
            pool_recycle=1800,
            echo=False,
            future=True,
        )
        self._pg_engine: Engine = create_engine(
            postgres_url,
            pool_size=pool_min,
            max_overflow=pool_max - pool_min,
            pool_pre_ping=True,
            pool_recycle=1800,
            echo=False,
            future=True,
        )

        # ── Session factories ───────────────────────────────────
        self._ts_session_factory = sessionmaker(
            bind=self._ts_engine, expire_on_commit=False,
        )
        self._pg_session_factory = sessionmaker(
            bind=self._pg_engine, expire_on_commit=False,
        )

        logger.info(
            "DatabaseManager initialised  "
            "TimescaleDB={} | PostgreSQL={}",
            self._timescale_url.split("@")[-1],   # hide credentials
            self._postgres_url.split("@")[-1],
        )

    # ── Properties ──────────────────────────────────────────────

    @property
    def timescale_engine(self) -> Engine:
        """SQLAlchemy engine for TimescaleDB (market_data)."""
        return self._ts_engine

    @property
    def postgres_engine(self) -> Engine:
        """SQLAlchemy engine for PostgreSQL (fundamentals)."""
        return self._pg_engine

    # ── Context-managed sessions ────────────────────────────────

    @contextmanager
    def timescale_session(self) -> Generator[Session, None, None]:
        """
        Yield a TimescaleDB session that auto-commits on success
        and rolls back on exception.
        """
        session: Session = self._ts_session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def postgres_session(self) -> Generator[Session, None, None]:
        """
        Yield a PostgreSQL session that auto-commits on success
        and rolls back on exception.
        """
        session: Session = self._pg_session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ── Health-check ────────────────────────────────────────────

    def check_connections(self) -> dict[str, bool]:
        """
        Ping both databases and return a health dict.

        Returns
        -------
        dict[str, bool]
            ``{"timescale": True/False, "postgres": True/False}``
        """
        results: dict[str, bool] = {}
        for label, engine in [
            ("timescale", self._ts_engine),
            ("postgres", self._pg_engine),
        ]:
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                results[label] = True
                logger.debug("✓ {} ping OK", label)
            except Exception as exc:
                results[label] = False
                logger.warning("✗ {} ping FAILED — {}", label, exc)
        return results

    # ── Shutdown ────────────────────────────────────────────────

    def dispose(self) -> None:
        """Close all pooled connections (call at shutdown)."""
        self._ts_engine.dispose()
        self._pg_engine.dispose()
        logger.info("DatabaseManager disposed — all connections closed")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Module-level Singleton
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_db_manager: DatabaseManager | None = None


def get_db_manager() -> DatabaseManager:
    """
    Return (and lazily create) the global ``DatabaseManager`` singleton.

    The manager is instantiated on first call using the URLs from
    ``config.settings``.
    """
    global _db_manager
    if _db_manager is None:
        cfg = _get_settings()
        _db_manager = DatabaseManager(
            timescale_url=cfg.timescale_url,
            postgres_url=cfg.postgres_url,
        )
    return _db_manager


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Database Initialisation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _exec_sql_list(
    engine: Engine, statements: list[str], label: str, **fmt_kwargs: int,
) -> None:
    """Execute a list of raw SQL statements inside a single transaction."""
    with engine.begin() as conn:
        for stmt in statements:
            rendered = stmt.format(**fmt_kwargs) if fmt_kwargs else stmt
            try:
                conn.execute(text(rendered))
            except Exception as exc:
                logger.warning(
                    "{} — statement failed (may be idempotent): {}",
                    label, exc,
                )


def init_databases() -> DatabaseManager:
    """
    Idempotent database bootstrap.

    Steps
    -----
    1. Create ORM tables on both databases (``CREATE TABLE IF NOT EXISTS``).
    2. Enable TimescaleDB extension and convert tables to hypertables.
    3. Set chunk intervals, retention, and compression policies.
    4. Install the full-text search trigger on ``news_articles``.

    Returns
    -------
    DatabaseManager
        The initialised singleton — ready for queries.
    """
    cfg = _get_settings()
    mgr = get_db_manager()

    # ── 1. ORM tables ───────────────────────────────────────────
    logger.info("Creating TimescaleDB tables (market_data)…")
    TimescaleBase.metadata.create_all(bind=mgr.timescale_engine)

    logger.info("Creating PostgreSQL tables (fundamentals)…")
    FundamentalBase.metadata.create_all(bind=mgr.postgres_engine)

    # ── 2. Hypertables & chunk intervals ────────────────────────
    logger.info("Converting to TimescaleDB hypertables…")
    _exec_sql_list(
        mgr.timescale_engine,
        TIMESCALE_POST_CREATE_SQL,
        "TimescaleDB-hypertable",
    )

    # ── 3. Retention & compression policies ─────────────────────
    logger.info("Setting retention & compression policies…")
    _exec_sql_list(
        mgr.timescale_engine,
        TIMESCALE_POLICY_SQL,
        "TimescaleDB-policy",
        retention_days=cfg.retention_ohlcv_days,
    )

    # ── 4. Full-text search trigger ─────────────────────────────
    logger.info("Installing full-text search trigger (news_articles)…")
    _exec_sql_list(
        mgr.postgres_engine,
        FUNDAMENTAL_POST_CREATE_SQL,
        "PostgreSQL-FTS",
    )

    # ── Health check ────────────────────────────────────────────
    health = mgr.check_connections()
    if all(health.values()):
        logger.success("Database initialisation complete ✓")
    else:
        logger.critical("Database health check FAILED: {}", health)
        sys.exit(1)

    return mgr


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CLI entry-point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    logger.info("Running database initialisation…")
    init_databases()
