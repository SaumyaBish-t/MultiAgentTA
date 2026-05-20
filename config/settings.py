"""
Trading System — Centralised Configuration
===========================================

Single source of truth for every tuneable parameter in the system.
Uses **pydantic-settings** (v2) to:
  • Load values from ``.env`` at project root
  • Override with real environment variables (12-factor friendly)
  • Validate types at import time — fail fast, not at 3 AM

Usage
-----
>>> from config import settings
>>> settings.polygon_api_key
>>> settings.validate()          # call once at startup
"""

from __future__ import annotations

import sys
from datetime import time
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar

from loguru import logger
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
"""Absolute path to the ``trading-system/`` directory."""


class Timeframe(str, Enum):
    """Supported OHLCV bar timeframes."""

    ONE_MIN = "1min"
    FIVE_MIN = "5min"
    FIFTEEN_MIN = "15min"
    ONE_HOUR = "1h"
    FOUR_HOUR = "4h"
    ONE_DAY = "1d"
    ONE_WEEK = "1w"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Settings
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class LLMConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    # Groq
    groq_api_key: str = ""
    groq_api_keys: str = ""
    groq_model_fast: str = "llama-3.3-70b-versatile"
    groq_model_simple: str = "llama-3.1-8b-instant"

    @field_validator("groq_api_keys", mode="before")
    @classmethod
    def _parse_groq_keys(cls, v: Any) -> str:
        if isinstance(v, list):
            return ",".join(v)
        return str(v or "")
    
    # Cerebras
    cerebras_api_key: str = ""
    cerebras_model: str = "qwen-3-235b-a22b-instruct-2507"
    cerebras_model_fast: str = "llama3.1-8b"
    cerebras_model_agent: str = "qwen-3-235b-a22b-instruct-2507"
    cerebras_model_main: str = "qwen-3-235b-a22b-instruct-2507"
    cerebras_base_url: str = "https://api.cerebras.ai/v1"
    
    # OpenRouter
    openrouter_api_key: str = ""
    openrouter_api_keys: str = ""
    openrouter_model: str = "deepseek/deepseek-r1"
    openrouter_base_url: str = "https://openrouter.ai/api/v1"

    @field_validator("openrouter_api_keys", mode="before")
    @classmethod
    def _parse_openrouter_keys(cls, v: Any) -> str:
        if isinstance(v, list):
            return ",".join(v)
        return str(v or "")
    
    # NVIDIA (embeddings only)
    nvidia_api_key: str = ""
    nvidia_embedding_model: str = "nvidia/nv-embedqa-e5-v5"
    nvidia_base_url: str = "https://integrate.api.nvidia.com/v1"
    
    # Mistral (fallback)
    mistral_api_key: str = ""
    mistral_model: str = "mistral-small-latest"

class Settings(BaseSettings):
    """
    Application-wide configuration loaded from environment variables.

    Sections
    --------
    API Keys         — credentials for every upstream data provider.
    Database         — connection strings for TimescaleDB, PostgreSQL, Redis, ChromaDB.
    Data             — which tickers / timeframes / market hours to track.
    Collector        — fetch intervals, retry policy, timeouts per source.
    Storage          — data-retention windows and write-batch sizes.

    Notes
    -----
    *  ``SecretStr`` fields are masked in logs / repr to prevent credential leaks.
    *  All durations are in **seconds**, all retention windows in **days**.
    """

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",            # don't blow up on unknown vars (e.g. PREFECT_*)
    )

    # ── API Keys ────────────────────────────────────────────────
    polygon_api_key: SecretStr = Field(
        ..., description="Polygon.io REST / WebSocket key"
    )
    fmp_api_key: SecretStr = Field(
        ..., description="Financial Modeling Prep key"
    )
    alpaca_api_key: SecretStr = Field(
        ..., description="Alpaca trading / market-data key"
    )
    alpaca_secret_key: SecretStr = Field(
        ..., description="Alpaca secret (paired with api key)"
    )
    fred_api_key: SecretStr = Field(
        ..., description="Federal Reserve Economic Data key"
    )
    news_api_key: SecretStr = Field(
        ..., description="NewsAPI.org key"
    )
    internal_api_key: str = Field(
        "secret-internal-token-123", description="API Key for internal microservices"
    )
    llm: LLMConfig = Field(default_factory=LLMConfig)

    # ── Database URLs ───────────────────────────────────────────
    timescale_url: str = Field(
        default="postgresql://trader:password@localhost:5435/market_data",
        description="TimescaleDB connection string (time-series OHLCV data)",
    )
    postgres_url: str = Field(
        default="postgresql://trader:password@localhost:5434/fundamentals",
        description="PostgreSQL connection string (fundamental / reference data)",
    )
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection string (caching & pub/sub)",
    )
    chroma_path: str = Field(
        default="./data/chromadb",
        description="Local filesystem path for ChromaDB vector store",
    )

    # ── Data Configuration ──────────────────────────────────────
    tickers: list[str] = Field(
        default=[
            # ── US Markets ──────────────────────────────────────
            # Mega-cap, highly liquid, sector-diversified — clean data and
            # deep liquidity give the strategy engine the best shot at a
            # tradeable edge.
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META",
            "AVGO", "LLY", "JPM", "V", "COST", "XOM",
            "SPY", "QQQ",
            # ── Indian Markets (NSE) ────────────────────────────
            "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
            "BHARTIARTL.NS", "SBIN.NS", "LT.NS", "HINDUNILVR.NS",
        ],
        description="Equity / ETF symbols to ingest",
    )
    timeframes: list[Timeframe] = Field(
        default=[
            Timeframe.ONE_MIN,
            Timeframe.FIVE_MIN,
            Timeframe.FIFTEEN_MIN,
            Timeframe.ONE_HOUR,
            Timeframe.ONE_DAY,
        ],
        description="Bar resolutions to collect for each ticker",
    )
    market_open_utc: time = Field(
        default=time(13, 30),
        description="NYSE regular-session open in UTC (09:30 ET → 13:30 UTC)",
    )
    market_close_utc: time = Field(
        default=time(20, 0),
        description="NYSE regular-session close in UTC (16:00 ET → 20:00 UTC)",
    )

    # ── Collector Configuration ─────────────────────────────────
    polygon_fetch_interval: int = Field(
        default=60,
        ge=1,
        description="Seconds between Polygon REST polls",
    )
    fmp_fetch_interval: int = Field(
        default=300,
        ge=1,
        description="Seconds between FMP REST polls",
    )
    alpaca_fetch_interval: int = Field(
        default=60,
        ge=1,
        description="Seconds between Alpaca REST polls",
    )
    fred_fetch_interval: int = Field(
        default=3600,
        ge=1,
        description="Seconds between FRED data pulls (macro data is slow-moving)",
    )
    news_fetch_interval: int = Field(
        default=900,
        ge=1,
        description="Seconds between NewsAPI headline pulls",
    )
    collector_retry_attempts: int = Field(
        default=3,
        ge=0,
        description="Max retries on transient collector failures",
    )
    collector_timeout: int = Field(
        default=30,
        ge=1,
        description="HTTP request timeout in seconds for all collectors",
    )
    collector_retry_backoff: float = Field(
        default=2.0,
        ge=1.0,
        description="Exponential back-off multiplier between retries",
    )

    # ── Storage Configuration ───────────────────────────────────
    retention_tick_days: int = Field(
        default=7,
        ge=1,
        description="Days to keep raw tick / 1-min data",
    )
    retention_ohlcv_days: int = Field(
        default=365,
        ge=1,
        description="Days to keep aggregated OHLCV bars",
    )
    retention_fundamental_days: int = Field(
        default=730,
        ge=1,
        description="Days to keep fundamental / filing snapshots",
    )
    retention_news_days: int = Field(
        default=90,
        ge=1,
        description="Days to keep news / sentiment records",
    )
    batch_size_timescale: int = Field(
        default=5000,
        ge=1,
        description="Rows per INSERT batch for TimescaleDB",
    )
    batch_size_postgres: int = Field(
        default=1000,
        ge=1,
        description="Rows per INSERT batch for PostgreSQL",
    )
    batch_size_chroma: int = Field(
        default=500,
        ge=1,
        description="Documents per upsert batch for ChromaDB",
    )

    # ── Internal ────────────────────────────────────────────────
    log_level: str = Field(
        default="INFO",
        description="Loguru / stdlib log level",
    )

    # ── Feature Flags (14-layer upgrade) ────────────────────────
    # Every new layer in the L0–L13 upgrade is gated by a flag.
    # Defaults are False/empty so behaviour is unchanged until enabled.
    # Flip via env (e.g. FEATURE_ENGINE_ENABLED=true) to roll a layer in;
    # flip back to False to instantly revert to the prior code path.
    feature_engine_enabled: bool = Field(
        default=False,
        description="L2 Feature Engine — Hurst, breadth, sector rotation, etc.",
    )
    review_gate_enabled: bool = Field(
        default=False,
        description="L5 Human Review Gate — LangGraph interrupt before Phase 4",
    )
    meta_analysis_enabled: bool = Field(
        default=False,
        description="L11 Meta-Analysis — agent calibration + human-override value",
    )
    memory_layer_enabled: bool = Field(
        default=False,
        description="L12 Memory Layer — Obsidian vault + LlamaIndex RAG",
    )
    insider_flow_enabled: bool = Field(
        default=False,
        description="L1 addition — SEC Form 4 insider transaction collector",
    )
    options_flow_enabled: bool = Field(
        default=False,
        description="L1 addition — CBOE put/call ratio collector",
    )
    etf_flow_enabled: bool = Field(
        default=False,
        description="L1 addition — sector ETF relative-flow collector",
    )
    obsidian_vault_path: str = Field(
        default="",
        description="Absolute path to the Obsidian vault root (L12). Empty disables vault writes.",
    )
    mlflow_tracking_uri: str = Field(
        default="./mlruns",
        description="MLflow tracking URI for L4 strategy experiment logging",
    )

    # Class-level constants (not loaded from env)
    _API_KEY_FIELDS: ClassVar[list[str]] = [
        "polygon_api_key",
        "fmp_api_key",
        "alpaca_api_key",
        "alpaca_secret_key",
        "fred_api_key",
        "news_api_key",
    ]

    # ── Validators ──────────────────────────────────────────────
    @field_validator("tickers", mode="before")
    @classmethod
    def _parse_tickers(cls, v: str | list[str]) -> list[str]:
        """Accept a comma-separated string *or* a list from env."""
        if isinstance(v, str):
            return [t.strip().upper() for t in v.split(",") if t.strip()]
        return [t.upper() for t in v]

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalise_log_level(cls, v: str) -> str:
        return v.upper()

    # ── Helpers ─────────────────────────────────────────────────
    @property
    def chroma_abs_path(self) -> Path:
        """Resolve ChromaDB path relative to PROJECT_ROOT."""
        p = Path(self.chroma_path)
        return p if p.is_absolute() else PROJECT_ROOT / p

    @property
    def fetch_intervals(self) -> dict[str, int]:
        """Convenience mapping of source → fetch interval (seconds)."""
        return {
            "polygon": self.polygon_fetch_interval,
            "fmp": self.fmp_fetch_interval,
            "alpaca": self.alpaca_fetch_interval,
            "fred": self.fred_fetch_interval,
            "news": self.news_fetch_interval,
        }

    # ── Startup Validation ──────────────────────────────────────
    def validate(self) -> None:
        """
        Run once at application startup to surface misconfigurations early.

        Checks
        ------
        1. Every API-key field is non-empty.
        2. TimescaleDB and PostgreSQL accept a connection.
        3. Redis is reachable (``PING``).
        4. ChromaDB directory exists (or can be created).

        Raises
        ------
        SystemExit
            On any fatal validation failure so the process never enters
            a half-working state.
        """
        ok = True

        # ── 1. API Keys ────────────────────────────────────────
        for field_name in self._API_KEY_FIELDS:
            secret: SecretStr = getattr(self, field_name)
            if not secret.get_secret_value().strip():
                logger.error("Missing API key: {}", field_name)
                ok = False
            else:
                logger.debug("✓ {} is set", field_name)

        # ── 2. TimescaleDB / PostgreSQL ─────────────────────────
        for label, dsn in [
            ("TimescaleDB", self.timescale_url),
            ("PostgreSQL", self.postgres_url),
        ]:
            try:
                import psycopg2  # type: ignore[import-untyped]

                conn = psycopg2.connect(dsn, connect_timeout=5)
                conn.close()
                logger.info("✓ {} connection OK", label)
            except Exception as exc:
                logger.warning("✗ {} unreachable — {}", label, exc)
                ok = False

        # ── 3. Redis ────────────────────────────────────────────
        try:
            import redis as _redis  # type: ignore[import-untyped]

            r = _redis.from_url(self.redis_url, socket_connect_timeout=5)
            r.ping()
            logger.info("✓ Redis connection OK")
        except Exception as exc:
            logger.warning("✗ Redis unreachable — {}", exc)
            ok = False

        # ── 4. ChromaDB directory ───────────────────────────────
        try:
            self.chroma_abs_path.mkdir(parents=True, exist_ok=True)
            logger.info("✓ ChromaDB path OK ({})", self.chroma_abs_path)
        except OSError as exc:
            logger.warning("✗ Cannot create ChromaDB dir — {}", exc)
            ok = False

        # ── Verdict ─────────────────────────────────────────────
        if not ok:
            logger.critical(
                "Configuration validation FAILED — fix errors above before running."
            )
            sys.exit(1)

        logger.success("All configuration checks passed ✓")

    def validate_llm_connections(self):
        """Minimal test call to Groq and Cerebras on startup to verify connectivity."""
        import time as _time
        if not self.llm.groq_api_key or not self.llm.cerebras_api_key:
            logger.warning("Skipping LLM validation because API keys are missing. Ensure .env is populated.")
            return

        import httpx

        # Test Groq
        try:
            r = httpx.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.llm.groq_api_key}"},
                json={"model": self.llm.groq_model_simple, "messages": [{"role": "user", "content": "Hi"}], "max_tokens": 5},
                timeout=5.0
            )
            if r.status_code == 200:
                logger.info("✅ SUCCESS: Groq connection verified.")
            else:
                logger.warning(f"❌ FAILED: Groq connection failed with {r.status_code}: {r.text}")
        except Exception as e:
            logger.warning(f"❌ FAILED: Groq connection error: {e}")

        _time.sleep(1)
        # Test Cerebras
        try:
            r = httpx.post(
                f"{self.llm.cerebras_base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.llm.cerebras_api_key}"},
                json={"model": self.llm.cerebras_model_main, "messages": [{"role": "user", "content": "Reply with only the word CEREBRAS"}], "max_tokens": 10},
                timeout=5.0
            )
            if r.status_code == 200 and "CEREBRAS" in r.text:
                logger.info("✅ SUCCESS: Cerebras connection verified (qwen-3-235b)")
            else:
                logger.warning(f"❌ FAILED: Cerebras connection failed with {r.status_code}: {r.text}")
        except Exception as e:
            logger.warning(f"❌ FAILED: Cerebras connection error: {e}")
        
        _time.sleep(2)
        # Test Cerebras Agent Model
        try:
            r = httpx.post(
                f"{self.llm.cerebras_base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.llm.cerebras_api_key}"},
                json={"model": self.llm.cerebras_model_agent, "messages": [{"role": "user", "content": "Reply with only the word CEREBRAS"}], "max_tokens": 10},
                timeout=5.0
            )
            if r.status_code == 200 and "CEREBRAS" in r.text:
                logger.info("✅ SUCCESS: Cerebras GLM agent model verified")
            else:
                logger.warning(f"❌ FAILED: Cerebras GLM agent connection failed with {r.status_code}: {r.text}")
        except Exception as e:
            logger.warning(f"❌ FAILED: Cerebras GLM agent connection error: {e}")
        
        _time.sleep(1)
        # Test Cerebras Fast Model
        try:
            r = httpx.post(
                f"{self.llm.cerebras_base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.llm.cerebras_api_key}"},
                json={"model": self.llm.cerebras_model_fast, "messages": [{"role": "user", "content": "Reply with only the word CEREBRAS"}], "max_tokens": 10},
                timeout=5.0
            )
            if r.status_code == 200 and "CEREBRAS" in r.text:
                logger.info("✅ SUCCESS: Cerebras fast model verified")
            else:
                logger.warning(f"❌ FAILED: Cerebras fast model connection failed with {r.status_code}: {r.text}")
        except Exception as e:
            logger.warning(f"❌ FAILED: Cerebras fast model connection error: {e}")
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Singleton
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

settings = Settings()  # type: ignore[call-arg]
"""
Module-level singleton — import this everywhere::

    from config.settings import settings
"""
