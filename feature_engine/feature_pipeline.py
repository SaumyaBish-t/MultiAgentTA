"""
feature_engine.feature_pipeline
===============================

Prefect flow that computes L2 features and persists them.

Per ticker
----------
1. Pull ~3y daily closes (yfinance) → Hurst exponent → stock_profiles
2. Realised volatility (20d std of log returns)
3. Sector rotation score (from L1 ETF cache)
4. Cache the whole bundle to Redis ``features:{ticker}`` (2h TTL)

Globally
--------
1. Market breadth (one snapshot) → Redis + ``computed_features`` (per ticker row)

Publishes ``features.computed`` on Redis when done so L3 research agents
can pick up the new features.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import numpy as np
import psycopg2
import redis
from loguru import logger
from prefect import flow, task

from config.settings import settings
from core.channel_router import Channels
from feature_engine.agents.hurst_calculator import hurst_for_prices
from feature_engine.agents.market_breadth_agent import run as run_breadth
from feature_engine.agents.sector_rotation import score_for_ticker

try:
    import yfinance as yf  # type: ignore[import-untyped]
except Exception:  # pragma: no cover
    yf = None  # type: ignore[assignment]


def _redis() -> redis.Redis:
    return redis.from_url(settings.redis_url, socket_connect_timeout=3)


def _pg():
    return psycopg2.connect(settings.postgres_url, connect_timeout=5)


@task(retries=1)
def compute_for_ticker(ticker: str, breadth_ratio: float) -> dict[str, Any]:
    if yf is None:
        return {"ticker": ticker, "error": "yfinance not installed"}

    try:
        hist = yf.Ticker(ticker).history(period="3y", interval="1d")
    except Exception as exc:
        return {"ticker": ticker, "error": f"yf fetch failed: {exc}"}

    if hist.empty or len(hist) < 100:
        return {"ticker": ticker, "error": "insufficient history"}

    closes = hist["Close"].values
    volumes = hist["Volume"].values
    hurst = hurst_for_prices(closes)
    log_returns = np.diff(np.log(closes))
    realized_vol = float(np.std(log_returns[-20:]) * np.sqrt(252))
    sector_score = score_for_ticker(ticker)
    avg_volume = float(np.mean(volumes[-20:]))

    payload = {
        "ticker": ticker,
        "hurst_exponent": hurst.exponent,
        "hurst_class": hurst.classification,
        "preferred_strategy": hurst.preferred_strategy,
        "realized_vol": realized_vol,
        "market_breadth": breadth_ratio,
        "sector_rotation_score": sector_score,
        "avg_daily_volume": avg_volume,
        "feature_date": date.today().isoformat(),
    }

    # Cache to Redis with 2h TTL
    try:
        _redis().setex(f"features:{ticker}", timedelta(hours=2), json.dumps(payload))
    except Exception as exc:
        logger.warning("Redis cache failed for features:{}: {}", ticker, exc)

    # Persist stock_profiles (upsert) and computed_features (daily snapshot)
    try:
        conn = _pg()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stock_profiles
                    (ticker, hurst_exponent, hurst_class, preferred_strategy,
                     avg_daily_volume, last_computed_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    hurst_exponent = EXCLUDED.hurst_exponent,
                    hurst_class = EXCLUDED.hurst_class,
                    preferred_strategy = EXCLUDED.preferred_strategy,
                    avg_daily_volume = EXCLUDED.avg_daily_volume,
                    last_computed_at = NOW(),
                    updated_at = NOW()
                """,
                (ticker, hurst.exponent, hurst.classification,
                 hurst.preferred_strategy, avg_volume),
            )
            cur.execute(
                """
                INSERT INTO computed_features
                    (ticker, feature_date, hurst_exponent, market_breadth,
                     sector_rotation_score, realized_vol)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, feature_date) DO UPDATE SET
                    hurst_exponent = EXCLUDED.hurst_exponent,
                    market_breadth = EXCLUDED.market_breadth,
                    sector_rotation_score = EXCLUDED.sector_rotation_score,
                    realized_vol = EXCLUDED.realized_vol
                """,
                (ticker, date.today(), hurst.exponent, breadth_ratio,
                 sector_score, realized_vol),
            )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("Postgres write failed for {}: {}", ticker, exc)

    return payload


@flow(name="feature-engine-pipeline")
def feature_pipeline() -> dict:
    if not settings.feature_engine_enabled:
        logger.info("Feature engine disabled — skipping")
        return {"skipped": "feature flag off"}

    breadth = run_breadth()
    breadth_ratio = float(breadth.get("ratio", 1.0)) if isinstance(breadth, dict) else 1.0

    results = []
    for t in settings.tickers:
        results.append(compute_for_ticker(t, breadth_ratio))

    try:
        _redis().publish(Channels.FEATURES_COMPUTED, json.dumps({
            "tickers": [r["ticker"] for r in results if "ticker" in r],
            "feature_date": date.today().isoformat(),
        }))
    except Exception as exc:
        logger.warning("Could not publish features.computed: {}", exc)

    logger.info("Feature pipeline complete: {} tickers", len(results))
    return {"tickers": len(results), "breadth": breadth}


if __name__ == "__main__":
    print(feature_pipeline())
