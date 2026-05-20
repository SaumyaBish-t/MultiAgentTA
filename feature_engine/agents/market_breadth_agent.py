"""
feature_engine.agents.market_breadth_agent
==========================================

Advance/decline ratio across the tracked universe.

Interpretation
--------------
* >1.5  broad rally — healthy
* 1.0–1.5 moderate participation
* <1.0  narrow / fragile — risk-off bias warranted
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Iterable

import redis
from loguru import logger

from config.settings import settings

try:
    import yfinance as yf  # type: ignore[import-untyped]
except Exception:  # pragma: no cover
    yf = None  # type: ignore[assignment]


def compute_breadth(tickers: Iterable[str]) -> dict:
    """Return ``{advancers, decliners, ratio, label}`` over yesterday's close."""
    if yf is None:
        return {"error": "yfinance not installed"}

    advancers = 0
    decliners = 0
    for t in tickers:
        try:
            hist = yf.Ticker(t).history(period="5d", interval="1d")
            if len(hist) < 2:
                continue
            change = hist["Close"].iloc[-1] - hist["Close"].iloc[-2]
            if change > 0:
                advancers += 1
            elif change < 0:
                decliners += 1
        except Exception:
            continue

    if decliners == 0:
        ratio = float("inf") if advancers else 1.0
    else:
        ratio = advancers / decliners

    if ratio > 1.5:
        label = "broad_rally"
    elif ratio > 1.0:
        label = "moderate"
    elif ratio > 0.6:
        label = "narrow"
    else:
        label = "broad_decline"

    return {"advancers": advancers, "decliners": decliners, "ratio": round(ratio, 3), "label": label}


def cache_breadth(payload: dict) -> None:
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=3)
        r.setex("market:breadth:current", timedelta(hours=1), json.dumps(payload))
    except Exception as exc:
        logger.warning("Failed to cache market breadth: {}", exc)


def run() -> dict:
    payload = compute_breadth(settings.tickers)
    if "error" not in payload:
        cache_breadth(payload)
    logger.info("Market breadth: {}", payload)
    return payload
