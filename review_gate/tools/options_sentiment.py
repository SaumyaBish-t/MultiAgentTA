"""
review_gate.tools.options_sentiment
===================================

Reads the put/call ratio from Redis (populated by L1's
options_flow_collector) and decides whether it confirms or contradicts
the proposed trade direction.
"""

from __future__ import annotations

import json

import redis
from loguru import logger

from config.settings import settings
from review_gate.models import OptionsCheckResult


def check(ticker: str, direction: str) -> OptionsCheckResult:
    """``direction`` should be ``'long'`` or ``'short'``."""
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        raw = r.get(f"options:pcr:{ticker}")
    except Exception as exc:
        logger.warning("options_sentiment Redis read failed: {}", exc)
        return OptionsCheckResult()

    if not raw:
        return OptionsCheckResult()

    payload = json.loads(raw)
    pcr = float(payload.get("ratio", 1.0))
    sentiment = payload.get("sentiment", "")
    bullish = pcr < 1.0
    aligned = (direction == "long" and bullish) or (direction == "short" and not bullish)
    return OptionsCheckResult(
        put_call_ratio=pcr,
        sentiment=sentiment,
        aligned_with_signal=aligned,
    )
