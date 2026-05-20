"""
alpha_research.utils.feature_context
====================================

L2 → L3 bridge: research agents call ``get_feature_context(ticker)``
and get back a dict of L2-computed features (Hurst, breadth, sector
score, vol, etc.). If the feature engine is disabled or the cache miss,
sensible defaults are returned so existing agents never crash.
"""

from __future__ import annotations

import json
from typing import Any

import redis
from loguru import logger

from config.settings import settings

_DEFAULTS: dict[str, Any] = {
    "hurst_exponent": 0.5,
    "hurst_class": "random",
    "preferred_strategy": "factor_tilt",
    "realized_vol": 0.20,        # 20% annualised
    "market_breadth": 1.0,
    "sector_rotation_score": 0.0,
    "put_call_ratio": None,
    "insider_signal": 0.0,
}


def get_feature_context(ticker: str) -> dict[str, Any]:
    """Return L2 features for ``ticker`` from Redis (with safe defaults)."""
    if not settings.feature_engine_enabled:
        return dict(_DEFAULTS)

    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        raw = r.get(f"features:{ticker}")
        if not raw:
            return dict(_DEFAULTS)
        payload = json.loads(raw)
    except Exception as exc:
        logger.warning("feature_context read failed for {}: {}", ticker, exc)
        return dict(_DEFAULTS)

    merged = dict(_DEFAULTS)
    merged.update({k: payload.get(k, v) for k, v in _DEFAULTS.items()})

    # Pull supplementary L1 caches if present.
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        pcr_raw = r.get(f"options:pcr:{ticker}")
        if pcr_raw:
            merged["put_call_ratio"] = json.loads(pcr_raw).get("ratio")
        ins_raw = r.get(f"insider:signal:{ticker}")
        if ins_raw:
            merged["insider_signal"] = json.loads(ins_raw).get("signal", 0.0)
    except Exception:
        pass

    return merged
