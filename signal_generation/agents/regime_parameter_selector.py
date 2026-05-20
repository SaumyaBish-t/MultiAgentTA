"""
signal_generation.agents.regime_parameter_selector
==================================================

Picks regime-conditional parameters for a given strategy family.

Tables are keyed by ``(strategy_family, regime)`` and yield concrete
defaults — EMA periods, lookbacks, stop multipliers, position sizing
multiplier. Falls back to baseline if the regime isn't in Redis yet.
"""

from __future__ import annotations

import json
from typing import Any

import redis
from loguru import logger

from config.settings import settings

# (strategy, regime) → params
PARAMETERS: dict[tuple[str, str], dict[str, Any]] = {
    # Momentum family
    ("momentum", "bull_low_vol"):   {"ema_fast": 10, "ema_slow": 30, "stop_mult": 2.0, "size_mult": 1.0},
    ("momentum", "bull_high_vol"):  {"ema_fast": 8,  "ema_slow": 26, "stop_mult": 2.5, "size_mult": 0.7},
    ("momentum", "bear_low_vol"):   {"ema_fast": 12, "ema_slow": 34, "stop_mult": 1.5, "size_mult": 0.5},
    ("momentum", "bear_high_vol"):  {"ema_fast": 14, "ema_slow": 40, "stop_mult": 1.2, "size_mult": 0.2},
    # Mean reversion family
    ("mean_reversion", "bull_low_vol"):  {"zscore": 1.5, "lookback": 20, "stop_mult": 1.5, "size_mult": 1.0},
    ("mean_reversion", "bull_high_vol"): {"zscore": 2.0, "lookback": 15, "stop_mult": 2.0, "size_mult": 0.7},
    ("mean_reversion", "bear_low_vol"):  {"zscore": 1.8, "lookback": 25, "stop_mult": 1.5, "size_mult": 0.5},
    ("mean_reversion", "bear_high_vol"): {"zscore": 2.5, "lookback": 20, "stop_mult": 1.5, "size_mult": 0.2},
    # Factor tilt family
    ("factor_tilt", "bull_low_vol"):  {"rebalance_days": 21, "top_n": 5, "size_mult": 1.0},
    ("factor_tilt", "bull_high_vol"): {"rebalance_days": 14, "top_n": 5, "size_mult": 0.7},
    ("factor_tilt", "bear_low_vol"):  {"rebalance_days": 30, "top_n": 3, "size_mult": 0.5},
    ("factor_tilt", "bear_high_vol"): {"rebalance_days": 21, "top_n": 3, "size_mult": 0.2},
}

BASELINE: dict[str, dict[str, Any]] = {
    "momentum":       {"ema_fast": 10, "ema_slow": 30, "stop_mult": 2.0, "size_mult": 0.8},
    "mean_reversion": {"zscore": 2.0, "lookback": 20, "stop_mult": 1.5, "size_mult": 0.8},
    "factor_tilt":    {"rebalance_days": 21, "top_n": 5, "size_mult": 0.8},
}


def _current_regime() -> str:
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        raw = r.get("monitoring:regime:current")
        if not raw:
            return "bull_low_vol"
        payload = json.loads(raw)
        return payload.get("regime", "bull_low_vol")
    except Exception as exc:
        logger.warning("regime read failed: {}", exc)
        return "bull_low_vol"


def select_parameters(strategy: str) -> dict[str, Any]:
    """Return params for the given strategy family, conditioned on regime."""
    regime = _current_regime()
    params = PARAMETERS.get((strategy, regime))
    if params is not None:
        return {**params, "_regime": regime, "_strategy": strategy}
    fallback = BASELINE.get(strategy, {"size_mult": 0.5})
    return {**fallback, "_regime": regime, "_strategy": strategy, "_fallback": True}
