"""
signal_generation.agents.stock_classifier
=========================================

Uses the Hurst classification produced by L2 to constrain which
strategy families a ticker is eligible for. Momentum stocks don't get
mean-reversion strategies; mean-reverting stocks don't get trend-following.

Returns a list of allowed strategy types that the StrategyCoder agent
can use to constrain its generation.
"""

from __future__ import annotations

import psycopg2
from loguru import logger

from config.settings import settings

ALLOWED_STRATEGIES: dict[str, list[str]] = {
    "trending":       ["momentum", "trend_following", "breakout"],
    "mean_reverting": ["mean_reversion", "pairs_trade", "vwap_revert"],
    "random":         ["factor_tilt", "long_short", "market_neutral"],
}


def allowed_strategies(ticker: str) -> list[str]:
    """Return the list of strategy families appropriate for this ticker.

    Falls back to a permissive default if L2 hasn't profiled the ticker
    yet (so existing pipeline behaviour isn't blocked).
    """
    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT hurst_class FROM stock_profiles WHERE ticker = %s",
                (ticker,),
            )
            row = cur.fetchone()
        conn.close()
    except Exception as exc:
        logger.warning("stock_classifier DB read failed for {}: {}", ticker, exc)
        return ["momentum", "mean_reversion", "factor_tilt"]

    if not row or not row[0]:
        return ["momentum", "mean_reversion", "factor_tilt"]

    return ALLOWED_STRATEGIES.get(row[0], ["factor_tilt"])
