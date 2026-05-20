"""
feature_engine.agents.sector_rotation
=====================================

Identify gaining-strength sectors by re-reading the ETF flow cache
written by L1's etf_flow_collector. Produces a ``sector_rotation_score``
per ticker (we approximate by averaging the score of every sector the
ticker plausibly belongs to — coarse but useful).
"""

from __future__ import annotations

import json
from typing import Dict

import redis
from loguru import logger

from config.settings import settings


def load_sector_flows() -> Dict[str, float]:
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=3)
        raw = r.get("etf:sector_flows")
        if not raw:
            return {}
        return json.loads(raw)
    except Exception as exc:
        logger.warning("Could not read etf:sector_flows: {}", exc)
        return {}


# Very coarse mapping — overridden by real classification if available.
# Keep additive: anything not in the map gets a 0 score (neutral).
TICKER_SECTOR: Dict[str, str] = {
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Communication Services",
    "AMZN": "Consumer Discretionary", "NVDA": "Technology", "TSLA": "Consumer Discretionary",
    "JPM": "Financials", "SPY": "Technology", "QQQ": "Technology",
}


def score_for_ticker(ticker: str) -> float:
    flows = load_sector_flows()
    sector = TICKER_SECTOR.get(ticker)
    if sector and sector in flows:
        return float(flows[sector])
    return 0.0
