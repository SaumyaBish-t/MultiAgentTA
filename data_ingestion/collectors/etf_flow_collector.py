"""
data_ingestion.collectors.etf_flow_collector
============================================

Sector ETF relative-flow proxy (L1 addition).

Approach
--------
True ETF inflow/outflow data is gated behind paid feeds, but the
5-day vs 20-day relative-return spread of each sector ETF is a strong
*proxy* for institutional rotation — money chasing winners and fleeing
losers shows up in price first.

Tracked sectors (US SPDR family):
    XLK   Technology
    XLF   Financials
    XLV   Healthcare
    XLE   Energy
    XLY   Consumer Discretionary
    XLP   Consumer Staples
    XLI   Industrials
    XLU   Utilities
    XLB   Materials
    XLRE  Real Estate
    XLC   Communication Services

Storage
-------
Redis ``etf:sector_flows`` JSON ``{sector: flow_score}`` with 1h TTL.
``flow_score`` is the 5-day return minus the 20-day return (both in
percentage points).
"""

from __future__ import annotations

import json
from datetime import timedelta

import redis
from loguru import logger

from config.settings import settings

try:
    import yfinance as yf  # type: ignore[import-untyped]
except Exception:  # pragma: no cover
    yf = None  # type: ignore[assignment]

SECTOR_ETFS: dict[str, str] = {
    "XLK": "Technology",
    "XLF": "Financials",
    "XLV": "Healthcare",
    "XLE": "Energy",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLI": "Industrials",
    "XLU": "Utilities",
    "XLB": "Materials",
    "XLRE": "Real Estate",
    "XLC": "Communication Services",
}


def _return_pct(series, lookback: int) -> float | None:
    if len(series) < lookback + 1:
        return None
    start = series.iloc[-lookback - 1]
    end = series.iloc[-1]
    if start == 0 or start is None:
        return None
    return float((end - start) / start * 100.0)


def compute_sector_flows() -> dict[str, float]:
    """Compute the 5d-minus-20d return spread for every sector ETF."""
    if yf is None:
        logger.warning("yfinance not installed — cannot compute ETF flows")
        return {}

    out: dict[str, float] = {}
    for etf, label in SECTOR_ETFS.items():
        try:
            hist = yf.Ticker(etf).history(period="2mo", interval="1d")
            if hist.empty:
                continue
            closes = hist["Close"]
            r5 = _return_pct(closes, 5)
            r20 = _return_pct(closes, 20)
            if r5 is None or r20 is None:
                continue
            out[label] = round(r5 - r20, 3)
        except Exception as exc:
            logger.warning("ETF flow fetch failed for {}: {}", etf, exc)
    return out


def collect_etf_flows() -> dict:
    if not settings.etf_flow_enabled:
        return {"skipped": "feature flag off"}

    flows = compute_sector_flows()
    if not flows:
        return {"error": "no flows computed"}

    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=3)
        r.setex("etf:sector_flows", timedelta(hours=1), json.dumps(flows))
    except Exception as exc:
        logger.warning("Redis cache failed for etf:sector_flows: {}", exc)

    logger.info("ETF flows computed for {} sectors", len(flows))
    return flows


if __name__ == "__main__":
    print(collect_etf_flows())
