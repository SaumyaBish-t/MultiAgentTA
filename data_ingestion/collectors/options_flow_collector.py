"""
data_ingestion.collectors.options_flow_collector
================================================

Put/Call ratio collector (L1 addition).

Data source
-----------
yfinance option chains are free and sufficient for daily-resolution
P/C ratios on US listings. For Indian tickers (``.NS`` / ``.BSE``)
options data isn't reliably available via yfinance — the collector
records ``None`` and notes Opstra.in as the recommended manual source.

Storage
-------
Redis ``options:pcr:{ticker}`` JSON ``{ratio, sentiment}`` with 1h TTL.

Feature flag
------------
Skips silently when ``settings.options_flow_enabled`` is ``False``.
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


def _classify(pcr: float) -> str:
    """Map P/C ratio to a coarse sentiment label."""
    if pcr < 0.7:
        return "bullish"
    if pcr < 1.0:
        return "neutral_bullish"
    if pcr < 1.3:
        return "neutral_bearish"
    return "bearish"


def _is_indian(ticker: str) -> bool:
    return ticker.endswith(".NS") or ticker.endswith(".BSE")


def collect_put_call_ratio(ticker: str) -> dict:
    """Compute a same-day put/call open-interest ratio.

    Returns ``{ratio, sentiment, ...}``. Empty dict if feature flag off.
    """
    if not settings.options_flow_enabled:
        return {"ticker": ticker, "skipped": "feature flag off"}
    if _is_indian(ticker):
        return {
            "ticker": ticker,
            "skipped": "Indian ticker — use Opstra.in for manual P/C checks",
        }
    if yf is None:
        return {"ticker": ticker, "error": "yfinance not installed"}

    try:
        tk = yf.Ticker(ticker)
        expiries = tk.options
        if not expiries:
            return {"ticker": ticker, "error": "no options data"}
        # Take the nearest expiry — that's where flow is most informative.
        chain = tk.option_chain(expiries[0])
        put_oi = float(chain.puts["openInterest"].fillna(0).sum())
        call_oi = float(chain.calls["openInterest"].fillna(0).sum())
        if call_oi == 0:
            return {"ticker": ticker, "error": "zero call OI"}
        pcr = put_oi / call_oi
    except Exception as exc:
        logger.warning("yfinance options chain failed for {}: {}", ticker, exc)
        return {"ticker": ticker, "error": str(exc)}

    sentiment = _classify(pcr)
    payload = {"ratio": pcr, "sentiment": sentiment, "expiry": expiries[0]}
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=3)
        r.setex(
            f"options:pcr:{ticker}",
            timedelta(hours=1),
            json.dumps(payload),
        )
    except Exception as exc:
        logger.warning("Redis cache failed for options:pcr:{}: {}", ticker, exc)

    logger.info("Options P/C {}: {:.2f} ({})", ticker, pcr, sentiment)
    return {"ticker": ticker, **payload}


def collect_all() -> list[dict]:
    return [collect_put_call_ratio(t) for t in settings.tickers]


if __name__ == "__main__":
    for r in collect_all():
        print(r)
