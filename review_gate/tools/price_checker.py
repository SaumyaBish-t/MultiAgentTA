"""
review_gate.tools.price_checker
===============================

Compares the price at signal generation with the current price.
A move beyond a strategy-type-specific threshold invalidates the signal.
"""

from __future__ import annotations

from loguru import logger

from review_gate.models import PriceCheckResult

# strategy_type -> invalidation threshold (percent)
THRESHOLDS = {
    "momentum":       1.5,
    "breakout":       1.0,
    "mean_reversion": 2.5,
    "factor_tilt":    3.0,
}

DEFAULT_THRESHOLD = 2.0


def _latest_price(ticker: str) -> float | None:
    try:
        import yfinance as yf  # type: ignore[import-untyped]
        hist = yf.Ticker(ticker).history(period="1d", interval="1m")
        if hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception as exc:
        logger.warning("price_checker yfinance failed for {}: {}", ticker, exc)
        return None


def check(ticker: str, price_at_signal: float, strategy_type: str) -> PriceCheckResult:
    current = _latest_price(ticker) or price_at_signal
    pct = (current - price_at_signal) / price_at_signal * 100.0 if price_at_signal else 0.0
    threshold = THRESHOLDS.get(strategy_type, DEFAULT_THRESHOLD)
    still_valid = abs(pct) <= threshold
    return PriceCheckResult(
        price_at_signal=price_at_signal,
        current_price=current,
        pct_move=round(pct, 3),
        still_valid=still_valid,
    )
