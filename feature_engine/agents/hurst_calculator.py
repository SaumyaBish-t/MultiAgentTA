"""
feature_engine.agents.hurst_calculator
======================================

Hurst exponent via Rescaled-Range (R/S) analysis.

Interpretation
--------------
* H > 0.55 → trending (momentum strategies work)
* H ≈ 0.5  → random walk (factor tilts only)
* H < 0.45 → mean-reverting (mean-reversion strategies work)

Reference: Hurst 1951; Peters 1991 (Fractal Market Hypothesis).
Window: doc spec says 756 trading days (~3 years).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


HURST_WINDOW = 756


@dataclass
class HurstResult:
    exponent: float
    classification: str  # "trending" | "random" | "mean_reverting"
    preferred_strategy: str  # "momentum" | "factor_tilt" | "mean_reversion"


def _classify(h: float) -> tuple[str, str]:
    if h > 0.55:
        return "trending", "momentum"
    if h < 0.45:
        return "mean_reverting", "mean_reversion"
    return "random", "factor_tilt"


def compute_hurst(prices: np.ndarray, max_lag: int = 100) -> float:
    """Compute Hurst exponent via R/S analysis on a 1-D price series.

    Parameters
    ----------
    prices : np.ndarray
        Daily closing prices, oldest to newest.
    max_lag : int
        Maximum lag for the R/S regression (capped at len(prices)//4).
    """
    prices = np.asarray(prices, dtype=float)
    if len(prices) < 100:
        # Too little data — return 0.5 (random walk) as a sensible default.
        return 0.5

    log_returns = np.diff(np.log(prices))
    max_lag = min(max_lag, len(log_returns) // 4)
    lags = list(range(10, max_lag, 5))
    if not lags:
        return 0.5

    rs_values: list[float] = []
    for lag in lags:
        # Partition into non-overlapping chunks of size `lag`.
        n_chunks = len(log_returns) // lag
        if n_chunks < 2:
            continue
        chunks = log_returns[: n_chunks * lag].reshape(n_chunks, lag)
        mean = chunks.mean(axis=1, keepdims=True)
        centred = chunks - mean
        cumulative = centred.cumsum(axis=1)
        rng = cumulative.max(axis=1) - cumulative.min(axis=1)
        std = chunks.std(axis=1, ddof=1)
        # Avoid divide-by-zero on dead chunks.
        valid = std > 0
        if not valid.any():
            continue
        rs = (rng[valid] / std[valid]).mean()
        if rs > 0:
            rs_values.append(rs)

    if len(rs_values) < 3:
        return 0.5

    log_lags = np.log(lags[: len(rs_values)])
    log_rs = np.log(rs_values)
    # Slope of log(R/S) vs log(lag) is the Hurst exponent.
    slope, _ = np.polyfit(log_lags, log_rs, 1)
    return float(np.clip(slope, 0.0, 1.0))


def hurst_for_prices(prices: np.ndarray) -> HurstResult:
    h = compute_hurst(prices)
    cls, strat = _classify(h)
    return HurstResult(exponent=h, classification=cls, preferred_strategy=strat)
