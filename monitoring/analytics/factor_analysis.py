"""
monitoring.analytics.factor_analysis
====================================

Alphalens wrapper — evaluates whether agent scores predict forward
returns. Optional dependency (alphalens-reloaded).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from loguru import logger

try:
    import alphalens as al  # type: ignore[import-untyped]
    _HAS_AL = True
except Exception:  # pragma: no cover
    al = None  # type: ignore[assignment]
    _HAS_AL = False


def available() -> bool:
    return _HAS_AL


def factor_returns(
    factor: pd.Series,
    prices: pd.DataFrame,
    periods: tuple[int, ...] = (1, 5, 10, 20),
) -> dict[str, Any]:
    """Run Alphalens' standard tearsheet computation.

    Parameters
    ----------
    factor : MultiIndex(date, asset) -> factor value (e.g. agent score)
    prices : DataFrame indexed by date, columns = asset, values = close.
    """
    if not _HAS_AL:
        return {"skipped": "alphalens not installed"}

    try:
        factor_data = al.utils.get_clean_factor_and_forward_returns(
            factor=factor, prices=prices, periods=periods,
        )
    except Exception as exc:
        logger.warning("alphalens factor prep failed: {}", exc)
        return {"error": str(exc)}

    try:
        mean_ret, _ = al.performance.mean_return_by_quantile(factor_data)
        ic_summary = al.performance.factor_information_coefficient(factor_data).mean().to_dict()
    except Exception as exc:
        return {"error": f"perf calc failed: {exc}"}

    return {
        "mean_return_by_quantile": mean_ret.reset_index().to_dict(orient="records"),
        "ic_by_period": ic_summary,
    }
