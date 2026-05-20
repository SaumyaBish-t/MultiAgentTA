"""
monitoring.analytics.tearsheet_generator
========================================

PyFolio wrapper — produce weekly performance tearsheets (HTML).
Optional dependency (pyfolio-reloaded).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

try:
    import pyfolio as pf  # type: ignore[import-untyped]
    _HAS_PF = True
except Exception:  # pragma: no cover
    pf = None  # type: ignore[assignment]
    _HAS_PF = False


def available() -> bool:
    return _HAS_PF


def generate_tearsheet(
    returns: pd.Series, output_path: str, benchmark_returns: pd.Series | None = None,
) -> dict[str, Any]:
    if not _HAS_PF:
        return {"skipped": "pyfolio not installed"}

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        # PyFolio's interactive tearsheet prints to stdout/matplotlib;
        # for HTML output we render perf stats + simple chart info.
        stats = pf.timeseries.perf_stats(returns).to_dict()
        html = ["<html><body><h1>PyFolio Performance Tearsheet</h1><ul>"]
        for k, v in stats.items():
            html.append(f"<li><b>{k}:</b> {v}</li>")
        html.append("</ul></body></html>")
        out.write_text("\n".join(html), encoding="utf-8")
        return {"written": str(out), "stats": stats}
    except Exception as exc:
        logger.warning("tearsheet generation failed: {}", exc)
        return {"error": str(exc)}
