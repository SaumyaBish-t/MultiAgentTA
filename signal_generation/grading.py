"""
signal_generation.grading
==========================

Single source of truth for strategy quality grading.

A strategy is graded A/B/C/D from its backtest metrics:

* HARD disqualifiers (always grade D): lost money, catastrophic
  drawdown, statistically meaningless trade count.
* SOFT criteria feed a 0-100 score: Sharpe, trade count, win rate,
  profit factor, drawdown depth, and excess return over the benchmark.
  Trailing the benchmark is a heavy penalty but not an outright
  disqualifier.

score >= 75 -> A   55-74 -> B   35-54 -> C   else -> D
A strategy "passes" at grade C or better with no hard disqualifier.
"""

from __future__ import annotations

from typing import Any


def grade_metrics(m: dict[str, Any]) -> dict[str, Any]:
    """Grade a strategy from a metrics dict.

    Expects (missing keys default to 0): sharpe_ratio, total_return_pct,
    benchmark_return_pct, max_drawdown_pct, total_trades, win_rate,
    profit_factor.
    """
    sharpe = m.get("sharpe_ratio") or 0.0
    ret = m.get("total_return_pct") or 0.0
    bench = m.get("benchmark_return_pct") or 0.0
    dd = m.get("max_drawdown_pct") or 0.0
    trades = m.get("total_trades") or 0
    win = m.get("win_rate") or 0.0
    pf = m.get("profit_factor") or 0.0
    excess = ret - bench

    hard_fail: list[str] = []
    if ret <= 0:
        hard_fail.append("NEGATIVE_RETURN")
    if dd < -40:
        hard_fail.append("CATASTROPHIC_DRAWDOWN")
    if trades < 5:
        hard_fail.append("TOO_FEW_TRADES")

    score = 0
    if sharpe >= 1.5:   score += 30
    elif sharpe >= 1.0: score += 24
    elif sharpe >= 0.5: score += 15
    elif sharpe >= 0.0: score += 6
    if trades >= 40:    score += 20
    elif trades >= 20:  score += 14
    elif trades >= 10:  score += 8
    elif trades >= 5:   score += 3
    if win >= 0.55:     score += 15
    elif win >= 0.45:   score += 10
    elif win >= 0.35:   score += 4
    if pf >= 1.8:       score += 20
    elif pf >= 1.4:     score += 14
    elif pf >= 1.1:     score += 7
    if dd >= -10:       score += 8
    elif dd >= -20:     score += 5
    elif dd >= -30:     score += 2
    if excess >= 15:    score += 8
    elif excess >= 5:   score += 5
    elif excess > 0:    score += 2
    elif excess > -10:  score -= 12
    else:               score -= 22
    score = max(0, score)

    if hard_fail:
        grade = "D"
    elif score >= 75:
        grade = "A"
    elif score >= 55:
        grade = "B"
    elif score >= 35:
        grade = "C"
    else:
        grade = "D"

    passed = (not hard_fail) and grade in ("A", "B", "C")
    reasons = list(hard_fail)
    if excess <= 0:
        reasons.append("UNDERPERFORMS_BENCHMARK")

    return {
        "quality_grade": grade,
        "quality_score": score,
        "passed": passed,
        "rejection_reasons": reasons,
    }
