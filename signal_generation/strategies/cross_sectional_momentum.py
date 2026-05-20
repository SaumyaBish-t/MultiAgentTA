"""
signal_generation.strategies.cross_sectional_momentum
=====================================================

Cross-sectional momentum — the most robust, most-documented systematic
equity strategy (Jegadeesh & Titman 1993; still works decades later).

Why this exists
---------------
Timing ONE stock long-only is where edge is thinnest and trades are
scarcest — a regime-filtered single-name strategy makes only a handful of
trades over two years, so it can never clear a statistical-significance
bar. Cross-sectional momentum sidesteps both problems:

* It ranks the WHOLE universe and holds an equal-weight basket of the
  top-N strongest names — diversification + a real, persistent edge.
* Rebalancing monthly across many names generates dozens of trades, so
  the result is statistically meaningful.

Signal
------
12-1 momentum: each rebalance, score every ticker by its trailing return
from ``t-lookback`` to ``t-skip``. The ``skip`` (default 1 month) drops
the most recent month to avoid the well-known short-term reversal effect.

The backtest is a transparent, vectorised pandas computation — no hidden
vectorbt portfolio machinery — so every number can be audited.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from loguru import logger

from config.settings import settings


# ── Data loading ────────────────────────────────────────────────
def load_universe_prices(tickers: list[str], days: int = 900) -> pd.DataFrame:
    """Return a (dates x tickers) daily-close matrix from TimescaleDB."""
    conn = psycopg2.connect(settings.timescale_url, connect_timeout=5)
    frames: dict[str, pd.Series] = {}
    try:
        with conn.cursor() as cur:
            for t in tickers:
                cur.execute(
                    """
                    SELECT timestamp::date, close FROM ohlcv_bars
                    WHERE ticker = %s AND timeframe = '1d'
                      AND timestamp >= NOW() - (%s * INTERVAL '1 day')
                    ORDER BY timestamp
                    """,
                    (t, days),
                )
                rows = cur.fetchall()
                if rows:
                    frames[t] = pd.Series({r[0]: float(r[1]) for r in rows})
    finally:
        conn.close()

    if not frames:
        return pd.DataFrame()
    df = pd.DataFrame(frames)
    df.index = pd.to_datetime(df.index)
    return df.sort_index().ffill().dropna(how="all")


# ── Grading (mirrors backtester apply_quality_filters_node) ──────
def _grade(metrics: dict[str, Any]) -> tuple[int, str, bool, list[str]]:
    sharpe = metrics.get("sharpe_ratio", 0.0)
    ret = metrics.get("total_return_pct", 0.0)
    bench = metrics.get("benchmark_return_pct", 0.0)
    dd = metrics.get("max_drawdown_pct", 0.0)
    trades = metrics.get("total_trades", 0)
    win = metrics.get("win_rate", 0.0)
    pf = metrics.get("profit_factor", 0.0)
    excess = ret - bench

    # Hard disqualifiers: genuinely bad (lost money / blew up / no trades).
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
    # Beating the benchmark is a strong reward; trailing it is a heavy
    # penalty — but NOT an outright disqualifier. A strategy that still
    # made real money is a weak candidate, not garbage.
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
        reasons.append("UNDERPERFORMS_BENCHMARK")   # informational, not a hard fail
    return score, grade, passed, reasons


# ── The backtest ────────────────────────────────────────────────
@dataclass
class CrossSectionalResult:
    metrics: dict[str, Any] = field(default_factory=dict)
    grade: str = "D"
    quality_score: int = 0
    passed: bool = False
    rejection_reasons: list[str] = field(default_factory=list)
    equity_curve: list[dict] = field(default_factory=list)
    final_holdings: list[str] = field(default_factory=list)
    error: str | None = None

    def summary(self) -> str:
        if self.error:
            return f"cross-sectional momentum FAILED: {self.error}"
        m = self.metrics
        return (
            f"Cross-sectional momentum  grade={self.grade} "
            f"(score {self.quality_score}, passed={self.passed})\n"
            f"  return={m.get('total_return_pct', 0):.2f}%  "
            f"benchmark={m.get('benchmark_return_pct', 0):.2f}%  "
            f"sharpe={m.get('sharpe_ratio', 0):.2f}  "
            f"max_dd={m.get('max_drawdown_pct', 0):.2f}%\n"
            f"  trades={m.get('total_trades', 0)}  "
            f"win_rate={m.get('win_rate', 0):.2f}  "
            f"profit_factor={m.get('profit_factor', 0):.2f}\n"
            f"  current holdings: {', '.join(self.final_holdings) or '-'}"
        )


def backtest_cross_sectional_momentum(
    tickers: list[str] | None = None,
    top_n: int = 5,
    lookback: int = 126,
    skip: int = 21,
    rebalance_days: int = 21,
    fees: float = 0.001,
    slippage: float = 0.001,
    start: str | None = None,
    end: str | None = None,
    prices: pd.DataFrame | None = None,
) -> CrossSectionalResult:
    """Backtest a long-only top-N cross-sectional momentum portfolio.

    Parameters
    ----------
    tickers        : universe; defaults to ``settings.tickers``.
    top_n          : how many of the strongest names to hold (equal weight).
    lookback       : momentum lookback in trading days (126 ≈ 6 months).
    skip           : days skipped at the recent end (21 ≈ 1 month).
    rebalance_days : trading days between rebalances (21 ≈ monthly).
    fees, slippage : per-unit-turnover cost charged at each rebalance.
    start, end     : optional evaluation window (ISO dates). Only rebalances
                     and returns inside it are scored; momentum still uses
                     ALL prior history. Used for walk-forward optimisation.
    prices         : optionally pass a pre-loaded price matrix (lets the
                     optimiser avoid reloading from the DB for every combo).
    """
    if prices is None:
        universe = tickers or list(settings.tickers)
        # ETFs make poor cross-sectional constituents — drop the obvious ones.
        universe = [t for t in universe if t not in ("SPY", "QQQ", "DIA", "IWM")]
        prices = load_universe_prices(universe)
    if prices.empty or prices.shape[1] < top_n + 1:
        return CrossSectionalResult(error="insufficient universe price data")
    if len(prices) < lookback + rebalance_days + 5:
        return CrossSectionalResult(error="insufficient history for the lookback window")

    # 12-1 momentum score for every ticker on every day.
    momentum = prices.shift(skip) / prices.shift(lookback) - 1.0

    # Rebalance dates: every `rebalance_days` rows once the lookback is warm.
    idx = prices.index
    rebal_positions = list(range(lookback + skip, len(idx), rebalance_days))
    # Optional evaluation window — momentum lookback still uses all prior
    # history; this only limits which rebalances/returns are scored.
    if start:
        _s = pd.Timestamp(start)
        rebal_positions = [p for p in rebal_positions if idx[p] >= _s]
    if end:
        _e = pd.Timestamp(end)
        rebal_positions = [p for p in rebal_positions if idx[p] <= _e]
    if len(rebal_positions) < 3:
        return CrossSectionalResult(error="not enough rebalance periods")

    # Build the target-weight matrix. Rebalance rows are set fully (0 for
    # non-winners, 1/top_n for winners); all other rows stay NaN so a
    # forward-fill carries each rebalance's weights until the next one.
    weights = pd.DataFrame(np.nan, index=idx, columns=prices.columns)
    holdings_log: list[tuple] = []
    for pos in rebal_positions:
        day = idx[pos]
        scores = momentum.iloc[pos].dropna()
        if len(scores) < top_n:
            continue
        winners = scores.sort_values(ascending=False).head(top_n).index.tolist()
        weights.loc[day, :] = 0.0
        weights.loc[day, winners] = 1.0 / top_n
        holdings_log.append((day, winners))

    if not holdings_log:
        return CrossSectionalResult(error="no valid rebalance periods produced holdings")

    # Hold weights constant between rebalances.
    weights = weights.ffill().fillna(0.0)

    # ── Portfolio returns (no look-ahead: yesterday's weights × today's return) ──
    asset_rets = prices.pct_change().fillna(0.0)
    held = weights.shift(1).fillna(0.0)
    gross_ret = (held * asset_rets).sum(axis=1)

    # Turnover cost charged on rebalance days.
    turnover = (weights - weights.shift(1)).abs().sum(axis=1)
    cost = turnover * (fees + slippage)
    port_ret = (gross_ret - cost).fillna(0.0)

    # Trim to the evaluation window (first in-window rebalance .. end).
    first_day = holdings_log[0][0]
    last_day = idx[-1] if not end else min(idx[-1], pd.Timestamp(end))
    port_ret = port_ret.loc[first_day:last_day]
    equity = 100_000.0 * (1.0 + port_ret).cumprod()

    # ── Metrics ─────────────────────────────────────────────────
    n = len(port_ret)
    total_return = float(equity.iloc[-1] / 100_000.0 - 1.0) * 100
    ann_factor = 252.0 / max(n, 1)
    annualized = float((equity.iloc[-1] / 100_000.0) ** ann_factor - 1.0) * 100
    vol = float(port_ret.std() * np.sqrt(252)) * 100
    sharpe = float(port_ret.mean() / port_ret.std() * np.sqrt(252)) if port_ret.std() > 0 else 0.0
    downside = port_ret[port_ret < 0].std()
    sortino = float(port_ret.mean() / downside * np.sqrt(252)) if downside and downside > 0 else 0.0
    running_max = equity.cummax()
    drawdown = (equity - running_max) / running_max
    max_dd = float(drawdown.min()) * 100

    # Benchmark: equal-weight buy-and-hold of the whole universe.
    bench_ret = asset_rets.loc[first_day:last_day].mean(axis=1)
    bench_equity = (1.0 + bench_ret).cumprod()
    benchmark_return = float(bench_equity.iloc[-1] - 1.0) * 100

    # ── Per-name trades (a "trade" = one contiguous holding run) ──
    eval_weights = weights.loc[first_day:last_day]
    trade_returns: list[float] = []
    for tk in eval_weights.columns:
        w = eval_weights[tk]
        in_pos = False
        entry_px = None
        for day in w.index:
            holding = w.loc[day] > 0
            if holding and not in_pos:
                in_pos = True
                entry_px = prices[tk].get(day, np.nan)
            elif not holding and in_pos:
                in_pos = False
                exit_px = prices[tk].get(day, np.nan)
                if entry_px and not np.isnan(entry_px) and not np.isnan(exit_px) and entry_px > 0:
                    trade_returns.append(exit_px / entry_px - 1.0)
        if in_pos and entry_px and entry_px > 0:  # close any open position at window end
            last_px = float(prices[tk].loc[:last_day].iloc[-1])
            trade_returns.append(last_px / entry_px - 1.0)

    wins = [r for r in trade_returns if r > 0]
    losses = [r for r in trade_returns if r <= 0]
    total_trades = len(trade_returns)
    win_rate = len(wins) / total_trades if total_trades else 0.0
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else (gross_win if gross_win else 0.0)

    metrics = {
        "total_return_pct": total_return,
        "annualized_return_pct": annualized,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown_pct": max_dd,
        "volatility_annualized": vol,
        "total_trades": total_trades,
        "win_rate": win_rate,
        "profit_factor": float(profit_factor),
        "benchmark_return_pct": benchmark_return,
        "avg_trade_return_pct": float(np.mean(trade_returns) * 100) if trade_returns else 0.0,
        "rebalances": len(holdings_log),
        "universe_size": len(prices.columns),
        "top_n": top_n,
    }

    score, grade, passed, hard_fail = _grade(metrics)
    metrics["quality_score"] = score
    metrics["quality_grade"] = grade

    result = CrossSectionalResult(
        metrics=metrics,
        grade=grade,
        quality_score=score,
        passed=passed,
        rejection_reasons=hard_fail,
        equity_curve=[{"date": str(d.date()), "value": float(v)} for d, v in equity.items()],
        final_holdings=list(holdings_log[-1][1]) if holdings_log else [],
    )
    logger.info(result.summary())
    return result


# ── Walk-forward parameter optimisation ─────────────────────────
def optimize_cross_sectional_momentum(
    tickers: list[str] | None = None,
    train_frac: float = 0.6,
) -> dict[str, Any]:
    """Grid-search cross-sectional momentum params, validated out-of-sample.

    The grid is searched on a TRAIN window; the best set (by train Sharpe)
    is then evaluated on a held-out TEST window. The TEST result is the
    honest one — train numbers are always optimistic because the params
    were fitted to them. This is the overfitting guard the research insists
    on: a parameter set only counts if it survives data it never saw.
    """
    universe = tickers or list(settings.tickers)
    universe = [t for t in universe if t not in ("SPY", "QQQ", "DIA", "IWM")]
    prices = load_universe_prices(universe)
    if prices.empty or len(prices) < 250:
        return {"error": "insufficient data for optimisation"}

    dates = prices.index
    split = dates[int(len(dates) * train_frac)]
    split_str = str(split.date())

    grid = [
        (top_n, lookback, skip, rebalance)
        for top_n in (3, 5, 8)
        for lookback in (63, 126, 252)
        for skip in (0, 21)
        for rebalance in (21, 42)
    ]

    train_results: list[dict] = []
    for top_n, lookback, skip, rebalance in grid:
        r = backtest_cross_sectional_momentum(
            top_n=top_n, lookback=lookback, skip=skip, rebalance_days=rebalance,
            end=split_str, prices=prices,
        )
        if r.error:
            continue
        train_results.append({
            "params": {"top_n": top_n, "lookback": lookback,
                       "skip": skip, "rebalance_days": rebalance},
            "sharpe": r.metrics["sharpe_ratio"],
            "return_pct": r.metrics["total_return_pct"],
            "grade": r.grade,
        })

    if not train_results:
        return {"error": "no valid parameter combination on the train window"}

    # Best by train Sharpe; tie-break on train return.
    train_results.sort(key=lambda x: (x["sharpe"], x["return_pct"]), reverse=True)
    best = train_results[0]

    # Honest out-of-sample evaluation on the held-out test window.
    test = backtest_cross_sectional_momentum(
        start=split_str, prices=prices, **best["params"],
    )

    return {
        "split_date": split_str,
        "grid_size": len(grid),
        "best_params": best["params"],
        "train": {k: best[k] for k in ("sharpe", "return_pct", "grade")},
        "test": test,
        "top_train": train_results[:5],
    }


if __name__ == "__main__":
    print(backtest_cross_sectional_momentum().summary())
