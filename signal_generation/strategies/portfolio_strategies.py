"""
signal_generation.strategies.portfolio_strategies
=================================================

Two more historical-data strategies that plug into the same backtest
engine as the cross-sectional models:

* Trend-following (time-series momentum) — for EACH stock, judge it
  against its OWN past: hold an equal-weight basket of every name that
  is in its own uptrend, rebalanced monthly; sit in cash otherwise.
  This is the classic managed-futures / CTA approach.

* Risk parity — hold the WHOLE universe, but weight each name inversely
  to its volatility so every position contributes roughly equal risk.
  A portfolio-construction method, not an "edge" — but robust and fully
  backtestable.

Both are long-only and reuse one shared weighted-portfolio backtester.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from loguru import logger

from config.settings import settings
from signal_generation.grading import grade_metrics
from signal_generation.strategies.cross_sectional_momentum import (
    CrossSectionalResult,
    load_universe_prices,
)

_ETFS = ("SPY", "QQQ", "DIA", "IWM")


def _universe(tickers: list[str] | None) -> list[str]:
    return [t for t in (tickers or list(settings.tickers)) if t not in _ETFS]


def _backtest_from_weights(
    prices: pd.DataFrame,
    weight_mat: pd.DataFrame,
    first_day,
    fees: float = 0.001,
    slippage: float = 0.001,
) -> CrossSectionalResult:
    """Shared engine: take a forward-filled weight matrix and produce a
    graded CrossSectionalResult (returns, equity curve, metrics, trades)."""
    asset_rets = prices.pct_change().fillna(0.0)
    held = weight_mat.shift(1).fillna(0.0)
    gross_ret = (held * asset_rets).sum(axis=1)
    turnover = (weight_mat - weight_mat.shift(1)).abs().sum(axis=1)
    port_ret = (gross_ret - turnover * (fees + slippage)).fillna(0.0).loc[first_day:]

    equity = 100_000.0 * (1.0 + port_ret).cumprod()
    n = len(port_ret)
    total_return = float(equity.iloc[-1] / 100_000.0 - 1.0) * 100
    annualized = float((equity.iloc[-1] / 100_000.0) ** (252.0 / max(n, 1)) - 1.0) * 100
    vol_ann = float(port_ret.std() * np.sqrt(252)) * 100
    sharpe = float(port_ret.mean() / port_ret.std() * np.sqrt(252)) if port_ret.std() > 0 else 0.0
    downside = port_ret[port_ret < 0].std()
    sortino = float(port_ret.mean() / downside * np.sqrt(252)) if downside and downside > 0 else 0.0
    running_max = equity.cummax()
    max_dd = float(((equity - running_max) / running_max).min()) * 100

    bench_ret = asset_rets.loc[first_day:].mean(axis=1)
    benchmark_return = float((1.0 + bench_ret).cumprod().iloc[-1] - 1.0) * 100

    # Per-name trades (one contiguous holding run = one trade).
    trade_returns: list[float] = []
    for tk in weight_mat.columns:
        w = weight_mat[tk]
        in_pos, entry_px = False, None
        for day in w.index:
            holding = w.loc[day] > 0
            if holding and not in_pos:
                in_pos, entry_px = True, prices[tk].get(day, np.nan)
            elif not holding and in_pos:
                in_pos = False
                exit_px = prices[tk].get(day, np.nan)
                if entry_px and not np.isnan(entry_px) and not np.isnan(exit_px) and entry_px > 0:
                    trade_returns.append(exit_px / entry_px - 1.0)
        if in_pos and entry_px and entry_px > 0:
            trade_returns.append(prices[tk].iloc[-1] / entry_px - 1.0)

    wins = [r for r in trade_returns if r > 0]
    losses = [r for r in trade_returns if r <= 0]
    total_trades = len(trade_returns)
    win_rate = len(wins) / total_trades if total_trades else 0.0
    gross_loss = abs(sum(losses))
    profit_factor = (sum(wins) / gross_loss) if gross_loss > 0 else (sum(wins) or 0.0)

    metrics = {
        "total_return_pct": total_return,
        "annualized_return_pct": annualized,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown_pct": max_dd,
        "volatility_annualized": vol_ann,
        "total_trades": total_trades,
        "win_rate": win_rate,
        "profit_factor": float(profit_factor),
        "benchmark_return_pct": benchmark_return,
        "avg_trade_return_pct": float(np.mean(trade_returns) * 100) if trade_returns else 0.0,
        "universe_size": len(prices.columns),
    }
    g = grade_metrics(metrics)
    metrics["quality_score"] = g["quality_score"]
    metrics["quality_grade"] = g["quality_grade"]

    return CrossSectionalResult(
        metrics=metrics,
        grade=g["quality_grade"],
        quality_score=g["quality_score"],
        passed=g["passed"],
        rejection_reasons=g["rejection_reasons"],
        equity_curve=[{"date": str(d.date()), "value": float(v)} for d, v in equity.items()],
        final_holdings=[c for c in weight_mat.columns if weight_mat[c].iloc[-1] > 0],
    )


def backtest_trend_following(
    tickers: list[str] | None = None,
    lookback: int = 252,
    rebalance_days: int = 21,
    fees: float = 0.001,
    slippage: float = 0.001,
) -> CrossSectionalResult:
    """Time-series momentum: hold every name in its own uptrend, equal weight."""
    prices = load_universe_prices(_universe(tickers))
    if prices.empty or prices.shape[1] < 2:
        return CrossSectionalResult(error="insufficient universe price data")
    if len(prices) < lookback + rebalance_days + 5:
        return CrossSectionalResult(error="insufficient history for the lookback window")

    own_momentum = prices / prices.shift(lookback) - 1.0
    idx = prices.index
    rebal = list(range(lookback, len(idx), rebalance_days))
    if len(rebal) < 3:
        return CrossSectionalResult(error="not enough rebalance periods")

    weight_mat = pd.DataFrame(np.nan, index=idx, columns=prices.columns)
    first_day = None
    for pos in rebal:
        day = idx[pos]
        mom = own_momentum.iloc[pos]
        winners = mom[mom > 0].index.tolist()  # names in their own uptrend
        weight_mat.loc[day, :] = 0.0
        if winners:
            weight_mat.loc[day, winners] = 1.0 / len(winners)
        if first_day is None:
            first_day = day
    weight_mat = weight_mat.ffill().fillna(0.0)

    result = _backtest_from_weights(prices, weight_mat, first_day, fees, slippage)
    logger.info("Trend-following: grade={} return={:.2f}% sharpe={:.2f}",
                result.grade, result.metrics.get("total_return_pct", 0),
                result.metrics.get("sharpe_ratio", 0))
    return result


def backtest_risk_parity(
    tickers: list[str] | None = None,
    vol_window: int = 63,
    rebalance_days: int = 21,
    fees: float = 0.001,
    slippage: float = 0.001,
) -> CrossSectionalResult:
    """Risk parity: hold the whole universe, weight each name inversely to
    its volatility so every position contributes roughly equal risk."""
    prices = load_universe_prices(_universe(tickers))
    if prices.empty or prices.shape[1] < 2:
        return CrossSectionalResult(error="insufficient universe price data")
    if len(prices) < vol_window + rebalance_days + 5:
        return CrossSectionalResult(error="insufficient history for the volatility window")

    vol = prices.pct_change().rolling(vol_window).std()
    idx = prices.index
    rebal = list(range(vol_window, len(idx), rebalance_days))
    if len(rebal) < 3:
        return CrossSectionalResult(error="not enough rebalance periods")

    weight_mat = pd.DataFrame(np.nan, index=idx, columns=prices.columns)
    first_day = None
    for pos in rebal:
        day = idx[pos]
        v = vol.iloc[pos].replace(0.0, np.nan).dropna()
        if len(v) < 2:
            continue
        inv = 1.0 / v
        w = inv / inv.sum()                       # inverse-vol weights, sum to 1
        weight_mat.loc[day, :] = 0.0
        weight_mat.loc[day, w.index] = w.values
        if first_day is None:
            first_day = day
    if first_day is None:
        return CrossSectionalResult(error="no valid rebalance periods")
    weight_mat = weight_mat.ffill().fillna(0.0)

    result = _backtest_from_weights(prices, weight_mat, first_day, fees, slippage)
    logger.info("Risk parity: grade={} return={:.2f}% sharpe={:.2f}",
                result.grade, result.metrics.get("total_return_pct", 0),
                result.metrics.get("sharpe_ratio", 0))
    return result


if __name__ == "__main__":
    print("TREND-FOLLOWING:\n" + backtest_trend_following().summary())
    print("\nRISK PARITY:\n" + backtest_risk_parity().summary())
