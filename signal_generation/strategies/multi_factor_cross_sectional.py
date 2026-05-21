"""
signal_generation.strategies.multi_factor_cross_sectional
=========================================================

Multi-factor cross-sectional equity strategy — the professional
paradigm: rank the whole universe by a BLEND of factors, hold an
equal-weight basket of the top-N, rebalance monthly.

This is what real systematic-equity funds run. A single stock has no
edge and too few trades; a *ranked basket* refreshed across many names
diversifies away idiosyncratic risk and trades enough to be
statistically meaningful.

Factors
-------
All four are derived from daily price history, so — unlike LLM agent
scores — they have the history needed to BACKTEST honestly. Each is
z-scored across the universe at every rebalance, then blended:

* momentum           12-1 return (trend persistence; Jegadeesh-Titman)
* risk_adj_momentum  momentum / volatility (return per unit of risk)
* low_volatility     the low-volatility anomaly (calmer names win risk-adj)
* trend              price relative to its 200-day average

The per-stock research agents (sentiment, fundamentals, …) can tilt the
*live* picks as additional factors, but only price-derived factors have
the history to backtest — so the backtest uses these four.
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

# Default factor blend — momentum-led, which is the most robust single
# factor, with risk-adjustment and a low-vol / trend overlay.
DEFAULT_WEIGHTS: dict[str, float] = {
    "momentum": 0.35,
    "risk_adj_momentum": 0.25,
    "low_volatility": 0.20,
    "trend": 0.20,
}


def _zscore_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Cross-sectional z-score: each row (date) standardised across columns
    (tickers). Clipped to +/-3 to stop one outlier dominating the blend."""
    clean = df.replace([np.inf, -np.inf], np.nan)
    mu = clean.mean(axis=1)
    sd = clean.std(axis=1).replace(0.0, np.nan)
    z = clean.sub(mu, axis=0).div(sd, axis=0)
    return z.clip(-3.0, 3.0).fillna(0.0)


def compute_composite(
    prices: pd.DataFrame,
    weights: dict[str, float],
    lookback: int = 126,
    skip: int = 21,
    vol_window: int = 63,
    trend_window: int = 200,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Return (composite score matrix, individual factor matrices).

    Higher composite = more attractive. All matrices are dates x tickers.
    """
    rets = prices.pct_change()

    momentum = prices.shift(skip) / prices.shift(lookback) - 1.0
    vol = rets.rolling(vol_window).std() * np.sqrt(252)
    risk_adj = momentum / vol.replace(0.0, np.nan)
    low_vol = -vol  # lower volatility -> higher score
    trend = prices / prices.rolling(trend_window, min_periods=1).mean() - 1.0

    factors = {
        "momentum": momentum,
        "risk_adj_momentum": risk_adj,
        "low_volatility": low_vol,
        "trend": trend,
    }

    composite = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)
    for name, fdf in factors.items():
        w = weights.get(name, 0.0)
        if w:
            composite = composite + w * _zscore_rows(fdf)
    return composite, factors


def backtest_multi_factor(
    tickers: list[str] | None = None,
    top_n: int = 5,
    rebalance_days: int = 21,
    lookback: int = 126,
    skip: int = 21,
    vol_window: int = 63,
    trend_window: int = 200,
    weights: dict[str, float] | None = None,
    fees: float = 0.001,
    slippage: float = 0.001,
) -> CrossSectionalResult:
    """Backtest a long-only top-N multi-factor cross-sectional portfolio."""
    weights = weights or DEFAULT_WEIGHTS
    universe = tickers or list(settings.tickers)
    universe = [t for t in universe if t not in ("SPY", "QQQ", "DIA", "IWM")]

    prices = load_universe_prices(universe)
    if prices.empty or prices.shape[1] < top_n + 1:
        return CrossSectionalResult(error="insufficient universe price data")

    warmup = max(trend_window, lookback + skip)
    if len(prices) < warmup + rebalance_days + 5:
        return CrossSectionalResult(error="insufficient history for the factor windows")

    composite, _factors = compute_composite(
        prices, weights, lookback, skip, vol_window, trend_window
    )

    idx = prices.index
    rebal_positions = list(range(warmup, len(idx), rebalance_days))
    if len(rebal_positions) < 3:
        return CrossSectionalResult(error="not enough rebalance periods")

    # Target-weight matrix: rebalance rows fully set, others NaN -> ffill.
    weight_mat = pd.DataFrame(np.nan, index=idx, columns=prices.columns)
    holdings_log: list[tuple] = []
    for pos in rebal_positions:
        day = idx[pos]
        scores = composite.iloc[pos].replace([np.inf, -np.inf], np.nan).dropna()
        # require a real momentum reading too (avoid ranking on warmup noise)
        scores = scores[scores != 0.0]
        if len(scores) < top_n:
            continue
        winners = scores.sort_values(ascending=False).head(top_n).index.tolist()
        weight_mat.loc[day, :] = 0.0
        weight_mat.loc[day, winners] = 1.0 / top_n
        holdings_log.append((day, winners))

    if not holdings_log:
        return CrossSectionalResult(error="no valid rebalance periods produced holdings")

    weight_mat = weight_mat.ffill().fillna(0.0)

    asset_rets = prices.pct_change().fillna(0.0)
    held = weight_mat.shift(1).fillna(0.0)
    gross_ret = (held * asset_rets).sum(axis=1)
    turnover = (weight_mat - weight_mat.shift(1)).abs().sum(axis=1)
    cost = turnover * (fees + slippage)
    port_ret = (gross_ret - cost).fillna(0.0)

    first_day = holdings_log[0][0]
    port_ret = port_ret.loc[first_day:]
    equity = 100_000.0 * (1.0 + port_ret).cumprod()

    n = len(port_ret)
    total_return = float(equity.iloc[-1] / 100_000.0 - 1.0) * 100
    ann_factor = 252.0 / max(n, 1)
    annualized = float((equity.iloc[-1] / 100_000.0) ** ann_factor - 1.0) * 100
    vol_ann = float(port_ret.std() * np.sqrt(252)) * 100
    sharpe = (
        float(port_ret.mean() / port_ret.std() * np.sqrt(252))
        if port_ret.std() > 0 else 0.0
    )
    downside = port_ret[port_ret < 0].std()
    sortino = (
        float(port_ret.mean() / downside * np.sqrt(252))
        if downside and downside > 0 else 0.0
    )
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
        "rebalances": len(holdings_log),
        "universe_size": len(prices.columns),
        "top_n": top_n,
        "factor_weights": weights,
    }

    g = grade_metrics(metrics)
    metrics["quality_score"] = g["quality_score"]
    metrics["quality_grade"] = g["quality_grade"]

    result = CrossSectionalResult(
        metrics=metrics,
        grade=g["quality_grade"],
        quality_score=g["quality_score"],
        passed=g["passed"],
        rejection_reasons=g["rejection_reasons"],
        equity_curve=[{"date": str(d.date()), "value": float(v)} for d, v in equity.items()],
        final_holdings=list(holdings_log[-1][1]) if holdings_log else [],
    )
    logger.info(
        "Multi-factor cross-sectional: grade={} return={:.2f}% sharpe={:.2f} trades={}",
        result.grade, total_return, sharpe, total_trades,
    )
    return result


if __name__ == "__main__":
    print(backtest_multi_factor().summary())
