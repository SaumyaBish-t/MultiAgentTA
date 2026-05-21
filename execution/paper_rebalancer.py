"""
execution.paper_rebalancer
==========================

Core logic for taking a strategy "live on paper": compute the rebalance
plan against the Alpaca paper account, and (optionally) execute it.

Used by both the dashboard's /live/* endpoints and the CLI script. All
trading is paper (paper=True) — no real money is ever at risk.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from config.settings import settings


def us_universe() -> list[str]:
    """Alpaca trades US equities only — drop the NSE/BSE tickers."""
    return [t for t in settings.tickers if not t.endswith((".NS", ".BSE"))]


def get_target_holdings(strategy: str) -> list[str]:
    """Run the strategy and return its current target holdings (US only)."""
    us = us_universe()
    if strategy == "momentum":
        from signal_generation.strategies.cross_sectional_momentum import (
            backtest_cross_sectional_momentum,
        )
        r = backtest_cross_sectional_momentum(tickers=us)
    elif strategy == "multi-factor":
        from signal_generation.strategies.multi_factor_cross_sectional import (
            backtest_multi_factor,
        )
        r = backtest_multi_factor(tickers=us)
    elif strategy == "trend-following":
        from signal_generation.strategies.portfolio_strategies import (
            backtest_trend_following,
        )
        r = backtest_trend_following(tickers=us)
    else:
        raise ValueError(f"unknown strategy: {strategy}")

    if r.error:
        raise RuntimeError(r.error)
    return [t for t in r.final_holdings if not t.endswith((".NS", ".BSE"))]


def _alpaca_client():
    from alpaca.trading.client import TradingClient
    return TradingClient(
        api_key=settings.alpaca_api_key.get_secret_value(),
        secret_key=settings.alpaca_secret_key.get_secret_value(),
        paper=True,  # INVARIANT — paper only
    )


def compute_rebalance_plan(strategy: str) -> dict[str, Any]:
    """Build the rebalance plan for a strategy vs the current paper account.
    Does NOT place any orders."""
    targets = get_target_holdings(strategy)

    try:
        client = _alpaca_client()
        account = client.get_account()
        raw_positions = client.get_all_positions()
    except Exception as exc:
        logger.warning("Alpaca read failed: {}", exc)
        return {"error": str(exc), "strategy": strategy}

    equity = float(account.equity)
    positions = {
        p.symbol: {
            "ticker": p.symbol,
            "qty": float(p.qty),
            "market_value": float(p.market_value),
            "unrealized_pl": float(p.unrealized_pl),
            "unrealized_pl_pct": float(p.unrealized_plpc) * 100,
        }
        for p in raw_positions
    }

    n = len(targets)
    notional_per = round(equity / n, 2) if n else 0.0

    return {
        "strategy": strategy,
        "equity": equity,
        "cash": float(account.cash),
        "is_paper": True,
        "targets": targets,
        "notional_per_name": notional_per,
        "positions": list(positions.values()),
        "to_sell": [s for s in positions if s not in targets],
        "to_buy": [s for s in targets if s not in positions],
        "retained": [s for s in targets if s in positions],
    }


def execute_rebalance(strategy: str) -> dict[str, Any]:
    """Compute the plan and submit the paper orders to reach it."""
    plan = compute_rebalance_plan(strategy)
    if plan.get("error"):
        return plan

    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    client = _alpaca_client()
    notional = plan["notional_per_name"]
    executed: list[dict[str, Any]] = []

    for sym in plan["to_sell"]:
        try:
            client.close_position(sym)
            executed.append({"action": "SELL", "ticker": sym, "status": "submitted"})
        except Exception as exc:
            executed.append({"action": "SELL", "ticker": sym, "status": f"failed: {exc}"})

    for sym in plan["to_buy"]:
        try:
            client.submit_order(MarketOrderRequest(
                symbol=sym, notional=notional,
                side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
            ))
            executed.append({
                "action": "BUY", "ticker": sym,
                "notional": notional, "status": "submitted",
            })
        except Exception as exc:
            executed.append({"action": "BUY", "ticker": sym, "status": f"failed: {exc}"})

    plan["executed"] = executed
    plan["order_count"] = len(executed)
    logger.info("Paper rebalance executed for {}: {} orders", strategy, len(executed))
    return plan
