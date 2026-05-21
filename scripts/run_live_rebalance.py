"""
scripts/run_live_rebalance.py
=============================

Rebalance the Alpaca **paper** account to a strategy's current targets —
this is how you take a backtested strategy "live on paper".

What it does
------------
1. Runs the chosen strategy to get its CURRENT target holdings (the most
   recent rebalance's picks — not a backtest of the past).
2. Reads your Alpaca paper account (equity + open positions).
3. Builds a rebalance plan: sell what's no longer a target, buy the new
   targets equal-weight (notional = equity / N).
4. DRY RUN by default — prints the plan and places nothing.
   Pass --execute to actually submit the paper orders.

Usage
-----
$ python scripts/run_live_rebalance.py --strategy trend-following
$ python scripts/run_live_rebalance.py --strategy multi-factor --execute

Run it once per rebalance period (monthly). Alpaca trades US equities
only, so this is US-universe only.

Everything is paper (paper=True) — no real money is ever at risk.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loguru import logger  # noqa: E402

from config.settings import settings  # noqa: E402


def _us_universe() -> list[str]:
    return [t for t in settings.tickers if not t.endswith((".NS", ".BSE"))]


def get_target_holdings(strategy: str) -> list[str]:
    """Run the strategy and return its current target holdings (US only)."""
    us = _us_universe()
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
        raise RuntimeError(f"strategy backtest failed: {r.error}")
    # only tradeable US equities
    return [t for t in r.final_holdings if not t.endswith((".NS", ".BSE"))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--strategy",
        choices=["momentum", "multi-factor", "trend-following"],
        default="trend-following",
    )
    ap.add_argument(
        "--execute", action="store_true",
        help="actually submit the paper orders (default is a dry run)",
    )
    args = ap.parse_args()

    print(f"\n=== LIVE PAPER REBALANCE — strategy: {args.strategy} ===")

    # 1. target holdings
    targets = get_target_holdings(args.strategy)
    if not targets:
        print("No target holdings produced — nothing to do.")
        return 0
    print(f"Target portfolio ({len(targets)} names, equal weight): {', '.join(targets)}")

    # 2. Alpaca paper account
    try:
        from alpaca.trading.client import TradingClient
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
    except Exception:
        print("alpaca-py is not installed — cannot place paper orders.")
        return 1

    client = TradingClient(
        api_key=settings.alpaca_api_key.get_secret_value(),
        secret_key=settings.alpaca_secret_key.get_secret_value(),
        paper=True,  # INVARIANT — paper only
    )
    account = client.get_account()
    equity = float(account.equity)
    positions = {p.symbol: p for p in client.get_all_positions()}
    print(f"Paper account equity: ${equity:,.2f} | open positions: "
          f"{', '.join(positions) or 'none'}")

    # 3. rebalance plan
    notional_per = round(equity / len(targets), 2)
    to_sell = [s for s in positions if s not in targets]
    to_buy = [s for s in targets if s not in positions]
    retained = [s for s in targets if s in positions]

    print("\n--- REBALANCE PLAN ---")
    print(f"  SELL (no longer a target): {', '.join(to_sell) or 'none'}")
    print(f"  BUY  (new targets, ~${notional_per:,.2f} each): {', '.join(to_buy) or 'none'}")
    print(f"  HOLD (already a target):   {', '.join(retained) or 'none'}")

    if not args.execute:
        print("\nDRY RUN — no orders placed. Re-run with --execute to submit them.")
        return 0

    # 4. execute (paper)
    print("\n--- SUBMITTING PAPER ORDERS ---")
    for sym in to_sell:
        try:
            client.close_position(sym)
            print(f"  closed {sym}")
        except Exception as e:
            logger.warning(f"close {sym} failed: {e}")
    for sym in to_buy:
        try:
            client.submit_order(MarketOrderRequest(
                symbol=sym, notional=notional_per,
                side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
            ))
            print(f"  bought {sym}  ~${notional_per:,.2f}")
        except Exception as e:
            logger.warning(f"buy {sym} failed: {e}")

    print("\nPaper orders submitted. Markets-closed orders queue for the next open.")
    print("Re-run this at your next rebalance (monthly). Track performance on the dashboard.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
