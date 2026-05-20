"""
scripts/run_cross_sectional.py
==============================

Run the cross-sectional momentum strategy over the configured universe.

Usage
-----
$ python scripts/run_cross_sectional.py                 # defaults
$ python scripts/run_cross_sectional.py --top-n 6 --rebalance 21
$ python scripts/run_cross_sectional.py --market us     # US tickers only
$ python scripts/run_cross_sectional.py --market in     # NSE tickers only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings  # noqa: E402
from signal_generation.strategies.cross_sectional_momentum import (  # noqa: E402
    backtest_cross_sectional_momentum,
    optimize_cross_sectional_momentum,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-n", type=int, default=5)
    ap.add_argument("--lookback", type=int, default=126)
    ap.add_argument("--skip", type=int, default=21)
    ap.add_argument("--rebalance", type=int, default=21)
    ap.add_argument("--market", choices=["all", "us", "in"], default="all")
    ap.add_argument("--optimize", action="store_true",
                    help="grid-search params on a train window, validate out-of-sample")
    args = ap.parse_args()

    tickers = list(settings.tickers)
    if args.market == "us":
        tickers = [t for t in tickers if not t.endswith((".NS", ".BSE"))]
    elif args.market == "in":
        tickers = [t for t in tickers if t.endswith((".NS", ".BSE"))]

    if args.optimize:
        opt = optimize_cross_sectional_momentum(tickers=tickers)
        if opt.get("error"):
            print("optimisation failed:", opt["error"])
            return 1
        print()
        print(f"Walk-forward optimisation  (grid: {opt['grid_size']} combos, "
              f"train/test split @ {opt['split_date']})")
        print(f"  best params: {opt['best_params']}")
        tr = opt["train"]
        print(f"  TRAIN  (in-sample, optimistic): "
              f"return={tr['return_pct']:.2f}%  sharpe={tr['sharpe']:.2f}  grade={tr['grade']}")
        print("  TEST   (out-of-sample — the honest number):")
        print("  " + opt["test"].summary().replace("\n", "\n  "))
        return 0 if not opt["test"].error else 1

    result = backtest_cross_sectional_momentum(
        tickers=tickers,
        top_n=args.top_n,
        lookback=args.lookback,
        skip=args.skip,
        rebalance_days=args.rebalance,
    )
    print()
    print(result.summary())
    return 0 if not result.error else 1


if __name__ == "__main__":
    sys.exit(main())
