"""
monitoring.api.new_endpoints
============================

The new endpoints added in Step 10 of the upgrade plan, all collected
into a single APIRouter so the existing monitoring app can mount them
without being modified:

    from monitoring.api.new_endpoints import router as new_router
    from review_gate.api.review_endpoints import router as review_router
    app.include_router(new_router)
    app.include_router(review_router, prefix="/review", tags=["review"])

Endpoints
---------
GET  /meta/summary               cached meta-analysis from Redis
GET  /account/status             paper-account status from Alpaca
GET  /cross-sectional/run        run the cross-sectional momentum backtest
GET  /cross-sectional/optimize   walk-forward parameter optimisation
"""

from __future__ import annotations

import json
from typing import Any

import redis
from fastapi import APIRouter, HTTPException
from loguru import logger

from config.settings import settings


router = APIRouter()


@router.get("/meta/summary")
def meta_summary() -> dict[str, Any]:
    """Return the latest cached L11 meta-analysis summary."""
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        raw = r.get("meta:calibration_summary")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"redis: {exc}")
    if not raw:
        return {"status": "no_data", "hint": "run meta_pipeline first"}
    return json.loads(raw)


def _account_payload(cash: float, portfolio_value: float, **extra: Any) -> dict[str, Any]:
    """Build the /account/status response in the shape the frontend expects.

    The PaperTradingStatus React component reads ``cash_balance`` as a
    STRING (it calls ``.split`` on it), so EVERY return path must include
    it. Numeric fields are provided too for other consumers.
    """
    return {
        "mode": "PAPER TRADING",
        "explanation": "All trades are 100% simulated. No real money is at risk.",
        "cash_balance": f"${cash:,.2f} from Alpaca paper account",
        "portfolio_value": f"${portfolio_value:,.2f}",
        "is_real_money": False,
        "paper": True,
        "banner": "PAPER TRADING — all positions simulated",
        **extra,
    }


# ── Cross-sectional momentum strategy ────────────────────────────
def _filter_universe(market: str) -> list[str]:
    tickers = list(settings.tickers)
    if market == "us":
        return [t for t in tickers if not t.endswith((".NS", ".BSE"))]
    if market == "in":
        return [t for t in tickers if t.endswith((".NS", ".BSE"))]
    return tickers


@router.get("/cross-sectional/run")
def cross_sectional_run(
    market: str = "us",
    top_n: int = 5,
    lookback: int = 126,
    skip: int = 21,
    rebalance: int = 21,
) -> dict[str, Any]:
    """Run the cross-sectional momentum backtest over the chosen universe."""
    from signal_generation.strategies.cross_sectional_momentum import (
        backtest_cross_sectional_momentum,
    )
    r = backtest_cross_sectional_momentum(
        tickers=_filter_universe(market), top_n=top_n, lookback=lookback,
        skip=skip, rebalance_days=rebalance,
    )
    if r.error:
        return {"error": r.error}
    return {
        "grade": r.grade,
        "quality_score": r.quality_score,
        "passed": r.passed,
        "rejection_reasons": r.rejection_reasons,
        "metrics": r.metrics,
        "equity_curve": r.equity_curve,
        "final_holdings": r.final_holdings,
    }


@router.get("/cross-sectional/multi-factor")
def cross_sectional_multi_factor(
    market: str = "us",
    top_n: int = 5,
    rebalance: int = 21,
) -> dict[str, Any]:
    """Multi-factor cross-sectional backtest — blends momentum, risk-adjusted
    momentum, low-volatility and trend factors into the ranking."""
    from signal_generation.strategies.multi_factor_cross_sectional import (
        backtest_multi_factor,
    )
    r = backtest_multi_factor(
        tickers=_filter_universe(market), top_n=top_n, rebalance_days=rebalance,
    )
    if r.error:
        return {"error": r.error}
    return {
        "grade": r.grade,
        "quality_score": r.quality_score,
        "passed": r.passed,
        "rejection_reasons": r.rejection_reasons,
        "metrics": r.metrics,
        "equity_curve": r.equity_curve,
        "final_holdings": r.final_holdings,
    }


def _xs_result(r: Any) -> dict[str, Any]:
    """Serialise a CrossSectionalResult into the standard response shape."""
    if r.error:
        return {"error": r.error}
    return {
        "grade": r.grade,
        "quality_score": r.quality_score,
        "passed": r.passed,
        "rejection_reasons": r.rejection_reasons,
        "metrics": r.metrics,
        "equity_curve": r.equity_curve,
        "final_holdings": r.final_holdings,
    }


@router.get("/cross-sectional/trend-following")
def cross_sectional_trend_following(market: str = "us") -> dict[str, Any]:
    """Time-series momentum: hold every name in its own uptrend, equal weight."""
    from signal_generation.strategies.portfolio_strategies import backtest_trend_following
    return _xs_result(backtest_trend_following(tickers=_filter_universe(market)))


@router.get("/cross-sectional/risk-parity")
def cross_sectional_risk_parity(market: str = "us") -> dict[str, Any]:
    """Risk parity: hold the whole universe, weighted inversely to volatility."""
    from signal_generation.strategies.portfolio_strategies import backtest_risk_parity
    return _xs_result(backtest_risk_parity(tickers=_filter_universe(market)))


@router.get("/cross-sectional/optimize")
def cross_sectional_optimize(market: str = "us") -> dict[str, Any]:
    """Walk-forward parameter optimisation — grid-search on train, report test."""
    from signal_generation.strategies.cross_sectional_momentum import (
        optimize_cross_sectional_momentum,
    )
    opt = optimize_cross_sectional_momentum(tickers=_filter_universe(market))
    if opt.get("error"):
        return {"error": opt["error"]}
    test = opt["test"]
    return {
        "split_date": opt["split_date"],
        "grid_size": opt["grid_size"],
        "best_params": opt["best_params"],
        "train": opt["train"],
        "top_train": opt["top_train"],
        "test": {
            "grade": test.grade,
            "quality_score": test.quality_score,
            "passed": test.passed,
            "metrics": test.metrics,
            "equity_curve": test.equity_curve,
            "final_holdings": test.final_holdings,
        },
    }


@router.get("/account/status")
def account_status() -> dict[str, Any]:
    """Alpaca paper-account snapshot. Always returns a frontend-safe shape."""
    try:
        from alpaca.trading.client import TradingClient  # type: ignore[import-untyped]
    except Exception:
        return _account_payload(100000.0, 100000.0, source="fallback:alpaca-py-missing")

    try:
        client = TradingClient(
            api_key=settings.alpaca_api_key.get_secret_value(),
            secret_key=settings.alpaca_secret_key.get_secret_value(),
            paper=True,  # invariant — never change without explicit auth
        )
        account = client.get_account()
    except Exception as exc:
        logger.warning("Alpaca account fetch failed: {}", exc)
        return _account_payload(100000.0, 100000.0, source=f"fallback:{exc}")

    cash = float(getattr(account, "cash", 0) or 0)
    pv = float(getattr(account, "portfolio_value", 0) or 0)
    return _account_payload(
        cash, pv,
        source="alpaca",
        account_number=getattr(account, "account_number", None),
        status=getattr(account, "status", None),
        buying_power=float(getattr(account, "buying_power", 0) or 0),
        cash_usd=cash,
        equity=float(getattr(account, "equity", 0) or 0),
        pattern_day_trader=getattr(account, "pattern_day_trader", False),
    )
