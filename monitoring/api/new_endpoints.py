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
GET  /meta/summary            cached meta-analysis from Redis
GET  /account/status          paper-account status from Alpaca
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


@router.get("/account/status")
def account_status() -> dict[str, Any]:
    """Alpaca paper-account snapshot with an explicit paper-trading banner."""
    try:
        from alpaca.trading.client import TradingClient  # type: ignore[import-untyped]
    except Exception:
        return {"error": "alpaca-py not installed", "paper": True}

    try:
        client = TradingClient(
            api_key=settings.alpaca_api_key.get_secret_value(),
            secret_key=settings.alpaca_secret_key.get_secret_value(),
            paper=True,  # invariant — never change without explicit auth
        )
        account = client.get_account()
    except Exception as exc:
        logger.warning("Alpaca account fetch failed: {}", exc)
        return {"error": str(exc), "paper": True}

    return {
        "paper": True,
        "banner": "PAPER TRADING — all positions simulated",
        "account_number": getattr(account, "account_number", None),
        "status": getattr(account, "status", None),
        "buying_power": float(getattr(account, "buying_power", 0) or 0),
        "cash": float(getattr(account, "cash", 0) or 0),
        "equity": float(getattr(account, "equity", 0) or 0),
        "portfolio_value": float(getattr(account, "portfolio_value", 0) or 0),
        "pattern_day_trader": getattr(account, "pattern_day_trader", False),
    }
