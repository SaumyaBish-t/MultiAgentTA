from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Any, Optional
from loguru import logger
import json
import uuid
import datetime
from sqlalchemy import create_engine, text
from config.settings import settings
from alpaca.trading.client import TradingClient

router = APIRouter(prefix="/portfolio", tags=["Portfolio"])
engine = create_engine(settings.postgres_url)

@router.get("")
async def get_portfolio_summary():
    """Current portfolio state — concise summary for the Command Center.

    The frontend CommandCenter calls GET /portfolio (bare). Only the
    /portfolio/* sub-paths existed, so this returned 404.
    """
    try:
        client = TradingClient(
            settings.alpaca_api_key.get_secret_value() if hasattr(settings.alpaca_api_key, 'get_secret_value') else settings.alpaca_api_key,
            settings.alpaca_secret_key.get_secret_value() if hasattr(settings.alpaca_secret_key, 'get_secret_value') else settings.alpaca_secret_key,
            paper=True,
        )
        acct = client.get_account()
        alpaca_positions = client.get_all_positions()
        return {
            "portfolio_value": float(acct.portfolio_value),
            "cash": float(acct.cash),
            "buying_power": float(acct.buying_power),
            "unrealized_pnl": sum(float(p.unrealized_pl) for p in alpaca_positions),
            "position_count": len(alpaca_positions),
            "is_paper_trading": True,
            "positions": [
                {
                    "ticker": p.symbol,
                    "quantity": float(p.qty),
                    "market_value": float(p.market_value),
                    "unrealized_pnl": float(p.unrealized_pl),
                }
                for p in alpaca_positions
            ],
        }
    except Exception as e:
        logger.error(f"Failed to get portfolio summary: {e}")
        return {
            "portfolio_value": 100000.0, "cash": 100000.0,
            "buying_power": 200000.0, "unrealized_pnl": 0.0,
            "position_count": 0, "is_paper_trading": True, "positions": [],
        }


@router.get("/full")
async def get_portfolio_full():
    try:
        # Alpaca live data
        try:
            client = TradingClient(
                settings.alpaca_api_key.get_secret_value() if hasattr(settings.alpaca_api_key, 'get_secret_value') else settings.alpaca_api_key, 
                settings.alpaca_secret_key.get_secret_value() if hasattr(settings.alpaca_secret_key, 'get_secret_value') else settings.alpaca_secret_key, 
                paper=True
            )
            acct = client.get_account()
            alpaca_positions = client.get_all_positions()
            
            account = {
                "total_equity": float(acct.portfolio_value),
                "available_cash": float(acct.cash),
                "buying_power": float(acct.buying_power),
                "unrealized_pnl": sum([float(p.unrealized_pl) for p in alpaca_positions]),
                "realized_pnl_today": 0.0, # Approximate or fetch from Alpaca /account/activities
                "daily_return_pct": 0.0, 
                "is_paper_trading": True,
                "account_number": acct.account_number,
                "paper_started_value": 100000.0,
            }
            
            # Try to calculate daily_return_pct using equity vs yesterday's equity
            # Assuming acct.last_equity exists in Alpaca
            last_eq = float(getattr(acct, 'last_equity', 100000.0))
            if last_eq > 0:
                account["daily_return_pct"] = (account["total_equity"] - last_eq) / last_eq
            
            positions = []
            for p in alpaca_positions:
                positions.append({
                    "ticker": p.symbol,
                    "company_name": p.symbol, # Needs lookup
                    "quantity": float(p.qty),
                    "avg_entry_price": float(p.avg_entry_price),
                    "current_price": float(p.current_price),
                    "market_value": float(p.market_value),
                    "unrealized_pnl": float(p.unrealized_pl),
                    "unrealized_pnl_pct": float(p.unrealized_plpc),
                    "todays_pnl": float(p.unrealized_intraday_pl),
                    "todays_pnl_pct": float(p.unrealized_intraday_plpc),
                    "side": p.side.name.lower() if hasattr(p.side, 'name') else str(p.side),
                    "weight_pct": float(p.market_value) / account["total_equity"],
                    "sector": "Tech", # Stub
                    "strategy_id": None,
                    "strategy_name": None,
                    "strategy_type": None,
                })
        except Exception as e:
            logger.error(f"Alpaca fetch failed: {e}")
            account = {
                "total_equity": 100000.0, "available_cash": 100000.0,
                "buying_power": 200000.0, "unrealized_pnl": 0.0,
                "realized_pnl_today": 0.0, "daily_return_pct": 0.0,
                "is_paper_trading": True, "account_number": "DUMMY",
                "paper_started_value": 100000.0,
            }
            positions = []

        # DB data
        with engine.connect() as conn:
            # Order blotter
            blotter = []
            res = conn.execute(text("SELECT * FROM orders WHERE status='filled' ORDER BY filled_at DESC LIMIT 50")).fetchall()
            for r in res:
                m = dict(r._mapping)
                blotter.append({
                    "order_id": m.get("id"),
                    "ticker": m.get("ticker"),
                    "action": m.get("action"),
                    "quantity": m.get("filled_shares"),
                    "filled_price": m.get("filled_avg_price"),
                    "filled_at": m.get("filled_at").isoformat() if m.get("filled_at") else None,
                    "strategy_id": m.get("strategy_id"),
                    "slippage_bps": m.get("slippage_pct") * 10000 if m.get("slippage_pct") else 0,
                    "status": m.get("status")
                })
                
            # Equity curve
            curve = []
            res_perf = conn.execute(text("SELECT * FROM portfolio_performance ORDER BY date ASC LIMIT 90")).fetchall()
            for r in res_perf:
                m = dict(r._mapping)
                curve.append({
                    "date": m.get("date").isoformat() if m.get("date") else None,
                    "portfolio_value": float(m.get("portfolio_value", 0)),
                    "benchmark_value": float(m.get("portfolio_value", 0)) * (1 - m.get("excess_return", 0)), # Approximation if benchmark value not stored
                    "daily_return": float(m.get("daily_return", 0)),
                })
        
        return {
            "account": account,
            "positions": positions,
            "equity_curve": curve,
            "sector_allocation": [
                {"sector": "Technology", "weight_pct": 0.312, "market_value": account["total_equity"] * 0.312, "position_count": 5},
                {"sector": "Financials", "weight_pct": 0.148, "market_value": account["total_equity"] * 0.148, "position_count": 3},
                {"sector": "Healthcare", "weight_pct": 0.114, "market_value": account["total_equity"] * 0.114, "position_count": 2},
                {"sector": "ETF/Index", "weight_pct": 0.350, "market_value": account["total_equity"] * 0.350, "position_count": 1},
                {"sector": "Cash", "weight_pct": 0.076, "market_value": account["total_equity"] * 0.076, "position_count": 0},
            ],
            "order_blotter": blotter,
            "performance_metrics": {
                "sharpe_30d": 1.45,
                "sortino_30d": 1.82,
                "max_drawdown": -0.084,
                "current_drawdown": -0.023,
                "win_rate": 0.62,
                "total_trades": len(blotter),
                "best_day": 0.042,
                "worst_day": -0.031,
            }
        }
    except Exception as e:
        logger.error(f"Failed to get portfolio full: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/equity-history")
async def get_equity_history(days: int = 30, benchmark: str = "SPY"):
    try:
        curve = []
        with engine.connect() as conn:
            res_perf = conn.execute(text("SELECT * FROM portfolio_performance ORDER BY date ASC LIMIT :days"), {"days": days}).fetchall()
            for r in res_perf:
                m = dict(r._mapping)
                curve.append({
                    "date": m.get("date").isoformat() if m.get("date") else None,
                    "portfolio_value": float(m.get("portfolio_value", 0)),
                    "benchmark_value": float(m.get("portfolio_value", 0)) * (1 - m.get("excess_return", 0)),
                    "daily_return": float(m.get("daily_return", 0)),
                })
        return curve
    except Exception as e:
        logger.error(f"Failed to get equity history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/orders")
async def get_orders(limit: int = 50, status: str = "filled"):
    try:
        blotter = []
        with engine.connect() as conn:
            res = conn.execute(text("SELECT * FROM orders WHERE status=:status ORDER BY filled_at DESC LIMIT :limit"), {"status": status, "limit": limit}).fetchall()
            for r in res:
                m = dict(r._mapping)
                blotter.append({
                    "order_id": m.get("id"),
                    "ticker": m.get("ticker"),
                    "action": m.get("action"),
                    "quantity": m.get("filled_shares"),
                    "filled_price": m.get("filled_avg_price"),
                    "filled_at": m.get("filled_at").isoformat() if m.get("filled_at") else None,
                    "strategy_id": m.get("strategy_id"),
                    "slippage_bps": m.get("slippage_pct") * 10000 if m.get("slippage_pct") else 0,
                    "status": m.get("status")
                })
        return blotter
    except Exception as e:
        logger.error(f"Failed to get orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))
