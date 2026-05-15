import json
import uuid
import asyncio
from datetime import datetime, timezone
from typing import TypedDict, Any, Optional
from dataclasses import dataclass
import math

import httpx
import numpy as np
import pandas as pd
import redis
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from langgraph.graph import StateGraph, END

from config.settings import settings
import config.llm_config as llm_config
from risk_management.storage.risk_models import PositionLimit

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class PositionSizingState(TypedDict):
    signal: dict
    backtest_metrics: dict
    current_portfolio_value: float
    current_cash: float
    current_positions: list[dict]
    volatility_data: dict
    kelly_fraction: float
    volatility_scaled_size: float
    fixed_pct_size: float
    recommended_size_pct: float
    recommended_size_usd: float
    sizing_method_used: str
    size_adjustments: list[str]
    final_size_usd: float
    error: str | None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_portfolio_state_node(state: PositionSizingState) -> dict[str, Any]:
    """Fetch current portfolio state and signal backtest metrics."""
    # 1. Get current portfolio from Redis
    r = redis.from_url(settings.redis_url, decode_responses=True)
    portfolio_state = r.get("portfolio:current:state")
    
    if portfolio_state:
        try:
            pf_data = json.loads(portfolio_state)
            total_value = float(pf_data.get("total_value", 100_000.0))
            cash = float(pf_data.get("cash", 100_000.0))
            positions = pf_data.get("positions", [])
        except Exception as e:
            logger.warning(f"Error parsing portfolio state from Redis: {e}. Using defaults.")
            total_value, cash, positions = 100_000.0, 100_000.0, []
    else:
        total_value, cash, positions = 100_000.0, 100_000.0, []

    # 2. Fetch backtest metrics from DB
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    backtest_metrics = {}
    error = state.get("error")
    
    with Session() as session:
        # Assuming we can query the raw table directly if ORM mapping isn't imported here
        from signal_generation.storage.signal_models import BacktestResult
        signal_id = state["signal"].get("id")
        
        if signal_id:
            try:
                # Need to convert string ID back to UUID for DB lookup if it came as string
                uid = uuid.UUID(signal_id) if isinstance(signal_id, str) else signal_id
                stmt = select(BacktestResult).where(BacktestResult.signal_id == uid).order_by(BacktestResult.backtested_at.desc()).limit(1)
                result = session.execute(stmt).scalar_one_or_none()
                if result:
                    backtest_metrics = {
                        "win_rate": result.win_rate,
                        "avg_trade_return_pct": result.avg_trade_return_pct,
                        "worst_trade_pct": result.worst_trade_pct,
                        "sharpe_ratio": result.sharpe_ratio
                    }
                else:
                    logger.warning(f"No backtest metrics found for signal {signal_id}")
            except Exception as e:
                logger.error(f"Failed to fetch backtest metrics: {e}")
                error = f"Failed to fetch backtest metrics: {e}"
        
    return {
        "current_portfolio_value": total_value,
        "current_cash": cash,
        "current_positions": positions,
        "backtest_metrics": backtest_metrics,
        "error": error
    }

async def calculate_kelly_size_node(state: PositionSizingState) -> dict[str, Any]:
    """Calculate the Kelly Criterion position size."""
    if state.get("error"): return {}
    metrics = state["backtest_metrics"]
    if not metrics:
        return {"kelly_fraction": 0.0}
        
    win_rate = metrics.get('win_rate', 0.0)
    avg_win = metrics.get('avg_trade_return_pct', 0.0) / 100.0
    avg_loss = abs(metrics.get('worst_trade_pct', 0.0)) / 100.0
    
    if avg_loss == 0 or avg_win == 0:
        kelly = 0.0
    else:
        b = avg_win / avg_loss  # win/loss ratio
        kelly = (win_rate * b - (1 - win_rate)) / b
        
    # Prevent negative kelly
    kelly = max(0.0, kelly)
    
    # Use fractional Kelly (25%)
    fractional_kelly = kelly * 0.25
    
    # Convert to USD
    current_value = state["current_portfolio_value"]
    kelly_size_usd = fractional_kelly * current_value
    
    # Cap at 10% of portfolio
    kelly_size_usd = min(kelly_size_usd, current_value * 0.10)
    
    return {"kelly_fraction": fractional_kelly}

async def calculate_volatility_size_node(state: PositionSizingState) -> dict[str, Any]:
    """Calculate volatility-scaled position size."""
    if state.get("error"): return {}
    ticker = state["signal"].get("ticker")
    if not ticker:
        return {"volatility_scaled_size": 0.0}
        
    vol_size_usd = 0.0
    
    # Internal API key logic
    headers = {"x-api-key": settings.internal_api_key}
    
    async with httpx.AsyncClient() as client:
        try:
            url = f"http://localhost:8000/prices/{ticker}/history?days=60"
            resp = await client.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 2:
                    df = pd.DataFrame(data)
                    # Convert to numeric just in case
                    df['close'] = pd.to_numeric(df['close'])
                    returns = df['close'].pct_change().dropna()
                    if not returns.empty:
                        realized_vol = returns.std() * math.sqrt(252)
                        if realized_vol > 0:
                            target_vol_contribution = 0.01
                            vol_size_pct = target_vol_contribution / realized_vol
                            current_value = state["current_portfolio_value"]
                            vol_size_usd = vol_size_pct * current_value
                            # Cap at 15% of portfolio
                            vol_size_usd = min(vol_size_usd, current_value * 0.15)
        except Exception as e:
            logger.error(f"Error fetching price history for Volatility sizing: {e}")
            
    return {"volatility_scaled_size": vol_size_usd}

async def calculate_fixed_pct_size_node(state: PositionSizingState) -> dict[str, Any]:
    """Calculate simple fixed percentage size based on conviction."""
    if state.get("error"): return {}
    base_pct = 0.05  # 5% default
    
    # Try conviction_score, composite_score, or default to 0.6
    conviction = state["signal"].get("conviction_score", state["signal"].get("composite_score", 0.6))
    
    adjusted_pct = base_pct * (conviction / 0.6)
    
    # Cap at 8%
    fixed_pct = min(adjusted_pct, 0.08)
    fixed_size_usd = fixed_pct * state["current_portfolio_value"]
    
    return {"fixed_pct_size": fixed_size_usd}

async def select_and_adjust_size_node(state: PositionSizingState) -> dict[str, Any]:
    """Select the best sizing method and apply mandatory risk adjustments."""
    if state.get("error"): return {}
    
    sharpe = state["backtest_metrics"].get("sharpe_ratio", 0.0)
    current_value = state["current_portfolio_value"]
    
    # 1. Choose Sizing Method
    if sharpe >= 2.0 and state.get("kelly_fraction", 0) > 0:
        base_size_usd = state["kelly_fraction"] * current_value
        # Recalculate bounded kelly to match node 2 logic
        base_size_usd = min(base_size_usd, current_value * 0.10)
        method = "kelly"
    elif sharpe >= 1.0 and state.get("volatility_scaled_size", 0) > 0:
        base_size_usd = state["volatility_scaled_size"]
        method = "volatility_scaled"
    else:
        base_size_usd = state.get("fixed_pct_size", current_value * 0.05)
        method = "fixed_pct"
        
    adjustments = []
    final_size_usd = base_size_usd
    
    # ADJUSTMENT 1: Cash constraint
    cash = state["current_cash"]
    if cash < final_size_usd:
        final_size_usd = cash * 0.95
        adjustments.append("CASH_CONSTRAINED")
        
    # ADJUSTMENT 2: Concentration limit
    ticker = state["signal"].get("ticker")
    existing = [p for p in state["current_positions"] if p.get("ticker") == ticker]
    if existing:
        final_size_usd *= 0.5
        adjustments.append("EXISTING_POSITION_REDUCED")
        
    # ADJUSTMENT 3: Drawdown adjustment
    r = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        dd_str = r.get("portfolio:drawdown:current")
        current_dd = float(dd_str) if dd_str else 0.0
    except:
        current_dd = 0.0
        
    if current_dd < -0.08:
        final_size_usd *= 0.25
        adjustments.append("SEVERE_DRAWDOWN_REDUCTION")
    elif current_dd < -0.05:
        final_size_usd *= 0.5
        adjustments.append("DRAWDOWN_REDUCTION")
        
    # ADJUSTMENT 4: Volatility regime
    headers = {"x-api-key": settings.internal_api_key}
    vix_val = 0.0
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get("http://localhost:8000/macro/VIXCLS?limit=1", headers=headers, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    vix_val = float(data[0].get("value", 0.0))
        except Exception as e:
            logger.warning(f"Could not fetch VIX: {e}")
            
    if vix_val > 40:
        final_size_usd *= 0.3
        adjustments.append("EXTREME_VIX_REDUCTION")
    elif vix_val > 30:
        final_size_usd *= 0.7
        adjustments.append("HIGH_VIX_REDUCTION")
        
    # ADJUSTMENT 5: Minimum size
    if final_size_usd < 1000:
        final_size_usd = 0.0
        adjustments.append("BELOW_MINIMUM_SIZE")
        
    pct_size = final_size_usd / current_value if current_value > 0 else 0.0
    
    return {
        "recommended_size_usd": base_size_usd,
        "recommended_size_pct": base_size_usd / current_value if current_value > 0 else 0.0,
        "sizing_method_used": method,
        "size_adjustments": adjustments,
        "final_size_usd": final_size_usd
    }

async def store_position_limit_node(state: PositionSizingState) -> dict[str, Any]:
    """Store the finalized position limits to DB and Redis."""
    if state.get("error"): return {}
    if state.get("final_size_usd", 0) <= 0:
        return {} # Rejected or too small
        
    signal_id_str = state["signal"].get("id")
    if not signal_id_str: return {}
    
    uid = uuid.UUID(signal_id_str) if isinstance(signal_id_str, str) else signal_id_str
    ticker = state["signal"].get("ticker", "UNKNOWN")
    final_usd = state["final_size_usd"]
    final_pct = final_usd / state["current_portfolio_value"] if state["current_portfolio_value"] > 0 else 0.0
    method = state.get("sizing_method_used", "fixed_pct")
    kelly_frac = state.get("kelly_fraction") if method == "kelly" else None
    
    # 1. DB Save
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        try:
            limit = PositionLimit(
                signal_id=uid,
                ticker=ticker,
                max_position_size_pct=final_pct,
                max_position_size_usd=final_usd,
                sizing_method=method,
                kelly_fraction=kelly_frac,
                volatility_scalar=None,
                approved=True,
                approved_at=datetime.now(timezone.utc)
            )
            session.add(limit)
            session.commit()
        except Exception as e:
            logger.error(f"Failed to save PositionLimit to DB: {e}")
            return {"error": f"DB Store Error: {e}"}
            
    # 2. Redis Cache
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        cache_val = {
            "size_usd": final_usd,
            "size_pct": final_pct,
            "method": method,
            "adjustments": state.get("size_adjustments", [])
        }
        r.setex(f"risk:position_size:{signal_id_str}", 3600, json.dumps(cache_val))
    except Exception as e:
        logger.error(f"Failed to cache PositionLimit in Redis: {e}")
        
    return {}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GRAPH DEFINITION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def build_position_sizing_graph() -> StateGraph:
    workflow = StateGraph(PositionSizingState)
    
    workflow.add_node("fetch_portfolio_state_node", fetch_portfolio_state_node)
    workflow.add_node("calculate_kelly_size_node", calculate_kelly_size_node)
    workflow.add_node("calculate_volatility_size_node", calculate_volatility_size_node)
    workflow.add_node("calculate_fixed_pct_size_node", calculate_fixed_pct_size_node)
    workflow.add_node("select_and_adjust_size_node", select_and_adjust_size_node)
    workflow.add_node("store_position_limit_node", store_position_limit_node)
    
    workflow.set_entry_point("fetch_portfolio_state_node")
    workflow.add_edge("fetch_portfolio_state_node", "calculate_kelly_size_node")
    workflow.add_edge("calculate_kelly_size_node", "calculate_volatility_size_node")
    workflow.add_edge("calculate_volatility_size_node", "calculate_fixed_pct_size_node")
    workflow.add_edge("calculate_fixed_pct_size_node", "select_and_adjust_size_node")
    workflow.add_edge("select_and_adjust_size_node", "store_position_limit_node")
    workflow.add_edge("store_position_limit_node", END)
    
    return workflow.compile()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class PositionSize:
    signal_id: uuid.UUID
    ticker: str
    size_usd: float
    size_pct: float
    sizing_method: str
    kelly_fraction: float
    adjustments_applied: list[str]
    approved: bool

class PositionSizerAgent:
    """Public interface for Risk Management position sizing."""
    
    def __init__(self):
        self.graph = build_position_sizing_graph()
        
    async def size_position(self, signal: dict) -> Optional[PositionSize]:
        """Determine position size for a single signal."""
        initial_state: PositionSizingState = {
            "signal": signal,
            "backtest_metrics": {},
            "current_portfolio_value": 0.0,
            "current_cash": 0.0,
            "current_positions": [],
            "volatility_data": {},
            "kelly_fraction": 0.0,
            "volatility_scaled_size": 0.0,
            "fixed_pct_size": 0.0,
            "recommended_size_pct": 0.0,
            "recommended_size_usd": 0.0,
            "sizing_method_used": "",
            "size_adjustments": [],
            "final_size_usd": 0.0,
            "error": None
        }
        
        try:
            final_state = await self.graph.ainvoke(initial_state)
            
            if final_state.get("error") or final_state.get("final_size_usd", 0) <= 0:
                logger.info(f"Signal {signal.get('ticker')} rejected by PositionSizer: {final_state.get('size_adjustments', [])}")
                return None
                
            current_value = final_state.get("current_portfolio_value", 1.0)
            final_usd = final_state["final_size_usd"]
            
            return PositionSize(
                signal_id=uuid.UUID(str(signal.get("id"))) if signal.get("id") else uuid.uuid4(),
                ticker=signal.get("ticker", "UNKNOWN"),
                size_usd=final_usd,
                size_pct=final_usd / current_value if current_value > 0 else 0.0,
                sizing_method=final_state.get("sizing_method_used", "fixed_pct"),
                kelly_fraction=final_state.get("kelly_fraction", 0.0),
                adjustments_applied=final_state.get("size_adjustments", []),
                approved=True
            )
        except Exception as e:
            logger.error(f"Position sizing failed for {signal.get('ticker')}: {e}")
            return None

    async def size_portfolio(self, signals: list[dict]) -> list[PositionSize]:
        """Process a batch of signals and return approved sizes."""
        results = []
        for sig in signals:
            size = await self.size_position(sig)
            if size:
                results.append(size)
        return results
        
    def get_max_positions(self) -> int:
        """Helper to get theoretical max concurrent positions."""
        # Based on default minimum 5% size without adjustments
        return 20
        
    async def recalculate_all(self) -> list[PositionSize]:
        """Recalculate sizes for all currently active/validated signals."""
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        signals = []
        with Session() as session:
            from signal_generation.storage.signal_models import TradingSignal
            stmt = select(TradingSignal).where(TradingSignal.status == 'validated')
            records = session.execute(stmt).scalars().all()
            for r in records:
                signals.append({
                    "id": str(r.id),
                    "ticker": r.ticker,
                    "composite_score": getattr(r, 'composite_score', 0.6) # If missing, default applied later
                })
                
        return await self.size_portfolio(signals)
