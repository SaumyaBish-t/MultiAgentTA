import json
import uuid
import math
import asyncio
from datetime import datetime
from typing import TypedDict, Any, Optional
from dataclasses import dataclass

import httpx
import numpy as np
import pandas as pd
import redis
from loguru import logger
from langgraph.graph import StateGraph, END

from config.settings import settings

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class LiquidityState(TypedDict):
    signal: dict
    ticker: str
    position_size_usd: float
    avg_daily_volume: float
    avg_daily_volume_usd: float
    volume_std: float          # added from node 1 logic
    current_price: float       # needed for shares calc
    daily_returns_std: float   # needed for impact calc
    bid_ask_spread_pct: float
    days_to_exit: float
    market_impact_pct: float
    total_transaction_cost_pct: float
    liquidity_score: float     # 0-1, higher = more liquid
    liquidity_tier: str        # high/medium/low/illiquid
    max_safe_position_usd: float
    approved: bool
    rejection_reason: str | None
    notes: list[str]           # to capture "SIZE_REDUCED_FOR_LIQUIDITY" etc
    error: str | None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_volume_data_node(state: LiquidityState) -> dict[str, Any]:
    """Fetch 30-day volume and price data from the Data Ingestion API."""
    if state.get("error"): return {}
    
    ticker = state.get("ticker")
    if not ticker:
        return {"error": "No ticker provided"}
        
    headers = {"x-api-key": settings.internal_api_key}
    
    async with httpx.AsyncClient() as client:
        try:
            url = f"http://localhost:8000/prices/{ticker}/history?days=30"
            resp = await client.get(url, headers=headers, timeout=10.0)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) < 2:
                    return {"error": "Insufficient history for liquidity calculation"}
                    
                df = pd.DataFrame(data)
                df['close'] = pd.to_numeric(df['close'])
                df['volume'] = pd.to_numeric(df['volume'])
                
                avg_vol = df['volume'].mean()
                avg_close = df['close'].mean()
                avg_vol_usd = avg_vol * avg_close
                vol_std = df['volume'].std()
                
                current_price = df.iloc[-1]['close']
                returns_std = df['close'].pct_change().dropna().std()
                
                return {
                    "avg_daily_volume": float(avg_vol),
                    "avg_daily_volume_usd": float(avg_vol_usd),
                    "volume_std": float(vol_std),
                    "current_price": float(current_price),
                    "daily_returns_std": float(returns_std)
                }
            else:
                return {"error": f"Failed to fetch price history: {resp.status_code}"}
        except Exception as e:
            logger.error(f"Error fetching data for liquidity: {e}")
            return {"error": str(e)}

async def estimate_market_impact_node(state: LiquidityState) -> dict[str, Any]:
    """Estimate transaction cost, days to exit, and market impact."""
    if state.get("error"): return {}
    
    position_usd = state.get("position_size_usd", 0.0)
    avg_vol = state.get("avg_daily_volume", 0.0)
    current_px = state.get("current_price", 1.0)
    returns_std = state.get("daily_returns_std", 0.02) # default to 2% daily vol if 0
    if returns_std == 0: returns_std = 0.02
    
    if current_px <= 0 or avg_vol <= 0:
        return {"error": "Invalid price or volume metrics"}
        
    # Participation limits
    max_daily_participation = avg_vol * 0.10
    max_safe_position_usd = max_daily_participation * current_px
    
    # Exit metrics
    shares_to_trade = position_usd / current_px
    days_to_exit = shares_to_trade / max_daily_participation if max_daily_participation > 0 else float('inf')
    
    # Market impact estimate (simplified square root model)
    participation_rate = shares_to_trade / avg_vol if avg_vol > 0 else 1.0
    # Cap participation rate in the formula to avoid unreasonable theoretical impacts
    participation_rate = min(participation_rate, 1.0) 
    
    market_impact_pct = 0.1 * returns_std * math.sqrt(participation_rate)
    
    # Bid-ask spread approximation
    if current_px > 100:
        spread_pct = 0.001
    elif current_px > 10:
        spread_pct = 0.002
    else:
        spread_pct = 0.005
        
    total_transaction_cost_pct = market_impact_pct + spread_pct
    
    return {
        "max_safe_position_usd": float(max_safe_position_usd),
        "days_to_exit": float(days_to_exit),
        "market_impact_pct": float(market_impact_pct),
        "bid_ask_spread_pct": float(spread_pct),
        "total_transaction_cost_pct": float(total_transaction_cost_pct)
    }

async def score_liquidity_node(state: LiquidityState) -> dict[str, Any]:
    """Compute liquidity score (0-1) and tier classification."""
    if state.get("error"): return {}
    
    avg_vol_usd = state.get("avg_daily_volume_usd", 0.0)
    days_to_exit = state.get("days_to_exit", float('inf'))
    market_impact_pct = state.get("market_impact_pct", 1.0)
    
    # Volume score (0-1)
    vol_score = min(avg_vol_usd / 10_000_000.0, 1.0)
    
    # Days-to-exit score (0-1, lower is better)
    exit_score = max(0.0, 1.0 - (days_to_exit / 5.0))
    
    # Impact score (0-1, lower impact is better)
    impact_score = max(0.0, 1.0 - (market_impact_pct / 0.02))
    
    liquidity_score = (vol_score * 0.5) + (exit_score * 0.3) + (impact_score * 0.2)
    
    # Tier classification
    if liquidity_score >= 0.8:
        tier = "high"
    elif liquidity_score >= 0.5:
        tier = "medium"
    elif liquidity_score >= 0.3:
        tier = "low"
    else:
        tier = "illiquid"
        
    return {
        "liquidity_score": float(liquidity_score),
        "liquidity_tier": tier
    }

async def apply_liquidity_limits_node(state: LiquidityState) -> dict[str, Any]:
    """Enforce liquidity rules and potentially reduce position sizes."""
    if state.get("error"): return {}
    
    tier = state.get("liquidity_tier", "illiquid")
    days_to_exit = state.get("days_to_exit", float('inf'))
    position_usd = state.get("position_size_usd", 0.0)
    max_safe_usd = state.get("max_safe_position_usd", 0.0)
    notes = state.get("notes", [])
    
    approved = True
    rejection_reason = None
    
    if tier == "illiquid":
        approved = False
        rejection_reason = "ILLIQUID_SECURITY"
    elif days_to_exit > 5:
        approved = False
        rejection_reason = "POSITION_TOO_LARGE_FOR_LIQUIDITY"
    else:
        if position_usd > max_safe_usd:
            position_usd = max_safe_usd * 0.8
            notes.append("SIZE_REDUCED_FOR_LIQUIDITY")
            
        if tier == "low":
            position_usd *= 0.5
            notes.append("SIZE_REDUCED_FOR_LOW_TIER")
            
    return {
        "approved": approved,
        "rejection_reason": rejection_reason,
        "position_size_usd": float(position_usd),
        "notes": notes
    }

async def store_results_node(state: LiquidityState) -> dict[str, Any]:
    """Cache liquidity evaluations in Redis."""
    if state.get("error"): return {}
    
    ticker = state.get("ticker")
    if not ticker: return {}
    
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        cache_val = {
            "score": state.get("liquidity_score", 0.0),
            "tier": state.get("liquidity_tier", "illiquid"),
            "max_safe_usd": state.get("max_safe_position_usd", 0.0),
            "days_to_exit": state.get("days_to_exit", float('inf')),
            "impact_pct": state.get("market_impact_pct", 0.0)
        }
        r.setex(f"risk:liquidity:{ticker}", 3600, json.dumps(cache_val))
    except Exception as e:
        logger.error(f"Failed to cache liquidity metrics in Redis: {e}")
        
    return {}

def build_liquidity_graph() -> StateGraph:
    workflow = StateGraph(LiquidityState)
    
    workflow.add_node("fetch_volume_data_node", fetch_volume_data_node)
    workflow.add_node("estimate_market_impact_node", estimate_market_impact_node)
    workflow.add_node("score_liquidity_node", score_liquidity_node)
    workflow.add_node("apply_liquidity_limits_node", apply_liquidity_limits_node)
    workflow.add_node("store_results_node", store_results_node)
    
    workflow.set_entry_point("fetch_volume_data_node")
    workflow.add_edge("fetch_volume_data_node", "estimate_market_impact_node")
    workflow.add_edge("estimate_market_impact_node", "score_liquidity_node")
    workflow.add_edge("score_liquidity_node", "apply_liquidity_limits_node")
    workflow.add_edge("apply_liquidity_limits_node", "store_results_node")
    workflow.add_edge("store_results_node", END)
    
    return workflow.compile()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class LiquidityResult:
    ticker: str
    original_size_usd: float
    final_size_usd: float
    liquidity_score: float
    liquidity_tier: str
    days_to_exit: float
    market_impact_pct: float
    total_transaction_cost_pct: float
    approved: bool
    rejection_reason: Optional[str]
    notes: list[str]

class LiquidityAgent:
    """Public interface for evaluating exit liquidity and execution friction."""
    
    def __init__(self):
        self.graph = build_liquidity_graph()
        
    async def check(self, signal: dict, position_size_usd: float) -> Optional[LiquidityResult]:
        """Perform full liquidity and market impact analysis on a proposed position."""
        ticker = signal.get("ticker", "")
        if not ticker: return None
        
        initial_state: LiquidityState = {
            "signal": signal,
            "ticker": ticker.upper(),
            "position_size_usd": float(position_size_usd),
            "avg_daily_volume": 0.0,
            "avg_daily_volume_usd": 0.0,
            "volume_std": 0.0,
            "current_price": 0.0,
            "daily_returns_std": 0.0,
            "bid_ask_spread_pct": 0.0,
            "days_to_exit": 0.0,
            "market_impact_pct": 0.0,
            "total_transaction_cost_pct": 0.0,
            "liquidity_score": 0.0,
            "liquidity_tier": "",
            "max_safe_position_usd": 0.0,
            "approved": False,
            "rejection_reason": None,
            "notes": [],
            "error": None
        }
        
        try:
            final_state = await self.graph.ainvoke(initial_state)
            
            if final_state.get("error"):
                logger.error(f"Liquidity check failed for {ticker}: {final_state['error']}")
                return None
                
            return LiquidityResult(
                ticker=ticker,
                original_size_usd=position_size_usd,
                final_size_usd=final_state.get("position_size_usd", 0.0),
                liquidity_score=final_state.get("liquidity_score", 0.0),
                liquidity_tier=final_state.get("liquidity_tier", "illiquid"),
                days_to_exit=final_state.get("days_to_exit", 0.0),
                market_impact_pct=final_state.get("market_impact_pct", 0.0),
                total_transaction_cost_pct=final_state.get("total_transaction_cost_pct", 0.0),
                approved=final_state.get("approved", False),
                rejection_reason=final_state.get("rejection_reason"),
                notes=final_state.get("notes", [])
            )
        except Exception as e:
            logger.exception("Error executing Liquidity graph")
            return None

    async def check_batch(self, requests: list[dict]) -> list[LiquidityResult]:
        """Process a batch of signals for liquidity checks concurrently.
           Expects a list of dicts: [{'signal': dict, 'position_size_usd': float}]
        """
        tasks = []
        for req in requests:
            tasks.append(self.check(req.get('signal', {}), req.get('position_size_usd', 0.0)))
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_results = [r for r in results if isinstance(r, LiquidityResult)]
        return valid_results

    def get_liquidity_tier(self, ticker: str) -> str:
        """Fetch cached liquidity tier from Redis."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data_str = r.get(f"risk:liquidity:{ticker.upper()}")
            if data_str:
                data = json.loads(data_str)
                return data.get("tier", "unknown")
        except Exception:
            pass
        return "unknown"

    def get_max_safe_position(self, ticker: str) -> float:
        """Fetch cached max safe position from Redis."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data_str = r.get(f"risk:liquidity:{ticker.upper()}")
            if data_str:
                data = json.loads(data_str)
                return float(data.get("max_safe_usd", 0.0))
        except Exception:
            pass
        return 0.0

    def estimate_exit_cost(self, ticker: str, shares: float) -> float:
        """Simple theoretical estimation of exit cost for a specific share amount using cached metrics."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data_str = r.get(f"risk:liquidity:{ticker.upper()}")
            if data_str:
                data = json.loads(data_str)
                impact_pct = float(data.get("impact_pct", 0.005))
                # Note: true cost depends on price, but we just return percentage penalty here.
                return impact_pct
        except Exception:
            pass
        return 0.005
