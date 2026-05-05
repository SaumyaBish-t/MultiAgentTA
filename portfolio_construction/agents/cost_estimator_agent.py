import json
import asyncio
import math
import uuid
from datetime import datetime, timezone
from typing import TypedDict, Any, Optional, List, Dict, Union
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import redis
import httpx
from loguru import logger
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from langgraph.graph import StateGraph, END

from config.settings import settings
from portfolio_construction.storage.portfolio_models import CostEstimate

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CostState(TypedDict):
    trades: List[Dict[str, Any]]         # proposed trades: {ticker, action, shares, price}
    ticker_data: Dict[str, Dict[str, Any]] # volume, price, volatility
    cost_breakdown: List[Dict[str, Any]] # per trade costs
    total_cost: float
    total_cost_bps: float                # basis points of trade value
    high_cost_trades: List[str]
    recommendations: List[str]
    error: Optional[str]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_market_data_node(state: CostState) -> Dict[str, Any]:
    """Fetch latest price, volume history and calculate volatility."""
    trades = state["trades"]
    tickers = list(set(t["ticker"] for t in trades))
    ticker_data = {}
    headers = {"x-api-key": settings.internal_api_key}
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for ticker in tickers:
            tasks.append(client.get(f"http://localhost:8000/prices/{ticker}/latest", headers=headers, timeout=10.0))
            tasks.append(client.get(f"http://localhost:8000/prices/{ticker}/history?days=30", headers=headers, timeout=10.0))
            
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i in range(0, len(responses), 2):
            ticker = tickers[i//2]
            latest_resp = responses[i]
            history_resp = responses[i+1]
            
            try:
                if (not isinstance(latest_resp, Exception) and latest_resp.status_code == 200 and 
                    not isinstance(history_resp, Exception) and history_resp.status_code == 200):
                    
                    latest = latest_resp.json()
                    history = history_resp.json()
                    
                    if not history: continue
                    
                    df = pd.DataFrame(history)
                    df["returns"] = df["close"].pct_change()
                    
                    avg_vol = df["volume"].mean()
                    avg_price = df["close"].mean()
                    avg_dollar_vol = (df["volume"] * df["close"]).mean()
                    realized_vol = df["returns"].std() * math.sqrt(252) if len(df) > 1 else 0.2
                    
                    ticker_data[ticker] = {
                        "current_price": float(latest.get("close", avg_price)),
                        "avg_30d_volume": float(avg_vol),
                        "avg_30d_dollar_volume": float(avg_dollar_vol),
                        "realized_vol_30d": float(realized_vol),
                        "avg_price": float(avg_price)
                    }
                else:
                    logger.warning(f"Data fetch incomplete for {ticker}: Latest={latest_resp}, History={history_resp}")
            except Exception as e:
                logger.error(f"Error processing data for {ticker}: {e}")
                
    return {"ticker_data": ticker_data}

async def estimate_commission_node(state: CostState) -> Dict[str, Any]:
    """Calculate regulatory fees (SEC, FINRA) for Alpaca (commission-free)."""
    trades = state["trades"]
    ticker_data = state["ticker_data"]
    breakdown = []
    
    for t in trades:
        ticker = t["ticker"]
        if ticker not in ticker_data: continue
        
        price = ticker_data[ticker]["current_price"]
        shares = t["shares"]
        action = t["action"].lower()
        trade_value = shares * price
        
        # SEC Fee: $27.80 per million (only on sells)
        sec_fee = 0.0
        if action in ["sell", "close"]:
            sec_fee = trade_value * 0.0000278
            
        # FINRA TAF: $0.000145 per share, max $7.27
        finra_taf = min(shares * 0.000145, 7.27)
        
        breakdown.append({
            "ticker": ticker,
            "action": action,
            "shares": shares,
            "trade_value": trade_value,
            "commission": 0.0,
            "sec_fee": sec_fee,
            "finra_taf": finra_taf
        })
        
    return {"cost_breakdown": breakdown}

async def estimate_spread_cost_node(state: CostState) -> Dict[str, Any]:
    """Estimate bid-ask spread cost based on price buckets."""
    breakdown = state["cost_breakdown"]
    ticker_data = state["ticker_data"]
    
    for item in breakdown:
        ticker = item["ticker"]
        price = ticker_data[ticker]["current_price"]
        
        # Effective half-spread estimate
        if price > 100:
            half_spread = 0.01
        elif price > 10:
            half_spread = 0.02
        else:
            half_spread = 0.05
            
        spread_pct = half_spread / price
        item["spread_cost"] = item["trade_value"] * spread_pct
        
    return {"cost_breakdown": breakdown}

async def estimate_market_impact_node(state: CostState) -> Dict[str, Any]:
    """Estimate market impact using the square root model."""
    breakdown = state["cost_breakdown"]
    ticker_data = state["ticker_data"]
    
    for item in breakdown:
        ticker = item["ticker"]
        data = ticker_data[ticker]
        
        sigma = data["realized_vol_30d"] / math.sqrt(252) # daily vol
        adv = data["avg_30d_dollar_volume"]
        trade_val = item["trade_value"]
        
        participation = trade_val / adv if adv > 0 else 0.001
        
        # Square root model: Impact = 0.1 * sigma * sqrt(participation)
        impact_pct = 0.1 * sigma * math.sqrt(participation)
        
        # Linear impact for large trades (>10% ADV)
        if participation > 0.10:
            impact_pct += 0.5 * sigma * (participation - 0.10)
            
        item["market_impact_usd"] = impact_pct * trade_val
        
    return {"cost_breakdown": breakdown}

async def estimate_timing_cost_node(state: CostState) -> Dict[str, Any]:
    """Apply penalty based on market hours (UTC)."""
    breakdown = state["cost_breakdown"]
    current_hour = datetime.now(timezone.utc).hour
    
    # Market Open (14:30-15:00 UTC) - High volatility/spread
    if 14 <= current_hour <= 15:
        timing_penalty = 0.0005 # 5 bps
    # Market Close (19:30-20:00 UTC) - High volume but wider spreads
    elif 19 <= current_hour <= 20:
        timing_penalty = 0.0003 # 3 bps
    else:
        timing_penalty = 0.0
        
    for item in breakdown:
        item["timing_cost"] = item["trade_value"] * timing_penalty
        
    return {"cost_breakdown": breakdown}

async def compile_cost_report_node(state: CostState) -> Dict[str, Any]:
    """Aggregate all costs and generate warnings/recommendations."""
    breakdown = state["cost_breakdown"]
    total_cost = 0.0
    total_value = 0.0
    high_cost_trades = []
    recommendations = []
    
    for item in breakdown:
        item["total_cost"] = (
            item["commission"] + item["sec_fee"] + item["finra_taf"] +
            item["spread_cost"] + item["market_impact_usd"] + item["timing_cost"]
        )
        
        item["total_cost_bps"] = (item["total_cost"] / item["trade_value"]) * 10000 if item["trade_value"] > 0 else 0
        
        total_cost += item["total_cost"]
        total_value += item["trade_value"]
        
        if item["total_cost_bps"] > 30:
            high_cost_trades.append(item["ticker"])
            recommendations.append(f"Trade {item['ticker']} is expensive ({item['total_cost_bps']:.1f} bps). Split into smaller chunks.")
            
    total_cost_bps = (total_cost / total_value) * 10000 if total_value > 0 else 0
    
    if total_cost > 500:
        recommendations.append("Total portfolio rebalance friction exceeds $500. Consider phased execution over 2-3 days.")
        
    return {
        "total_cost": total_cost,
        "total_cost_bps": total_cost_bps,
        "high_cost_trades": high_cost_trades,
        "recommendations": recommendations
    }

async def store_estimates_node(state: CostState) -> Dict[str, Any]:
    """Store summary in Redis and detailed logs in DB (if needed)."""
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    summary = {
        "total_cost": state["total_cost"],
        "total_cost_bps": state["total_cost_bps"],
        "high_cost_tickers": state["high_cost_trades"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    r.setex("portfolio:cost:estimate:latest", 1800, json.dumps(summary))
    logger.info(f"Cost Estimator: Total friction ${state['total_cost']:.2f} ({state['total_cost_bps']:.1f} bps)")
    
    return {}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class CostReport:
    total_cost_usd: float
    total_cost_bps: float
    per_trade_breakdown: List[Dict[str, Any]]
    high_cost_trades: List[str]
    recommendations: List[str]
    optimal_execution_window: str = "16:00 - 18:00 UTC"

class CostEstimatorAgent:
    """Agent for estimating transaction costs and market impact."""
    
    def __init__(self):
        self.workflow = StateGraph(CostState)
        
        self.workflow.add_node("fetch_data", fetch_market_data_node)
        self.workflow.add_node("estimate_commission", estimate_commission_node)
        self.workflow.add_node("estimate_spread", estimate_spread_cost_node)
        self.workflow.add_node("estimate_impact", estimate_market_impact_node)
        self.workflow.add_node("estimate_timing", estimate_timing_cost_node)
        self.workflow.add_node("compile_report", compile_cost_report_node)
        self.workflow.add_node("store_estimates", store_estimates_node)
        
        self.workflow.set_entry_point("fetch_data")
        self.workflow.add_edge("fetch_data", "estimate_commission")
        self.workflow.add_edge("estimate_commission", "estimate_spread")
        self.workflow.add_edge("estimate_spread", "estimate_impact")
        self.workflow.add_edge("estimate_impact", "estimate_timing")
        self.workflow.add_edge("estimate_timing", "compile_report")
        self.workflow.add_edge("compile_report", "store_estimates")
        self.workflow.add_edge("store_estimates", END)
        
        self.app = self.workflow.compile()
        
    async def estimate(self, trades: List[Dict[str, Any]]) -> Optional[CostReport]:
        """Main method to calculate costs for a list of trades."""
        initial_state: CostState = {
            "trades": trades,
            "ticker_data": {},
            "cost_breakdown": [],
            "total_cost": 0.0,
            "total_cost_bps": 0.0,
            "high_cost_trades": [],
            "recommendations": [],
            "error": None
        }
        
        try:
            final_state = await self.app.ainvoke(initial_state)
            return CostReport(
                total_cost_usd=final_state["total_cost"],
                total_cost_bps=final_state["total_cost_bps"],
                per_trade_breakdown=final_state["cost_breakdown"],
                high_cost_trades=final_state["high_cost_trades"],
                recommendations=final_state["recommendations"]
            )
        except Exception as e:
            logger.exception(f"Cost estimation failed: {e}")
            return None

    async def estimate_single(self, ticker: str, shares: int, action: str) -> float:
        """Convenience method for a single ticker."""
        report = await self.estimate([{"ticker": ticker, "shares": shares, "action": action}])
        return report.total_cost_usd if report else 0.0

    def get_optimal_execution_time(self, ticker: str) -> str:
        """Returns the best window for trading based on liquidity."""
        return "16:00 - 18:00 UTC (Midday liquidity)"

    def suggest_trade_splitting(self, trade: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Logic to split a large trade into smaller chunks."""
        # Split into 3 days if high cost detected
        shares = trade["shares"]
        chunk = shares // 3
        return [{"ticker": trade["ticker"], "shares": chunk, "action": trade["action"]} for _ in range(3)]
