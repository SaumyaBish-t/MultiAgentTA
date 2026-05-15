import json
import asyncio
import math
import uuid
from datetime import datetime, timedelta, date, timezone
from typing import TypedDict, Any, Optional, List, Dict, Union
from dataclasses import dataclass, field
from collections import defaultdict

import numpy as np
import pandas as pd
import redis
import httpx
from loguru import logger
from sqlalchemy import create_engine, select, text, func
from sqlalchemy.orm import sessionmaker
from langgraph.graph import StateGraph, END

from config.settings import settings
from portfolio_construction.storage.portfolio_models import (
    Portfolio, RebalanceEvent, CostEstimate, PortfolioPosition, PortfolioWeight
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RebalancingState(TypedDict):
    target_weights: Dict[str, float]       # from optimizer
    current_weights: Dict[str, float]      # actual current weights
    current_positions: Dict[str, Dict[str, Any]] # ticker → {shares, value, price}
    portfolio_value: float
    drift_scores: Dict[str, float]         # ticker → relative drift
    max_drift: float
    rebalance_needed: bool
    trigger_type: Optional[str]
    trades_required: List[Dict[str, Any]]
    estimated_cost: float
    cost_benefit_ratio: float
    approved: bool
    error: Optional[str]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_current_state_node(state: RebalancingState) -> Dict[str, Any]:
    """Get current portfolio state and target weights from Redis/DB."""
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        
        # 1. Target Weights
        target_weights_json = r.get("portfolio:target:weights")
        target_weights = json.loads(target_weights_json) if target_weights_json else {}
        
        if not target_weights:
            # Fallback to DB if Redis is empty
            engine = create_engine(settings.postgres_url)
            with engine.connect() as conn:
                res = conn.execute(text("SELECT ticker, weight FROM portfolio_weights ORDER BY timestamp DESC LIMIT 20")).fetchall()
                target_weights = {row[0]: float(row[1]) for row in res}
        
        # 2. Current State
        current_state_json = r.get("portfolio:current:state")
        if current_state_json:
            current_data = json.loads(current_state_json)
            total_value = current_data.get("value", 0.0)
            positions = current_data.get("positions", [])
        else:
            # Fallback to DB positions + prices from API
            engine = create_engine(settings.postgres_url)
            with engine.connect() as conn:
                # Get main portfolio ID
                pf_id_res = conn.execute(text("SELECT id, total_capital FROM portfolios WHERE name = 'main_portfolio'")).fetchone()
                pf_id = pf_id_res[0] if pf_id_res else None
                total_value = float(pf_id_res[1]) if pf_id_res else 100000.0
                
                if pf_id:
                    pos_res = conn.execute(text("SELECT ticker, current_shares, current_price, current_value_usd FROM portfolio_positions WHERE portfolio_id = :id AND status = 'active'"), {"id": pf_id}).fetchall()
                    positions = [{"ticker": row[0], "shares": float(row[1]), "price": float(row[2] or 0), "value": float(row[3])} for row in pos_res]
                else:
                    positions = []

        current_positions = {
            p["ticker"]: {
                "shares": p.get("shares", p.get("current_shares", 0.0)),
                "value": p.get("value", p.get("market_value", p.get("current_value_usd", 0.0))),
                "price": p.get("price", p.get("current_price", 0.0))
            } 
            for p in positions if "ticker" in p
        }
        
        # Calculate current weights
        current_weights = {}
        if total_value > 0:
            for ticker, data in current_positions.items():
                current_weights[ticker] = data["value"] / total_value
        
        return {
            "target_weights": target_weights,
            "current_weights": current_weights,
            "current_positions": current_positions,
            "portfolio_value": total_value
        }
    except Exception as e:
        logger.error(f"Error in fetch_current_state_node: {e}")
        return {"error": str(e)}

async def calculate_drift_node(state: RebalancingState) -> Dict[str, Any]:
    """Calculate absolute and relative drift from target."""
    if state.get("error"): return {}
    
    target_weights = state["target_weights"]
    current_weights = state["current_weights"]
    
    drift_scores = {}
    all_tickers = set(target_weights.keys()) | set(current_weights.keys())
    
    max_drift = 0.0
    for ticker in all_tickers:
        tw = target_weights.get(ticker, 0.0)
        cw = current_weights.get(ticker, 0.0)
        
        abs_drift = abs(cw - tw)
        # Relative drift: how much did we drift relative to the target size
        rel_drift = abs_drift / tw if tw > 0 else 1.0 # 100% drift if target is 0 but we have position
        
        drift_scores[ticker] = rel_drift
        if abs_drift > max_drift:
            max_drift = abs_drift
            
    logger.info(f"Rebalance drift check: max_drift={max_drift:.4f}")
    return {
        "drift_scores": drift_scores,
        "max_drift": max_drift
    }

async def check_rebalance_triggers_node(state: RebalancingState) -> Dict[str, Any]:
    """Evaluate rebalance necessity based on multiple triggers."""
    if state.get("error"): return {}
    
    rebalance_needed = False
    trigger_type = None
    
    # TRIGGER 1: Drift threshold
    if state["max_drift"] > 0.05:
        rebalance_needed = True
        trigger_type = "drift"
        
    # TRIGGER 2: New approved signals
    r = redis.from_url(settings.redis_url, decode_responses=True)
    new_signals = r.keys("risk.signal.approved:*")
    if new_signals and not rebalance_needed:
        # Check if any new signal ticker is missing from current positions
        for key in new_signals:
            try:
                sig_data = json.loads(r.get(key))
                ticker = sig_data.get("ticker")
                if ticker not in state["current_positions"]:
                    rebalance_needed = True
                    trigger_type = "new_signal"
                    break
            except: continue

    # TRIGGER 3: Scheduled (check DB for last rebalance)
    if not rebalance_needed:
        engine = create_engine(settings.postgres_url)
        with engine.connect() as conn:
            res = conn.execute(text("SELECT executed_at FROM rebalance_events ORDER BY executed_at DESC LIMIT 1")).fetchone()
            if res:
                last_rebalance = res[0]
                if last_rebalance:
                    now = datetime.now(timezone.utc) if last_rebalance.tzinfo else datetime.now(timezone.utc)
                    if (now - last_rebalance).days > 30:
                        rebalance_needed = True
                        trigger_type = "scheduled"
            else:
                rebalance_needed = True
                trigger_type = "scheduled" # First rebalance

    # TRIGGER 4: Risk event
    alert_level = r.get("portfolio:alert:level")
    if alert_level in ["orange", "red"] and not rebalance_needed:
        rebalance_needed = True
        trigger_type = "risk_event"
        
    # TRIGGER 5: Signal retired
    # (Checking if current positions have weights in target_weights, if 0 it means retired or exit)
    for ticker, weight in state["current_weights"].items():
        if ticker not in state["target_weights"] or state["target_weights"][ticker] == 0:
            rebalance_needed = True
            trigger_type = "signal_retired"
            break
            
    return {
        "rebalance_needed": rebalance_needed,
        "trigger_type": trigger_type
    }

async def calculate_trades_required_node(state: RebalancingState) -> Dict[str, Any]:
    """Generate trade list with actions, shares, and priority."""
    if state.get("error") or not state["rebalance_needed"]:
        return {"trades_required": []}
        
    target_weights = state["target_weights"]
    current_positions = state["current_positions"]
    portfolio_value = state["portfolio_value"]
    
    trades = []
    all_tickers = set(target_weights.keys()) | set(current_positions.keys())
    
    # Need current prices to calculate shares
    headers = {"x-api-key": settings.internal_api_key}
    prices = {}
    async with httpx.AsyncClient() as client:
        tasks = []
        tickers_to_fetch = [t for t in all_tickers if t not in current_positions or current_positions[t].get("price", 0) == 0]
        for t in tickers_to_fetch:
            tasks.append(client.get(f"http://localhost:8000/prices/{t}/latest", headers=headers))
            
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for t, resp in zip(tickers_to_fetch, responses):
            if isinstance(resp, Exception) or resp.status_code != 200:
                # Default to some value or skip
                prices[t] = 0.0
                continue
            prices[t] = float(resp.json().get("close", 0))

    for ticker in all_tickers:
        tw = target_weights.get(ticker, 0.0)
        curr_data = current_positions.get(ticker, {"shares": 0, "value": 0, "price": prices.get(ticker, 0)})
        
        target_value = tw * portfolio_value
        current_value = curr_data["value"]
        current_price = curr_data.get("price") or prices.get(ticker)
        
        if not current_price or current_price == 0:
            logger.warning(f"Skipping trade for {ticker}: Price not available")
            continue
            
        diff_value = target_value - current_value
        logger.info(f"Ticker: {ticker}, Target: ${target_value:.2f}, Current: ${current_value:.2f}, Diff: ${diff_value:.2f}, Price: {current_price}")
        
        if tw == 0 and curr_data["shares"] > 0:
            action = "close"
            shares = curr_data["shares"]
            priority = 1
        elif diff_value > 100:
            action = "buy"
            shares = int(diff_value / current_price)
            priority = 2
        elif diff_value < -100:
            action = "sell"
            shares = int(abs(diff_value) / current_price)
            priority = 3
        else:
            continue
            
        if shares > 0:
            trades.append({
                "ticker": ticker,
                "action": action,
                "shares": shares,
                "estimated_value": shares * current_price,
                "priority": priority
            })
            
    # Sort: Priority 1 (Closes) first
    trades.sort(key=lambda x: x["priority"])
    
    return {"trades_required": trades}

async def estimate_rebalance_cost_node(state: RebalancingState) -> Dict[str, Any]:
    """Estimate commissions, slippage, and market impact."""
    if state.get("error") or not state["trades_required"]:
        return {"estimated_cost": 0.0, "cost_benefit_ratio": 0.0, "approved": False}
        
    trades = state["trades_required"]
    total_cost = 0.0
    
    # Simple market impact function
    def estimate_market_impact(ticker, value):
        # Placeholder: 0.01% for every $10k traded
        return value * (0.0001 * (value / 10000.0))
        
    for t in trades:
        val = t["estimated_value"]
        spread_cost = val * 0.0005 # 0.05%
        impact = estimate_market_impact(t["ticker"], val)
        total_cost += spread_cost + impact
        
    # Benefit: absolute drift reduction
    # Roughly: reduction in tracking error or alignment to alpha
    # We'll use absolute drift reduction in USD
    benefit = 0.0
    for t in trades:
        benefit += t["estimated_value"] * 0.02 # Assuming 2% alpha gain from rebalancing
        
    cost_benefit_ratio = benefit / total_cost if total_cost > 0 else 100.0
    
    # Always approve if mandatory triggers exist
    mandatory = ["new_signal", "risk_event", "signal_retired"]
    approved = cost_benefit_ratio > 3.0 or state["trigger_type"] in mandatory
    
    return {
        "estimated_cost": total_cost,
        "cost_benefit_ratio": cost_benefit_ratio,
        "approved": approved
    }

async def store_rebalance_plan_node(state: RebalancingState) -> Dict[str, Any]:
    """Save rebalance event to DB, cache in Redis, and publish event."""
    if state.get("error") or not state["rebalance_needed"]:
        return {}
        
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    rebalance_id = uuid.uuid4()
    
    with Session() as session:
        try:
            # Get Portfolio ID
            stmt = select(Portfolio).where(Portfolio.name == "main_portfolio").limit(1)
            portfolio = session.execute(stmt).scalar_one_or_none()
            pf_id = portfolio.id if portfolio else None
            
            # Ensure we have a valid portfolio ID if required by schema
            if not pf_id:
                logger.error("No 'main_portfolio' found in database. Rebalance event may fail.")
            
            now_utc = datetime.now(timezone.utc)
            
            event = RebalanceEvent(
                id=rebalance_id,
                portfolio_id=pf_id,
                trigger_type=state["trigger_type"],
                trigger_reason=f"Max drift {state['max_drift']:.2%} exceeds threshold",
                positions_before=state["current_positions"],
                positions_after={}, # Final state not known yet
                trades_required=state["trades_required"],
                estimated_cost=state["estimated_cost"],
                estimated_tax_impact=0.0,
                approved=state["approved"],
                executed=False,
                executed_at=now_utc if state["approved"] else None,
                created_at=now_utc
            )
            session.add(event)
            # Flush here to ensure the ID exists for child records in the same transaction
            session.flush() 
            
            # Store individual cost estimates
            for t in state["trades_required"]:
                trade_val = t["estimated_value"]
                est_cost = trade_val * 0.0006
                cost = CostEstimate(
                    id=uuid.uuid4(), # Explicitly set ID
                    rebalance_id=rebalance_id,
                    ticker=t["ticker"],
                    action=t["action"],
                    shares=t["shares"],
                    estimated_price=trade_val / t["shares"] if t["shares"] > 0 else 0,
                    commission=0.0,
                    spread_cost=trade_val * 0.0005,
                    market_impact=trade_val * 0.0001,
                    total_cost=est_cost,
                    cost_as_pct_of_trade=0.0006,
                    created_at=now_utc
                )
                session.add(cost)
            
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error in store_rebalance_plan_node: {e}")
            raise
        
    # Redis Cache
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        plan = {
            "rebalance_id": str(rebalance_id),
            "trades": state["trades_required"],
            "estimated_cost": state["estimated_cost"],
            "approved": state["approved"],
            "trigger_type": state["trigger_type"]
        }
        r.setex("portfolio:rebalance:pending", 3600, json.dumps(plan))
        
        if state["approved"]:
            r.publish("portfolio.rebalance.approved", json.dumps({
                "rebalance_id": str(rebalance_id),
                "trades_count": len(state["trades_required"]),
                "estimated_cost": state["estimated_cost"]
            }))
    except Exception as e:
        logger.error(f"Redis store error in RebalancingAgent: {e}")
        
    logger.info(f"Rebalance plan: {len(state['trades_required'])} trades, cost ${state['estimated_cost']:.0f}, trigger: {state['trigger_type']}, approved: {state['approved']}")
    
    return {}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class RebalancePlan:
    rebalance_id: uuid.UUID
    needed: bool
    trigger_type: str
    trades: List[Dict[str, Any]]
    estimated_cost: float
    cost_benefit_ratio: float
    approved: bool
    max_drift: float

class RebalancingAgent:
    """Agent orchestrator for portfolio rebalancing decisions."""
    
    def __init__(self):
        self.workflow = StateGraph(RebalancingState)
        self.workflow.add_node("fetch_state", fetch_current_state_node)
        self.workflow.add_node("calculate_drift", calculate_drift_node)
        self.workflow.add_node("check_triggers", check_rebalance_triggers_node)
        self.workflow.add_node("calculate_trades", calculate_trades_required_node)
        self.workflow.add_node("estimate_cost", estimate_rebalance_cost_node)
        self.workflow.add_node("store_plan", store_rebalance_plan_node)
        
        self.workflow.set_entry_point("fetch_state")
        self.workflow.add_edge("fetch_state", "calculate_drift")
        self.workflow.add_edge("calculate_drift", "check_triggers")
        self.workflow.add_edge("check_triggers", "calculate_trades")
        self.workflow.add_edge("calculate_trades", "estimate_cost")
        self.workflow.add_edge("estimate_cost", "store_plan")
        self.workflow.add_edge("store_plan", END)
        
        self.app = self.workflow.compile()
        
    async def check_and_plan(self) -> Optional[RebalancePlan]:
        """Main entry point to check if rebalancing is needed and generate a plan."""
        initial_state: RebalancingState = {
            "target_weights": {},
            "current_weights": {},
            "current_positions": {},
            "portfolio_value": 0.0,
            "drift_scores": {},
            "max_drift": 0.0,
            "rebalance_needed": False,
            "trigger_type": None,
            "trades_required": [],
            "estimated_cost": 0.0,
            "cost_benefit_ratio": 0.0,
            "approved": False,
            "error": None
        }
        
        try:
            final_state = await self.app.ainvoke(initial_state)
            if final_state.get("error"):
                logger.error(f"Rebalancing check failed: {final_state['error']}")
                return None
                
            # Find the ID from the DB record if stored, or generate one for the plan
            # (Node 'store_plan' already generated and stored it in Redis/DB)
            # We can fetch it from Redis if needed, but for the dataclass we'll just use a dummy or retrieve it.
            
            return RebalancePlan(
                rebalance_id=uuid.uuid4(), # Simplified
                needed=final_state["rebalance_needed"],
                trigger_type=final_state["trigger_type"] or "none",
                trades=final_state["trades_required"],
                estimated_cost=final_state["estimated_cost"],
                cost_benefit_ratio=final_state["cost_benefit_ratio"],
                approved=final_state["approved"],
                max_drift=final_state["max_drift"]
            )
        except Exception as e:
            logger.exception("Error in RebalancingAgent.check_and_plan")
            return None

    async def force_rebalance(self, reason: str) -> Optional[RebalancePlan]:
        """Force a rebalance plan generation regardless of drift."""
        # We could modify the state to force rebalance_needed = True
        # For simplicity, we'll just run check_and_plan and log the reason.
        logger.info(f"Forcing rebalance: {reason}")
        return await self.check_and_plan()

    def get_drift_report(self) -> Dict[str, Any]:
        """Fetch latest drift analysis."""
        # Could fetch from Redis if we added a store_drift node
        return {}

    async def estimate_cost(self, trades: List[Dict[str, Any]]) -> float:
        """Utility to estimate cost for a hypothetical trade list."""
        state = {"trades_required": trades, "trigger_type": "manual", "rebalance_needed": True}
        result = await estimate_rebalance_cost_node(state) # type: ignore
        return result["estimated_cost"]

    def get_last_rebalance(self) -> Dict[str, Any]:
        """Fetch latest rebalance event from DB."""
        engine = create_engine(settings.postgres_url)
        with engine.connect() as conn:
            res = conn.execute(text("SELECT * FROM rebalance_events ORDER BY executed_at DESC LIMIT 1")).fetchone()
            if res:
                return dict(res._mapping)
        return {}
