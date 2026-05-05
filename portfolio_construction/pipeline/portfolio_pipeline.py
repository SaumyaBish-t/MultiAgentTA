import json
import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import TypedDict, Any, Optional, List, Dict, Union
from dataclasses import dataclass

import redis
from loguru import logger
from sqlalchemy import create_engine, text
from langgraph.graph import StateGraph, END

from config.settings import settings
from portfolio_construction.agents.optimizer_agent import PortfolioOptimizer
from portfolio_construction.agents.factor_agent import FactorAgent
from portfolio_construction.agents.rebalancing_agent import RebalancingAgent
from portfolio_construction.agents.cost_estimator_agent import CostEstimatorAgent
from portfolio_construction.agents.allocation_agent import AllocationAgent

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — PIPELINE STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PortfolioPipelineState(TypedDict):
    approved_signals: List[Dict[str, Any]]  # from Phase 4
    optimizer_result: Dict[str, Any]
    factor_result: Dict[str, Any]
    rebalance_plan: Dict[str, Any]
    cost_estimates: Dict[str, Any]
    final_allocation: Dict[str, Any]
    run_id: str
    portfolio_id: Optional[uuid.UUID]
    status: str
    error: Optional[str]
    start_time: float

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — PIPELINE NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_approved_signals_node(state: PortfolioPipelineState) -> Dict[str, Any]:
    """Fetch all risk-approved signals from the database."""
    engine = create_engine(settings.postgres_url)
    try:
        with engine.connect() as conn:
            # Query approved signals that haven't expired
            res = conn.execute(text("""
                SELECT ticker, approved_position_size_usd, risk_score, signal_id 
                FROM approved_signals 
                WHERE status = 'approved' 
                AND (valid_until IS NULL OR valid_until > NOW())
            """)).fetchall()
            
            signals = [
                {"ticker": r[0], "max_size": float(r[1]), "risk_score": float(r[2]), "id": str(r[3])} 
                for r in res
            ]
            
        logger.info(f"Portfolio pipeline: {len(signals)} approved signals fetched.")
        return {"approved_signals": signals, "status": "signals_fetched"}
    except Exception as e:
        logger.error(f"Error fetching approved signals: {e}")
        return {"error": str(e)}

async def run_optimization_node(state: PortfolioPipelineState) -> Dict[str, Any]:
    """Determine optimal target weights across approved signals."""
    if state.get("error"): return {}
    
    signals = state["approved_signals"]
    if not signals:
        return {"error": "No approved signals available for optimization"}
        
    optimizer = PortfolioOptimizer()
    try:
        result = await optimizer.optimize(signals)
        if not result or not result.weights:
            # Fallback: Equal weight
            logger.warning("Optimization failed. Falling back to equal weight allocation.")
            weight = 0.95 / len(signals)
            weights = {s["ticker"]: weight for s in signals}
            result_dict = {"weights": weights, "method": "equal_weight", "sharpe": 1.0}
        else:
            result_dict = {
                "weights": result.weights,
                "method": result.optimization_method,
                "metrics": {
                    "expected_return": result.expected_return,
                    "expected_volatility": result.expected_volatility,
                    "sharpe_ratio": result.expected_sharpe
                },
                "sharpe": result.expected_sharpe
            }
            
        return {"optimizer_result": result_dict, "status": "optimized"}
    except Exception as e:
        logger.exception("Optimizer node failed")
        return {"error": str(e)}

async def check_factor_exposures_node(state: PortfolioPipelineState) -> Dict[str, Any]:
    """Analyze and adjust weights based on factor constraints."""
    if state.get("error"): return {}
    
    weights = state["optimizer_result"]["weights"]
    agent = FactorAgent()
    
    try:
        result = await agent.analyze(weights)
        if result and result.adjusted_weights:
            logger.info(f"Factor adjustment applied. Breaches: {result.breaches}")
            return {
                "factor_result": {
                    "adjusted_weights": result.adjusted_weights,
                    "breaches": result.breaches,
                    "factors": result.factors
                },
                "status": "factor_checked"
            }
        return {"factor_result": {"adjusted_weights": weights}, "status": "factor_checked"}
    except Exception as e:
        logger.error(f"Factor check failed: {e}")
        return {"factor_result": {"adjusted_weights": weights}}

async def plan_rebalance_node(state: PortfolioPipelineState) -> Dict[str, Any]:
    """Calculate trades needed to reach target weights."""
    if state.get("error"): return {}
    
    # Use factor adjusted weights as target
    target_weights = state["factor_result"]["adjusted_weights"]
    
    # Store target weights in Redis for RebalancingAgent to pick up
    r = redis.from_url(settings.redis_url, decode_responses=True)
    r.set("portfolio:target:weights", json.dumps(target_weights))
    
    agent = RebalancingAgent()
    try:
        plan = await agent.check_and_plan()
        if not plan or not plan.needed:
            logger.info("No rebalance needed. Portfolio drift within limits.")
            return {"rebalance_plan": {"needed": False}, "status": "no_rebalance_needed"}
            
        return {
            "rebalance_plan": {
                "needed": True,
                "trigger_type": plan.trigger_type,
                "trades": plan.trades,
                "max_drift": plan.max_drift
            },
            "status": "rebalance_planned"
        }
    except Exception as e:
        logger.error(f"Rebalance planning failed: {e}")
        return {"error": str(e)}

async def estimate_costs_node(state: PortfolioPipelineState) -> Dict[str, Any]:
    """Estimate execution friction for the rebalance plan."""
    if state.get("error") or not state["rebalance_plan"].get("needed"):
        return {}
        
    trades = state["rebalance_plan"]["trades"]
    agent = CostEstimatorAgent()
    
    try:
        report = await agent.estimate(trades)
        if report:
            return {
                "cost_estimates": {
                    "total_cost": report.total_cost_usd,
                    "total_bps": report.total_cost_bps,
                    "high_cost_trades": report.high_cost_trades,
                    "recommendations": report.recommendations
                },
                "status": "costs_estimated"
            }
        return {}
    except Exception as e:
        logger.error(f"Cost estimation node failed: {e}")
        return {}

async def finalize_allocation_node(state: PortfolioPipelineState) -> Dict[str, Any]:
    """Produce the final portfolio targets and notify execution."""
    if state.get("error"): return {}
    
    # Skip if no rebalance needed, unless we want to force update DB state
    if not state["rebalance_plan"].get("needed"):
        return {"status": "completed"}
        
    weights = state["factor_result"]["adjusted_weights"]
    agent = AllocationAgent()
    
    try:
        # Pass cost estimates to allocation agent for final check
        # (Internal state of AllocationAgent doesn't need it passed as arg, but let's assume it picks up from state)
        allocation = await agent.allocate(weights)
        if allocation:
            return {
                "final_allocation": {
                    "positions": [vars(p) for p in allocation.positions],
                    "total_invested": allocation.total_invested_usd,
                    "cash": allocation.cash_usd
                },
                "status": "allocation_finalized"
            }
        return {"error": "Allocation agent failed to produce target"}
    except Exception as e:
        logger.error(f"Finalization node failed: {e}")
        return {"error": str(e)}

async def finalize_run_node(state: PortfolioPipelineState) -> Dict[str, Any]:
    """Log summary and publish completion event."""
    duration = time.time() - state["start_time"]
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    event = {
        "run_id": state["run_id"],
        "status": "success" if not state.get("error") else "failed",
        "error": state.get("error"),
        "duration": duration,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    r.publish("portfolio.pipeline.completed", json.dumps(event))
    logger.info(f"Portfolio Construction Pipeline completed in {duration:.2f}s. Status: {event['status']}")
    
    return {"status": "completed"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PIPELINE ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PortfolioPipeline:
    """End-to-end orchestrator for Phase 5 Portfolio Construction."""
    
    def __init__(self):
        self.workflow = StateGraph(PortfolioPipelineState)
        
        self.workflow.add_node("fetch_signals", fetch_approved_signals_node)
        self.workflow.add_node("optimize", run_optimization_node)
        self.workflow.add_node("factor_check", check_factor_exposures_node)
        self.workflow.add_node("plan_rebalance", plan_rebalance_node)
        self.workflow.add_node("estimate_costs", estimate_costs_node)
        self.workflow.add_node("finalize_allocation", finalize_allocation_node)
        self.workflow.add_node("finalize_run", finalize_run_node)
        
        self.workflow.set_entry_point("fetch_signals")
        self.workflow.add_edge("fetch_signals", "optimize")
        self.workflow.add_edge("optimize", "factor_check")
        self.workflow.add_edge("factor_check", "plan_rebalance")
        self.workflow.add_edge("plan_rebalance", "estimate_costs")
        self.workflow.add_edge("estimate_costs", "finalize_allocation")
        self.workflow.add_edge("finalize_allocation", "finalize_run")
        self.workflow.add_edge("finalize_run", END)
        
        self.app = self.workflow.compile()

    async def run(self) -> Dict[str, Any]:
        """Execute the full pipeline."""
        initial_state: PortfolioPipelineState = {
            "approved_signals": [],
            "optimizer_result": {},
            "factor_result": {},
            "rebalance_plan": {},
            "cost_estimates": {},
            "final_allocation": {},
            "run_id": str(uuid.uuid4()),
            "portfolio_id": None,
            "status": "started",
            "error": None,
            "start_time": time.time()
        }
        
        try:
            return await self.app.ainvoke(initial_state)
        except Exception as e:
            logger.exception("Critical failure in PortfolioPipeline")
            return {"error": str(e), "status": "failed"}

    def get_current_portfolio(self) -> Dict[str, Any]:
        """Utility to fetch current allocation from Redis."""
        r = redis.from_url(settings.redis_url, decode_responses=True)
        data = r.get("portfolio:current:state")
        return json.loads(data) if data else {}
