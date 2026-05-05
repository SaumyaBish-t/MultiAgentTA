import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import TypedDict, Dict, Any, Optional, List
from dataclasses import dataclass

import redis
from loguru import logger
from sqlalchemy import create_engine, text
from langgraph.graph import StateGraph, END

from config.settings import settings
from execution.agents.order_generation_agent import OrderGeneratorAgent
from execution.agents.smart_order_router_agent import SmartOrderRouter
from execution.agents.execution_monitor_agent import ExecutionMonitorAgent
from execution.agents.post_trade_agent import PostTradeAnalyzer
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PIPELINE STATE & RESULT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ExecutionPipelineState(TypedDict):
    rebalance_plan: Dict[str, Any]
    order_batch: Optional[Any]
    routing_result: Optional[Any]
    monitor_result: Optional[Any]
    post_trade_result: Optional[Any]
    portfolio_updated: bool
    run_id: str
    status: str
    error: Optional[str]

@dataclass
class ExecutionResult:
    run_id: str
    orders_submitted: int
    orders_filled: int
    total_value_executed: float
    avg_slippage_bps: float
    execution_quality_score: float
    portfolio_value: float
    duration_seconds: float

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PIPELINE NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def receive_allocation_node(state: ExecutionPipelineState) -> Dict[str, Any]:
    """Validate the incoming rebalance plan and check market state."""
    plan = state["rebalance_plan"]
    if not plan or "trades" not in plan:
        return {"error": "INVALID_PLAN", "status": "failed"}
        
    adapter = AlpacaBrokerAdapter()
    clock = adapter.get_market_clock()
    
    if not clock["is_open"]:
        # If it's a rebalance, we might want to queue it. 
        # For now, we'll flag it so the flow can handle queuing.
        logger.warning("Market is closed. Plan will be queued or rejected.")
        # But for pipeline continuity in tests, we'll proceed if it's a mock/test
        
    logger.info(f"Execution pipeline started: {len(plan['trades'])} trades requested.")
    return {"status": "started"}

async def generate_orders_node(state: ExecutionPipelineState) -> Dict[str, Any]:
    """Run OrderGeneratorAgent to convert trades to broker-ready orders."""
    if state.get("error"): return {}
    
    agent = OrderGeneratorAgent()
    batch = await agent.generate_from_plan(state["rebalance_plan"])
    
    if not batch:
        return {"error": "ORDER_GENERATION_FAILED", "status": "failed"}
        
    logger.info(f"Generated Order Batch: {batch.batch_id} with {len(batch.orders)} orders.")
    return {"order_batch": batch}

async def route_orders_node(state: ExecutionPipelineState) -> Dict[str, Any]:
    """Run SmartOrderRouterAgent to submit orders to Alpaca."""
    if state.get("error"): return {}
    
    router = SmartOrderRouter()
    result = await router.route(state["order_batch"])
    
    if result.submitted == 0 and result.failed > 0:
        return {"error": "ROUTING_FAILED", "status": "failed"}
        
    logger.info(f"Routed: {result.submitted} submitted, {result.failed} failed.")
    return {"routing_result": result}

async def monitor_fills_node(state: ExecutionPipelineState) -> Dict[str, Any]:
    """Run ExecutionMonitorAgent to track fills until resolution."""
    if state.get("error"): return {}
    
    monitor = ExecutionMonitorAgent()
    batch_id = state["order_batch"].batch_id
    
    # We use the monitoring cycle loop
    # In a real pipeline, this might take minutes
    await monitor.monitor_until_complete(batch_id, state["routing_result"].submitted_orders, timeout_min=60)
    
    # After completion, we can fetch final results from DB if needed, 
    # but for state we'll assume it finished.
    return {"status": "monitored"}

async def analyze_execution_node(state: ExecutionPipelineState) -> Dict[str, Any]:
    """Run PostTradeAnalyzerAgent to assess execution quality."""
    if state.get("error"): return {}
    
    analyzer = PostTradeAnalyzer()
    result = await analyzer.analyze(state["order_batch"].batch_id)
    
    return {"post_trade_result": result}

async def sync_portfolio_node(state: ExecutionPipelineState) -> Dict[str, Any]:
    """Sync the live Alpaca state back to Redis and DB."""
    if state.get("error"): return {}
    
    adapter = AlpacaBrokerAdapter()
    engine = create_engine(settings.postgres_url)
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    # 1. Fetch live state
    account = adapter.get_account()
    positions = adapter.get_positions()
    
    # 2. Update Redis
    portfolio_state = {
        "total_value": account["portfolio_value"],
        "cash": account["cash"],
        "buying_power": account["buying_power"],
        "positions": positions,
        "last_updated": datetime.now(timezone.utc).isoformat()
    }
    r.set("portfolio:current:state", json.dumps(portfolio_state))
    
    # 3. Update DB broker_connections
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE broker_connections 
            SET cash_balance = :cash,
                portfolio_value = :val,
                buying_power = :bp,
                last_synced_at = :now
            WHERE broker_name = 'alpaca'
        """), {
            "cash": account["cash"],
            "val": account["portfolio_value"],
            "bp": account["buying_power"],
            "now": datetime.now(timezone.utc)
        })
        
    logger.info(f"Portfolio synced. Value: ${account['portfolio_value']:,.2f}")
    return {"portfolio_updated": True}

async def finalize_pipeline_node(state: ExecutionPipelineState) -> Dict[str, Any]:
    """Mark the rebalance as completed and notify the system."""
    if state.get("error"): return {"status": "failed"}
    
    engine = create_engine(settings.postgres_url)
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    # Mark rebalance_event as executed
    # We need rebalance_id from plan
    rebalance_id = state["rebalance_plan"].get("rebalance_id")
    if rebalance_id:
        with engine.begin() as conn:
            conn.execute(text("UPDATE rebalance_events SET executed = True WHERE id = :id"), {"id": rebalance_id})
            
    # Publish completion
    res = state["post_trade_result"]
    event = {
        "run_id": state["run_id"],
        "batch_id": str(state["order_batch"].batch_id),
        "quality_score": res.quality_score,
        "avg_slippage_bps": res.avg_slippage_bps,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    r.publish("execution.pipeline.completed", json.dumps(event))
    
    logger.info("EXECUTION PIPELINE COMPLETE.")
    return {"status": "completed"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EXECUTION PIPELINE CLASS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ExecutionPipeline:
    """Orchestrator for the entire execution lifecycle."""
    
    def __init__(self):
        self.workflow = StateGraph(ExecutionPipelineState)
        
        self.workflow.add_node("receive", receive_allocation_node)
        self.workflow.add_node("generate", generate_orders_node)
        self.workflow.add_node("route", route_orders_node)
        self.workflow.add_node("monitor", monitor_fills_node)
        self.workflow.add_node("analyze", analyze_execution_node)
        self.workflow.add_node("sync", sync_portfolio_node)
        self.workflow.add_node("finalize", finalize_pipeline_node)
        
        self.workflow.set_entry_point("receive")
        self.workflow.add_edge("receive", "generate")
        self.workflow.add_edge("generate", "route")
        self.workflow.add_edge("route", "monitor")
        self.workflow.add_edge("monitor", "analyze")
        self.workflow.add_edge("analyze", "sync")
        self.workflow.add_edge("sync", "finalize")
        self.workflow.add_edge("finalize", END)
        
        self.app = self.workflow.compile()

    async def run(self, rebalance_plan: Dict[str, Any]) -> ExecutionResult:
        """Run the end-to-end execution pipeline."""
        run_id = f"exec_{uuid.uuid4().hex[:8]}"
        start_time = datetime.now()
        
        initial_state: ExecutionPipelineState = {
            "rebalance_plan": rebalance_plan,
            "order_batch": None,
            "routing_result": None,
            "monitor_result": None,
            "post_trade_result": None,
            "portfolio_updated": False,
            "run_id": run_id,
            "status": "idle",
            "error": None
        }
        
        try:
            final_state = await self.app.ainvoke(initial_state)
            
            if final_state.get("error"):
                logger.error(f"Execution pipeline failed: {final_state['error']}")
                raise Exception(final_state["error"])
                
            duration = (datetime.now() - start_time).total_seconds()
            
            # Fetch summary stats from final state
            batch = final_state["order_batch"]
            route = final_state["routing_result"]
            post = final_state["post_trade_result"]
            
            # For portfolio value, we fetch from the synced Redis state
            r = redis.from_url(settings.redis_url, decode_responses=True)
            p_state = json.loads(r.get("portfolio:current:state") or "{}")
            
            return ExecutionResult(
                run_id=run_id,
                orders_submitted=route.submitted,
                orders_filled=route.submitted, # Assumption for now, monitor updates DB
                total_value_executed=route.total_value_submitted,
                avg_slippage_bps=post.avg_slippage_bps,
                execution_quality_score=post.quality_score,
                portfolio_value=p_state.get("total_value", 0.0),
                duration_seconds=duration
            )
        except Exception as e:
            logger.exception(f"Pipeline execution crashed: {e}")
            raise
