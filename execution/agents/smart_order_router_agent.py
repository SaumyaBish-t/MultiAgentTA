import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import TypedDict, List, Dict, Any, Optional, Union
from dataclasses import dataclass

import redis
from loguru import logger
from sqlalchemy import create_engine, text
from langgraph.graph import StateGraph, END

from config.settings import settings
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter, InsufficientFundsError, InvalidTickerError
from execution.storage.execution_models import Order, OrderBatch as DBOrderBatch

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RouterState(TypedDict):
    order_batch: Dict[str, Any]          # from order generator
    pending_orders: List[Dict[str, Any]]
    submitted_orders: List[Dict[str, Any]]
    failed_orders: List[Dict[str, Any]]
    retry_queue: List[Dict[str, Any]]
    account_state: Dict[str, Any]
    current_prices: Dict[str, Any]
    submission_log: List[str]
    error: Optional[str]

@dataclass
class RoutingResult:
    batch_id: uuid.UUID
    submitted: int
    failed: int
    submitted_orders: List[Dict[str, Any]]
    failed_orders: List[Dict[str, Any]]
    total_value_submitted: float

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def pre_flight_check_node(state: RouterState) -> Dict[str, Any]:
    """Final safety checks before hitting the 'Submit' button."""
    adapter = AlpacaBrokerAdapter()
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    # 1. Market Open Check (unless emergency)
    clock = adapter.get_market_clock()
    if not clock["is_open"] and state["order_batch"].get("batch_type") != "emergency":
        logger.warning("Pre-flight: Market is closed. Aborting submission.")
        return {"error": "MARKET_CLOSED"}
        
    # 2. Risk Halt Check
    halted = r.get("risk:trading:halted")
    if halted == "True" and state["order_batch"].get("batch_type") != "emergency":
        logger.critical("Pre-flight: TRADING HALTED by risk system. Aborting.")
        return {"error": "TRADING_HALTED"}
        
    # 3. Account Data
    account = adapter.get_account()
    
    return {
        "account_state": account,
        "submission_log": ["Pre-flight checks passed"]
    }

async def sequence_orders_node(state: RouterState) -> Dict[str, Any]:
    """Sort orders to prioritize risk reduction and liquidity generation."""
    if state.get("error"): return {}
    
    orders = state["pending_orders"]
    
    def get_priority(o):
        action = o["action"].lower()
        if action == "close": return 1
        if action == "sell": return 2
        if action == "buy": return 3
        return 4
        
    # Priority: Close -> Sell -> Buy
    # Within priority: Largest value first
    sequenced = sorted(orders, key=lambda x: (get_priority(x), -x.get("value", 0)))
    
    return {"pending_orders": sequenced}

async def submit_orders_node(state: RouterState) -> Dict[str, Any]:
    """Submit sequenced orders to the broker with pre-trade compliance and rate limiting."""
    if state.get("error"): return {}
    
    from compliance.agents.pre_trade_compliance import PreTradeCompliance
    
    adapter = AlpacaBrokerAdapter()
    engine = create_engine(settings.postgres_url)
    compliance = PreTradeCompliance()
    
    submitted = []
    failed = []
    retries = []
    logs = state.get("submission_log", [])
    
    for order in state["pending_orders"]:
        # Rate limit: max 10 orders/sec
        await asyncio.sleep(0.1)
        
        ticker = order["ticker"]
        action = order["action"]
        shares = order["shares"]
        o_type = order["order_type"]
        
        # ── PRE-TRADE COMPLIANCE GATE ──
        # Skip compliance for emergency batches
        is_emergency = state["order_batch"].get("batch_type") == "emergency"
        if not is_emergency:
            decision = await compliance.check(order)
            if not decision.approved:
                logger.warning("PRE-TRADE REJECTED: {} — {}", ticker, decision.rejection_reason)
                failed.append({**order, "reason": f"COMPLIANCE: {decision.rejection_reason}"})
                logs.append(f"COMPLIANCE REJECTED: {action.upper()} {shares} {ticker} — {decision.rejection_reason}")
                continue
        
        try:
            result = None
            if o_type == "market":
                result = adapter.submit_market_order(
                    ticker=ticker,
                    shares=shares,
                    action=action
                )
            elif o_type == "limit":
                result = adapter.submit_limit_order(
                    ticker=ticker,
                    shares=shares,
                    action=action,
                    limit_price=order["limit_price"]
                )
            
            if result:
                # Update Order in DB
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE orders 
                        SET status = 'submitted', 
                            broker_order_id = :broker_id,
                            submitted_at = :now,
                            updated_at = :now
                        WHERE id = :id
                    """), {
                        "broker_id": result["broker_order_id"],
                        "now": datetime.now(timezone.utc),
                        "id": order["internal_id"]
                    })
                
                submitted.append(result)
                logs.append(f"Submitted: {action.upper()} {shares} {ticker} ({o_type})")
                
        except InsufficientFundsError:
            msg = f"Failed: Insufficient funds for {ticker}"
            logs.append(msg)
            failed.append({**order, "reason": "INSUFFICIENT_FUNDS"})
        except InvalidTickerError:
            msg = f"Failed: Invalid ticker {ticker}"
            logs.append(msg)
            failed.append({**order, "reason": "INVALID_TICKER"})
        except Exception as e:
            msg = f"Transient error for {ticker}: {str(e)}. Moving to retry queue."
            logs.append(msg)
            retries.append(order)
            
    return {
        "submitted_orders": submitted,
        "failed_orders": failed,
        "retry_queue": retries,
        "submission_log": logs
    }

async def retry_failed_orders_node(state: RouterState) -> Dict[str, Any]:
    """One-time retry for transient failures using market orders for certainty."""
    if state.get("error") or not state["retry_queue"]: return {}
    
    adapter = AlpacaBrokerAdapter()
    engine = create_engine(settings.postgres_url)
    
    submitted = state["submitted_orders"]
    failed = state["failed_orders"]
    logs = state["submission_log"]
    
    for order in state["retry_queue"]:
        await asyncio.sleep(2.0) # Grace period
        
        ticker = order["ticker"]
        action = order["action"]
        shares = order["shares"]
        
        try:
            # Retry always uses Market order to maximize fill probability
            result = adapter.submit_market_order(
                ticker=ticker,
                shares=shares,
                action=action
            )
            
            if result:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE orders 
                        SET status = 'submitted', 
                            broker_order_id = :broker_id,
                            submitted_at = :now,
                            updated_at = :now
                        WHERE id = :id
                    """), {
                        "broker_id": result["broker_order_id"],
                        "now": datetime.now(timezone.utc),
                        "id": order["internal_id"]
                    })
                
                submitted.append(result)
                logs.append(f"✅ Retry Success: {action.upper()} {shares} {ticker}")
                
        except Exception as e:
            msg = f"❌ Retry Failed for {ticker}: {str(e)}"
            logs.append(msg)
            failed.append({**order, "reason": f"RETRY_FAILED: {str(e)}"})
            
    return {
        "submitted_orders": submitted,
        "failed_orders": failed,
        "submission_log": logs
    }

async def update_batch_status_node(state: RouterState) -> Dict[str, Any]:
    """Finalize the batch record and notify the system."""
    if state.get("error"): return {}
    
    batch_id = state["order_batch"]["batch_id"]
    submitted = state["submitted_orders"]
    failed = state["failed_orders"]
    
    status = "completed" if not failed else "partial"
    if not submitted and failed:
        status = "failed"
        
    engine = create_engine(settings.postgres_url)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE order_batches 
            SET status = :status,
                filled_orders = :filled,
                failed_orders = :failed,
                completed_at = :now
            WHERE id = :id
        """), {
            "status": status,
            "filled": len(submitted),
            "failed": len(failed),
            "now": datetime.now(timezone.utc),
            "id": batch_id
        })
        
    # Publish to Redis
    r = redis.from_url(settings.redis_url, decode_responses=True)
    event = {
        "batch_id": str(batch_id),
        "status": status,
        "submitted_count": len(submitted),
        "failed_count": len(failed),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    r.publish("execution.orders.submitted", json.dumps(event))
    
    # Summary Log
    logger.info("\nSUBMISSION SUMMARY")
    logger.info("════════════════════════════════════════")
    logger.info(f"Batch ID:  {batch_id}")
    logger.info(f"Status:    {status.upper()}")
    logger.info(f"Submitted: {len(submitted)} orders")
    logger.info(f"Failed:    {len(failed)} orders")
    logger.info("════════════════════════════════════════")
    
    return {"error": None} # Clear state for next run if reuse occurs

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SmartOrderRouter:
    """Smart router for sequenced and reliable order submission."""
    
    def __init__(self):
        self.workflow = StateGraph(RouterState)
        
        self.workflow.add_node("pre_flight", pre_flight_check_node)
        self.workflow.add_node("sequence", sequence_orders_node)
        self.workflow.add_node("submit", submit_orders_node)
        self.workflow.add_node("retry", retry_failed_orders_node)
        self.workflow.add_node("finalize", update_batch_status_node)
        
        self.workflow.set_entry_point("pre_flight")
        self.workflow.add_edge("pre_flight", "sequence")
        self.workflow.add_edge("sequence", "submit")
        self.workflow.add_edge("submit", "retry")
        self.workflow.add_edge("retry", "finalize")
        self.workflow.add_edge("finalize", END)
        
        self.app = self.workflow.compile()

    async def route(self, order_batch: Any) -> RoutingResult:
        """Route a batch of orders to the broker."""
        initial_state: RouterState = {
            "order_batch": {
                "batch_id": order_batch.batch_id,
                "batch_type": getattr(order_batch, "batch_type", "rebalance")
            },
            "pending_orders": order_batch.orders,
            "submitted_orders": [],
            "failed_orders": [],
            "retry_queue": [],
            "account_state": {},
            "current_prices": {},
            "submission_log": [],
            "error": None
        }
        
        try:
            final_state = await self.app.ainvoke(initial_state)
            
            if final_state.get("error"):
                logger.error(f"Routing failed: {final_state['error']}")
                return RoutingResult(
                    batch_id=order_batch.batch_id,
                    submitted=0,
                    failed=len(order_batch.orders),
                    submitted_orders=[],
                    failed_orders=order_batch.orders,
                    total_value_submitted=0.0
                )
                
            submitted = final_state["submitted_orders"]
            failed = final_state["failed_orders"]
            
            total_val = sum(o["filled_shares"] * o["filled_avg_price"] if o["filled_shares"] > 0 else 0 for o in submitted)
            # Since these are just submitted, total_value might be 0 until filled. 
            # We'll calculate target value instead.
            target_val = sum(o["requested_shares"] * o["filled_avg_price"] for o in submitted if "filled_avg_price" in o)
            
            return RoutingResult(
                batch_id=order_batch.batch_id,
                submitted=len(submitted),
                failed=len(failed),
                submitted_orders=submitted,
                failed_orders=failed,
                total_value_submitted=target_val
            )
        except Exception as e:
            logger.exception(f"Routing process crashed: {e}")
            raise

    async def route_emergency(self, tickers: List[str]) -> RoutingResult:
        """Emergency liquidate tickers."""
        from execution.agents.order_generation_agent import OrderGeneratorAgent
        agent = OrderGeneratorAgent()
        batch = await agent.generate_emergency_close(tickers)
        if not batch:
            raise Exception("Failed to generate emergency orders")
        return await self.route(batch)

    def cancel_batch(self, batch_id: uuid.UUID) -> int:
        """Cancel all submitted but unfilled orders in a batch."""
        adapter = AlpacaBrokerAdapter()
        engine = create_engine(settings.postgres_url)
        with engine.connect() as conn:
            orders = conn.execute(text("""
                SELECT broker_order_id FROM orders 
                WHERE status IN ('submitted', 'partial')
                AND id IN (SELECT id FROM orders WHERE rebalance_id = :id) -- This logic depends on FK mapping
            """), {"id": batch_id}).fetchall()
            
        count = 0
        for o in orders:
            if o[0] and adapter.cancel_order(o[0]):
                count += 1
        return count
