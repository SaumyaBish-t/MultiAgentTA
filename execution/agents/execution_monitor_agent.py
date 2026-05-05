import asyncio
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import TypedDict, List, Dict, Any, Optional, Union
from dataclasses import dataclass

import redis
from loguru import logger
from sqlalchemy import create_engine, text
from langgraph.graph import StateGraph, END

from config.settings import settings
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter
from execution.storage.execution_models import Order, ExecutionPerformance, BrokerConnection

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MonitorState(TypedDict):
    batch_id: uuid.UUID
    submitted_orders: List[Dict[str, Any]]
    filled_orders: List[Dict[str, Any]]
    partial_orders: List[Dict[str, Any]]
    pending_orders: List[Dict[str, Any]]
    timed_out_orders: List[Dict[str, Any]]
    fill_rate: float
    total_filled_value: float
    monitoring_complete: bool
    error: Optional[str]

@dataclass
class MonitorResult:
    batch_id: uuid.UUID
    filled_count: int
    partial_count: int
    pending_count: int
    total_value: float
    avg_slippage_bps: float
    complete: bool

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def poll_order_status_node(state: MonitorState) -> Dict[str, Any]:
    """Check the status of all submitted orders at the broker."""
    adapter = AlpacaBrokerAdapter()
    engine = create_engine(settings.postgres_url)
    
    filled = []
    partial = []
    pending = []
    logs = []
    
    for order in state["submitted_orders"]:
        try:
            broker_id = order.get("broker_order_id")
            if not broker_id:
                logger.warning(f"No broker ID for order {order.get('ticker')}")
                continue
                
            status = adapter.get_order_status(broker_id)
            
            # 1. Handle Fills
            if status["status"] == "filled":
                filled.append(status)
                # Update Order in DB
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE orders 
                        SET status = 'filled', 
                            filled_shares = :filled,
                            filled_avg_price = :price,
                            filled_total_value = :val,
                            filled_at = :now,
                            updated_at = :now
                        WHERE broker_order_id = :bid
                    """), {
                        "filled": status["filled_shares"],
                        "price": status["filled_avg_price"],
                        "val": status["filled_shares"] * status["filled_avg_price"],
                        "now": datetime.now(timezone.utc),
                        "bid": broker_id
                    })
                logger.info(f"✅ FILLED: {status['filled_shares']} {status['ticker']} @ ${status['filled_avg_price']:.2f}")
                
            # 2. Handle Partials
            elif status["status"] == "partially_filled":
                partial.append(status)
                logger.info(f"⚠️ PARTIAL: {status['filled_shares']}/{status['requested_shares']} {status['ticker']}")
                
            # 3. Handle Pending
            elif status["status"] in ["new", "accepted", "pending_new"]:
                pending.append(status)
                
            # 4. Handle Failures
            elif status["status"] in ["cancelled", "rejected", "expired"]:
                logger.warning(f"❌ {status['status'].upper()}: {status['ticker']}")
                with engine.begin() as conn:
                    conn.execute(text("UPDATE orders SET status = :s WHERE broker_order_id = :bid"), 
                                 {"s": status["status"], "bid": broker_id})
        except Exception as e:
            logger.error(f"Poll failed for order {order.get('ticker')}: {e}")
            
    return {
        "filled_orders": filled,
        "partial_orders": partial,
        "pending_orders": pending,
        "submitted_orders": pending + partial # Continue monitoring these
    }

async def handle_timeouts_node(state: MonitorState) -> Dict[str, Any]:
    """Cancel long-pending limit orders and optionally convert to market orders."""
    if not state["pending_orders"]: return {}
    
    adapter = AlpacaBrokerAdapter()
    timed_out = []
    now = datetime.now(timezone.utc)
    
    for order in state["pending_orders"]:
        # Check submission time from DB or state
        # Assume status dict from Alpaca has submitted_at
        try:
            submitted_at = datetime.fromisoformat(order["submitted_at"])
            if now - submitted_at > timedelta(minutes=55):
                logger.warning(f"⏱️ TIMEOUT: Order for {order['ticker']} is > 55 min old. Cancelling.")
                cancelled = adapter.cancel_order(order["broker_order_id"])
                if cancelled:
                    timed_out.append(order)
                    # Convert to market if market still open (> 10 min left)
                    clock = adapter.get_market_clock()
                    next_close = datetime.fromisoformat(clock["next_close"])
                    if next_close - now > timedelta(minutes=10):
                        logger.info(f"Resubmitting {order['ticker']} as Market order for finality.")
                        adapter.submit_market_order(
                            ticker=order["ticker"],
                            shares=order["requested_shares"] - order["filled_shares"],
                            action=order["action"]
                        )
        except Exception as e:
            logger.error(f"Timeout handler failed: {e}")
            
    return {"timed_out_orders": timed_out}

async def calculate_fill_metrics_node(state: MonitorState) -> Dict[str, Any]:
    """Record execution performance metrics (slippage/market impact)."""
    if not state["filled_orders"]: return {}
    
    engine = create_engine(settings.postgres_url)
    total_slippage = 0
    
    for o in state["filled_orders"]:
        try:
            ticker = o["ticker"]
            # Fetch 'arrival_price' which was 'requested_price' or price at submission
            # For simplicity, we'll fetch from the orders table
            with engine.connect() as conn:
                arrival = conn.execute(text("SELECT requested_price FROM orders WHERE broker_order_id = :bid"), 
                                       {"bid": o["broker_order_id"]}).scalar()
            
            if not arrival: # If market order, requested_price might be null, so use filled price as benchmark (bad but safe)
                arrival = o["filled_avg_price"]
                
            execution_price = o["filled_avg_price"]
            
            # Slippage Calculation
            if o["action"] == "buy":
                slippage_pct = (execution_price - arrival) / arrival if arrival > 0 else 0
            else:
                slippage_pct = (arrival - execution_price) / arrival if arrival > 0 else 0
                
            slippage_bps = slippage_pct * 10000
            total_slippage += slippage_bps
            
            # Write Performance
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO execution_performance (
                        id, order_id, ticker, arrival_price, execution_price, slippage_bps, benchmark, measured_at
                    ) VALUES (
                        :id, (SELECT id FROM orders WHERE broker_order_id = :bid), :ticker, :arr, :exec, :slip, 'arrival', :now
                    )
                """), {
                    "id": uuid.uuid4(),
                    "bid": o["broker_order_id"],
                    "ticker": ticker,
                    "arr": arrival,
                    "exec": execution_price,
                    "slip": slippage_bps,
                    "now": datetime.now(timezone.utc)
                })
            
            if slippage_bps > 30:
                logger.warning(f"HIGH SLIPPAGE: {ticker} {slippage_bps:.1f} bps")
                
        except Exception as e:
            logger.error(f"Performance calc failed for {o.get('ticker')}: {e}")
            
    return {"total_filled_value": sum(o["filled_shares"] * o["filled_avg_price"] for o in state["filled_orders"])}

async def update_portfolio_positions_node(state: MonitorState) -> Dict[str, Any]:
    """Sync the local portfolio state with filled execution results."""
    if not state["filled_orders"]: return {}
    
    engine = create_engine(settings.postgres_url)
    adapter = AlpacaBrokerAdapter()
    
    for o in state["filled_orders"]:
        try:
            ticker = o["ticker"]
            shares = o["filled_shares"]
            price = o["filled_avg_price"]
            
            with engine.begin() as conn:
                # Update portfolio_positions status
                # (Assumes Phase 5 created these with status='pending')
                conn.execute(text("""
                    UPDATE portfolio_positions 
                    SET current_shares = :shares,
                        current_price = :price,
                        current_value_usd = :val,
                        status = 'active',
                        opened_at = :now,
                        updated_at = :now
                    WHERE ticker = :ticker AND status = 'pending'
                """), {
                    "shares": shares,
                    "price": price,
                    "val": shares * price,
                    "ticker": ticker,
                    "now": datetime.now(timezone.utc)
                })
        except Exception as e:
            logger.error(f"Portfolio sync failed for {o['ticker']}: {e}")
            
    # Sync Broker Connection Table
    try:
        acc = adapter.get_account()
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE broker_connections 
                SET cash_balance = :cash,
                    portfolio_value = :val,
                    buying_power = :bp,
                    last_synced_at = :now
                WHERE broker_name = 'alpaca'
            """), {
                "cash": acc["cash"],
                "val": acc["portfolio_value"],
                "bp": acc["buying_power"],
                "now": datetime.now(timezone.utc)
            })
    except Exception as e:
        logger.error(f"Broker sync failed: {e}")
        
    return {"status": "positions_synced"}

async def finalize_monitoring_node(state: MonitorState) -> Dict[str, Any]:
    """Conclude the monitoring process when no orders remain pending."""
    if state["pending_orders"]:
        return {"monitoring_complete": False}
        
    batch_id = state["batch_id"]
    engine = create_engine(settings.postgres_url)
    
    with engine.begin() as conn:
        conn.execute(text("UPDATE order_batches SET status = 'completed', completed_at = :now WHERE id = :id"), 
                     {"id": batch_id, "now": datetime.now(timezone.utc)})
        
    r = redis.from_url(settings.redis_url, decode_responses=True)
    r.publish("execution.batch.completed", json.dumps({
        "batch_id": str(batch_id),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }))
    
    logger.info(f"EXECUTION COMPLETE: Batch {batch_id} fully resolved.")
    return {"monitoring_complete": True}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 4 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ExecutionMonitorAgent:
    """Monitors order fill lifecycle and updates system state."""
    
    def __init__(self):
        self.workflow = StateGraph(MonitorState)
        
        self.workflow.add_node("poll", poll_order_status_node)
        self.workflow.add_node("timeout", handle_timeouts_node)
        self.workflow.add_node("metrics", calculate_fill_metrics_node)
        self.workflow.add_node("sync", update_portfolio_positions_node)
        self.workflow.add_node("finalize", finalize_monitoring_node)
        
        self.workflow.set_entry_point("poll")
        self.workflow.add_edge("poll", "timeout")
        self.workflow.add_edge("timeout", "metrics")
        self.workflow.add_edge("metrics", "sync")
        self.workflow.add_edge("sync", "finalize")
        self.workflow.add_edge("finalize", END)
        
        self.app = self.workflow.compile()

    async def run_monitoring_cycle(self, batch_id: uuid.UUID, submitted_orders: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run a single loop of the monitoring workflow."""
        initial_state: MonitorState = {
            "batch_id": batch_id,
            "submitted_orders": submitted_orders,
            "filled_orders": [],
            "partial_orders": [],
            "pending_orders": [],
            "timed_out_orders": [],
            "fill_rate": 0.0,
            "total_filled_value": 0.0,
            "monitoring_complete": False,
            "error": None
        }
        
        try:
            return await self.app.ainvoke(initial_state)
        except Exception as e:
            logger.exception(f"Monitoring cycle crashed: {e}")
            return {"error": str(e), "monitoring_complete": False}

    async def monitor_until_complete(self, batch_id: uuid.UUID, orders: List[Dict[str, Any]], timeout_min: int = 60):
        """Loop until all orders in batch are filled or cancelled."""
        start_time = datetime.now()
        current_orders = orders
        
        while True:
            result = await self.run_monitoring_cycle(batch_id, current_orders)
            
            if result.get("monitoring_complete"):
                break
                
            if datetime.now() - start_time > timedelta(minutes=timeout_min):
                logger.warning(f"Monitoring timeout for batch {batch_id}")
                break
                
            # Update list of orders to monitor for next cycle (only pending/partial)
            current_orders = result.get("submitted_orders", [])
            
            await asyncio.sleep(10)
