import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

import redis
from loguru import logger
from sqlalchemy import create_engine, text

from config.settings import settings
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EMERGENCY HANDLER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class EmergencyHandler:
    """
    Immediate response unit for risk-triggered liquidations.
    Prioritizes speed and reliability over cost optimization.
    """
    
    def __init__(self):
        self.adapter = AlpacaBrokerAdapter()
        self.r = redis.from_url(settings.redis_url, decode_responses=True)
        self.engine = create_engine(settings.postgres_url)
        self._listener_task: Optional[asyncio.Task] = None

    async def handle_close_all(self, reason: str = "Circuit Breaker Triggered") -> Dict[str, Any]:
        """Liquidate the entire portfolio immediately and halt trading."""
        logger.critical(f"⚠️ EMERGENCY: Closing all positions. Reason: {reason}")
        
        # 1. Cancel all pending orders first to clear the way
        cancelled_count = self.adapter.cancel_all_orders()
        logger.warning(f"Cancelled {cancelled_count} pending orders.")
        
        # 2. Close all positions via Alpaca (submits market orders for everything)
        close_orders = self.adapter.close_all_positions()
        logger.info(f"Submitted {len(close_orders)} close orders to Alpaca.")
        
        # 3. Update Database & Redis State
        with self.engine.begin() as conn:
            # Update positions to 'closing'
            conn.execute(text("UPDATE portfolio_positions SET status = 'closing' WHERE status != 'closed'"))
            
            # Record the close orders
            for order in close_orders:
                order_id = uuid.uuid4()
                conn.execute(text("""
                    INSERT INTO orders (
                        id, ticker, action, order_type, requested_shares, 
                        status, broker_order_id, time_in_force, 
                        filled_shares, commission_paid, extended_hours,
                        created_at, updated_at
                    ) VALUES (
                        :id, :ticker, :action, 'market', :shares, 
                        'submitted', :bid, 'day', 
                        0, 0.0, False,
                        :now, :now
                    )
                """), {
                    "id": order_id,
                    "ticker": order["ticker"],
                    "action": order["action"],
                    "shares": order["requested_shares"],
                    "bid": order["broker_order_id"],
                    "now": datetime.now(timezone.utc)
                })
        
        # 4. Set Safety Flags in Redis
        self.r.set("risk:trading:halted", "True")
        self.r.set("portfolio:emergency:active", "True")
        self.r.set("portfolio:emergency:reason", reason)
        
        # 5. Notify the system
        event = {
            "event": "CLOSE_ALL",
            "reason": reason,
            "orders_count": len(close_orders),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self.r.publish("execution.emergency.close_all", json.dumps(event))
        
        # 6. Kick off fill monitor in background
        asyncio.create_task(self._monitor_emergency_fills(close_orders))
        
        return {"success": True, "orders": len(close_orders), "cancelled": cancelled_count}

    async def handle_reduce_all(self, factor: float = 0.5, reason: str = "Deleveraging") -> Dict[str, Any]:
        """Reduce all positions by a specific factor (default 50%)."""
        logger.warning(f"⚠️ REDUCE: Reducing all positions by {factor:.0%}. Reason: {reason}")
        
        positions = self.adapter.get_positions()
        sell_orders = []
        
        for pos in positions:
            shares_to_sell = int(pos["shares"] * factor)
            if shares_to_sell > 0:
                try:
                    order = self.adapter.submit_market_order(
                        ticker=pos["ticker"],
                        shares=shares_to_sell,
                        action="sell"
                    )
                    sell_orders.append(order)
                    logger.info(f"Reduced {pos['ticker']} by {shares_to_sell} shares.")
                except Exception as e:
                    logger.error(f"Failed to reduce {pos['ticker']}: {e}")
                    
        # Update state and notify
        self.r.publish("execution.emergency.reduce", json.dumps({
            "factor": factor,
            "orders_count": len(sell_orders),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))
        
        return {"success": True, "orders": len(sell_orders)}

    async def handle_force_close(self, ticker: str, reason: str = "Risk Limit Breached") -> Dict[str, Any]:
        """Liquidate a specific ticker immediately."""
        logger.warning(f"⚠️ FORCE CLOSE: {ticker}. Reason: {reason}")
        
        # 1. Cancel pending orders for this ticker
        open_orders = self.adapter.get_open_orders()
        for o in open_orders:
            if o["ticker"] == ticker:
                self.adapter.cancel_order(o["broker_order_id"])
                
        # 2. Close the position
        try:
            order = self.adapter.close_position(ticker)
            
            # 3. Update DB
            with self.engine.begin() as conn:
                conn.execute(text("UPDATE portfolio_positions SET status = 'closing' WHERE ticker = :t"), {"t": ticker})
            
            # 4. Cleanup
            self.r.delete(f"risk:close_position:{ticker}")
            
            self.r.publish("execution.emergency.force_close", json.dumps({
                "ticker": ticker,
                "order_id": order["broker_order_id"],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }))
            
            return {"success": True, "ticker": ticker, "order_id": order["broker_order_id"]}
        except Exception as e:
            logger.error(f"Force close failed for {ticker}: {e}")
            return {"success": False, "error": str(e)}

    async def _monitor_emergency_fills(self, orders: List[Dict[str, Any]]):
        """Background task to confirm fills for emergency liquidations."""
        from execution.agents.execution_monitor_agent import ExecutionMonitorAgent
        monitor = ExecutionMonitorAgent()
        
        logger.info("Starting background monitor for emergency fills...")
        # Use a temporary batch ID for monitoring
        batch_id = uuid.uuid4()
        await monitor.monitor_until_complete(batch_id, orders, timeout_min=30)
        logger.info(f"Emergency fill monitoring complete for {len(orders)} orders.")

    def get_emergency_status(self) -> Dict[str, Any]:
        """Get current emergency status from Redis."""
        return {
            "halted": self.r.get("risk:trading:halted") == "True",
            "emergency_active": self.r.get("portfolio:emergency:active") == "True",
            "reason": self.r.get("portfolio:emergency:reason")
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # REDIS LISTENER
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def start_listener(self):
        """Launch the background Redis subscriber."""
        if self._listener_task and not self._listener_task.done():
            logger.warning("Emergency listener is already running.")
            return
            
        self._listener_task = asyncio.create_task(self._listen_loop())
        logger.info("Emergency Execution Handler listening for Redis events...")

    async def _listen_loop(self):
        """Subscribe to risk channels and dispatch handlers."""
        pubsub = self.r.pubsub()
        pubsub.subscribe(
            "risk.circuit_breaker.emergency",
            "risk.circuit_breaker.reduce",
            "risk.position.force_close"
        )
        
        try:
            while True:
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message:
                    channel = message["channel"]
                    data = json.loads(message["data"])
                    
                    if "emergency" in channel:
                        await self.handle_close_all(reason=data.get("reason", "Circuit Breaker"))
                        
                    elif "reduce" in channel:
                        await self.handle_reduce_all(
                            factor=data.get("factor", 0.5),
                            reason=data.get("reason", "Deleveraging")
                        )
                        
                    elif "force_close" in channel:
                        ticker = data.get("ticker")
                        if ticker:
                            await self.handle_force_close(ticker, reason=data.get("reason", "Manual/Risk"))
                            
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            logger.info("Emergency listener task cancelled.")
        except Exception as e:
            logger.error(f"Emergency listener error: {e}")
            # Optional: Restart loop after delay
            await asyncio.sleep(5)
            asyncio.create_task(self._listen_loop())
