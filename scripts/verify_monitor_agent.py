import asyncio
import uuid
from unittest.mock import MagicMock, patch
from execution.agents.execution_monitor_agent import ExecutionMonitorAgent

async def test_monitor():
    print("Testing Execution Monitor Agent...")
    agent = ExecutionMonitorAgent()
    
    batch_id = uuid.uuid4()
    # Mock orders as they would appear in the Router output
    submitted_orders = [
        {
            "ticker": "AAPL",
            "broker_order_id": "alpaca-123",
            "action": "buy",
            "requested_shares": 100,
            "internal_id": str(uuid.uuid4())
        },
        {
            "ticker": "MSFT",
            "broker_order_id": "alpaca-456",
            "action": "buy",
            "requested_shares": 50,
            "internal_id": str(uuid.uuid4())
        }
    ]
    
    # 1. Mock the Adapter responses
    # AAPL = Filled, MSFT = Pending
    mock_responses = {
        "alpaca-123": {
            "status": "filled",
            "ticker": "AAPL",
            "filled_shares": 100,
            "filled_avg_price": 195.50,
            "broker_order_id": "alpaca-123",
            "action": "buy"
        },
        "alpaca-456": {
            "status": "new",
            "ticker": "MSFT",
            "broker_order_id": "alpaca-456",
            "submitted_at": "2026-05-02T10:00:00Z"
        }
    }
    
    with patch("execution.agents.execution_monitor_agent.AlpacaBrokerAdapter") as MockAdapter:
        adapter_instance = MockAdapter.return_value
        adapter_instance.get_order_status.side_effect = lambda bid: mock_responses[bid]
        adapter_instance.get_market_clock.return_value = {
            "is_open": True, 
            "next_close": "2026-05-04T16:00:00-04:00"
        }
        adapter_instance.get_account.return_value = {
            "cash": 95000.0, "portfolio_value": 105000.0, "buying_power": 190000.0
        }

        print("\nRunning single monitoring cycle...")
        result = await agent.run_monitoring_cycle(batch_id, submitted_orders)
        
        print(f"Cycle Result:")
        print(f"  Filled: {len(result['filled_orders'])}")
        print(f"  Pending: {len(result['pending_orders'])}")
        print(f"  Complete: {result['monitoring_complete']}")
        
        for f in result["filled_orders"]:
            print(f"    - FILLED: {f['ticker']} @ ${f['filled_avg_price']}")
            
        # Check if AAPL was removed from 'submitted_orders' in next cycle state
        remaining = result.get("submitted_orders", [])
        print(f"  Orders to monitor in next cycle: {[o['ticker'] for o in remaining]}")

if __name__ == "__main__":
    asyncio.run(test_monitor())
