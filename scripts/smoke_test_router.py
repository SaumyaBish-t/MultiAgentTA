import asyncio
import uuid
from execution.agents.smart_order_router_agent import SmartOrderRouter
from execution.agents.order_generation_agent import OrderBatch

async def test_router():
    print("Testing Smart Order Router Agent...")
    router = SmartOrderRouter()
    
    # 1. Test Emergency Liquidation (bypasses weekend check)
    print("\nRunning route_emergency(['AAPL'])...")
    try:
        result = await router.route_emergency(["AAPL"])
        print(f"SUCCESS: Routing Complete for Batch {result.batch_id}")
        print(f"  Submitted: {result.submitted}")
        print(f"  Failed:    {result.failed}")
        
        for o in result.submitted_orders:
            print(f"    - SUBMITTED: {o['ticker']} {o['action']} {o['requested_shares']} (Broker ID: {o['broker_order_id']})")
        
        for o in result.failed_orders:
            print(f"    - FAILED:    {o['ticker']} Reason: {o.get('reason')}")
            
    except Exception as e:
        print(f"FAILED: Emergency routing failed: {e}")

    # 2. Test manual routing with a mock batch
    print("\nRunning manual route() with mock batch...")
    mock_batch = OrderBatch(
        batch_id=uuid.uuid4(),
        batch_type="rebalance",
        orders=[
            {
                "ticker": "MSFT", "action": "buy", "shares": 10, 
                "order_type": "market", "time_in_force": "day",
                "internal_id": str(uuid.uuid4()) # In real run, this would be in DB
            }
        ],
        total_buy_value=4000.0,
        total_sell_value=0.0,
        execution_strategy="immediate",
        estimated_completion_time="Immediate"
    )
    # This might fail on pre-flight if market is closed, but let's see.
    result = await router.route(mock_batch)
    print(f"Manual Route Result: {result.submitted} submitted, {result.failed} failed.")

if __name__ == "__main__":
    asyncio.run(test_router())
