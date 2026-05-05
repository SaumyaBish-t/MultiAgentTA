import asyncio
from execution.agents.order_generation_agent import OrderGeneratorAgent

async def test_order_generation():
    print("Testing Order Generation Agent...")
    agent = OrderGeneratorAgent()
    
    # Mock Rebalance Plan from Phase 5
    # (Simplified version of what AllocationAgent outputs)
    rebalance_plan = {
        "portfolio_id": "main_portfolio",
        "trades": [
            {"ticker": "AAPL", "action": "buy", "shares": 147, "value": 40000},
            {"ticker": "MSFT", "action": "buy", "shares": 50, "value": 20000},
            {"ticker": "GOOGL", "action": "buy", "shares": 100, "value": 15000}
        ]
    }
    
    # 1. Test standard generation
    print("\nRunning generate_from_plan()...")
    batch = await agent.generate_from_plan(rebalance_plan)
    
    if batch:
        print(f"SUCCESS: Generated Batch {batch.batch_id}")
        print(f"  Orders Count: {len(batch.orders)}")
        print(f"  Total Buy Value: ${batch.total_buy_value:,.2f}")
        print(f"  Strategy: {batch.execution_strategy}")
        
        for o in batch.orders:
            print(f"    - {o['action'].upper()} {o['shares']} {o['ticker']} ({o['order_type']})")
    else:
        print("FAILED: No batch generated (check logs, might be market closed)")

    # 2. Test Emergency Close
    print("\nRunning generate_emergency_close(['AAPL'])...")
    emergency_batch = await agent.generate_emergency_close(["AAPL"])
    if emergency_batch:
        print(f"SUCCESS: Emergency Batch {emergency_batch.batch_id}")
        for o in emergency_batch.orders:
            print(f"    - {o['action'].upper()} {o['shares']} {o['ticker']} ({o['order_type']})")

if __name__ == "__main__":
    asyncio.run(test_order_generation())
