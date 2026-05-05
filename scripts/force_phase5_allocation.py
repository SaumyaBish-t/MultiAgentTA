import asyncio
import json
import redis
import uuid
from sqlalchemy import create_engine, text
from config.settings import settings
from portfolio_construction.agents.allocation_agent import AllocationAgent

async def force_allocation():
    print("Forcing Phase 5 Allocation for Verification...")
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    # 1. Set target weights
    weights = {'AAPL': 0.4, 'MSFT': 0.3, 'GOOGL': 0.25}
    r.set("portfolio:target:weights", json.dumps(weights))
    
    # 2. Run Allocation Agent directly
    agent = AllocationAgent()
    # We'll mock the rebalance plan to force it
    alloc = await agent.allocate(weights)
    
    if alloc:
        print(f"SUCCESS: Allocated {len(alloc.positions)} positions.")
        for p in alloc.positions:
            print(f"  - {p.ticker}: {p.shares} shares (Status: pending)")
    else:
        print("FAILED: Allocation agent returned None.")

if __name__ == "__main__":
    asyncio.run(force_allocation())
