import asyncio
import json
import redis
from portfolio_construction.agents.allocation_agent import AllocationAgent
from config.settings import settings

async def run_smoke_test():
    print("Starting Portfolio Allocation Agent Smoke Test")
    
    # 1. Mock Input Weights (from Optimizer/Factor)
    # Total = 100% (Should be scaled down to 95%)
    weights = {
        "AAPL": 0.40,
        "MSFT": 0.30,
        "GOOGL": 0.30
    }
    
    # 2. Add a manual override in Redis
    # Force AAPL to 20%
    r = redis.from_url(settings.redis_url, decode_responses=True)
    overrides = {"AAPL": 0.20}
    r.set("portfolio:manual:overrides", json.dumps(overrides))
    print("Set manual override (AAPL 20%) in Redis.")
    
    # 3. Add a circuit breaker for GOOGL
    r.set("risk:close_position:GOOGL", "true")
    print("Set circuit breaker for GOOGL in Redis.")
    
    # 4. Run Allocation Agent
    agent = AllocationAgent()
    print("Calculating final allocation...")
    allocation = await agent.allocate(weights, portfolio_value=100000.0)
    
    if allocation:
        print("\n[SUCCESS] Final Allocation Produced!")
        print(f"Total Invested: ${allocation.total_invested_usd:,.2f}")
        print(f"Cash: ${allocation.cash_usd:,.2f}")
        print(f"Positions: {allocation.n_positions}")
        
        for p in allocation.positions:
            print(f"  {p.ticker}: {p.weight:.2%} ({p.shares} shares @ ${p.current_price:.2f})")
            
        # Verify overrides and circuit breakers
        tickers = [p.ticker for p in allocation.positions]
        if "GOOGL" not in tickers:
            print("\nVerified: GOOGL excluded by circuit breaker.")
        if any(p.ticker == "AAPL" and p.weight <= 0.21 for p in allocation.positions):
            print("Verified: AAPL override applied.")
            
    else:
        print("\n[FAILED] Allocation Failed")
        
    # Cleanup
    r.delete("portfolio:manual:overrides")
    r.delete("risk:close_position:GOOGL")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
