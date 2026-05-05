import asyncio
import json
import redis
from portfolio_construction.agents.rebalancing_agent import RebalancingAgent
from config.settings import settings

async def run_smoke_test():
    print("Starting Portfolio Rebalancing Smoke Test")
    
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    # 1. Mock Target Weights (AAPL 10%, MSFT 45%, GOOGL 45%)
    target_weights = {
        "AAPL": 0.10,
        "MSFT": 0.45,
        "GOOGL": 0.45
    }
    r.set("portfolio:target:weights", json.dumps(target_weights))
    print("Set target weights in Redis.")
    
    # 2. Mock Current State (Currently AAPL 100% - Large drift)
    # Portfolio value $100,000
    current_state = {
        "value": 100000.0,
        "positions": [
            {
                "ticker": "AAPL",
                "shares": 500,
                "price": 200.0,
                "market_value": 100000.0
            }
        ]
    }
    r.set("portfolio:current:state", json.dumps(current_state))
    print("Set current portfolio state in Redis.")
    
    # 3. Run Rebalancing Agent
    agent = RebalancingAgent()
    print("Running rebalance check...")
    plan = await agent.check_and_plan()
    
    if plan:
        print("\n[SUCCESS] Rebalance Plan Generated!")
        print(f"Needed: {plan.needed}")
        print(f"Trigger: {plan.trigger_type}")
        print(f"Max Drift: {plan.max_drift:.2%}")
        print(f"Estimated Cost: ${plan.estimated_cost:.2f}")
        print(f"Cost/Benefit Ratio: {plan.cost_benefit_ratio:.2f}")
        print(f"Approved for Execution: {plan.approved}")
        
        print("\nTrades Required:")
        for t in plan.trades:
            print(f"  - {t['action'].upper()} {t['shares']} shares of {t['ticker']} (~${t['estimated_value']:,.0f})")
    else:
        print("\n[FAILED] Rebalance Plan Failed")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
