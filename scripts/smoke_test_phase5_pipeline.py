import asyncio
import json
from portfolio_construction.pipeline.portfolio_pipeline import PortfolioPipeline

async def run_smoke_test():
    print("Starting Portfolio Construction Pipeline End-to-End Smoke Test")
    
    pipeline = PortfolioPipeline()
    print("Executing full pipeline...")
    result = await pipeline.run()
    
    if result.get("error"):
        print(f"\n[FAILED] Pipeline Error: {result['error']}")
    else:
        print("\n[SUCCESS] Pipeline Completed!")
        print(f"Run ID: {result['run_id']}")
        print(f"Status: {result['status']}")
        
        # Check Optimizer
        opt = result.get("optimizer_result", {})
        print(f"\nOptimizer: {opt.get('method')}")
        print(f"  Sharpe: {opt.get('sharpe', 0):.2f}")
        
        # Check Rebalance
        plan = result.get("rebalance_plan", {})
        print(f"\nRebalance Needed: {plan.get('needed')}")
        if plan.get("needed"):
            print(f"  Trigger: {plan.get('trigger_type')}")
            print(f"  Trades: {len(plan.get('trades', []))}")
            
        # Check Final Allocation
        alloc = result.get("final_allocation", {})
        if alloc:
            print(f"\nFinal Allocation:")
            print(f"  Total Invested: ${alloc.get('total_invested', 0):,.2f}")
            print(f"  Cash: ${alloc.get('cash', 0):,.2f}")
            print(f"  Positions: {len(alloc.get('positions', []))}")
            for p in alloc.get('positions', []):
                print(f"    - {p['ticker']}: {p['weight']:.1%} ({p['shares']} shares)")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
