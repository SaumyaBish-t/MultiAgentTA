import asyncio
import json
from portfolio_construction.agents.factor_agent import FactorAgent

async def run_smoke_test():
    print("Starting Portfolio Factor Exposure Smoke Test")
    
    # 1. Mock portfolio weights (High Beta / Concentration risk intentionally)
    weights = {
        "AAPL": 0.40,  # 40% AAPL (High beta, High concentration)
        "MSFT": 0.30,
        "GOOGL": 0.30
    }
    
    # 2. Run Factor Agent
    agent = FactorAgent()
    print("Analyzing factor exposures...")
    result = await agent.analyze(weights)
    
    if result:
        print("\n[SUCCESS] Factor Analysis Complete!")
        print(f"Portfolio Market Beta: {result.factors.get('market_beta', 0):.2f}")
        print(f"Portfolio Value Score: {result.factors.get('value', 0):.2f}")
        print(f"Portfolio Momentum Score: {result.factors.get('momentum', 0):.2f}")
        print(f"Portfolio Quality Score: {result.factors.get('quality', 0):.2f}")
        print(f"Max Sector Weight: {result.factors.get('sector_max', 0):.2%}")
        print(f"Balance Score: {result.factors.get('balance_score', 0):.2f}")
        
        if result.breaches:
            print("\nBreaches Detected:")
            for breach in result.breaches:
                print(f"  - {breach}")
                
        if result.adjustments:
            print("\nSuggested Adjustments:")
            for adj in result.adjustments:
                print(f"  - {adj['ticker']}: {adj['current_weight']:.2%} -> {adj['suggested_weight']:.2%} ({adj['reason']})")
                
            print("\nAdjusted Weights:")
            for t, w in result.adjusted_weights.items():
                print(f"  {t}: {w:.2%}")
    else:
        print("\n[FAILED] Factor Analysis Failed")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
