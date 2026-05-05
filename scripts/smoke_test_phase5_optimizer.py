import asyncio
import uuid
from portfolio_construction.agents.optimizer_agent import PortfolioOptimizer

async def run_smoke_test():
    print("Starting Portfolio Optimizer Smoke Test")
    
    # 1. Mock risk-approved signals
    signals = [
        {
            "ticker": "AAPL",
            "approved_position_size_usd": 10000.0,
            "risk_score": 0.85,
            "direction": "long",
            "conviction_score": 0.9
        },
        {
            "ticker": "MSFT",
            "approved_position_size_usd": 8000.0,
            "risk_score": 0.70,
            "direction": "long",
            "conviction_score": 0.7
        },
        {
            "ticker": "GOOGL",
            "approved_position_size_usd": 5000.0,
            "risk_score": 0.60,
            "direction": "long",
            "conviction_score": 0.6
        }
    ]
    
    # 2. Run Optimizer
    optimizer = PortfolioOptimizer()
    print("Running optimization...")
    result = await optimizer.optimize(signals)
    
    if result:
        print("\n[SUCCESS] Optimization Successful!")
        print(f"Method: {result.optimization_method}")
        print(f"Expected Return: {result.expected_return:.2%}")
        print(f"Expected Volatility: {result.expected_volatility:.2%}")
        print(f"Sharpe Ratio: {result.expected_sharpe:.2f}")
        print(f"Cash Weight: {result.cash_weight:.2%}")
        
        print("\nTarget Weights:")
        for ticker, weight in result.weights.items():
            print(f"  {ticker}: {weight:.2%}")
            
        print("\nConstraints Applied:")
        for constraint in result.constraints_applied:
            print(f"  - {constraint}")
    else:
        print("\n[FAILED] Optimization Failed")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
