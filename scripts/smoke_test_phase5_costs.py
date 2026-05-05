import asyncio
from portfolio_construction.agents.cost_estimator_agent import CostEstimatorAgent

async def run_smoke_test():
    print("Starting Transaction Cost Estimator Smoke Test")
    
    # 1. Mock Trades
    # - Small trade: AAPL 100 shares
    # - Large trade: MSFT 50,000 shares (High market impact)
    # - Sell trade: GOOGL 100 shares (SEC fees)
    trades = [
        {"ticker": "AAPL", "action": "buy", "shares": 100},
        {"ticker": "MSFT", "action": "buy", "shares": 50000},
        {"ticker": "GOOGL", "action": "sell", "shares": 100}
    ]
    
    # 2. Run Cost Estimator
    agent = CostEstimatorAgent()
    print("Estimating costs for trades...")
    report = await agent.estimate(trades)
    
    if report:
        print("\n[SUCCESS] Cost Report Generated!")
        print(f"Total Friction: ${report.total_cost_usd:,.2f}")
        print(f"Total Friction (bps): {report.total_cost_bps:.1f} bps")
        
        print("\nPer Trade Breakdown:")
        for t in report.per_trade_breakdown:
            print(f"  {t['ticker']} ({t['action'].upper()} {t['shares']}):")
            print(f"    Value: ${t['trade_value']:,.0f}")
            print(f"    Spread Cost: ${t['spread_cost']:.2f}")
            print(f"    Market Impact: ${t['market_impact_usd']:.2f}")
            print(f"    SEC/FINRA Fees: ${t['sec_fee'] + t['finra_taf']:.2f}")
            print(f"    Total: ${t['total_cost']:.2f} ({t['total_cost_bps']:.1f} bps)")
            
        if report.high_cost_trades:
            print("\nHigh Cost Trades Detected:")
            for ticker in report.high_cost_trades:
                print(f"  - {ticker}")
                
        if report.recommendations:
            print("\nRecommendations:")
            for rec in report.recommendations:
                print(f"  - {rec}")
                
        print(f"\nOptimal Execution Window: {report.optimal_execution_window}")
    else:
        print("\n[FAILED] Cost Estimation Failed")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
