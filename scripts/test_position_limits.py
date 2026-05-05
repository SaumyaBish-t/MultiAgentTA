"""Smoke test for Position Limit Agent."""
import asyncio
from compliance.agents.position_limit_agent import PositionLimitAgent

async def main():
    agent = PositionLimitAgent()

    # Full check
    print("=" * 60)
    print("POSITION LIMIT CHECK")
    print("=" * 60)
    result = await agent.check()
    print(f"Status:      {result.overall_status}")
    print(f"Portfolio:   ${result.portfolio_value:,.0f}")
    print(f"Positions:   {result.position_count}")
    print(f"HHI:         {result.hhi}")
    print(f"Breaches:    {len(result.breaches)}")
    for b in result.breaches:
        ticker = b.get("ticker", b.get("sector", "N/A"))
        print(f"  {b.get('rule'):35s} | {ticker:5s} | {b.get('severity'):10s} | {b.get('current', 0):.2%}")
    print(f"Warnings:    {len(result.warnings)}")
    for w in result.warnings:
        print(f"  {w.get('rule', 'N/A'):35s} | {w}")
    print(f"Actions:     {len(result.actions_taken)}")
    for a in result.actions_taken:
        print(f"  {a}")

    # Concentration report
    print("\nCONCENTRATION REPORT")
    print("-" * 40)
    report = agent.get_concentration_report()
    print(f"HHI:                 {report['hhi']}")
    print(f"Equiv Positions:     {report['equivalent_positions']}")
    print(f"Top positions:")
    for ticker, w in report["top_positions"]:
        print(f"  {ticker:6s} {w:.1%}")
    print(f"Sector weights:")
    for sector, w in report["sector_weights"].items():
        print(f"  {sector:20s} {w:.1%}")

    # Buy blocked?
    blocked = agent.is_new_buy_blocked()
    print(f"\nNew buys blocked: {blocked}")

if __name__ == "__main__":
    asyncio.run(main())
