"""Smoke test for Pre-Trade Compliance Agent."""
import asyncio
from compliance.agents.pre_trade_compliance import PreTradeCompliance

async def main():
    compliance = PreTradeCompliance()

    # TEST 1: Normal small buy — should PASS
    print("=" * 60)
    print("TEST 1: Small buy (should PASS)")
    d1 = await compliance.check({
        "ticker": "AAPL", "action": "buy", "shares": 5, "price": 190.0
    })
    print(f"  Approved: {d1.approved}  Checks: {d1.checks_run}  Violations: {len(d1.violations)}  Warnings: {len(d1.warnings)}")

    # TEST 2: Add TSLA to restricted list, then try to buy
    print("\nTEST 2: Restricted list block (should REJECT)")
    compliance.add_to_restricted_list("TSLA", "no_trade", "SEC investigation")
    d2 = await compliance.check({
        "ticker": "TSLA", "action": "buy", "shares": 10, "price": 250.0
    })
    print(f"  Approved: {d2.approved}  Reason: {d2.rejection_reason}")
    compliance.remove_from_restricted_list("TSLA")

    # TEST 3: Duplicate order check
    print("\nTEST 3: Duplicate order (result depends on pending orders in DB)")
    d3 = await compliance.check({
        "ticker": "AAPL", "action": "buy", "shares": 10, "price": 190.0
    })
    print(f"  Approved: {d3.approved}  Checks: {d3.checks_run}")

    # TEST 4: Sell order (most buy-only checks skip)
    print("\nTEST 4: Sell order (should PASS)")
    d4 = await compliance.check({
        "ticker": "AAPL", "action": "sell", "shares": 5, "price": 190.0
    })
    print(f"  Approved: {d4.approved}  Checks: {d4.checks_run}  Violations: {len(d4.violations)}")

    # TEST 5: Today's violations
    print("\nTEST 5: Violations today")
    violations = compliance.get_violations_today()
    print(f"  Total violations today: {len(violations)}")
    for v in violations[:3]:
        print(f"    {v['rule_id']} | {v['ticker']} | {v['severity']} | {v['description'][:60]}")

    print("\nAll tests complete.")

if __name__ == "__main__":
    asyncio.run(main())
