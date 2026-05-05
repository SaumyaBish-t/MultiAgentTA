"""Smoke test for Wash Sale & PDT trackers."""
import asyncio
from compliance.agents.wash_sale_pdt_tracker import wash_sale_tracker, pdt_tracker

async def main():
    print("=" * 60)
    print("WASH SALE TRACKER TESTS")
    print("=" * 60)

    # 1. Record a loss sale for MSFT
    print("\n[1] Recording MSFT loss sale (bought $420, sold $400, 10 shares)")
    result = await wash_sale_tracker.record_sale(
        ticker="MSFT", shares=10, price=400.0, cost_basis=420.0
    )
    print(f"    Window opened: {result}")

    # 2. Record a profit sale for AAPL (should NOT open window)
    print("\n[2] Recording AAPL profit sale (bought $180, sold $195)")
    result2 = await wash_sale_tracker.record_sale(
        ticker="AAPL", shares=5, price=195.0, cost_basis=180.0
    )
    print(f"    Window opened: {result2}")

    # 3. Check if buying MSFT triggers wash sale
    print("\n[3] Checking MSFT purchase (should be wash sale)")
    check = wash_sale_tracker.check_purchase("MSFT")
    print(f"    Is wash sale: {check['is_wash_sale']}")
    if check["is_wash_sale"]:
        print(f"    Warning: {check['warning']}")
        print(f"    Days remaining: {check['days_remaining']}")

    # 4. Check AAPL purchase (no wash sale)
    print("\n[4] Checking AAPL purchase (should NOT be wash sale)")
    check2 = wash_sale_tracker.check_purchase("AAPL")
    print(f"    Is wash sale: {check2['is_wash_sale']}")

    # 5. Active windows
    print("\n[5] Active wash sale windows:")
    windows = wash_sale_tracker.get_active_windows()
    for w in windows:
        print(f"    {w['ticker']} | Loss: ${w['loss']:,.2f} | Days left: {w['days_remaining']}")

    # 6. Trigger replacement purchase
    print("\n[6] Recording MSFT replacement purchase (triggers wash sale)")
    triggered = await wash_sale_tracker.record_replacement_purchase("MSFT")
    print(f"    Triggered: {triggered}")

    # 7. Disallowed losses YTD
    print(f"\n[7] Disallowed losses YTD: ${wash_sale_tracker.get_disallowed_losses_ytd():,.2f}")

    # 8. Expire old windows
    expired = wash_sale_tracker.expire_old_windows()
    print(f"\n[8] Expired windows: {expired}")

    print("\n" + "=" * 60)
    print("PDT TRACKER TESTS")
    print("=" * 60)

    # 9. PDT report
    print("\n[9] PDT Report:")
    report = pdt_tracker.get_pdt_report()
    for k, v in report.items():
        print(f"    {k}: {v}")

    # 10. PDT limit check
    print(f"\n[10] PDT limit reached: {pdt_tracker.is_pdt_limit_reached()}")

    # 11. Recent day trades
    print("\n[11] Recent day trades:")
    trades = pdt_tracker.get_recent_day_trades(5)
    if trades:
        for t in trades:
            print(f"    {t['date']} | {t['ticker']} | rolling: {t['rolling']} | limit: {t['limit_hit']}")
    else:
        print("    None recorded")

    print("\nAll tests complete.")

if __name__ == "__main__":
    asyncio.run(main())
