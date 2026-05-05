"""Smoke test for Report Generator — skips LLM calls."""
import asyncio
from compliance.agents.report_generator import ReportGenerator, _generate_daily_pnl, _generate_compliance, _generate_execution, _generate_weekly
from datetime import date

async def main():
    gen = ReportGenerator()
    today = date.today()

    print("=" * 60)
    print("REPORT 1: DAILY P&L (data only, no LLM)")
    print("=" * 60)
    data = _generate_daily_pnl(today)
    print(f"  Portfolio:  ${data['portfolio_value']:,.0f}")
    print(f"  Cash:       ${data['cash']:,.0f}")
    print(f"  Daily P&L:  ${data['daily_pnl']:,.2f} ({data['daily_pnl_pct']:.2%})")
    print(f"  Positions:  {data['position_count']}")
    for p in data["positions"][:3]:
        print(f"    {p['ticker']:6s} {p['shares']:4d} sh  ${p['current_value']:>10,.2f}  wt={p['weight']:.1%}")
    print(f"  Trades:     {data['trade_count']}")
    print(f"  Best:       {data['best_performer']}")
    print(f"  Worst:      {data['worst_performer']}")

    print("\n" + "=" * 60)
    print("REPORT 2: WEEKLY PERFORMANCE (data only)")
    print("=" * 60)
    wdata = _generate_weekly(today)
    print(f"  Period:       {wdata['period']}")
    print(f"  Portfolio:    ${wdata['portfolio_value']:,.0f}")
    print(f"  Weekly ret:   {wdata['weekly_return']:.2%}")
    print(f"  YTD ret:      {wdata['ytd_return']:.2%}")
    print(f"  Trades:       {wdata['execution_summary']['total_trades']}")
    print(f"  Avg slippage: {wdata['execution_summary']['avg_slippage_bps']:.1f} bps")
    print(f"  Violations:   {wdata['compliance_summary']['violations']}")

    print("\n" + "=" * 60)
    print("REPORT 3: COMPLIANCE SUMMARY (data only)")
    print("=" * 60)
    cdata = _generate_compliance(today)
    print(f"  Checks run:    {cdata['checks_run']}")
    print(f"  Violations:    {cdata['violation_count']}")
    for v in cdata["violations"][:3]:
        print(f"    {v['rule_id']:30s} | {v['ticker'] or 'N/A':5s} | {v['severity']}")
    print(f"  Wash sales:    {len(cdata['wash_sales']['active_windows'])} active windows")
    print(f"  PDT count:     {cdata['pdt_status']['rolling_5day_count']}/4")
    print(f"  Restricted:    {cdata['restricted_list']['count']} tickers")
    print(f"  Audit events:  {cdata['audit_events_today']}")

    print("\n" + "=" * 60)
    print("REPORT 4: EXECUTION QUALITY (data only)")
    print("=" * 60)
    edata = _generate_execution(today)
    print(f"  Total orders:  {edata['total_orders']}")
    print(f"  Filled:        {edata['filled_orders']}")
    print(f"  Fill rate:     {edata['fill_rate']:.0%}")
    print(f"  Avg slippage:  {edata['slippage']['avg_bps']:.1f} bps")
    print(f"  Total cost:    ${edata['slippage']['total_cost_usd']:,.2f}")
    print(f"  Recommendations:")
    for r in edata["recommendations"]:
        print(f"    - {r}")

    # Test full generate (with save, no LLM)
    print("\n" + "=" * 60)
    print("FULL DAILY P&L REPORT (with save, fallback summary)")
    print("=" * 60)
    full = await gen.generate_daily_pnl(today)
    print(f"  Summary: {full.get('summary', 'N/A')[:120]}")

    # Report history
    history = gen.get_report_history("daily_pnl", days=7)
    print(f"\n  Report history (last 7d): {len(history)} entries")

    # Latest cached
    latest = gen.get_latest_report("daily_pnl")
    print(f"  Latest cached: {'Yes' if latest else 'No'}")

    print("\nAll report tests complete.")

if __name__ == "__main__":
    asyncio.run(main())
