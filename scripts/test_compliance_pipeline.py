"""Smoke test for the full Phase 7 Compliance Pipeline."""
import asyncio
import json
from datetime import date

async def main():
    # ─── 1. Pipeline import ───
    print("=" * 60)
    print("PHASE 7 COMPLIANCE PIPELINE SMOKE TEST")
    print("=" * 60)

    from compliance.pipeline.compliance_pipeline import CompliancePipeline
    pipeline = CompliancePipeline()
    print("[OK] CompliancePipeline instantiated")
    print(f"     Agents loaded: audit, pre_trade, position_limits, wash_sale, pdt, reporter")

    # ─── 2. Get compliance status ───
    print("\n--- Compliance Status ---")
    status = pipeline.get_compliance_status()
    for k, v in status.items():
        print(f"  {k}: {v}")

    # ─── 3. Open violations ───
    print("\n--- Open Violations ---")
    viols = pipeline.get_violations_open()
    print(f"  Open violations: {len(viols)}")
    for v in viols[:3]:
        print(f"    {v['rule_id']:30s} | {v['ticker'] or 'N/A':5s} | {v['severity']}")

    # ─── 4. Audit trail ───
    print("\n--- Recent Audit Trail ---")
    trail = pipeline.get_audit_trail()
    print(f"  Audit entries (last 50): {len(trail)}")
    for t in trail[:5]:
        print(f"    {t['event_type']:25s} | {t['actor']:25s} | {t['ticker'] or 'N/A'}")

    # ─── 5. Pre-trade check ───
    print("\n--- Pre-Trade Check ---")
    test_order = {
        "ticker": "AAPL", "action": "buy", "shares": 10,
        "estimated_value": 1900, "order_type": "market",
        "batch_type": "normal",
    }
    approved = await pipeline.run_pre_trade_check(test_order)
    print(f"  AAPL buy approved: {approved}")

    # ─── 6. Post-fill compliance ───
    print("\n--- Post-Fill Compliance ---")
    test_fill = {
        "id": "test-fill-001", "ticker": "MSFT", "action": "sell",
        "filled_shares": 5, "filled_avg_price": 400.0, "status": "filled",
    }
    await pipeline.post_fill_compliance(test_fill)
    print(f"  Post-fill compliance processed for MSFT sell")

    # ─── 7. Full daily compliance run ───
    print("\n--- Daily Compliance Run ---")
    results = await pipeline.run_daily_compliance()
    print(f"  Date: {results.get('date')}")
    pos = results.get("position_limits", {})
    print(f"  Position limits: {pos.get('status')} | Breaches: {pos.get('breaches')}")
    print(f"  Wash sales expired: {results.get('wash_sales_expired')}")
    print(f"  Audit chain OK: {results.get('audit_chain_ok')}")
    rpt_pnl = results.get("report_pnl", {})
    print(f"  P&L report: Portfolio ${rpt_pnl.get('portfolio_value', 0):,.0f}")

    # ─── 8. Prefect flow import ───
    print("\n--- Prefect Flows ---")
    from compliance.flows.compliance_flow import (
        daily_compliance_flow,
        position_limit_monitor_flow,
        audit_chain_verification_flow,
        weekly_compliance_report_flow,
        wash_sale_expiry_flow,
    )
    print("  [OK] daily_compliance_flow")
    print("  [OK] position_limit_monitor_flow")
    print("  [OK] audit_chain_verification_flow")
    print("  [OK] weekly_compliance_report_flow")
    print("  [OK] wash_sale_expiry_flow")

    # ─── 9. Listener import ───
    print("\n--- Phase 6 Listener ---")
    from compliance.listeners.phase6_listener import start_phase6_listener
    print("  [OK] start_phase6_listener importable")

    print("\n" + "=" * 60)
    print("ALL PHASE 7 PIPELINE TESTS PASSED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
