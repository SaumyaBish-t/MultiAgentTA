"""Quick smoke test for the AuditLogger."""
import asyncio
from compliance.agents.audit_logger import audit_log

async def main():
    # 1. Log system startup
    h1 = await audit_log.log_system_startup()
    print(f"[1] system_startup       hash={h1[:16]}...")

    # 2. Log a signal approval
    h2 = await audit_log.log_signal_approved(
        signal={"id": "aaaaaaaa-1111-2222-3333-444444444444", "ticker": "AAPL"},
        decision={"risk_score": 0.35, "approved": True},
    )
    print(f"[2] signal_approved      hash={h2[:16]}...")

    # 3. Log an order submitted
    h3 = await audit_log.log_order_submitted({
        "id": "bbbbbbbb-1111-2222-3333-444444444444",
        "ticker": "AAPL", "action": "buy", "requested_shares": 50,
    })
    print(f"[3] order_submitted      hash={h3[:16]}...")

    # 4. Log an order fill
    h4 = await audit_log.log_order_filled({
        "id": "bbbbbbbb-1111-2222-3333-444444444444",
        "ticker": "AAPL", "filled_shares": 50, "filled_avg_price": 189.42,
    })
    print(f"[4] order_filled         hash={h4[:16]}...")

    # 5. Log a risk breach
    h5 = await audit_log.log_risk_breach("daily_loss", -0.032, -0.03, "halt_trading")
    print(f"[5] risk_breach          hash={h5[:16]}...")

    # 6. Verify chain
    ok = audit_log.verify_chain_integrity()
    print(f"\nChain integrity: {'PASS' if ok else 'FAIL'}")

    # 7. Recent events
    events = audit_log.get_recent_events(limit=5)
    print(f"Recent events in DB: {len(events)}")

if __name__ == "__main__":
    asyncio.run(main())
