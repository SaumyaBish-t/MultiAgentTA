from compliance.agents.audit_logger import audit_log
import asyncio

async def test():
    h = await audit_log.log_parameter_changed("max_position_size", 0.05, 0.08, "admin")
    print(f"Appended to chain: {h[:16]}...")
    ok = audit_log.verify_chain_integrity()
    status = "PASS" if ok else "FAIL"
    print(f"Chain integrity (6 records): {status}")
    events = audit_log.get_recent_events(2)
    print(f"Last 2 events: {[e['event_type'] for e in events]}")

asyncio.run(test())
