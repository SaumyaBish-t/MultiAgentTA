"""
Test Alert Manager
"""

import asyncio
import json
import redis
import uuid
from monitoring.alerts.alert_manager import alert_manager
from config.settings import settings
from sqlalchemy import create_engine, text

async def test_alert_manager():
    # 1. Clear previous alerts for testing
    engine = create_engine(settings.postgres_url)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM alerts"))
    
    r = redis.from_url(settings.redis_url)
    # Clear dedup keys
    for key in r.keys("alert:dedup:*"):
        r.delete(key)

    print("Sending first alert...")
    sent1 = await alert_manager.send_alert(
        alert_type="test_alert",
        severity="warning",
        title="Test Alert 1",
        message="This is a test alert.",
        ticker="AAPL"
    )
    print(f"  First alert sent: {sent1}")

    print("Sending duplicate alert (should be deduped)...")
    sent2 = await alert_manager.send_alert(
        alert_type="test_alert",
        severity="warning",
        title="Test Alert 2",
        message="This is a duplicate test alert.",
        ticker="AAPL"
    )
    print(f"  Duplicate alert sent: {sent2}")

    print("Sending alert for different ticker...")
    sent3 = await alert_manager.send_alert(
        alert_type="test_alert",
        severity="warning",
        title="Test Alert 3",
        message="This is an alert for MSFT.",
        ticker="MSFT"
    )
    print(f"  MSFT alert sent: {sent3}")

    # 2. Check summary
    summary = alert_manager.get_alert_summary()
    print("\nAlert Summary:")
    print(f"  Total Active: {summary.get('total_active')}")
    print(f"  By Severity: {summary.get('by_severity')}")
    print(f"  Most Common: {summary.get('most_common_type')}")

    # 3. Acknowledge
    active = alert_manager.get_active_alerts()
    if active:
        alert_id = active[0]["id"]
        print(f"\nAcknowledging alert {alert_id}...")
        ack = alert_manager.acknowledge_alert(alert_id)
        print(f"  Acknowledged: {ack}")

    # 4. Final summary
    summary2 = alert_manager.get_alert_summary()
    print(f"\nFinal Total Active: {summary2.get('total_active')}")

if __name__ == "__main__":
    asyncio.run(test_alert_manager())
