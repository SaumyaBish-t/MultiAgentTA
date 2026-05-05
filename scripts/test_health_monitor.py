"""
Test System Health Monitor
"""

import asyncio
import json
from monitoring.agents.health_monitor_agent import SystemHealthMonitor

async def test_health_monitor():
    monitor = SystemHealthMonitor()
    
    print("Running full system health check...")
    report = await monitor.run_full_health_check()
    
    print("\nSYSTEM HEALTH REPORT")
    print(f"Overall Status: {report.overall.upper()}")
    print(f"Active Alerts:  {report.active_alerts}")
    print(f"Checked At:     {report.checked_at}")
    
    print("\nPHASE STATUSES:")
    for phase_id, health in report.phases.items():
        print(f"  {phase_id:10}: {health.status.upper()}")
        for check in health.checks:
            mark = "[OK]" if check["pass"] else "[X]"
            print(f"    {mark} {check['check']}: {check['detail']}")
            
    print("\nINFRASTRUCTURE:")
    for db, status in report.databases.items():
        print(f"  {db:12}: {status['status'].upper()} ({status.get('latency_ms', 0):.1f}ms)")

    print("\nLLM PROVIDERS:")
    for llm, status in report.llm_providers.items():
        print(f"  {llm:12}: {status['status'].upper()}")

if __name__ == "__main__":
    asyncio.run(test_health_monitor())
