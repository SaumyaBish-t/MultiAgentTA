"""
Phase 7 — Prefect Compliance Flows
====================================
Scheduled flows for daily compliance, position monitoring,
audit verification, weekly reports, and wash-sale expiry.
"""

import asyncio
import json
from datetime import datetime, date, timezone

from prefect import flow, task
from loguru import logger

from config.settings import settings

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TASKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@task(retries=2, retry_delay_seconds=120, name="Daily Compliance Run Task")
async def run_daily_compliance_task() -> dict:
    """Execute the full daily compliance sweep."""
    from compliance.pipeline.compliance_pipeline import CompliancePipeline
    pipeline = CompliancePipeline()
    return await pipeline.run_daily_compliance()


@task(name="Position Limit Check Task")
async def check_position_limits_task() -> dict:
    """Check all position limits (runs every 5 min during market hours)."""
    from compliance.agents.position_limit_agent import PositionLimitAgent
    agent = PositionLimitAgent()
    result = await agent.check()
    if result.breaches:
        logger.warning(
            "POSITION BREACHES: {} | Status: {} | HHI: {}",
            len(result.breaches), result.overall_status, result.hhi,
        )
    return {
        "status": result.overall_status,
        "breaches": len(result.breaches),
        "warnings": len(result.warnings),
    }


@task(name="Audit Chain Verification Task")
def verify_audit_chain_task() -> bool:
    """Verify immutability of the audit log hash chain."""
    from compliance.agents.audit_logger import audit_log
    ok = audit_log.verify_chain_integrity()
    if not ok:
        logger.critical("AUDIT CHAIN INTEGRITY FAILED — possible tampering detected")
    else:
        logger.info("Audit chain integrity verified OK")
    return ok


@task(retries=1, retry_delay_seconds=60, name="Weekly Report Task")
async def generate_weekly_report_task() -> dict:
    """Generate the weekly compliance + performance report."""
    from compliance.agents.report_generator import ReportGenerator
    reporter = ReportGenerator()
    return await reporter.generate_weekly()


@task(name="Wash Sale Expiry Task")
def expire_wash_sale_windows_task() -> int:
    """Expire monitoring windows past their 30-day window."""
    from compliance.agents.wash_sale_pdt_tracker import wash_sale_tracker
    expired = wash_sale_tracker.expire_old_windows()
    logger.info("Expired {} wash sale monitoring windows", expired)
    return expired


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FLOWS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@flow(name="Daily Compliance Run")
async def daily_compliance_flow():
    """
    Full daily compliance run.
    Schedule: Every day at 20:30 UTC (after market close).
    """
    logger.info("Starting daily compliance flow...")
    result = await run_daily_compliance_task()
    logger.info("Daily compliance flow complete: {}", result.get("audit_chain_ok"))
    return result


@flow(name="Position Limit Monitor")
async def position_limit_monitor_flow():
    """
    Continuous position limit check.
    Schedule: Every 5 minutes during market hours (13:30–20:00 UTC).
    """
    return await check_position_limits_task()


@flow(name="Audit Chain Verification")
def audit_chain_verification_flow():
    """
    Nightly audit chain integrity check.
    Schedule: Every day at 00:00 UTC.
    """
    return verify_audit_chain_task()


@flow(name="Weekly Compliance Report")
async def weekly_compliance_report_flow():
    """
    Weekly performance & compliance report.
    Schedule: Every Sunday at 22:00 UTC.
    """
    logger.info("Generating weekly compliance report...")
    report = await generate_weekly_report_task()
    logger.info("Weekly report generated for period: {}", report.get("period"))
    return report


@flow(name="Wash Sale Window Expiry")
def wash_sale_expiry_flow():
    """
    Expire old wash sale monitoring windows.
    Schedule: Every day at 01:00 UTC.
    """
    return expire_wash_sale_windows_task()


if __name__ == "__main__":
    # Quick test — run daily compliance
    asyncio.run(daily_compliance_flow())
