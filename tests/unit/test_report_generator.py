import pytest
import asyncio
from datetime import date
from compliance.agents.report_generator import ReportGenerator

@pytest.fixture
def reporter():
    return ReportGenerator()

@pytest.mark.asyncio
async def test_daily_pnl_report_has_required_fields(reporter):
    report = await reporter.generate_daily_pnl()
    assert "portfolio_value" in report
    assert "daily_pnl" in report
    assert "daily_pnl_pct" in report
    assert "positions" in report

@pytest.mark.asyncio
async def test_compliance_report_includes_violations(reporter):
    report = await reporter.generate_compliance()
    assert "violation_count" in report
    assert "violations" in report
    assert "wash_sales" in report

@pytest.mark.asyncio
async def test_report_saved_to_db(reporter):
    report = await reporter.generate_daily_pnl()
    history = reporter.get_report_history("daily_pnl", days=1)
    assert len(history) >= 1

@pytest.mark.asyncio
async def test_report_cached_in_redis(reporter):
    await reporter.generate_daily_pnl()
    cached = reporter.get_latest_report("daily_pnl")
    assert cached is not None
    assert "portfolio_value" in cached
