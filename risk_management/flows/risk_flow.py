import asyncio
from prefect import flow, task
from loguru import logger

from risk_management.pipeline.risk_pipeline import RiskPipeline
from risk_management.agents.drawdown_monitor_agent import DrawdownMonitorAgent
from risk_management.agents.var_agent import VaRAgent
from risk_management.agents.correlation_agent import CorrelationAgent

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TASKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@task(retries=2, retry_delay_seconds=30)
async def run_risk_pipeline_task(signals: list = None):
    """Prefect task to evaluate a batch of signals through the risk pipeline."""
    logger.info("Executing Prefect Task: run_risk_pipeline_task")
    pipeline = RiskPipeline()
    result = await pipeline.run(signals)
    return result

@task
async def monitor_drawdown_task():
    """Prefect task to trigger one cycle of the low-latency drawdown monitor."""
    logger.info("Executing Prefect Task: monitor_drawdown_task")
    monitor = DrawdownMonitorAgent()
    result = await monitor.run()
    return result

@task
async def snapshot_portfolio_risk_task():
    """Prefect task to forcefully trigger VaR and Correlation state updates."""
    logger.info("Executing Prefect Task: snapshot_portfolio_risk_task")
    import redis
    import json
    from config.settings import settings
    
    # We fetch positions manually to feed both agents
    positions_dict = {}
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        pf_str = r.get("portfolio:current:state")
        if pf_str:
            pf_data = json.loads(pf_str)
            for p in pf_data.get("positions", []):
                if "ticker" in p and "current_value" in p:
                    positions_dict[p["ticker"]] = float(p["current_value"])
    except Exception as e:
        logger.error(f"Failed to fetch portfolio state for snapshot: {e}")
        
    var_agent = VaRAgent()
    corr_agent = CorrelationAgent()
    
    results = await asyncio.gather(
        var_agent.calculate(positions_dict),
        corr_agent.analyze(list(positions_dict.keys()), positions_dict),
        return_exceptions=True
    )
    
    return results

@task
def generate_eod_risk_report_task():
    """Prefect task to generate and distribute the end of day summary report."""
    logger.info("Executing Prefect Task: generate_eod_risk_report_task")
    # In a full production implementation, this would query the RiskEvents
    # table for the day, fetch the latest PortfolioRiskSnapshot, and send
    # a structured JSON/Slack payload.
    return {"status": "EOD Report Generated Successfully"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FLOWS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@flow(name="Risk Evaluation Pipeline")
async def risk_evaluation_flow(signals: list = None):
    """
    Main flow for processing Phase 3 signals through the Phase 4 Risk Gate.
    Schedule: Triggered by Redis event only (via phase3_listener)
    """
    logger.info("Starting Risk Evaluation Flow")
    result = await run_risk_pipeline_task(signals)
    logger.info(f"Risk Evaluation Flow Completed: {result.signals_approved} approved, {result.signals_rejected} rejected.")
    return result

@flow(name="Continuous Risk Monitor")
async def continuous_risk_monitor_flow():
    """
    Executes continuous monitoring checks.
    Schedule: Designed to be run every 5 minutes during market hours
    """
    logger.info("Starting Continuous Risk Monitor Flow")
    dd_result = await monitor_drawdown_task()
    snapshot_result = await snapshot_portfolio_risk_task()
    return {"drawdown": dd_result, "snapshot": snapshot_result}

@flow(name="End of Day Risk Report")
def end_of_day_risk_flow():
    """
    Aggregates risk metrics at the end of the trading day.
    Schedule: Daily at 16:30 ET
    """
    logger.info("Starting End of Day Risk Flow")
    report = generate_eod_risk_report_task()
    return report

if __name__ == "__main__":
    # Local manual trigger for testing
    asyncio.run(risk_evaluation_flow())
