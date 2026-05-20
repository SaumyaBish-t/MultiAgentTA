"""
data_ingestion.flows.new_sources_flow
=====================================

Prefect flow that runs the three new L1 collectors (insider, options
P/C, sector ETF flows). Each collector internally checks its own
feature flag so the flow is safe to schedule unconditionally.
"""

from __future__ import annotations

from prefect import flow, task
from loguru import logger

from data_ingestion.collectors.insider_collector import collect_all as collect_insider
from data_ingestion.collectors.options_flow_collector import collect_all as collect_options
from data_ingestion.collectors.etf_flow_collector import collect_etf_flows


@task(retries=2, retry_delay_seconds=30)
def _insider_task() -> list[dict]:
    return collect_insider()


@task(retries=2, retry_delay_seconds=30)
def _options_task() -> list[dict]:
    return collect_options()


@task(retries=2, retry_delay_seconds=30)
def _etf_task() -> dict:
    return collect_etf_flows()


@flow(name="new-sources-flow")
def new_sources_flow() -> dict:
    insider = _insider_task()
    options = _options_task()
    etf = _etf_task()
    summary = {
        "insider_tickers": len(insider),
        "options_tickers": len(options),
        "etf_sectors": len(etf) if isinstance(etf, dict) else 0,
    }
    logger.info("new_sources_flow summary: {}", summary)
    return summary


if __name__ == "__main__":
    print(new_sources_flow())
