"""
Prefect Workflows for Data Ingestion Phase 1.

Public API
----------
::

    from data_ingestion.flows import (
        market_data_flow,
        end_of_day_flow,
        news_flow,
        historical_backfill_flow,
        macro_flow,
        health_monitor_flow,
    )
"""

from data_ingestion.flows.health_monitor import health_monitor_flow
from data_ingestion.flows.ingestion_flow import (
    end_of_day_flow,
    historical_backfill_flow,
    macro_flow,
    market_data_flow,
    news_flow,
)

__all__ = [
    "market_data_flow",
    "end_of_day_flow",
    "news_flow",
    "historical_backfill_flow",
    "macro_flow",
    "health_monitor_flow",
]
