"""
Collectors sub-package — data ingestion from external APIs.

Public API
----------
::

    from data_ingestion.collectors import (
        MarketDataCollector, collector,
        FundamentalsCollector, fundamentals_collector,
        NewsCollector, news_collector,
        MacroCollector, macro_collector,
    )
"""

from data_ingestion.collectors.market_data_collector import (
    MarketDataCollector, collector,
)
from data_ingestion.collectors.fundamentals_collector import (
    FundamentalsCollector, fundamentals_collector,
)
from data_ingestion.collectors.news_collector import (
    NewsCollector, news_collector,
)
from data_ingestion.collectors.macro_collector import (
    MacroCollector, macro_collector,
)

__all__ = [
    "MarketDataCollector", "collector",
    "FundamentalsCollector", "fundamentals_collector",
    "NewsCollector", "news_collector",
    "MacroCollector", "macro_collector",
]
