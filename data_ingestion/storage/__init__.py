"""
Storage sub-package — ORM models, schema DDL, and database management.

Public API
----------
::

    from data_ingestion.storage import (
        # Manager & init
        DatabaseManager, get_db_manager, init_databases,
        # TimescaleDB models
        OHLCVBar, RawTick,
        # PostgreSQL models
        Company, IncomeStatement, BalanceSheet, NewsArticle, MacroSeries,
        # Storage Manager
        StorageManager, WriteResult,
    )
"""

from data_ingestion.storage.init_db import (
    DatabaseManager,
    get_db_manager,
    init_databases,
)
from data_ingestion.storage.models import (
    BalanceSheet,
    Company,
    IncomeStatement,
    MacroSeries,
    NewsArticle,
    OhlcvBar,
    RawTick,
)
from data_ingestion.storage.storage_manager import (
    StorageManager,
    WriteResult,
)

__all__ = [
    "DatabaseManager",
    "get_db_manager",
    "init_databases",
    "OhlcvBar",
    "RawTick",
    "Company",
    "IncomeStatement",
    "BalanceSheet",
    "NewsArticle",
    "MacroSeries",
    "StorageManager",
    "WriteResult",
]
