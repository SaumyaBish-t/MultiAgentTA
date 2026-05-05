import asyncio
from datetime import datetime, timezone
import pandas as pd
from loguru import logger
from data_ingestion.storage.storage_manager import StorageManager

async def run():
    storage = StorageManager()
    
    # 1. Company Profiles
    companies = [
        {
            "ticker": "AAPL", "name": "Apple Inc.", "sector": "Technology", 
            "industry": "Consumer Electronics", "exchange": "NASDAQ",
            "market_cap": 3000000000000, "shares_outstanding": 15000000000,
            "currency": "USD", "updated_at": datetime.now(tz=timezone.utc)
        },
        {
            "ticker": "MSFT", "name": "Microsoft Corporation", "sector": "Technology", 
            "industry": "Software—Infrastructure", "exchange": "NASDAQ",
            "market_cap": 3200000000000, "shares_outstanding": 7400000000,
            "currency": "USD", "updated_at": datetime.now(tz=timezone.utc)
        },
        {
            "ticker": "TSLA", "name": "Tesla, Inc.", "sector": "Consumer Cyclical", 
            "industry": "Auto Manufacturers", "exchange": "NASDAQ",
            "market_cap": 800000000000, "shares_outstanding": 3100000000,
            "currency": "USD", "updated_at": datetime.now(tz=timezone.utc)
        }
    ]
    storage.write_fundamentals(pd.DataFrame(companies), "companies")
    logger.info("✅ Inserted mock company profiles")

    # 2. Mock Income Statements (latest quarter)
    income = [
        {
            "ticker": "AAPL", "period_type": "quarterly", "fiscal_date": datetime(2025, 12, 31, tzinfo=timezone.utc),
            "revenue": 119500000000, "gross_profit": 54000000000, "operating_income": 35000000000,
            "net_income": 33900000000, "eps": 2.18, "ebitda": 40000000000, "source": "mock"
        },
        {
            "ticker": "MSFT", "period_type": "quarterly", "fiscal_date": datetime(2025, 12, 31, tzinfo=timezone.utc),
            "revenue": 62000000000, "gross_profit": 44000000000, "operating_income": 27000000000,
            "net_income": 21800000000, "eps": 2.93, "ebitda": 30000000000, "source": "mock"
        },
        {
            "ticker": "TSLA", "period_type": "quarterly", "fiscal_date": datetime(2025, 12, 31, tzinfo=timezone.utc),
            "revenue": 25100000000, "gross_profit": 4400000000, "operating_income": 2100000000,
            "net_income": 7900000000, "eps": 0.71, "ebitda": 4000000000, "source": "mock"
        }
    ]
    storage.write_fundamentals(pd.DataFrame(income), "income_statements")
    logger.info("✅ Inserted mock income statements")

    # 3. Mock Balance Sheets
    balance = [
        {
            "ticker": "AAPL", "period_type": "quarterly", "fiscal_date": datetime(2025, 12, 31, tzinfo=timezone.utc),
            "total_assets": 350000000000, "total_liabilities": 270000000000, "equity": 74000000000,
            "cash": 73000000000, "total_debt": 108000000000, "source": "mock"
        },
        {
            "ticker": "MSFT", "period_type": "quarterly", "fiscal_date": datetime(2025, 12, 31, tzinfo=timezone.utc),
            "total_assets": 470000000000, "total_liabilities": 220000000000, "equity": 250000000000,
            "cash": 80000000000, "total_debt": 72000000000, "source": "mock"
        },
        {
            "ticker": "TSLA", "period_type": "quarterly", "fiscal_date": datetime(2025, 12, 31, tzinfo=timezone.utc),
            "total_assets": 106000000000, "total_liabilities": 43000000000, "equity": 63000000000,
            "cash": 29000000000, "total_debt": 5000000000, "source": "mock"
        }
    ]
    storage.write_fundamentals(pd.DataFrame(balance), "balance_sheets")
    logger.info("✅ Inserted mock balance sheets")

if __name__ == "__main__":
    asyncio.run(run())
