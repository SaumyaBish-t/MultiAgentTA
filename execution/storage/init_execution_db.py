import asyncio
import uuid
from datetime import datetime, date, timedelta, timezone
import json

from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import sessionmaker
from loguru import logger
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetCalendarRequest

from config.settings import settings
from data_ingestion.storage.models import FundamentalBase
from execution.storage.execution_models import (
    Order, Execution, OrderBatch, ExecutionPerformance, BrokerConnection, MarketHour
)

def init_db():
    """Initialize Execution database tables and seed initial data."""
    logger.info("Initializing Phase 6 Execution Database...")
    engine = create_engine(settings.postgres_url)
    
    # 1. Create Tables
    try:
        FundamentalBase.metadata.create_all(engine)
        logger.info("Successfully created/verified execution tables.")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        return

    Session = sessionmaker(bind=engine)
    with Session() as session:
        # 2. Seed Broker Connection (Alpaca Paper)
        try:
            # Check if already exists
            existing_broker = session.execute(
                select(BrokerConnection).where(BrokerConnection.broker_name == "alpaca")
            ).scalar_one_or_none()
            
            if not existing_broker:
                logger.info("Seeding Alpaca Paper broker connection...")
                
                # Fetch live data from Alpaca to seed accurately
                trading_client = TradingClient(
                    settings.alpaca_api_key.get_secret_value(), 
                    settings.alpaca_secret_key.get_secret_value(), 
                    paper=True
                )
                account = trading_client.get_account()
                
                broker = BrokerConnection(
                    id=uuid.uuid4(),
                    broker_name="alpaca",
                    account_number=account.account_number,
                    account_type="paper",
                    cash_balance=float(account.cash),
                    portfolio_value=float(account.portfolio_value),
                    buying_power=float(account.buying_power),
                    day_trade_count=account.daytrade_count,
                    pattern_day_trader=account.pattern_day_trader,
                    trading_blocked=account.trading_blocked,
                    last_synced_at=datetime.now(timezone.utc),
                    created_at=datetime.now(timezone.utc)
                )
                session.add(broker)
                logger.info(f"Seeded Alpaca account {account.account_number}")
            else:
                logger.info("Alpaca broker connection already exists.")
        except Exception as e:
            logger.warning(f"Failed to seed broker connection: {e}")

        # 3. Populate Market Hours (Next 30 days)
        try:
            logger.info("Populating Market Hours for the next 30 days...")
            trading_client = TradingClient(
                settings.alpaca_api_key.get_secret_value(), 
                settings.alpaca_secret_key.get_secret_value(), 
                paper=True
            )
            
            start_date = date.today()
            end_date = start_date + timedelta(days=30)
            
            calendar = trading_client.get_calendar(GetCalendarRequest(start=start_date, end=end_date))
            
            for day in calendar:
                # Upsert to avoid duplicates
                existing_day = session.execute(
                    select(MarketHour).where(MarketHour.date == day.date)
                ).scalar_one_or_none()
                
                if not existing_day:
                    # Convert Alpaca times (usually strings like '0930') to datetime objects
                    # Alpaca returns datetime objects in newer SDK versions, but let's check
                    # They are actually datetime objects now.
                    
                    mh = MarketHour(
                        id=uuid.uuid4(),
                        date=day.date,
                        is_open=True, # Calendar only returns open days by default, but we can verify
                        open_time=day.open,
                        close_time=day.close,
                        session_type="regular",
                        created_at=datetime.now(timezone.utc)
                    )
                    session.add(mh)
            
            # Fill in weekends/holidays as closed if not in calendar
            current = start_date
            while current <= end_date:
                if not any(day.date == current for day in calendar):
                    existing_closed = session.execute(
                        select(MarketHour).where(MarketHour.date == current)
                    ).scalar_one_or_none()
                    
                    if not existing_closed:
                        mh = MarketHour(
                            id=uuid.uuid4(),
                            date=current,
                            is_open=False,
                            session_type="closed",
                            created_at=datetime.now(timezone.utc)
                        )
                        session.add(mh)
                current += timedelta(days=1)
                
            logger.info(f"Populated market hours up to {end_date}")
        except Exception as e:
            logger.warning(f"Failed to populate market hours: {e}")

        # 4. Insert Default Order Batch for Testing
        try:
            existing_batch = session.execute(
                select(OrderBatch).where(OrderBatch.batch_type == "test_init")
            ).scalar_one_or_none()
            
            if not existing_batch:
                logger.info("Inserting default test order batch...")
                batch = OrderBatch(
                    id=uuid.uuid4(),
                    batch_type="test_init",
                    total_orders=0,
                    filled_orders=0,
                    failed_orders=0,
                    total_value=0.0,
                    status="pending",
                    created_at=datetime.now(timezone.utc)
                )
                session.add(batch)
            else:
                logger.info("Test order batch already exists.")
        except Exception as e:
            logger.warning(f"Failed to insert test batch: {e}")

        session.commit()
        logger.info("Phase 6 database initialization complete.")

if __name__ == "__main__":
    init_db()
