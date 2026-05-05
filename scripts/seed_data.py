import asyncio
import sys
from datetime import datetime, timedelta

from loguru import logger
from data_ingestion.collectors import collector as market_data_collector
from data_ingestion.storage.storage_manager import StorageManager
from data_ingestion.normalizers import DataNormalizer
from config.settings import settings

async def run(tickers_to_seed):
    storage = StorageManager()
    norm = DataNormalizer()
    
    # By default, seed 2 years of data up to today
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    
    logger.info(f"Starting data backfill for tickers: {tickers_to_seed}")
    
    for ticker in tickers_to_seed:
        logger.info(f"Fetching {ticker} data from {start_date} to {end_date}...")
        try:
            df = await market_data_collector.fetch_historical(ticker, start_date, end_date, '1d')
            if not df.empty:
                df = norm.normalize_timestamps(df)
                storage.write_price_bars(df)
                logger.info(f"✅ Successfully seeded {len(df)} historical bars for {ticker}.")
            else:
                logger.warning(f"⚠️ No data returned from API for {ticker}.")
        except Exception as e:
            logger.error(f"❌ Failed to fetch data for {ticker}: {e}")

if __name__ == '__main__':
    # If arguments are provided (e.g. `python scripts/seed_data.py MSFT TSLA`), use those.
    # Otherwise, fall back to the default TICKERS list in your .env / config.
    args = sys.argv[1:]
    tickers = [arg.upper() for arg in args] if args else settings.tickers
    
    asyncio.run(run(tickers))
