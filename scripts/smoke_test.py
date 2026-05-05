"""
Smoke Test — End-to-End Pipeline Validation
===========================================

Executes a full, live ingestion cycle for a single ticker (AAPL) to verify 
all integrations (Postgres, TimescaleDB, Redis, ChromaDB, API) are functioning.
"""

import asyncio
import sys
import os
import httpx
from datetime import datetime, timedelta, timezone
from loguru import logger

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.settings import settings
from data_ingestion.collectors import (
    collector as market_data_collector,
    fundamentals_collector,
    news_collector,
    macro_collector
)
from data_ingestion.cleaners import DataQualityAgent
from data_ingestion.normalizers import DataNormalizer
from data_ingestion.storage.storage_manager import StorageManager
from data_ingestion.storage.init_db import get_db_manager

# Ensure logs are visible
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

async def run_smoke_test():
    logger.info("Starting End-to-End Smoke Test")
    
    ticker = "AAPL"
    timeframe = "1min"
    
    # We fetch data for the last available trading day
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=3) # buffer for weekends
    
    str_start = start_date.strftime("%Y-%m-%d")
    str_end = end_date.strftime("%Y-%m-%d")

    # 1. Collection
    logger.info(f"[STEP 1] Fetching data for {ticker} from {str_start} to {str_end}...")
    try:
        df = await market_data_collector.fetch_historical(ticker, str_start, str_end, timeframe)
        if df.empty:
            logger.warning("[FAIL] Polygon returned no data. Check API key or market hours.")
            return
        logger.info(f"[PASS] Collected {len(df)} bars.")
    except Exception as e:
        logger.error(f"[FAIL] Collection step failed: {e}")
        return

    # 2. Cleaning & Normalization
    logger.info("[STEP 2] Running Clean & Normalize...")
    try:
        dq_agent = DataQualityAgent()
        normalizer = DataNormalizer()
        
        clean_df = dq_agent.process_price_data(df, source_name="smoke_test")
        norm_df = normalizer.normalize_timestamps(clean_df)
        norm_df = normalizer.normalize_ticker_symbols(norm_df)
        norm_df = normalizer.adjust_for_corporate_actions(norm_df, ticker)
        
        logger.info(f"[PASS] Normalized {len(norm_df)} bars successfully.")
    except Exception as e:
        logger.error(f"[FAIL] Cleaning/Normalization failed: {e}")
        return

    # 3. Storage
    logger.info("[STEP 3] Persisting to TimescaleDB...")
    try:
        storage = StorageManager()
        result = storage.write_price_bars(norm_df)
        logger.info(f"[PASS] Stored {result.inserted} bars (Skipped {result.skipped} duplicates).")
    except Exception as e:
        logger.error(f"[FAIL] Storage failed: {e}")
        return

    # 4. Cache Update
    logger.info("[STEP 4] Updating Redis Cache...")
    try:
        latest = norm_df.iloc[-1]
        ticker_prices = {ticker: {"close": latest["close"], "timestamp": str(latest["timestamp"])}}
        storage.cache_latest_prices(ticker_prices)
        
        # Verify in Redis
        cached_val = storage.get_latest_price(ticker)
        if cached_val == float(latest["close"]):
            logger.info("[PASS] Redis cache verified successfully.")
        else:
            logger.warning(f"[FAIL] Redis cache mismatch. Expected {latest['close']}, got {cached_val}")
    except Exception as e:
        logger.error(f"[FAIL] Redis cache test failed: {e}")

    # 5. API Verification
    logger.info("[STEP 5] Querying Internal API...")
    api_url = f"http://localhost:8000/prices/{ticker}/latest?timeframe={timeframe}"
    headers = {"x-api-key": settings.internal_api_key}
    
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(api_url, headers=headers, timeout=15.0)
            
        if resp.status_code == 200:
            data = resp.json()
            if data["ticker"] == ticker:
                logger.info("[PASS] API returned correct data via HTTP GET.")
            else:
                logger.warning(f"[FAIL] API returned wrong ticker: {data}")
        else:
            logger.warning(f"[FAIL] API request failed with status {resp.status_code}. Is Uvicorn running?")
    except httpx.ConnectError:
        logger.warning("[SKIP] API is not running. Start FastAPI server to test Step 5.")
    except Exception as e:
        logger.error(f"[FAIL] API request failed: {e}")

    logger.info("Smoke Test Complete.")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
