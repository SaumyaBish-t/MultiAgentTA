import asyncio
import json
import os
import sys
from datetime import datetime, timezone
import httpx
from loguru import logger
from sqlalchemy.dialects.postgresql import insert
import redis

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.settings import settings
from data_ingestion.storage.init_db import get_db_manager
from data_ingestion.storage.models import OhlcvBar

async def fetch_yahoo_direct(client, ticker):
    """Fetch 1m bars directly from Yahoo Finance API."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        resp = await client.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logger.error(f"Yahoo API error for {ticker}: {resp.status_code}")
            return None
            
        data = resp.json()
        result = data['chart']['result'][0]
        indicators = result['indicators']['quote'][0]
        timestamps = result['timestamp']
        
        # Latest bar
        idx = -1
        while idx >= -len(timestamps) and indicators['close'][idx] is None:
            idx -= 1
            
        if idx < -len(timestamps): return None
        
        return {
            "ticker": ticker,
            "timestamp": datetime.fromtimestamp(timestamps[idx], tz=timezone.utc),
            "open": float(indicators['open'][idx]),
            "high": float(indicators['high'][idx]),
            "low": float(indicators['low'][idx]),
            "close": float(indicators['close'][idx]),
            "volume": int(indicators['volume'][idx] or 0)
        }
    except Exception as e:
        logger.error(f"Failed to fetch {ticker} from Yahoo: {e}")
        return None

async def fetch_indian_stock_api(client, ticker):
    """Fetch real-time data from the Indian Stock Market API (0xramm)."""
    # Strip .NS if present, as the API handles both but the search is cleaner
    clean_ticker = ticker.replace(".NS", "")
    url = f"http://65.0.104.9/stock?symbol={clean_ticker}"
    
    try:
        resp = await client.get(url, timeout=10)
        if resp.status_code != 200:
            return None
            
        json_data = resp.json()
        if json_data.get("status") != "success":
            return None
            
        data = json_data.get("data", {})
        
        # Parse timestamp from API (e.g. "2026-05-09 04:03:39")
        ts_str = data.get("timestamp")
        if ts_str:
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        else:
            ts = datetime.now(timezone.utc)
            
        return {
            "ticker": ticker,
            "timestamp": ts,
            "open": float(data.get("open", {}).get("value", 0)),
            "high": float(data.get("day_high", {}).get("value", 0)),
            "low": float(data.get("day_low", {}).get("value", 0)),
            "close": float(data.get("last_price", {}).get("value", 0)),
            "volume": int(data.get("volume", {}).get("value", 0))
        }
    except Exception as e:
        logger.debug(f"Indian Stock API failed for {ticker}: {e}")
        return None

async def collect_realtime():
    logger.info("🚀 Starting Real-time Data Collector (Direct Yahoo Feed)...")
    db_manager = get_db_manager()
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    tickers = settings.tickers
    
    async with httpx.AsyncClient() as client:
        while True:
            try:
                logger.info(f"Tick: Updating {len(tickers)} tickers...")
                new_bars_total = 0
                
                for ticker in tickers:
                    bar = None
                    # For Indian stocks, try the Indian Stock Market API first
                    if ".NS" in ticker or ".BO" in ticker:
                        bar = await fetch_indian_stock_api(client, ticker)
                    
                    # Fallback or primary for others: Yahoo
                    if not bar:
                        bar = await fetch_yahoo_direct(client, ticker)
                        
                    if not bar: continue
                    
                    with db_manager.timescale_session() as session:
                        stmt = insert(OhlcvBar).values(
                            ticker=ticker,
                            timestamp=bar['timestamp'],
                            open=bar['open'],
                            high=bar['high'],
                            low=bar['low'],
                            close=bar['close'],
                            volume=bar['volume'],
                            timeframe='1min',
                            source='yahoo_direct'
                        ).on_conflict_do_nothing()
                        
                        res = session.execute(stmt)
                        if res.rowcount > 0:
                            new_bars_total += 1
                            # Update Redis cache
                            r.setex(f"price:{ticker}", 120, str(bar['close']))
                            # Publish event
                            r.publish(f"realtime:bars:{ticker}", json.dumps({
                                "ticker": ticker,
                                "price": bar['close'],
                                "timestamp": bar['timestamp'].isoformat()
                            }))
                    
                    await asyncio.sleep(0.2) # Be polite
                
                logger.info(f"Tick complete. New bars: {new_bars_total}. Sleeping 30s.")
                await asyncio.sleep(30) # High frequency for "TradingView" feel
                
            except Exception as e:
                logger.error(f"Collector loop error: {e}")
                await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(collect_realtime())
