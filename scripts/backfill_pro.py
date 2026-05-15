
import httpx
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from config.settings import settings
from loguru import logger

POLYGON_KEY = settings.polygon_api_key.get_secret_value()
FMP_KEY = settings.fmp_api_key.get_secret_value()

async def backfill_us_polygon(ticker):
    logger.info(f"Polygon: Fetching US data for {ticker}...")
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}?adjusted=true&sort=asc&limit=1000&apiKey={POLYGON_KEY}"
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            logger.error(f"Polygon failed for {ticker}: {resp.status_code}")
            return []
        
        data = resp.json()
        results = data.get("results", [])
        bars = []
        for r in results:
            bars.append({
                "ticker": ticker,
                "timestamp": datetime.fromtimestamp(r["t"] / 1000.0),
                "open": float(r["o"]),
                "high": float(r["h"]),
                "low": float(r["l"]),
                "close": float(r["c"]),
                "volume": int(r["v"]),
                "timeframe": "1d"
            })
        return bars

async def backfill_intl_fmp(ticker):
    logger.info(f"FMP: Fetching data for {ticker}...")
    # FMP uses slightly different ticker format sometimes, but RELIANCE.NS usually works or RELIANCE
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={FMP_KEY}"
    
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            logger.error(f"FMP failed for {ticker}: {resp.status_code}")
            return []
        
        data = resp.json()
        historical = data.get("historical", [])
        bars = []
        for r in historical:
            bars.append({
                "ticker": ticker,
                "timestamp": datetime.strptime(r["date"], "%Y-%m-%d"),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": int(r["volume"]),
                "timeframe": "1d"
            })
        return bars

async def run_backfill():
    engine = create_engine(settings.timescale_url)
    tickers = settings.tickers
    
    all_bars = []
    for ticker in tickers:
        try:
            if ".NS" in ticker:
                bars = await backfill_intl_fmp(ticker)
            else:
                bars = await backfill_us_polygon(ticker)
            
            if not bars:
                # Fallback to FMP for US if Polygon fails
                bars = await backfill_intl_fmp(ticker)
                
            if bars:
                logger.info(f"Inserting {len(bars)} bars for {ticker}...")
                with engine.connect() as conn:
                    for b in bars:
                        conn.execute(text("""
                            INSERT INTO ohlcv_bars (ticker, timestamp, open, high, low, close, volume, timeframe)
                            VALUES (:ticker, :timestamp, :open, :high, :low, :close, :volume, :timeframe)
                            ON CONFLICT (ticker, timestamp, timeframe) DO UPDATE SET
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume
                        """), b)
                    conn.commit()
                logger.info(f"Successfully backfilled {ticker}")
            else:
                logger.warning(f"No data found for {ticker} from any pro source.")
        except Exception as e:
            logger.error(f"Error backfilling {ticker}: {e}")

if __name__ == "__main__":
    asyncio.run(run_backfill())
