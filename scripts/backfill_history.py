
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from config.settings import settings
from loguru import logger

def backfill_history():
    # Connect to TimescaleDB
    engine = create_engine(settings.timescale_url)
    tickers = settings.tickers
    
    logger.info(f"Starting historical backfill for {len(tickers)} tickers...")
    
    for ticker in tickers:
        try:
            logger.info(f"Processing {ticker}...")
            
            # Fetch 2 years of daily data.
            # auto_adjust=True -> split- and dividend-adjusted prices, so a
            # stock split (e.g. NVDA 10:1 in 2024) doesn't leave a fake -90%
            # bar that wrecks momentum backtests.
            stock = yf.Ticker(ticker)
            df = stock.history(period="2y", interval="1d", auto_adjust=True)

            if df.empty:
                logger.warning(f"No daily data found for {ticker}")
                continue

            logger.info(f"Fetched {len(df)} daily bars for {ticker}. Inserting into DB...")

            with engine.connect() as conn:
                for idx, row in df.iterrows():
                    # Normalise to midnight of the calendar date (tz-naive).
                    # yfinance returns market-local tz-aware timestamps, so the
                    # SAME trading day lands at different absolute times for US
                    # vs NSE tickers — that created duplicate-day rows on
                    # re-runs. Keying on the date makes ON CONFLICT idempotent.
                    ts = pd.Timestamp(idx.date()).to_pydatetime()

                    conn.execute(text("""
                        INSERT INTO ohlcv_bars (ticker, timestamp, open, high, low, close, volume, timeframe)
                        VALUES (:t, :ts, :o, :h, :l, :c, :v, :tf)
                        ON CONFLICT (ticker, timestamp, timeframe) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                    """), {
                        "t": ticker,
                        "ts": ts,
                        "o": float(row["Open"]),
                        "h": float(row["High"]),
                        "l": float(row["Low"]),
                        "c": float(row["Close"]),
                        "v": int(row["Volume"]),
                        "tf": "1d"
                    })
                conn.commit()
            logger.info(f"Successfully backfilled {ticker}")
            
        except Exception as e:
            logger.error(f"Failed to backfill {ticker}: {e}")

if __name__ == "__main__":
    backfill_history()
