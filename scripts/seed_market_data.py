import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from config.settings import settings

def seed_ohlcv_data():
    # Connect to TimescaleDB (market_data)
    engine = create_engine(settings.timescale_url)
    
    # Common tickers
    tickers = settings.tickers # AAPL, MSFT, GOOGL, AMZN, NVDA, TSLA, JPM, SPY, QQQ
    
    base_prices = {
        'AAPL': 180.0,
        'MSFT': 410.0,
        'GOOGL': 145.0,
        'AMZN': 175.0,
        'NVDA': 850.0,
        'TSLA': 170.0,
        'JPM': 190.0,
        'SPY': 510.0,
        'QQQ': 440.0
    }
    
    now = datetime.now(timezone.utc)
    
    with engine.connect() as conn:
        print(f"Seeding OHLCV data for {len(tickers)} tickers into market_data...")
        
        for ticker in tickers:
            base_price = base_prices.get(ticker, 100.0)
            
            # Seed 90 days of history
            for i in range(90):
                # We need 1d bars for the comparison chart
                ts = now - timedelta(days=i)
                ts = ts.replace(hour=20, minute=0, second=0, microsecond=0) # Market closeish
                
                # Random walk
                price = base_price * (1 + (i % 15 - 7) / 200.0)
                
                conn.execute(text("""
                    INSERT INTO ohlcv_bars (ticker, timestamp, open, high, low, close, volume, timeframe)
                    VALUES (:t, :ts, :o, :h, :l, :c, :v, :tf)
                    ON CONFLICT (ticker, timestamp, timeframe) DO NOTHING
                """), {
                    "t": ticker,
                    "ts": ts,
                    "o": price * 0.99,
                    "h": price * 1.01,
                    "l": price * 0.98,
                    "c": price,
                    "v": 1000000 + (i * 1000),
                    "tf": "1d"
                })
                
                # Also seed a 1min bar for "latest price"
                if i == 0:
                    conn.execute(text("""
                        INSERT INTO ohlcv_bars (ticker, timestamp, open, high, low, close, volume, timeframe)
                        VALUES (:t, :ts, :o, :h, :l, :c, :v, :tf)
                        ON CONFLICT (ticker, timestamp, timeframe) DO NOTHING
                    """), {
                        "t": ticker,
                        "ts": now.replace(second=0, microsecond=0),
                        "o": price,
                        "h": price * 1.001,
                        "l": price * 0.999,
                        "c": price,
                        "v": 5000,
                        "tf": "1min"
                    })
        
        conn.commit()
        print("Success! Seeded historical and latest prices for all tickers.")

if __name__ == "__main__":
    seed_ohlcv_data()
