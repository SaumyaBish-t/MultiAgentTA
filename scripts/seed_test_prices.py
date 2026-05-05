import asyncio
import uuid
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from config.settings import settings

def seed_prices():
    engine = create_engine(settings.postgres_url)
    with engine.connect() as conn:
        print("Seeding test prices for AAPL, MSFT, GOOGL...")
        
        # Clear old prices to avoid duplicates or conflicts if needed
        # (Though usually we just insert new ones)
        
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'SPY']
        prices = {
            'AAPL': 180.0,
            'MSFT': 400.0,
            'GOOGL': 140.0,
            'SPY': 500.0
        }
        
        now = datetime.utcnow()
        for ticker in tickers:
            base_price = prices[ticker]
            # Seed 260 days of history
            for i in range(260):
                d = now - timedelta(days=i)
                # Add some random walk
                price = base_price * (1 + (i % 10 - 5) / 100.0)
                conn.execute(text("""
                    INSERT INTO ticker_prices (id, ticker, price_date, open, high, low, close, volume)
                    VALUES (:id, :t, :d, :p, :p, :p, :p, 1000000)
                    ON CONFLICT (ticker, price_date) DO NOTHING
                """), {
                    "id": uuid.uuid4(),
                    "t": ticker,
                    "d": d.date(),
                    "p": price
                })
        conn.commit()
        print("Seeded history for 4 tickers.")

if __name__ == "__main__":
    seed_prices()
