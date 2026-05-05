import uuid
from sqlalchemy import create_engine, text
from config.settings import settings

def seed_companies():
    engine = create_engine(settings.postgres_url)
    with engine.connect() as conn:
        print("Seeding companies table...")
        tickers = [
            ('AAPL', 'Apple Inc.', 'Technology', 'Consumer Electronics'),
            ('MSFT', 'Microsoft Corp.', 'Technology', 'Software'),
            ('GOOGL', 'Alphabet Inc.', 'Technology', 'Interactive Media'),
            ('SPY', 'SPDR S&P 500 ETF Trust', 'ETF', 'Broad Market')
        ]
        
        for ticker, name, sector, industry in tickers:
            conn.execute(text("""
                INSERT INTO companies (id, ticker, name, sector, industry, market_cap)
                VALUES (:id, :t, :n, :s, :i, 1000000000000)
                ON CONFLICT (ticker) DO UPDATE SET 
                name = EXCLUDED.name, 
                sector = EXCLUDED.sector, 
                industry = EXCLUDED.industry
            """), {
                "id": uuid.uuid4(),
                "t": ticker,
                "n": name,
                "s": sector,
                "i": industry
            })
        conn.commit()
        print("Companies seeded.")

if __name__ == "__main__":
    seed_companies()
