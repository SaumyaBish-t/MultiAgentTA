from sqlalchemy import create_engine, text
from config.settings import settings

def fix_missing_data():
    engine = create_engine(settings.postgres_url)
    with engine.connect() as conn:
        print("Adding NVDA to companies table...")
        conn.execute(text("""
            INSERT INTO companies (ticker, name, sector, industry, exchange) 
            VALUES ('NVDA', 'NVIDIA Corporation', 'Technology', 'Semiconductors', 'NASDAQ')
            ON CONFLICT (ticker) DO NOTHING
        """))
        conn.commit()
    print("Done.")

if __name__ == "__main__":
    fix_missing_data()
