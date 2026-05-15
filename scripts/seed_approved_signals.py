import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from config.settings import settings

def seed():
    engine = create_engine(settings.postgres_url)
    with engine.connect() as conn:
        # Disable FK checks
        conn.execute(text("SET session_replication_role = 'replica'"))
        # Check if table exists
        conn.execute(text("DELETE FROM portfolio_positions"))
        conn.execute(text("DELETE FROM approved_signals WHERE ticker IN ('AAPL', 'MSFT', 'GOOGL')"))
        
        tickers = [('AAPL', 50000, 0.8), ('MSFT', 40000, 0.7), ('GOOGL', 30000, 0.9)]
        for ticker, size, risk in tickers:
            sig_id = uuid.uuid4()
            app_id = uuid.uuid4()
            conn.execute(text("""
                INSERT INTO approved_signals 
                (id, signal_id, ticker, approved_position_size_pct, approved_position_size_usd, risk_score, approval_reason, status, valid_until, approved_at, created_at) 
                VALUES (:id, :sig_id, :t, :pct, :size, :risk, 'Good signal', 'approved', :until, :now, :now)
            """), {
                "id": app_id,
                "sig_id": sig_id,
                "t": ticker,
                "pct": size / 100000.0,
                "size": size,
                "risk": risk,
                "now": datetime.now(timezone.utc),
                "until": datetime.now(timezone.utc) + timedelta(days=1)
            })
        conn.commit()
        print(f"Seeded {len(tickers)} approved signals.")

if __name__ == "__main__":
    seed()
