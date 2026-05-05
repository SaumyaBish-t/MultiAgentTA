import json
import redis
from sqlalchemy import create_engine, text
from config.settings import settings

def final_check():
    engine = create_engine(settings.postgres_url)
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    print("PHASE 5 FINAL VERIFICATION")
    print("-" * 30)
    
    with engine.connect() as conn:
        # 1. Portfolios
        pf = conn.execute(text("SELECT name, total_capital FROM portfolios WHERE name = 'main_portfolio'")).fetchone()
        print(f"Portfolio Record: {'[OK]' if pf else '[MISSING]'} {pf}")
        
        # 2. Pending Positions
        pos = conn.execute(text("SELECT ticker, target_shares FROM portfolio_positions WHERE status = 'pending'")).fetchall()
        print(f"Pending Positions: {len(pos)}")
        for p in pos:
            print(f"  - {p[0]}: {p[1]} shares")
            
    # 3. Redis keys
    weights = r.get("portfolio:target:weights")
    print(f"Redis Weights Cache: {'[OK]' if weights else '[MISSING]'}")
    
    state = r.get("portfolio:current:state")
    print(f"Redis Current State: {'[OK]' if state else '[MISSING]'}")
    
    print("-" * 30)

if __name__ == "__main__":
    final_check()
