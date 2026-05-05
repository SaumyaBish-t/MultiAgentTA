import sys
from pathlib import Path
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure project root is in path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from config.settings import settings
from data_ingestion.storage.models import FundamentalBase

# Import risk models to ensure they are registered with FundamentalBase
import risk_management.storage.risk_models as rm

def init_risk_database():
    logger.info("Initializing Risk Management Database Schema...")
    
    # 1. Create engine
    engine = create_engine(settings.postgres_url, echo=False)
    
    # 2. Create all tables defined on FundamentalBase
    # (This will also create tables from other modules if they share the base, 
    # but create_all is safe to run multiple times)
    try:
        FundamentalBase.metadata.create_all(engine)
        logger.info("✅ Risk management tables created successfully.")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        return

    # 3. Insert default circuit breakers
    Session = sessionmaker(bind=engine)
    with Session() as session:
        defaults = [
            rm.CircuitBreaker(
                breaker_type="portfolio_drawdown",
                threshold=-0.10,
                current_value=0.0,
                action="halt_new_trades"
            ),
            rm.CircuitBreaker(
                breaker_type="daily_loss",
                threshold=-0.03,
                current_value=0.0,
                action="halt_new_trades"
            ),
            rm.CircuitBreaker(
                breaker_type="volatility",
                threshold=0.40,
                current_value=0.0,
                action="reduce_50pct"
            ),
            rm.CircuitBreaker(
                breaker_type="sector_concentration",
                threshold=0.30,
                current_value=0.0,
                action="halt_new_trades"
            )
        ]
        
        # Check if they already exist to avoid duplicates on re-run
        existing = session.query(rm.CircuitBreaker.breaker_type).all()
        existing_types = {e[0] for e in existing}
        
        added_count = 0
        for cb in defaults:
            if cb.breaker_type not in existing_types:
                session.add(cb)
                logger.info(f"Inserted default Circuit Breaker: {cb.breaker_type} (threshold: {cb.threshold})")
                added_count += 1
                
        if added_count > 0:
            session.commit()
            logger.info(f"✅ Successfully initialized {added_count} default circuit breakers.")
        else:
            logger.info("⚡ Default circuit breakers already exist. Skipping insertion.")

if __name__ == "__main__":
    init_risk_database()
