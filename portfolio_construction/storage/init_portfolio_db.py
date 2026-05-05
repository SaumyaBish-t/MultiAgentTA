import os
import sys
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config.settings import settings
from data_ingestion.storage.models import FundamentalBase

# Import the models to ensure they are registered with Base.metadata
from portfolio_construction.storage.portfolio_models import (
    Portfolio,
    PortfolioPosition,
    PortfolioWeight,
    RebalanceEvent,
    FactorExposure,
    PortfolioPerformance,
    CostEstimate
)

def init_db():
    logger.info("Initializing Phase 5 Portfolio Construction Database...")
    
    # 1. Create Engine
    try:
        engine = create_engine(settings.postgres_url)
        logger.info(f"Connected to DB at {settings.postgres_url}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return

    # 2. Create Tables
    try:
        FundamentalBase.metadata.create_all(engine)
        logger.info("Successfully created all portfolio_construction tables in PostgreSQL.")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        return

    # 3. Seed initial default portfolio
    Session = sessionmaker(bind=engine)
    with Session() as session:
        try:
            # Check if it already exists
            existing = session.query(Portfolio).filter_by(name="main_portfolio").first()
            if not existing:
                default_portfolio = Portfolio(
                    name="main_portfolio",
                    strategy="black_litterman",
                    total_capital=100000.0,
                    invested_capital=0.0,
                    cash=100000.0,
                    status="active"
                )
                session.add(default_portfolio)
                session.commit()
                logger.info("Created default portfolio: main_portfolio (Black-Litterman, $100k)")
            else:
                logger.info("Default portfolio 'main_portfolio' already exists.")
        except Exception as e:
            logger.error(f"Failed to seed default portfolio: {e}")
            session.rollback()

if __name__ == "__main__":
    init_db()
