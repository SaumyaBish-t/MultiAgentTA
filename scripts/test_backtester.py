import asyncio
import uuid
import sys
from loguru import logger

# Add root to pythonpath
sys.path.append(".")

from signal_generation.agents.backtester_agent import Backtester

async def main():
    logger.info("Starting BacktesterAgent test...")
    
    # We will use the built-in BREAKOUT template for our test signal
    strategy_code = """import pandas as pd
import numpy as np
import vectorbt as vbt

def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    # Extract parameters
    lookback = params.get('lookback', 20)
    
    # Calculate breakout levels
    high_break = price_data['close'] > price_data['high'].rolling(lookback).max().shift(1)
    low_break = price_data['close'] < price_data['low'].rolling(lookback).min().shift(1)
    
    # Generate signals
    entries = high_break
    exits = low_break
    
    return entries, exits
"""
    
    mock_hypo_id = uuid.uuid4()
    mock_signal_id = uuid.uuid4()
    
    # Insert into DB
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from signal_generation.storage.signal_models import TradingSignal
    from alpha_research.storage.research_models import ResearchHypothesis
    from config.settings import settings
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Create mock hypothesis first
        hypo = ResearchHypothesis(
            id=mock_hypo_id,
            ticker="AAPL",
            hypothesis_type="technical",
            title="Test Breakout Signal",
            expected_direction="long",
            expected_timeframe="swing",
            conviction_score=0.8,
            status="pending",
            description="Test hypothesis",
            created_by_agent="tester"
        )
        session.add(hypo)
        
        # Create signal linked to hypothesis
        mock_signal_data = {
            "id": mock_signal_id,
            "hypothesis_id": mock_hypo_id,
            "ticker": "AAPL",
            "strategy_code": strategy_code,
            "parameters": {"lookback": 20},
            "signal_name": "Test Breakout",
            "signal_type": "breakout",
            "entry_condition": "Price > High[20]",
            "exit_condition": "Price < Low[20]",
            "timeframe": "1D",
            "status": "draft",
            "created_by": "tester"
        }
        db_signal = TradingSignal(**mock_signal_data)
        session.add(db_signal)
        session.commit()
    
    backtester = Backtester()
    result = await backtester.backtest(mock_signal_data)
    
    logger.info("Test finished.")
    logger.info(f"Success: {result.get('error') is None}")
    if result.get('error'):
        logger.error(f"Error: {result.get('error')}")
    else:
        metrics = result.get('metrics', {})
        logger.info(f"Passed Filters: {result.get('passed_filters')}")
        logger.info(f"Rejection Reasons: {result.get('rejection_reasons')}")
        logger.info(f"Sharpe Ratio: {metrics.get('sharpe_ratio')}")
        logger.info(f"Total Return: {metrics.get('total_return_pct')}%")
        logger.info(f"Win Rate: {metrics.get('win_rate')}")
        
if __name__ == "__main__":
    asyncio.run(main())
