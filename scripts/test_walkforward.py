import asyncio
import uuid
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal
from alpha_research.storage.research_models import ResearchHypothesis
from signal_generation.agents.walk_forward_agent import WalkForwardAgent

async def main():
    logger.info("Starting WalkForwardAgent test...")
    
    strategy_code = """import pandas as pd
import numpy as np
import vectorbt as vbt

def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    lookback = params.get('lookback', 20)
    
    # Calculate highs and lows
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
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Create mock hypothesis first
        hypo = ResearchHypothesis(
            id=mock_hypo_id,
            ticker="AAPL",
            hypothesis_type="technical",
            title="WF Test Signal",
            expected_direction="long",
            expected_timeframe="swing",
            conviction_score=0.8,
            status="pending",
            description="WF hypothesis",
            created_by_agent="wf_tester"
        )
        session.add(hypo)
        
        # Create signal linked to hypothesis
        mock_signal_data = {
            "id": mock_signal_id,
            "hypothesis_id": mock_hypo_id,
            "ticker": "AAPL",
            "strategy_code": strategy_code,
            "parameters": {"lookback": 20},
            "signal_name": "WF Test Breakout",
            "signal_type": "breakout",
            "entry_condition": "Price > High[20]",
            "exit_condition": "Price < Low[20]",
            "timeframe": "1D",
            "status": "validated", # Assuming it passed backtest
            "created_by": "wf_tester"
        }
        db_signal = TradingSignal(**mock_signal_data)
        session.add(db_signal)
        session.commit()
    
    validator = WalkForwardAgent()
    # Test with 3 splits for speed
    result = await validator.validate(mock_signal_data, n_splits=3, train_pct=0.7)
    
    logger.info("Test finished.")
    logger.info(f"Success: {result.get('error') is None}")
    
    if result.get("error"):
        logger.error(f"Error: {result['error']}")
    else:
        logger.info(f"Passed Filters: {result['passed']}")
        logger.info(f"Consistency Score: {result['consistency_score']:.2f}")
        logger.info(f"Overfit Score: {result['overfit_score']:.2f}")
        logger.info(f"LLM Recommendation: {result['recommendation']}")
        
        logger.info("\nSplit Details:")
        for res in result["out_sample_results"]:
            split_id = res["split_id"]
            best_p = res.get("best_params")
            sharpe = res["sharpe"]
            logger.info(f" Split {split_id} | OOS Sharpe: {sharpe:.2f} | Best Params: {best_p}")

if __name__ == "__main__":
    asyncio.run(main())
