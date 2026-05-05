import asyncio
import uuid
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal, SignalParameter
from alpha_research.storage.research_models import ResearchHypothesis
from signal_generation.agents.optimizer_agent import ParameterOptimizer

async def main():
    logger.info("Starting ParameterOptimizer test...")
    
    strategy_code = """import pandas as pd
import numpy as np
import vectorbt as vbt

def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    lookback = params.get('lookback', 20)
    multiplier = params.get('multiplier', 1.5)
    
    # Simple bollinger bands breakout
    mean = price_data['close'].rolling(lookback).mean()
    std = price_data['close'].rolling(lookback).std()
    
    upper = mean + (std * multiplier)
    lower = mean - (std * multiplier)
    
    entries = price_data['close'] > upper
    exits = price_data['close'] < lower
    
    return entries, exits
"""
    
    mock_hypo_id = uuid.uuid4()
    mock_signal_id = uuid.uuid4()
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        hypo = ResearchHypothesis(
            id=mock_hypo_id,
            ticker="AAPL",
            hypothesis_type="technical",
            title="Opt Test Signal",
            expected_direction="long",
            expected_timeframe="swing",
            conviction_score=0.8,
            status="pending",
            description="Opt hypothesis",
            created_by_agent="opt_tester"
        )
        session.add(hypo)
        
        mock_signal_data = {
            "id": mock_signal_id,
            "hypothesis_id": mock_hypo_id,
            "ticker": "AAPL",
            "strategy_code": strategy_code,
            "parameters": {"lookback": 20, "multiplier": 1.5},
            "signal_name": "Opt Test",
            "signal_type": "breakout",
            "entry_condition": "Upper BB",
            "exit_condition": "Lower BB",
            "timeframe": "1D",
            "status": "validated",
            "created_by": "opt_tester"
        }
        db_signal = TradingSignal(**mock_signal_data)
        session.add(db_signal)
        session.flush()
        
        p1 = SignalParameter(
            id=uuid.uuid4(),
            signal_id=mock_signal_id,
            parameter_name="lookback",
            optimal_value=20.0,
            search_range={"min": 10, "max": 30, "step": 2},
            optimization_method="none",
            stability_score=0.0
        )
        p2 = SignalParameter(
            id=uuid.uuid4(),
            signal_id=mock_signal_id,
            parameter_name="multiplier",
            optimal_value=1.5,
            search_range={"min": 1.0, "max": 3.0, "step": 0.5},
            optimization_method="none",
            stability_score=0.0
        )
        session.add(p1)
        session.add(p2)
        session.commit()
    
    # Setup ranges based on what's in DB
    ranges = {
        "lookback": {"min": 10, "max": 30, "step": 2},
        "multiplier": {"min": 1.0, "max": 3.0, "step": 0.5}
    }
    
    optimizer = ParameterOptimizer()
    
    logger.info("--- Testing Grid Search ---")
    res_grid = await optimizer.optimize(mock_signal_data, param_ranges=ranges, method="grid")
    
    if res_grid.get("error"):
        logger.error(f"Grid Error: {res_grid['error']}")
    else:
        logger.info(f"Grid Best Params: {res_grid['best_params']}")
        logger.info(f"Grid Best Sharpe: {res_grid['best_sharpe']:.2f}")
        logger.info(f"Grid Stability: {res_grid['stability_scores']}")
        
    logger.info("--- Testing Bayesian Search ---")
    res_bayes = await optimizer.optimize(mock_signal_data, param_ranges=ranges, n_trials=10, method="bayesian")
    
    if res_bayes.get("error"):
        logger.error(f"Bayesian Error: {res_bayes['error']}")
    else:
        logger.info(f"Bayesian Best Params: {res_bayes['best_params']}")
        logger.info(f"Bayesian Best Sharpe: {res_bayes['best_sharpe']:.2f}")

if __name__ == "__main__":
    asyncio.run(main())
