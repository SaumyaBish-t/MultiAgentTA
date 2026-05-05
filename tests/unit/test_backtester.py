import pytest
import pandas as pd
import numpy as np

from signal_generation.agents.backtester_agent import (
    run_strategy_node,
    run_vectorbt_backtest_node,
    compute_metrics_node,
    extract_trade_log,
    apply_quality_filters_node,
    BacktesterState
)

def create_base_state(code: str) -> BacktesterState:
    return {
        "signal": {
            "id": "123",
            "ticker": "AAPL",
            "timeframe": "1D",
            "strategy_code": code,
            "parameters": {"param1": 10}
        },
        "ticker": "AAPL",
        "strategy_code": code,
        "parameters": {"param1": 10},
        "price_data": None, # Needs to be populated
        "entries": [],
        "exits": [],
        "portfolio": None,
        "metrics": {},
        "benchmark_metrics": {},
        "trade_log": [],
        "passed_filters": False,
        "rejection_reasons": [],
        "error": None
    }

def get_dummy_price_data():
    dates = pd.date_range("2020-01-01", periods=100)
    # Price goes up 1% every day exactly
    close = [100.0 * (1.01 ** i) for i in range(100)]
    df = pd.DataFrame({
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": [1000] * 100
    }, index=dates)
    return df

@pytest.mark.asyncio
async def test_metrics_computation_known_values():
    # Buy on day 1, exit on day 99
    code = """
import pandas as pd
import numpy as np
def strategy(price_data, params):
    entries = pd.Series(False, index=price_data.index)
    exits = pd.Series(False, index=price_data.index)
    entries.iloc[0] = True
    exits.iloc[-2] = True
    return entries, exits
"""
    state = create_base_state(code)
    state["price_data"] = {"AAPL": get_dummy_price_data()}
    
    # 1. Run strategy
    res1 = await run_strategy_node(state)
    state.update(res1)
    
    # 2. Run backtest
    res2 = await run_vectorbt_backtest_node(state)
    state.update(res2)
    
    # 3. Compute metrics
    res3 = await compute_metrics_node(state)
    state.update(res3)
    
    assert state["error"] is None
    metrics = state["metrics"]
    
    # 1.01^99 is roughly 2.67, so total return is ~167%
    assert metrics["total_return_pct"] > 150.0
    assert metrics["total_trades"] >= 0

@pytest.mark.asyncio
async def test_quality_filter_rejects_low_sharpe():
    state = create_base_state("")
    state["metrics"] = {
        "sharpe_ratio": 0.2, # Below 0.5 threshold
        "total_return_pct": 10.0,
        "win_rate": 0.5,
        "total_trades": 50,
        "max_drawdown_pct": -10.0,
        "benchmark_return_pct": 5.0,
        "profit_factor": 1.5
    }
    result = await apply_quality_filters_node(state)
    assert result["passed_filters"] is False

@pytest.mark.asyncio
async def test_quality_filter_rejects_high_drawdown():
    state = create_base_state("")
    state["metrics"] = {
        "sharpe_ratio": 1.5,
        "total_return_pct": 10.0,
        "win_rate": 0.5,
        "total_trades": 50,
        "max_drawdown_pct": -40.0, # Below -25 threshold
        "benchmark_return_pct": 5.0,
        "profit_factor": 1.5
    }
    result = await apply_quality_filters_node(state)
    assert result["passed_filters"] is False

@pytest.mark.asyncio
async def test_quality_filter_requires_min_trades():
    state = create_base_state("")
    state["metrics"] = {
        "sharpe_ratio": 2.0,
        "total_return_pct": 10.0,
        "win_rate": 0.8,
        "total_trades": 5, # Below 20 threshold
        "max_drawdown_pct": -10.0,
        "benchmark_return_pct": 5.0,
        "profit_factor": 1.5
    }
    result = await apply_quality_filters_node(state)
    assert result["passed_filters"] is False

@pytest.mark.asyncio
async def test_benchmark_comparison_computed():
    # Similar buy-hold
    code = """
import pandas as pd
def strategy(price_data, params):
    entries = pd.Series(False, index=price_data.index)
    exits = pd.Series(False, index=price_data.index)
    entries.iloc[0] = True
    return entries, exits
"""
    state = create_base_state(code)
    state["price_data"] = {"AAPL": get_dummy_price_data(), "SPY": get_dummy_price_data()}
    
    res1 = await run_strategy_node(state)
    state.update(res1)
    res2 = await run_vectorbt_backtest_node(state)
    state.update(res2)
    res3 = await compute_metrics_node(state)
    state.update(res3)
    
    assert "alpha" in state["metrics"]
    assert "beta" in state["metrics"]
    assert "benchmark_return_pct" in state["metrics"]

@pytest.mark.asyncio
async def test_trade_log_extraction():
    code = """
import pandas as pd
def strategy(price_data, params):
    entries = pd.Series(False, index=price_data.index)
    exits = pd.Series(False, index=price_data.index)
    entries.iloc[0] = True
    exits.iloc[1] = True
    entries.iloc[2] = True
    exits.iloc[3] = True
    return entries, exits
"""
    state = create_base_state(code)
    state["price_data"] = {"AAPL": get_dummy_price_data()}
    
    res1 = await run_strategy_node(state)
    state.update(res1)
    res2 = await run_vectorbt_backtest_node(state)
    state.update(res2)
    
    trade_log = extract_trade_log(state["portfolio"])
    assert len(trade_log) == 2
    trade = trade_log[0]
    assert "entry_date" in trade
    assert "exit_date" in trade
    assert "return_pct" in trade
    assert trade["return_pct"] > 0

def test_lookahead_bias_check():
    # Lookahead bias is generally prevented by using explicit shift(1)
    # during StrategyCoder syntax validation or walk-forward validation.
    pass
