import pytest
import pandas as pd
import numpy as np

# Adjust imports based on the actual file structure
from signal_generation.agents.strategy_coder_agent import (
    validate_code_node,
    StrategyCoderState
)

def create_base_state(code: str) -> StrategyCoderState:
    return {
        "hypothesis": {"id": "123"},
        "ticker": "AAPL",
        "timeframe": "1D",
        "strategy_attempts": [],
        "current_code": code,
        "validation_errors": [],
        "parameters": {"lookback": 14},
        "strategy_type": "technical",
        "attempt_count": 1,
        "success": False,
        "error": None
    }

@pytest.mark.asyncio
async def test_code_syntax_validation_catches_errors():
    # Invalid python syntax (missing colon)
    bad_code = "def strategy(price_data, params)\n    return None"
    state = create_base_state(bad_code)
    
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) > 0
    assert "SyntaxError" in result["validation_errors"][0]

@pytest.mark.asyncio
async def test_safety_check_blocks_os_import():
    dangerous_code = "import os\ndef strategy(price_data, params):\n    os.system('ls')\n    return None, None"
    state = create_base_state(dangerous_code)
    
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) > 0
    assert any("Forbidden" in e and "os" in e for e in result["validation_errors"])

@pytest.mark.asyncio
async def test_safety_check_blocks_eval():
    dangerous_code = "def strategy(price_data, params):\n    eval('print(1)')\n    return None, None"
    state = create_base_state(dangerous_code)
    
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) > 0
    assert any("Forbidden function call" in e and "eval" in e for e in result["validation_errors"])

@pytest.mark.asyncio
async def test_strategy_signature_validation():
    # Correct signature (dry run fails if returns are missing, so we must return pd.Series to pass full validate)
    good_code = """
import pandas as pd
def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    entries = pd.Series(False, index=price_data.index)
    exits = pd.Series(False, index=price_data.index)
    return entries, exits
"""
    state = create_base_state(good_code)
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) == 0

    # Bad signature (name)
    bad_code = "def my_strat(data):\n    pass"
    state = create_base_state(bad_code)
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) > 0
    assert any("Function 'strategy' not found" in e for e in result["validation_errors"])

@pytest.mark.asyncio
async def test_template_ema_crossover_runs():
    code = """
import pandas as pd
def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    fast = params.get('fast', 10)
    slow = params.get('slow', 20)
    close = price_data['close']
    ema_fast = close.ewm(span=fast).mean()
    ema_slow = close.ewm(span=slow).mean()
    entries = (ema_fast > ema_slow) & (ema_fast.shift(1) <= ema_slow.shift(1))
    exits = (ema_fast < ema_slow) & (ema_fast.shift(1) >= ema_slow.shift(1))
    return entries, exits
"""
    state = create_base_state(code)
    state["parameters"] = {"fast": 10, "slow": 20}
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) == 0
    assert result["success"] is True

@pytest.mark.asyncio
async def test_template_rsi_mean_reversion_runs():
    code = """
import pandas as pd
import vectorbt as vbt
def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    window = params.get('window', 14)
    low_thresh = params.get('low', 30)
    high_thresh = params.get('high', 70)
    rsi = vbt.RSI.run(price_data['close'], window=window).rsi
    entries = rsi < low_thresh
    exits = rsi > high_thresh
    return entries, exits
"""
    state = create_base_state(code)
    state["parameters"] = {"window": 14, "low": 30, "high": 70}
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) == 0
    assert result["success"] is True

@pytest.mark.asyncio
async def test_template_breakout_runs():
    code = """
import pandas as pd
def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    window = params.get('window', 20)
    highs = price_data['high'].rolling(window=window).max().shift(1)
    lows = price_data['low'].rolling(window=window).min().shift(1)
    entries = price_data['close'] > highs
    exits = price_data['close'] < lows
    return entries, exits
"""
    state = create_base_state(code)
    state["parameters"] = {"window": 20}
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) == 0
    assert result["success"] is True

@pytest.mark.asyncio
async def test_dry_run_validates_output_types():
    # Returns incorrect types
    code = """
def strategy(price_data, params):
    return "True", "False"
"""
    state = create_base_state(code)
    result = await validate_code_node(state)
    assert len(result["validation_errors"]) > 0
    assert any("must be pandas Series" in e or "got <class 'str'>" in e for e in result["validation_errors"])
