import pytest
import pandas as pd
from signal_generation.agents.walk_forward_agent import (
    prepare_splits_node,
    compute_robustness_node,
    WalkForwardState
)

def create_base_state() -> WalkForwardState:
    return {
        "signal": {
            "id": "123",
            "ticker": "AAPL",
            "timeframe": "1D",
            "strategy_code": "",
            "parameters": {}
        },
        "ticker": "AAPL",
        "price_data": pd.DataFrame(), # To be mocked
        "n_splits": 5,
        "train_pct": 0.7,
        "splits": [],
        "in_sample_results": [],
        "out_sample_results": [],
        "consistency_score": 0.0,
        "overfit_score": 0.0,
        "passed": False,
        "recommendation": "",
        "error": None
    }

@pytest.mark.asyncio
async def test_splits_created_correctly():
    state = create_base_state()
    # Mock 1000 days of data
    dates = pd.date_range("2010-01-01", periods=1000)
    df = pd.DataFrame({"close": [1]*1000}, index=dates)
    state["price_data"] = {"AAPL": df}
    
    result = prepare_splits_node(state)
    splits = result["splits"]
    
    assert len(splits) == 5
    for i in range(1, 5):
        assert "train_end" in splits[i]
        assert "test_start" in splits[i]
        assert "test_end" in splits[i]

@pytest.mark.asyncio
async def test_consistency_score_calculation():
    state = create_base_state()
    # 5 splits, 4 of them positive return
    state["out_sample_results"] = [
        {"return": 0.05, "sharpe": 1.0},
        {"return": 0.02, "sharpe": 0.8},
        {"return": -0.01, "sharpe": -0.2},
        {"return": 0.08, "sharpe": 1.5},
        {"return": 0.01, "sharpe": 0.5}
    ]
    
    result = await compute_robustness_node(state)
    # 4 / 5 positive returns = 80% consistency
    assert result["consistency_score"] == 0.8
    assert result["passed"] is True # 80% > 60% threshold

@pytest.mark.asyncio
async def test_overfit_score_calculation():
    state = create_base_state()
    # In-sample was great, out-sample was terrible (overfit)
    state["in_sample_results"] = [
        {"sharpe": 3.0}, {"sharpe": 3.0}, {"sharpe": 3.0}, {"sharpe": 3.0}, {"sharpe": 3.0}
    ]
    state["out_sample_results"] = [
        {"return": 0.01, "sharpe": 0.5},
        {"return": 0.01, "sharpe": 0.5},
        {"return": 0.01, "sharpe": 0.5},
        {"return": 0.01, "sharpe": 0.5},
        {"return": 0.01, "sharpe": 0.5}
    ]
    
    result = await compute_robustness_node(state)
    assert result["overfit_score"] == 6.0
    assert result["passed"] is False # 6.0 > 2.0 threshold

@pytest.mark.asyncio
async def test_passes_with_good_oos_sharpe():
    state = create_base_state()
    state["in_sample_results"] = [{"sharpe": 1.5}] * 5
    state["out_sample_results"] = [{"return": 0.05, "sharpe": 1.2}] * 5
    
    result = await compute_robustness_node(state)
    assert result["passed"] is True
    assert isinstance(result["recommendation"], str)

@pytest.mark.asyncio
async def test_fails_with_low_oos_sharpe():
    state = create_base_state()
    state["in_sample_results"] = [{"sharpe": 1.5}] * 5
    # Very poor performance out of sample
    state["out_sample_results"] = [{"return": -0.05, "sharpe": 0.1}] * 5
    
    result = await compute_robustness_node(state)
    assert result["passed"] is False
    assert isinstance(result["recommendation"], str)

@pytest.mark.asyncio
async def test_fails_with_high_overfit_score():
    state = create_base_state()
    state["in_sample_results"] = [{"sharpe": 5.0}] * 5
    state["out_sample_results"] = [{"return": 0.05, "sharpe": 0.2}] * 5
    
    result = await compute_robustness_node(state)
    assert result["passed"] is False
    assert result["overfit_score"] == 25.0
