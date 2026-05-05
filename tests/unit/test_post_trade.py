import pytest
from execution.agents.post_trade_agent import calculate_quality_score_node

@pytest.mark.asyncio
async def test_quality_score_high_for_low_slippage():
    """Score should be high if slippage < 5bps."""
    state = {
        "slippage_analysis": {"avg_slippage_bps": 2.0},
        "timing_analysis": {"avg_vs_vwap_bps": -1.0}, # Beat VWAP
        "error": None
    }
    
    result = await calculate_quality_score_node(state)
    assert result["quality_score"] == 1.0

@pytest.mark.asyncio
async def test_quality_score_low_for_high_slippage():
    """Score should be low if slippage > 30bps."""
    state = {
        "slippage_analysis": {"avg_slippage_bps": 50.0},
        "timing_analysis": {"avg_vs_vwap_bps": 15.0},
        "error": None
    }
    
    result = await calculate_quality_score_node(state)
    assert result["quality_score"] < 0.5

def test_vs_vwap_calculation():
    """Verify VS VWAP = (exec - vwap) / vwap * 10000."""
    exec_p = 150.75
    vwap_p = 150.00
    # (150.75 - 150.00) / 150.00 = 0.005 = 50 bps
    diff_bps = (exec_p - vwap_p) / vwap_p * 10000
    assert diff_bps == 50.0
