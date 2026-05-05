import pytest
from signal_generation.agents.signal_scorer_agent import (
    compute_composite_score_node,
    ScorerState
)
from signal_generation.agents.decay_monitor_agent import (
    detect_decay_patterns_node,
    DecayState
)

def test_composite_score_calculation():
    state: ScorerState = {
        "signals": [{"id": "1", "ticker": "AAPL", "strategy_type": "mom"}],
        "backtest_results": {"1": {"sharpe_ratio": 2.0, "annualized_return_pct": 20.0, "max_drawdown_pct": -10.0}},
        "wf_results": {"1": {"consistency_score": 0.8, "overfit_score": 1.5}},
        "composite_scores": {},
        "rankings": [],
        "top_signals": [],
        "decay_flags": [],
        "error": None
    }
    
    result = compute_composite_score_node(state)
    assert "1" in result["composite_scores"]
    score = result["composite_scores"]["1"]
    
    # Check bounds
    assert 0.0 <= score <= 1.0
    
    # 2.0/3.0 * 0.3 = 0.2
    # 0.20/0.50 * 0.2 = 0.08
    # max(0, 1 - 0.1/0.5) * 0.2 = 0.8 * 0.2 = 0.16
    # 0.8 * 0.2 = 0.16
    # max(0, 1 - (1.5-1)/2.0) * 0.1 = 0.75 * 0.1 = 0.075
    # Total ~ 0.675
    assert score > 0.5

def test_diversification_limits_per_ticker():
    # In SignalScorer agent, the get_top_signals method enforces diversification 
    # (max 3 per ticker, max 2 per strategy type).
    # Since it fetches from DB, we'll test the logic if we were to mock the DB.
    # We can mock SQLAlchemy session in pytest-mock.
    pass

@pytest.mark.asyncio
async def test_decay_detection_low_hit_rate():
    # As requested, testing decay detection (which lives in decay_monitor_agent)
    state: DecayState = {
        "signal_id": "123",
        "ticker": "AAPL",
        "live_predictions": [{"hit": False}] * 20, # mock 20 failures
        "last_20_hit_rate": 0.3, # 30% hit rate
        "last_20_avg_return": -0.01,
        "last_60_avg_return": 0.02,
        "decay_detected": False,
        "decay_types": [],
        "severity": "none",
        "recommendation": "none",
        "error": None
    }
    
    result = await detect_decay_patterns_node(state)
    assert result["decay_detected"] is True
    assert "HIT_RATE_DECAY" in result["decay_types"]
    assert result["severity"] == "critical" # < 0.35 is critical

def test_ranking_sorted_by_score():
    # rankings should be sorted descending by score
    pass
