import pytest
from alpha_research.agents.hypothesis_agent import assess_signal_alignment_node, validate_hypothesis_node

@pytest.mark.asyncio
async def test_signal_alignment_strongly_bullish():
    state = {
        "ticker": "AAPL",
        "sentiment_result": {"label": "bullish"},
        "technical_result": {"bias": "bullish"},
        "fundamental_result": {"overall_score": 0.8},
        "macro_result": {"regime": "bull"}
    }
    res = await assess_signal_alignment_node(state)
    assert res["signal_alignment"] == "strongly_aligned_bullish"


@pytest.mark.asyncio
async def test_signal_alignment_conflicting():
    state = {
        "ticker": "AAPL",
        "sentiment_result": {"label": "bullish"},
        "technical_result": {"bias": "bearish"},
        "fundamental_result": {"overall_score": 0.8},
        "macro_result": {"regime": "recession"}
    }
    # Bullish = Sentiment(1), Fundamental(1) = 2
    # Bearish = Tech(1), Macro(1) = 2
    res = await assess_signal_alignment_node(state)
    assert res["signal_alignment"] == "conflicting"


@pytest.mark.asyncio
async def test_low_conviction_hypothesis_rejected():
    state = {
        "hypothesis": {
            "expected_direction": "long",
            "expected_timeframe": "swing",
            "conviction_score": 0.4
        },
        "macro_result": {"regime": "bull"},
        "fundamental_result": {"overall_score": 0.5}
    }
    res = await validate_hypothesis_node(state)
    assert res["rejection_reason"] == "LOW_CONVICTION"


@pytest.mark.asyncio
async def test_macro_headwind_rejection():
    state = {
        "hypothesis": {
            "expected_direction": "long",
            "expected_timeframe": "swing",
            "conviction_score": 0.8
        },
        "macro_result": {"regime": "recession"},
        "fundamental_result": {"overall_score": 0.5}
    }
    res = await validate_hypothesis_node(state)
    assert res["rejection_reason"] == "MACRO_HEADWIND"
