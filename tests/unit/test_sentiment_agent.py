import pytest
from alpha_research.agents.sentiment_agent import aggregate_scores_node

@pytest.mark.asyncio
async def test_score_calculation_weighted_average():
    # News weight = 0.7, Reddit weight = 0.3
    state = {
        "ticker": "AAPL",
        "raw_scores": [
            {"source": "news", "score": 1.0, "magnitude": 1.0},
            {"source": "news", "score": 0.5, "magnitude": 0.8},
            {"source": "reddit", "score": -0.5, "magnitude": 0.5},
            {"source": "reddit", "score": 0.0, "magnitude": 0.2}
        ]
    }
    
    # Avg news score = 0.75, Avg news mag = 0.9
    # Avg reddit score = -0.25, Avg reddit mag = 0.35
    # Composite score = (0.75 * 0.7) + (-0.25 * 0.3) = 0.525 - 0.075 = 0.45
    # Composite mag = (0.9 * 0.7) + (0.35 * 0.3) = 0.63 + 0.105 = 0.735
    
    result = await aggregate_scores_node(state)
    assert round(result["aggregated_score"], 2) == 0.45
    assert round(result["magnitude"], 3) == 0.735


@pytest.mark.asyncio
async def test_sentiment_label_thresholds():
    # Bullish > 0.2, Bearish < -0.2, else Neutral
    
    # Bullish
    state1 = {"ticker": "AAPL", "raw_scores": [{"source": "news", "score": 0.3}]}
    res1 = await aggregate_scores_node(state1)
    assert res1["sentiment_label"] == "bullish"
    
    # Bearish
    state2 = {"ticker": "AAPL", "raw_scores": [{"source": "news", "score": -0.3}]}
    res2 = await aggregate_scores_node(state2)
    assert res2["sentiment_label"] == "bearish"
    
    # Neutral
    state3 = {"ticker": "AAPL", "raw_scores": [{"source": "news", "score": 0.1}]}
    res3 = await aggregate_scores_node(state3)
    assert res3["sentiment_label"] == "neutral"


@pytest.mark.asyncio
async def test_low_sample_risk_flag():
    # Less than 3 samples -> LOW_SAMPLE_COUNT
    state = {
        "ticker": "AAPL",
        "raw_scores": [
            {"source": "news", "score": 0.5, "magnitude": 0.5},
            {"source": "reddit", "score": 0.2, "magnitude": 0.2}
        ]
    }
    result = await aggregate_scores_node(state)
    assert "LOW_SAMPLE_COUNT" in result["risk_flags"]


@pytest.mark.asyncio
async def test_empty_news_handled_gracefully():
    state = {
        "ticker": "AAPL",
        "raw_scores": []
    }
    result = await aggregate_scores_node(state)
    assert result["aggregated_score"] == 0.0
    assert result["sentiment_label"] == "neutral"
    assert "NO_DATA_AVAILABLE" in result["risk_flags"]
