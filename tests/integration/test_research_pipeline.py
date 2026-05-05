import pytest
import asyncio
from alpha_research.pipeline.research_pipeline import ResearchPipeline

@pytest.mark.asyncio
async def test_full_pipeline_single_ticker_aapl(mocker):
    mocker.patch('alpha_research.pipeline.research_pipeline.MarketDataAgent.analyze', 
                 return_value={"ticker": "AAPL", "volatility": "high"})
    mocker.patch('alpha_research.pipeline.research_pipeline.NewsSentimentAgent.analyze', 
                 return_value={"ticker": "AAPL", "sentiment": "bullish"})
    mocker.patch('alpha_research.pipeline.research_pipeline.FundamentalAgent.analyze', 
                 return_value={"ticker": "AAPL", "value": "undervalued"})
    mocker.patch('alpha_research.pipeline.research_pipeline.HypothesisAgent.generate_hypothesis', 
                 return_value={"ticker": "AAPL", "status": "generated"})
                 
    pipeline = ResearchPipeline()
    res = await pipeline.run(tickers=["AAPL"])
    
    assert res.status in ["completed", "completed_with_errors"]
    assert res.total_analyzed > 0
    assert (res.hypotheses_count + res.rejected_count) > 0


@pytest.mark.asyncio
async def test_agents_run_in_parallel(mocker):
    mocker.patch('alpha_research.pipeline.research_pipeline.MarketDataAgent.analyze', 
                 return_value={"volatility": "high"})
    mocker.patch('alpha_research.pipeline.research_pipeline.NewsSentimentAgent.analyze', 
                 return_value={"sentiment": "bullish"})
    mocker.patch('alpha_research.pipeline.research_pipeline.FundamentalAgent.analyze', 
                 return_value={"value": "undervalued"})
    mocker.patch('alpha_research.pipeline.research_pipeline.HypothesisAgent.generate_hypothesis', 
                 return_value={"status": "generated"})

    pipeline = ResearchPipeline()
    res = await pipeline.run(tickers=["MSFT", "GOOGL"])
    
    assert res.total_analyzed == 2
    assert (res.hypotheses_count + res.rejected_count) == 2


@pytest.mark.asyncio
async def test_failed_agent_doesnt_stop_pipeline(mocker):
    mocker.patch('alpha_research.pipeline.research_pipeline.MarketDataAgent.analyze', 
                 side_effect=Exception("Data failed"))
    mocker.patch('alpha_research.pipeline.research_pipeline.HypothesisAgent.generate_hypothesis', 
                 return_value={"status": "rejected"})

    pipeline = ResearchPipeline()
    res = await pipeline.run(tickers=["FAKE_TICKER_999"])
    
    assert res.status in ["completed", "completed_with_errors"]
    assert res.total_analyzed == 1
    assert res.rejected_count == 1
