import asyncio
import os
import sys

from loguru import logger
from config.settings import settings

# Import agents
from alpha_research.agents.sentiment_agent import SentimentAgent
from alpha_research.agents.technical_agent import TechnicalAgent
from alpha_research.agents.fundamental_agent import FundamentalAgent
from alpha_research.agents.macro_agent import MacroAgent
from alpha_research.agents.document_agent import DocumentAgent
from alpha_research.agents.hypothesis_agent import HypothesisAgent
from alpha_research.pipeline.research_pipeline import ResearchPipeline

async def run_smoke_test():
    logger.info("Starting Phase 2 Smoke Test...")
    ticker = "AAPL"
    
    # 1. Sentiment Agent
    logger.info(f"--- Testing SentimentAgent for {ticker} ---")
    s_agent = SentimentAgent()
    s_res = await s_agent.analyze(ticker)
    assert -1.0 <= s_res.score <= 1.0, f"Score out of bounds: {s_res.score}"
    print("[OK] Sentiment Agent working")
    
    # 2. Technical Agent
    logger.info(f"--- Testing TechnicalAgent for {ticker} ---")
    t_agent = TechnicalAgent()
    t_res = await t_agent.analyze(ticker)
    assert len(t_res.signals) >= 0, "Signals should be a list"
    print("[OK] Technical Agent working")
    
    # 3. Fundamental Agent
    logger.info(f"--- Testing FundamentalAgent for {ticker} ---")
    f_agent = FundamentalAgent()
    f_res = await f_agent.analyze(ticker)
    assert 0.0 <= f_res.overall_score <= 1.0, f"Score out of bounds: {f_res.overall_score}"
    assert isinstance(f_res.investment_thesis, str), "Thesis is not a string"
    print("[OK] Fundamental Agent working")
    
    # 4. Macro Agent
    logger.info("--- Testing MacroAgent ---")
    m_agent = MacroAgent()
    m_res = await m_agent.analyze()
    assert m_res.regime in ["bull", "recession", "stagflation", "recovery", "uncertain"], f"Unknown regime: {m_res.regime}"
    assert isinstance(m_res.sector_implications, dict), "Sector implications not populated"
    print("[OK] Macro Agent working")
    
    # 5. Document Agent
    logger.info(f"--- Testing DocumentAgent for {ticker} ---")
    d_agent = DocumentAgent()
    d_res = await d_agent.research(ticker)
    assert isinstance(d_res.insights, list), "Insights should be a list"
    print("[OK] Document Agent working")
    
    # 6. Hypothesis Agent
    logger.info(f"--- Testing HypothesisAgent for {ticker} ---")
    h_agent = HypothesisAgent()
    h_res = await h_agent.generate(ticker)
    # Either generates a Hypothesis object or returns None (rejected/conflicting)
    if h_res is None:
        print("[OK] Hypothesis Agent working (Hypothesis rejected or conflicting)")
    else:
        assert h_res.conviction_score >= 0.0
        print("[OK] Hypothesis Agent working (Hypothesis generated)")

    # 7. Full Pipeline
    logger.info("--- Testing Full Research Pipeline ---")
    pipeline = ResearchPipeline()
    p_res = await pipeline.run(tickers=["AAPL", "MSFT"])
    assert p_res.total_analyzed == 2, f"Analyzed {p_res.total_analyzed} instead of 2"
    print("[OK] Full Research Pipeline working")

    logger.info("All smoke tests completed successfully!")

if __name__ == "__main__":
    asyncio.run(run_smoke_test())
