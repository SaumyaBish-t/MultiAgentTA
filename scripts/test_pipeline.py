import asyncio
import uuid
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import settings
from alpha_research.storage.research_models import ResearchHypothesis
from signal_generation.pipeline.signal_pipeline import SignalPipeline

async def main():
    logger.info("Starting SignalPipeline test...")
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    mock_hypo_id = uuid.uuid4()
    
    with Session() as session:
        hypo = ResearchHypothesis(
            id=mock_hypo_id,
            ticker="AAPL",
            hypothesis_type="technical",
            title="Pipeline Full Integration Test",
            expected_direction="long",
            expected_timeframe="swing",
            conviction_score=0.95,
            status="pending",
            description="A simple SMA crossover for testing the entire pipeline.",
            created_by_agent="pipeline_tester"
        )
        session.add(hypo)
        session.commit()
        
    logger.info(f"Inserted pending hypothesis {mock_hypo_id}")
    
    # We run the Prefect-like entrypoint
    pipeline = SignalPipeline()
    logger.info("Running pipeline.run() ...")
    
    # Run the pipeline (this will fetch the hypothesis automatically)
    result = await pipeline.run()
    
    logger.info(f"--- Pipeline Finished (Run ID: {result.run_id}) ---")
    logger.info(f"Hypotheses processed: {result.hypotheses_processed}")
    logger.info(f"Signals generated: {result.signals_generated}")
    logger.info(f"Signals validated: {result.signals_validated}")
    logger.info(f"Signals rejected: {result.signals_rejected}")
    logger.info(f"Top signals ready: {len(result.top_signals)}")
    logger.info(f"Best Sharpe: {result.best_sharpe}")
    
    if result.top_signals:
        for s in result.top_signals:
            logger.info(f"Top Signal -> ID: {s['id']}, Score: {s['score']}, Sharpe: {s['sharpe']}")

if __name__ == "__main__":
    asyncio.run(main())
