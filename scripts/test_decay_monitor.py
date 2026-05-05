import asyncio
import uuid
from datetime import datetime, timezone, timedelta
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal, SignalPerformanceLive
from alpha_research.storage.research_models import ResearchHypothesis
from signal_generation.agents.decay_monitor_agent import DecayMonitor

async def main():
    logger.info("Starting DecayMonitor test...")
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    mock_hypo_id = uuid.uuid4()
    
    sig_good_id = uuid.uuid4()
    sig_hit_decay_id = uuid.uuid4()
    sig_crit_decay_id = uuid.uuid4()
    sig_ret_decay_id = uuid.uuid4()
    
    with Session() as session:
        hypo = ResearchHypothesis(
            id=mock_hypo_id,
            ticker="AAPL",
            hypothesis_type="technical",
            title="Decay Test",
            expected_direction="long",
            expected_timeframe="swing",
            conviction_score=0.8,
            status="pending",
            description="Decay hypothesis",
            created_by_agent="decay_tester"
        )
        session.add(hypo)
        session.flush()
        
        configs = [
            (sig_good_id, "AAPL", "Good Signal"),
            (sig_hit_decay_id, "MSFT", "Hit Decay Signal"),
            (sig_crit_decay_id, "TSLA", "Critical Decay Signal"),
            (sig_ret_decay_id, "AMZN", "Return Decay Signal"),
        ]
        
        for sig_id, ticker, name in configs:
            sig = TradingSignal(
                id=sig_id,
                hypothesis_id=mock_hypo_id,
                ticker=ticker,
                signal_name=name,
                signal_type="momentum",
                entry_condition="Test",
                exit_condition="Test",
                strategy_code="pass",
                timeframe="1D",
                parameters={},
                status="live",
                created_by="decay_tester"
            )
            session.add(sig)
        session.flush()
        
        # Helper to insert perf records
        def insert_perf(s_id, ticker, is_hit, ret_val, days_ago):
            perf = SignalPerformanceLive(
                id=uuid.uuid4(),
                signal_id=s_id,
                ticker=ticker,
                date=(datetime.now(timezone.utc) - timedelta(days=days_ago)).date(),
                predicted_direction="LONG",
                actual_direction="LONG" if is_hit else "SHORT",
                predicted_return=1.0,
                actual_return=ret_val,
                hit=is_hit,
                cumulative_hit_rate=0.5
            )
            session.add(perf)
            
        # 1. Good Signal (last 20: 60% hit rate, positive return)
        for i in range(20):
            # 12 hits, 8 misses = 60% hit rate
            is_hit = i < 12 
            insert_perf(sig_good_id, "AAPL", is_hit, 2.0 if is_hit else -1.0, i)
            
        # 2. Hit Decay Signal (last 20: 40% hit rate) -> medium severity
        for i in range(20):
            # 8 hits, 12 misses = 40% hit rate (< 0.45)
            is_hit = i < 8
            insert_perf(sig_hit_decay_id, "MSFT", is_hit, 2.0 if is_hit else -1.0, i)
            
        # 3. Critical Decay Signal (last 20: 30% hit rate) -> critical severity
        for i in range(20):
            # 6 hits, 14 misses = 30% hit rate (< 0.35)
            is_hit = i < 6
            insert_perf(sig_crit_decay_id, "TSLA", is_hit, 2.0 if is_hit else -1.0, i)
            
        # 4. Return Decay Signal (last 20 avg < 0, but last 60 avg > 0)
        # We need 60 records here.
        # Older 40 records: super positive returns
        for i in range(20, 60):
            insert_perf(sig_ret_decay_id, "AMZN", True, 5.0, i)
        # Recent 20 records: negative returns
        for i in range(20):
            insert_perf(sig_ret_decay_id, "AMZN", True, -1.0, i)
            
        session.commit()
        
    monitor = DecayMonitor()
    
    logger.info("--- Testing check_all_live_signals ---")
    results = await monitor.check_all_live_signals()
    
    for r in results:
        logger.info(f"Signal {r.ticker}: Detected={r.decay_detected}, Types={r.decay_types}, Severity={r.severity}, Rec={r.recommendation}")
        
    logger.info("\n--- Testing get_health_report ---")
    report = await monitor.get_health_report()
    logger.info(f"Health Report: {report}")

if __name__ == "__main__":
    asyncio.run(main())
