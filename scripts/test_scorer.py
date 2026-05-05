import asyncio
import uuid
from datetime import datetime, timezone
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal, BacktestResult, WalkForwardResult
from alpha_research.storage.research_models import ResearchHypothesis
from signal_generation.agents.signal_scorer_agent import SignalScorer

async def main():
    logger.info("Starting SignalScorer test...")
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    mock_hypo_id = uuid.uuid4()
    
    # 5 Mock Signals:
    # 3 AAPL Momentum (to test strategy type limit of 2)
    # 1 AAPL Breakout (to test ticker limit of 3, this is the 4th AAPL)
    # 1 TSLA Trend
    
    with Session() as session:
        hypo = ResearchHypothesis(
            id=mock_hypo_id,
            ticker="AAPL",
            hypothesis_type="technical",
            title="Scorer Test",
            expected_direction="long",
            expected_timeframe="swing",
            conviction_score=0.8,
            status="pending",
            description="Scorer hypothesis",
            created_by_agent="scorer_tester"
        )
        session.add(hypo)
        session.flush()
        
        signal_configs = [
            {"id": uuid.uuid4(), "t": "AAPL", "st": "momentum", "bt_ret": 20.0, "bt_dd": -10.0, "wf_con": 0.8},
            {"id": uuid.uuid4(), "t": "AAPL", "st": "momentum", "bt_ret": 15.0, "bt_dd": -12.0, "wf_con": 0.7},
            {"id": uuid.uuid4(), "t": "AAPL", "st": "momentum", "bt_ret": 10.0, "bt_dd": -15.0, "wf_con": 0.6}, # Should be excluded (max 2 per strategy type)
            {"id": uuid.uuid4(), "t": "AAPL", "st": "breakout", "bt_ret": 25.0, "bt_dd": -8.0,  "wf_con": 0.9}, # Should be excluded (max 3 per ticker)
            {"id": uuid.uuid4(), "t": "TSLA", "st": "trend",    "bt_ret": 40.0, "bt_dd": -20.0, "wf_con": 0.5},
        ]
        
        # Insert signals with descending performance so they naturally sort 1, 2, 3, 4, 5 but get filtered
        # Actually, let's just make their composite scores obviously rank them.
        for config in signal_configs:
            sig = TradingSignal(
                id=config["id"],
                hypothesis_id=mock_hypo_id,
                ticker=config["t"],
                signal_name=f"Test {config['t']} {config['st']}",
                signal_type=config["st"],
                entry_condition="Test",
                exit_condition="Test",
                strategy_code="pass",
                timeframe="1D",
                parameters={},
                status="validated",
                created_by="scorer_tester"
            )
            session.add(sig)
            session.flush()
            
            bt = BacktestResult(
                id=uuid.uuid4(),
                signal_id=config["id"],
                ticker=config["t"],
                start_date=datetime(2020, 1, 1).date(),
                end_date=datetime(2021, 1, 1).date(),
                final_capital=120000.0,
                total_return_pct=config["bt_ret"],
                annualized_return_pct=config["bt_ret"],
                sharpe_ratio=config["bt_ret"] / 10.0,
                sortino_ratio=0.0,
                calmar_ratio=0.0,
                max_drawdown_pct=config["bt_dd"],
                max_drawdown_duration_days=10,
                win_rate=0.5,
                profit_factor=1.5,
                total_trades=50,
                avg_trade_return_pct=1.0,
                avg_holding_days=2.0,
                best_trade_pct=5.0,
                worst_trade_pct=-2.0,
                volatility_annualized=15.0,
                benchmark_return_pct=10.0,
                alpha=5.0,
                beta=1.0,
                equity_curve=[],
                monthly_returns={},
                trade_log=[],
                engine="vectorbt"
            )
            session.add(bt)
            
            wf = WalkForwardResult(
                id=uuid.uuid4(),
                signal_id=config["id"],
                ticker=config["t"],
                n_splits=5,
                train_pct=0.7,
                in_sample_sharpe=2.0,
                out_sample_sharpe=1.5,
                consistency_score=config["wf_con"],
                overfit_score=1.1,
                passed=True,
                splits_detail=[]
            )
            session.add(wf)
            
        session.commit()
        
    scorer = SignalScorer()
    
    logger.info("--- Testing score_all ---")
    all_signals = await scorer.score_all()
    logger.info(f"Total Validated Signals Scored: {len(all_signals)}")
    for s in all_signals:
        logger.info(f"Rank {s.rank}: {s.ticker} {s.strategy_type} | Score: {s.composite_score:.2f}")
        
    logger.info("\n--- Testing get_top_signals (Diversification check) ---")
    top_signals = await scorer.get_top_signals()
    logger.info(f"Selected Top Signals: {len(top_signals)}")
    for s in top_signals:
        logger.info(f"{s.ticker} {s.strategy_type} | Score: {s.composite_score:.2f}")
        
    logger.info("\n--- Testing get_signal_report ---")
    report = await scorer.get_signal_report()
    logger.info(f"LLM Report:\n{report}")

if __name__ == "__main__":
    asyncio.run(main())
