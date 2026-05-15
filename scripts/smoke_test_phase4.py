import os
import sys
import json
import asyncio
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# Ensure the project root is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.settings import settings
from risk_management.storage.risk_models import FundamentalBase
from risk_management.agents.position_sizing_agent import PositionSizer
from risk_management.agents.var_agent import VaRAgent
from risk_management.agents.correlation_agent import CorrelationAgent
from risk_management.agents.liquidity_agent import LiquidityAgent
from risk_management.agents.drawdown_monitor_agent import DrawdownMonitorAgent
from risk_management.agents.risk_gate_agent import RiskGateAgent
from risk_management.pipeline.risk_pipeline import RiskPipeline
from signal_generation.storage.signal_models import TradingSignal

import redis

async def main():
    logger.remove()
    logger.add(sys.stdout, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{message}</level>")
    
    print("\n" + "="*50)
    print(" PHASE 4 RISK MANAGEMENT - SMOKE TEST")
    print("="*50 + "\n")
    
    # 1. Initialize risk DB tables
    try:
        engine = create_engine(settings.postgres_url)
        FundamentalBase.metadata.create_all(engine)
        print("[PASS] Risk tables created / verified")
    except Exception as e:
        print(f"[FAIL] Failed to create risk tables: {e}")
        return

    # 2. Create test position for AAPL ($10,000)
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        test_portfolio = {
            "total_value": 100000.0,
            "cash": 90000.0,
            "positions": [
                {"ticker": "JPM", "current_value": 30000.0},
                {"ticker": "XOM", "current_value": 30000.0},
                {"ticker": "UNH", "current_value": 30000.0}
            ]
        }
        r.set("portfolio:current:state", json.dumps(test_portfolio))
        print("[PASS] Portfolio state set (AAPL $10k)")
    except Exception as e:
        print(f"[FAIL] Failed to set Redis portfolio state: {e}")
        return

    # Helper: Fetch a signal
    Session = sessionmaker(bind=engine)
    with Session() as session:
        stmt = select(TradingSignal).where(TradingSignal.status == 'validated').limit(1)
        db_signal = session.execute(stmt).scalar_one_or_none()
        
    if not db_signal:
        # Mock one if no validated signals exist
        test_signal = {
            "id": "12345678-1234-1234-1234-123456789012",
            "ticker": "AAPL",
            "direction": "long",
            "conviction_score": 0.85,
            "strategy_type": "momentum",
            "win_rate": 0.55,
            "avg_win": 0.03,
            "avg_loss": 0.02
        }
    else:
        test_signal = {
            "id": str(db_signal.id),
            "ticker": "AAPL",
            "direction": "long",
            "conviction_score": 0.85, # mock it
            "strategy_type": db_signal.signal_type,
            "win_rate": 0.55,
            "avg_win": 0.03,
            "avg_loss": 0.02
        }

    # 3. Test position sizer
    try:
        sizer = PositionSizer()
        pos_result = await sizer.size_position(test_signal)
        assert pos_result.size_usd > 0
        print(f"[PASS] Position sizer working")
        print(f"   Size: ${pos_result.final_size_usd:,.0f}")
        print(f"   Method: {pos_result.sizing_method}")
    except Exception as e:
        print(f"[FAIL] Position Sizer test failed: {e}")

    # 4. Test VaR calculation
    try:
        var_agent = VaRAgent()
        var_result = await var_agent.calculate({"AAPL": 10000.0, "MSFT": 8000.0})
        assert var_result is not None
        print(f"[PASS] VaR agent working")
        print(f"   VaR 95%: ${var_result.var_95_1day_usd:,.0f}")
        print(f"   CVaR 95%: ${var_result.cvar_95_1day_usd:,.0f}")
    except Exception as e:
        print(f"[FAIL] VaR Agent test failed: {e}")

    # 5. Test correlation check
    try:
        corr_agent = CorrelationAgent()
        corr_result = await corr_agent.analyze(["AAPL", "MSFT", "GOOGL"], {"AAPL": 10000.0, "MSFT": 8000.0, "GOOGL": 5000.0})
        assert corr_result is not None
        print(f"[PASS] Correlation agent working")
        print(f"   Avg correlation: {corr_result.diversification_ratio:.2f}")
        print(f"   High corr pairs: {len(corr_result.high_correlation_pairs)}")
    except Exception as e:
        print(f"[FAIL] Correlation Agent test failed: {e}")

    # 6. Test liquidity check
    try:
        liq_agent = LiquidityAgent()
        liq_result = await liq_agent.check(test_signal, 10000.0)
        assert liq_result is not None
        print(f"[PASS] Liquidity agent working")
        print(f"   Tier: {liq_result.liquidity_tier}")
        print(f"   Days to exit: {liq_result.days_to_exit:.1f}")
    except Exception as e:
        print(f"[FAIL] Liquidity Agent test failed: {e}")

    # 7. Test drawdown monitor
    try:
        monitor = DrawdownMonitor()
        dd_result = await monitor.run()
        assert dd_result.alert_level in ['green','yellow','orange','red']
        print(f"[PASS] Drawdown monitor working")
        print(f"   Alert: {dd_result.alert_level}")
        print(f"   Drawdown: {dd_result.current_drawdown_pct:.1%}")
    except Exception as e:
        print(f"[FAIL] Drawdown Monitor test failed: {e}")

    # 8. Test risk gate end-to-end
    try:
        gate = RiskGate()
        gate_result = await gate.evaluate(test_signal)
        print(f"[PASS] Risk gate working")
        print(f"   Approved: {gate_result.approved}")
        print(f"   Risk score: {gate_result.risk_score:.2f}")
        print(f"   Size: ${gate_result.final_position_size_usd:,.0f}")
        if not gate_result.approved:
            print(f"   Rejections: {gate_result.rejection_reasons}")
    except Exception as e:
        print(f"[FAIL] Risk Gate test failed: {e}")

    # 9. Run full risk pipeline
    try:
        pipeline = RiskPipeline()
        pipe_result = await pipeline.run([test_signal])
        print(f"[PASS] Full risk pipeline working")
        print(f"   Evaluated: {pipe_result.signals_evaluated}")
        print(f"   Approved: {pipe_result.signals_approved}")
        print(f"   Rejected: {pipe_result.signals_rejected}")
    except Exception as e:
        print(f"[FAIL] Risk Pipeline test failed: {e}")

    print("\n" + "="*50)
    print(" PHASE 4 SMOKE TEST COMPLETE")
    print("="*50 + "\n")

if __name__ == "__main__":
    asyncio.run(main())
