"""
Test Regime & Decay Agent
"""

import asyncio
import json
import redis
import uuid
import numpy as np
from datetime import datetime, timezone, date, timedelta
from monitoring.agents.regime_decay_agent import RegimeDecayAgent
from monitoring.storage.monitoring_models import SignalLivePerformance
from config.settings import settings
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

async def test_regime_decay():
    r = redis.from_url(settings.redis_url)
    engine = create_engine(settings.postgres_url)
    
    # 1. Clear old data
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM signal_live_performance"))
        conn.execute(text("DELETE FROM regime_detections"))
        conn.execute(text("DELETE FROM feedback_actions"))
        conn.execute(text("DELETE FROM retraining_triggers"))

    # 2. Mock a signal with decay
    signal_id = uuid.uuid4()
    ticker = "AAPL"
    
    # Create 60 days of performance records
    with Session(engine) as session:
        for i in range(60):
            day = date.today() - timedelta(days=i)
            # Make recent 20 days "bad" (low hit rate, low return)
            # and older 40 days "good"
            if i < 20:
                hit = np.random.choice([True, False], p=[0.3, 0.7])
                ret = np.random.normal(-0.005, 0.01)
            else:
                hit = np.random.choice([True, False], p=[0.7, 0.3])
                ret = np.random.normal(0.005, 0.01)
                
            perf = SignalLivePerformance(
                id=uuid.uuid4(),
                signal_id=signal_id,
                ticker=ticker,
                tracking_date=day,
                predicted_direction="up",
                actual_direction="up" if ret > 0 else "down",
                predicted_return=0.01,
                actual_return=ret,
                hit=bool(hit),
                signal_strength=0.8,
                created_at=datetime.now(timezone.utc)
            )
            session.add(perf)
        session.commit()

    # 3. Run Agent
    agent = RegimeDecayAgent()
    
    # Mocking the signal detector's private method to return our test signal
    original_get_signals = agent.decay_detector._get_all_active_signals
    agent.decay_detector._get_all_active_signals = lambda: [{"id": signal_id, "ticker": ticker, "regime_at_creation": "bull"}]
    
    # Note: Phase 1 API call will likely fail and use defaults (VIX=20, etc.)
    result = await agent.run()
    
    print("Regime & Decay Result:")
    print(f"  Current Regime: {result.current_regime} (Confidence: {result.regime_confidence})")
    print(f"  Regime Changed: {result.regime_changed}")
    print(f"  Healthy Signals: {result.healthy_signals}")
    print(f"  Decaying Signals: {result.decaying_signals}")
    print(f"  Feedback Actions: {len(result.feedback_actions)}")
    for action in result.feedback_actions:
        print(f"    - {action['type']}: {action.get('signal_id') or action.get('regime')}")

if __name__ == "__main__":
    asyncio.run(test_regime_decay())
