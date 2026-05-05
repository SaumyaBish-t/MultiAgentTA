"""
Test Feedback Loop Agent
"""

import asyncio
import json
import redis
import uuid
from dataclasses import dataclass
from typing import List, Dict, Any

from monitoring.feedback.feedback_agent import FeedbackAgent, FeedbackResult
from monitoring.agents.regime_decay_agent import RegimeResult, DecayResult
from monitoring.agents.pnl_monitor_agent import PnLResult
from monitoring.agents.anomaly_detection_agent import AnomalyReport
from config.settings import settings
from sqlalchemy import create_engine, text

@dataclass
class MockRegimeResult:
    regime: str
    confidence: float
    regime_changed: bool
    previous_regime: str
    
@dataclass
class MockDecayResult:
    signal_id: uuid.UUID
    ticker: str
    status: str
    hit_rate_recent: float

@dataclass
class MockPnLResult:
    rolling_metrics: Dict[str, Any]

@dataclass
class MockAnomalyReport:
    critical: int

async def test_feedback_loop():
    engine = create_engine(settings.postgres_url)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM feedback_actions"))
        conn.execute(text("DELETE FROM retraining_triggers"))
    
    agent = FeedbackAgent()
    
    # 1. Mock Results
    regime = MockRegimeResult(regime="bear", confidence=0.85, regime_changed=True, previous_regime="bull")
    
    decay = [
        MockDecayResult(signal_id=uuid.uuid4(), ticker="AAPL", status="critical_decay", hit_rate_recent=0.2)
    ]
    
    pnl = MockPnLResult(rolling_metrics={"30d": {"sharpe": 0.1, "return": -0.1}})
    
    anomaly = MockAnomalyReport(critical=6)
    
    # 2. Process Feedback
    print("Processing feedback results...")
    result = await agent.process_feedback(regime, decay, pnl, anomaly)
    
    print("\nFeedback Result:")
    print(f"  Actions Generated: {len(result.actions)}")
    print(f"  Actions Applied: {result.applied}")
    print(f"  Actions Failed: {result.failed}")
    
    for action in result.actions:
        print(f"    - {action['action_type']} ({action['status']}): {action['details']}")

    # 3. Verify Redis State
    r = redis.from_url(settings.redis_url, decode_responses=True)
    regime_json = r.get("research:regime:current")
    halted = r.get("risk:trading:halted")
    
    print("\nRedis State:")
    print(f"  Current Regime in Redis: {json.loads(regime_json)['regime'] if regime_json else 'None'}")
    print(f"  Trading Halted: {halted}")

if __name__ == "__main__":
    asyncio.run(test_feedback_loop())
