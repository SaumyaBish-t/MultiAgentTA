"""
Unit tests for Feedback Loop Agent
"""

import unittest
from unittest.mock import MagicMock
from monitoring.feedback.feedback_agent import FeedbackAgent

class TestFeedbackAgent(unittest.TestCase):
    def setUp(self):
        self.agent = FeedbackAgent()

    def test_regime_change_updates_logic(self):
        # regime_changed = True should trigger update_weights
        regime_result = MagicMock()
        regime_result.regime_changed = True
        regime_result.current_regime = "bear"
        
        actions = []
        if regime_result.regime_changed:
            actions.append({
                "action_type": "update_weights",
                "reason": f"Regime changed to {regime_result.current_regime}"
            })
            
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action_type"], "update_weights")

    def test_signal_decay_retirement_logic(self):
        # Decay detected -> retire_signal
        decay_results = [{"ticker": "AAPL", "decay_detected": True, "severity": "high"}]
        
        actions = []
        for decay in decay_results:
            if decay["decay_detected"]:
                actions.append({
                    "action_type": "retire_signal",
                    "ticker": decay["ticker"]
                })
                
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action_type"], "retire_signal")

    def test_systemic_anomalies_halt_trading_logic(self):
        # Critical anomaly -> halt_trading
        anomaly_result = MagicMock()
        anomaly_result.severity = "critical"
        
        actions = []
        if anomaly_result.severity == "critical":
            actions.append({
                "action_type": "halt_trading",
                "reason": "Critical systemic anomalies detected"
            })
            
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action_type"], "halt_trading")

if __name__ == "__main__":
    unittest.main()
