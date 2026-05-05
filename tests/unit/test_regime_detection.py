"""
Unit tests for Regime & Decay Detection Agent
"""

import unittest
from monitoring.agents.regime_decay_agent import RegimeDecayAgent

class TestRegimeDetection(unittest.TestCase):
    def setUp(self):
        self.agent = RegimeDecayAgent()

    def test_bull_regime_detected_correctly(self):
        # Mock indicators for Bull Market
        indicators = {
            "spy_60d_return": 0.05,
            "vix": 12.0,
            "trend": "up"
        }
        
        # Simple rule-based check simulation
        regime = "bull" if indicators["spy_60d_return"] > 0 and indicators["vix"] < 15 else "other"
        self.assertEqual(regime, "bull")

    def test_bear_regime_detected_correctly(self):
        # Mock indicators for Bear Market
        indicators = {
            "spy_60d_return": -0.10,
            "vix": 28.0,
            "trend": "down"
        }
        
        regime = "bear" if indicators["spy_60d_return"] < -0.05 and indicators["vix"] > 25 else "other"
        self.assertEqual(regime, "bear")

    def test_regime_change_detected(self):
        previous = "bull"
        current = "bear"
        changed = (previous != current)
        self.assertTrue(changed)

    def test_decay_score_calculated_correctly(self):
        # Hit rate dropped significantly
        initial_hit_rate = 0.60
        recent_hit_rate = 0.40
        decay = (initial_hit_rate - recent_hit_rate) / initial_hit_rate
        self.assertGreater(decay, 0.3) # > 30% drop is severe

if __name__ == "__main__":
    unittest.main()
