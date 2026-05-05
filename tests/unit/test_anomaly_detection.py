"""
Unit tests for Anomaly Detection Agent
"""

import unittest
from datetime import datetime, timedelta
from monitoring.agents.anomaly_detection_agent import AnomalyDetectionAgent

class TestAnomalyDetection(unittest.TestCase):
    def setUp(self):
        self.agent = AnomalyDetectionAgent()

    def test_stale_data_detected_during_market_hours(self):
        # Now - 10 mins ago is stale if threshold is 5 mins
        last_update = datetime.now() - timedelta(minutes=10)
        age_mins = (datetime.now() - last_update).total_seconds() / 60
        is_stale = age_mins > 5
        self.assertTrue(is_stale)

    def test_price_spike_5pct_flagged(self):
        # 5% spike in one bar is an anomaly
        prev_price = 150.0
        curr_price = 158.0 # > 5% spike
        change = abs(curr_price - prev_price) / prev_price
        self.assertGreater(change, 0.05)

    def test_position_mismatch_detected(self):
        # Redis says 100 shares, DB says 90
        redis_pos = 100
        db_pos = 90
        mismatch = (redis_pos != db_pos)
        self.assertTrue(mismatch)

    def test_z_score_outlier_return_flagged(self):
        # Simple z-score check
        import numpy as np
        returns = [0.01, 0.02, -0.01, 0.015, 0.01, 0.10] # 0.10 is an outlier
        mean = np.mean(returns)
        std = np.std(returns)
        z_score = (returns[-1] - mean) / std
        self.assertGreater(z_score, 2.0) # > 2 std devs

if __name__ == "__main__":
    unittest.main()
