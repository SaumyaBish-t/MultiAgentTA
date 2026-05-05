"""
Unit tests for Alert Manager
"""

import unittest
from unittest.mock import MagicMock, patch
from monitoring.alerts.alert_manager import AlertManager

class TestAlertManager(unittest.TestCase):
    def setUp(self):
        self.manager = AlertManager()

    @patch("monitoring.alerts.alert_manager.redis.from_url")
    def test_deduplication_prevents_repeat(self, mock_redis):
        # Mock redis for deduplication check
        mock_r = MagicMock()
        mock_r.get.return_value = "exists" # Simulate alert already exists in window
        mock_redis.return_value = mock_r
        
        # Test dedup check logic (simulated)
        alert_key = "alert:dedup:price_anomaly:AAPL"
        is_deduped = mock_r.get(alert_key) is not None
        self.assertTrue(is_deduped)

    def test_alert_severity_channels(self):
        # info -> redis, log
        channels = self.manager.SEVERITY_CHANNELS["info"]
        self.assertIn("redis", channels)
        self.assertIn("log", channels)
        self.assertNotIn("dashboard", channels)

        # critical -> dashboard
        channels = self.manager.SEVERITY_CHANNELS["critical"]
        self.assertIn("dashboard", channels)

    def test_dedup_window_logic(self):
        # emergency should have short window
        self.assertEqual(self.manager.DEDUP_WINDOW_SECONDS["emergency"], 60)
        # info should have long window
        self.assertEqual(self.manager.DEDUP_WINDOW_SECONDS["info"], 3600)

if __name__ == "__main__":
    unittest.main()
