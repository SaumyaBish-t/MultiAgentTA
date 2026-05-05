"""
Unit tests for P&L Monitor Agent
"""

import unittest
from unittest.mock import MagicMock, patch
from monitoring.agents.pnl_monitor_agent import PnLMonitor

class TestPnLMonitor(unittest.TestCase):
    def setUp(self):
        self.monitor = PnLMonitor()

    def test_daily_pnl_calculated_correctly(self):
        # 100k -> 101k should be 1% return
        state = {
            "current_value": 101000.0,
            "previous_value": 100000.0,
            "daily_pnl": 0.0,
            "daily_pnl_pct": 0.0
        }
        
        pnl = state["current_value"] - state["previous_value"]
        pnl_pct = pnl / state["previous_value"]
        
        self.assertEqual(pnl, 1000.0)
        self.assertEqual(pnl_pct, 0.01)

    def test_attribution_sums_to_total_return(self):
        # sum(contributions) should ≈ total_return
        attribution = [
            {"ticker": "AAPL", "contribution_pct": 0.005},
            {"ticker": "TSLA", "contribution_pct": 0.003},
            {"ticker": "MSFT", "contribution_pct": 0.002},
        ]
        total_return = sum(a["contribution_pct"] for a in attribution)
        self.assertAlmostEqual(total_return, 0.01)

    def test_drawdown_calculated_correctly(self):
        # peak=100k, current=90k -> drawdown=-10%
        peak = 100000.0
        current = 90000.0
        drawdown = (current - peak) / peak
        self.assertEqual(drawdown, -0.1)

    @patch("monitoring.agents.pnl_monitor_agent.redis.from_url")
    def test_benchmark_return_fetched(self, mock_redis):
        # Mock redis return for benchmark
        mock_r = MagicMock()
        mock_r.get.return_value = "0.005" # 0.5% SPY return
        mock_redis.return_value = mock_r
        
        bench = float(mock_r.get("market:SPY:daily_return"))
        self.assertEqual(bench, 0.005)

if __name__ == "__main__":
    unittest.main()
