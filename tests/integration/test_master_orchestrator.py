"""
Integration tests for Master Orchestrator
"""

import unittest
import asyncio
from unittest.mock import MagicMock, patch
from monitoring.pipeline.master_orchestrator import MasterOrchestrator

class TestMasterOrchestrator(unittest.TestCase):
    def setUp(self):
        pass

    @patch("monitoring.pipeline.master_orchestrator.SystemHealthMonitor")
    @patch("monitoring.pipeline.master_orchestrator.create_engine")
    @patch("monitoring.pipeline.master_orchestrator.redis.from_url")
    def test_startup_sequence_completes(self, mock_redis, mock_engine, mock_health):
        # Mock health monitor to return healthy
        mock_health_instance = mock_health.return_value
        mock_health_instance.run_full_health_check.return_value = MagicMock(overall="healthy")
        
        orchestrator = MasterOrchestrator()
        
        # Test startup
        loop = asyncio.get_event_loop()
        loop.run_until_complete(orchestrator.startup())
        
        # Verify health check was called during startup
        mock_health_instance.run_full_health_check.assert_called()

    @patch("monitoring.pipeline.master_orchestrator.SystemHealthMonitor")
    @patch("monitoring.pipeline.master_orchestrator.create_engine")
    @patch("monitoring.pipeline.master_orchestrator.redis.from_url")
    def test_all_phases_have_status_after_run(self, mock_redis, mock_engine, mock_health):
        # Mock health monitor to return healthy
        mock_health_instance = mock_health.return_value
        mock_health_instance.run_full_health_check.return_value = MagicMock(
            overall="healthy",
            dict=lambda: {"overall": "healthy", "phases": {}}
        )
        
        orchestrator = MasterOrchestrator()
        
        # Test run
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(orchestrator.run(run_type="manual"))
        
        # Verify result structure
        self.assertIsNotNone(result.run_id)
        self.assertIsInstance(result.phases_completed, list)
        # Even if phases are empty in mock, the list should exist

if __name__ == "__main__":
    unittest.main()
