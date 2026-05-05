"""
Agent Orchestration Layer
-------------------------

Exposes the Intelligent LangGraph Agent for data pipeline automation.
"""

from data_ingestion.agent.data_ingestion_agent import DataIngestionCoordinator

__all__ = ["DataIngestionCoordinator"]
