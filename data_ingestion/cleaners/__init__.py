"""
Cleaners sub-package — data validation and anomaly detection.

Public API
----------
::

    from data_ingestion.cleaners import DataQualityAgent
"""

from data_ingestion.cleaners.data_quality_agent import DataQualityAgent

__all__ = ["DataQualityAgent"]
