"""
meta_analysis.pipeline.meta_pipeline
====================================

Daily Prefect flow that runs all L11 analyses and caches a summary
to Redis as ``meta:calibration_summary``.
"""

from __future__ import annotations

import json

import redis
from loguru import logger
from prefect import flow, task

from config.settings import settings
from meta_analysis.agents.agent_calibration import run as run_calibration
from meta_analysis.agents.human_override_analyzer import run as run_override


@task
def _calibration() -> dict:
    return run_calibration()


@task
def _override() -> dict:
    return run_override()


@flow(name="meta-analysis-pipeline")
def meta_pipeline() -> dict:
    if not settings.meta_analysis_enabled:
        return {"skipped": "feature flag off"}

    calib = _calibration()
    override = _override()
    summary = {"calibration": calib, "human_override": override}
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=3)
        r.setex("meta:calibration_summary", 24 * 3600, json.dumps(summary))
    except Exception as exc:
        logger.warning("meta cache write failed: {}", exc)
    return summary


if __name__ == "__main__":
    print(meta_pipeline())
