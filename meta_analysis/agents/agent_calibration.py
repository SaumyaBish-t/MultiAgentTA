"""
meta_analysis.agents.agent_calibration
======================================

When an agent says "0.8 confidence", does that trade work 80% of the time?
Buckets agent confidence scores by decile and compares to the realised
outcome of the resulting trades. Writes one row per
``(agent, date, bucket)`` to the ``agent_calibration`` table.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable

import psycopg2
from loguru import logger

from config.settings import settings


BUCKETS: list[float] = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]


def _bucket_of(confidence: float) -> float:
    for b in BUCKETS:
        if confidence <= b:
            return b
    return 1.0


def _fetch_agent_predictions() -> list[tuple[str, float, float, str]]:
    """Pull (agent_name, confidence, outcome_pct, regime) tuples.

    This relies on the existing audit_log + executions + regime_detections
    tables. The query is intentionally permissive — missing joins yield
    NULLs that we skip, so an empty system returns an empty list.
    """
    sql = """
        SELECT al.agent_name,
               (al.payload->>'confidence')::float                          AS confidence,
               COALESCE((e.payload->>'outcome_pct')::float, 0)             AS outcome_pct,
               COALESCE(rd.regime, 'unknown')                              AS regime
          FROM audit_log al
          LEFT JOIN executions e
            ON e.signal_id::text = al.payload->>'signal_id'
          LEFT JOIN regime_detections rd
            ON rd.detected_at::date = al.created_at::date
         WHERE al.payload ? 'confidence'
           AND al.created_at > NOW() - INTERVAL '180 days'
    """
    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        conn.close()
        return rows  # type: ignore[return-value]
    except Exception as exc:
        logger.warning("agent_calibration fetch failed: {}", exc)
        return []


def compute_calibration(rows: Iterable[tuple[str, float, float, str]]) -> list[dict]:
    """Group rows by (agent, bucket, regime) and compute hit-rate accuracy."""
    bucketed: dict[tuple[str, float, str], list[float]] = {}
    for agent, conf, outcome, regime in rows:
        if conf is None:
            continue
        key = (agent, _bucket_of(float(conf)), regime)
        bucketed.setdefault(key, []).append(float(outcome))

    results = []
    for (agent, bucket, regime), outcomes in bucketed.items():
        if not outcomes:
            continue
        wins = sum(1 for o in outcomes if o > 0)
        accuracy = wins / len(outcomes)
        results.append({
            "agent_name": agent,
            "confidence_bucket": bucket,
            "regime": regime,
            "actual_accuracy": accuracy,
            "sample_count": len(outcomes),
        })
    return results


def persist(results: list[dict]) -> int:
    if not results:
        return 0
    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        with conn.cursor() as cur:
            for r in results:
                cur.execute(
                    """
                    INSERT INTO agent_calibration
                        (agent_name, measurement_date, confidence_bucket,
                         actual_accuracy, sample_count, regime)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (agent_name, measurement_date, confidence_bucket)
                    DO UPDATE SET actual_accuracy = EXCLUDED.actual_accuracy,
                                  sample_count = EXCLUDED.sample_count,
                                  regime = EXCLUDED.regime
                    """,
                    (r["agent_name"], date.today(), r["confidence_bucket"],
                     r["actual_accuracy"], r["sample_count"], r["regime"]),
                )
        conn.commit()
        conn.close()
        return len(results)
    except Exception as exc:
        logger.warning("agent_calibration write failed: {}", exc)
        return 0


def run() -> dict:
    rows = _fetch_agent_predictions()
    results = compute_calibration(rows)
    persisted = persist(results)
    return {"buckets": persisted, "samples_examined": len(rows)}
