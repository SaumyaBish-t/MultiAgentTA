"""
meta_analysis.agents.human_override_analyzer
============================================

The crown jewel of L11: is your review gate adding alpha or hurting?

Method
------
1. Pull every ``trade_reviews`` row that has been decided.
2. For each, compare:
     - actual outcome (if the trade was approved/reduced)
     - hypothetical outcome (if rejected — we use the price path of the
       ticker over the same horizon the trade would have held).
3. Bucket by agreement (operator agreed with AI vs disagreed) and
   compute net P/L impact.

Result is appended to ``human_override_tracking`` and cached to Redis
as ``meta:human_override_analysis``.
"""

from __future__ import annotations

import json

import psycopg2
import redis
from loguru import logger

from config.settings import settings


def _fetch_decided_reviews() -> list[dict]:
    sql = """
        SELECT review_id, ticker, recommendation, human_decision,
               proposed_position_usd, final_position_usd,
               decided_at
          FROM trade_reviews
         WHERE status = 'decided'
           AND human_decision IS NOT NULL
    """
    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            out = [dict(zip(cols, row)) for row in cur.fetchall()]
        conn.close()
        return out
    except Exception as exc:
        logger.warning("human_override_analyzer fetch failed: {}", exc)
        return []


def _outcome_for_ticker(ticker: str, decided_at) -> float | None:
    """Approximate the 5-day forward return after ``decided_at``."""
    try:
        import yfinance as yf  # type: ignore[import-untyped]
        hist = yf.Ticker(ticker).history(period="14d", interval="1d")
        if hist.empty:
            return None
        # Find first index on/after the decision date
        try:
            forward = hist.loc[hist.index >= decided_at]
        except Exception:
            forward = hist
        if len(forward) < 5:
            return None
        start = forward["Close"].iloc[0]
        end = forward["Close"].iloc[4]
        if start == 0:
            return None
        return float((end - start) / start * 100.0)
    except Exception as exc:
        logger.debug("outcome fetch failed for {}: {}", ticker, exc)
        return None


def run() -> dict:
    if not settings.meta_analysis_enabled:
        return {"skipped": "feature flag off"}

    rows = _fetch_decided_reviews()
    if len(rows) < 5:
        result = {"insufficient_data": True, "decided_reviews": len(rows)}
        _cache(result)
        return result

    agreed_outcomes: list[float] = []
    disagreed_outcomes: list[float] = []
    persisted = 0

    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        cur = conn.cursor()
    except Exception:
        conn = None
        cur = None

    for r in rows:
        outcome = _outcome_for_ticker(r["ticker"], r["decided_at"])
        if outcome is None:
            continue
        agreed = (r["recommendation"] == "APPROVE" and r["human_decision"] == "approved") or \
                 (r["recommendation"] == "REJECT" and r["human_decision"] == "rejected")
        signed_outcome = outcome if r["human_decision"] in ("approved", "reduced") else -outcome
        if agreed:
            agreed_outcomes.append(signed_outcome)
        else:
            disagreed_outcomes.append(signed_outcome)

        if cur is not None:
            try:
                cur.execute(
                    """
                    INSERT INTO human_override_tracking
                        (review_id, ticker, ai_recommendation, human_decision,
                         agreed, outcome_pct, outcome_recorded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (r["review_id"], r["ticker"], r["recommendation"],
                     r["human_decision"], agreed, outcome),
                )
                persisted += 1
            except Exception as exc:
                logger.debug("override insert skipped: {}", exc)

    if conn is not None:
        try:
            conn.commit()
        finally:
            conn.close()

    result = {
        "n_agreed": len(agreed_outcomes),
        "n_disagreed": len(disagreed_outcomes),
        "avg_outcome_when_agreed": _mean(agreed_outcomes),
        "avg_outcome_when_disagreed": _mean(disagreed_outcomes),
        "net_override_value":
            _mean(disagreed_outcomes) - _mean(agreed_outcomes)
            if disagreed_outcomes and agreed_outcomes else None,
        "rows_persisted": persisted,
    }
    _cache(result)
    return result


def _mean(xs: list[float]) -> float | None:
    return round(sum(xs) / len(xs), 4) if xs else None


def _cache(result: dict) -> None:
    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        r.setex("meta:human_override_analysis", 24 * 3600, json.dumps(result))
    except Exception as exc:
        logger.debug("override analyzer cache failed: {}", exc)
