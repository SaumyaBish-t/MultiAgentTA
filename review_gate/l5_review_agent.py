"""
review_gate.l5_review_agent
===========================

LangGraph agent that orchestrates the Human Review Gate.

Graph
-----
::

    run_all_checks ──► synthesize_recommendation ──► write_to_vault ──►
    notify_user ──► [INTERRUPT] human_decision ──► process_decision

LangGraph's checkpointer persists state on the interrupt so the graph
can be resumed when the operator submits a decision through the API.

If ``langgraph`` isn't installed, the module exposes a ``run_review``
function that performs the checks + scoring synchronously and returns
the recommendation — useful for tests and for the API route to call
even before the full graph is wired up.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

import psycopg2
from loguru import logger

from config.settings import settings
from review_gate.brief_generator import brief_body, score_and_recommend
from review_gate.models import ReviewRecommendation
from review_gate.notifications import notify_pending, notify_decision
from review_gate.tools import (
    memory_lookup,
    news_calendar,
    options_sentiment,
    price_checker,
)
from review_gate.vault_writer import write_trade_brief, move_brief_to


def _persist(record: dict[str, Any]) -> None:
    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trade_reviews
                    (review_id, signal_id, ticker, direction, strategy_type,
                     recommendation, recommendation_confidence,
                     headline, key_concern, key_support,
                     price_check_json, news_check_json,
                     options_check_json, memory_check_json,
                     proposed_position_usd, signal_valid_hours,
                     vault_note_path, thread_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (review_id) DO NOTHING
                """,
                (
                    record["review_id"], record.get("signal_id"),
                    record["ticker"], record["direction"], record["strategy_type"],
                    record["recommendation"], record["recommendation_confidence"],
                    record["headline"], record["key_concern"], record["key_support"],
                    json.dumps(record["price_check"]),
                    json.dumps(record["news_check"]),
                    json.dumps(record["options_check"]),
                    json.dumps(record["memory_check"]),
                    record.get("proposed_position_usd"),
                    record.get("signal_valid_hours"),
                    record.get("vault_note_path", ""),
                    record.get("thread_id", ""),
                ),
            )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("trade_reviews insert failed: {}", exc)


def run_review(signal: dict[str, Any]) -> dict[str, Any]:
    """Run all 4 checks, score, persist the review, notify the dashboard.

    Returns the full review record. The actual human decision step is
    handled separately by the API route (``POST /review/decide``) so
    callers can plug this into either a LangGraph interrupt or a
    plain HTTP flow.

    Required ``signal`` keys:
        ticker, direction (long|short), strategy_type, price_at_signal,
        proposed_position_usd, signal_valid_hours.
    Optional:
        signal_id.
    """
    if not settings.review_gate_enabled:
        return {"skipped": "review gate disabled"}

    review_id = "rv_" + uuid.uuid4().hex[:14]
    thread_id = signal.get("thread_id") or uuid.uuid4().hex

    price = price_checker.check(
        signal["ticker"], signal["price_at_signal"], signal["strategy_type"],
    )
    news = news_calendar.check(signal["ticker"])
    options = options_sentiment.check(signal["ticker"], signal["direction"])
    memory = memory_lookup.check(signal["ticker"], signal["strategy_type"])

    rec: ReviewRecommendation = score_and_recommend(
        ticker=signal["ticker"],
        direction=signal["direction"],
        strategy_type=signal["strategy_type"],
        price=price, news=news, options=options, memory=memory,
    )

    body = brief_body(rec, price, news, options, memory)
    record_for_vault = {
        "review_id": review_id,
        "ticker": signal["ticker"],
        "direction": signal["direction"],
        "strategy_type": signal["strategy_type"],
        "recommendation_confidence": rec.confidence,
    }
    vault_path = write_trade_brief(record_for_vault, body=body) or ""

    record = {
        "review_id": review_id,
        "signal_id": signal.get("signal_id"),
        "ticker": signal["ticker"],
        "direction": signal["direction"],
        "strategy_type": signal["strategy_type"],
        "recommendation": rec.recommendation,
        "recommendation_confidence": rec.confidence,
        "headline": rec.headline,
        "key_concern": rec.key_concern,
        "key_support": rec.key_support,
        "price_check": price.model_dump(),
        "news_check": news.model_dump(),
        "options_check": options.model_dump(),
        "memory_check": memory.model_dump(),
        "proposed_position_usd": signal.get("proposed_position_usd"),
        "signal_valid_hours": signal.get("signal_valid_hours", 4),
        "vault_note_path": vault_path,
        "thread_id": thread_id,
        "created_at": datetime.utcnow().isoformat(),
    }
    _persist(record)
    notify_pending(review_id, {
        "ticker": signal["ticker"],
        "recommendation": rec.recommendation,
        "headline": rec.headline,
        "confidence": rec.confidence,
    })
    return record


def apply_decision(review_id: str, decision: str, notes: str = "",
                   final_position_usd: float | None = None) -> dict:
    """Persist the operator's decision and route the signal."""
    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE trade_reviews
                   SET human_decision = %s,
                       human_notes = %s,
                       final_position_usd = %s,
                       status = %s,
                       decided_at = NOW()
                 WHERE review_id = %s
                 RETURNING ticker, vault_note_path
                """,
                (decision, notes, final_position_usd,
                 "decided" if decision != "pending" else "pending",
                 review_id),
            )
            row = cur.fetchone()
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("apply_decision DB write failed: {}", exc)
        row = None

    ticker = row[0] if row else None
    vault_path = row[1] if row else ""

    if decision == "approved":
        move_brief_to("approved", vault_path)
    elif decision == "rejected":
        move_brief_to("rejected", vault_path)

    notify_decision(review_id, decision, {"ticker": ticker, "notes": notes})
    return {"review_id": review_id, "decision": decision, "ticker": ticker}
