"""
review_gate.notifications
=========================

Push review events onto Redis so the dashboard SSE endpoint can stream
them to the operator in real time.
"""

from __future__ import annotations

import json

import redis
from loguru import logger

from config.settings import settings
from core.channel_router import Channels


def _redis() -> redis.Redis:
    return redis.from_url(settings.redis_url, socket_connect_timeout=3)


def notify_pending(review_id: str, payload: dict) -> None:
    try:
        r = _redis()
        r.rpush("review:pending", json.dumps({"review_id": review_id, **payload}))
        r.publish(Channels.REVIEW_PENDING, json.dumps({"review_id": review_id, **payload}))
    except Exception as exc:
        logger.warning("notify_pending failed: {}", exc)


def notify_decision(review_id: str, decision: str, payload: dict) -> None:
    try:
        r = _redis()
        channel = (
            Channels.SIGNALS_REVIEW_APPROVED if decision in ("approved", "reduced")
            else Channels.SIGNALS_REVIEW_REJECTED
        )
        r.publish(channel, json.dumps({"review_id": review_id, "decision": decision, **payload}))
    except Exception as exc:
        logger.warning("notify_decision failed: {}", exc)
