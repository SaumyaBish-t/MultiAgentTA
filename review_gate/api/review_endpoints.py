"""
review_gate.api.review_endpoints
================================

FastAPI router for the L5 Review Gate. Mount with:

    from review_gate.api.review_endpoints import router as review_router
    app.include_router(review_router, prefix="/review", tags=["review"])

Endpoints
---------
GET  /review/pending      — pending reviews for the dashboard
POST /review/decide       — submit human decision
GET  /review/stream       — SSE for new review notifications
GET  /review/{review_id}  — single review detail
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import psycopg2
import psycopg2.extras
import redis.asyncio as aredis
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel

from config.settings import settings
from core.channel_router import Channels
from review_gate.l5_review_agent import apply_decision


router = APIRouter()


class DecideRequest(BaseModel):
    review_id: str
    decision: str  # approved | reduced | rejected
    notes: str = ""
    final_position_usd: float | None = None


def _conn():
    return psycopg2.connect(settings.postgres_url, connect_timeout=5)


@router.get("/pending")
def list_pending(limit: int = 25) -> list[dict[str, Any]]:
    try:
        conn = _conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT review_id, ticker, direction, strategy_type,
                       recommendation, recommendation_confidence,
                       headline, key_concern, key_support,
                       proposed_position_usd, signal_valid_hours,
                       vault_note_path, created_at
                  FROM trade_reviews
                 WHERE status = 'pending'
                 ORDER BY created_at DESC
                 LIMIT %s
                """,
                (limit,),
            )
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as exc:
        logger.warning("/review/pending failed: {}", exc)
        raise HTTPException(status_code=503, detail=str(exc))


@router.get("/{review_id}")
def get_review(review_id: str) -> dict[str, Any]:
    try:
        conn = _conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM trade_reviews WHERE review_id = %s",
                (review_id,),
            )
            row = cur.fetchone()
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    if not row:
        raise HTTPException(status_code=404, detail="review not found")
    return dict(row)


@router.post("/decide")
def decide(req: DecideRequest) -> dict[str, Any]:
    if req.decision not in {"approved", "reduced", "rejected"}:
        raise HTTPException(status_code=400, detail="invalid decision")
    return apply_decision(
        req.review_id, req.decision, req.notes, req.final_position_usd,
    )


@router.get("/stream")
async def stream():
    async def event_stream():
        try:
            r = aredis.from_url(settings.redis_url)
            pubsub = r.pubsub()
            await pubsub.subscribe(Channels.REVIEW_PENDING)
            while True:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=15)
                if msg is None:
                    yield ": keepalive\n\n"
                    continue
                data = msg.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8", errors="replace")
                yield f"data: {data}\n\n"
                await asyncio.sleep(0)
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
