"""
Phase 7 — Phase 6 Event Listener
==================================
Subscribes to Redis Pub/Sub events from the execution and risk
pipelines, routes them to the compliance engine for audit
logging, post-fill compliance, and end-of-day triggers.

Channels:
  execution.orders.submitted   → audit log batch submission
  execution.batch.completed    → post-fill compliance (wash-sale, PDT)
  execution.pipeline.completed → trigger EOD compliance if after close
  risk.circuit_breaker.*       → immediate audit log
  risk.signal.approved         → audit log
  risk.signal.rejected         → audit log
  portfolio.allocation.final   → audit log allocation decision
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import redis
from loguru import logger

from config.settings import settings


async def _handle_message(channel: str, data: dict) -> None:
    """Route a single message to the appropriate compliance handler."""

    # Lazy imports to avoid circular dependencies at module load
    from compliance.agents.audit_logger import audit_log
    from compliance.pipeline.compliance_pipeline import CompliancePipeline

    pipeline = CompliancePipeline()

    try:
        # ── Execution Events ────────────────────────────────────
        if channel == "execution.orders.submitted":
            batch_id = data.get("batch_id", "unknown")
            count = data.get("order_count", len(data.get("orders", [])))
            await audit_log.log(
                event_type="order_submitted",
                entity_type="batch",
                action=f"Batch {batch_id}: {count} orders submitted",
                actor="execution_pipeline",
                details=data,
            )

        elif channel == "execution.batch.completed":
            fills = data.get("filled_orders", data.get("fills", []))
            logger.info("Phase 6 batch completed — processing {} fills for compliance", len(fills))
            for fill in fills:
                try:
                    await pipeline.post_fill_compliance(fill)
                except Exception as exc:
                    logger.error("Post-fill compliance error for {}: {}", fill.get("ticker"), exc)

        elif channel == "execution.pipeline.completed":
            await audit_log.log(
                event_type="compliance_check",
                entity_type="pipeline",
                action="Execution pipeline completed",
                actor="execution_pipeline",
                details=data,
            )
            # Trigger EOD compliance if after 20:00 UTC
            now = datetime.now(timezone.utc)
            if now.hour >= 20:
                logger.info("Post-close execution detected — triggering daily compliance")
                asyncio.create_task(_run_daily_compliance_safe())

        # ── Risk Events ─────────────────────────────────────────
        elif channel.startswith("risk.circuit_breaker"):
            breaker_type = data.get("type", channel.split(".")[-1])
            action = data.get("action", "triggered")
            await audit_log.log_circuit_breaker(breaker_type, action)
            logger.critical("CIRCUIT BREAKER logged: {} — {}", breaker_type, action)

        elif channel == "risk.signal.approved":
            signal = data.get("signal", data)
            decision = data.get("decision", {})
            await audit_log.log_signal_approved(signal, decision)

        elif channel == "risk.signal.rejected":
            signal = data.get("signal", data)
            reasons = data.get("reasons", [data.get("reason", "unknown")])
            await audit_log.log_signal_rejected(signal, reasons)

        # ── Portfolio Events ────────────────────────────────────
        elif channel == "portfolio.allocation.final":
            await audit_log.log_rebalance(data)

        else:
            # Log any unknown channel for visibility
            await audit_log.log(
                event_type="compliance_check",
                entity_type="event",
                action=f"Event received on {channel}",
                actor="phase6_listener",
                details=data,
            )

    except Exception as exc:
        logger.error("Compliance listener error on {}: {}", channel, exc)


async def _run_daily_compliance_safe() -> None:
    """Run daily compliance with error isolation."""
    try:
        from compliance.pipeline.compliance_pipeline import CompliancePipeline
        pipeline = CompliancePipeline()
        await pipeline.run_daily_compliance()
    except Exception as exc:
        logger.error("Daily compliance auto-trigger failed: {}", exc)


async def start_phase6_listener() -> None:
    """
    Main listener loop — subscribes to execution, risk, and
    portfolio Redis channels and routes to compliance handlers.

    Also starts the AuditLogger's automatic listen_and_log()
    in the background to capture ALL system events.
    """
    r = redis.from_url(settings.redis_url, decode_responses=True)
    pubsub = r.pubsub()

    channels = [
        "execution.orders.submitted",
        "execution.batch.completed",
        "execution.pipeline.completed",
        "risk.circuit_breaker.emergency",
        "risk.circuit_breaker.reduce",
        "risk.signal.approved",
        "risk.signal.rejected",
        "portfolio.allocation.final",
    ]

    pubsub.subscribe(*channels)
    logger.info("Phase 6 → Phase 7 listener active on {} channels", len(channels))

    # Start audit auto-logger in background
    try:
        from compliance.agents.audit_logger import audit_log
        asyncio.create_task(_start_audit_auto_logger(audit_log))
    except Exception as exc:
        logger.warning("Audit auto-logger start failed: {}", exc)

    try:
        while True:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("type") == "message":
                channel = message["channel"]
                try:
                    data = json.loads(message["data"])
                except (json.JSONDecodeError, TypeError):
                    data = {"raw": message["data"]}

                logger.debug("Phase 7 received: {} | keys={}", channel, list(data.keys())[:5])
                asyncio.create_task(_handle_message(channel, data))

            await asyncio.sleep(0.05)

    except asyncio.CancelledError:
        logger.info("Phase 6 listener cancelled — shutting down")
        pubsub.unsubscribe()
    except Exception as exc:
        logger.error("Phase 6 listener crashed: {} — restarting in 5s", exc)
        await asyncio.sleep(5)
        await start_phase6_listener()


async def _start_audit_auto_logger(audit_log) -> None:
    """Start audit_log.listen_and_log() if available."""
    try:
        if hasattr(audit_log, "listen_and_log"):
            await audit_log.listen_and_log()
    except Exception as exc:
        logger.warning("Audit auto-logger error: {}", exc)


if __name__ == "__main__":
    asyncio.run(start_phase6_listener())
