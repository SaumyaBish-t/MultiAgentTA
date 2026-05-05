"""
Phase 7 — Immutable Audit Logger
=================================
Every system action flows through this agent. The audit log is:
  • APPEND-ONLY — records are never updated or deleted
  • HASH-CHAINED — each record's SHA-256 includes the previous hash
  • ASYNC-SAFE — never blocks the main pipeline
  • FAIL-SAFE — falls back to file logging if DB is unreachable
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import UUID

import redis
from loguru import logger
from sqlalchemy import create_engine, text, select

from config.settings import settings

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GENESIS_HASH = "GENESIS"
FALLBACK_LOG = Path("logs/audit_fallback.jsonl")

VALID_EVENT_TYPES = {
    "order_submitted", "order_filled", "order_cancelled",
    "position_opened", "position_closed",
    "signal_approved", "signal_rejected",
    "risk_breach", "circuit_breaker", "rebalance",
    "hypothesis_generated", "backtest_completed",
    "parameter_changed", "system_startup", "system_shutdown",
    "compliance_check", "rule_violation", "alert_sent",
    "human_override", "emergency_action",
}

VALID_ENTITY_TYPES = {
    "order", "position", "signal", "portfolio", "risk", "system",
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  AuditLogger
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class AuditLogger:
    """Immutable, hash-chained audit trail for the entire trading system."""

    def __init__(self) -> None:
        self._engine = create_engine(settings.postgres_url)
        self._redis = redis.from_url(settings.redis_url, decode_responses=True)
        self._lock = threading.Lock()
        self._last_hash: str = self._get_last_hash()
        FALLBACK_LOG.parent.mkdir(parents=True, exist_ok=True)
        logger.info("AuditLogger initialised  |  chain tip = {}...", self._last_hash[:12])

    # ── internal helpers ────────────────────────────────────────
    def _get_last_hash(self) -> str:
        """Fetch the most recent hash from the DB to continue the chain."""
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("SELECT immutable_hash FROM audit_log ORDER BY created_at DESC LIMIT 1")
                ).fetchone()
                return row[0] if row else GENESIS_HASH
        except Exception:
            logger.warning("Could not read last audit hash — starting from GENESIS")
            return GENESIS_HASH

    @staticmethod
    def _compute_hash(record: dict[str, Any], previous_hash: str) -> str:
        payload = {
            "id": str(record["id"]),
            "event_type": record["event_type"],
            "entity_type": record["entity_type"],
            "entity_id": str(record["entity_id"]) if record["entity_id"] else None,
            "ticker": record["ticker"],
            "action": record["action"],
            "actor": record["actor"],
            "details": record["details"],
            "previous_state": record["previous_state"],
            "new_state": record["new_state"],
            "created_at": str(record["created_at"]),
            "previous_hash": previous_hash,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest()

    def _write_to_db(self, record: dict[str, Any]) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO audit_log
                            (id, event_type, entity_type, entity_id, ticker,
                             action, actor, details, previous_state, new_state,
                             ip_address, session_id, immutable_hash, created_at)
                        VALUES
                            (:id, :event_type, :entity_type, :entity_id, :ticker,
                             :action, :actor, :details, :previous_state, :new_state,
                             :ip_address, :session_id, :immutable_hash, :created_at)
                    """),
                    {
                        "id": str(record["id"]),
                        "event_type": record["event_type"],
                        "entity_type": record["entity_type"],
                        "entity_id": str(record["entity_id"]) if record["entity_id"] else None,
                        "ticker": record["ticker"],
                        "action": record["action"],
                        "actor": record["actor"],
                        "details": json.dumps(record["details"], default=str),
                        "previous_state": json.dumps(record["previous_state"], default=str) if record["previous_state"] else None,
                        "new_state": json.dumps(record["new_state"], default=str) if record["new_state"] else None,
                        "ip_address": record.get("ip_address"),
                        "session_id": record.get("session_id"),
                        "immutable_hash": record["immutable_hash"],
                        "created_at": record["created_at"],
                    },
                )
        except Exception as exc:
            logger.error("DB write failed for audit record {} — falling back to file: {}", record["id"], exc)
            self._write_fallback(record)

    def _write_fallback(self, record: dict[str, Any]) -> None:
        try:
            with open(FALLBACK_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except Exception as exc:
            logger.critical("AUDIT FALLBACK WRITE FAILED: {}", exc)

    def _publish_to_redis(self, record: dict[str, Any]) -> None:
        try:
            self._redis.xadd(
                "audit:stream",
                {
                    "event_type": record["event_type"],
                    "ticker": record["ticker"] or "",
                    "action": record["action"],
                    "actor": record["actor"],
                    "hash": record["immutable_hash"],
                    "timestamp": record["created_at"].isoformat(),
                },
                maxlen=10000,
            )
        except Exception as exc:
            logger.warning("Redis audit stream publish failed: {}", exc)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 2 — Core log method
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    async def log(
        self,
        event_type: str,
        entity_type: str,
        action: str,
        actor: str,
        details: dict[str, Any],
        entity_id: Optional[UUID] = None,
        ticker: Optional[str] = None,
        previous_state: Optional[dict[str, Any]] = None,
        new_state: Optional[dict[str, Any]] = None,
    ) -> str:
        """Append an immutable, hash-chained audit event. Returns the record hash."""
        now = datetime.now(timezone.utc)
        record: dict[str, Any] = {
            "id": uuid.uuid4(),
            "event_type": event_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "ticker": ticker,
            "action": action,
            "actor": actor,
            "details": details,
            "previous_state": previous_state,
            "new_state": new_state,
            "created_at": now,
        }

        with self._lock:
            record_hash = self._compute_hash(record, self._last_hash)
            record["immutable_hash"] = record_hash
            self._last_hash = record_hash

        # Non-blocking DB + Redis writes
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write_to_db, record)
        await loop.run_in_executor(None, self._publish_to_redis, record)

        logger.info("AUDIT | {} | {} | {} | {}", event_type, actor, action, ticker or "N/A")
        return record_hash

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 3 — Convenience methods
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    async def log_order_submitted(self, order: dict[str, Any]) -> str:
        return await self.log(
            event_type="order_submitted",
            entity_type="order",
            entity_id=order.get("id"),
            ticker=order.get("ticker"),
            action=f"{order.get('action', 'buy')} {order.get('requested_shares', 0)} shares",
            actor="smart_order_router",
            details=order,
        )

    async def log_order_filled(self, order: dict[str, Any]) -> str:
        price = order.get("filled_avg_price", 0)
        return await self.log(
            event_type="order_filled",
            entity_type="order",
            entity_id=order.get("id"),
            ticker=order.get("ticker"),
            action=f"Filled {order.get('filled_shares', 0)} @ ${price:.2f}",
            actor="execution_monitor",
            details=order,
            previous_state={"status": "submitted"},
            new_state={"status": "filled", "filled_price": price},
        )

    async def log_order_cancelled(self, order: dict[str, Any], reason: str = "") -> str:
        return await self.log(
            event_type="order_cancelled",
            entity_type="order",
            entity_id=order.get("id"),
            ticker=order.get("ticker"),
            action=f"Cancelled: {reason}" if reason else "Order cancelled",
            actor="execution_monitor",
            details={**order, "cancel_reason": reason},
        )

    async def log_position_opened(self, position: dict[str, Any]) -> str:
        return await self.log(
            event_type="position_opened",
            entity_type="position",
            entity_id=position.get("id"),
            ticker=position.get("ticker"),
            action=f"Opened {position.get('shares', 0)} shares @ ${position.get('entry_price', 0):.2f}",
            actor="execution_pipeline",
            details=position,
            new_state={"status": "active"},
        )

    async def log_position_closed(self, position: dict[str, Any], pnl: float = 0) -> str:
        return await self.log(
            event_type="position_closed",
            entity_type="position",
            entity_id=position.get("id"),
            ticker=position.get("ticker"),
            action=f"Closed position  PnL=${pnl:+,.2f}",
            actor="execution_pipeline",
            details={**position, "realised_pnl": pnl},
            previous_state={"status": "active"},
            new_state={"status": "closed"},
        )

    async def log_signal_approved(self, signal: dict[str, Any], decision: dict[str, Any]) -> str:
        return await self.log(
            event_type="signal_approved",
            entity_type="signal",
            entity_id=signal.get("id"),
            ticker=signal.get("ticker"),
            action=f"Signal approved: {decision.get('risk_score', 0):.2f} risk score",
            actor="risk_gate",
            details={**signal, **decision},
        )

    async def log_signal_rejected(self, signal: dict[str, Any], reasons: list[str]) -> str:
        return await self.log(
            event_type="signal_rejected",
            entity_type="signal",
            entity_id=signal.get("id"),
            ticker=signal.get("ticker"),
            action=f"Signal rejected: {', '.join(reasons)}",
            actor="risk_gate",
            details={"signal": signal, "reasons": reasons},
        )

    async def log_risk_breach(
        self, breach_type: str, value: float, threshold: float, action_taken: str
    ) -> str:
        return await self.log(
            event_type="risk_breach",
            entity_type="risk",
            action=f"{breach_type}: {value:.4f} vs limit {threshold:.4f}",
            actor="risk_management",
            details={
                "breach_type": breach_type,
                "current_value": value,
                "threshold": threshold,
                "action_taken": action_taken,
            },
        )

    async def log_circuit_breaker(self, breaker_type: str, action: str) -> str:
        return await self.log(
            event_type="circuit_breaker",
            entity_type="risk",
            action=f"Circuit breaker triggered: {breaker_type}",
            actor="drawdown_monitor",
            details={"breaker": breaker_type, "action": action},
            new_state={"trading_halted": True},
        )

    async def log_rebalance(self, rebalance: dict[str, Any]) -> str:
        return await self.log(
            event_type="rebalance",
            entity_type="portfolio",
            action=f"Rebalance: {rebalance.get('trade_count', 0)} trades",
            actor="portfolio_optimizer",
            details=rebalance,
        )

    async def log_hypothesis_generated(self, hypothesis: dict[str, Any]) -> str:
        direction = hypothesis.get("expected_direction", "?")
        conviction = hypothesis.get("conviction_score", 0)
        return await self.log(
            event_type="hypothesis_generated",
            entity_type="signal",
            ticker=hypothesis.get("ticker"),
            action=f"Hypothesis: {direction}  conviction:{conviction:.2f}",
            actor="hypothesis_agent",
            details=hypothesis,
        )

    async def log_compliance_check(self, check: dict[str, Any]) -> str:
        return await self.log(
            event_type="compliance_check",
            entity_type="portfolio",
            ticker=check.get("ticker"),
            action=f"Rule {check.get('rule_id', '?')}: {check.get('check_result', '?')}",
            actor="compliance_engine",
            details=check,
        )

    async def log_rule_violation(self, violation: dict[str, Any]) -> str:
        return await self.log(
            event_type="rule_violation",
            entity_type="portfolio",
            ticker=violation.get("ticker"),
            action=f"VIOLATION {violation.get('rule_id', '?')}: excess {violation.get('excess', 0):.4f}",
            actor="compliance_engine",
            details=violation,
        )

    async def log_human_override(
        self, what: str, who: str, reason: str,
        before: dict[str, Any], after: dict[str, Any],
    ) -> str:
        return await self.log(
            event_type="human_override",
            entity_type="system",
            action=f"Human override: {what}",
            actor=who,
            details={"reason": reason},
            previous_state=before,
            new_state=after,
        )

    async def log_parameter_changed(
        self, param_name: str, old_value: Any, new_value: Any, changed_by: str,
    ) -> str:
        return await self.log(
            event_type="parameter_changed",
            entity_type="system",
            action=f"Parameter {param_name} changed",
            actor=changed_by,
            details={"parameter": param_name},
            previous_state={"value": old_value},
            new_state={"value": new_value},
        )

    async def log_emergency_action(self, action_desc: str, details: dict[str, Any]) -> str:
        return await self.log(
            event_type="emergency_action",
            entity_type="risk",
            action=action_desc,
            actor="emergency_handler",
            details=details,
            new_state={"emergency": True},
        )

    async def log_system_startup(self) -> str:
        return await self.log(
            event_type="system_startup",
            entity_type="system",
            action="Trading system started",
            actor="system",
            details={"tickers": settings.tickers, "version": "1.0.0", "mode": "paper"},
        )

    async def log_system_shutdown(self) -> str:
        return await self.log(
            event_type="system_shutdown",
            entity_type="system",
            action="Trading system stopped",
            actor="system",
            details={"timestamp": datetime.now(timezone.utc).isoformat()},
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 4 — Verification
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def verify_chain_integrity(self) -> bool:
        """Re-compute every hash and verify the chain is unbroken."""
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT id, event_type, entity_type, entity_id, ticker,
                               action, actor, details, previous_state, new_state,
                               immutable_hash, created_at
                        FROM audit_log ORDER BY created_at ASC
                    """)
                ).fetchall()
        except Exception as exc:
            logger.error("Chain verification DB read failed: {}", exc)
            return False

        if not rows:
            logger.info("Audit chain empty — nothing to verify")
            return True

        prev_hash = GENESIS_HASH
        for row in rows:
            record = {
                "id": row[0], "event_type": row[1], "entity_type": row[2],
                "entity_id": row[3], "ticker": row[4], "action": row[5],
                "actor": row[6], "details": row[7], "previous_state": row[8],
                "new_state": row[9], "created_at": row[11],
            }
            expected = self._compute_hash(record, prev_hash)
            if expected != row[10]:
                logger.critical("AUDIT CHAIN BREACH at record {}", row[0])
                return False
            prev_hash = row[10]

        logger.info("Audit chain verified: {} records intact", len(rows))
        return True

    def get_entity_history(self, entity_id: UUID) -> list[dict[str, Any]]:
        """Full audit trail for a single entity."""
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM audit_log WHERE entity_id = :eid ORDER BY created_at ASC"),
                {"eid": str(entity_id)},
            ).fetchall()
        return [dict(r._mapping) for r in rows]

    def get_events_by_type(
        self, event_type: str, start: datetime, end: datetime,
    ) -> list[dict[str, Any]]:
        """Query audit log by event type within a time window."""
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT * FROM audit_log
                    WHERE event_type = :et AND created_at BETWEEN :s AND :e
                    ORDER BY created_at ASC
                """),
                {"et": event_type, "s": start, "e": end},
            ).fetchall()
        return [dict(r._mapping) for r in rows]

    def get_recent_events(self, limit: int = 50) -> list[dict[str, Any]]:
        """Fetch the N most recent audit entries."""
        with self._engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM audit_log ORDER BY created_at DESC LIMIT :lim"),
                {"lim": limit},
            ).fetchall()
        return [dict(r._mapping) for r in rows]

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 5 — Redis event auto-logger
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    _CHANNEL_MAP: dict[str, tuple[str, str, str]] = {
        # channel_pattern → (event_type, entity_type, actor)
        "signals.pipeline.completed":          ("compliance_check",       "signal",    "signal_pipeline"),
        "risk.signal.approved":                ("signal_approved",        "signal",    "risk_gate"),
        "risk.signal.rejected":                ("signal_rejected",        "signal",    "risk_gate"),
        "risk.circuit_breaker.emergency":      ("circuit_breaker",        "risk",      "drawdown_monitor"),
        "risk.circuit_breaker.reduce":         ("circuit_breaker",        "risk",      "drawdown_monitor"),
        "execution.orders.submitted":          ("order_submitted",        "order",     "smart_order_router"),
        "execution.batch.completed":           ("order_filled",           "order",     "execution_monitor"),
        "portfolio.allocation.final":          ("rebalance",              "portfolio", "portfolio_optimizer"),
        "research.hypothesis.high_conviction": ("hypothesis_generated",   "signal",    "hypothesis_agent"),
        "execution.pipeline.completed":        ("compliance_check",       "portfolio", "execution_pipeline"),
    }

    async def _auto_log_event(self, channel: str, data: dict[str, Any]) -> None:
        """Route a Redis pub/sub message to the correct audit entry."""
        mapping = self._CHANNEL_MAP.get(channel)
        if not mapping:
            # Still log unknown channels at debug level
            event_type, entity_type, actor = "compliance_check", "system", "redis_listener"
        else:
            event_type, entity_type, actor = mapping

        await self.log(
            event_type=event_type,
            entity_type=entity_type,
            ticker=data.get("ticker"),
            action=f"Auto-logged from {channel}",
            actor=actor,
            details=data,
        )

    async def listen_and_log(self) -> None:
        """Subscribe to ALL Redis channels and auto-log every event."""
        pubsub = self._redis.pubsub()
        pubsub.psubscribe("*")
        logger.info("AuditLogger Redis listener started — subscribed to *")

        while True:
            try:
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message["type"] == "pmessage":
                    channel = message["channel"]
                    try:
                        data = json.loads(message["data"])
                    except (json.JSONDecodeError, TypeError):
                        data = {"raw": str(message["data"])}
                    await self._auto_log_event(channel, data)
                else:
                    await asyncio.sleep(0.1)
            except Exception as exc:
                logger.warning("Audit listener error (will retry): {}", exc)
                await asyncio.sleep(2)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 6 — Singleton
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
audit_log = AuditLogger()
