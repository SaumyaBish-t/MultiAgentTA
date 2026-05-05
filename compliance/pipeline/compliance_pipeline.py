"""
Phase 7 — Compliance Pipeline
===============================
Central orchestrator that wires together every compliance agent.
Provides the unified interface for pre-trade checks, post-fill
compliance, daily compliance runs, and status queries.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, date, timezone
from typing import Any, Optional
from uuid import UUID

import redis
from loguru import logger
from sqlalchemy import create_engine, text

from config.settings import settings
from compliance.agents.audit_logger import audit_log, AuditLogger
from compliance.agents.pre_trade_compliance import PreTradeCompliance
from compliance.agents.position_limit_agent import PositionLimitAgent
from compliance.agents.wash_sale_pdt_tracker import wash_sale_tracker, pdt_tracker, WashSaleTracker, PatternDayTradeTracker
from compliance.agents.report_generator import ReportGenerator


class CompliancePipeline:
    """
    Central compliance orchestrator.

    Responsibilities:
      1. Pre-trade gating (called by SmartOrderRouter)
      2. Post-fill compliance (wash-sale, PDT tracking)
      3. Daily compliance sweeps (position limits, reports, audit chain)
      4. Status & audit queries
    """

    def __init__(self) -> None:
        self.audit = audit_log
        self.pre_trade = PreTradeCompliance()
        self.position_limits = PositionLimitAgent()
        self.wash_sale: WashSaleTracker = wash_sale_tracker
        self.pdt: PatternDayTradeTracker = pdt_tracker
        self.reporter = ReportGenerator()
        self._engine = create_engine(settings.postgres_url)
        self._redis = redis.from_url(settings.redis_url, decode_responses=True)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  1. Daily Compliance Run
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def run_daily_compliance(self) -> dict[str, Any]:
        """
        Full end-of-day compliance sweep.
        Checks positions, expires wash-sale windows, generates
        reports, verifies audit chain, and publishes completion.
        """
        results: dict[str, Any] = {"date": str(date.today())}

        # 1. Position limit checks
        try:
            limits_result = await self.position_limits.check()
            results["position_limits"] = {
                "status": limits_result.overall_status,
                "breaches": len(limits_result.breaches),
                "warnings": len(limits_result.warnings),
                "hhi": limits_result.hhi,
            }
        except Exception as exc:
            logger.error("Position limit check failed: {}", exc)
            results["position_limits"] = {"status": "ERROR", "error": str(exc)}

        # 2. Expire wash-sale windows
        try:
            expired = self.wash_sale.expire_old_windows()
            results["wash_sales_expired"] = expired
        except Exception as exc:
            logger.error("Wash sale expiry failed: {}", exc)
            results["wash_sales_expired"] = 0

        # 3. Generate daily reports (P&L, compliance, execution)
        try:
            pnl = await self.reporter.generate_daily_pnl()
            results["report_pnl"] = {
                "portfolio_value": pnl.get("portfolio_value"),
                "daily_pnl": pnl.get("daily_pnl"),
            }
        except Exception as exc:
            logger.error("Daily P&L report failed: {}", exc)
            results["report_pnl"] = {"error": str(exc)}

        try:
            comp = await self.reporter.generate_compliance()
            results["report_compliance"] = {
                "checks_run": comp.get("checks_run"),
                "violation_count": comp.get("violation_count"),
            }
        except Exception as exc:
            logger.error("Compliance report failed: {}", exc)
            results["report_compliance"] = {"error": str(exc)}

        try:
            exe = await self.reporter.generate_execution()
            results["report_execution"] = {
                "total_orders": exe.get("total_orders"),
                "fill_rate": exe.get("fill_rate"),
            }
        except Exception as exc:
            logger.error("Execution report failed: {}", exc)
            results["report_execution"] = {"error": str(exc)}

        # 4. Verify audit chain integrity
        try:
            chain_ok = self.audit.verify_chain_integrity()
            results["audit_chain_ok"] = chain_ok
            if not chain_ok:
                await self.audit.log(
                    event_type="compliance_check",
                    entity_type="system",
                    action="CRITICAL: Audit chain integrity FAILED",
                    actor="compliance_pipeline",
                    details={"chain_ok": False},
                )
                logger.critical("AUDIT CHAIN INTEGRITY FAILED — investigate immediately")
        except Exception as exc:
            logger.error("Audit chain verification failed: {}", exc)
            results["audit_chain_ok"] = False

        # 5. Log pipeline completion
        try:
            await self.audit.log(
                event_type="compliance_check",
                entity_type="system",
                action="Daily compliance run completed",
                actor="compliance_pipeline",
                details=results,
            )
        except Exception:
            pass

        # 6. Publish completion
        try:
            breach_count = results.get("position_limits", {}).get("breaches", 0)
            self._redis.publish("compliance.daily.completed", json.dumps({
                "date": str(date.today()),
                "violations": breach_count,
                "reports_generated": 3,
                "chain_ok": results.get("audit_chain_ok", False),
            }))
        except Exception:
            pass

        logger.info(
            "COMPLIANCE PIPELINE | Daily run complete | Breaches: {} | Chain: {}",
            results.get("position_limits", {}).get("breaches", "?"),
            results.get("audit_chain_ok", "?"),
        )
        return results

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  2. Pre-Trade Check
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def run_pre_trade_check(self, order: dict[str, Any]) -> bool:
        """Called by SmartOrderRouter before every order submission."""
        decision = await self.pre_trade.check(order)
        return decision.approved

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  3. Post-Fill Compliance
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def post_fill_compliance(self, fill: dict[str, Any]) -> None:
        """
        Called after every order fill.
        • Audit logs the fill
        • Tracks wash-sale windows for loss sales
        • Records replacement purchases
        • Tracks PDT day trades
        """
        ticker = fill.get("ticker", "")
        action = fill.get("action", "")
        order_id = fill.get("id")
        if isinstance(order_id, str):
            try:
                order_id = UUID(order_id)
            except ValueError:
                order_id = None

        # Audit log the fill
        try:
            await self.audit.log_order_filled(fill)
        except Exception:
            pass

        # Wash-sale tracking
        if action.lower() == "sell":
            cost_basis = self._get_cost_basis(ticker)
            filled_price = float(fill.get("filled_avg_price", 0))
            filled_shares = int(fill.get("filled_shares", 0))
            if cost_basis > 0 and filled_price > 0:
                await self.wash_sale.record_sale(
                    ticker=ticker,
                    shares=filled_shares,
                    price=filled_price,
                    cost_basis=cost_basis,
                    order_id=order_id,
                )
        elif action.lower() == "buy":
            await self.wash_sale.record_replacement_purchase(ticker, order_id)

        # PDT tracking
        await self.pdt.record_trade(ticker, action, order_id)

    def _get_cost_basis(self, ticker: str) -> float:
        """Fetch entry price from portfolio_positions for wash-sale calculation."""
        try:
            with self._engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT entry_price FROM portfolio_positions "
                    "WHERE ticker = :t AND status = 'active' LIMIT 1"
                ), {"t": ticker}).fetchone()
            return float(row[0]) if row else 0
        except Exception:
            return 0

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  4. Status & Queries
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_compliance_status(self) -> dict[str, Any]:
        """Quick snapshot of overall compliance health."""
        # Position status from Redis cache
        pos_status = self._redis.get("compliance:position:status")
        pos = json.loads(pos_status) if pos_status else {"overall_status": "UNKNOWN"}

        # PDT
        pdt_report = self.pdt.get_pdt_report()

        # Wash sale
        active_windows = self.wash_sale.get_active_windows()

        # Buy blocked?
        buy_blocked = self.position_limits.is_new_buy_blocked()

        # Today's violations
        today = date.today()
        with self._engine.connect() as conn:
            viols = conn.execute(text(
                "SELECT COUNT(*) FROM rule_violations WHERE DATE(created_at) = :d"
            ), {"d": today}).fetchone()

        return {
            "position_status": pos.get("overall_status", "UNKNOWN"),
            "violations_today": int(viols[0]) if viols else 0,
            "pdt": pdt_report,
            "wash_sale_windows": len(active_windows),
            "new_buys_blocked": buy_blocked,
            "audit_chain_tip": self.audit._last_hash[:12] if self.audit._last_hash else "N/A",
        }

    def get_violations_open(self) -> list[dict[str, Any]]:
        """All unresolved violations."""
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT rule_id, ticker, severity, description, current_value, "
                "allowed_value, excess, status, created_at "
                "FROM rule_violations WHERE status = 'open' "
                "ORDER BY created_at DESC LIMIT 50"
            )).fetchall()
        return [
            {
                "rule_id": r[0], "ticker": r[1], "severity": r[2],
                "description": str(r[3])[:100], "current": r[4],
                "allowed": r[5], "excess": r[6], "status": r[7],
                "created_at": r[8].isoformat() if r[8] else None,
            }
            for r in rows
        ]

    def get_audit_trail(self, entity_id: Optional[str] = None) -> list[dict[str, Any]]:
        """Fetch audit log entries, optionally filtered by entity_id."""
        if entity_id:
            rows_sql = (
                "SELECT event_type, entity_type, ticker, action, actor, created_at "
                "FROM audit_log WHERE entity_id = :eid ORDER BY created_at DESC LIMIT 50"
            )
            with self._engine.connect() as conn:
                rows = conn.execute(text(rows_sql), {"eid": entity_id}).fetchall()
        else:
            rows_sql = (
                "SELECT event_type, entity_type, ticker, action, actor, created_at "
                "FROM audit_log ORDER BY created_at DESC LIMIT 50"
            )
            with self._engine.connect() as conn:
                rows = conn.execute(text(rows_sql)).fetchall()

        return [
            {
                "event_type": r[0], "entity_type": r[1], "ticker": r[2],
                "action": r[3], "actor": r[4],
                "created_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ]
