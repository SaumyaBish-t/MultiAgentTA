"""
Phase 7 — Pre-Trade Compliance Agent
=====================================
Runs BEFORE any order is submitted to Alpaca.
If this agent rejects — the order never reaches the broker.

Design:
  • Load rules from compliance_rules table
  • Run 10 independent checks per proposed order
  • Record every check in compliance_checks + rule_violations
  • Return a typed ComplianceDecision (never raise)
"""

from __future__ import annotations

import uuid
import json
from dataclasses import dataclass, field
from datetime import datetime, date, timezone, timedelta
from typing import Any, Optional
from uuid import UUID

import redis
from loguru import logger
from sqlalchemy import create_engine, text

from config.settings import settings
from compliance.agents.audit_logger import audit_log

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Data types
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class ComplianceDecision:
    order_id: UUID
    ticker: str
    approved: bool
    rejection_reason: Optional[str]
    violations: list[dict[str, Any]]
    warnings: list[dict[str, Any]]
    checks_run: int
    checked_at: datetime


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Sector lookup (static — keeps the agent self-contained)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_SECTOR_MAP: dict[str, str] = {
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology",
    "AMZN": "Consumer Cyclical", "NVDA": "Technology", "TSLA": "Consumer Cyclical",
    "JPM": "Financial Services", "META": "Technology", "V": "Financial Services",
    "JNJ": "Healthcare", "UNH": "Healthcare", "PG": "Consumer Defensive",
    "XOM": "Energy", "CVX": "Energy",
    "SPY": "ETF", "QQQ": "ETF",
}


def _get_sector(ticker: str) -> str:
    return _SECTOR_MAP.get(ticker, "Unknown")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PreTradeCompliance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PreTradeCompliance:
    """Gate-keeper that evaluates every proposed order against the rule book."""

    def __init__(self) -> None:
        self._engine = create_engine(settings.postgres_url)
        self._redis = redis.from_url(settings.redis_url, decode_responses=True)

    # ── helpers ──────────────────────────────────────────────────
    def _load_rules(self) -> dict[str, dict[str, Any]]:
        """Load enabled compliance rules from DB keyed by rule_id."""
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT rule_id, rule_category, rule_logic, severity, auto_action "
                "FROM compliance_rules WHERE enabled = true"
            )).fetchall()
        return {
            r[0]: {
                "rule_id": r[0], "category": r[1],
                "logic": json.loads(r[2]) if isinstance(r[2], str) else r[2],
                "severity": r[3], "auto_action": r[4],
            }
            for r in rows
        }

    def _get_portfolio_state(self) -> dict[str, Any]:
        """Get current portfolio value, cash, and positions from broker_connections + portfolio_positions."""
        with self._engine.connect() as conn:
            broker = conn.execute(text(
                "SELECT cash_balance, portfolio_value, buying_power "
                "FROM broker_connections WHERE broker_name = 'alpaca' LIMIT 1"
            )).fetchone()
            positions = conn.execute(text(
                "SELECT ticker, current_shares, current_value_usd "
                "FROM portfolio_positions WHERE status = 'active'"
            )).fetchall()

        cash = float(broker[0]) if broker else 0
        portfolio_value = float(broker[1]) if broker else 0
        pos_map: dict[str, dict[str, Any]] = {}
        for p in positions:
            pos_map[p[0]] = {"shares": int(p[1] or 0), "value": float(p[2] or 0)}

        return {
            "cash": cash,
            "portfolio_value": max(portfolio_value, 1),  # avoid div-by-zero
            "positions": pos_map,
        }

    def _record_check(
        self,
        rule_id: str,
        entity_type: str,
        ticker: Optional[str],
        result: str,
        current_value: Optional[float],
        threshold_value: Optional[float],
        details: str,
        auto_action_taken: Optional[str] = None,
        entity_id: Optional[UUID] = None,
    ) -> UUID:
        check_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        with self._engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO compliance_checks
                    (id, rule_id, entity_type, entity_id, ticker,
                     check_result, current_value, threshold_value,
                     details, auto_action_taken, checked_at, created_at)
                VALUES
                    (:id, :rule_id, :etype, :eid, :ticker,
                     :result, :cv, :tv,
                     :details, :aa, :now, :now)
            """), {
                "id": str(check_id), "rule_id": rule_id,
                "etype": entity_type, "eid": str(entity_id) if entity_id else None,
                "ticker": ticker, "result": result,
                "cv": current_value, "tv": threshold_value,
                "details": details, "aa": auto_action_taken, "now": now,
            })
        return check_id

    def _record_violation(
        self,
        check_id: UUID,
        rule_id: str,
        violation_type: str,
        ticker: Optional[str],
        severity: str,
        description: str,
        current_value: float,
        allowed_value: float,
    ) -> None:
        now = datetime.now(timezone.utc)
        excess = abs(current_value - allowed_value)
        with self._engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO rule_violations
                    (id, compliance_check_id, rule_id, violation_type,
                     ticker, severity, description, current_value,
                     allowed_value, excess, status, created_at)
                VALUES
                    (:id, :cid, :rid, :vtype,
                     :ticker, :sev, :desc, :cv,
                     :av, :excess, 'open', :now)
            """), {
                "id": str(uuid.uuid4()), "cid": str(check_id),
                "rid": rule_id, "vtype": violation_type,
                "ticker": ticker, "sev": severity,
                "desc": description, "cv": current_value,
                "av": allowed_value, "excess": excess, "now": now,
            })

    # ── individual rule checks ──────────────────────────────────

    def _check_restricted_list(
        self, ticker: str, action: str, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 1: restricted_list_check"""
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT restriction_type, reason FROM restricted_list "
                "WHERE ticker = :t AND active = true "
                "AND (expires_at IS NULL OR expires_at > :now)"
            ), {"t": ticker, "now": datetime.now(timezone.utc)}).fetchall()

        if not rows:
            return "pass", None, None

        for rtype, reason in rows:
            if rtype == "no_trade":
                return "violation", {
                    "rule_id": "RESTRICTED_LIST_CHECK", "severity": "critical",
                    "description": f"{ticker} is on the no-trade restricted list: {reason}",
                    "current_value": 1, "allowed_value": 0,
                }, None
            if rtype == "no_buy" and action.lower() == "buy":
                return "violation", {
                    "rule_id": "RESTRICTED_LIST_CHECK", "severity": "critical",
                    "description": f"{ticker} is restricted from buying: {reason}",
                    "current_value": 1, "allowed_value": 0,
                }, None
            if rtype == "no_sell" and action.lower() == "sell":
                return "violation", {
                    "rule_id": "RESTRICTED_LIST_CHECK", "severity": "critical",
                    "description": f"{ticker} is restricted from selling: {reason}",
                    "current_value": 1, "allowed_value": 0,
                }, None
        return "pass", None, None

    def _check_max_position_size(
        self, ticker: str, action: str, estimated_value: float,
        portfolio: dict, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 2: max_position_size_check"""
        if action.lower() != "buy":
            return "pass", None, None

        rule = rules.get("MAX_POSITION_SIZE_5PCT")
        if not rule:
            return "pass", None, None

        max_pct = rule["logic"].get("max_pct", 0.05)
        current_pos_value = portfolio["positions"].get(ticker, {}).get("value", 0)
        new_value = current_pos_value + estimated_value
        new_weight = new_value / portfolio["portfolio_value"]

        if new_weight > max_pct:
            return "violation", {
                "rule_id": "MAX_POSITION_SIZE_5PCT", "severity": rule["severity"],
                "description": f"{ticker} would be {new_weight:.1%} of portfolio (limit {max_pct:.0%})",
                "current_value": round(new_weight, 6), "allowed_value": max_pct,
            }, None
        return "pass", None, None

    def _check_no_leverage(
        self, action: str, estimated_value: float, portfolio: dict, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 3: no_leverage_check"""
        if action.lower() != "buy":
            return "pass", None, None

        rule = rules.get("NO_LEVERAGE")
        if not rule:
            return "pass", None, None

        total_invested = sum(p["value"] for p in portfolio["positions"].values())
        total_after = total_invested + estimated_value
        pv = portfolio["portfolio_value"]

        if total_after > pv:
            return "violation", {
                "rule_id": "NO_LEVERAGE", "severity": rule["severity"],
                "description": f"Total invested ${total_after:,.0f} would exceed portfolio ${pv:,.0f}",
                "current_value": round(total_after, 2), "allowed_value": round(pv, 2),
            }, None
        return "pass", None, None

    def _check_min_cash(
        self, action: str, estimated_value: float, portfolio: dict, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 4: min_cash_check (warning only)"""
        if action.lower() != "buy":
            return "pass", None, None

        rule = rules.get("MIN_CASH_5PCT")
        if not rule:
            return "pass", None, None

        min_pct = rule["logic"].get("min_cash_pct", 0.05)
        cash_after = portfolio["cash"] - estimated_value
        cash_pct = cash_after / portfolio["portfolio_value"]

        if cash_pct < min_pct:
            return "warning", None, {
                "rule_id": "MIN_CASH_5PCT", "severity": "warning",
                "description": f"Cash would drop to {cash_pct:.1%} (min {min_pct:.0%})",
                "current_value": round(cash_pct, 6), "allowed_value": min_pct,
            }
        return "pass", None, None

    def _check_pdt(
        self, ticker: str, action: str, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 5: pdt_check"""
        rule = rules.get("PDT_WARNING_3TRADES")
        if not rule:
            return "pass", None, None

        max_trades = rule["logic"].get("max_day_trades", 3)
        window = rule["logic"].get("window_days", 5)
        today = date.today()
        window_start = today - timedelta(days=window)

        with self._engine.connect() as conn:
            row = conn.execute(text(
                "SELECT COUNT(*) FROM pattern_day_trade_tracker "
                "WHERE is_day_trade = true AND trade_date >= :start"
            ), {"start": window_start}).fetchone()

        count = int(row[0]) if row else 0

        # Check if this proposed trade is itself a day trade
        opposite = "sell" if action.lower() == "buy" else "buy"
        with self._engine.connect() as conn:
            same_day = conn.execute(text(
                "SELECT COUNT(*) FROM orders "
                "WHERE ticker = :t AND action = :a AND status = 'filled' "
                "AND DATE(filled_at) = :today"
            ), {"t": ticker, "a": opposite, "today": today}).fetchone()

        is_day_trade = (same_day and int(same_day[0]) > 0)
        effective_count = count + (1 if is_day_trade else 0)

        if effective_count >= max_trades + 1:
            return "violation", {
                "rule_id": "PDT_WARNING_3TRADES", "severity": "violation",
                "description": f"PDT limit exceeded: {effective_count} day trades in {window} days",
                "current_value": effective_count, "allowed_value": max_trades,
            }, None
        if effective_count >= max_trades:
            return "warning", None, {
                "rule_id": "PDT_WARNING_3TRADES", "severity": "warning",
                "description": f"PDT limit approaching: {effective_count}/{max_trades} day trades",
                "current_value": effective_count, "allowed_value": max_trades,
            }
        return "pass", None, None

    def _check_wash_sale(
        self, ticker: str, action: str, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 6: wash_sale_check (warning only)"""
        if action.lower() != "buy":
            return "pass", None, None

        rule = rules.get("WASH_SALE_30DAY")
        if not rule:
            return "pass", None, None

        now = datetime.now(timezone.utc)
        with self._engine.connect() as conn:
            row = conn.execute(text(
                "SELECT loss_amount, wash_sale_window_end FROM wash_sale_tracker "
                "WHERE ticker = :t AND status = 'monitoring' "
                "AND wash_sale_window_end > :now LIMIT 1"
            ), {"t": ticker, "now": now}).fetchone()

        if row:
            return "warning", None, {
                "rule_id": "WASH_SALE_30DAY", "severity": "warning",
                "description": f"Wash sale: buying {ticker} within 30-day window of ${float(row[0]):,.2f} loss sale",
                "current_value": float(row[0]), "allowed_value": 0,
            }
        return "pass", None, None

    def _check_market_hours(
        self, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 7: market_hours_check"""
        try:
            from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter
            adapter = AlpacaBrokerAdapter()
            clock = adapter.get_market_clock()
            if not clock["is_open"]:
                return "violation", {
                    "rule_id": "MARKET_HOURS_CHECK", "severity": "critical",
                    "description": "Market is closed",
                    "current_value": 0, "allowed_value": 1,
                }, None
        except Exception as exc:
            logger.warning("Market hours check failed (allowing): {}", exc)
        return "pass", None, None

    def _check_gross_exposure(
        self, action: str, estimated_value: float, portfolio: dict, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 8: gross_exposure_check"""
        if action.lower() != "buy":
            return "pass", None, None

        rule = rules.get("MAX_GROSS_EXPOSURE_95PCT")
        if not rule:
            return "pass", None, None

        max_exp = rule["logic"].get("max_gross_exposure", 0.95)
        total_long = sum(p["value"] for p in portfolio["positions"].values())
        exposure_after = (total_long + estimated_value) / portfolio["portfolio_value"]

        if exposure_after > max_exp:
            return "violation", {
                "rule_id": "MAX_GROSS_EXPOSURE_95PCT", "severity": rule["severity"],
                "description": f"Gross exposure would be {exposure_after:.1%} (limit {max_exp:.0%})",
                "current_value": round(exposure_after, 6), "allowed_value": max_exp,
            }, None
        return "pass", None, None

    def _check_sector_concentration(
        self, ticker: str, action: str, estimated_value: float,
        portfolio: dict, rules: dict,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 9: sector_concentration_check"""
        if action.lower() != "buy":
            return "pass", None, None

        rule = rules.get("MAX_SECTOR_30PCT")
        if not rule:
            return "pass", None, None

        max_sector = rule["logic"].get("max_sector_pct", 0.30)
        sector = _get_sector(ticker)
        sector_value = sum(
            p["value"] for t, p in portfolio["positions"].items()
            if _get_sector(t) == sector
        )
        sector_after = (sector_value + estimated_value) / portfolio["portfolio_value"]

        if sector_after > 0.35:
            return "violation", {
                "rule_id": "MAX_SECTOR_30PCT", "severity": "violation",
                "description": f"{sector} sector would be {sector_after:.1%} (hard limit 35%)",
                "current_value": round(sector_after, 6), "allowed_value": 0.35,
            }, None
        if sector_after > max_sector:
            return "warning", None, {
                "rule_id": "MAX_SECTOR_30PCT", "severity": "warning",
                "description": f"{sector} sector would be {sector_after:.1%} (soft limit {max_sector:.0%})",
                "current_value": round(sector_after, 6), "allowed_value": max_sector,
            }
        return "pass", None, None

    def _check_duplicate_order(
        self, ticker: str, action: str,
    ) -> tuple[str, Optional[dict], Optional[dict]]:
        """RULE 10: duplicate_order_check"""
        with self._engine.connect() as conn:
            row = conn.execute(text(
                "SELECT COUNT(*) FROM orders "
                "WHERE ticker = :t AND action = :a "
                "AND status IN ('pending', 'submitted', 'partial')"
            ), {"t": ticker, "a": action.lower()}).fetchone()

        if row and int(row[0]) > 0:
            return "violation", {
                "rule_id": "DUPLICATE_ORDER_CHECK", "severity": "violation",
                "description": f"Duplicate {action} order already pending for {ticker}",
                "current_value": int(row[0]), "allowed_value": 0,
            }, None
        return "pass", None, None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  Public API
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def check(self, order: dict[str, Any]) -> ComplianceDecision:
        """
        Run all compliance checks on a proposed order.
        Returns a ComplianceDecision — never raises.
        """
        now = datetime.now(timezone.utc)
        order_id = order.get("id") or order.get("internal_id") or uuid.uuid4()
        ticker = order.get("ticker", "")
        action = order.get("action", "buy")
        shares = int(order.get("shares", order.get("requested_shares", 0)))
        estimated_value = float(order.get("value", order.get("estimated_value", shares * order.get("price", 0))))

        violations: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        all_checks: dict[str, str] = {}

        try:
            rules = self._load_rules()
            portfolio = self._get_portfolio_state()
        except Exception as exc:
            logger.error("Compliance data load failed — REJECTING for safety: {}", exc)
            return ComplianceDecision(
                order_id=order_id, ticker=ticker, approved=False,
                rejection_reason=f"Compliance system error: {exc}",
                violations=[], warnings=[], checks_run=0, checked_at=now,
            )

        # ── run checks ──────────────────────────────────────────
        checks = [
            ("restricted_list",      lambda: self._check_restricted_list(ticker, action, rules)),
            ("max_position_size",    lambda: self._check_max_position_size(ticker, action, estimated_value, portfolio, rules)),
            ("no_leverage",          lambda: self._check_no_leverage(action, estimated_value, portfolio, rules)),
            ("min_cash",             lambda: self._check_min_cash(action, estimated_value, portfolio, rules)),
            ("pdt",                  lambda: self._check_pdt(ticker, action, rules)),
            ("wash_sale",            lambda: self._check_wash_sale(ticker, action, rules)),
            ("market_hours",         lambda: self._check_market_hours(rules)),
            ("gross_exposure",       lambda: self._check_gross_exposure(action, estimated_value, portfolio, rules)),
            ("sector_concentration", lambda: self._check_sector_concentration(ticker, action, estimated_value, portfolio, rules)),
            ("duplicate_order",      lambda: self._check_duplicate_order(ticker, action)),
        ]

        # Map check names → actual rule_ids in compliance_rules table
        _CHECK_RULE_MAP = {
            "restricted_list": "RESTRICTED_LIST_CHECK",
            "max_position_size": "MAX_POSITION_SIZE_5PCT",
            "no_leverage": "NO_LEVERAGE",
            "min_cash": "MIN_CASH_5PCT",
            "pdt": "PDT_WARNING_3TRADES",
            "wash_sale": "WASH_SALE_30DAY",
            "market_hours": "MARKET_HOURS_CHECK",
            "gross_exposure": "MAX_GROSS_EXPOSURE_95PCT",
            "sector_concentration": "MAX_SECTOR_30PCT",
            "duplicate_order": "DUPLICATE_ORDER_CHECK",
        }

        for name, check_fn in checks:
            try:
                result, violation, warning = check_fn()
                all_checks[name] = result

                # Use the rule_id from the violation/warning dict first, then the static map
                rule_id = (violation or warning or {}).get("rule_id", _CHECK_RULE_MAP.get(name, "RESTRICTED_LIST_CHECK"))

                # Only record non-pass checks to DB (avoids FK issues for checks without dedicated rules)
                if result != "pass":
                    check_db_id = self._record_check(
                        rule_id=rule_id, entity_type="order", ticker=ticker,
                        result=result, entity_id=order_id if isinstance(order_id, UUID) else None,
                        current_value=(violation or warning or {}).get("current_value"),
                        threshold_value=(violation or warning or {}).get("allowed_value"),
                        details=(violation or warning or {}).get("description", f"{name}: {result}"),
                        auto_action_taken=rules.get(rule_id, {}).get("auto_action"),
                    )

                    if violation:
                        violations.append(violation)
                        self._record_violation(
                            check_id=check_db_id, rule_id=violation["rule_id"],
                            violation_type=name, ticker=ticker,
                            severity=violation["severity"],
                            description=violation["description"],
                            current_value=violation["current_value"],
                            allowed_value=violation["allowed_value"],
                        )

                    if warning:
                        warnings.append(warning)
                else:
                    if violation:
                        violations.append(violation)
                    if warning:
                        warnings.append(warning)

            except Exception as exc:
                logger.warning("Check '{}' failed for {} — skipping: {}", name, ticker, exc)
                all_checks[name] = "error"

        # ── decision ────────────────────────────────────────────
        approved = True
        rejection_reason: Optional[str] = None

        critical = [v for v in violations if v.get("severity") == "critical"]
        if critical:
            approved = False
            rejection_reason = critical[0]["description"]
        elif violations:
            approved = False
            rejection_reason = violations[0]["description"]

        # ── audit ───────────────────────────────────────────────
        try:
            await audit_log.log(
                event_type="compliance_check",
                entity_type="order",
                entity_id=order_id if isinstance(order_id, UUID) else None,
                ticker=ticker,
                action=f"Pre-trade: {'APPROVED' if approved else 'REJECTED'}",
                actor="pre_trade_compliance",
                details={
                    "checks": all_checks,
                    "violations": violations,
                    "warnings": warnings,
                    "approved": approved,
                    "rejection_reason": rejection_reason,
                },
            )
        except Exception:
            pass  # audit failure must never block the decision

        status = "APPROVED" if approved else "REJECTED"
        logger.info(
            "PRE-TRADE {} | {} {} {} shares ${:,.0f} | checks={} violations={} warnings={}",
            status, action.upper(), ticker, shares, estimated_value,
            len(all_checks), len(violations), len(warnings),
        )

        return ComplianceDecision(
            order_id=order_id, ticker=ticker, approved=approved,
            rejection_reason=rejection_reason,
            violations=violations, warnings=warnings,
            checks_run=len(all_checks), checked_at=now,
        )

    async def check_batch(self, orders: list[dict[str, Any]]) -> list[ComplianceDecision]:
        """Run compliance on a full batch of proposed orders."""
        return [await self.check(o) for o in orders]

    # ── restricted list management ──────────────────────────────

    def add_to_restricted_list(
        self, ticker: str, restriction_type: str, reason: str, added_by: str = "system",
        expires_at: Optional[datetime] = None,
    ) -> bool:
        """Add a ticker to the restricted list."""
        try:
            now = datetime.now(timezone.utc)
            with self._engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO restricted_list
                        (id, ticker, restriction_type, reason, added_by, active, expires_at, created_at)
                    VALUES (:id, :ticker, :rtype, :reason, :by, true, :exp, :now)
                """), {
                    "id": str(uuid.uuid4()), "ticker": ticker.upper(),
                    "rtype": restriction_type, "reason": reason,
                    "by": added_by, "exp": expires_at, "now": now,
                })
            logger.warning("RESTRICTED LIST: Added {} ({}) — {}", ticker, restriction_type, reason)
            return True
        except Exception as exc:
            logger.error("Failed to add {} to restricted list: {}", ticker, exc)
            return False

    def remove_from_restricted_list(self, ticker: str) -> bool:
        """Deactivate all restrictions for a ticker."""
        try:
            now = datetime.now(timezone.utc)
            with self._engine.begin() as conn:
                conn.execute(text(
                    "UPDATE restricted_list SET active = false, removed_at = :now "
                    "WHERE ticker = :t AND active = true"
                ), {"t": ticker.upper(), "now": now})
            logger.info("RESTRICTED LIST: Removed {}", ticker)
            return True
        except Exception as exc:
            logger.error("Failed to remove {} from restricted list: {}", ticker, exc)
            return False

    def get_violations_today(self) -> list[dict[str, Any]]:
        """Return all violations recorded today."""
        today = date.today()
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT rule_id, ticker, severity, description, current_value, allowed_value, excess, status "
                "FROM rule_violations WHERE DATE(created_at) = :today ORDER BY created_at DESC"
            ), {"today": today}).fetchall()
        return [
            {"rule_id": r[0], "ticker": r[1], "severity": r[2], "description": r[3],
             "current_value": r[4], "allowed_value": r[5], "excess": r[6], "status": r[7]}
            for r in rows
        ]
