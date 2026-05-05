"""
Phase 7 — Position Limit Agent
================================
Continuously monitors ALL positions against defined limits.
Runs every 5 minutes during market hours + once at EOD.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, TypedDict

import redis
from loguru import logger
from sqlalchemy import create_engine, text

from config.settings import settings
from compliance.agents.audit_logger import audit_log

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Sector map (same as pre_trade_compliance)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_SECTOR_MAP: dict[str, str] = {
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology",
    "AMZN": "Consumer Cyclical", "NVDA": "Technology", "TSLA": "Consumer Cyclical",
    "JPM": "Financial Services", "META": "Technology", "V": "Financial Services",
    "JNJ": "Healthcare", "UNH": "Healthcare", "PG": "Consumer Defensive",
    "XOM": "Energy", "CVX": "Energy", "SPY": "ETF", "QQQ": "ETF",
}

def _get_sector(ticker: str) -> str:
    return _SECTOR_MAP.get(ticker, "Unknown")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State & result types
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PositionLimitState(TypedDict):
    positions: list[dict[str, Any]]
    portfolio_value: float
    position_weights: dict[str, float]
    sector_exposures: dict[str, dict[str, Any]]
    limit_checks: list[dict[str, Any]]
    breaches: list[dict[str, Any]]
    warnings: list[dict[str, Any]]
    actions_required: list[dict[str, Any]]
    overall_status: str
    error: Optional[str]


@dataclass
class PositionLimitResult:
    breaches: list[dict[str, Any]]
    warnings: list[dict[str, Any]]
    actions_taken: list[dict[str, Any]]
    overall_status: str
    portfolio_value: float
    position_count: int
    hhi: float
    checked_at: datetime


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Graph Nodes (standalone functions)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def fetch_current_positions_node(state: PositionLimitState) -> dict[str, Any]:
    """Node 1: Fetch live positions and recalculate weights."""
    engine = create_engine(settings.postgres_url)
    r = redis.from_url(settings.redis_url, decode_responses=True)

    # Try Redis cache first
    cached = r.get("portfolio:current:state")
    positions_raw: list[dict[str, Any]] = []

    if cached:
        try:
            data = json.loads(cached)
            positions_raw = data.get("positions", [])
        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback / supplement from DB
    if not positions_raw:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT ticker, current_shares, current_value_usd, entry_price "
                "FROM portfolio_positions WHERE status = 'active'"
            )).fetchall()
        positions_raw = [
            {"ticker": r[0], "shares": int(r[1] or 0),
             "value": float(r[2] or 0), "entry_price": float(r[3] or 0)}
            for r in rows
        ]

    # Get portfolio value
    with engine.connect() as conn:
        broker = conn.execute(text(
            "SELECT portfolio_value, cash_balance FROM broker_connections "
            "WHERE broker_name = 'alpaca' LIMIT 1"
        )).fetchone()

    pv = float(broker[0]) if broker else 1
    pv = max(pv, 1)  # avoid div-by-zero

    # Calculate weights
    weights: dict[str, float] = {}
    for p in positions_raw:
        w = p["value"] / pv if pv > 0 else 0
        weights[p["ticker"]] = round(w, 6)

    return {
        "positions": positions_raw,
        "portfolio_value": pv,
        "position_weights": weights,
    }


def check_individual_position_limits_node(state: PositionLimitState) -> dict[str, Any]:
    """Node 2: Check each position against size limits."""
    if state.get("error"):
        return {}

    breaches = list(state.get("breaches", []))
    warnings = list(state.get("warnings", []))
    actions = list(state.get("actions_required", []))
    checks = list(state.get("limit_checks", []))
    pv = state["portfolio_value"]

    for pos in state["positions"]:
        ticker = pos["ticker"]
        weight = state["position_weights"].get(ticker, 0)
        value = pos.get("value", 0)

        # Hard cap 10%
        if weight > 0.10:
            breach = {
                "ticker": ticker, "rule": "MAX_POSITION_HARD_CAP_10PCT",
                "current": round(weight, 4), "limit": 0.10,
                "excess_pct": round((weight - 0.10) * 100, 2),
                "severity": "critical",
            }
            breaches.append(breach)
            actions.append({
                "action": "REDUCE_IMMEDIATELY", "ticker": ticker,
                "current_weight": weight, "target_weight": 0.05,
            })
        # Soft cap 5%
        elif weight > 0.05:
            breaches.append({
                "ticker": ticker, "rule": "MAX_POSITION_5PCT",
                "current": round(weight, 4), "limit": 0.05,
                "excess_pct": round((weight - 0.05) * 100, 2),
                "severity": "violation",
            })

        # Too small
        if weight < 0.005 and value < 500 and value > 0:
            warnings.append({
                "ticker": ticker, "rule": "POSITION_BELOW_MINIMUM",
                "current_weight": round(weight, 4), "value": value,
                "action_required": "CONSIDER_CLOSING",
            })

        checks.append({"ticker": ticker, "weight": round(weight, 4), "value": value})

    return {
        "breaches": breaches, "warnings": warnings,
        "actions_required": actions, "limit_checks": checks,
    }


def check_sector_limits_node(state: PositionLimitState) -> dict[str, Any]:
    """Node 3: Aggregate by sector, check concentration limits."""
    if state.get("error"):
        return {}

    breaches = list(state.get("breaches", []))
    warnings = list(state.get("warnings", []))
    pv = state["portfolio_value"]

    sectors: dict[str, dict[str, Any]] = {}
    for pos in state["positions"]:
        sector = _get_sector(pos["ticker"])
        if sector not in sectors:
            sectors[sector] = {"value": 0, "tickers": []}
        sectors[sector]["value"] += pos.get("value", 0)
        sectors[sector]["tickers"].append(pos["ticker"])

    for sector, data in sectors.items():
        weight = data["value"] / pv if pv > 0 else 0
        data["weight"] = round(weight, 4)

        if weight > 0.30:
            breaches.append({
                "sector": sector, "rule": "MAX_SECTOR_30PCT",
                "current": round(weight, 4), "limit": 0.30,
                "excess": round(weight - 0.30, 4),
                "severity": "violation",
                "tickers_in_sector": data["tickers"],
            })
        elif weight > 0.25:
            warnings.append({
                "sector": sector, "rule": "SECTOR_APPROACHING_LIMIT",
                "current": round(weight, 4), "limit": 0.30,
            })

    return {"sector_exposures": sectors, "breaches": breaches, "warnings": warnings}


def check_leverage_limits_node(state: PositionLimitState) -> dict[str, Any]:
    """Node 4: Check gross/net exposure and leverage."""
    if state.get("error"):
        return {}

    breaches = list(state.get("breaches", []))
    actions = list(state.get("actions_required", []))
    pv = state["portfolio_value"]

    total_long = sum(p.get("value", 0) for p in state["positions"])
    gross_exposure = total_long / pv if pv > 0 else 0
    net_exposure = gross_exposure  # no shorts in our system

    if net_exposure > 1.0:
        breaches.append({
            "rule": "LEVERAGE_DETECTED", "current": round(net_exposure, 4),
            "limit": 1.0, "severity": "critical",
        })
        actions.append({"action": "EMERGENCY_REDUCE", "current_exposure": net_exposure})
    elif gross_exposure > 0.95:
        breaches.append({
            "rule": "GROSS_EXPOSURE_EXCEEDED", "current": round(gross_exposure, 4),
            "limit": 0.95, "severity": "violation",
        })
        actions.append({"action": "BLOCK_NEW_BUYS"})

    return {"breaches": breaches, "actions_required": actions}


def check_concentration_score_node(state: PositionLimitState) -> dict[str, Any]:
    """Node 5: Herfindahl-Hirschman Index for portfolio concentration."""
    if state.get("error"):
        return {}

    warnings = list(state.get("warnings", []))
    weights = state.get("position_weights", {})

    if not weights:
        return {"warnings": warnings}

    hhi = sum(w ** 2 for w in weights.values())
    equiv_positions = round(1 / hhi, 1) if hhi > 0 else 0

    if hhi > 0.25:
        warnings.append({
            "rule": "HIGH_CONCENTRATION_HHI",
            "hhi": round(hhi, 4),
            "equivalent_positions": equiv_positions,
            "severity": "warning",
        })

    return {"warnings": warnings}


def execute_required_actions_node(state: PositionLimitState) -> dict[str, Any]:
    """Node 6: Publish Redis events for critical breaches."""
    if state.get("error"):
        return {}

    r = redis.from_url(settings.redis_url, decode_responses=True)
    actions = state.get("actions_required", [])

    for act in actions:
        action_type = act.get("action", "")
        try:
            if action_type == "REDUCE_IMMEDIATELY":
                r.publish("compliance.position.reduce_required", json.dumps({
                    "ticker": act["ticker"],
                    "current_weight": act["current_weight"],
                    "target_weight": act["target_weight"],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }))
                logger.warning("COMPLIANCE ACTION: Reduce {} from {:.1%} to {:.1%}",
                               act["ticker"], act["current_weight"], act["target_weight"])

            elif action_type == "EMERGENCY_REDUCE":
                r.publish("compliance.emergency.leverage", json.dumps({
                    "current_exposure": act.get("current_exposure", 0),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }))
                logger.critical("COMPLIANCE ACTION: Emergency leverage reduction triggered")

            elif action_type == "BLOCK_NEW_BUYS":
                r.set("compliance:block_new_buys", "True", ex=3600)
                logger.warning("COMPLIANCE ACTION: New buys blocked for 1 hour")
        except Exception as exc:
            logger.error("Failed to execute compliance action {}: {}", action_type, exc)

    return {}


async def store_and_audit_node(state: PositionLimitState) -> dict[str, Any]:
    """Node 7: Persist checks/violations to DB and audit log."""
    if state.get("error"):
        return {}

    engine = create_engine(settings.postgres_url)
    r = redis.from_url(settings.redis_url, decode_responses=True)
    now = datetime.now(timezone.utc)

    breaches = state.get("breaches", [])
    warnings = state.get("warnings", [])

    # Determine overall status
    critical = any(b.get("severity") == "critical" for b in breaches)
    has_violations = len(breaches) > 0
    if critical:
        status = "CRITICAL"
    elif has_violations:
        status = "VIOLATION"
    elif warnings:
        status = "WARNING"
    else:
        status = "COMPLIANT"

    # Write breaches to DB
    for breach in breaches:
        rule_id = breach.get("rule", "MAX_POSITION_SIZE_5PCT")
        # Map to actual compliance_rules.rule_id
        db_rule_id = {
            "MAX_POSITION_5PCT": "MAX_POSITION_SIZE_5PCT",
            "MAX_POSITION_HARD_CAP_10PCT": "MAX_POSITION_SIZE_5PCT",
            "MAX_SECTOR_30PCT": "MAX_SECTOR_30PCT",
            "GROSS_EXPOSURE_EXCEEDED": "MAX_GROSS_EXPOSURE_95PCT",
            "LEVERAGE_DETECTED": "NO_LEVERAGE",
        }.get(rule_id, "MAX_POSITION_SIZE_5PCT")

        try:
            check_id = uuid.uuid4()
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO compliance_checks
                        (id, rule_id, entity_type, entity_id, ticker,
                         check_result, current_value, threshold_value,
                         details, auto_action_taken, checked_at, created_at)
                    VALUES (:id, :rid, 'portfolio', NULL, :ticker,
                            'violation', :cv, :tv, :det, :aa, :now, :now)
                """), {
                    "id": str(check_id), "rid": db_rule_id,
                    "ticker": breach.get("ticker"),
                    "cv": breach.get("current"), "tv": breach.get("limit"),
                    "det": json.dumps(breach, default=str),
                    "aa": breach.get("action_required"),
                    "now": now,
                })
                conn.execute(text("""
                    INSERT INTO rule_violations
                        (id, compliance_check_id, rule_id, violation_type,
                         ticker, severity, description, current_value,
                         allowed_value, excess, status, created_at)
                    VALUES (:id, :cid, :rid, :vtype,
                            :ticker, :sev, :desc, :cv, :av, :excess, 'open', :now)
                """), {
                    "id": str(uuid.uuid4()), "cid": str(check_id),
                    "rid": db_rule_id, "vtype": "position_limit",
                    "ticker": breach.get("ticker") or breach.get("sector"),
                    "sev": breach.get("severity", "violation"),
                    "desc": json.dumps(breach, default=str),
                    "cv": float(breach.get("current", 0)),
                    "av": float(breach.get("limit", 0)),
                    "excess": float(breach.get("excess_pct", breach.get("excess", 0))),
                    "now": now,
                })
        except Exception as exc:
            logger.error("Failed to persist breach to DB: {}", exc)

    # Audit log breaches
    for breach in breaches:
        try:
            await audit_log.log_risk_breach(
                breach_type=breach.get("rule", "UNKNOWN"),
                value=float(breach.get("current", 0)),
                threshold=float(breach.get("limit", 0)),
                action_taken=str(breach.get("action_required", "logged")),
            )
        except Exception:
            pass

    # Cache status in Redis
    try:
        r.set("compliance:position:status", json.dumps({
            "overall_status": status,
            "breach_count": len(breaches),
            "warning_count": len(warnings),
            "portfolio_value": state["portfolio_value"],
            "checked_at": now.isoformat(),
        }), ex=300)
    except Exception:
        pass

    logger.info(
        "POSITION LIMITS | Status: {} | Breaches: {} | Warnings: {} | Portfolio: ${:,.0f}",
        status, len(breaches), len(warnings), state["portfolio_value"],
    )

    return {"overall_status": status}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PositionLimitAgent:
    """Continuous position limit monitoring agent."""

    def __init__(self) -> None:
        self._engine = create_engine(settings.postgres_url)
        self._redis = redis.from_url(settings.redis_url, decode_responses=True)

    async def check(self) -> PositionLimitResult:
        """Run full position limit check pipeline."""
        state: PositionLimitState = {
            "positions": [], "portfolio_value": 0,
            "position_weights": {}, "sector_exposures": {},
            "limit_checks": [], "breaches": [], "warnings": [],
            "actions_required": [], "overall_status": "UNKNOWN",
            "error": None,
        }

        try:
            # Node 1
            state.update(fetch_current_positions_node(state))
            # Node 2
            state.update(check_individual_position_limits_node(state))
            # Node 3
            state.update(check_sector_limits_node(state))
            # Node 4
            state.update(check_leverage_limits_node(state))
            # Node 5
            state.update(check_concentration_score_node(state))
            # Node 6
            execute_required_actions_node(state)
            # Node 7 (async)
            result = await store_and_audit_node(state)
            state.update(result)
        except Exception as exc:
            logger.error("Position limit check failed: {}", exc)
            state["error"] = str(exc)
            state["overall_status"] = "ERROR"

        # Compute HHI
        weights = state.get("position_weights", {})
        hhi = sum(w ** 2 for w in weights.values()) if weights else 0

        return PositionLimitResult(
            breaches=state.get("breaches", []),
            warnings=state.get("warnings", []),
            actions_taken=state.get("actions_required", []),
            overall_status=state.get("overall_status", "UNKNOWN"),
            portfolio_value=state.get("portfolio_value", 0),
            position_count=len(state.get("positions", [])),
            hhi=round(hhi, 4),
            checked_at=datetime.now(timezone.utc),
        )

    def get_current_breaches(self) -> list[dict[str, Any]]:
        """Get cached breaches from Redis."""
        cached = self._redis.get("compliance:position:status")
        if cached:
            return json.loads(cached)
        return []

    def get_concentration_report(self) -> dict[str, Any]:
        """Quick concentration snapshot without full check."""
        with self._engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT ticker, current_value_usd FROM portfolio_positions "
                "WHERE status = 'active'"
            )).fetchall()
            broker = conn.execute(text(
                "SELECT portfolio_value FROM broker_connections "
                "WHERE broker_name = 'alpaca' LIMIT 1"
            )).fetchone()

        pv = float(broker[0]) if broker else 1
        pv = max(pv, 1)

        weights = {r[0]: float(r[1] or 0) / pv for r in rows}
        hhi = sum(w ** 2 for w in weights.values()) if weights else 0

        sectors: dict[str, float] = {}
        for ticker, w in weights.items():
            s = _get_sector(ticker)
            sectors[s] = sectors.get(s, 0) + w

        return {
            "hhi": round(hhi, 4),
            "equivalent_positions": round(1 / hhi, 1) if hhi > 0 else 0,
            "position_count": len(weights),
            "top_positions": sorted(weights.items(), key=lambda x: -x[1])[:5],
            "sector_weights": {k: round(v, 4) for k, v in sorted(sectors.items(), key=lambda x: -x[1])},
            "portfolio_value": pv,
        }

    def is_new_buy_blocked(self) -> bool:
        """Check if compliance has blocked new buys."""
        return self._redis.get("compliance:block_new_buys") == "True"
