"""
Phase 7 — Report Generator Agent
==================================
Generates daily P&L, weekly performance, compliance, and
execution quality reports with optional LLM summaries.

Schedule:
  • Daily P&L          — market close (20:30 UTC)
  • Daily compliance    — market close
  • Daily execution     — market close
  • Weekly performance  — Sunday 22:00 UTC
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, date, timezone, timedelta
from typing import Any, Optional, TypedDict

import redis
from loguru import logger
from sqlalchemy import create_engine, text

from config.settings import settings
from compliance.agents.audit_logger import audit_log

_engine = create_engine(settings.postgres_url)
_redis = redis.from_url(settings.redis_url, decode_responses=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ReportState(TypedDict):
    report_date: date
    report_type: str
    portfolio_data: dict[str, Any]
    execution_data: dict[str, Any]
    compliance_data: dict[str, Any]
    risk_data: dict[str, Any]
    research_data: dict[str, Any]
    report_content: dict[str, Any]
    report_summary: str
    error: Optional[str]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Helper: safe DB query
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _q(sql: str, params: dict | None = None) -> list:
    with _engine.connect() as conn:
        return conn.execute(text(sql), params or {}).fetchall()


def _q1(sql: str, params: dict | None = None):
    with _engine.connect() as conn:
        return conn.execute(text(sql), params or {}).fetchone()


def _scalar(sql: str, params: dict | None = None, default=0):
    row = _q1(sql, params)
    return row[0] if row and row[0] is not None else default


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REPORT 1: Daily P&L
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _generate_daily_pnl(report_date: date) -> dict[str, Any]:
    """Daily portfolio P&L report."""

    # Current portfolio value
    broker = _q1(
        "SELECT portfolio_value, cash_balance FROM broker_connections "
        "WHERE broker_name = 'alpaca' LIMIT 1"
    )
    pv = float(broker[0]) if broker else 0
    cash = float(broker[1]) if broker else 0

    # Previous day's report value (fallback to current)
    prev_report = _q1(
        "SELECT portfolio_value FROM daily_reports "
        "WHERE report_type = 'daily_pnl' AND report_date < :d "
        "ORDER BY report_date DESC LIMIT 1",
        {"d": report_date},
    )
    prev_value = float(prev_report[0]) if prev_report else pv

    daily_pnl = pv - prev_value
    daily_pnl_pct = daily_pnl / prev_value if prev_value > 0 else 0

    # Active positions
    positions_raw = _q(
        "SELECT ticker, current_shares, current_value_usd, entry_price "
        "FROM portfolio_positions WHERE status = 'active' ORDER BY current_value_usd DESC"
    )
    positions = []
    best = {"ticker": "N/A", "pnl_pct": -999}
    worst = {"ticker": "N/A", "pnl_pct": 999}
    for r in positions_raw:
        ticker, shares, value, entry = r[0], int(r[1] or 0), float(r[2] or 0), float(r[3] or 0)
        weight = value / pv if pv > 0 else 0
        pos_pnl_pct = (value / (entry * shares) - 1) if entry and shares else 0
        p = {
            "ticker": ticker, "shares": shares, "current_value": round(value, 2),
            "weight": round(weight, 4), "daily_pnl_pct": round(pos_pnl_pct, 4),
        }
        positions.append(p)
        if pos_pnl_pct > best["pnl_pct"]:
            best = {"ticker": ticker, "pnl_pct": pos_pnl_pct}
        if pos_pnl_pct < worst["pnl_pct"]:
            worst = {"ticker": ticker, "pnl_pct": pos_pnl_pct}

    # Trades today
    trades = _q(
        "SELECT ticker, action, filled_shares, filled_avg_price, status "
        "FROM orders WHERE DATE(filled_at) = :d AND status = 'filled' "
        "ORDER BY filled_at",
        {"d": report_date},
    )
    trades_list = [
        {"ticker": t[0], "action": t[1], "shares": int(t[2] or 0),
         "price": float(t[3] or 0)}
        for t in trades
    ]

    # Risk metrics
    current_drawdown = float(_scalar(
        "SELECT COALESCE(MIN(daily_pnl_pct), 0) FROM daily_reports "
        "WHERE report_type = 'daily_pnl' AND report_date >= :s",
        {"s": report_date - timedelta(days=30)},
    ))

    return {
        "date": str(report_date),
        "portfolio_value": round(pv, 2),
        "cash": round(cash, 2),
        "previous_value": round(prev_value, 2),
        "daily_pnl": round(daily_pnl, 2),
        "daily_pnl_pct": round(daily_pnl_pct, 4),
        "positions": positions,
        "position_count": len(positions),
        "trades_today": trades_list,
        "trade_count": len(trades_list),
        "best_performer": best["ticker"],
        "worst_performer": worst["ticker"],
        "risk_metrics": {
            "current_drawdown_30d": round(current_drawdown, 4),
        },
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REPORT 2: Weekly Performance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _generate_weekly(week_end: date) -> dict[str, Any]:
    """Weekly performance report."""
    week_start = week_end - timedelta(days=7)
    year_start = date(week_end.year, 1, 1)

    # Current value
    pv = float(_scalar(
        "SELECT portfolio_value FROM broker_connections "
        "WHERE broker_name = 'alpaca' LIMIT 1",
    ))

    # Week start value
    start_report = _q1(
        "SELECT portfolio_value FROM daily_reports "
        "WHERE report_type = 'daily_pnl' AND report_date <= :d "
        "ORDER BY report_date DESC LIMIT 1",
        {"d": week_start},
    )
    start_val = float(start_report[0]) if start_report else pv
    weekly_return = (pv - start_val) / start_val if start_val > 0 else 0

    # YTD
    ytd_report = _q1(
        "SELECT portfolio_value FROM daily_reports "
        "WHERE report_type = 'daily_pnl' AND report_date <= :d "
        "ORDER BY report_date ASC LIMIT 1",
        {"d": year_start + timedelta(days=5)},
    )
    ytd_start = float(ytd_report[0]) if ytd_report else pv
    ytd_return = (pv - ytd_start) / ytd_start if ytd_start > 0 else 0

    # Execution summary
    total_trades = int(_scalar(
        "SELECT COUNT(*) FROM orders WHERE status = 'filled' AND DATE(filled_at) BETWEEN :s AND :e",
        {"s": week_start, "e": week_end},
    ))
    total_value = float(_scalar(
        "SELECT COALESCE(SUM(filled_shares * filled_avg_price), 0) FROM orders "
        "WHERE status = 'filled' AND DATE(filled_at) BETWEEN :s AND :e",
        {"s": week_start, "e": week_end},
    ))
    avg_slippage = float(_scalar(
        "SELECT COALESCE(AVG(slippage_bps), 0) FROM execution_performance "
        "WHERE DATE(measured_at) BETWEEN :s AND :e",
        {"s": week_start, "e": week_end},
    ))

    # Compliance
    checks = int(_scalar(
        "SELECT COUNT(*) FROM compliance_checks WHERE DATE(checked_at) BETWEEN :s AND :e",
        {"s": week_start, "e": week_end},
    ))
    violations = int(_scalar(
        "SELECT COUNT(*) FROM rule_violations WHERE DATE(created_at) BETWEEN :s AND :e",
        {"s": week_start, "e": week_end},
    ))

    return {
        "period": f"{week_start} to {week_end}",
        "portfolio_value": round(pv, 2),
        "weekly_return": round(weekly_return, 4),
        "ytd_return": round(ytd_return, 4),
        "execution_summary": {
            "total_trades": total_trades,
            "total_value": round(total_value, 2),
            "avg_slippage_bps": round(avg_slippage, 2),
        },
        "compliance_summary": {
            "checks_run": checks,
            "violations": violations,
        },
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REPORT 3: Compliance Summary
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _generate_compliance(report_date: date) -> dict[str, Any]:
    """Daily compliance summary report."""

    checks = int(_scalar(
        "SELECT COUNT(*) FROM compliance_checks WHERE DATE(checked_at) = :d",
        {"d": report_date},
    ))

    violations = _q(
        "SELECT rule_id, ticker, severity, description FROM rule_violations "
        "WHERE DATE(created_at) = :d ORDER BY created_at DESC",
        {"d": report_date},
    )
    violations_list = [
        {"rule_id": v[0], "ticker": v[1], "severity": v[2], "description": str(v[3])[:100]}
        for v in violations
    ]

    # Wash sale windows
    now = datetime.now(timezone.utc)
    wash_windows = _q(
        "SELECT ticker, loss_amount, wash_sale_window_end FROM wash_sale_tracker "
        "WHERE status = 'monitoring' AND wash_sale_window_end > :now",
        {"now": now},
    )
    wash_list = [
        {"ticker": w[0], "loss": float(w[1]), "expires": str(w[2])}
        for w in wash_windows
    ]

    triggered = int(_scalar(
        "SELECT COUNT(*) FROM wash_sale_tracker "
        "WHERE status = 'triggered' AND DATE(sold_at) = :d",
        {"d": report_date},
    ))

    # PDT
    pdt_count = int(_scalar(
        "SELECT COUNT(*) FROM pattern_day_trade_tracker "
        "WHERE is_day_trade = true AND trade_date >= :s",
        {"s": report_date - timedelta(days=7)},
    ))

    # Restricted list
    restricted = _q(
        "SELECT ticker, restriction_type, reason FROM restricted_list WHERE active = true"
    )
    restricted_list = [{"ticker": r[0], "type": r[1], "reason": r[2]} for r in restricted]

    # Audit events
    audit_count = int(_scalar(
        "SELECT COUNT(*) FROM audit_log WHERE DATE(created_at) = :d",
        {"d": report_date},
    ))

    return {
        "date": str(report_date),
        "checks_run": checks,
        "violations": violations_list,
        "violation_count": len(violations_list),
        "wash_sales": {
            "active_windows": wash_list,
            "triggered_today": triggered,
        },
        "pdt_status": {
            "rolling_5day_count": pdt_count,
            "limit": 4,
            "at_risk": pdt_count >= 3,
        },
        "restricted_list": {
            "active_restrictions": restricted_list,
            "count": len(restricted_list),
        },
        "audit_events_today": audit_count,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REPORT 4: Execution Quality
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _generate_execution(report_date: date) -> dict[str, Any]:
    """Daily execution quality report."""

    total = int(_scalar(
        "SELECT COUNT(*) FROM orders WHERE DATE(created_at) = :d",
        {"d": report_date},
    ))
    filled = int(_scalar(
        "SELECT COUNT(*) FROM orders WHERE DATE(filled_at) = :d AND status = 'filled'",
        {"d": report_date},
    ))
    fill_rate = filled / total if total > 0 else 0

    # Slippage
    avg_slip = float(_scalar(
        "SELECT COALESCE(AVG(slippage_bps), 0) FROM execution_performance "
        "WHERE DATE(measured_at) = :d",
        {"d": report_date},
    ))
    worst_slip = float(_scalar(
        "SELECT COALESCE(MAX(slippage_bps), 0) FROM execution_performance "
        "WHERE DATE(measured_at) = :d",
        {"d": report_date},
    ))
    total_cost = float(_scalar(
        "SELECT COALESCE(SUM(total_cost_bps), 0) FROM execution_performance "
        "WHERE DATE(measured_at) = :d",
        {"d": report_date},
    ))

    # By order type
    market_count = int(_scalar(
        "SELECT COUNT(*) FROM orders WHERE DATE(filled_at) = :d AND order_type = 'market' AND status = 'filled'",
        {"d": report_date},
    ))
    limit_count = int(_scalar(
        "SELECT COUNT(*) FROM orders WHERE DATE(filled_at) = :d AND order_type = 'limit' AND status = 'filled'",
        {"d": report_date},
    ))

    # Recommendations
    recs = []
    if avg_slip > 5:
        recs.append("Average slippage exceeds 5 bps — consider increasing limit order usage.")
    if fill_rate < 0.90 and total > 0:
        recs.append(f"Fill rate {fill_rate:.0%} is below 90% — review order sizing and pricing.")
    if worst_slip > 20:
        recs.append("Worst slippage exceeds 20 bps — review large market orders.")
    if not recs:
        recs.append("Execution quality within normal parameters.")

    return {
        "date": str(report_date),
        "total_orders": total,
        "filled_orders": filled,
        "fill_rate": round(fill_rate, 4),
        "slippage": {
            "avg_bps": round(avg_slip, 2),
            "worst_bps": round(worst_slip, 2),
            "total_cost_usd": round(total_cost, 2),
        },
        "by_order_type": {
            "market_orders": {"count": market_count},
            "limit_orders": {"count": limit_count},
        },
        "recommendations": recs,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LLM Summary
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _generate_summary(report_type: str, data: dict) -> str:
    """Generate a human-readable executive summary via LLM."""
    try:
        if report_type == "weekly":
            from config.llm_config import research_llm as llm
        else:
            from config.llm_config import simple_llm as llm

        prompt = (
            f"You are a portfolio manager writing a {report_type} report.\n"
            f"Data: {json.dumps(data, indent=2, default=str)}\n\n"
            "Write a professional 3-5 sentence executive summary. "
            "Include key performance metrics, notable events, concerns, "
            "and forward-looking notes. Be specific with numbers. Professional tone."
        )
        response = llm.invoke(prompt)
        return response.content
    except Exception as exc:
        logger.warning("LLM summary generation failed: {}", exc)
        # Fallback: build a basic summary from data
        pv = data.get("portfolio_value", 0)
        pnl = data.get("daily_pnl", data.get("weekly_return", 0))
        viols = data.get("violation_count", data.get("violations", 0))
        if isinstance(viols, list):
            viols = len(viols)
        return (
            f"Report for {data.get('date', data.get('period', 'N/A'))}. "
            f"Portfolio value: ${pv:,.0f}. "
            f"P&L: ${pnl:,.2f} ({viols} violations)."
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Storage & Delivery
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _save_report(
    report_type: str, report_date: date, data: dict, summary: str,
) -> None:
    """Persist report to DB, cache in Redis, audit-log, and publish."""
    now = datetime.now(timezone.utc)
    report_id = uuid.uuid4()

    pv = data.get("portfolio_value", 0)
    daily_pnl = data.get("daily_pnl", 0)
    daily_pnl_pct = data.get("daily_pnl_pct", 0)
    total_trades = data.get("trade_count", data.get("total_orders", 0))
    traded_value = data.get("execution_summary", {}).get("total_value", 0)
    viols = data.get("violation_count", 0)
    if isinstance(viols, list):
        viols = len(viols)
    warns = data.get("warning_count", 0)
    if isinstance(warns, list):
        warns = len(warns)

    try:
        with _engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO daily_reports
                    (id, report_date, report_type, portfolio_value,
                     daily_pnl, daily_pnl_pct, total_trades, total_traded_value,
                     violations_count, warnings_count, report_data,
                     generated_at, delivered, created_at)
                VALUES
                    (:id, :rd, :rt, :pv,
                     :dpnl, :dpct, :tt, :ttv,
                     :vc, :wc, :data,
                     :now, false, :now)
            """), {
                "id": str(report_id), "rd": report_date, "rt": report_type,
                "pv": pv, "dpnl": daily_pnl, "dpct": daily_pnl_pct,
                "tt": total_trades, "ttv": traded_value,
                "vc": viols, "wc": warns,
                "data": json.dumps({**data, "summary": summary}, default=str),
                "now": now,
            })
    except Exception as exc:
        logger.error("Failed to save report to DB: {}", exc)

    # Redis cache
    try:
        _redis.set(
            f"reports:{report_type}:latest",
            json.dumps({**data, "summary": summary}, default=str),
            ex=86400,
        )
    except Exception:
        pass

    # Audit log
    try:
        await audit_log.log(
            event_type="compliance_check",
            entity_type="report",
            action=f"Report generated: {report_type}",
            actor="report_generator",
            details={"report_type": report_type, "date": str(report_date)},
        )
    except Exception:
        pass

    # Publish event
    try:
        _redis.publish("compliance.report.generated", json.dumps({
            "type": report_type,
            "date": str(report_date),
            "summary": summary[:200],
        }))
    except Exception:
        pass

    logger.info("REPORT | {} for {} saved | Portfolio ${:,.0f}", report_type, report_date, pv)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ReportGenerator:
    """Generates and stores compliance & performance reports."""

    async def generate_daily_pnl(self, report_date: Optional[date] = None) -> dict[str, Any]:
        """Generate daily P&L report."""
        rd = report_date or date.today()
        data = _generate_daily_pnl(rd)
        summary = await _generate_summary("daily_pnl", data)
        data["summary"] = summary
        await _save_report("daily_pnl", rd, data, summary)
        return data

    async def generate_weekly(self, week_end: Optional[date] = None) -> dict[str, Any]:
        """Generate weekly performance report."""
        we = week_end or date.today()
        data = _generate_weekly(we)
        summary = await _generate_summary("weekly", data)
        data["summary"] = summary
        await _save_report("weekly", we, data, summary)
        return data

    async def generate_compliance(self, report_date: Optional[date] = None) -> dict[str, Any]:
        """Generate daily compliance summary."""
        rd = report_date or date.today()
        data = _generate_compliance(rd)
        summary = await _generate_summary("compliance", data)
        data["summary"] = summary
        await _save_report("compliance", rd, data, summary)
        return data

    async def generate_execution(self, report_date: Optional[date] = None) -> dict[str, Any]:
        """Generate daily execution quality report."""
        rd = report_date or date.today()
        data = _generate_execution(rd)
        summary = await _generate_summary("execution", data)
        data["summary"] = summary
        await _save_report("execution", rd, data, summary)
        return data

    def get_latest_report(self, report_type: str) -> Optional[dict[str, Any]]:
        """Fetch latest cached report from Redis."""
        cached = _redis.get(f"reports:{report_type}:latest")
        if cached:
            return json.loads(cached)
        # Fallback to DB
        row = _q1(
            "SELECT report_data FROM daily_reports "
            "WHERE report_type = :rt ORDER BY report_date DESC LIMIT 1",
            {"rt": report_type},
        )
        if row and row[0]:
            data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
            return data
        return None

    def get_report_history(self, report_type: str, days: int = 30) -> list[dict[str, Any]]:
        """Fetch report history for the last N days."""
        cutoff = date.today() - timedelta(days=days)
        rows = _q(
            "SELECT report_date, portfolio_value, daily_pnl, daily_pnl_pct, "
            "total_trades, violations_count FROM daily_reports "
            "WHERE report_type = :rt AND report_date >= :c "
            "ORDER BY report_date DESC",
            {"rt": report_type, "c": cutoff},
        )
        return [
            {
                "date": str(r[0]), "portfolio_value": float(r[1]),
                "daily_pnl": float(r[2]), "daily_pnl_pct": float(r[3]),
                "total_trades": int(r[4]), "violations": int(r[5]),
            }
            for r in rows
        ]
