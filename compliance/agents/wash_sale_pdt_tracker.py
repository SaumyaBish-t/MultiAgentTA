"""
Phase 7 — Wash Sale & Pattern Day Trade Tracker
=================================================
Real US regulatory rules. Non-compliance has tax/legal consequences.

• Wash Sale (IRS): Loss disallowed if same security re-purchased within 30 days.
• PDT (FINRA): 4+ day trades in 5 business days → Pattern Day Trader classification.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, date, timezone, timedelta
from typing import Any, Optional
from uuid import UUID

import redis
from loguru import logger
from sqlalchemy import create_engine, text

from config.settings import settings
from compliance.agents.audit_logger import audit_log

_engine = create_engine(settings.postgres_url)
_redis = redis.from_url(settings.redis_url, decode_responses=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 1 — Wash Sale Tracker
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class WashSaleTracker:
    """
    IRS Wash Sale Rule:
    Selling a security at a loss and buying the same (or substantially
    identical) security within 30 days before or after the sale causes
    the loss to be DISALLOWED for tax purposes.
    """

    def __init__(self) -> None:
        self._engine = _engine
        self._redis = _redis

    async def record_sale(
        self,
        ticker: str,
        shares: int,
        price: float,
        cost_basis: float,
        order_id: Optional[UUID] = None,
    ) -> Optional[dict[str, Any]]:
        """Record a sale. If it's a loss, open a 30-day wash-sale monitoring window."""
        pnl_per_share = price - cost_basis
        total_pnl = pnl_per_share * shares

        if total_pnl >= 0:
            return None  # profit — no wash sale concern

        now = datetime.now(timezone.utc)
        window_end = now + timedelta(days=30)
        loss = abs(total_pnl)
        record_id = uuid.uuid4()

        with self._engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO wash_sale_tracker
                    (id, ticker, sold_at, sold_price, sold_shares,
                     loss_amount, wash_sale_window_end, replacement_purchase,
                     disallowed_loss, status, created_at)
                VALUES
                    (:id, :ticker, :sold_at, :price, :shares,
                     :loss, :window_end, false, 0, 'monitoring', :now)
            """), {
                "id": str(record_id), "ticker": ticker.upper(),
                "sold_at": now, "price": price, "shares": shares,
                "loss": loss, "window_end": window_end, "now": now,
            })

        try:
            await audit_log.log(
                event_type="compliance_check",
                entity_type="order",
                entity_id=order_id,
                ticker=ticker,
                action=f"Wash sale window opened: loss ${loss:,.2f}",
                actor="wash_sale_tracker",
                details={
                    "sold_price": price, "cost_basis": cost_basis,
                    "shares": shares, "loss": loss,
                    "window_end": window_end.isoformat(),
                },
            )
        except Exception:
            pass

        logger.warning(
            "WASH SALE | Window opened for {} | Loss ${:,.2f} | Expires {}",
            ticker, loss, window_end.strftime("%Y-%m-%d"),
        )
        return {
            "id": str(record_id), "ticker": ticker, "loss": loss,
            "window_end": window_end.isoformat(),
        }

    def check_purchase(self, ticker: str) -> dict[str, Any]:
        """Check BEFORE any buy — returns wash sale warning if applicable."""
        now = datetime.now(timezone.utc)
        with self._engine.connect() as conn:
            row = conn.execute(text("""
                SELECT id, loss_amount, wash_sale_window_end, sold_at
                FROM wash_sale_tracker
                WHERE ticker = :t AND status = 'monitoring'
                AND wash_sale_window_end > :now
                ORDER BY sold_at DESC LIMIT 1
            """), {"t": ticker.upper(), "now": now}).fetchone()

        if not row:
            return {"is_wash_sale": False}

        window_end = row[2]
        days_remaining = (window_end - now).days if hasattr(window_end, '__sub__') else 0
        loss = float(row[1])

        return {
            "is_wash_sale": True,
            "ticker": ticker,
            "original_loss": loss,
            "window_ends": window_end.isoformat() if hasattr(window_end, 'isoformat') else str(window_end),
            "days_remaining": max(days_remaining, 0),
            "warning": (
                f"Buying {ticker} within 30 days of loss sale. "
                f"Loss of ${loss:,.2f} will be DISALLOWED for tax purposes."
            ),
        }

    async def record_replacement_purchase(
        self, ticker: str, order_id: Optional[UUID] = None,
    ) -> bool:
        """Mark a wash sale as triggered when the same security is re-purchased."""
        now = datetime.now(timezone.utc)
        with self._engine.begin() as conn:
            row = conn.execute(text("""
                SELECT id, loss_amount FROM wash_sale_tracker
                WHERE ticker = :t AND status = 'monitoring'
                AND wash_sale_window_end > :now
                ORDER BY sold_at DESC LIMIT 1
            """), {"t": ticker.upper(), "now": now}).fetchone()

            if not row:
                return False

            record_id, loss = str(row[0]), float(row[1])
            conn.execute(text("""
                UPDATE wash_sale_tracker
                SET replacement_purchase = true,
                    replacement_purchase_at = :now,
                    disallowed_loss = :loss,
                    status = 'triggered'
                WHERE id = :id
            """), {"now": now, "loss": loss, "id": record_id})

        try:
            await audit_log.log(
                event_type="compliance_check",
                entity_type="order",
                entity_id=order_id,
                ticker=ticker,
                action=f"Wash sale TRIGGERED — loss ${loss:,.2f} disallowed",
                actor="wash_sale_tracker",
                details={"disallowed_loss": loss, "record_id": record_id},
            )
        except Exception:
            pass

        logger.critical("WASH SALE TRIGGERED | {} | Disallowed loss: ${:,.2f}", ticker, loss)
        return True

    def get_active_windows(self) -> list[dict[str, Any]]:
        """Return all currently active monitoring windows."""
        now = datetime.now(timezone.utc)
        with self._engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, loss_amount, sold_at, wash_sale_window_end,
                       replacement_purchase, status
                FROM wash_sale_tracker
                WHERE status = 'monitoring' AND wash_sale_window_end > :now
                ORDER BY wash_sale_window_end ASC
            """), {"now": now}).fetchall()

        return [
            {
                "ticker": r[0], "loss": float(r[1]),
                "sold_at": r[2].isoformat() if r[2] else None,
                "window_end": r[3].isoformat() if r[3] else None,
                "days_remaining": max((r[3] - now).days, 0) if r[3] else 0,
                "status": r[5],
            }
            for r in rows
        ]

    def expire_old_windows(self) -> int:
        """Move expired monitoring windows to 'expired' status."""
        now = datetime.now(timezone.utc)
        with self._engine.begin() as conn:
            result = conn.execute(text("""
                UPDATE wash_sale_tracker
                SET status = 'expired'
                WHERE status = 'monitoring' AND wash_sale_window_end <= :now
            """), {"now": now})
        count = result.rowcount
        if count:
            logger.info("WASH SALE | Expired {} monitoring windows", count)
        return count

    def get_disallowed_losses_ytd(self) -> float:
        """Total disallowed losses year-to-date."""
        year_start = datetime(date.today().year, 1, 1, tzinfo=timezone.utc)
        with self._engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COALESCE(SUM(disallowed_loss), 0)
                FROM wash_sale_tracker
                WHERE status = 'triggered' AND sold_at >= :start
            """), {"start": year_start}).fetchone()
        return float(row[0]) if row else 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 2 — Pattern Day Trade Tracker
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PatternDayTradeTracker:
    """
    FINRA PDT Rule:
    4+ day trades in 5 business days with >6% of total trades
    → classified as Pattern Day Trader (requires $25k minimum).
    We track this for paper trading compliance simulation.
    """

    def __init__(self) -> None:
        self._engine = _engine
        self._redis = _redis

    def _count_day_trades_last_5_days(self) -> int:
        window_start = date.today() - timedelta(days=7)  # 7 calendar ≈ 5 business
        with self._engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*) FROM pattern_day_trade_tracker
                WHERE is_day_trade = true AND trade_date >= :start
            """), {"start": window_start}).fetchone()
        return int(row[0]) if row else 0

    def _get_today_day_trade_count(self) -> int:
        with self._engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*) FROM pattern_day_trade_tracker
                WHERE is_day_trade = true AND trade_date = :today
            """), {"today": date.today()}).fetchone()
        return int(row[0]) if row else 0

    def _check_is_day_trade(self, ticker: str, action: str) -> bool:
        """Check if this trade completes a same-day round trip."""
        opposite = "sell" if action.lower() == "buy" else "buy"
        with self._engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*) FROM orders
                WHERE ticker = :t AND action = :a AND status = 'filled'
                AND DATE(filled_at) = :today
            """), {"t": ticker, "a": opposite, "today": date.today()}).fetchone()
        return bool(row and int(row[0]) > 0)

    def _get_matching_order_id(self, ticker: str, action: str) -> Optional[str]:
        """Find the matching order from today for the round-trip."""
        with self._engine.connect() as conn:
            row = conn.execute(text("""
                SELECT id FROM orders
                WHERE ticker = :t AND action = :a AND status = 'filled'
                AND DATE(filled_at) = :today
                ORDER BY filled_at DESC LIMIT 1
            """), {"t": ticker, "a": action, "today": date.today()}).fetchone()
        return str(row[0]) if row else None

    async def record_trade(
        self, ticker: str, action: str, order_id: Optional[UUID] = None,
    ) -> dict[str, Any]:
        """Call after every fill. Returns PDT status."""
        is_day_trade = self._check_is_day_trade(ticker, action)

        if not is_day_trade:
            return {"is_day_trade": False, "rolling_count": self._count_day_trades_last_5_days()}

        rolling = self._count_day_trades_last_5_days() + 1

        # Determine buy/sell order IDs
        if action.lower() == "sell":
            buy_id = self._get_matching_order_id(ticker, "buy")
            sell_id = str(order_id) if order_id else None
        else:
            buy_id = str(order_id) if order_id else None
            sell_id = self._get_matching_order_id(ticker, "sell")

        record_id = uuid.uuid4()
        now = datetime.now(timezone.utc)

        with self._engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO pattern_day_trade_tracker
                    (id, account_id, trade_date, ticker, buy_order_id,
                     sell_order_id, is_day_trade, rolling_5day_count,
                     pdt_limit_reached, created_at)
                VALUES
                    (:id, 'paper_account', :today, :ticker, :buy_id,
                     :sell_id, true, :rolling, :limit_hit, :now)
            """), {
                "id": str(record_id), "today": date.today(),
                "ticker": ticker.upper(), "buy_id": buy_id,
                "sell_id": sell_id, "rolling": rolling,
                "limit_hit": rolling >= 4, "now": now,
            })

        # Audit at warning threshold
        if rolling >= 3:
            severity = "critical" if rolling >= 4 else "warning"
            try:
                await audit_log.log(
                    event_type="compliance_check",
                    entity_type="order",
                    entity_id=order_id,
                    ticker=ticker,
                    action=f"PDT {'LIMIT REACHED' if rolling >= 4 else 'warning'}: {rolling} day trades in 5 days",
                    actor="pdt_tracker",
                    details={"rolling_count": rolling, "limit": 4, "severity": severity},
                )
            except Exception:
                pass

            if rolling >= 4:
                logger.critical("PDT LIMIT REACHED | {} day trades in 5 business days", rolling)
                # Cache PDT status
                self._redis.set("compliance:pdt:limit_reached", "True", ex=86400)
            else:
                logger.warning("PDT WARNING | {} day trades in 5 business days (limit: 4)", rolling)

        return {
            "is_day_trade": True,
            "rolling_count": rolling,
            "pdt_warning": rolling >= 3,
            "pdt_limit_reached": rolling >= 4,
        }

    def get_rolling_count(self) -> int:
        """Current rolling 5-day day-trade count."""
        return self._count_day_trades_last_5_days()

    def is_pdt_limit_reached(self) -> bool:
        """Check if PDT limit is currently breached."""
        cached = self._redis.get("compliance:pdt:limit_reached")
        if cached == "True":
            return True
        return self._count_day_trades_last_5_days() >= 4

    def get_pdt_report(self) -> dict[str, Any]:
        """Full PDT status report."""
        rolling = self._count_day_trades_last_5_days()
        today_count = self._get_today_day_trade_count()
        return {
            "rolling_5day_count": rolling,
            "limit": 4,
            "remaining": max(0, 4 - rolling),
            "at_risk": rolling >= 3,
            "pdt_limit_reached": rolling >= 4,
            "trades_today": today_count,
        }

    def get_recent_day_trades(self, limit: int = 10) -> list[dict[str, Any]]:
        """Fetch recent day trades for review."""
        with self._engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT trade_date, ticker, rolling_5day_count, pdt_limit_reached
                FROM pattern_day_trade_tracker
                WHERE is_day_trade = true
                ORDER BY created_at DESC LIMIT :lim
            """), {"lim": limit}).fetchall()
        return [
            {"date": str(r[0]), "ticker": r[1], "rolling": r[2], "limit_hit": r[3]}
            for r in rows
        ]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 3 — Singletons
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

wash_sale_tracker = WashSaleTracker()
pdt_tracker = PatternDayTradeTracker()
