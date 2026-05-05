"""
Phase 8: Alert Manager
======================
Central hub for routing, deduplicating, and managing system alerts.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union

import redis
from loguru import logger
from sqlalchemy import create_engine, text, func, select, desc, update
from sqlalchemy.orm import Session

from config.settings import settings
from monitoring.storage.monitoring_models import Alert

class AlertManager:
    SEVERITY_CHANNELS = {
        "info":      ["redis", "log"],
        "warning":   ["redis", "log", "dashboard"],
        "critical":  ["redis", "log", "dashboard"],
        "emergency": ["redis", "log", "dashboard"]
    }
    
    # Deduplicate: same alert type + ticker within this window won't fire again
    DEDUP_WINDOW_SECONDS = {
        "info":      3600,   # 1 hour
        "warning":   1800,   # 30 min
        "critical":  300,    # 5 min
        "emergency": 60      # 1 min
    }

    def __init__(self):
        self.redis = redis.from_url(settings.redis_url, decode_responses=True)
        self.engine = create_engine(settings.postgres_url)

    async def send_alert(
        self,
        alert_type: str,
        severity: str,
        title: str,
        message: str,
        data: Dict[str, Any] = {},
        ticker: Optional[str] = None
    ) -> bool:
        """Sends an alert through configured channels with deduplication."""
        try:
            # 1. Deduplication check
            dedup_key = f"alert:dedup:{alert_type}:{ticker or 'global'}"
            if self.redis.exists(dedup_key):
                logger.debug(f"Alert deduplicated: {alert_type} for {ticker or 'global'}")
                return False
            
            # 2. Write to alerts table
            now = datetime.now(timezone.utc)
            channels = self.SEVERITY_CHANNELS.get(severity, ["log"])
            
            with Session(self.engine) as session:
                alert = Alert(
                    alert_type=alert_type,
                    severity=severity,
                    title=title,
                    message=message,
                    data=data,
                    channel=",".join(channels),
                    created_at=now,
                    acknowledged=False
                )
                session.add(alert)
                session.commit()
                alert_id = alert.id
            
            # 3. Set dedup key
            dedup_seconds = self.DEDUP_WINDOW_SECONDS.get(severity, 3600)
            self.redis.set(dedup_key, "1", ex=dedup_seconds)
            
            # 4. Route to channels
            if "redis" in channels:
                self.redis.publish(f"alerts.{severity}", json.dumps({
                    "id": str(alert_id),
                    "type": alert_type,
                    "title": title,
                    "message": message,
                    "ticker": ticker,
                    "severity": severity,
                    "timestamp": now.isoformat()
                }))
            
            if "log" in channels:
                log_fn = {
                    "info": logger.info,
                    "warning": logger.warning,
                    "critical": logger.critical,
                    "emergency": logger.critical
                }.get(severity, logger.info)
                log_fn(f"🔔 ALERT [{severity.upper()}] {title}: {message}")
            
            if "dashboard" in channels:
                self.redis.lpush("dashboard:alerts", json.dumps({
                    "id": str(alert_id),
                    "type": alert_type,
                    "severity": severity,
                    "title": title,
                    "message": message,
                    "timestamp": now.isoformat()
                }))
                self.redis.ltrim("dashboard:alerts", 0, 99)  # keep last 100
            
            return True

        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            return False

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PRE-BUILT ALERT METHODS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def alert_drawdown(self, drawdown_pct: float) -> None:
        severity = "emergency" if drawdown_pct < -0.15 else \
                   "critical" if drawdown_pct < -0.10 else "warning"
        await self.send_alert(
            alert_type="drawdown",
            severity=severity,
            title=f"Portfolio Drawdown: {drawdown_pct:.1%}",
            message=f"Portfolio is down {abs(drawdown_pct):.1%} from peak.",
            data={"drawdown_pct": float(drawdown_pct)}
        )

    async def alert_signal_decay(self, signal_id: uuid.UUID, ticker: str, hit_rate: float, expected: float = 0.5) -> None:
        await self.send_alert(
            alert_type="signal_decay",
            severity="warning",
            title=f"Signal Decay Detected: {ticker}",
            message=f"Hit rate dropped to {hit_rate:.0%} (expected >{expected:.0%})",
            ticker=ticker,
            data={"signal_id": str(signal_id), "hit_rate": float(hit_rate), "expected": float(expected)}
        )

    async def alert_regime_change(self, old: str, new: str, confidence: float) -> None:
        await self.send_alert(
            alert_type="regime_change",
            severity="warning",
            title=f"Market Regime Change: {old} -> {new}",
            message=f"Regime shifted with {confidence:.0%} confidence. Review strategy weights.",
            data={"old_regime": old, "new_regime": new, "confidence": float(confidence)}
        )

    async def alert_system_health(self, component: str, status: str, error: str) -> None:
        severity = "emergency" if status == "offline" else "critical"
        await self.send_alert(
            alert_type="system_health",
            severity=severity,
            title=f"System Component {status.upper()}: {component}",
            message=f"{component} is {status}. Error: {error}",
            data={"component": component, "status": status, "error": error}
        )

    async def alert_position_mismatch(self, ticker: str, db_shares: int, broker_shares: int) -> None:
        await self.send_alert(
            alert_type="position_mismatch",
            severity="critical",
            title=f"Position Mismatch: {ticker}",
            message=f"DB has {db_shares} shares, broker has {broker_shares} shares. Manual reconciliation needed.",
            ticker=ticker,
            data={"db_shares": int(db_shares), "broker_shares": int(broker_shares)}
        )

    async def alert_data_quality(self, ticker: str, issue: str) -> None:
        await self.send_alert(
            alert_type="data_quality",
            severity="warning",
            title=f"Data Quality Issue: {ticker}",
            message=issue,
            ticker=ticker
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ALERT MANAGEMENT
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_active_alerts(self, severity: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetches unacknowledged alerts."""
        with self.engine.connect() as conn:
            query = select(Alert).where(Alert.acknowledged == False)
            if severity:
                query = query.where(Alert.severity == severity)
            query = query.order_by(desc(Alert.created_at))
            
            res = conn.execute(query).fetchall()
            return [dict(r._mapping) for r in res]

    def acknowledge_alert(self, alert_id: uuid.UUID, acknowledged_by: str = "system") -> bool:
        """Marks an alert as acknowledged."""
        try:
            now = datetime.now(timezone.utc)
            with Session(self.engine) as session:
                stmt = update(Alert).where(Alert.id == alert_id).values(
                    acknowledged=True,
                    acknowledged_at=now,
                    acknowledged_by=acknowledged_by
                )
                session.execute(stmt)
                session.commit()
            return True
        except Exception:
            return False

    def acknowledge_all(self, alert_type: str) -> int:
        """Acknowledges all alerts of a specific type."""
        try:
            now = datetime.now(timezone.utc)
            with Session(self.engine) as session:
                stmt = update(Alert).where(
                    Alert.alert_type == alert_type,
                    Alert.acknowledged == False
                ).values(
                    acknowledged=True,
                    acknowledged_at=now,
                    acknowledged_by="system"
                )
                result = session.execute(stmt)
                session.commit()
                return result.rowcount
        except Exception:
            return 0

    def get_alert_summary(self) -> Dict[str, Any]:
        """Returns a high-level summary of active alerts."""
        try:
            with self.engine.connect() as conn:
                # Total active
                total = conn.execute(select(func.count(Alert.id)).where(Alert.acknowledged == False)).scalar() or 0
                
                # By severity
                counts_by_sev = {}
                for sev in ["emergency", "critical", "warning", "info"]:
                    c = conn.execute(select(func.count(Alert.id)).where(Alert.severity == sev, Alert.acknowledged == False)).scalar() or 0
                    counts_by_sev[sev] = c
                
                # Oldest unacknowledged
                oldest = conn.execute(select(Alert.created_at).where(Alert.acknowledged == False).order_by(Alert.created_at)).scalar()
                
                # Most common type
                common = conn.execute(
                    select(Alert.alert_type, func.count(Alert.id).label("cnt"))
                    .where(Alert.acknowledged == False)
                    .group_by(Alert.alert_type)
                    .order_by(desc("cnt"))
                    .limit(1)
                ).fetchone()
                
                return {
                    "total_active": total,
                    "by_severity": counts_by_sev,
                    "oldest_unacknowledged": oldest.isoformat() if oldest else None,
                    "most_common_type": common[0] if common else None
                }
        except Exception as e:
            logger.error(f"Failed to get alert summary: {e}")
            return {}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SINGLETON
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
alert_manager = AlertManager()
