"""
Phase 8: Feedback Loop Agent
===========================
Self-improving mechanism that updates system parameters based on monitoring insights.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone, date
from typing import Dict, List, Any, Optional, TypedDict, cast
from dataclasses import dataclass

import redis
import httpx
from loguru import logger
from sqlalchemy import create_engine, text, select, desc, update
from sqlalchemy.orm import Session

from config.settings import settings
from monitoring.storage.monitoring_models import FeedbackAction, RetrainingTrigger
from monitoring.alerts.alert_manager import alert_manager

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATE & DATA CLASSES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FeedbackState(TypedDict):
    regime_result: Dict[str, Any]
    decay_results: List[Dict[str, Any]]
    pnl_result: Dict[str, Any]
    anomaly_result: Dict[str, Any]
    feedback_actions: List[Dict[str, Any]]
    actions_applied: List[Dict[str, Any]]
    actions_failed: List[Dict[str, Any]]
    retraining_triggered: bool
    error: Optional[str]

@dataclass
class FeedbackResult:
    actions: List[Dict[str, Any]]
    applied: int
    failed: int

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FEEDBACK AGENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FeedbackAgent:
    def __init__(self):
        self.redis = redis.from_url(settings.redis_url, decode_responses=True)
        self.engine = create_engine(settings.postgres_url)
        self.api_base_url = "http://localhost:8001" # Phase 1 FastAPI

    async def process_feedback(
        self,
        regime_result: Any,
        decay_results: List[Any],
        pnl_result: Any,
        anomaly_result: Any
    ) -> FeedbackResult:
        """Processes all monitoring results and triggers feedback actions."""
        actions = []
        
        try:
            # 1. Regime Change Response
            if hasattr(regime_result, "regime_changed") and regime_result.regime_changed:
                action = await self.handle_regime_change(regime_result)
                actions.append(action)
            
            # 2. Signal Decay Response
            for signal in decay_results:
                if hasattr(signal, "status") and signal.status == "critical_decay":
                    action = await self.handle_signal_decay(signal)
                    actions.append(action)
            
            # 3. Consistent Underperformance
            if hasattr(pnl_result, "rolling_metrics"):
                m30 = pnl_result.rolling_metrics.get("30d")
                if m30 and m30.get("sharpe", 1.0) < 0.3:
                    action = await self.handle_poor_performance(m30)
                    actions.append(action)
            
            # 4. Execution Quality Issues
            quality = self.redis.get("execution:quality:score:latest")
            if quality and float(quality) < 0.5:
                action = await self.handle_poor_execution(float(quality))
                actions.append(action)
            
            # 5. Systemic Anomalies
            if hasattr(anomaly_result, "critical") and anomaly_result.critical > 3:
                action = await self.handle_systemic_issues(anomaly_result)
                actions.append(action)
            
            # Store and publish
            applied_count = 0
            failed_count = 0
            
            for action in actions:
                self._write_feedback_action_db(action)
                if action.get("status") == "applied":
                    applied_count += 1
                else:
                    failed_count += 1
                    
            self.redis.publish("monitoring.feedback.processed", json.dumps({
                "actions_count": len(actions),
                "retraining": any(a.get("action_type") == "retrain_research" for a in actions)
            }))
            
            return FeedbackResult(
                actions=actions,
                applied=applied_count,
                failed=failed_count
            )

        except Exception as e:
            logger.error(f"Feedback processing failed: {e}")
            raise

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # SPECIFIC HANDLERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def handle_regime_change(self, regime: Any) -> Dict[str, Any]:
        """Updates Phase 2 research weights and Phase 4 risk thresholds on regime change."""
        implications = self.get_regime_implications(regime.regime)
        now_str = datetime.now(timezone.utc).isoformat()
        
        # Update Redis for Phase 2
        self.redis.set("research:regime:current", json.dumps({
            "regime": regime.regime,
            "confidence": regime.confidence,
            "favored_strategies": implications["favored"],
            "avoid_strategies": implications["avoid"],
            "sizing_multiplier": implications["sizing"],
            "updated_at": now_str
        }))
        
        # Adjust Phase 4 risk thresholds
        if regime.regime == "bear":
            self.redis.set("risk:regime_override:max_position", "0.03")
            self.redis.set("risk:regime_override:min_cash", "0.15")
        elif regime.regime == "volatile":
            self.redis.set("risk:regime_override:max_position", "0.04")
            self.redis.set("risk:regime_override:min_cash", "0.10")
        else:
            self.redis.delete("risk:regime_override:max_position")
            self.redis.delete("risk:regime_override:min_cash")
            
        # Send Alert
        await alert_manager.alert_regime_change(
            old=getattr(regime, "previous_regime", "UNKNOWN"),
            new=regime.regime,
            confidence=regime.confidence
        )
        
        return {
            "action_type": "change_regime_weights",
            "status": "applied",
            "details": {"regime": regime.regime, "implications": implications}
        }

    async def handle_signal_decay(self, signal: Any) -> Dict[str, Any]:
        """Retires critical signals and triggers replacement generation."""
        try:
            with self.engine.begin() as conn:
                # 1. Retire in Phase 3
                conn.execute(text("UPDATE trading_signals SET status='retired' WHERE id=:sid"), {"sid": signal.signal_id})
                # 2. Expire in Phase 4
                conn.execute(text("UPDATE approved_signals SET status='expired' WHERE signal_id=:sid"), {"sid": signal.signal_id})
                
            # 3. Trigger replacement research
            self.redis.publish("monitoring.signal.retired", json.dumps({
                "signal_id": str(signal.signal_id),
                "ticker": signal.ticker,
                "reason": "critical_decay",
                "trigger_new_research": True
            }))
            
            await alert_manager.alert_signal_decay(
                signal_id=signal.signal_id,
                ticker=signal.ticker,
                hit_rate=signal.hit_rate_recent
            )
            
            return {
                "action_type": "disable_strategy",
                "status": "applied",
                "details": {"signal_id": str(signal.signal_id), "ticker": signal.ticker}
            }
        except Exception as e:
            logger.error(f"Failed to handle signal decay for {signal.signal_id}: {e}")
            return {"action_type": "disable_strategy", "status": "failed", "details": {"error": str(e)}}

    async def handle_poor_performance(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Adjusts parameters or triggers recalibration on poor performance."""
        # Check if systemic market issue
        spy_30d = self._get_spy_30d_return()
        
        if metrics.get("return", 0) < spy_30d - 0.05:
            # Significant underperformance
            new_limit = 0.03
            self.redis.set("risk:position_size:default", str(new_limit))
            
            self._write_retraining_trigger_db({
                "reason": "sustained_underperformance",
                "retrain_type": "recalibrate",
                "metrics": metrics
            })
            
            return {
                "action_type": "adjust_parameters",
                "status": "applied",
                "details": {"change": "reduced_max_position", "new_limit": new_limit}
            }
        else:
            return {
                "action_type": "flag_for_review",
                "status": "applied",
                "details": {"reason": "market_wide_decline"}
            }

    async def handle_poor_execution(self, quality_score: float) -> Dict[str, Any]:
        """Handles poor execution quality scores."""
        return {
            "action_type": "flag_for_review",
            "status": "applied",
            "details": {"reason": "poor_execution_quality", "score": quality_score}
        }

    async def handle_systemic_issues(self, anomalies: Any) -> Dict[str, Any]:
        """Halts trading if too many critical anomalies are detected."""
        if anomalies.critical >= 5:
            self.redis.set("risk:trading:halted", "True", ex=3600)
            
            await alert_manager.send_alert(
                alert_type="system_health",
                severity="emergency",
                title="Trading Halted: Multiple Critical Anomalies",
                message=f"{anomalies.critical} critical anomalies detected. Trading halted for safety.",
                data={"anomaly_count": anomalies.critical}
            )
            
            return {
                "action_type": "trading_halt",
                "status": "applied",
                "details": {"critical_count": anomalies.critical, "trading_halted": True}
            }
            
        return {
            "action_type": "flag_for_review",
            "status": "applied",
            "details": {"anomaly_count": anomalies.critical}
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # HELPERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_regime_implications(self, regime: str) -> Dict[str, Any]:
        """Returns strategy implications for a given market regime."""
        implications = {
            "bull": {"favored": ["momentum", "trend", "breakout"], "avoid": ["mean_reversion"], "sizing": 1.0, "risk_level": "normal"},
            "bear": {"favored": ["mean_reversion", "defensive"], "avoid": ["momentum", "breakout"], "sizing": 0.5, "risk_level": "reduced"},
            "volatile": {"favored": ["mean_reversion", "volatility"], "avoid": ["trend", "momentum"], "sizing": 0.6, "risk_level": "reduced"},
            "sideways": {"favored": ["mean_reversion", "pairs"], "avoid": ["trend", "breakout"], "sizing": 0.8, "risk_level": "normal"},
            "recession_risk": {"favored": ["defensive", "quality"], "avoid": ["momentum", "growth", "leverage"], "sizing": 0.4, "risk_level": "high"}
        }
        return implications.get(regime, implications["sideways"])

    def _get_spy_30d_return(self) -> float:
        """Mock: Fetches SPY 30-day return."""
        # In real system, query market data or performance metrics for SPY
        return -0.02 # Mock

    def _write_feedback_action_db(self, action_data: Dict[str, Any]):
        """Persists feedback action to PostgreSQL."""
        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            action = FeedbackAction(
                trigger_type=action_data.get("action_type", "unknown"),
                trigger_details=action_data.get("details", {}),
                action_type=action_data.get("action_type", "unknown"),
                action_details=action_data.get("details", {}),
                target_phase="phase2", # Default
                target_agent="feedback_agent",
                status=action_data.get("status", "pending"),
                created_at=now
            )
            session.add(action)
            session.commit()

    def _write_retraining_trigger_db(self, trigger_data: Dict[str, Any]):
        """Persists retraining trigger to PostgreSQL."""
        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            trigger = RetrainingTrigger(
                trigger_date=date.today(),
                trigger_reason=trigger_data.get("reason"),
                affected_signals=json.dumps(trigger_data.get("affected_signals", [])),
                metrics_at_trigger=trigger_data.get("metrics", {}),
                retrain_type=trigger_data.get("retrain_type", "full"),
                retrain_status="pending",
                triggered_at=now
            )
            session.add(trigger)
            session.commit()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PUBLIC INTERFACE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_pending_actions(self) -> List[Dict[str, Any]]:
        """Fetches pending feedback actions."""
        with self.engine.connect() as conn:
            query = select(FeedbackAction).where(FeedbackAction.status == "pending")
            res = conn.execute(query).fetchall()
            return [dict(r._mapping) for r in res]

    def get_feedback_history(self, days=30) -> List[Dict[str, Any]]:
        """Fetches historical feedback actions."""
        with self.engine.connect() as conn:
            query = select(FeedbackAction).where(FeedbackAction.created_at > datetime.now(timezone.utc) - timedelta(days=days)).order_by(desc(FeedbackAction.created_at))
            res = conn.execute(query).fetchall()
            return [dict(r._mapping) for r in res]
