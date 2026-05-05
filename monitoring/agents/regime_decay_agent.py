"""
Phase 8: Regime Detection & Signal Decay Agent
=============================================
Detects market regime shifts and monitors signals for performance decay.
"""

import asyncio
import json
import uuid
import math
from datetime import datetime, timezone, date, timedelta
from typing import Dict, List, Any, Optional, TypedDict, cast
from dataclasses import dataclass, asdict

import pandas as pd
import numpy as np
import httpx
import redis
from loguru import logger
from sqlalchemy import create_engine, text, select, desc
from sqlalchemy.orm import Session

from config.settings import settings
from monitoring.storage.monitoring_models import (
    RegimeDetection, SignalLivePerformance, Alert, FeedbackAction, RetrainingTrigger
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATE & DATA CLASSES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RegimeDecayState(TypedDict):
    current_regime: str
    previous_regime: str
    regime_confidence: float
    regime_change_detected: bool
    decaying_signals: List[Dict[str, Any]]
    healthy_signals: List[Dict[str, Any]]
    regime_indicators: Dict[str, Any]
    feedback_actions: List[Dict[str, Any]]
    retraining_needed: bool
    error: Optional[str]

@dataclass
class RegimeResult:
    regime: str
    confidence: float
    indicators: Dict[str, Any]
    momentum_60d: float
    vix: float
    yield_curve: float

    def dict(self):
        return asdict(self)

@dataclass
class DecayResult:
    signal_id: uuid.UUID
    ticker: str
    status: str # critical_decay, moderate_decay, early_decay, healthy, insufficient_data
    decay_score: float = 0.0
    hit_rate_recent: float = 0.0
    hit_rate_older: float = 0.0
    regime_mismatch: bool = False
    recommendation: str = "continue"

@dataclass
class RegimeDecayResult:
    current_regime: str
    regime_confidence: float
    regime_changed: bool
    healthy_signals: int
    decaying_signals: int
    feedback_actions: List[Dict[str, Any]]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# REGIME DETECTOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RegimeDetector:
    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url

    async def detect_current_regime(self) -> RegimeResult:
        """Detect market regime using macro indicators."""
        async with httpx.AsyncClient() as client:
            try:
                # 1. Fetch indicators
                vix_resp = await client.get(f"{self.api_base_url}/macro/VIXCLS")
                vix = vix_resp.json().get("value", 20.0) if vix_resp.status_code == 200 else 20.0
                
                spy_60d_resp = await client.get(f"{self.api_base_url}/prices/SPY/history", params={"days": 60})
                spy_60d = spy_60d_resp.json() if spy_60d_resp.status_code == 200 else []
                
                spy_252d_resp = await client.get(f"{self.api_base_url}/prices/SPY/history", params={"days": 252})
                spy_252d = spy_252d_resp.json() if spy_252d_resp.status_code == 200 else []
                
                yc_resp = await client.get(f"{self.api_base_url}/macro/T10Y2Y")
                yield_curve = yc_resp.json().get("value", 1.0) if yc_resp.status_code == 200 else 1.0
                
            except Exception as e:
                logger.error(f"Failed to fetch regime indicators: {e}")
                # Fallback defaults
                vix = 20.0
                spy_60d = []
                spy_252d = []
                yield_curve = 1.0

        indicators = {}
        
        # Trend (SMA 50 vs 200)
        df_spy = pd.DataFrame(spy_252d)
        if not df_spy.empty and "close" in df_spy.columns:
            sma_50 = df_spy["close"].rolling(50).mean().iloc[-1]
            sma_200 = df_spy["close"].rolling(200).mean().iloc[-1]
            trend = "up" if sma_50 > sma_200 else "down"
            momentum_60d = (df_spy["close"].iloc[-1] / df_spy["close"].iloc[-60]) - 1 if len(df_spy) >= 60 else 0.0
            realized_vol = df_spy["close"].pct_change().std() * math.sqrt(252)
        else:
            trend = "up"
            momentum_60d = 0.0
            realized_vol = 0.15

        indicators["trend"] = trend
        
        # Volatility
        if vix > 30 or realized_vol > 0.25:
            vol_regime = "high_vol"
        elif vix < 15:
            vol_regime = "low_vol"
        else:
            vol_regime = "normal_vol"
        indicators["volatility"] = vol_regime
        
        # Momentum
        momentum_label = (
            "strong_up" if momentum_60d > 0.10 else 
            "moderate_up" if momentum_60d > 0 else 
            "moderate_down" if momentum_60d > -0.10 else "strong_down"
        )
        indicators["momentum"] = momentum_label
        
        # Yield Curve
        curve_label = "inverted" if yield_curve < 0 else "flat" if yield_curve < 0.5 else "normal"
        indicators["yield_curve"] = curve_label
        
        # Classification
        if trend == "up" and vol_regime == "low_vol" and momentum_60d > 0:
            regime = "bull"
            confidence = 0.85
        elif trend == "down" and vix > 25 and momentum_60d < -0.10:
            regime = "bear"
            confidence = 0.80
        elif vol_regime == "high_vol" and abs(momentum_60d) < 0.05:
            regime = "volatile"
            confidence = 0.75
        elif trend == "up" and abs(momentum_60d) < 0.03:
            regime = "sideways"
            confidence = 0.65
        elif yield_curve < 0 and trend == "down":
            regime = "recession_risk"
            confidence = 0.70
        else:
            regime = "transitioning"
            confidence = 0.50
            
        return RegimeResult(
            regime=regime,
            confidence=confidence,
            indicators=indicators,
            momentum_60d=momentum_60d,
            vix=vix,
            yield_curve=yield_curve
        )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIGNAL DECAY DETECTOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SignalDecayDetector:
    def __init__(self, engine, redis_conn):
        self.engine = engine
        self.redis = redis_conn

    def check_all_signals(self) -> List[DecayResult]:
        """Check all active signals for decay."""
        signals = self._get_all_active_signals()
        results = []
        for sig in signals:
            res = self.check_signal(sig)
            results.append(res)
        return results

    def check_signal(self, signal: Dict[str, Any]) -> DecayResult:
        """Multi-factor decay detection for one signal."""
        signal_id = signal["id"]
        ticker = signal["ticker"]
        
        # Get live performance records
        perf = self._get_signal_live_performance(signal_id, days=60)
        
        if len(perf) < 20:
            return DecayResult(signal_id, ticker, status="insufficient_data")
            
        # 1. Hit Rate Decline
        recent_20 = perf[-20:]
        older_40 = perf[:-20] if len(perf) > 20 else []
        
        recent_hit_rate = np.mean([1 if r["hit"] else 0 for r in recent_20])
        older_hit_rate = np.mean([1 if r["hit"] else 0 for r in older_40]) if older_40 else recent_hit_rate
        
        hit_rate_decline = float(older_hit_rate - recent_hit_rate)
        
        # 2. Return Magnitude Decline
        recent_returns = [r["actual_return"] for r in recent_20]
        older_returns = [r["actual_return"] for r in older_40] if older_40 else recent_returns
        
        return_decline = float(np.mean(older_returns) - np.mean(recent_returns))
        
        # 3. Regime Mismatch
        current_regime_json = self.redis.get("monitoring:regime:current")
        current_regime = json.loads(current_regime_json).get("regime", "bull") if current_regime_json else "bull"
        signal_created_regime = signal.get("regime_at_creation", "bull")
        regime_mismatch = (current_regime != signal_created_regime)
        
        # 4. Decay Score
        decay_score = (
            max(0, hit_rate_decline) * 0.40 +
            max(0, return_decline * 10) * 0.30 +
            (0.30 if regime_mismatch else 0)
        )
        
        # Classification
        if decay_score > 0.6:
            status = "critical_decay"
            recommendation = "retire_signal"
        elif decay_score > 0.35:
            status = "moderate_decay"
            recommendation = "reduce_size"
        elif decay_score > 0.15:
            status = "early_decay"
            recommendation = "monitor_closely"
        else:
            status = "healthy"
            recommendation = "continue"
            
        # Update signal_live_performance DB
        self._update_latest_performance(signal_id, {
            "decay_detected": decay_score > 0.15,
            "decay_severity": status,
            "rolling_hit_rate_20": float(recent_hit_rate),
            "rolling_hit_rate_60": float(np.mean([1 if r["hit"] else 0 for r in perf]))
        })
        
        return DecayResult(
            signal_id=signal_id,
            ticker=ticker,
            status=status,
            decay_score=decay_score,
            hit_rate_recent=recent_hit_rate,
            hit_rate_older=older_hit_rate,
            regime_mismatch=regime_mismatch,
            recommendation=recommendation
        )

    def _get_all_active_signals(self) -> List[Dict[str, Any]]:
        """Mock: Fetch from signals DB."""
        # In real system, query trading_signals table
        return [] # Simplified

    def _get_signal_live_performance(self, signal_id: uuid.UUID, days: int) -> List[Dict[str, Any]]:
        """Fetch historical performance for a signal."""
        with self.engine.connect() as conn:
            query = text(f"""
                SELECT hit, actual_return, predicted_return 
                FROM signal_live_performance 
                WHERE signal_id = :sid 
                ORDER BY tracking_date DESC LIMIT :limit
            """)
            res = conn.execute(query, {"sid": signal_id, "limit": days}).fetchall()
            return [{"hit": r[0], "actual_return": r[1], "predicted_return": r[2]} for r in reversed(res)]

    def _update_latest_performance(self, signal_id: uuid.UUID, data: Dict[str, Any]):
        """Updates the latest performance record with decay info."""
        with Session(self.engine) as session:
            perf = session.query(SignalLivePerformance).filter_by(signal_id=signal_id).order_by(desc(SignalLivePerformance.tracking_date)).first()
            if perf:
                perf.decay_detected = data["decay_detected"]
                perf.decay_severity = data["decay_severity"]
                perf.rolling_hit_rate_20 = data["rolling_hit_rate_20"]
                perf.rolling_hit_rate_60 = data["rolling_hit_rate_60"]
                session.commit()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN AGENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RegimeDecayAgent:
    def __init__(self):
        self.engine = create_engine(settings.postgres_url)
        self.redis = redis.from_url(settings.redis_url, decode_responses=True)
        self.regime_detector = RegimeDetector(api_base_url="http://localhost:8001")
        self.decay_detector = SignalDecayDetector(self.engine, self.redis)

    async def run(self) -> RegimeDecayResult:
        """Main execution loop for regime and decay detection."""
        try:
            # 1. Detect current regime
            regime = await self.regime_detector.detect_current_regime()
            
            # 2. Check for regime change
            prev_json = self.redis.get("monitoring:regime:current")
            previous_regime = json.loads(prev_json).get("regime") if prev_json else None
            regime_changed = (regime.regime != previous_regime if previous_regime else False)
            
            # 3. Store regime detection
            self._write_regime_detection(regime, previous_regime, regime_changed)
            self.redis.set("monitoring:regime:current", json.dumps(regime.dict()), ex=3600)
            
            # 4. Check all signal decay
            decay_results = self.decay_detector.check_all_signals()
            
            critical_decay = [d for d in decay_results if d.status == "critical_decay"]
            moderate_decay = [d for d in decay_results if d.status == "moderate_decay"]
            healthy_signals = [d for d in decay_results if d.status == "healthy"]
            
            # 5. Generate feedback actions
            actions = self._generate_feedback_actions(regime, regime_changed, critical_decay)
            
            # 6. Publish findings
            if regime_changed:
                self.redis.publish("monitoring.regime.changed", json.dumps({
                    "old": previous_regime,
                    "new": regime.regime,
                    "confidence": regime.confidence
                }))
            
            if critical_decay:
                self.redis.publish("monitoring.signals.decay", json.dumps({
                    "critical_count": len(critical_decay),
                    "signal_ids": [str(d.signal_id) for d in critical_decay]
                }))
                
            return RegimeDecayResult(
                current_regime=regime.regime,
                regime_confidence=regime.confidence,
                regime_changed=regime_changed,
                healthy_signals=len(healthy_signals),
                decaying_signals=len(critical_decay) + len(moderate_decay),
                feedback_actions=actions
            )

        except Exception as e:
            logger.error(f"RegimeDecayAgent run failed: {e}")
            raise

    def _write_regime_detection(self, result: RegimeResult, prev: Optional[str], changed: bool):
        """Persists regime detection to database."""
        with Session(self.engine) as session:
            implications = self._get_regime_implications(result.regime)
            detection = RegimeDetection(
                detection_date=date.today(),
                regime=result.regime,
                confidence=result.confidence,
                duration_days=1, # TBD
                indicators_used=result.indicators,
                previous_regime=prev,
                regime_change=changed,
                implications=implications,
                created_at=datetime.now(timezone.utc)
            )
            session.add(detection)
            session.commit()

    def _generate_feedback_actions(self, regime: RegimeResult, changed: bool, critical_decay: List[DecayResult]) -> List[Dict[str, Any]]:
        """Creates feedback actions based on detections."""
        actions = []
        now = datetime.now(timezone.utc)
        
        with Session(self.engine) as session:
            # Regime change action
            if changed:
                action = FeedbackAction(
                    trigger_type="regime_change",
                    trigger_details={"new_regime": regime.regime},
                    action_type="change_regime_weights",
                    action_details={"implications": self._get_regime_implications(regime.regime)},
                    target_phase="phase2",
                    target_agent="research_coordinator",
                    status="pending",
                    created_at=now
                )
                session.add(action)
                actions.append({"type": "change_regime_weights", "regime": regime.regime})
            
            # Critical decay actions
            for decay in critical_decay:
                action = FeedbackAction(
                    trigger_type="signal_decay",
                    trigger_details={"signal_id": str(decay.signal_id), "score": decay.decay_score},
                    action_type="disable_strategy",
                    action_details={"reason": "critical_decay"},
                    target_phase="phase3",
                    target_agent="signal_scorer",
                    status="pending",
                    created_at=now
                )
                session.add(action)
                actions.append({"type": "disable_strategy", "signal_id": str(decay.signal_id)})
            
            # Systemic decay trigger
            if len(critical_decay) > 3:
                trigger = RetrainingTrigger(
                    trigger_date=date.today(),
                    trigger_reason="systemic_signal_decay",
                    affected_signals=json.dumps([str(d.signal_id) for d in critical_decay]),
                    metrics_at_trigger={"decay_count": len(critical_decay)},
                    retrain_type="full",
                    retrain_status="pending",
                    triggered_at=now
                )
                session.add(trigger)
                actions.append({"type": "retrain_research", "reason": "systemic_decay"})
                
            session.commit()
            
        return actions

    def _get_regime_implications(self, regime: str) -> Dict[str, Any]:
        """Rules for how to handle different regimes."""
        implications = {
            "bull": {"favored": ["momentum", "growth"], "avoid": ["defensive"], "multiplier": 1.2, "risk": "low"},
            "bear": {"favored": ["inverse", "volatility"], "avoid": ["long_only"], "multiplier": 0.5, "risk": "high"},
            "volatile": {"favored": ["mean_reversion"], "avoid": ["trend"], "multiplier": 0.7, "risk": "critical"},
            "sideways": {"favored": ["mean_reversion", "carry"], "avoid": ["breakout"], "multiplier": 1.0, "risk": "normal"},
            "recession_risk": {"favored": ["bonds", "defensive"], "avoid": ["growth"], "multiplier": 0.4, "risk": "high"},
            "transitioning": {"favored": [], "avoid": [], "multiplier": 0.8, "risk": "normal"}
        }
        return implications.get(regime, implications["transitioning"])

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PUBLIC INTERFACE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_current_regime(self) -> str:
        """Fetch latest regime from Redis."""
        data = self.redis.get("monitoring:regime:current")
        return json.loads(data).get("regime", "UNKNOWN") if data else "UNKNOWN"

    def get_regime_history(self, days=90) -> List[Dict[str, Any]]:
        """Fetch historical regimes from DB."""
        with self.engine.connect() as conn:
            query = text(f"SELECT detection_date, regime, confidence FROM regime_detections ORDER BY detection_date DESC LIMIT {days}")
            res = conn.execute(query).fetchall()
            return [{"date": str(r[0]), "regime": r[1], "confidence": r[2]} for r in res]

    def get_decaying_signals(self) -> List[Dict[str, Any]]:
        """Fetch signals with critical or moderate decay."""
        with self.engine.connect() as conn:
            query = text("SELECT signal_id, ticker, decay_severity FROM signal_live_performance WHERE decay_severity IN ('critical_decay', 'moderate_decay')")
            res = conn.execute(query).fetchall()
            return [{"id": str(r[0]), "ticker": r[1], "severity": r[2]} for r in res]
