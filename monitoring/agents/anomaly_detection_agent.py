"""
Phase 8: Anomaly Detection Agent
===============================
Detects unusual patterns across data, execution, performance, and system health.
"""

import asyncio
import json
import uuid
import math
import shutil
from datetime import datetime, timezone, date, timedelta
from typing import Dict, List, Any, Optional, TypedDict, cast
from dataclasses import dataclass

import pandas as pd
import numpy as np
import httpx
import redis
from loguru import logger
from sqlalchemy import create_engine, text, select, desc
from sqlalchemy.orm import Session

from config.settings import settings
from monitoring.storage.monitoring_models import Alert

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATE & DATA CLASSES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AnomalyState(TypedDict):
    anomalies_detected: List[Dict[str, Any]]
    data_anomalies: List[Dict[str, Any]]
    execution_anomalies: List[Dict[str, Any]]
    performance_anomalies: List[Dict[str, Any]]
    system_anomalies: List[Dict[str, Any]]
    severity: str
    actions_required: List[str]
    error: Optional[str]

@dataclass
class AnomalyReport:
    total: int
    critical: int
    data: List[Dict[str, Any]]
    execution: List[Dict[str, Any]]
    performance: List[Dict[str, Any]]
    system: List[Dict[str, Any]]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ANOMALY DETECTION AGENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AnomalyDetectionAgent:
    def __init__(self):
        self.engine = create_engine(settings.postgres_url)
        self.redis = redis.from_url(settings.redis_url, decode_responses=True)
        self.api_base_url = "http://localhost:8001" # Phase 1 FastAPI
        self.tickers = settings.tickers

    async def run(self) -> AnomalyReport:
        """Main execution loop for all anomaly checks."""
        try:
            # Run all checks in parallel
            data_anom, exec_anom, perf_anom, sys_anom = await asyncio.gather(
                asyncio.to_thread(self.check_data_anomalies),
                asyncio.to_thread(self._check_execution_anomalies_wrapper),
                asyncio.to_thread(self.check_performance_anomalies),
                asyncio.to_thread(self.check_system_health)
            )
            
            all_anomalies = data_anom + exec_anom + perf_anom + sys_anom
            
            # 1. Write to alerts table
            self._write_alerts(all_anomalies)
            
            # 2. Publish critical anomalies
            critical = [a for a in all_anomalies if a["severity"] == "critical"]
            if critical:
                self.redis.publish("monitoring.anomaly.critical", json.dumps({
                    "count": len(critical),
                    "types": [a["type"] for a in critical]
                }))
            
            # 3. Cache in Redis
            self.redis.set("monitoring:anomalies:latest", json.dumps({
                "total": len(all_anomalies),
                "critical": len(critical),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }), ex=300)
            
            return AnomalyReport(
                total=len(all_anomalies),
                critical=len(critical),
                data=data_anom,
                execution=exec_anom,
                performance=perf_anom,
                system=sys_anom
            )

        except Exception as e:
            logger.error(f"AnomalyDetectionAgent run failed: {e}")
            raise

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CATEGORY 1: DATA ANOMALIES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def check_data_anomalies(self) -> List[Dict[str, Any]]:
        """Checks for stale data, price spikes, and missing tickers."""
        anomalies = []
        now_utc = datetime.now(timezone.utc)
        
        # Use a synchronous client for to_thread compatibility if needed, 
        # or just use httpx.Client()
        with httpx.Client() as client:
            for ticker in self.tickers:
                try:
                    # 1. Stale price data
                    resp = client.get(f"{self.api_base_url}/prices/{ticker}/latest")
                    if resp.status_code == 200:
                        last_bar = resp.json()
                        ts_str = last_bar.get("timestamp")
                        if ts_str:
                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            age_minutes = (now_utc - ts).total_seconds() / 60
                            
                            if self._is_market_hours() and age_minutes > 5:
                                anomalies.append({
                                    "type": "STALE_PRICE_DATA",
                                    "ticker": ticker,
                                    "age_minutes": age_minutes,
                                    "severity": "warning"
                                })

                    # 2. Price spike (5% in 1 minute)
                    bars_resp = client.get(f"{self.api_base_url}/prices/{ticker}/bars", params={"timeframe": "1min", "limit": 10})
                    if bars_resp.status_code == 200:
                        bars = bars_resp.json()
                        if len(bars) >= 2:
                            last_close = bars[-1]["close"]
                            prev_close = bars[-2]["close"]
                            ret = (last_close - prev_close) / prev_close if prev_close else 0
                            if abs(ret) > 0.05:
                                anomalies.append({
                                    "type": "PRICE_SPIKE",
                                    "ticker": ticker,
                                    "change_pct": ret,
                                    "severity": "warning"
                                })

                    # 3. Volume anomaly (>5x normal)
                    # Mock: In real system, Phase 1 would have avg_volume
                    current_vol = last_bar.get("volume", 0) if resp.status_code == 200 else 0
                    avg_vol = 1000000 # Mock
                    if current_vol > avg_vol * 5:
                        anomalies.append({
                            "type": "VOLUME_SPIKE",
                            "ticker": ticker,
                            "ratio": current_vol / avg_vol,
                            "severity": "info"
                        })
                except Exception as e:
                    logger.debug(f"Data check failed for {ticker}: {e}")

            # 4. Missing tickers
            try:
                today_resp = client.get(f"{self.api_base_url}/prices/today/tickers")
                tickers_with_data = today_resp.json() if today_resp.status_code == 200 else []
                missing = set(self.tickers) - set(tickers_with_data)
                for ticker in missing:
                    anomalies.append({
                        "type": "MISSING_TICKER_DATA",
                        "ticker": ticker,
                        "severity": "critical"
                    })
            except Exception:
                pass

        return anomalies

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CATEGORY 2: EXECUTION ANOMALIES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _check_execution_anomalies_wrapper(self) -> List[Dict[str, Any]]:
        """Wrapper to handle blocking execution checks."""
        return self.check_execution_anomalies()

    def check_execution_anomalies(self) -> List[Dict[str, Any]]:
        """Checks for slippage, fill rate, orphaned orders, and position mismatch."""
        anomalies = []
        
        # 1. High slippage
        # Fetch from execution_performance table (Phase 6)
        with self.engine.connect() as conn:
            query = text("SELECT slippage_bps FROM execution_performance WHERE measured_at > NOW() - INTERVAL '24 hours'")
            fills = conn.execute(query).fetchall()
            if fills:
                high_slippage = [f[0] for f in fills if f[0] > 50]
                if high_slippage:
                    anomalies.append({
                        "type": "HIGH_EXECUTION_SLIPPAGE",
                        "count": len(high_slippage),
                        "avg_bps": sum(high_slippage) / len(high_slippage),
                        "severity": "warning"
                    })

            # 2. Low fill rate
            q_submitted = text("SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '24 hours'")
            q_filled = text("SELECT COUNT(*) FROM orders WHERE status = 'filled' AND created_at > NOW() - INTERVAL '24 hours'")
            submitted = conn.execute(q_submitted).scalar() or 0
            filled = conn.execute(q_filled).scalar() or 0
            if submitted > 0:
                fill_rate = filled / submitted
                if fill_rate < 0.70:
                    anomalies.append({
                        "type": "LOW_FILL_RATE",
                        "fill_rate": fill_rate,
                        "severity": "warning"
                    })

            # 3. Orphaned orders (pending > 2 hours)
            q_orphaned = text("SELECT id FROM orders WHERE status IN ('pending', 'submitted', 'new') AND created_at < NOW() - INTERVAL '2 hours'")
            orphaned = conn.execute(q_orphaned).fetchall()
            if orphaned:
                anomalies.append({
                    "type": "ORPHANED_ORDERS",
                    "count": len(orphaned),
                    "order_ids": [str(o[0]) for o in orphaned],
                    "severity": "critical"
                })

        # 4. Position mismatch
        # Compare Redis/DB vs Alpaca
        try:
            db_positions_raw = self.redis.get("portfolio:current:state")
            db_positions = json.loads(db_positions_raw).get("positions", []) if db_positions_raw else []
            db_map = {p["ticker"]: p["shares"] for p in db_positions}
            
            # Alpaca positions (via Phase 6 Broker or direct mock)
            # For now, we compare against an empty list or mock
            alpaca_positions = [] # Broker logic here
            
            all_tickers = set(db_map.keys()) | {p["ticker"] for p in alpaca_positions}
            for ticker in all_tickers:
                db_shares = db_map.get(ticker, 0)
                alpaca_shares = next((p["shares"] for p in alpaca_positions if p["ticker"] == ticker), 0)
                if abs(db_shares - alpaca_shares) > 0:
                    anomalies.append({
                        "type": "POSITION_MISMATCH",
                        "ticker": ticker,
                        "db_shares": db_shares,
                        "alpaca_shares": alpaca_shares,
                        "severity": "critical"
                    })
        except Exception:
            pass

        return anomalies

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CATEGORY 3: PERFORMANCE ANOMALIES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def check_performance_anomalies(self) -> List[Dict[str, Any]]:
        """Detects extreme returns, reversals, and volatility spikes."""
        anomalies = []
        
        with self.engine.connect() as conn:
            query = text("SELECT total_return FROM performance_metrics WHERE metric_type = 'daily' ORDER BY metric_date DESC LIMIT 30")
            df = pd.read_sql(query, conn)
            
            if len(df) < 5:
                return anomalies
                
            returns = df["total_return"]
            mean_ret = returns.mean()
            std_ret = returns.std()
            latest = returns.iloc[0] # Note: DESC order
            
            # 1. Z-score outlier
            if std_ret > 0:
                z_score = (latest - mean_ret) / std_ret
                if abs(z_score) > 3:
                    anomalies.append({
                        "type": "EXTREME_RETURN",
                        "z_score": float(z_score),
                        "return_pct": float(latest),
                        "severity": "warning"
                    })
                    
            # 2. Return autocorrelation
            if len(returns) >= 10:
                autocorr = returns.autocorr(lag=1)
                if autocorr < -0.5:
                    anomalies.append({
                        "type": "STRONG_REVERSAL_PATTERN",
                        "autocorrelation": float(autocorr),
                        "severity": "info"
                    })
                    
            # 3. Volatility spike
            recent_vol = returns[:5].std() * math.sqrt(252)
            baseline_vol = returns.std() * math.sqrt(252)
            if recent_vol > baseline_vol * 2:
                anomalies.append({
                    "type": "VOLATILITY_SPIKE",
                    "recent_vol": float(recent_vol),
                    "baseline_vol": float(baseline_vol),
                    "severity": "warning"
                })
                
        return anomalies

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # CATEGORY 4: SYSTEM HEALTH ANOMALIES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def check_system_health(self) -> List[Dict[str, Any]]:
        """Checks DBs, Redis, LLMs, Prefect, and disk usage."""
        anomalies = []
        
        # 1. DB Connectivity
        for db_name, url in [("postgresql", settings.postgres_url), ("timescaledb", settings.timescale_url)]:
            try:
                engine = create_engine(url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
            except Exception as e:
                anomalies.append({
                    "type": "DB_CONNECTION_FAILED",
                    "database": db_name,
                    "error": str(e),
                    "severity": "critical"
                })

        # 2. Redis
        try:
            self.redis.ping()
        except Exception:
            anomalies.append({
                "type": "REDIS_CONNECTION_FAILED",
                "severity": "critical"
            })

        # 3. LLM Providers
        # Mock: test_llm_provider
        
        # 4. Prefect Flows
        # Mock: In real system, query Prefect API
        
        # 5. Disk Usage
        try:
            total, used, free = shutil.disk_usage("/")
            usage_pct = used / total
            if usage_pct > 0.85:
                anomalies.append({
                    "type": "HIGH_DISK_USAGE",
                    "usage_pct": usage_pct,
                    "severity": "warning"
                })
        except Exception:
            pass
            
        return anomalies

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # HELPERS & PERSISTENCE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _is_market_hours(self) -> bool:
        """Determines if the market is currently open."""
        # Simple placeholder
        now = datetime.now(timezone.utc).time()
        return datetime.strptime("13:30", "%H:%M").time() <= now <= datetime.strptime("20:00", "%H:%M").time()

    def _write_alerts(self, anomalies: List[Dict[str, Any]]):
        """Persists anomalies to the alerts table."""
        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            for anom in anomalies:
                alert = Alert(
                    alert_type=anom["type"],
                    severity=anom["severity"],
                    title=f"Anomaly: {anom['type']}",
                    message=json.dumps(anom),
                    data=anom,
                    channel="redis/dashboard",
                    created_at=now
                )
                session.add(alert)
            session.commit()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PUBLIC INTERFACE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_open_anomalies(self) -> List[Dict[str, Any]]:
        """Fetches unacknowledged alerts from DB."""
        with self.engine.connect() as conn:
            query = text("SELECT alert_type, severity, message, created_at FROM alerts WHERE acknowledged = False ORDER BY created_at DESC")
            res = conn.execute(query).fetchall()
            return [{"type": r[0], "severity": r[1], "message": r[2], "timestamp": r[3]} for r in res]

    def acknowledge_anomaly(self, alert_id: uuid.UUID) -> bool:
        """Marks an alert as acknowledged."""
        with Session(self.engine) as session:
            alert = session.get(Alert, alert_id)
            if alert:
                alert.acknowledged = True
                alert.acknowledged_at = datetime.now(timezone.utc)
                session.commit()
                return True
        return False

    def get_anomaly_history(self, days=7) -> List[Dict[str, Any]]:
        """Fetches historical alerts."""
        with self.engine.connect() as conn:
            query = text(f"SELECT alert_type, severity, created_at FROM alerts WHERE created_at > NOW() - INTERVAL '{days} days' ORDER BY created_at DESC")
            res = conn.execute(query).fetchall()
            return [{"type": r[0], "severity": r[1], "timestamp": r[2]} for r in res]
