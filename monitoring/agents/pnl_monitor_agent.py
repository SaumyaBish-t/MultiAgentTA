"""
Phase 8: P&L Monitor Agent
=========================
Tracks portfolio performance in real-time and provides attribution of returns to signals.
"""

import asyncio
import json
import uuid
import math
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
from monitoring.storage.monitoring_models import PerformanceMetrics, PnLAttribution, Alert

class PnLState(TypedDict):
    current_value: float
    previous_value: float
    daily_pnl: float
    daily_pnl_pct: float
    position_pnls: Dict[str, float]        # ticker → pnl
    benchmark_return: float                # SPY daily return
    excess_return: float
    attribution: List[Dict[str, Any]]      # per position attribution
    rolling_metrics: Dict[str, Any]        # 30d, 60d, 90d metrics
    drawdown: float
    peak_value: float
    alerts_triggered: List[str]
    error: Optional[str]

@dataclass
class PnLResult:
    portfolio_value: float
    daily_pnl: float
    daily_pnl_pct: float
    excess_return: float
    drawdown: float
    attribution: List[Dict[str, Any]]
    rolling_30d_sharpe: float
    rolling_30d_return: float
    alerts_triggered: List[str]

class PnLMonitor:
    def __init__(self):
        self.redis = redis.from_url(settings.redis_url, decode_responses=True)
        self.engine = create_engine(settings.postgres_url)
        self.api_base_url = "http://localhost:8001" # Phase 1 FastAPI
        self.risk_free_rate = 0.04 # Assume 4% annual

    async def calculate(self) -> PnLResult:
        """Runs the full P&L calculation pipeline."""
        state: PnLState = {
            "current_value": 0.0,
            "previous_value": 0.0,
            "daily_pnl": 0.0,
            "daily_pnl_pct": 0.0,
            "position_pnls": {},
            "benchmark_return": 0.0,
            "excess_return": 0.0,
            "attribution": [],
            "rolling_metrics": {},
            "drawdown": 0.0,
            "peak_value": 0.0,
            "alerts_triggered": [],
            "error": None
        }

        try:
            # Node 1: Fetch current portfolio
            state = await self._fetch_current_portfolio_node(state)
            
            # Node 2: Calculate daily P&L
            state = await self._calculate_daily_pnl_node(state)
            
            # Node 3: Calculate attribution
            state = await self._calculate_attribution_node(state)
            
            # Node 4: Calculate rolling metrics
            state = await self._calculate_rolling_metrics_node(state)
            
            # Node 5: Check performance alerts
            state = await self._check_performance_alerts_node(state)
            
            # Node 6: Store metrics
            state = await self._store_metrics_node(state)

            return PnLResult(
                portfolio_value=state["current_value"],
                daily_pnl=state["daily_pnl"],
                daily_pnl_pct=state["daily_pnl_pct"],
                excess_return=state["excess_return"],
                drawdown=state["drawdown"],
                attribution=state["attribution"],
                rolling_30d_sharpe=state["rolling_metrics"].get("30d", {}).get("sharpe", 0.0),
                rolling_30d_return=state["rolling_metrics"].get("30d", {}).get("return", 0.0),
                alerts_triggered=state["alerts_triggered"]
            )

        except Exception as e:
            logger.error(f"PnL Monitor failed: {e}")
            state["error"] = str(e)
            raise

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # NODES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_current_portfolio_node(self, state: PnLState) -> PnLState:
        """Gets current portfolio state and live prices."""
        # 1. Get from Redis
        portfolio_raw = self.redis.get("portfolio:current:state")
        if not portfolio_raw:
            logger.warning("No portfolio state found in Redis.")
            return state
            
        portfolio = json.loads(portfolio_raw)
        positions = portfolio.get("positions", [])
        tickers = [p["ticker"] for p in positions]
        
        if not tickers:
            state["current_value"] = portfolio.get("cash", 0.0)
            return state

        # 2. Get current prices from Phase 1 FastAPI
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{self.api_base_url}/prices/batch", params={"tickers": ",".join(tickers)})
                prices = response.json() if response.status_code == 200 else {}
            except Exception as e:
                logger.error(f"Failed to fetch batch prices: {e}")
                prices = {}

        # 3. Calculate current values
        total_value = portfolio.get("cash", 0.0)
        for pos in positions:
            ticker = pos["ticker"]
            shares = pos["shares"]
            entry_price = pos["entry_price"]
            current_price = prices.get(ticker, entry_price) # Fallback to entry if price fetch fails
            
            pos_value = shares * current_price
            unrealized_pnl = pos_value - (shares * entry_price)
            
            total_value += pos_value
            state["position_pnls"][ticker] = unrealized_pnl
            
            # Store current price back in state for attribution node
            pos["current_price"] = current_price

        state["current_value"] = total_value
        state["portfolio_raw"] = portfolio # Keep for later nodes
        return state

    async def _calculate_daily_pnl_node(self, state: PnLState) -> PnLState:
        """Calculates daily returns, benchmark comparison, and drawdown."""
        # 1. Get yesterday's portfolio value
        with Session(self.engine) as session:
            prev_metric = session.query(PerformanceMetrics).order_by(desc(PerformanceMetrics.metric_date)).first()
            if prev_metric:
                state["previous_value"] = prev_metric.portfolio_value
            else:
                state["previous_value"] = state["current_value"] # First run

        # 2. Daily P&L
        if state["previous_value"] > 0:
            state["daily_pnl"] = state["current_value"] - state["previous_value"]
            state["daily_pnl_pct"] = state["daily_pnl"] / state["previous_value"]
        
        # 3. Get SPY return
        async with httpx.AsyncClient() as client:
            try:
                # Assuming Phase 1 has a way to get SPY latest and prev close
                # Mocking logic: get SPY price
                spy_resp = await client.get(f"{self.api_base_url}/prices/SPY/latest")
                if spy_resp.status_code == 200:
                    spy_data = spy_resp.json()
                    spy_today = spy_data["price"]
                    spy_yesterday = spy_data.get("prev_close", spy_today) # Fallback
                    state["benchmark_return"] = (spy_today - spy_yesterday) / spy_yesterday if spy_yesterday else 0.0
            except Exception:
                state["benchmark_return"] = 0.0

        # 4. Excess return
        state["excess_return"] = state["daily_pnl_pct"] - state["benchmark_return"]

        # 5. Peak and Drawdown
        peak_str = self.redis.get("portfolio:peak:value")
        peak = float(peak_str) if peak_str else state["current_value"]
        
        if state["current_value"] > peak:
            peak = state["current_value"]
            self.redis.set("portfolio:peak:value", str(peak))
            
        state["peak_value"] = peak
        state["drawdown"] = (state["current_value"] - peak) / peak if peak else 0.0
        
        self.redis.set("portfolio:drawdown:current", str(state["drawdown"]))
        
        return state

    async def _calculate_attribution_node(self, state: PnLState) -> PnLState:
        """Calculates contribution of each position to today's return."""
        portfolio = state.get("portfolio_raw", {})
        positions = portfolio.get("positions", [])
        total_value_yesterday = state["previous_value"] if state["previous_value"] > 0 else state["current_value"]
        
        attribution = []
        for pos in positions:
            ticker = pos["ticker"]
            current_price = pos.get("current_price", 0.0)
            # Need prev_close for day return. If not available, use entry or mock
            # In a real system, Phase 1 price API would return prev_close
            prev_close = pos.get("prev_close", pos["entry_price"]) 
            
            shares = pos["shares"]
            pos_value_today = shares * current_price
            
            position_weight = pos_value_today / state["current_value"] if state["current_value"] > 0 else 0.0
            position_return = (current_price - prev_close) / prev_close if prev_close else 0.0
            
            # Contribution to TOTAL portfolio return
            # (Weight at start of period * Return of period)
            # Using current weight as proxy if we don't have start weights perfectly
            contribution = position_weight * position_return
            
            signal_id = self._get_signal_for_position(ticker)
            
            attribution.append({
                "ticker": ticker,
                "signal_id": signal_id,
                "strategy_type": self._get_strategy_type(signal_id),
                "contribution_pct": float(contribution),
                "position_return_pct": float(position_return),
                "position_weight": float(position_weight),
                "alpha_contribution": float(contribution - (position_weight * state["benchmark_return"]))
            })

        # Sort by contribution (best to worst)
        attribution.sort(key=lambda x: x['contribution_pct'], reverse=True)
        state["attribution"] = attribution
        return state

    async def _calculate_rolling_metrics_node(self, state: PnLState) -> PnLState:
        """Calculates performance over 30, 60, 90, 252 day windows."""
        windows = [30, 60, 90, 252]
        
        for window in windows:
            returns = self._get_daily_returns_last_n_days(window)
            if returns.empty:
                continue
                
            cum_ret = (1 + returns).prod() - 1
            vol = returns.std() * math.sqrt(252)
            
            # Annualize cum_ret
            # annual_ret = (1 + cum_ret) ** (252 / len(returns)) - 1
            days = len(returns)
            annual_ret = (1 + cum_ret) ** (252 / days) - 1 if days > 0 else 0.0
            
            # Sharpe (excess return / vol)
            excess_annual = annual_ret - self.risk_free_rate
            sharpe = excess_annual / vol if vol > 0 else 0.0
            
            # Sortino (downside vol)
            downside_returns = returns[returns < 0]
            downside_vol = downside_returns.std() * math.sqrt(252)
            sortino = excess_annual / downside_vol if downside_vol > 0 else 0.0
            
            # Max Drawdown
            cum_vals = (1 + returns).cumprod()
            peaks = cum_vals.cummax()
            dd = (cum_vals - peaks) / peaks
            max_dd = dd.min()

            state["rolling_metrics"][f"{window}d"] = {
                "return": float(cum_ret),
                "annualized": float(annual_ret),
                "sharpe": float(sharpe),
                "sortino": float(sortino),
                "max_drawdown": float(max_dd),
                "volatility": float(vol),
                "win_rate": float((returns > 0).mean()),
                "best_day": float(returns.max()),
                "worst_day": float(returns.min())
            }

        # Alpha/Beta calculation for 252d (if possible)
        if "252d" in state["rolling_metrics"]:
            portfolio_returns = self._get_daily_returns_last_n_days(252)
            spy_returns = self._get_spy_returns(252)
            
            # Align indices
            combined = pd.concat([portfolio_returns, spy_returns], axis=1).dropna()
            if not combined.empty:
                cov = combined.cov().iloc[0, 1]
                mkt_var = combined.iloc[:, 1].var()
                beta = cov / mkt_var if mkt_var > 0 else 1.0
                
                annual_ret = state["rolling_metrics"]["252d"]["annualized"]
                market_annual = (1 + spy_returns.mean()) ** 252 - 1
                alpha = annual_ret - (self.risk_free_rate + beta * (market_annual - self.risk_free_rate))
                
                state["rolling_metrics"]["beta"] = float(beta)
                state["rolling_metrics"]["alpha"] = float(alpha)

        return state

    async def _check_performance_alerts_node(self, state: PnLState) -> PnLState:
        """Triggers alerts based on performance thresholds."""
        alerts = []
        
        if state["daily_pnl_pct"] < -0.02:
            alerts.append("LARGE_DAILY_LOSS")
            await self._trigger_alert("LARGE_DAILY_LOSS", "warning", {"pnl": state["daily_pnl_pct"]})
            
        if state["drawdown"] < -0.07:
            alerts.append("SIGNIFICANT_DRAWDOWN")
            await self._trigger_alert("SIGNIFICANT_DRAWDOWN", "warning", {"drawdown": state["drawdown"]})
            
        m30 = state["rolling_metrics"].get("30d", {})
        if m30.get("sharpe", 0) < 0:
            alerts.append("NEGATIVE_30D_SHARPE")
            await self._trigger_alert("NEGATIVE_30D_SHARPE", "warning", {"sharpe": m30.get("sharpe")})
            
        if state["excess_return"] < -0.03:
            alerts.append("UNDERPERFORMING_BENCHMARK")
            await self._trigger_alert("UNDERPERFORMING_BENCHMARK", "info", {"excess": state["excess_return"]})
            
        if state["daily_pnl_pct"] > 0.03:
            alerts.append("STRONG_DAILY_PERFORMANCE")
            await self._trigger_alert("STRONG_DAILY_PERFORMANCE", "info", {"pnl": state["daily_pnl_pct"]})

        state["alerts_triggered"] = alerts
        return state

    async def _store_metrics_node(self, state: PnLState) -> PnLState:
        """Persists metrics to database and updates Redis."""
        now = datetime.now(timezone.utc)
        
        # 1. Write performance_metrics record
        with Session(self.engine) as session:
            m30 = state["rolling_metrics"].get("30d", {})
            m252 = state["rolling_metrics"].get("252d", {})
            
            metrics = PerformanceMetrics(
                metric_date=date.today(),
                metric_type="daily",
                portfolio_value=state["current_value"],
                total_return=state["daily_pnl_pct"], # For daily record, store daily return
                annualized_return=m30.get("annualized", 0.0),
                sharpe_ratio=m30.get("sharpe", 0.0),
                sortino_ratio=m30.get("sortino", 0.0),
                calmar_ratio=m30.get("annualized", 0.0) / abs(state["drawdown"]) if state["drawdown"] != 0 else 0.0,
                max_drawdown=state["drawdown"],
                volatility=m30.get("volatility", 0.0),
                beta_to_spy=state["rolling_metrics"].get("beta", 1.0),
                alpha=state["rolling_metrics"].get("alpha", 0.0),
                information_ratio=0.0, # TBD
                benchmark_return=state["benchmark_return"],
                excess_return=state["excess_return"],
                win_days=0, # Aggregated later
                loss_days=0,
                win_day_rate=0.0,
                avg_win_day=0.0,
                avg_loss_day=0.0,
                best_day=state["daily_pnl_pct"],
                worst_day=state["daily_pnl_pct"],
                created_at=now
            )
            session.add(metrics)
            
            # 2. Write pnl_attribution records
            for attr in state["attribution"]:
                pnl_attr = PnLAttribution(
                    attribution_date=date.today(),
                    ticker=attr["ticker"],
                    signal_id=attr["signal_id"],
                    strategy_type=attr["strategy_type"],
                    contribution_pct=attr["contribution_pct"],
                    position_return_pct=attr["position_return_pct"],
                    position_weight=attr["position_weight"],
                    alpha_contribution=attr["alpha_contribution"],
                    holding_days=0, # TBD
                    entry_date=date.today(), # TBD
                    created_at=now
                )
                session.add(pnl_attr)
                
            session.commit()

        # 3. Update Redis
        latest_data = {
            "value": state["current_value"],
            "daily_pnl": state["daily_pnl"],
            "daily_pnl_pct": state["daily_pnl_pct"],
            "drawdown": state["drawdown"],
            "excess_return": state["excess_return"],
            "timestamp": now.isoformat()
        }
        self.redis.set("monitoring:pnl:latest", json.dumps(latest_data), ex=300)
        
        # 4. Publish update
        self.redis.publish("monitoring.pnl.updated", json.dumps({
            "value": state["current_value"],
            "pnl_pct": state["daily_pnl_pct"],
            "drawdown": state["drawdown"],
            "attribution_count": len(state["attribution"])
        }))
        
        return state

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # HELPERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _get_signal_for_position(self, ticker: str) -> Optional[uuid.UUID]:
        """Fetches active signal ID for a ticker from Redis."""
        # Check Redis "signals:active" or similar
        return None # Simplified for now

    def _get_strategy_type(self, signal_id: Optional[uuid.UUID]) -> str:
        """Determines strategy type based on signal."""
        return "momentum" # Default

    def _get_daily_returns_last_n_days(self, n: int) -> pd.Series:
        """Fetches historical daily returns from performance_metrics table."""
        with self.engine.connect() as conn:
            query = text(f"""
                SELECT metric_date, total_return 
                FROM performance_metrics 
                WHERE metric_type = 'daily'
                ORDER BY metric_date DESC LIMIT {n}
            """)
            df = pd.read_sql(query, conn)
            if df.empty:
                return pd.Series()
            return df.set_index("metric_date")["total_return"].sort_index()

    def _get_spy_returns(self, n: int) -> pd.Series:
        """Fetches historical SPY returns."""
        # In a real system, this would query the market_data DB
        # For now, return random returns or zeros
        dates = pd.date_range(end=date.today(), periods=n)
        return pd.Series(np.random.normal(0.0001, 0.01, n), index=dates)

    async def _trigger_alert(self, alert_type: str, severity: str, data: Dict[str, Any]):
        """Saves alert to DB and publishes to Redis."""
        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            alert = Alert(
                alert_type=alert_type,
                severity=severity,
                title=f"Performance Alert: {alert_type}",
                message=f"Threshold reached: {data}",
                data=data,
                channel="redis/dashboard",
                created_at=now
            )
            session.add(alert)
            session.commit()
        
        self.redis.publish("monitoring.alerts", json.dumps({
            "type": alert_type,
            "severity": severity,
            "data": data,
            "timestamp": now.isoformat()
        }))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PUBLIC INTERFACE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def get_attribution(self, days=30) -> List[Dict[str, Any]]:
        """Fetches historical attribution data."""
        with self.engine.connect() as conn:
            query = text(f"""
                SELECT ticker, SUM(contribution_pct) as total_contribution
                FROM pnl_attribution
                WHERE attribution_date >= CURRENT_DATE - INTERVAL '{days} days'
                GROUP BY ticker
                ORDER BY total_contribution DESC
            """)
            res = conn.execute(query).fetchall()
            return [{"ticker": r[0], "contribution": r[1]} for r in res]

    def get_rolling_metrics(self) -> Dict[str, Any]:
        """Fetches latest metrics from Redis."""
        data = self.redis.get("monitoring:pnl:latest")
        return json.loads(data) if data else {}

    def get_best_signals(self) -> List[Dict[str, Any]]:
        """Identifies best performing signals."""
        return [] # TBD

    def get_worst_signals(self) -> List[Dict[str, Any]]:
        """Identifies worst performing signals."""
        return [] # TBD

    def get_daily_returns(self, days=252) -> pd.Series:
        """Fetches historical returns."""
        return self._get_daily_returns_last_n_days(days)
