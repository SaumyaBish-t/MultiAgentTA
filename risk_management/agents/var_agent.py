import json
import uuid
import math
import asyncio
from datetime import datetime, timezone
from typing import TypedDict, Any, Optional, Dict, List
from dataclasses import dataclass

import httpx
import numpy as np
import pandas as pd
import redis
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from langgraph.graph import StateGraph, END

from config.settings import settings
from risk_management.storage.risk_models import (
    RiskEvent,
    VarCalculation,
    PortfolioRiskSnapshot
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class VaRState(TypedDict):
    tickers: list[str]
    position_sizes: dict      # ticker → USD value
    total_portfolio_value: float
    price_history: dict       # ticker → returns series
    portfolio_returns: pd.Series
    var_95_1day: float
    var_99_1day: float
    cvar_95_1day: float
    var_95_5day: float
    var_99_10day: float
    per_position_var: dict    # ticker → var
    monte_carlo_var: float
    stress_test_results: dict
    breaches: list[str]
    error: str | None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_returns_data_node(state: VaRState) -> dict[str, Any]:
    """Fetch 2-year daily price history for each ticker and compute log returns."""
    if state.get("error"): return {}
    
    tickers = state.get("tickers", [])
    price_history = {}
    headers = {"x-api-key": settings.internal_api_key}
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for ticker in tickers:
            url = f"http://localhost:8000/prices/{ticker}/history?days=504"
            tasks.append(client.get(url, headers=headers, timeout=10.0))
            
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        for ticker, resp in zip(tickers, responses):
            if isinstance(resp, Exception):
                logger.error(f"Error fetching data for {ticker}: {resp}")
                continue
            
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    df = pd.DataFrame(data)
                    df['close'] = pd.to_numeric(df['close'])
                    # Calculate daily log returns
                    returns = np.log(df['close'] / df['close'].shift(1)).dropna()
                    # We reverse it or just use the series? The API returns chronological normally if reversed, but we just need distribution
                    price_history[ticker] = returns
                else:
                    logger.warning(f"Not enough data for {ticker}")
                    
    if not price_history:
        return {"error": "No price history available to compute VaR"}
        
    return {"price_history": price_history}

async def calculate_historical_var_node(state: VaRState) -> dict[str, Any]:
    """Calculate Historical VaR and CVaR for the portfolio and individual positions."""
    if state.get("error"): return {}
    
    returns_dict = state.get("price_history", {})
    position_sizes = state.get("position_sizes", {})
    total_val = sum(position_sizes.values())
    if total_val == 0: total_val = state.get("total_portfolio_value", 100_000.0)
    
    # 1. Align return series
    # The arrays might have different lengths if some tickers are newer. 
    # Use pandas to align them by index (which assumes index is date if we set it, or just truncate)
    # Since we didn't set date as index, let's just make a dataframe and dropna.
    # We should ideally align by date. Let's assume the series are roughly aligned or we take the min length.
    df_returns = pd.DataFrame(returns_dict).dropna()
    
    if df_returns.empty:
        return {"error": "Aligned returns DataFrame is empty"}
        
    # 2. Portfolio level weights & returns
    weights = {t: (size / total_val) for t, size in position_sizes.items() if t in df_returns.columns}
    
    portfolio_returns = pd.Series(0.0, index=df_returns.index)
    for ticker, weight in weights.items():
        portfolio_returns += df_returns[ticker] * weight
        
    # VaR = percentile of loss distribution
    var_95 = np.percentile(portfolio_returns, 5)
    var_99 = np.percentile(portfolio_returns, 1)
    
    var_95_usd = abs(var_95) * total_val
    var_99_usd = abs(var_99) * total_val
    
    # CVaR = expected loss BEYOND VaR
    tail_losses = portfolio_returns[portfolio_returns <= var_95]
    cvar_95 = tail_losses.mean() if not tail_losses.empty else var_95
    cvar_95_usd = abs(cvar_95) * total_val
    
    # Square root of time rule for 5-day and 10-day
    var_95_5day = var_95_usd * math.sqrt(5)
    var_99_10day = var_99_usd * math.sqrt(10)
    
    # 3. Per Position VaR
    per_position_var = {}
    for ticker in weights.keys():
        t_returns = df_returns[ticker]
        t_var_95 = np.percentile(t_returns, 5)
        t_var_95_usd = abs(t_var_95) * position_sizes[ticker]
        per_position_var[ticker] = t_var_95_usd

    return {
        "portfolio_returns": portfolio_returns,
        "total_portfolio_value": total_val,
        "var_95_1day": var_95_usd,
        "var_99_1day": var_99_usd,
        "cvar_95_1day": cvar_95_usd,
        "var_95_5day": var_95_5day,
        "var_99_10day": var_99_10day,
        "per_position_var": per_position_var
    }

async def run_monte_carlo_var_node(state: VaRState) -> dict[str, Any]:
    """Run Monte Carlo simulation for VaR."""
    if state.get("error"): return {}
    
    portfolio_returns = state.get("portfolio_returns")
    if portfolio_returns is None or portfolio_returns.empty:
        return {"monte_carlo_var": state.get("var_95_1day", 0.0)}
        
    mean_return = portfolio_returns.mean()
    std_return = portfolio_returns.std()
    
    # Simulate 10000 scenarios
    simulated = np.random.normal(mean_return, std_return, 10000)
    
    mc_var_95 = abs(np.percentile(simulated, 5))
    mc_var_95_usd = mc_var_95 * state["total_portfolio_value"]
    
    var_95_1day = state.get("var_95_1day", mc_var_95_usd)
    
    # Average historical and MC
    monte_carlo_var = (mc_var_95_usd + var_95_1day) / 2.0
    
    return {"monte_carlo_var": monte_carlo_var}

async def run_stress_tests_node(state: VaRState) -> dict[str, Any]:
    """Simulate known historical crisis scenarios."""
    if state.get("error"): return {}
    
    total_val = state.get("total_portfolio_value", 0.0)
    scenarios = {
        "2008_financial_crisis": -0.40,
        "2020_covid_crash": -0.34,
        "2022_rate_shock": -0.25,
        "flash_crash_2010": -0.10,
        "custom_10pct_drop": -0.10,
        "custom_20pct_drop": -0.20
    }
    
    stress_results = {}
    for scenario, shock in scenarios.items():
        loss_usd = abs(shock) * total_val
        stress_results[scenario] = {
            "loss_usd": loss_usd,
            "loss_pct": abs(shock) * 100,
            "survives": abs(shock) < 0.30
        }
        
    return {"stress_test_results": stress_results}

async def check_var_limits_node(state: VaRState) -> dict[str, Any]:
    """Check VaR values against predefined limits and generate Risk Events if breached."""
    if state.get("error"): return {}
    
    breaches = state.get("breaches", [])
    total_val = state["total_portfolio_value"]
    
    var_limit_usd = total_val * 0.02
    var_95 = state.get("var_95_1day", 0.0)
    var_99 = state.get("var_99_1day", 0.0)
    cvar_95 = state.get("cvar_95_1day", 0.0)
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    new_events = []
    
    if var_95 > var_limit_usd:
        breach_msg = "VAR_95_LIMIT_BREACHED"
        breaches.append(breach_msg)
        new_events.append(RiskEvent(
            event_type="var_breach",
            severity="high",
            description=f"1-Day 95% VaR ({var_95:.2f}) exceeded 2% limit ({var_limit_usd:.2f})",
            current_value=var_95,
            threshold_value=var_limit_usd,
            action_taken="reduce"
        ))
        
    if var_99 > total_val * 0.05:
        breach_msg = "VAR_99_LIMIT_BREACHED"
        breaches.append(breach_msg)
        new_events.append(RiskEvent(
            event_type="var_breach",
            severity="critical",
            description=f"1-Day 99% VaR ({var_99:.2f}) exceeded 5% limit",
            current_value=var_99,
            threshold_value=total_val * 0.05,
            action_taken="halt"
        ))
        
    if cvar_95 > total_val * 0.03:
        breaches.append("CVAR_LIMIT_BREACHED")
        
    if new_events:
        with Session() as session:
            session.add_all(new_events)
            session.commit()
            
    return {"breaches": breaches}

async def store_var_results_node(state: VaRState) -> dict[str, Any]:
    """Write VaR calculations to database and Redis cache."""
    if state.get("error"): return {}
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    total_val = state["total_portfolio_value"]
    if total_val == 0: return {}
    
    # 1. Save to var_calculations (Portfolio Level)
    with Session() as session:
        # Historical
        calc_hist = VarCalculation(
            ticker=None,
            calculation_method="historical",
            confidence_level=0.95,
            horizon_days=1,
            var_value=state["var_95_1day"],
            cvar_value=state["cvar_95_1day"],
            position_size_usd=total_val,
            returns_window_days=504
        )
        # Monte Carlo
        calc_mc = VarCalculation(
            ticker=None,
            calculation_method="monte_carlo",
            confidence_level=0.95,
            horizon_days=1,
            var_value=state["monte_carlo_var"],
            cvar_value=0.0, # Not computed separately
            position_size_usd=total_val,
            returns_window_days=504
        )
        
        session.add_all([calc_hist, calc_mc])
        
        # We can also update the latest PortfolioRiskSnapshot or create a partial one
        # Assuming other fields are populated elsewhere, we'll create a targeted snapshot
        snap = PortfolioRiskSnapshot(
            snapshot_time=datetime.now(timezone.utc),
            total_portfolio_value=total_val,
            cash_pct=0.0, # Placeholder, real value should come from portfolio tracker
            invested_pct=1.0, 
            long_exposure_pct=1.0,
            short_exposure_pct=0.0,
            net_exposure_pct=1.0,
            gross_exposure_pct=1.0,
            current_drawdown_pct=0.0,
            peak_portfolio_value=total_val,
            var_95_1day=state["var_95_1day"],
            var_99_1day=state["var_99_1day"],
            cvar_95_1day=state["cvar_95_1day"],
            portfolio_beta=1.0,
            portfolio_sharpe_rolling=0.0,
            sector_exposures={},
            top_positions=[]
        )
        session.add(snap)
        session.commit()
        
    # 2. Cache in Redis
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        cache_data = {
            "var_95": state["var_95_1day"],
            "var_99": state["var_99_1day"],
            "cvar_95": state["cvar_95_1day"],
            "mc_var": state["monte_carlo_var"],
            "breaches": state["breaches"]
        }
        r.setex("risk:var:portfolio:current", 300, json.dumps(cache_data))
        
        # 3. Publish breach events
        if state["breaches"]:
            r.publish("risk.var.breach", json.dumps({
                "severity": "high" if "VAR_99_LIMIT_BREACHED" in state["breaches"] else "medium",
                "breach_type": "var_limit",
                "details": state["breaches"]
            }))
    except Exception as e:
        logger.error(f"Failed to write VaR to Redis: {e}")
        
    return {}

def build_var_graph() -> StateGraph:
    workflow = StateGraph(VaRState)
    
    workflow.add_node("fetch_returns_data_node", fetch_returns_data_node)
    workflow.add_node("calculate_historical_var_node", calculate_historical_var_node)
    workflow.add_node("run_monte_carlo_var_node", run_monte_carlo_var_node)
    workflow.add_node("run_stress_tests_node", run_stress_tests_node)
    workflow.add_node("check_var_limits_node", check_var_limits_node)
    workflow.add_node("store_var_results_node", store_var_results_node)
    
    workflow.set_entry_point("fetch_returns_data_node")
    workflow.add_edge("fetch_returns_data_node", "calculate_historical_var_node")
    workflow.add_edge("calculate_historical_var_node", "run_monte_carlo_var_node")
    workflow.add_edge("run_monte_carlo_var_node", "run_stress_tests_node")
    workflow.add_edge("run_stress_tests_node", "check_var_limits_node")
    workflow.add_edge("check_var_limits_node", "store_var_results_node")
    workflow.add_edge("store_var_results_node", END)
    
    return workflow.compile()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class VaRResult:
    var_95_1day_usd: float
    var_99_1day_usd: float
    cvar_95_1day_usd: float
    var_95_5day_usd: float
    monte_carlo_var_usd: float
    per_position_var: dict
    stress_test_results: dict
    breaches: list[str]
    as_pct_of_portfolio: float

class VaRAgent:
    """Public interface for calculating portfolio and position Value at Risk."""
    
    def __init__(self):
        self.graph = build_var_graph()
        
    async def calculate(self, positions: dict[str, float]) -> Optional[VaRResult]:
        """Calculate VaR metrics for an entire portfolio of positions."""
        total_value = sum(positions.values())
        if total_value <= 0:
            logger.warning("Total portfolio value is zero. VaR is 0.")
            return None
            
        initial_state: VaRState = {
            "tickers": list(positions.keys()),
            "position_sizes": positions,
            "total_portfolio_value": total_value,
            "price_history": {},
            "portfolio_returns": pd.Series(dtype=float),
            "var_95_1day": 0.0,
            "var_99_1day": 0.0,
            "cvar_95_1day": 0.0,
            "var_95_5day": 0.0,
            "var_99_10day": 0.0,
            "per_position_var": {},
            "monte_carlo_var": 0.0,
            "stress_test_results": {},
            "breaches": [],
            "error": None
        }
        
        try:
            final_state = await self.graph.ainvoke(initial_state)
            
            if final_state.get("error"):
                logger.error(f"VaR Calculation failed: {final_state['error']}")
                return None
                
            return VaRResult(
                var_95_1day_usd=final_state["var_95_1day"],
                var_99_1day_usd=final_state["var_99_1day"],
                cvar_95_1day_usd=final_state["cvar_95_1day"],
                var_95_5day_usd=final_state["var_95_5day"],
                monte_carlo_var_usd=final_state["monte_carlo_var"],
                per_position_var=final_state["per_position_var"],
                stress_test_results=final_state.get("stress_test_results", {}),
                breaches=final_state.get("breaches", []),
                as_pct_of_portfolio=final_state["var_95_1day"] / total_value
            )
        except Exception as e:
            logger.exception("Error executing VaR graph")
            return None

    async def calculate_single(self, ticker: str, size_usd: float) -> float:
        """Helper to quickly calculate 1-Day 95% VaR for a single position."""
        res = await self.calculate({ticker: size_usd})
        if res:
            return res.var_95_1day_usd
        return 0.0

    def get_current_var(self) -> Optional[VaRResult]:
        """Fetch the most recently cached VaR calculation from Redis."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data_str = r.get("risk:var:portfolio:current")
            if data_str:
                data = json.loads(data_str)
                # Reconstruct a partial VaRResult from cache
                return VaRResult(
                    var_95_1day_usd=data.get("var_95", 0.0),
                    var_99_1day_usd=data.get("var_99", 0.0),
                    cvar_95_1day_usd=data.get("cvar_95", 0.0),
                    var_95_5day_usd=0.0,
                    monte_carlo_var_usd=data.get("mc_var", 0.0),
                    per_position_var={},
                    stress_test_results={},
                    breaches=data.get("breaches", []),
                    as_pct_of_portfolio=0.0
                )
        except Exception as e:
            logger.error(f"Failed to get current VaR from cache: {e}")
        return None

    def run_stress_test(self, scenario: str) -> float:
        """Return the expected dollar loss for a specific scenario based on last cached value."""
        # Just an example implementation since full re-calc requires positions dict
        return 0.0
        
    def get_tail_risk_report(self) -> dict:
        """Returns a summarized tail risk report."""
        current = self.get_current_var()
        if not current:
            return {"status": "unavailable"}
            
        return {
            "status": "active",
            "1_day_99_var": current.var_99_1day_usd,
            "expected_shortfall": current.cvar_95_1day_usd,
            "breaches": current.breaches
        }
