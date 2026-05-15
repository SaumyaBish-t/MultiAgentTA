import json
import asyncio
import math
from datetime import datetime, date, timezone
from typing import TypedDict, Any, Optional, List, Dict
from dataclasses import dataclass, asdict

import numpy as np
import pandas as pd
import redis
import httpx
from loguru import logger
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from scipy.optimize import minimize
from langgraph.graph import StateGraph, END

from config.settings import settings
from portfolio_construction.storage.portfolio_models import (
    Portfolio,
    PortfolioWeight,
    FactorExposure
)
from risk_management.storage.risk_models import PositionLimit
from data_ingestion.storage.models import Company

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class OptimizerState(TypedDict):
    approved_signals: List[dict]
    tickers: List[str]
    expected_returns: Dict[str, float]       # ticker → expected return
    covariance_matrix: Dict[str, Dict[str, float]] # ticker → ticker → covariance
    risk_free_rate: float
    optimization_method: str
    raw_weights: Dict[str, float]            # unconstrained weights
    mv_weights: Dict[str, float]
    rp_weights: Dict[str, float]
    bl_weights: Dict[str, float]
    constrained_weights: Dict[str, float]    # after applying limits
    portfolio_metrics: Dict[str, Any]      # expected return, vol, sharpe
    efficient_frontier: List[Dict[str, float]]
    constraints_applied: List[str]
    error: Optional[str]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def prepare_inputs_node(state: OptimizerState) -> Dict[str, Any]:
    """Fetch history, calculate expected returns and covariance matrix."""
    if state.get("error"): return {}
    
    signals = state.get("approved_signals", [])
    if not signals:
        return {"error": "No approved signals to optimize"}
        
    tickers = list(set([s['ticker'] for s in signals]))
    all_returns = {}
    prices_history = {}
    
    headers = {"x-api-key": settings.internal_api_key}
    
    # Fetch 252 days history for each ticker + SPY
    fetch_tickers = list(set(tickers + ["SPY"]))
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for ticker in fetch_tickers:
            url = f"http://localhost:8000/prices/{ticker}/history?days=252"
            tasks.append(client.get(url, headers=headers, timeout=10.0))
            
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        for ticker, resp in zip(fetch_tickers, responses):
            if isinstance(resp, Exception) or resp.status_code != 200:
                logger.error(f"Error fetching data for {ticker}: {resp}")
                continue
                
            data = resp.json()
            if len(data) > 10:
                df = pd.DataFrame(data)
                df['close'] = pd.to_numeric(df['close'])
                all_returns[ticker] = df['close'].pct_change().dropna()
                prices_history[ticker] = df['close'].values
                
    if len(all_returns) < len(tickers):
        return {"error": "Insufficient history for some tickers"}
        
    # Get Risk Free Rate
    risk_free_rate = 0.04 # Default
    try:
        macro_url = "http://localhost:8000/macro/FEDFUNDS"
        macro_resp = await client.get(macro_url, headers=headers, timeout=5.0)
        if macro_resp.status_code == 200:
            val = macro_resp.json()
            if isinstance(val, (int, float)):
                risk_free_rate = val / 100.0
            elif isinstance(val, list) and len(val) > 0:
                risk_free_rate = float(val[-1].get('value', 4.0)) / 100.0
    except Exception as e:
        logger.warning(f"Could not fetch risk free rate: {e}")

    # Expected Returns Calculations
    expected_returns = {}
    spy_returns = all_returns.get("SPY")
    
    for ticker in tickers:
        returns = all_returns[ticker]
        price = prices_history[ticker]
        
        # METHOD 1: Historical Mean
        hist_return = returns.mean() * 252
        
        # METHOD 2: CAPM
        beta = 1.0
        if spy_returns is not None:
            # Align returns
            common_idx = returns.index.intersection(spy_returns.index)
            if len(common_idx) > 20:
                cov = np.cov(returns.loc[common_idx], spy_returns.loc[common_idx])[0, 1]
                var = np.var(spy_returns.loc[common_idx])
                beta = cov / var if var > 0 else 1.0
        
        market_premium = 0.06
        capm_return = risk_free_rate + beta * market_premium
        
        # METHOD 3: Momentum-Adjusted
        # momentum_12_1: Price 1 month ago / Price 12 months ago
        if len(price) >= 252:
            momentum_12_1 = (price[-21] / price[-252]) - 1
        else:
            momentum_12_1 = 0
        momentum_adj = hist_return * (1 + momentum_12_1 * 0.1)
        
        expected_returns[ticker] = (hist_return + capm_return + momentum_adj) / 3.0

    # Covariance Matrix
    returns_df = pd.DataFrame({t: all_returns[t] for t in tickers}).dropna()
    cov_matrix = returns_df.cov().to_dict()
    # Annualized for state storage
    ann_cov = {t1: {t2: cov_matrix[t1][t2] * 252 for t2 in tickers} for t1 in tickers}
    
    return {
        "tickers": tickers,
        "expected_returns": expected_returns,
        "covariance_matrix": ann_cov,
        "risk_free_rate": risk_free_rate
    }

async def run_mean_variance_optimization_node(state: OptimizerState) -> Dict[str, Any]:
    """Classical Markowitz Sharpe optimization."""
    if state.get("error"): return {}
    
    tickers = state["tickers"]
    n = len(tickers)
    expected_returns = np.array([state["expected_returns"][t] for t in tickers])
    cov_matrix = np.array([[state["covariance_matrix"][t1][t2] for t2 in tickers] for t1 in tickers])
    rf = state["risk_free_rate"]
    
    def portfolio_sharpe(weights):
        port_return = np.dot(weights, expected_returns)
        port_vol = np.sqrt(weights.T @ cov_matrix @ weights)
        if port_vol == 0: return 0
        return -(port_return - rf) / port_vol
        
    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0}]
    bounds = [(0.01, 0.15)] * n
    
    result = minimize(
        portfolio_sharpe,
        x0=np.array([1.0/n] * n),
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )
    
    if not result.success:
        logger.warning(f"MV Optimization failed: {result.message}")
        # Fallback to equal weight
        weights = [1.0/n] * n
    else:
        weights = result.x
        
    mv_weights = dict(zip(tickers, [float(w) for w in weights]))
    return {"mv_weights": mv_weights}

async def run_risk_parity_node(state: OptimizerState) -> Dict[str, Any]:
    """Equal Risk Contribution optimization."""
    if state.get("error"): return {}
    
    tickers = state["tickers"]
    n = len(tickers)
    cov_matrix = np.array([[state["covariance_matrix"][t1][t2] for t2 in tickers] for t1 in tickers])
    
    def risk_contribution(weights, cov):
        portfolio_vol = np.sqrt(weights.T @ cov @ weights)
        marginal_contrib = cov @ weights
        risk_contrib = weights * marginal_contrib / portfolio_vol
        return risk_contrib
        
    def risk_parity_objective(weights, cov):
        rc = risk_contribution(weights, cov)
        target = np.ones(len(weights)) / len(weights)
        return np.sum((rc - target) ** 2)
        
    result = minimize(
        risk_parity_objective,
        args=(cov_matrix,),
        x0=np.array([1.0/n] * n),
        method='SLSQP',
        bounds=[(0.01, 0.30)] * n,
        constraints=[{'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0}]
    )
    
    if not result.success:
        logger.warning(f"RP Optimization failed: {result.message}")
        weights = [1.0/n] * n
    else:
        weights = result.x
        
    rp_weights = dict(zip(tickers, [float(w) for w in weights]))
    return {"rp_weights": rp_weights}

async def run_black_litterman_node(state: OptimizerState) -> Dict[str, Any]:
    """Black-Litterman optimization using market caps and analyst views."""
    if state.get("error"): return {}
    
    tickers = state["tickers"]
    n = len(tickers)
    cov_matrix = np.array([[state["covariance_matrix"][t1][t2] for t2 in tickers] for t1 in tickers])
    approved_signals = state["approved_signals"]
    
    # 1. Market Equilibrium
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    market_caps = {}
    with Session() as session:
        for t in tickers:
            comp = session.execute(select(Company).where(Company.ticker == t)).scalar_one_or_none()
            market_caps[t] = float(comp.market_cap) if comp and comp.market_cap else 1e11 # Default $100B
            
    total_cap = sum(market_caps.values())
    market_weight_vector = np.array([market_caps[t]/total_cap for t in tickers])
    
    risk_aversion = 2.5
    pi = risk_aversion * cov_matrix @ market_weight_vector
    
    # 2. Views
    # We use ticker index for Pick Matrix P
    ticker_to_idx = {t: i for i, t in enumerate(tickers)}
    views_returns = []
    pick_matrix = []
    uncertainties = []
    
    for signal in approved_signals:
        ticker = signal['ticker']
        if ticker not in ticker_to_idx: continue
        
        p_row = np.zeros(n)
        p_row[ticker_to_idx[ticker]] = 1.0
        pick_matrix.append(p_row)
        
        # Simple view return: 15% long, -10% short
        view_ret = 0.15 if signal.get('direction', 'long') == 'long' else -0.10
        views_returns.append(view_ret)
        
        # Uncertainty linked to conviction
        conviction = signal.get('conviction_score', 0.5)
        # Higher conviction = lower uncertainty
        # Omega is typically P * (tau * Cov) * P.T
        uncertainty = 0.05 * (1.1 - conviction)
        uncertainties.append(uncertainty)
        
    if not pick_matrix:
        # No views, just use equilibrium weights
        bl_weights = dict(zip(tickers, [float(w) for w in market_weight_vector]))
        return {"bl_weights": bl_weights}
        
    P = np.array(pick_matrix)
    Q = np.array(views_returns)
    Omega = np.diag(uncertainties)
    tau = 0.05
    
    # BL Formula
    inv_tau_cov = np.linalg.inv(tau * cov_matrix)
    inv_omega = np.linalg.inv(Omega)
    
    term1 = np.linalg.inv(inv_tau_cov + P.T @ inv_omega @ P)
    term2 = inv_tau_cov @ pi + P.T @ inv_omega @ Q
    bl_returns = term1 @ term2
    
    # Re-optimize with BL returns (Sharpe maximize)
    rf = state["risk_free_rate"]
    def bl_sharpe(weights):
        port_return = np.dot(weights, bl_returns)
        port_vol = np.sqrt(weights.T @ cov_matrix @ weights)
        if port_vol == 0: return 0
        return -(port_return - rf) / port_vol
        
    res = minimize(
        bl_sharpe,
        x0=np.array([1.0/n] * n),
        method='SLSQP',
        bounds=[(0.01, 0.15)] * n,
        constraints=[{'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0}]
    )
    
    weights = res.x if res.success else market_weight_vector
    bl_weights = dict(zip(tickers, [float(w) for w in weights]))
    return {"bl_weights": bl_weights}

async def blend_and_constrain_weights_node(state: OptimizerState) -> Dict[str, Any]:
    """Blend optimization outputs and apply hard constraints."""
    if state.get("error"): return {}
    
    tickers = state["tickers"]
    mv = state.get("mv_weights", {})
    rp = state.get("rp_weights", {})
    bl = state.get("bl_weights", {})
    
    # 1. Blend (30% MV, 40% RP, 30% BL)
    blended = {}
    for t in tickers:
        blended[t] = mv.get(t, 0) * 0.30 + rp.get(t, 0) * 0.40 + bl.get(t, 0) * 0.30
        
    # 2. Hard Constraints
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    # Get Sectors
    ticker_to_sector = {}
    with Session() as session:
        for t in tickers:
            comp = session.execute(select(Company).where(Company.ticker == t)).scalar_one_or_none()
            ticker_to_sector[t] = comp.sector if comp and comp.sector else "Unknown"
            
    # Get Position Limits from Phase 4
    max_pos_pct = {}
    with Session() as session:
        for t in tickers:
            # Get latest approved limit for this ticker
            stmt = select(PositionLimit).where(PositionLimit.ticker == t).order_by(PositionLimit.created_at.desc()).limit(1)
            limit_row = session.execute(stmt).scalar_one_or_none()
            max_pos_pct[t] = float(limit_row.max_position_size_pct) if limit_row else 0.15
            
    constrained = blended.copy()
    applied = []
    
    # CONSTRAINT 1 & 4: Min/Max per position & Position Limits
    for t in tickers:
        limit = min(0.15, max_pos_pct.get(t, 0.15))
        if constrained[t] > limit:
            constrained[t] = limit
            applied.append(f"LIMIT_BREACH_{t}")
        if constrained[t] < 0.01:
            constrained[t] = 0.01
            applied.append(f"MIN_WEIGHT_{t}")
            
    # CONSTRAINT 2: Sector Limit 30%
    sector_weights = {}
    for t, w in constrained.items():
        s = ticker_to_sector[t]
        sector_weights[s] = sector_weights.get(s, 0) + w
        
    for s, w in sector_weights.items():
        if w > 0.30:
            scale = 0.30 / w
            for t in tickers:
                if ticker_to_sector[t] == s:
                    constrained[t] *= scale
            applied.append(f"SECTOR_LIMIT_{s}")
            
    # CONSTRAINT 3 & 5: Sum <= 0.95 (5% cash buffer)
    total_invested = sum(constrained.values())
    if total_invested > 0.95:
        scale = 0.95 / total_invested
        for t in tickers:
            constrained[t] *= scale
        applied.append("PORTFOLIO_CASH_BUFFER_APPLIED")
    elif total_invested < 0.20:
        # If very low, maybe scaling up isn't desired, but we keep at least 5% cash
        pass
        
    return {
        "constrained_weights": constrained,
        "constraints_applied": list(set(applied))
    }

async def calculate_portfolio_metrics_node(state: OptimizerState) -> Dict[str, Any]:
    """Final performance projection for the optimized weights."""
    if state.get("error"): return {}
    
    weights_dict = state["constrained_weights"]
    tickers = state["tickers"]
    exp_returns = state["expected_returns"]
    ann_cov = state["covariance_matrix"]
    rf = state["risk_free_rate"]
    
    w_vec = np.array([weights_dict[t] for t in tickers])
    r_vec = np.array([exp_returns[t] for t in tickers])
    cov_mat = np.array([[ann_cov[t1][t2] for t2 in tickers] for t1 in tickers])
    
    port_return = np.dot(w_vec, r_vec)
    port_vol = np.sqrt(w_vec.T @ cov_mat @ w_vec)
    port_sharpe = (port_return - rf) / port_vol if port_vol > 0 else 0
    
    metrics = {
        "expected_annual_return": float(port_return),
        "expected_annual_volatility": float(port_vol),
        "expected_sharpe": float(port_sharpe),
        "cash_weight": float(1.0 - np.sum(w_vec)),
        "n_positions": len(tickers)
    }
    
    return {"portfolio_metrics": metrics}

async def store_weights_node(state: OptimizerState) -> Dict[str, Any]:
    """Save results to DB and Redis, publish event."""
    if state.get("error"): return {}
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    metrics = state["portfolio_metrics"]
    weights = state["constrained_weights"]
    
    with Session() as session:
        # 1. Get Portfolio ID
        stmt = select(Portfolio).where(Portfolio.name == "main_portfolio").limit(1)
        portfolio = session.execute(stmt).scalar_one_or_none()
        pf_id = portfolio.id if portfolio else None
        
        if pf_id:
            # 2. Store Weights
            weight_entry = PortfolioWeight(
                portfolio_id=pf_id,
                optimization_method="blended_mv_rp_bl",
                weights=weights,
                expected_return=metrics["expected_annual_return"],
                expected_volatility=metrics["expected_annual_volatility"],
                expected_sharpe=metrics["expected_sharpe"],
                optimization_inputs={
                    "risk_free_rate": state["risk_free_rate"],
                    "tickers": state["tickers"]
                },
                constraints_applied={"list": state["constraints_applied"]}
            )
            session.add(weight_entry)
            
            # 3. Factor Exposure (Placeholder - simple sector weights for now)
            # Fetch sectors for all
            ticker_to_sector = {}
            for t in state["tickers"]:
                comp = session.execute(select(Company).where(Company.ticker == t)).scalar_one_or_none()
                ticker_to_sector[t] = comp.sector if comp and comp.sector else "Unknown"
                
            sector_weights = {}
            for t, w in weights.items():
                s = ticker_to_sector[t]
                sector_weights[s] = sector_weights.get(s, 0) + w
                
            exposure = FactorExposure(
                portfolio_id=pf_id,
                snapshot_date=date.today(),
                market_beta=1.0, # Approximate
                size_factor=0.0,
                value_factor=0.0,
                momentum_factor=0.0,
                quality_factor=0.0,
                volatility_factor=0.0,
                sector_weights=sector_weights,
                geographic_weights={"US": 1.0}
            )
            session.add(exposure)
            session.commit()
            
    # 4. Redis Cache
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        r.setex("portfolio:target:weights", 3600, json.dumps(weights))
        
        # 5. Publish Event
        event = {
            "method": "blended_optimization",
            "sharpe": metrics["expected_sharpe"],
            "n_positions": metrics["n_positions"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        r.publish("portfolio.weights.calculated", json.dumps(event))
    except Exception as e:
        logger.error(f"Redis error in store_weights: {e}")
        
    return {}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class OptimizationResult:
    weights: dict
    expected_return: float
    expected_volatility: float
    expected_sharpe: float
    cash_weight: float
    optimization_method: str
    constraints_applied: List[str]

class PortfolioOptimizer:
    """Agent orchestrator for portfolio optimization workflow."""
    
    def __init__(self):
        self.workflow = StateGraph(OptimizerState)
        self.workflow.add_node("prepare_inputs", prepare_inputs_node)
        self.workflow.add_node("run_mv", run_mean_variance_optimization_node)
        self.workflow.add_node("run_rp", run_risk_parity_node)
        self.workflow.add_node("run_bl", run_black_litterman_node)
        self.workflow.add_node("blend_constrain", blend_and_constrain_weights_node)
        self.workflow.add_node("metrics", calculate_portfolio_metrics_node)
        self.workflow.add_node("store", store_weights_node)
        
        self.workflow.set_entry_point("prepare_inputs")
        self.workflow.add_edge("prepare_inputs", "run_mv")
        self.workflow.add_edge("run_mv", "run_rp")
        self.workflow.add_edge("run_rp", "run_bl")
        self.workflow.add_edge("run_bl", "blend_constrain")
        self.workflow.add_edge("blend_constrain", "metrics")
        self.workflow.add_edge("metrics", "store")
        self.workflow.add_edge("store", END)
        
        self.app = self.workflow.compile()
        
    async def optimize(self, approved_signals: List[dict]) -> Optional[OptimizationResult]:
        """Main entry point to optimize portfolio based on risk-approved signals."""
        initial_state: OptimizerState = {
            "approved_signals": approved_signals,
            "tickers": [],
            "expected_returns": {},
            "covariance_matrix": {},
            "risk_free_rate": 0.04,
            "optimization_method": "blended",
            "raw_weights": {},
            "mv_weights": {},
            "rp_weights": {},
            "bl_weights": {},
            "constrained_weights": {},
            "portfolio_metrics": {},
            "efficient_frontier": [],
            "constraints_applied": [],
            "error": None
        }
        
        try:
            final_state = await self.app.ainvoke(initial_state)
            if final_state.get("error"):
                logger.error(f"Optimization failed: {final_state['error']}")
                return None
                
            metrics = final_state["portfolio_metrics"]
            return OptimizationResult(
                weights=final_state["constrained_weights"],
                expected_return=metrics["expected_annual_return"],
                expected_volatility=metrics["expected_annual_volatility"],
                expected_sharpe=metrics["expected_sharpe"],
                cash_weight=metrics["cash_weight"],
                optimization_method="blended_mv_rp_bl",
                constraints_applied=final_state["constraints_applied"]
            )
        except Exception as e:
            logger.exception("Error in PortfolioOptimizer.optimize")
            return None

    def get_current_weights(self) -> dict:
        """Fetch latest calculated weights from Redis."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data = r.get("portfolio:target:weights")
            return json.loads(data) if data else {}
        except Exception:
            return {}

    async def get_efficient_frontier(self, approved_signals: List[dict], n_points: int = 20) -> List[dict]:
        """Calculate points on the Efficient Frontier for the given signals."""
        # This would require running multiple optimizations with varying target returns
        # For brevity, we implement a simplified version or return empty
        return []

    async def what_if_add(self, ticker: str, weight: float) -> dict:
        """Impact analysis of adding a specific asset at a target weight."""
        # 1. Get current weights
        current = self.get_current_weights()
        if not current: return {"error": "No current portfolio"}
        
        # 2. Add ticker
        new_weights = current.copy()
        new_weights[ticker] = weight
        # Re-normalize
        total = sum(new_weights.values())
        new_weights = {t: w/total for t, w in new_weights.items()}
        
        # 3. Quick metrics calculation (omitted full cov logic for speed)
        return {"new_target_weights": new_weights, "note": "Full metrics require covariance data"}
