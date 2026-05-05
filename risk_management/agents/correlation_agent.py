import json
import asyncio
import math
from datetime import datetime
from collections import defaultdict
from typing import TypedDict, Any, Optional
from dataclasses import dataclass

import httpx
import numpy as np
import pandas as pd
import redis
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage

from config.settings import settings
from config.llm_config import LLMFactory
from risk_management.storage.risk_models import (
    RiskEvent,
    CorrelationMatrixSnapshot
)
from data_ingestion.storage.models import Company

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class CorrelationState(TypedDict):
    tickers: list[str]
    position_weights: dict    # ticker → portfolio weight
    returns_data: dict        # ticker → returns series
    correlation_matrix: dict
    high_corr_pairs: list[dict]
    sector_exposures: dict
    factor_exposures: dict
    concentration_score: float
    diversification_ratio: float
    breaches: list[str]
    recommendations: list[str]
    error: str | None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_data_node(state: CorrelationState) -> dict[str, Any]:
    """Fetch 252 days returns and static sector/market_cap info."""
    if state.get("error"): return {}
    
    tickers = state.get("tickers", [])
    if not tickers:
        return {"error": "No tickers provided for correlation analysis"}
        
    returns_data = {}
    headers = {"x-api-key": settings.internal_api_key}
    
    # Also fetch SPY for market beta calculations later
    fetch_tickers = list(set(tickers + ["SPY"]))
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for ticker in fetch_tickers:
            url = f"http://localhost:8000/prices/{ticker}/history?days=252"
            tasks.append(client.get(url, headers=headers, timeout=10.0))
            
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        for ticker, resp in zip(fetch_tickers, responses):
            if isinstance(resp, Exception):
                logger.error(f"Error fetching data for {ticker}: {resp}")
                continue
            
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    df = pd.DataFrame(data)
                    df['close'] = pd.to_numeric(df['close'])
                    returns = df['close'].pct_change().dropna()
                    returns_data[ticker] = returns
                    
    if len(returns_data) < 2 and len(tickers) >= 2:
        return {"error": "Insufficient return data to calculate correlations"}
        
    # We don't fetch sector here into state directly because calculate_sector_concentration_node does it.
    # But node 1 says: "Build position_weights from current positions" (already in state usually, but let's ensure)
    # The prompt actually implies position_weights are passed in or derived.
    # We will assume position_weights are provided in initial_state as per normal flow.
    
    return {"returns_data": returns_data}

async def calculate_correlation_matrix_node(state: CorrelationState) -> dict[str, Any]:
    """Calculate the correlation matrix and identify highly correlated pairs."""
    if state.get("error"): return {}
    
    returns_data = state.get("returns_data", {})
    tickers = state.get("tickers", [])
    weights = state.get("position_weights", {})
    
    # Filter returns_data to only include our target tickers (exclude SPY if it was just for beta)
    portfolio_returns = {t: returns_data[t] for t in tickers if t in returns_data}
    
    if len(portfolio_returns) < 2:
        return {
            "correlation_matrix": {},
            "high_corr_pairs": [],
            "concentration_score": 0.0
        }
        
    df = pd.DataFrame(portfolio_returns).dropna()
    corr_matrix = df.corr()
    
    high_corr_pairs = []
    for i, t1 in enumerate(tickers):
        for j, t2 in enumerate(tickers):
            if i < j and t1 in corr_matrix.columns and t2 in corr_matrix.columns:
                corr = corr_matrix.loc[t1, t2]
                if abs(corr) > 0.70:
                    high_corr_pairs.append({
                        "ticker1": t1,
                        "ticker2": t2,
                        "correlation": float(corr),
                        "combined_weight": weights.get(t1, 0) + weights.get(t2, 0)
                    })
                    
    # Calculate avg correlation (upper triangle excluding diagonal)
    if len(corr_matrix.columns) > 1:
        vals = corr_matrix.values
        avg_correlation = float(vals[np.triu_indices_from(vals, k=1)].mean())
    else:
        avg_correlation = 1.0
        
    # Convert correlation matrix to dict for JSON serialization
    corr_dict = corr_matrix.to_dict()
    
    return {
        "correlation_matrix": corr_dict,
        "high_corr_pairs": high_corr_pairs,
        "concentration_score": avg_correlation # Using avg corr as a simple concentration score
    }

async def calculate_sector_concentration_node(state: CorrelationState) -> dict[str, Any]:
    """Calculate sector concentration and flag breaches."""
    if state.get("error"): return {}
    
    weights = state.get("position_weights", {})
    breaches = state.get("breaches", [])
    recommendations = state.get("recommendations", [])
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    sector_exposures = defaultdict(float)
    
    with Session() as session:
        for ticker, weight in weights.items():
            comp = session.execute(select(Company).where(Company.ticker == ticker)).scalar_one_or_none()
            sector = comp.sector if comp and comp.sector else "Unknown"
            sector_exposures[sector] += weight
            
    max_sector_pct = 0.30
    for sector, exposure in sector_exposures.items():
        if exposure > max_sector_pct:
            breaches.append(f"SECTOR_CONCENTRATION_{sector}")
            recommendations.append(
                f"Reduce {sector} exposure from {exposure:.0%} to below {max_sector_pct:.0%}"
            )
            
    return {
        "sector_exposures": dict(sector_exposures),
        "breaches": breaches,
        "recommendations": recommendations
    }

async def calculate_factor_exposures_node(state: CorrelationState) -> dict[str, Any]:
    """Approximate market beta, size, and momentum factor exposures."""
    if state.get("error"): return {}
    
    returns_data = state.get("returns_data", {})
    weights = state.get("position_weights", {})
    tickers = state.get("tickers", [])
    
    portfolio_beta = 1.0
    if "SPY" in returns_data:
        spy_returns = returns_data["SPY"]
        spy_var = spy_returns.var()
        
        if spy_var > 0:
            beta_sum = 0.0
            for t, w in weights.items():
                if t in returns_data:
                    df = pd.DataFrame({"asset": returns_data[t], "spy": spy_returns}).dropna()
                    if not df.empty:
                        cov = df.cov().iloc[0, 1]
                        beta_sum += (cov / spy_var) * w
            portfolio_beta = beta_sum
            
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    large_cap_pct = 0.0
    small_cap_pct = 0.0
    
    with Session() as session:
        for ticker, weight in weights.items():
            comp = session.execute(select(Company).where(Company.ticker == ticker)).scalar_one_or_none()
            if comp and comp.market_cap:
                if comp.market_cap > 10_000_000_000:
                    large_cap_pct += weight
                elif comp.market_cap < 2_000_000_000:
                    small_cap_pct += weight
                    
    # Momentum (last 252 days total return approximation)
    port_momentum = 0.0
    for t, w in weights.items():
        if t in returns_data and len(returns_data[t]) > 20:
            ret_series = returns_data[t]
            # Cumulative return over the period
            mom = (1 + ret_series).prod() - 1
            port_momentum += mom * w
            
    return {
        "factor_exposures": {
            "market_beta": float(portfolio_beta),
            "large_cap_exposure": float(large_cap_pct),
            "small_cap_exposure": float(small_cap_pct),
            "momentum_1y": float(port_momentum)
        }
    }

async def calculate_diversification_ratio_node(state: CorrelationState) -> dict[str, Any]:
    """Calculate the portfolio Diversification Ratio (DR)."""
    if state.get("error"): return {}
    
    returns_data = state.get("returns_data", {})
    weights = state.get("position_weights", {})
    tickers = state.get("tickers", [])
    
    df = pd.DataFrame({t: returns_data[t] for t in tickers if t in returns_data}).dropna()
    if df.empty or len(df.columns) < 2:
        return {"diversification_ratio": 1.0}
        
    # Calculate individual vols (annualized)
    individual_vols = {t: df[t].std() * math.sqrt(252) for t in df.columns}
    
    weighted_vol = sum(weights.get(t, 0) * vol for t, vol in individual_vols.items())
    
    # Portfolio vol using returns
    port_returns = pd.Series(0.0, index=df.index)
    for t in df.columns:
        port_returns += df[t] * weights.get(t, 0)
        
    portfolio_vol = port_returns.std() * math.sqrt(252)
    
    dr = 1.0
    if portfolio_vol > 0:
        dr = weighted_vol / portfolio_vol
        
    return {"diversification_ratio": float(dr)}

async def generate_recommendations_node(state: CorrelationState) -> dict[str, Any]:
    """Use an LLM to generate actionable risk reduction recommendations if breaches exist."""
    if state.get("error"): return {}
    
    high_pairs = state.get("high_corr_pairs", [])
    breaches = state.get("breaches", [])
    dr = state.get("diversification_ratio", 1.0)
    existing_recs = state.get("recommendations", [])
    
    if not high_pairs and not breaches:
        return {} # No urgent need for LLM recommendations
        
    llm = LLMFactory.get_risk_llm() # Using the fast model (Groq 8B if configured)
    
    prompt = f"""
    Given these portfolio risk metrics:
    High correlation pairs: {json.dumps(high_pairs)}
    Sector breaches: {json.dumps(breaches)}
    Diversification ratio: {dr:.2f} (Ideal > 1.5)
    
    Give 3 specific, actionable recommendations to reduce portfolio concentration and risk.
    Return ONLY valid JSON in this exact format: {{"recommendations": ["rec1", "rec2", "rec3"]}}
    """
    
    try:
        messages = [
            SystemMessage(content="You are a strict risk management AI. Output JSON only."),
            HumanMessage(content=prompt)
        ]
        response = await llm.ainvoke(messages)
        content = response.content.strip()
        
        # Strip markdown code blocks if present
        if content.startswith("```json"): content = content[7:]
        if content.startswith("```"): content = content[3:]
        if content.endswith("```"): content = content[:-3]
        
        parsed = json.loads(content.strip())
        new_recs = parsed.get("recommendations", [])
        
        return {"recommendations": existing_recs + new_recs}
        
    except Exception as e:
        logger.error(f"Failed to generate LLM recommendations: {e}")
        return {}

async def store_results_node(state: CorrelationState) -> dict[str, Any]:
    """Store matrix snapshots, dispatch alerts, and cache results."""
    if state.get("error"): return {}
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    corr_matrix = state.get("correlation_matrix", {})
    high_pairs = state.get("high_corr_pairs", [])
    avg_corr = state.get("concentration_score", 1.0)
    
    # 1. DB Save
    with Session() as session:
        # Create CorrelationMatrixSnapshot
        snap = CorrelationMatrixSnapshot(
            tickers=state["tickers"],
            correlation_matrix=corr_matrix,
            avg_correlation=avg_corr,
            max_correlation=max(p["correlation"] for p in high_pairs) if high_pairs else avg_corr,
            high_correlation_pairs=high_pairs,
            snapshot_date=datetime.utcnow().date()
        )
        session.add(snap)
        
        # Create RiskEvents for breaches
        for breach in state.get("breaches", []):
            event = RiskEvent(
                event_type="concentration_breach",
                severity="medium" if "SECTOR" in breach else "high",
                description=f"Concentration breach: {breach}",
                current_value=0.0,
                threshold_value=0.30, # default threshold
                action_taken="monitor"
            )
            session.add(event)
            
        session.commit()
        
    # 2. Redis Cache
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        cache_val = {
            "avg_corr": avg_corr,
            "high_pairs_count": len(high_pairs),
            "sector_exposures": state.get("sector_exposures", {}),
            "dr": state.get("diversification_ratio", 1.0),
            "recommendations": state.get("recommendations", [])
        }
        r.setex("risk:correlation:current", 1800, json.dumps(cache_val))
        r.publish("risk.correlation.updated", json.dumps(cache_val))
    except Exception as e:
        logger.error(f"Failed to write Correlation state to Redis: {e}")
        
    return {}

def build_correlation_graph() -> StateGraph:
    workflow = StateGraph(CorrelationState)
    
    workflow.add_node("fetch_data_node", fetch_data_node)
    workflow.add_node("calculate_correlation_matrix_node", calculate_correlation_matrix_node)
    workflow.add_node("calculate_sector_concentration_node", calculate_sector_concentration_node)
    workflow.add_node("calculate_factor_exposures_node", calculate_factor_exposures_node)
    workflow.add_node("calculate_diversification_ratio_node", calculate_diversification_ratio_node)
    workflow.add_node("generate_recommendations_node", generate_recommendations_node)
    workflow.add_node("store_results_node", store_results_node)
    
    workflow.set_entry_point("fetch_data_node")
    workflow.add_edge("fetch_data_node", "calculate_correlation_matrix_node")
    workflow.add_edge("calculate_correlation_matrix_node", "calculate_sector_concentration_node")
    workflow.add_edge("calculate_sector_concentration_node", "calculate_factor_exposures_node")
    workflow.add_edge("calculate_factor_exposures_node", "calculate_diversification_ratio_node")
    workflow.add_edge("calculate_diversification_ratio_node", "generate_recommendations_node")
    workflow.add_edge("generate_recommendations_node", "store_results_node")
    workflow.add_edge("store_results_node", END)
    
    return workflow.compile()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class CorrelationResult:
    correlation_matrix: pd.DataFrame
    high_correlation_pairs: list[dict]
    sector_exposures: dict
    factor_exposures: dict
    diversification_ratio: float
    breaches: list[str]
    recommendations: list[str]

class CorrelationAgent:
    """Public interface for analyzing portfolio correlation and concentration."""
    
    def __init__(self):
        self.graph = build_correlation_graph()
        
    async def analyze(self, tickers: list[str], position_weights: dict[str, float]) -> Optional[CorrelationResult]:
        """Perform full correlation and concentration analysis on a portfolio."""
        initial_state: CorrelationState = {
            "tickers": tickers,
            "position_weights": position_weights,
            "returns_data": {},
            "correlation_matrix": {},
            "high_corr_pairs": [],
            "sector_exposures": {},
            "factor_exposures": {},
            "concentration_score": 0.0,
            "diversification_ratio": 1.0,
            "breaches": [],
            "recommendations": [],
            "error": None
        }
        
        try:
            final_state = await self.graph.ainvoke(initial_state)
            
            if final_state.get("error"):
                logger.error(f"Correlation analysis failed: {final_state['error']}")
                return None
                
            return CorrelationResult(
                correlation_matrix=pd.DataFrame(final_state.get("correlation_matrix", {})),
                high_correlation_pairs=final_state.get("high_corr_pairs", []),
                sector_exposures=final_state.get("sector_exposures", {}),
                factor_exposures=final_state.get("factor_exposures", {}),
                diversification_ratio=final_state.get("diversification_ratio", 1.0),
                breaches=final_state.get("breaches", []),
                recommendations=final_state.get("recommendations", [])
            )
        except Exception as e:
            logger.exception("Error executing Correlation graph")
            return None

    def get_correlation_matrix(self) -> pd.DataFrame:
        """Fetch the latest correlation matrix from the database."""
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        with Session() as session:
            stmt = select(CorrelationMatrixSnapshot).order_by(CorrelationMatrixSnapshot.created_at.desc()).limit(1)
            snap = session.execute(stmt).scalar_one_or_none()
            if snap:
                return pd.DataFrame(snap.correlation_matrix)
        return pd.DataFrame()

    async def check_new_position(self, ticker: str, positions: dict[str, float]) -> dict:
        """Quickly evaluates how adding a new ticker affects the portfolio."""
        # Simple simulation: add ticker with a 5% test weight
        test_weights = positions.copy()
        test_weights[ticker] = 0.05
        
        # Re-normalize
        total = sum(test_weights.values())
        test_weights = {t: w/total for t, w in test_weights.items()}
        
        res = await self.analyze(list(test_weights.keys()), test_weights)
        if not res: return {}
        
        return {
            "diversification_ratio_impact": res.diversification_ratio,
            "new_high_corr_pairs": [p for p in res.high_correlation_pairs if p["ticker1"] == ticker or p["ticker2"] == ticker],
            "recommendations": res.recommendations
        }

    def get_most_correlated_pair(self) -> tuple[str, str, float]:
        """Returns the most highly correlated pair from the latest snapshot."""
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        with Session() as session:
            stmt = select(CorrelationMatrixSnapshot).order_by(CorrelationMatrixSnapshot.created_at.desc()).limit(1)
            snap = session.execute(stmt).scalar_one_or_none()
            if snap and snap.high_correlation_pairs:
                # Find max correlation
                max_pair = max(snap.high_correlation_pairs, key=lambda x: x["correlation"])
                return (max_pair["ticker1"], max_pair["ticker2"], max_pair["correlation"])
        return ("", "", 0.0)

    def suggest_diversification(self) -> list[str]:
        """Fetch latest cached diversification recommendations."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data_str = r.get("risk:correlation:current")
            if data_str:
                data = json.loads(data_str)
                return data.get("recommendations", [])
        except Exception:
            pass
        return []
