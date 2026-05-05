import json
import asyncio
import math
from datetime import datetime, date
from typing import TypedDict, Any, Optional, List, Dict, Union
from dataclasses import dataclass
from collections import defaultdict

import numpy as np
import pandas as pd
import redis
import httpx
from loguru import logger
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from langgraph.graph import StateGraph, END

from config.settings import settings
from config.llm_config import LLMFactory
from portfolio_construction.storage.portfolio_models import FactorExposure, Portfolio
from data_ingestion.storage.models import Company, FundamentalBase

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FactorState(TypedDict):
    portfolio_weights: Dict[str, float]    # ticker → weight
    price_history: Dict[str, List[float]]  # ticker → list of returns
    factor_scores: Dict[str, Dict[str, float]] # ticker → factor → score
    portfolio_factors: Dict[str, float]    # portfolio level factors
    factor_breaches: List[str]
    factor_adjustments: List[Dict[str, Any]] # suggested weight changes
    adjusted_weights: Dict[str, float]     # after factor correction
    error: Optional[str]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_factor_data_node(state: FactorState) -> Dict[str, Any]:
    """Fetch history, fundamentals and company info."""
    if state.get("error"): return {}
    
    weights = state.get("portfolio_weights", {})
    tickers = list(weights.keys())
    if not tickers:
        return {"error": "No tickers in portfolio"}
        
    price_history = {}
    fundamentals_summary = {}
    
    headers = {"x-api-key": settings.internal_api_key}
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
                # Store returns for factor calculation
                price_history[ticker] = df['close'].pct_change().dropna().tolist()
    
    return {
        "price_history": price_history
    }

async def calculate_security_factors_node(state: FactorState) -> Dict[str, Any]:
    """Calculate beta, size, value, momentum, quality, volatility scores."""
    if state.get("error"): return {}
    
    weights = state["portfolio_weights"]
    tickers = list(weights.keys())
    price_history = state["price_history"]
    spy_returns = np.array(price_history.get("SPY", []))
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    factor_scores = {}
    
    # 1. Market Caps and Sectors
    market_caps = {}
    sectors = {}
    with Session() as session:
        for t in tickers:
            comp = session.execute(text("SELECT market_cap, sector FROM companies WHERE ticker = :t"), {"t": t}).fetchone()
            market_caps[t] = float(comp[0]) if comp and comp[0] else 1e11
            sectors[t] = comp[1] if comp and comp[1] else "Unknown"

    # 2. Fundamentals (PE, ROE, Debt/Equity)
    fundamental_data = {}
    async with httpx.AsyncClient() as client:
        for t in tickers:
            # Get latest PE from summary
            try:
                summary_resp = await client.get(f"http://localhost:8000/fundamentals/{t}/summary", 
                                       headers={"x-api-key": settings.internal_api_key},
                                       timeout=15.0)
                pe = 20.0 # Default
                if summary_resp.status_code == 200:
                    data = summary_resp.json()
                    pe = data.get("pe_ratio")
                    if pe is None: pe = 20.0
            except Exception as e:
                logger.warning(f"Failed to fetch fundamentals for {t}: {e}")
                pe = 20.0
            
            with Session() as session:
                # Get ROE and Debt Score
                scores = session.execute(text("SELECT roe_score, debt_score FROM fundamental_scores WHERE ticker = :t ORDER BY scored_at DESC LIMIT 1"), {"t": t}).fetchone()
                roe_score = float(scores[0]) if scores and scores[0] else 0.5
                debt_score = float(scores[1]) if scores and scores[1] else 0.5
            
            fundamental_data[t] = {
                "pe": float(pe),
                "roe_score": roe_score,
                "debt_score": debt_score
            }

    # 3. Calculate Scores
    # Momentum needs relative ranking, so we store raw momentum first
    raw_momentum = {}
    
    for t in tickers:
        returns = np.array(price_history.get(t, []))
        if len(returns) < 20 or len(spy_returns) < 20:
            continue
            
        # MARKET BETA
        # Align returns lengths
        min_len = min(len(returns), len(spy_returns))
        r = returns[-min_len:]
        s = spy_returns[-min_len:]
        beta = np.cov(r, s)[0, 1] / np.var(s) if np.var(s) > 0 else 1.0
        
        # SIZE FACTOR
        mc = market_caps[t]
        if mc > 2e11: size_score = 1.0
        elif mc > 1e10: size_score = 0.7
        elif mc > 2e9: size_score = 0.4
        else: size_score = 0.1
        
        # VALUE FACTOR
        pe = fundamental_data[t]["pe"]
        if pe < 12: value_score = 1.0
        elif pe < 20: value_score = 0.6
        elif pe < 35: value_score = 0.3
        else: value_score = 0.0
        
        # QUALITY FACTOR
        q_score = fundamental_data[t]["roe_score"] * 0.6 + fundamental_data[t]["debt_score"] * 0.4
        
        # VOLATILITY FACTOR
        realized_vol = np.std(returns) * math.sqrt(252)
        if realized_vol < 0.15: vol_score = 1.0
        elif realized_vol < 0.25: vol_score = 0.6
        elif realized_vol > 0.40: vol_score = 0.1
        else: vol_score = 0.4
        
        # MOMENTUM (Raw)
        # We need cumulative returns for 12-1 month
        # Simplified: last 252 returns, skip last 21
        if len(returns) >= 252:
            mom_12_1 = np.prod(1 + returns[-252:-21]) - 1
        else:
            mom_12_1 = np.prod(1 + returns) - 1
        raw_momentum[t] = mom_12_1
        
        factor_scores[t] = {
            "market_beta": float(beta),
            "size": size_score,
            "value": value_score,
            "quality": q_score,
            "volatility": vol_score,
            "momentum_raw": mom_12_1
        }
        
    # Calculate Momentum Rank
    if raw_momentum:
        sorted_mom = sorted(raw_momentum.values())
        for t in tickers:
            if t in factor_scores:
                val = factor_scores[t]["momentum_raw"]
                rank = (sorted_mom.index(val) + 1) / len(sorted_mom)
                factor_scores[t]["momentum"] = rank
                
    return {"factor_scores": factor_scores}

async def calculate_portfolio_factor_exposures_node(state: FactorState) -> Dict[str, Any]:
    """Aggregate factors and sector weights to portfolio level."""
    if state.get("error"): return {}
    
    weights = state["portfolio_weights"]
    factor_scores = state["factor_scores"]
    
    portfolio_factors = defaultdict(float)
    total_weight = sum(weights.values())
    
    if total_weight == 0:
        return {"error": "Portfolio total weight is zero"}
        
    for ticker, weight in weights.items():
        if ticker not in factor_scores: continue
        
        norm_w = weight / total_weight
        for factor, score in factor_scores[ticker].items():
            if factor == "momentum_raw": continue
            portfolio_factors[factor] += norm_w * score
            
    # Sector Weights
    engine = create_engine(settings.postgres_url)
    sector_weights = defaultdict(float)
    with engine.connect() as conn:
        for ticker, weight in weights.items():
            res = conn.execute(text("SELECT sector FROM companies WHERE ticker = :t"), {"t": ticker}).fetchone()
            sector = res[0] if res and res[0] else "Unknown"
            sector_weights[sector] += weight
            
    portfolio_factors["sector_max"] = max(sector_weights.values()) if sector_weights else 0.0
    portfolio_factors["sector_breakdown"] = dict(sector_weights)
    
    return {"portfolio_factors": dict(portfolio_factors)}

async def check_factor_limits_node(state: FactorState) -> Dict[str, Any]:
    """Check for exposure breaches and calculate balance score."""
    if state.get("error"): return {}
    
    pf = state["portfolio_factors"]
    breaches = []
    
    if pf.get("market_beta", 0) > 1.3:
        breaches.append("HIGH_BETA_EXPOSURE")
    if pf.get("market_beta", 0) < 0.5:
        breaches.append("LOW_BETA_DEFENSIVE")
        
    if pf.get("sector_max", 0) > 0.35:
        breaches.append("SECTOR_CONCENTRATION")
        
    if pf.get("momentum", 0) > 0.8:
        breaches.append("MOMENTUM_CROWDING")
        
    # Factor Balance Score (simplified: std of main factor scores)
    main_factors = [pf.get("size", 0), pf.get("value", 0), pf.get("momentum", 0), pf.get("quality", 0), pf.get("volatility", 0)]
    balance_score = 1.0 - np.std(main_factors)
    pf["balance_score"] = float(balance_score)
    
    return {"factor_breaches": breaches, "portfolio_factors": pf}

async def suggest_adjustments_node(state: FactorState) -> Dict[str, Any]:
    """Use LLM to suggest weight tweaks if breaches are detected."""
    if state.get("error") or not state.get("factor_breaches"):
        return {"factor_adjustments": [], "adjusted_weights": state.get("portfolio_weights", {})}
        
    llm = LLMFactory.get_simple_llm()
    
    prompt = f"""
    You are a Factor Risk Analyst.
    Given these portfolio factor exposures: {json.dumps(state['portfolio_factors'])}
    Breaches detected: {state['factor_breaches']}
    Current weights: {json.dumps(state['portfolio_weights'])}
    Factor scores for individual tickers: {json.dumps(state['factor_scores'])}
    
    Suggest specific weight adjustments (2-3 changes only) to fix the factor breaches 
    while maintaining overall portfolio quality. Return ONLY a JSON object:
    {{
        "adjustments": [
            {{"ticker": "AAPL", "current_weight": 0.15, "suggested_weight": 0.12, "reason": "High beta reduction"}}
        ]
    }}
    """
    
    try:
        response = await llm.ainvoke(prompt)
        # Parse JSON from response
        text = response.content
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
            
        data = json.loads(text)
        adjustments = data.get("adjustments", [])
        
        # Apply adjustments to create adjusted_weights
        adjusted = state["portfolio_weights"].copy()
        for adj in adjustments:
            ticker = adj["ticker"]
            if ticker in adjusted:
                adjusted[ticker] = adj["suggested_weight"]
                
        # Re-normalize
        total = sum(adjusted.values())
        if total > 0:
            adjusted = {t: w/total for t, w in adjusted.items()}
            
        return {
            "factor_adjustments": adjustments,
            "adjusted_weights": adjusted
        }
    except Exception as e:
        logger.error(f"LLM Adjustment suggestion failed: {e}")
        return {"factor_adjustments": [], "adjusted_weights": state["portfolio_weights"]}

async def store_exposures_node(state: FactorState) -> Dict[str, Any]:
    """Write results to DB and Redis."""
    if state.get("error"): return {}
    
    pf = state["portfolio_factors"]
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    with Session() as session:
        # Get Portfolio ID
        stmt = select(Portfolio).where(Portfolio.name == "main_portfolio").limit(1)
        portfolio = session.execute(stmt).scalar_one_or_none()
        pf_id = portfolio.id if portfolio else None
        
        if pf_id:
            exposure = FactorExposure(
                portfolio_id=pf_id,
                snapshot_date=date.today(),
                market_beta=pf.get("market_beta", 1.0),
                size_factor=pf.get("size", 0.0),
                value_factor=pf.get("value", 0.0),
                momentum_factor=pf.get("momentum", 0.0),
                quality_factor=pf.get("quality", 0.0),
                volatility_factor=pf.get("volatility", 0.0),
                sector_weights=pf.get("sector_breakdown", {}),
                geographic_weights={"US": 1.0} # Placeholder
            )
            session.add(exposure)
            session.commit()
            
    # Redis Cache
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        r.setex("portfolio:factor:exposures", 3600, json.dumps(pf))
    except Exception as e:
        logger.error(f"Redis cache error in FactorAgent: {e}")
        
    return {}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class FactorResult:
    factors: Dict[str, float]
    breaches: List[str]
    adjustments: List[Dict[str, Any]]
    adjusted_weights: Dict[str, float]

class FactorAgent:
    """Agent orchestrator for factor risk analysis."""
    
    def __init__(self):
        self.workflow = StateGraph(FactorState)
        self.workflow.add_node("fetch_data", fetch_factor_data_node)
        self.workflow.add_node("calculate_security", calculate_security_factors_node)
        self.workflow.add_node("calculate_portfolio", calculate_portfolio_factor_exposures_node)
        self.workflow.add_node("check_limits", check_factor_limits_node)
        self.workflow.add_node("suggest_adjustments", suggest_adjustments_node)
        self.workflow.add_node("store", store_exposures_node)
        
        self.workflow.set_entry_point("fetch_data")
        self.workflow.add_edge("fetch_data", "calculate_security")
        self.workflow.add_edge("calculate_security", "calculate_portfolio")
        self.workflow.add_edge("calculate_portfolio", "check_limits")
        self.workflow.add_edge("check_limits", "suggest_adjustments")
        self.workflow.add_edge("suggest_adjustments", "store")
        self.workflow.add_edge("store", END)
        
        self.app = self.workflow.compile()
        
    async def analyze(self, weights: Dict[str, float]) -> Optional[FactorResult]:
        """Analyze portfolio factor exposures and suggest adjustments."""
        initial_state: FactorState = {
            "portfolio_weights": weights,
            "price_history": {},
            "factor_scores": {},
            "portfolio_factors": {},
            "factor_breaches": [],
            "factor_adjustments": [],
            "adjusted_weights": {},
            "error": None
        }
        
        try:
            final_state = await self.app.ainvoke(initial_state)
            if final_state.get("error"):
                logger.error(f"Factor analysis failed: {final_state['error']}")
                return None
                
            return FactorResult(
                factors=final_state["portfolio_factors"],
                breaches=final_state["factor_breaches"],
                adjustments=final_state["factor_adjustments"],
                adjusted_weights=final_state["adjusted_weights"]
            )
        except Exception as e:
            logger.exception("Error in FactorAgent.analyze")
            return None

    def get_factor_report(self) -> Dict[str, Any]:
        """Fetch latest factor exposures from Redis."""
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data = r.get("portfolio:factor:exposures")
            return json.loads(data) if data else {}
        except Exception:
            return {}

    def get_sector_breakdown(self) -> Dict[str, float]:
        """Fetch sector breakdown from latest factor analysis."""
        report = self.get_factor_report()
        return report.get("sector_breakdown", {})

    async def suggest_factor_neutral_weights(self, weights: Dict[str, float]) -> Dict[str, float]:
        """Wrapper for analyze that returns the adjusted weights."""
        result = await self.analyze(weights)
        return result.adjusted_weights if result else weights
