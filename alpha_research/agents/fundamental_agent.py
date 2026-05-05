"""
Fundamental Analysis Agent — LangGraph Pipeline
=================================================

Calculates financial ratios and fundamental scores for a ticker,
and generates an investment thesis using an LLM.

Graph flow::

    fetch_fundamentals → compute_ratios → score_factors →
        generate_thesis → store_scores → END
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypedDict

import httpx
import numpy as np
import pandas as pd
import redis
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from alpha_research.storage.research_models import FundamentalScore
from config.llm_config import reasoning_llm
from config.settings import settings


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State & Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FundamentalState(TypedDict):
    """Mutable state passed through the LangGraph pipeline."""
    ticker: str
    income_data: list[dict[str, Any]]
    balance_data: list[dict[str, Any]]
    company_profile: dict[str, Any]
    computed_ratios: dict[str, Any]
    factor_scores: dict[str, float]
    overall_score: float
    investment_thesis: str
    red_flags: list[str]
    green_flags: list[str]
    error: str | None


@dataclass(frozen=True, slots=True)
class FundamentalResult:
    """Immutable output returned to callers."""
    ticker: str
    overall_score: float
    factor_scores: dict[str, float]
    computed_ratios: dict[str, Any]
    investment_thesis: str
    red_flags: list[str]
    green_flags: list[str]
    error: str | None = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_API_BASE = "http://localhost:8000"
_API_HEADERS = {"x-api-key": settings.internal_api_key}

_THESIS_SYSTEM_PROMPT = (
    "You are a fundamental equity analyst at a hedge fund.\n"
    "Given these financial metrics for {ticker}, write a concise 3-sentence "
    "investment thesis covering:\n"
    "1. Key fundamental strength or weakness\n"
    "2. Growth outlook based on trends\n"
    "3. Key risk to the thesis\n"
    "Return JSON: {{\n"
    '  "thesis": "string",\n'
    '  "red_flags": ["str"],\n'
    '  "green_flags": ["str"],\n'
    '  "conviction": float (0-1)\n'
    "}}\n"
    "Max 3 flags per category. Return ONLY valid JSON."
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Nodes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_fundamentals_node(state: FundamentalState) -> dict[str, Any]:
    ticker = state["ticker"]
    
    async def fetch(path: str, params: dict) -> list | dict | None:
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    f"{_API_BASE}{path}",
                    params=params,
                    headers=_API_HEADERS,
                )
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:
            logger.warning(f"fetch {path} failed for {ticker}: {exc}")
            return None

    income, balance, summary = await asyncio.gather(
        fetch(f"/fundamentals/{ticker}/income", {"period": "quarterly", "limit": 8}),
        fetch(f"/fundamentals/{ticker}/balance", {"period": "quarterly", "limit": 8}),
        fetch(f"/fundamentals/{ticker}/summary", {})
    )

    if not income or not balance:
        return {"error": "Insufficient fundamental data"}

    return {
        "income_data": income or [],
        "balance_data": balance or [],
        "company_profile": summary or {}
    }


def _safe_float(val: Any) -> float:
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _safe_div(n: float, d: float) -> float:
    if d == 0 or pd.isna(d):
        return 0.0
    return n / d


async def compute_ratios_node(state: FundamentalState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    ticker = state["ticker"]
    summary = state.get("company_profile", {})
    
    # We sort by fiscal_date ascending so the last element is the newest
    income_df = pd.DataFrame(state["income_data"])
    if not income_df.empty and "fiscal_date" in income_df.columns:
        income_df["fiscal_date"] = pd.to_datetime(income_df["fiscal_date"])
        income_df = income_df.sort_values("fiscal_date").reset_index(drop=True)
    
    balance_df = pd.DataFrame(state["balance_data"])
    if not balance_df.empty and "fiscal_date" in balance_df.columns:
        balance_df["fiscal_date"] = pd.to_datetime(balance_df["fiscal_date"])
        balance_df = balance_df.sort_values("fiscal_date").reset_index(drop=True)

    ratios = {}
    
    # Valuations (primarily from summary)
    ratios["pe_ratio"] = _safe_float(summary.get("pe_ratio"))
    ratios["ps_ratio"] = _safe_float(summary.get("ps_ratio"))
    ratios["pb_ratio"] = _safe_float(summary.get("pb_ratio"))
    ratios["roe"] = _safe_float(summary.get("roe"))
    ratios["roa"] = _safe_float(summary.get("roa"))
    ratios["debt_to_equity"] = _safe_float(summary.get("debt_to_equity"))
    ratios["current_ratio"] = _safe_float(summary.get("current_ratio"))

    # Growth and Trends from time series (latest vs previous)
    if len(income_df) >= 2:
        latest = income_df.iloc[-1]
        prev = income_df.iloc[-2]
        
        # Revenue
        rev_now = _safe_float(latest.get("revenue"))
        rev_prev = _safe_float(prev.get("revenue"))
        ratios["revenue_growth_qoq"] = _safe_div(rev_now - rev_prev, rev_prev)
        
        if len(income_df) >= 5: # YoY requires 4 quarters back
            prev_yr = income_df.iloc[-5]
            rev_yr_prev = _safe_float(prev_yr.get("revenue"))
            ratios["revenue_growth_yoy"] = _safe_div(rev_now - rev_yr_prev, rev_yr_prev)
            
            eps_now = _safe_div(_safe_float(latest.get("net_income")), 1e9) # simplified EPS proxy if missing shares
            eps_yr_prev = _safe_div(_safe_float(prev_yr.get("net_income")), 1e9)
            if eps_yr_prev != 0:
                ratios["eps_growth_yoy"] = _safe_div(eps_now - eps_yr_prev, abs(eps_yr_prev))
            else:
                ratios["eps_growth_yoy"] = 0.0
        
        # Margins
        gross_now = _safe_float(latest.get("gross_profit"))
        op_now = _safe_float(latest.get("operating_income"))
        
        ratios["gross_margin"] = _safe_div(gross_now, rev_now)
        ratios["operating_margin"] = _safe_div(op_now, rev_now)
        
        # Free Cash Flow Margin Proxy (Net Income + D&A - CapEx), simplifying here since we may not have CF statement
        ratios["fcf_margin"] = _safe_div(_safe_float(latest.get("net_income")), rev_now)

    # Convert back to clean floats
    clean_ratios = {k: (float(v) if not pd.isna(v) else 0.0) for k, v in ratios.items()}
    return {"computed_ratios": clean_ratios}


async def score_factors_node(state: FundamentalState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    r = state.get("computed_ratios", {})

    # 1. Value Score
    pe = r.get("pe_ratio", 0)
    pe_score = 0.5
    if pe > 0:
        if pe < 15: pe_score = 1.0
        elif pe <= 25: pe_score = 0.7
        elif pe > 40: pe_score = 0.2

    ps = r.get("ps_ratio", 0)
    ps_score = 0.5
    if ps > 0:
        if ps < 2: ps_score = 1.0
        elif ps <= 5: ps_score = 0.6
        elif ps > 10: ps_score = 0.2

    value_score = (pe_score + ps_score) / 2.0

    # 2. Growth Score
    rev_growth = r.get("revenue_growth_yoy", 0)
    if rev_growth > 0.20:
        growth_score = 1.0
    elif rev_growth > 0.10:
        growth_score = 0.7
    elif rev_growth > 0.0:
        growth_score = 0.4
    else:
        growth_score = 0.1

    # 3. Quality Score
    roe = r.get("roe", 0)
    if roe > 0.20:
        roe_score = 1.0
    elif roe > 0.10:
        roe_score = 0.7
    else:
        roe_score = 0.2

    de = r.get("debt_to_equity", 0)
    if de <= 0: de_score = 0.5 # missing or zero
    elif de < 0.5: de_score = 1.0
    elif de <= 1.5: de_score = 0.6
    else: de_score = 0.2

    quality_score = (roe_score + de_score) / 2.0

    overall = (value_score * 0.3) + (growth_score * 0.4) + (quality_score * 0.3)

    factors = {
        "value": value_score,
        "growth": growth_score,
        "quality": quality_score
    }

    return {"factor_scores": factors, "overall_score": overall}


async def generate_thesis_node(state: FundamentalState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    ticker = state["ticker"]
    ratios = state.get("computed_ratios", {})
    factors = state.get("factor_scores", {})

    prompt = _THESIS_SYSTEM_PROMPT.format(ticker=ticker)
    context = {
        "ratios": ratios,
        "scores": factors,
        "overall_score": state.get("overall_score")
    }

    messages = [
        SystemMessage(content=prompt),
        HumanMessage(content=f"Generate thesis for {ticker} based on: {json.dumps(context)}")
    ]

    thesis = "Failed to generate thesis."
    reds = []
    greens = []

    try:
        response = await reasoning_llm.ainvoke(messages)
        text = response.content if hasattr(response, "content") else str(response)

        # Clean code fences
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            data = json.loads(text[start:end+1])
            thesis = data.get("thesis", thesis)
            reds = data.get("red_flags", [])[:3]
            greens = data.get("green_flags", [])[:3]
            
    except Exception as exc:
        logger.warning(f"LLM thesis generation failed for {ticker}: {exc}")

    return {
        "investment_thesis": thesis,
        "red_flags": reds,
        "green_flags": greens
    }


async def store_scores_node(state: FundamentalState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    ticker = state["ticker"]
    now = datetime.now(timezone.utc)
    factors = state.get("factor_scores", {})

    # DB Persistence
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        with session_factory() as session:
            record = FundamentalScore(
                id=uuid.uuid4(),
                ticker=ticker,
                score_type="composite",
                overall_score=state["overall_score"],
                pe_score=factors.get("value"),
                growth_score=factors.get("growth"),
                margin_score=0.5, # Optional sub-score
                debt_score=0.5,   # Optional sub-score
                roe_score=factors.get("quality"),
                details={
                    "ratios": state.get("computed_ratios", {}),
                    "thesis": state.get("investment_thesis"),
                    "red_flags": state.get("red_flags"),
                    "green_flags": state.get("green_flags")
                },
                scored_at=now
            )
            session.add(record)
            session.commit()
        engine.dispose()
    except Exception as exc:
        logger.error("Failed to store FundamentalScore for {}: {}", ticker, exc)

    # Redis Cache and Pub/Sub
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        cache_val = {
            "overall_score": state["overall_score"],
            "value": factors.get("value"),
            "growth": factors.get("growth"),
            "quality": factors.get("quality"),
            "thesis": state.get("investment_thesis")
        }
        r.setex(f"fundamental:score:{ticker}", 3600, json.dumps(cache_val))
        
        pub_payload = {
            "ticker": ticker,
            "overall_score": state["overall_score"],
            "timestamp": now.isoformat()
        }
        r.publish("research.fundamental.updated", json.dumps(pub_payload))
        r.close()
    except Exception as exc:
        logger.warning("Redis publish failed for {}: {}", ticker, exc)

    return {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Graph Construction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_fundamental_graph() -> StateGraph:
    graph = StateGraph(FundamentalState)
    graph.add_node("fetch_fundamentals", fetch_fundamentals_node)
    graph.add_node("compute_ratios", compute_ratios_node)
    graph.add_node("score_factors", score_factors_node)
    graph.add_node("generate_thesis", generate_thesis_node)
    graph.add_node("store_scores", store_scores_node)

    graph.set_entry_point("fetch_fundamentals")
    graph.add_edge("fetch_fundamentals", "compute_ratios")
    graph.add_edge("compute_ratios", "score_factors")
    graph.add_edge("score_factors", "generate_thesis")
    graph.add_edge("generate_thesis", "store_scores")
    graph.add_edge("store_scores", END)
    
    return graph


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FundamentalAgent:
    def __init__(self) -> None:
        self._graph = _build_fundamental_graph().compile()
        logger.info("FundamentalAgent initialised")

    async def analyze(self, ticker: str) -> FundamentalResult:
        logger.info(f"Analyzing fundamentals for {ticker}")
        initial_state: FundamentalState = {
            "ticker": ticker.upper(),
            "income_data": [],
            "balance_data": [],
            "company_profile": {},
            "computed_ratios": {},
            "factor_scores": {},
            "overall_score": 0.0,
            "investment_thesis": "",
            "red_flags": [],
            "green_flags": [],
            "error": None
        }
        
        final_state = await self._graph.ainvoke(initial_state)
        
        return FundamentalResult(
            ticker=ticker.upper(),
            overall_score=final_state.get("overall_score", 0.0),
            factor_scores=final_state.get("factor_scores", {}),
            computed_ratios=final_state.get("computed_ratios", {}),
            investment_thesis=final_state.get("investment_thesis", ""),
            red_flags=final_state.get("red_flags", []),
            green_flags=final_state.get("green_flags", []),
            error=final_state.get("error")
        )

    async def analyze_batch(self, tickers: list[str]) -> dict[str, FundamentalResult]:
        tasks = [self.analyze(t) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        output = {}
        for t, r in zip(tickers, results):
            if isinstance(r, Exception):
                logger.error(f"Failed {t}: {r}")
                output[t.upper()] = FundamentalResult(
                    ticker=t.upper(), overall_score=0.0, factor_scores={}, 
                    computed_ratios={}, investment_thesis="", red_flags=[], 
                    green_flags=[], error=str(r)
                )
            else:
                output[t.upper()] = r
        return output

    async def get_top_quality_stocks(self, tickers: list[str], min_score: float = 0.7) -> list[str]:
        results = await self.analyze_batch(tickers)
        top = []
        for t, res in results.items():
            if res.error:
                continue
            if res.factor_scores.get("quality", 0) >= min_score:
                top.append(t)
        return top

    async def get_value_stocks(self, tickers: list[str], max_pe: float = 20.0) -> list[str]:
        results = await self.analyze_batch(tickers)
        value_stocks = []
        for t, res in results.items():
            if res.error:
                continue
            pe = res.computed_ratios.get("pe_ratio", float("inf"))
            if 0 < pe <= max_pe:
                value_stocks.append(t)
        return value_stocks

    async def screen(self, tickers: list[str], min_growth: float = 0.1, min_quality: float = 0.6) -> list[str]:
        results = await self.analyze_batch(tickers)
        passed = []
        for t, res in results.items():
            if res.error:
                continue
            g = res.factor_scores.get("growth", 0)
            q = res.factor_scores.get("quality", 0)
            if g >= min_growth and q >= min_quality:
                passed.append(t)
        return passed
