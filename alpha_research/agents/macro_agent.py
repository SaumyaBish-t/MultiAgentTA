"""
Macro Research Agent — LangGraph Pipeline
===========================================

Interprets macro-economic indicators, determines the market regime,
and infers sector and ticker-level implications.

Graph flow::

    fetch_macro_data → detect_regime → generate_signals →
        assess_sector_implications → assess_ticker_implications →
        store_signals → END
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, TypedDict

import httpx
import redis
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from alpha_research.storage.research_models import MacroSignal
from config.llm_config import research_llm
from config.settings import settings
from data_ingestion.storage.models import Company


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State & Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MacroState(TypedDict):
    """Mutable state passed through the LangGraph pipeline."""
    macro_data: dict[str, float]
    regime: str
    yield_curve: str
    fed_stance: str
    signals: list[dict[str, Any]]
    sector_implications: dict[str, str]
    ticker_implications: dict[str, float]
    risk_level: str
    error: str | None


@dataclass(frozen=True, slots=True)
class MacroResult:
    """Immutable output returned to callers."""
    regime: str
    yield_curve: str
    fed_stance: str
    signals: list[dict[str, Any]]
    sector_implications: dict[str, str]
    ticker_implications: dict[str, float]
    risk_level: str
    error: str | None = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_API_BASE = "http://localhost:8000"
_API_HEADERS = {"x-api-key": settings.internal_api_key}

_SIGNAL_SYSTEM_PROMPT = (
    "You are a macro strategist at a hedge fund.\n"
    "Given current macro indicators: {macro_data}\n"
    "Market regime: {regime}\n"
    "Yield curve: {yield_curve}\n"
    "Fed stance: {fed_stance}\n\n"
    "Identify 3-5 actionable macro signals.\n"
    "Return JSON array: [{{\n"
    '  "signal_name": "string",\n'
    '  "signal_direction": "bullish" | "bearish" | "neutral",\n'
    '  "severity": "low" | "medium" | "high" | "critical",\n'
    '  "description": "string (1-2 sentences)",\n'
    '  "affected_sectors": ["str"],\n'
    '  "timeframe": "string"\n'
    "}}]\n"
    "Return ONLY valid JSON array."
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Nodes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_macro_data_node(state: MacroState) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{_API_BASE}/macro/snapshot",
                headers=_API_HEADERS,
            )
            resp.raise_for_status()
            snapshot = resp.json()
    except Exception as exc:
        logger.warning("fetch_macro_data failed: {}", exc)
        return {"error": f"API error: {exc}"}

    # Extract raw values from the response objects
    macro_data = {}
    for series_id, obs in snapshot.items():
        macro_data[series_id] = obs.get("value", 0.0)

    # Ensure required defaults if missing
    for req in ["FEDFUNDS", "DGS10", "T10Y2Y", "CPIAUCSL", "UNRATE", "VIXCLS", "GDP"]:
        if req not in macro_data:
            macro_data[req] = 0.0

    return {"macro_data": macro_data}


async def detect_regime_node(state: MacroState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    data = state["macro_data"]
    
    t10y2y = data.get("T10Y2Y", 0.0)
    if t10y2y > 1.0:
        yield_curve = "normal"
    elif t10y2y >= 0.0:
        yield_curve = "flat"
    else:
        yield_curve = "inverted"

    # Simplified hawkish/dovish heuristic since we only have point-in-time here
    # (In a real system, we'd compare current vs historical to see "rising" or "falling")
    # For now, we use absolute thresholds to mimic the rule-based logic
    fedfunds = data.get("FEDFUNDS", 0.0)
    cpi = data.get("CPIAUCSL", 0.0) # Assume YoY % is what we receive or calculate
    
    if fedfunds > 4.0 and cpi > 3.0:
        fed_stance = "hawkish"
    elif fedfunds < 2.0:
        fed_stance = "dovish"
    else:
        fed_stance = "neutral"

    gdp = data.get("GDP", 0.0)
    unrate = data.get("UNRATE", 0.0)
    vix = data.get("VIXCLS", 0.0)

    if gdp > 2.0 and unrate < 5.0 and vix < 20.0:
        regime = "bull"
    elif gdp < 0.0 and unrate > 4.0:
        regime = "recession"
    elif gdp < 2.0 and cpi > 4.0:
        regime = "stagflation"
    elif gdp > 0.0 and vix < 25.0:
        regime = "recovery"
    else:
        regime = "uncertain"

    # Risk Level
    if vix > 30.0 or yield_curve == "inverted":
        risk_level = "extreme"
    elif vix > 20.0 or cpi > 5.0:
        risk_level = "high"
    elif regime == "bull":
        risk_level = "low"
    else:
        risk_level = "medium"

    return {
        "regime": regime,
        "yield_curve": yield_curve,
        "fed_stance": fed_stance,
        "risk_level": risk_level,
    }


async def generate_signals_node(state: MacroState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    prompt = _SIGNAL_SYSTEM_PROMPT.format(
        macro_data=json.dumps(state.get("macro_data", {})),
        regime=state.get("regime", "unknown"),
        yield_curve=state.get("yield_curve", "unknown"),
        fed_stance=state.get("fed_stance", "unknown"),
    )

    messages = [
        SystemMessage(content=prompt),
        HumanMessage(content="Generate the top macro signals.")
    ]

    signals = []
    try:
        response = await research_llm.ainvoke(messages)
        text = response.content if hasattr(response, "content") else str(response)

        # Clean code fences
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]

        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1:
            data = json.loads(text[start:end+1])
            if isinstance(data, list):
                signals = data
                
    except Exception as exc:
        logger.warning("LLM macro signal generation failed: {}", exc)

    return {"signals": signals}


async def assess_sector_implications_node(state: MacroState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    regime = state.get("regime", "")
    fed_stance = state.get("fed_stance", "")
    yield_curve = state.get("yield_curve", "")
    
    sector_impl = {}

    # Regime rules
    if regime == "bull":
        sector_impl["Technology"] = "bullish"
        sector_impl["Consumer Discretionary"] = "bullish"
        sector_impl["Financials"] = "bullish"
        sector_impl["Utilities"] = "neutral"
    elif regime == "recession":
        sector_impl["Consumer Staples"] = "bullish"
        sector_impl["Healthcare"] = "bullish"
        sector_impl["Utilities"] = "bullish"
        sector_impl["Technology"] = "bearish"
        sector_impl["Consumer Discretionary"] = "bearish"
        sector_impl["Financials"] = "bearish"

    # Fed rules override
    if fed_stance == "hawkish":
        sector_impl["Financials"] = "bullish"
        sector_impl["Real Estate"] = "bearish"
        sector_impl["Utilities"] = "bearish"
        # We don't explicitly have a "Growth stocks" sector, but Tech is a proxy
        sector_impl["Technology"] = "bearish"

    # Yield curve rule
    if yield_curve == "inverted":
        # Add a flag, but since sector_implications is dict[str, str], we append it to the string or just note it
        for k, v in sector_impl.items():
            if v == "bearish":
                sector_impl[k] = "bearish (RECESSION_RISK)"

    # Default others to neutral
    return {"sector_implications": sector_impl}


async def assess_ticker_implications_node(state: MacroState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    sector_impl = state.get("sector_implications", {})
    vix = state.get("macro_data", {}).get("VIXCLS", 20.0)
    
    ticker_scores = {}
    
    # DB read for sectors
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        
        tickers = settings.tickers
        
        with session_factory() as session:
            # Query all companies in our ticker list
            stmt = select(Company).where(Company.ticker.in_(tickers))
            companies = session.execute(stmt).scalars().all()
            
            for comp in companies:
                sector = comp.sector or "Unknown"
                bias = sector_impl.get(sector, "neutral")
                
                score = 0.0
                if "bullish" in bias:
                    score = 0.5
                elif "bearish" in bias:
                    score = -0.5
                
                # High VIX adjustment (drags everything down)
                if vix > 30.0:
                    score -= 0.3
                elif vix > 20.0:
                    score -= 0.1
                    
                ticker_scores[comp.ticker] = max(-1.0, min(1.0, score))
                
        engine.dispose()
    except Exception as exc:
        logger.error("Failed to map tickers to macro scores: {}", exc)

    return {"ticker_implications": ticker_scores}


async def store_signals_node(state: MacroState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    now = datetime.now(timezone.utc)
    
    # DB Persistence
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        with session_factory() as session:
            try:
                for s in state.get("signals", []):
                    aff_sectors = s.get("affected_sectors", [])
                    
                    record = MacroSignal(
                        id=uuid.uuid4(),
                        signal_name=s.get("signal_name", "MacroEvent")[:60],
                        signal_value=0.0,
                        signal_direction=s.get("signal_direction", "neutral"),
                        affected_sectors={"sectors": aff_sectors},
                        affected_tickers={"implications": state.get("ticker_implications", {})},
                        severity=s.get("severity", "medium"),
                        description=s.get("description", ""),
                        detected_at=now,
                        expires_at=now + timedelta(days=30)
                    )
                    session.add(record)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
        engine.dispose()
    except Exception as exc:
        logger.error("Failed to store MacroSignal to DB: {}", exc)

    # Redis Cache and Pub/Sub
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        
        payload = {
            "regime": state.get("regime", "uncertain"),
            "fed_stance": state.get("fed_stance", "neutral"),
            "yield_curve": state.get("yield_curve", "normal"),
            "risk_level": state.get("risk_level", "medium"),
            "timestamp": now.isoformat()
        }
        
        r.setex("macro:regime:current", 3600, json.dumps(payload))
        r.publish("research.macro.updated", json.dumps(payload))
        r.close()
    except Exception as exc:
        logger.warning("Redis publish failed for macro: {}", exc)

    return {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Graph Construction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_macro_graph() -> StateGraph:
    graph = StateGraph(MacroState)
    graph.add_node("fetch_macro_data", fetch_macro_data_node)
    graph.add_node("detect_regime", detect_regime_node)
    graph.add_node("generate_signals", generate_signals_node)
    graph.add_node("assess_sector_implications", assess_sector_implications_node)
    graph.add_node("assess_ticker_implications", assess_ticker_implications_node)
    graph.add_node("store_signals", store_signals_node)

    graph.set_entry_point("fetch_macro_data")
    graph.add_edge("fetch_macro_data", "detect_regime")
    graph.add_edge("detect_regime", "generate_signals")
    graph.add_edge("generate_signals", "assess_sector_implications")
    graph.add_edge("assess_sector_implications", "assess_ticker_implications")
    graph.add_edge("assess_ticker_implications", "store_signals")
    graph.add_edge("store_signals", END)
    
    return graph


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MacroAgent:
    def __init__(self) -> None:
        self._graph = _build_macro_graph().compile()
        logger.info("MacroAgent initialised")

    async def analyze(self) -> MacroResult:
        logger.info("Running macro analysis pipeline")
        initial_state: MacroState = {
            "macro_data": {},
            "regime": "uncertain",
            "yield_curve": "normal",
            "fed_stance": "neutral",
            "signals": [],
            "sector_implications": {},
            "ticker_implications": {},
            "risk_level": "medium",
            "error": None
        }
        
        final_state = await self._graph.ainvoke(initial_state)
        
        return MacroResult(
            regime=final_state.get("regime", "uncertain"),
            yield_curve=final_state.get("yield_curve", "normal"),
            fed_stance=final_state.get("fed_stance", "neutral"),
            signals=final_state.get("signals", []),
            sector_implications=final_state.get("sector_implications", {}),
            ticker_implications=final_state.get("ticker_implications", {}),
            risk_level=final_state.get("risk_level", "medium"),
            error=final_state.get("error")
        )

    def get_current_regime(self) -> str:
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data = r.get("macro:regime:current")
            r.close()
            if data:
                return json.loads(data).get("regime", "uncertain")
        except Exception:
            pass
        return "uncertain"

    def get_sector_bias(self, sector: str) -> str:
        # Without running the graph again, we'd ideally cache this too.
        # Returning a placeholder based on cached regime for simplicity if needed.
        regime = self.get_current_regime()
        if regime == "bull" and sector in ["Technology", "Consumer Discretionary", "Financials"]:
            return "bullish"
        elif regime == "recession" and sector in ["Consumer Staples", "Healthcare", "Utilities"]:
            return "bullish"
        return "neutral"

    def get_ticker_macro_score(self, ticker: str) -> float:
        # Similarly, would ideally hit Redis cache. Returning 0.0 as default placeholder.
        return 0.0

    def is_risk_on(self) -> bool:
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            data = r.get("macro:regime:current")
            r.close()
            if data:
                payload = json.loads(data)
                return payload.get("risk_level") == "low" and payload.get("regime") == "bull"
        except Exception:
            pass
        return False
