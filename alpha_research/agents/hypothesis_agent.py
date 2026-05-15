"""
Hypothesis Generator Agent — LangGraph Pipeline
===============================================

Synthesizes outputs from Sentiment, Technical, Fundamental, Macro,
and Document intelligence agents into actionable trade hypotheses.

Graph flow::

    gather_research → assess_signal_alignment
      ↳ if conflicting: store_hypothesis (rejected) → END
      ↳ else: generate_hypothesis → validate_hypothesis → store_hypothesis → END
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypedDict, cast

import redis
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from loguru import logger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from alpha_research.storage.research_models import ResearchHypothesis as DBHypothesis
from config.llm_config import reasoning_llm
from config.settings import settings

# Import sub-agents for fallback cache misses
from alpha_research.agents.sentiment_agent import SentimentAgent
from alpha_research.agents.technical_agent import TechnicalAgent
from alpha_research.agents.fundamental_agent import FundamentalAgent
from alpha_research.agents.macro_agent import MacroAgent
from alpha_research.agents.document_agent import DocumentAgent


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State & Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class HypothesisState(TypedDict):
    """Mutable state passed through the LangGraph pipeline."""
    ticker: str
    sentiment_result: dict[str, Any]
    technical_result: dict[str, Any]
    fundamental_result: dict[str, Any]
    macro_result: dict[str, Any]
    document_result: dict[str, Any]
    signal_alignment: str
    hypothesis: dict[str, Any] | None
    conviction_score: float
    rejection_reason: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class Hypothesis:
    """Immutable output returned to callers."""
    ticker: str
    title: str
    description: str
    direction: str
    timeframe: str
    conviction_score: float
    key_catalysts: list[str]
    key_risks: list[str]
    invalidation_conditions: list[str]
    status: str
    created_at: datetime
    expires_at: datetime


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_HYPOTHESIS_SYSTEM_PROMPT = (
    "You are a senior portfolio manager at a quantitative hedge fund.\n"
    "Given this multi-factor research for {ticker}:\n\n"
    "Sentiment: {sentiment_summary}\n"
    "Technical: {technical_summary}\n"
    "Fundamental: {fundamental_summary}\n"
    "Macro: {macro_summary}\n"
    "Document insights: {document_summary}\n"
    "Signal alignment: {alignment}\n\n"
    "Generate a precise, actionable trade hypothesis.\n"
    "Return JSON: {{\n"
    '  "title": "str (max 10 words)",\n'
    '  "description": "str (3-4 sentences with reasoning)",\n'
    '  "expected_direction": "long" | "short" | "neutral",\n'
    '  "expected_timeframe": "intraday" | "swing" | "position",\n'
    '  "conviction_score": float (0.0-1.0),\n'
    '  "key_catalysts": ["str"] (max 3),\n'
    '  "key_risks": ["str"] (max 3),\n'
    '  "invalidation_conditions": ["str"] (when thesis is wrong),\n'
    '  "price_targets": {{\n'
    '    "entry_zone": "str",\n'
    '    "target": "str",\n'
    '    "stop_loss": "str"\n'
    '  }}\n'
    "}}\n"
    "Generate a hypothesis even with limited data. Assess conviction honestly.\n"
    "If overall conviction is strictly < 0.3, return null instead of a dictionary. "
    "Return ONLY valid JSON."
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Nodes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def gather_research_node(state: HypothesisState) -> dict[str, Any]:
    ticker = state["ticker"]
    r = redis.from_url(settings.redis_url, decode_responses=True)
    
    # Check cache first
    sent_raw = r.get(f"sentiment:latest:{ticker}")
    tech_raw = r.get(f"technical:bias:{ticker}:1d")
    fund_raw = r.get(f"fundamental:score:{ticker}")
    macr_raw = r.get("macro:regime:current")
    docs_raw = r.get(f"document:insights:{ticker}")
    r.close()

    # Parse what we have
    sentiment = json.loads(sent_raw) if sent_raw else {}
    technical = json.loads(tech_raw) if tech_raw else {}
    fundamental = json.loads(fund_raw) if fund_raw else {}
    macro = json.loads(macr_raw) if macr_raw else {}
    # Document insights is just a string summary in Redis
    document = {"summary": docs_raw} if docs_raw else {}

    # Handle Misses by running agents
    if not sentiment:
        logger.info(f"Cache miss for {ticker} sentiment, running agent...")
        s_res = await SentimentAgent().analyze(ticker)
        sentiment = s_res.to_dict()

    if not technical:
        logger.info(f"Cache miss for {ticker} technical, running agent...")
        t_res = await TechnicalAgent().analyze(ticker)
        technical = {"bias": t_res.bias, "signals_count": len(t_res.signals)}

    if not fundamental:
        logger.info(f"Cache miss for {ticker} fundamental, running agent...")
        f_res = await FundamentalAgent().analyze(ticker)
        fundamental = {
            "overall_score": f_res.overall_score, 
            "value": f_res.factor_scores.get("value", 0),
            "growth": f_res.factor_scores.get("growth", 0),
            "quality": f_res.factor_scores.get("quality", 0)
        }

    if not macro:
        logger.info("Cache miss for macro regime, running agent...")
        m_res = await MacroAgent().analyze()
        macro = {"regime": m_res.regime, "risk_level": m_res.risk_level, "fed_stance": m_res.fed_stance}

    if not docs_raw:
        logger.info(f"Cache miss for {ticker} documents, running agent...")
        d_res = await DocumentAgent().research(ticker)
        document = {"summary": d_res.summary}

    return {
        "sentiment_result": sentiment,
        "technical_result": technical,
        "fundamental_result": fundamental,
        "macro_result": macro,
        "document_result": document
    }


async def assess_signal_alignment_node(state: HypothesisState) -> dict[str, Any]:
    bullish_count = 0
    bearish_count = 0
    neutral_count = 0

    # Sentiment bias
    s_label = state["sentiment_result"].get("label", "neutral")
    if s_label == "bullish": bullish_count += 1
    elif s_label == "bearish": bearish_count += 1
    else: neutral_count += 1

    # Technical bias
    t_bias = state["technical_result"].get("bias", "neutral")
    if t_bias == "bullish": bullish_count += 1
    elif t_bias == "bearish": bearish_count += 1
    else: neutral_count += 1

    # Fundamental (score > 0.6 is bullish, < 0.4 is bearish)
    f_score = state["fundamental_result"].get("overall_score", 0.5)
    if f_score > 0.6: bullish_count += 1
    elif f_score < 0.4: bearish_count += 1
    else: neutral_count += 1

    # Macro regime
    m_regime = state["macro_result"].get("regime", "uncertain")
    if m_regime in ["bull", "recovery"]: bullish_count += 1
    elif m_regime in ["recession", "stagflation"]: bearish_count += 1
    else: neutral_count += 1

    total_signals = bullish_count + bearish_count + neutral_count
    
    if bullish_count >= 3 and bearish_count <= 1:
        alignment = "strongly_aligned_bullish"
    elif bearish_count >= 3 and bullish_count <= 1:
        alignment = "strongly_aligned_bearish"
    elif bullish_count >= 2 and bearish_count == 0:
        alignment = "moderately_aligned_bullish"
    elif bearish_count >= 2 and bullish_count == 0:
        alignment = "moderately_aligned_bearish"
    elif bullish_count >= 2 and bearish_count >= 2:
        # Only truly conflicting when strong signals on BOTH sides
        alignment = "conflicting"
    else:
        # Mixed or limited data — let the LLM decide with conviction scoring
        alignment = "mixed"

    logger.debug(f"Signal alignment for {state['ticker']}: {alignment} (Bull: {bullish_count}, Bear: {bearish_count}, Neutral: {neutral_count})")
    
    return {"signal_alignment": alignment}


async def generate_hypothesis_node(state: HypothesisState) -> dict[str, Any]:
    # We do not run LLM if signals are conflicting entirely
    if state["signal_alignment"] == "conflicting":
        return {"hypothesis": None, "rejection_reason": "CONFLICTING_SIGNALS"}

    ticker = state["ticker"]
    prompt = _HYPOTHESIS_SYSTEM_PROMPT.format(
        ticker=ticker,
        sentiment_summary=json.dumps(state["sentiment_result"]),
        technical_summary=json.dumps(state["technical_result"]),
        fundamental_summary=json.dumps(state["fundamental_result"]),
        macro_summary=json.dumps(state["macro_result"]),
        document_summary=json.dumps(state["document_result"]),
        alignment=state["signal_alignment"]
    )

    messages = [
        SystemMessage(content=prompt),
        HumanMessage(content=f"Generate trade hypothesis for {ticker}")
    ]

    try:
        response = await reasoning_llm.ainvoke(messages)
        text = response.content if hasattr(response, "content") else str(response)

        # Clean JSON fences
        if "```json" in text: text = text.split("```json")[1].split("```")[0]
        elif "```" in text: text = text.split("```")[1].split("```")[0]

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            data = json.loads(text[start:end+1])
            if isinstance(data, dict):
                return {
                    "hypothesis": data,
                    "conviction_score": data.get("conviction_score", 0.0)
                }
    except Exception as exc:
        logger.warning(f"LLM Hypothesis generation failed for {ticker}: {exc}")

    return {"hypothesis": None, "rejection_reason": "LLM_GENERATION_FAILED"}


async def validate_hypothesis_node(state: HypothesisState) -> dict[str, Any]:
    if state.get("rejection_reason") or not state.get("hypothesis"):
        return {}

    h = state["hypothesis"]
    direction = h.get("expected_direction", "neutral")
    timeframe = h.get("expected_timeframe", "swing")
    conviction = float(h.get("conviction_score", 0.0))
    
    m_regime = state["macro_result"].get("regime", "uncertain")
    vix_level = state["macro_result"].get("risk_level", "medium")
    f_score = state["fundamental_result"].get("overall_score", 0.5)

    reason = None
    if conviction < 0.3:
        reason = "LOW_CONVICTION"
    elif m_regime == "recession" and direction == "long":
        reason = "MACRO_HEADWIND"
    elif f_score < 0.3 and direction == "long":
        reason = "WEAK_FUNDAMENTALS"
    elif vix_level == "extreme" and timeframe == "intraday":
        reason = "HIGH_VOLATILITY_REGIME"

    return {"rejection_reason": reason}


async def store_hypothesis_node(state: HypothesisState) -> dict[str, Any]:
    ticker = state["ticker"]
    reason = state.get("rejection_reason")
    h = state.get("hypothesis")
    
    now = datetime.now(timezone.utc)
    exp = now
    
    if reason or not h:
        status = "rejected"
        title = f"Rejected: {reason}"
        desc = "Hypothesis generation rejected by rules."
        conviction = 0.0
        direction = "neutral"
        timeframe = "n/a"
        catalysts = []
        risks = []
        invalidations = []
        
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            r.publish("research.hypothesis.rejected", json.dumps({"ticker": ticker, "reason": reason}))
            r.close()
        except Exception:
            pass
    else:
        status = "pending"
        title = h.get("title", f"{ticker} Hypothesis")
        desc = h.get("description", "")
        conviction = float(h.get("conviction_score", 0.0))
        direction = h.get("expected_direction", "neutral")
        timeframe = h.get("expected_timeframe", "swing")
        catalysts = h.get("key_catalysts", [])
        risks = h.get("key_risks", [])
        invalidations = h.get("invalidation_conditions", [])
        
        # Determine expiry based on timeframe
        if timeframe == "intraday": exp = now + timedelta(days=1)
        elif timeframe == "swing": exp = now + timedelta(days=14)
        else: exp = now + timedelta(days=90)
        
        if conviction > 0.8:
            try:
                r = redis.from_url(settings.redis_url, decode_responses=True)
                r.publish("research.hypothesis.high_conviction", json.dumps({
                    "ticker": ticker,
                    "direction": direction,
                    "conviction": conviction,
                    "title": title,
                    "timestamp": now.isoformat()
                }))
                r.close()
            except Exception:
                pass

    logger.info(f"Generated hypothesis for {ticker}: {direction} | conviction: {conviction} | status: {status}")

    # DB Persistence
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        with session_factory() as session:
            record = DBHypothesis(
                id=uuid.uuid4(),
                ticker=ticker,
                hypothesis_type="composite",
                title=title[:300], # Model allows 300
                description=desc,
                conviction_score=conviction,
                expected_direction=direction,
                expected_timeframe=timeframe,
                status=status,
                supporting_signals={
                    "catalysts": catalysts,
                    "price_targets": h.get("price_targets", {}) if h else {}
                },
                contradicting_signals={"risks": risks},
                data_sources_used={
                    "sentiment": bool(state.get("sentiment_result")),
                    "technical": bool(state.get("technical_result")),
                    "fundamental": bool(state.get("fundamental_result")),
                    "macro": bool(state.get("macro_result")),
                    "documents": bool(state.get("document_result"))
                },
                created_by_agent="HypothesisAgent",
                created_at=now,
                updated_at=now,
                expires_at=exp if not reason else now
            )
            session.add(record)
            session.commit()
        engine.dispose()
    except Exception as exc:
        logger.error(f"Failed to store ResearchHypothesis for {ticker}: {exc}")

    return {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Routing & Graph Construction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _route_alignment(state: HypothesisState) -> str:
    if state["signal_alignment"] == "conflicting":
        return "store_hypothesis"
    return "generate_hypothesis"


def _build_hypothesis_graph() -> StateGraph:
    graph = StateGraph(HypothesisState)
    graph.add_node("gather_research", gather_research_node)
    graph.add_node("assess_signal_alignment", assess_signal_alignment_node)
    graph.add_node("generate_hypothesis", generate_hypothesis_node)
    graph.add_node("validate_hypothesis", validate_hypothesis_node)
    graph.add_node("store_hypothesis", store_hypothesis_node)

    graph.set_entry_point("gather_research")
    graph.add_edge("gather_research", "assess_signal_alignment")
    
    # Conditional edge
    graph.add_conditional_edges(
        "assess_signal_alignment",
        _route_alignment,
        {
            "store_hypothesis": "store_hypothesis",
            "generate_hypothesis": "generate_hypothesis"
        }
    )
    
    graph.add_edge("generate_hypothesis", "validate_hypothesis")
    graph.add_edge("validate_hypothesis", "store_hypothesis")
    graph.add_edge("store_hypothesis", END)
    
    return graph


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class HypothesisAgent:
    def __init__(self) -> None:
        self._graph = _build_hypothesis_graph().compile()
        logger.info("HypothesisAgent initialised")

    async def generate(self, ticker: str) -> Hypothesis | None:
        logger.info(f"Running Hypothesis synthesis for {ticker}")
        initial_state: HypothesisState = {
            "ticker": ticker.upper(),
            "sentiment_result": {},
            "technical_result": {},
            "fundamental_result": {},
            "macro_result": {},
            "document_result": {},
            "signal_alignment": "mixed",
            "hypothesis": None,
            "conviction_score": 0.0,
            "rejection_reason": None,
            "error": None
        }
        
        final_state = await self._graph.ainvoke(initial_state)
        
        h = final_state.get("hypothesis")
        reason = final_state.get("rejection_reason")
        
        if not h or reason:
            return None
            
        now = datetime.now(timezone.utc)
        timeframe = h.get("expected_timeframe", "swing")
        if timeframe == "intraday": exp = now + timedelta(days=1)
        elif timeframe == "swing": exp = now + timedelta(days=14)
        else: exp = now + timedelta(days=90)
        
        return Hypothesis(
            ticker=ticker.upper(),
            title=h.get("title", ""),
            description=h.get("description", ""),
            direction=h.get("expected_direction", "neutral"),
            timeframe=timeframe,
            conviction_score=float(h.get("conviction_score", 0.0)),
            key_catalysts=h.get("key_catalysts", []),
            key_risks=h.get("key_risks", []),
            invalidation_conditions=h.get("invalidation_conditions", []),
            status="pending",
            created_at=now,
            expires_at=exp
        )

    async def generate_batch(self, tickers: list[str]) -> list[Hypothesis]:
        tasks = [self.generate(t) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid = []
        for t, r in zip(tickers, results):
            if isinstance(r, Exception):
                logger.error(f"Failed {t}: {r}")
            elif r is not None:
                valid.append(r)
        return valid

    def get_active_hypotheses(self) -> list[Hypothesis]:
        # Implement DB read if required, returning placeholder for now
        return []

    def get_high_conviction(self, min_score: float = 0.8) -> list[Hypothesis]:
        # Implement DB read if required, returning placeholder for now
        return []
