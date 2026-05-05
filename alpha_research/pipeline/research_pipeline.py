"""
Research Orchestrator Pipeline
==============================

Coordinates the entire Phase 2 Alpha Discovery pipeline using LangGraph.
Executes macro analysis globally, then parallelizes ticker-level research
before synthesizing hypotheses.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypedDict

import redis
from langgraph.graph import END, StateGraph
from loguru import logger
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker

from alpha_research.agents.document_agent import DocumentAgent
from alpha_research.agents.fundamental_agent import FundamentalAgent
from alpha_research.agents.hypothesis_agent import HypothesisAgent
from alpha_research.agents.macro_agent import MacroAgent
from alpha_research.agents.sentiment_agent import SentimentAgent
from alpha_research.agents.technical_agent import TechnicalAgent
from alpha_research.storage.research_models import ResearchRun
from config.settings import settings


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State & Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ResearchPipelineState(TypedDict):
    """Mutable state passed through the LangGraph pipeline."""
    tickers: list[str]
    current_ticker: str
    completed_tickers: list[str]
    failed_tickers: list[dict[str, Any]]
    hypotheses_generated: list[dict[str, Any]]
    hypotheses_rejected: list[dict[str, Any]]
    run_id: str
    started_at: str
    agents_status: dict[str, str]


@dataclass(frozen=True, slots=True)
class PipelineResult:
    """Immutable output returned to callers."""
    run_id: str
    status: str
    total_analyzed: int
    hypotheses_count: int
    rejected_count: int
    failed_tickers: list[dict[str, Any]]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Nodes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def initialize_run_node(state: ResearchPipelineState) -> dict[str, Any]:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    
    tickers_to_run = state.get("tickers") or settings.tickers
    
    # DB Persistence
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        with session_factory() as session:
            record = ResearchRun(
                id=uuid.UUID(run_id),
                run_type="triggered",
                status="running",
                started_at=started_at,
                tickers_analyzed={"queued_count": len(tickers_to_run)}
            )
            session.add(record)
            session.commit()
        engine.dispose()
    except Exception as exc:
        logger.error("Failed to initialize ResearchRun in DB: {}", exc)

    logger.info("Starting research pipeline [{}] for {} tickers", run_id, len(tickers_to_run))

    return {
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "tickers": list(tickers_to_run),  # clone to pop from later
        "current_ticker": "",
        "completed_tickers": [],
        "failed_tickers": [],
        "hypotheses_generated": [],
        "hypotheses_rejected": [],
        "agents_status": {}
    }


async def run_macro_analysis_node(state: ResearchPipelineState) -> dict[str, Any]:
    logger.info("Running global Macro analysis...")
    status_updates = {"macro": "running"}
    
    try:
        macro_agent = MacroAgent()
        await macro_agent.analyze()
        status_updates["macro"] = "success"
    except Exception as exc:
        logger.warning("Macro analysis failed: {}", exc)
        status_updates["macro"] = "failed"
        
    return {"agents_status": {**state.get("agents_status", {}), **status_updates}}


async def route_next_ticker_node(state: ResearchPipelineState) -> dict[str, Any]:
    """
    Pops the next ticker from the list to set current_ticker.
    LangGraph conditional edges will look at this state to branch.
    """
    tickers = list(state.get("tickers", []))
    if not tickers:
        return {"current_ticker": ""}
        
    next_ticker = tickers.pop(0)
    return {"current_ticker": next_ticker, "tickers": tickers}


async def run_ticker_research_node(state: ResearchPipelineState) -> dict[str, Any]:
    ticker = state["current_ticker"]
    if not ticker:
        return {}
        
    logger.info("Running research agents for {}", ticker)
    
    s_agent = SentimentAgent()
    t_agent = TechnicalAgent()
    f_agent = FundamentalAgent()
    d_agent = DocumentAgent()

    # Wrap in timeout for the whole batch
    try:
        async with asyncio.timeout(120.0):
            results = await asyncio.gather(
                s_agent.analyze(ticker),
                t_agent.analyze(ticker),
                f_agent.analyze(ticker),
                d_agent.research(ticker),
                return_exceptions=True
            )
    except TimeoutError:
        logger.warning("Research agents timed out for {}", ticker)
        return {"failed_tickers": state.get("failed_tickers", []) + [{"ticker": ticker, "reason": "timeout"}]}

    # Log failures but do not crash
    for name, res in zip(["Sentiment", "Technical", "Fundamental", "Document"], results):
        if isinstance(res, Exception):
            logger.warning("{} agent failed for {}: {}", name, ticker, res)

    return {}


async def generate_hypothesis_node(state: ResearchPipelineState) -> dict[str, Any]:
    ticker = state["current_ticker"]
    if not ticker:
        return {}

    # Check if the ticker failed completely (e.g. timeout)
    failed = any(f.get("ticker") == ticker for f in state.get("failed_tickers", []))
    if failed:
        return {}

    logger.info("Synthesizing hypothesis for {}", ticker)
    
    h_agent = HypothesisAgent()
    
    try:
        # Generate automatically handles gathering the cached results
        # and checking if it needs to skip conflicting signals.
        # It also returns None if rejected or conflicting.
        result = await h_agent.generate(ticker)
        
        if result is None:
            # Rejection was handled internally and logged to Redis
            rejected = state.get("hypotheses_rejected", []) + [{"ticker": ticker}]
            return {"hypotheses_rejected": rejected, "completed_tickers": state.get("completed_tickers", []) + [ticker]}
        else:
            generated = state.get("hypotheses_generated", []) + [{
                "ticker": ticker,
                "direction": result.direction,
                "conviction": result.conviction_score,
                "title": result.title
            }]
            return {"hypotheses_generated": generated, "completed_tickers": state.get("completed_tickers", []) + [ticker]}
            
    except Exception as exc:
        logger.error("Hypothesis generation failed for {}: {}", ticker, exc)
        return {"failed_tickers": state.get("failed_tickers", []) + [{"ticker": ticker, "reason": str(exc)}]}


async def finalize_run_node(state: ResearchPipelineState) -> dict[str, Any]:
    run_id = state["run_id"]
    completed = len(state.get("completed_tickers", []))
    generated = len(state.get("hypotheses_generated", []))
    rejected = len(state.get("hypotheses_rejected", []))
    failed = len(state.get("failed_tickers", []))
    
    now = datetime.now(timezone.utc)
    
    logger.info(
        "Research complete [{}] | Analyzed: {} | Hypotheses: {} | Rejected: {} | Failed: {}",
        run_id, completed, generated, rejected, failed
    )

    # DB Persistence
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        with session_factory() as session:
            session.execute(
                update(ResearchRun)
                .where(ResearchRun.id == uuid.UUID(run_id))
                .values(
                    status="completed" if failed == 0 else "completed_with_errors",
                    completed_at=now,
                    hypotheses_generated=generated,
                    error_message=json.dumps(state.get("failed_tickers", [])) if failed > 0 else None
                )
            )
            session.commit()
        engine.dispose()
    except Exception as exc:
        logger.error("Failed to update ResearchRun in DB: {}", exc)

    # Redis Publish
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        r.publish("research.pipeline.completed", json.dumps({
            "run_id": run_id,
            "tickers_analyzed": completed,
            "hypotheses_count": generated,
            "rejected_count": rejected,
            "failed_count": failed,
            "timestamp": now.isoformat()
        }))
        r.close()
    except Exception as exc:
        logger.warning("Redis publish failed: {}", exc)

    return {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Routing Logic
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _should_continue_loop(state: ResearchPipelineState) -> str:
    """Checks if current_ticker is empty to determine routing."""
    if state.get("current_ticker"):
        return "run_ticker_research"
    return "finalize_run"


def _build_pipeline_graph() -> StateGraph:
    graph = StateGraph(ResearchPipelineState)
    
    graph.add_node("initialize_run", initialize_run_node)
    graph.add_node("run_macro_analysis", run_macro_analysis_node)
    graph.add_node("route_next_ticker", route_next_ticker_node)
    graph.add_node("run_ticker_research", run_ticker_research_node)
    graph.add_node("generate_hypothesis", generate_hypothesis_node)
    graph.add_node("finalize_run", finalize_run_node)

    graph.set_entry_point("initialize_run")
    graph.add_edge("initialize_run", "run_macro_analysis")
    graph.add_edge("run_macro_analysis", "route_next_ticker")
    
    graph.add_conditional_edges(
        "route_next_ticker",
        _should_continue_loop,
        {
            "run_ticker_research": "run_ticker_research",
            "finalize_run": "finalize_run"
        }
    )
    
    graph.add_edge("run_ticker_research", "generate_hypothesis")
    # Loop back to routing
    graph.add_edge("generate_hypothesis", "route_next_ticker")
    
    graph.add_edge("finalize_run", END)
    
    return graph


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ResearchPipeline:
    def __init__(self) -> None:
        self._graph = _build_pipeline_graph().compile()
        logger.info("ResearchPipeline initialised")

    async def run(self, tickers: list[str] | None = None) -> PipelineResult:
        """Runs the entire pipeline sequentially across tickers."""
        target_tickers = tickers if tickers is not None else settings.tickers
        
        initial_state: ResearchPipelineState = {
            "tickers": target_tickers,
            "current_ticker": "",
            "completed_tickers": [],
            "failed_tickers": [],
            "hypotheses_generated": [],
            "hypotheses_rejected": [],
            "run_id": "",
            "started_at": "",
            "agents_status": {}
        }
        
        final_state = await self._graph.ainvoke(initial_state)
        
        return PipelineResult(
            run_id=final_state.get("run_id", ""),
            status="completed" if not final_state.get("failed_tickers") else "completed_with_errors",
            total_analyzed=len(final_state.get("completed_tickers", [])),
            hypotheses_count=len(final_state.get("hypotheses_generated", [])),
            rejected_count=len(final_state.get("hypotheses_rejected", [])),
            failed_tickers=final_state.get("failed_tickers", [])
        )

    async def run_single(self, ticker: str) -> dict[str, Any] | None:
        """Runs the full pipeline for a single ticker."""
        res = await self.run(tickers=[ticker])
        if res.hypotheses_count > 0:
            return {"ticker": ticker, "status": "generated"}
        return None

    def get_latest_results(self) -> list[Any]:
        # Implement DB read if required
        return []
        
    def get_pipeline_status(self) -> dict[str, Any]:
        return {"status": "idle"}
