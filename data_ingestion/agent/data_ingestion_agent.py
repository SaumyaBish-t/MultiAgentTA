"""
Data Ingestion Agent — LangGraph Orchestrator
===============================================

Intelligent orchestration agent handling data quality failures, backfilling,
and dynamic fallback routing using a LLM decision engine.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypedDict

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage
from config.llm_config import orchestrator_llm, simple_llm, LLMFactory
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import func, select

from config.settings import settings
from data_ingestion.api.cache import redis_client
from data_ingestion.flows.ingestion_flow import historical_backfill_flow
from data_ingestion.storage.init_db import get_db_manager
from data_ingestion.storage.models import DataQualityReport, OhlcvBar


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STATE DEFINITION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class IngestionState(TypedDict):
    tickers: list[str]
    timeframe: str
    data_gaps: list[dict[str, Any]]        # gaps found in data
    quality_reports: list[dict[str, Any]]  # quality check results
    actions_taken: list[str]               # log of decisions made
    alerts: list[str]                      # things needing attention
    current_step: str
    error_count: int


class DecisionOutput(BaseModel):
    action: Literal["trigger_backfill", "switch_to_fallback_source", "alert_human", "continue_normal", "pause_collection"]
    reasoning: str
    priority: str = "normal"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def check_data_freshness_node(state: IngestionState) -> dict[str, Any]:
    """Queries TimescaleDB for latest bar per ticker and identifies staleness."""
    logger.info("Executing node: check_data_freshness")
    manager = get_db_manager()
    gaps = []
    error_count = state.get("error_count", 0)

    try:
        with manager.timescale_session() as session:
            for ticker in state["tickers"]:
                latest = session.execute(
                    select(func.max(OhlcvBar.timestamp))
                    .where(OhlcvBar.ticker == ticker, OhlcvBar.timeframe == state["timeframe"])
                ).scalar()

                if not latest:
                    gaps.append({"ticker": ticker, "reason": "no_data", "last_timestamp": None})
                    continue

                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)

                age = datetime.now(timezone.utc) - latest
                if age > timedelta(minutes=5):
                    gaps.append({"ticker": ticker, "reason": "stale_data", "last_timestamp": str(latest), "age_minutes": age.total_seconds() / 60})
                    
    except Exception as e:
        logger.error("Failed to query freshness: {}", e)
        error_count += 1

    return {"data_gaps": gaps, "current_step": "check_data_freshness", "error_count": error_count}


async def assess_quality_node(state: IngestionState) -> dict[str, Any]:
    """Reads latest quality reports from DB and flags issues."""
    logger.info("Executing node: assess_quality")
    manager = get_db_manager()
    reports = []
    alerts = state.get("alerts", [])
    error_count = state.get("error_count", 0)

    try:
        with manager.postgres_session() as session:
            # Get latest 5 reports
            recent_reports = session.execute(
                select(DataQualityReport)
                .order_by(DataQualityReport.run_timestamp.desc())
                .limit(5)
            ).scalars().all()

            for r in recent_reports:
                report_dict = {
                    "source": r.source,
                    "failure_rate": float(r.failure_rate),
                    "anomalies_detected": r.anomalies_detected
                }
                reports.append(report_dict)

                if r.failure_rate > 0.10:
                    alerts.append(f"Critical quality issue in {r.source}: {r.failure_rate:.1%} failure rate.")
                    
    except Exception as e:
        logger.error("Failed to query quality reports: {}", e)
        error_count += 1

    return {"quality_reports": reports, "alerts": alerts, "current_step": "assess_quality", "error_count": error_count}


async def decide_action_node(state: IngestionState) -> dict[str, Any]:
    """Uses LLM to decide what to do based on the current state."""
    logger.info("Executing node: decide_action")
    
    logger.info("Orchestrator using Groq llama-3.3-70b")
    llm = LLMFactory.get_llm_with_fallback(orchestrator_llm)
    structured_llm = llm.with_structured_output(DecisionOutput)

    sys_prompt = (
        "You are a data pipeline manager for an algorithmic trading system running on Groq's Llama 3.3 70B. "
        "Analyze the current ingestion state and return a JSON decision with keys: action, reasoning, priority.\n"
        "Actions: continue_normal | trigger_backfill | switch_to_fallback_source | alert_human | pause_collection"
    )

    state_json = json.dumps({
        "tickers": state["tickers"],
        "data_gaps": state["data_gaps"],
        "quality_reports": state["quality_reports"],
        "error_count": state["error_count"],
        "alerts": state["alerts"]
    }, default=str)

    try:
        response = structured_llm.invoke([
            SystemMessage(content=sys_prompt),
            HumanMessage(content=f"Current State:\n{state_json}")
        ])
        
        # In case the structured output failed, default to alert
        if not response:
            decision = DecisionOutput(action="alert_human", reasoning="LLM parsing failed")
        else:
            decision = response

    except Exception as e:
        logger.error("LLM decision failed: {}", e)
        decision = DecisionOutput(action="alert_human", reasoning=f"LLM Error: {e}")

    actions = state.get("actions_taken", [])
    actions.append(f"Decided: {decision.action} because {decision.reasoning}")

    return {"current_step": decision.action, "actions_taken": actions}


async def execute_backfill_node(state: IngestionState) -> dict[str, Any]:
    """Triggers historical backfill for specific gaps."""
    logger.info("Executing node: execute_backfill")
    actions = state.get("actions_taken", [])
    
    # Extract tickers that need backfill
    tickers_to_backfill = [gap["ticker"] for gap in state["data_gaps"]]
    if tickers_to_backfill:
        start_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        try:
            # Dispatch prefect flow asynchronously
            logger.info("Dispatching historical backfill flow for {}", tickers_to_backfill)
            await historical_backfill_flow(
                tickers=tickers_to_backfill,
                start_date=start_date,
                end_date=end_date,
                timeframes=[state["timeframe"]]
            )
            actions.append(f"Triggered backfill for {tickers_to_backfill}")
        except Exception as e:
            logger.error("Failed to trigger backfill: {}", e)
            actions.append(f"Backfill failed: {e}")

    return {"actions_taken": actions, "current_step": "execute_backfill"}


async def switch_source_node(state: IngestionState) -> dict[str, Any]:
    """Updates config to use fallback data source."""
    logger.info("Executing node: switch_source")
    actions = state.get("actions_taken", [])
    
    # Simulated config switch. In a real system, you might toggle a Redis flag
    # or an environment variable that the collectors check dynamically.
    if redis_client:
        redis_client.set("ingestion_active_source", "yfinance")
        
    actions.append("Switched active source to fallback (yfinance).")
    return {"actions_taken": actions, "current_step": "switch_source"}


async def alert_node(state: IngestionState) -> dict[str, Any]:
    """Publishes alert to Redis and logs it."""
    logger.info("Executing node: alert_human")
    actions = state.get("actions_taken", [])
    
    payload = {
        "source": "data_ingestion_agent",
        "alerts": state["alerts"],
        "error_count": state["error_count"],
        "time": str(datetime.now(timezone.utc))
    }
    
    if redis_client:
        redis_client.publish("data.quality.alert", json.dumps(payload))
        
    logger.error(f"HUMAN ALERT TRIGGERED: {payload}")
    actions.append("Published critical alert to human operators.")
    
    return {"actions_taken": actions, "current_step": "alert_human"}


async def report_node(state: IngestionState) -> dict[str, Any]:
    """Generates final status summary."""
    logger.info("Executing node: report")
    logger.info("Agent run complete. Actions taken: {}", state["actions_taken"])
    return {"current_step": "report"}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EDGE ROUTING LOGIC
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def route_decision(state: IngestionState) -> str:
    """Routes based on the LLM's decision."""
    decision = state["current_step"]
    
    if decision == "trigger_backfill":
        return "execute_backfill"
    elif decision == "switch_to_fallback_source":
        return "switch_source"
    elif decision == "alert_human" or decision == "pause_collection":
        return "alert"
    else:
        return "report"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BUILD AND COMPILE GRAPH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_agent(checkpointer=None):
    """Compiles the LangGraph agent."""
    
    workflow = StateGraph(IngestionState)

    # Add Nodes
    workflow.add_node("check_data_freshness", check_data_freshness_node)
    workflow.add_node("assess_quality", assess_quality_node)
    workflow.add_node("decide_action", decide_action_node)
    workflow.add_node("execute_backfill", execute_backfill_node)
    workflow.add_node("switch_source", switch_source_node)
    workflow.add_node("alert", alert_node)
    workflow.add_node("report", report_node)

    # Add Edges
    workflow.add_edge(START, "check_data_freshness")
    workflow.add_edge("check_data_freshness", "assess_quality")
    workflow.add_edge("assess_quality", "decide_action")
    
    # Conditional Edges from decide_action
    workflow.add_conditional_edges(
        "decide_action",
        route_decision,
        {
            "execute_backfill": "execute_backfill",
            "switch_source": "switch_source",
            "alert": "alert",
            "report": "report"
        }
    )

    workflow.add_edge("execute_backfill", "report")
    workflow.add_edge("switch_source", "check_data_freshness")
    workflow.add_edge("alert", "report")
    workflow.add_edge("report", END)

    agent = workflow.compile(checkpointer=checkpointer)
    return agent


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  COORDINATOR INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class HealthStatus(BaseModel):
    is_healthy: bool
    active_issues: list[str]

class PipelineStatus(BaseModel):
    state: str
    last_actions: list[str]

class DataIngestionCoordinator:
    """
    Called by the Master Orchestrator (future Phase).
    Exposes simple interface to the intelligent agent.
    """
    
    def __init__(self):
        self.memory = MemorySaver()

    async def run_health_check(self, thread_id: str = "default") -> HealthStatus:
        """Trigger an agent run to assess and fix pipeline health."""
        config = {"configurable": {"thread_id": thread_id}}
        
        initial_state = {
            "tickers": settings.tickers,
            "timeframe": "1min",
            "data_gaps": [],
            "quality_reports": [],
            "actions_taken": [],
            "alerts": [],
            "current_step": "init",
            "error_count": 0
        }
        
        agent = build_agent(self.memory)
        final_state = await agent.ainvoke(initial_state, config)
        
        is_healthy = len(final_state.get("data_gaps", [])) == 0 and len(final_state.get("alerts", [])) == 0
        return HealthStatus(
            is_healthy=is_healthy,
            active_issues=final_state.get("alerts", []) + [g["ticker"] for g in final_state.get("data_gaps", [])]
        )
        
    async def request_data(self, ticker: str, timeframe: str, start: str, end: str) -> pd.DataFrame:
        """Fetch historical data on-demand for other pipelines."""
        manager = get_db_manager()
        try:
            with manager.timescale_session() as session:
                query = select(OhlcvBar).where(
                    OhlcvBar.ticker == ticker,
                    OhlcvBar.timeframe == timeframe,
                    OhlcvBar.timestamp >= start,
                    OhlcvBar.timestamp <= end
                ).order_by(OhlcvBar.timestamp.asc())
                
                results = session.execute(query).scalars().all()
                df = pd.DataFrame([r.__dict__ for r in results])
                if not df.empty and "_sa_instance_state" in df.columns:
                    df = df.drop(columns=["_sa_instance_state"])
                return df
        except Exception as e:
            logger.error("Failed to request_data: {}", e)
            return pd.DataFrame()
            
    async def get_status(self, thread_id: str = "default") -> PipelineStatus:
        """Retrieve the last known state from the agent's memory."""
        config = {"configurable": {"thread_id": thread_id}}
        
        agent = build_agent(self.memory)
        state_history = await agent.aget_state(config)
        
        if not state_history or not state_history.values:
            return PipelineStatus(state="unknown", last_actions=[])
            
        values = state_history.values
        return PipelineStatus(
            state="healthy" if not values.get("alerts") else "issues_detected",
            last_actions=values.get("actions_taken", [])
        )
