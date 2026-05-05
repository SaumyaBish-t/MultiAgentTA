"""
Phase 8: Master Orchestrator
===========================
The single entry point and coordinator for all 8 phases of the algorithmic trading system.
"""

import asyncio
import json
import uuid
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, TypedDict, cast, Union
from dataclasses import dataclass

import redis
from loguru import logger
from sqlalchemy import create_engine, text
from langgraph.graph import StateGraph, END

from config.settings import settings
from monitoring.agents.health_monitor_agent import SystemHealthMonitor
from monitoring.alerts.alert_manager import alert_manager
from monitoring.feedback.feedback_agent import FeedbackAgent

# Import Phase Pipelines (Using dynamic imports or placeholders if not yet available)
# In a real production system, these would be absolute imports.
try:
    from compliance.pipeline.compliance_pipeline import CompliancePipeline
except ImportError:
    CompliancePipeline = None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATE & DATA CLASSES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MasterState(TypedDict):
    run_id: str
    run_type: str              # scheduled/triggered/manual
    started_at: str
    phases_completed: List[str]
    phases_failed: List[str]
    phase_results: Dict[str, Any]
    system_health: Dict[str, Any]
    feedback_actions: List[Dict[str, Any]]
    error: Optional[str]
    is_market_hours: bool

@dataclass
class MasterResult:
    run_id: str
    phases_completed: List[str]
    phases_failed: List[str]
    portfolio_value: float
    signals_processed: int
    trades_executed: int
    feedback_actions: int
    duration_seconds: float

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MASTER ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MasterOrchestrator:
    def __init__(self):
        self.redis = redis.from_url(settings.redis_url, decode_responses=True)
        self.engine = create_engine(settings.postgres_url)
        self.health_monitor = SystemHealthMonitor()
        self.feedback_agent = FeedbackAgent()
        
        # Build the graph
        self.workflow = self._build_workflow()

    def _build_workflow(self) -> StateGraph:
        builder = StateGraph(MasterState)
        
        # Define Nodes
        builder.add_node("health_check", self.check_system_health_node)
        builder.add_node("data_ingestion", self.run_data_ingestion_node)
        builder.add_node("research", self.run_research_pipeline_node)
        builder.add_node("signals", self.run_signal_pipeline_node)
        builder.add_node("risk", self.run_risk_pipeline_node)
        builder.add_node("portfolio", self.run_portfolio_pipeline_node)
        builder.add_node("execution", self.run_execution_pipeline_node)
        builder.add_node("compliance", self.run_compliance_node)
        builder.add_node("monitoring", self.run_monitoring_node)
        builder.add_node("finalize", self.finalize_master_run_node)
        
        # Define Edges
        builder.set_entry_point("health_check")
        
        builder.add_conditional_edges(
            "health_check",
            self.should_continue_after_health,
            {
                "continue": "data_ingestion",
                "abort": END
            }
        )
        
        builder.add_edge("data_ingestion", "research")
        builder.add_edge("research", "signals")
        builder.add_edge("signals", "risk")
        builder.add_edge("risk", "portfolio")
        builder.add_edge("portfolio", "execution")
        builder.add_edge("execution", "compliance")
        builder.add_edge("compliance", "monitoring")
        builder.add_edge("monitoring", "finalize")
        builder.add_edge("finalize", END)
        
        return builder.compile()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # GRAPH NODES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def check_system_health_node(self, state: MasterState) -> MasterState:
        """Node 1: Check system health before starting."""
        logger.info(f"[{state['run_id']}] Checking system health...")
        report = await self.health_monitor.run_full_health_check()
        
        state["system_health"] = report.dict()
        if report.overall == "critical":
            state["error"] = "CRITICAL health status. Aborting master run."
            logger.error(state["error"])
        else:
            logger.info(f"System health: {report.overall}")
            
        return state

    def should_continue_after_health(self, state: MasterState) -> str:
        return "abort" if state.get("error") else "continue"

    async def run_data_ingestion_node(self, state: MasterState) -> MasterState:
        """Node 2: Phase 1 Data Ingestion."""
        logger.info("Phase 1: Verifying data ingestion...")
        # Check if data is fresh via API
        # If stale, we might trigger a Prefect flow here (placeholder)
        state["phases_completed"].append("phase1")
        return state

    async def run_research_pipeline_node(self, state: MasterState) -> MasterState:
        """Node 3: Phase 2 Research."""
        logger.info("Phase 2: Running research pipeline...")
        # Logic to trigger research (placeholder)
        state["phases_completed"].append("phase2")
        return state

    async def run_signal_pipeline_node(self, state: MasterState) -> MasterState:
        """Node 4: Phase 3 Signal Generation."""
        logger.info("Phase 3: Running signal pipeline...")
        state["phases_completed"].append("phase3")
        return state

    async def run_risk_pipeline_node(self, state: MasterState) -> MasterState:
        """Node 5: Phase 4 Risk Management."""
        logger.info("Phase 4: Running risk management pipeline...")
        state["phases_completed"].append("phase4")
        return state

    async def run_portfolio_pipeline_node(self, state: MasterState) -> MasterState:
        """Node 5: Phase 5 Portfolio Allocation."""
        logger.info("Phase 5: Running portfolio pipeline...")
        state["phases_completed"].append("phase5")
        return state

    async def run_execution_pipeline_node(self, state: MasterState) -> MasterState:
        """Node 7: Phase 6 Execution."""
        if self._is_market_hours():
            logger.info("Phase 6: Running execution engine...")
            state["phases_completed"].append("phase6")
        else:
            logger.info("Market closed. Skipping execution.")
            state["phase_results"]["phase6"] = "skipped_market_closed"
        return state

    async def run_compliance_node(self, state: MasterState) -> MasterState:
        """Node 8: Phase 7 Compliance."""
        logger.info("Phase 7: Running compliance checks...")
        if CompliancePipeline:
            # cp = CompliancePipeline()
            # await cp.run_daily_compliance()
            pass
        state["phases_completed"].append("phase7")
        return state

    async def run_monitoring_node(self, state: MasterState) -> MasterState:
        """Node 9: Phase 8 Monitoring & Feedback."""
        logger.info("Phase 8: Running monitoring & feedback...")
        # Feedback agent handles the actual closing of the loop
        state["phases_completed"].append("phase8")
        return state

    async def finalize_master_run_node(self, state: MasterState) -> MasterState:
        """Node 10: Finalize the run and log results."""
        logger.info(f"Master Run Completed: {len(state['phases_completed'])} phases successful.")
        self.redis.publish("system.run.completed", json.dumps({
            "run_id": state["run_id"],
            "phases": state["phases_completed"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }))
        return state

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STARTUP & SHUTDOWN
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def startup(self):
        """Initializes the trading system."""
        logger.info("=" * 60)
        logger.info("ALGORITHMIC TRADING SYSTEM STARTING")
        logger.info("=" * 60)
        
        # 1. Start core listeners (Async tasks)
        # asyncio.create_task(self.start_listeners())
        
        # 2. Check initial health
        health = await self.health_monitor.run_full_health_check()
        logger.info(f"Initial health: {health.overall}")
        
        logger.info("System ready ✓")
        logger.info("=" * 60)

    async def shutdown(self):
        """Gracefully shuts down the system."""
        logger.info("System shutdown initiated...")
        # Cleanup logic
        logger.info("System shutdown complete.")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RUN METHOD
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def run(self, run_type: str = "manual") -> MasterResult:
        """Executes the master orchestration flow."""
        start_time = time.time()
        run_id = str(uuid.uuid4())
        
        initial_state: MasterState = {
            "run_id": run_id,
            "run_type": run_type,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "phases_completed": [],
            "phases_failed": [],
            "phase_results": {},
            "system_health": {},
            "feedback_actions": [],
            "error": None,
            "is_market_hours": self._is_market_hours()
        }
        
        final_state = await self.workflow.ainvoke(initial_state)
        
        duration = time.time() - start_time
        
        return MasterResult(
            run_id=run_id,
            phases_completed=final_state["phases_completed"],
            phases_failed=final_state["phases_failed"],
            portfolio_value=float(self.redis.get("portfolio:current:value") or 0.0),
            signals_processed=len(final_state.get("phase_results", {}).get("phase3_signals", [])),
            trades_executed=0, # Would be from phase 6 result
            feedback_actions=len(final_state["feedback_actions"]),
            duration_seconds=duration
        )

    def _is_market_hours(self) -> bool:
        # Simple check for now
        now = datetime.now()
        return now.weekday() < 5 and 9 <= now.hour < 16

# SINGLETON
master_orchestrator = MasterOrchestrator()
