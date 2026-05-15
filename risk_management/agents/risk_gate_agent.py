import json
import asyncio
from datetime import datetime, timezone
from uuid import UUID
from typing import TypedDict, Any, Optional
from dataclasses import dataclass

import redis
from loguru import logger
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage

from config.settings import settings
from config.llm_config import LLMFactory

from signal_generation.storage.signal_models import TradingSignal
from risk_management.storage.risk_models import ApprovedSignal, RiskEvent

# Import the other risk agents
from risk_management.agents.position_sizing_agent import PositionSizerAgent
from risk_management.agents.var_agent import VaRAgent
from risk_management.agents.correlation_agent import CorrelationAgent
from risk_management.agents.liquidity_agent import LiquidityAgent
from risk_management.agents.drawdown_monitor_agent import DrawdownMonitorAgent

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class RiskGateState(TypedDict):
    signal: dict
    position_result: dict      # from PositionSizer
    var_result_usd: float      # from VaRAgent
    correlation_result: dict   # from CorrelationAgent
    liquidity_result: dict     # from LiquidityAgent
    drawdown_pct: float        # from DrawdownMonitor
    individual_checks: dict    # each check pass/fail
    overall_approved: bool
    final_position_size_usd: float
    risk_score: float          # 0-1, lower = safer
    approval_conditions: list[str]
    rejection_reasons: list[str]
    risk_summary: str
    error: str | None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def run_all_risk_checks_node(state: RiskGateState) -> dict[str, Any]:
    """Execute all underlying risk agents in parallel."""
    if state.get("error"): return {}
    
    signal = state.get("signal", {})
    ticker = signal.get("ticker", "")
    if not ticker:
        return {"error": "Invalid signal: missing ticker"}
        
    try:
        # Initialize agents
        position_sizer = PositionSizerAgent()
        var_agent = VaRAgent()
        correlation_agent = CorrelationAgent()
        liquidity_agent = LiquidityAgent()
        drawdown_monitor = DrawdownMonitorAgent()
        
        # We need current portfolio positions for correlation
        r = redis.from_url(settings.redis_url, decode_responses=True)
        pf_str = r.get("portfolio:current:state")
        positions_dict = {}
        if pf_str:
            pf_data = json.loads(pf_str)
            for p in pf_data.get("positions", []):
                if "ticker" in p and "current_value" in p:
                    positions_dict[p["ticker"]] = float(p["current_value"])
        
        # 1. First get initial position size to pass to other agents
        pos_result = await position_sizer.size_position(signal)
        initial_size_usd = 0.0
        if pos_result:
            initial_size_usd = pos_result.size_usd
            
        # 2. Run the rest in parallel
        # Note: drawdown_monitor.get_current_drawdown is synchronous, call directly
        dd_pct = drawdown_monitor.get_current_drawdown()
        
        var_task = var_agent.calculate_single(ticker, initial_size_usd)
        corr_task = correlation_agent.check_new_position(ticker, positions_dict)
        liq_task = liquidity_agent.check(signal, initial_size_usd)
        
        results = await asyncio.gather(var_task, corr_task, liq_task, return_exceptions=True)
        
        # Unpack results
        var_usd = results[0] if not isinstance(results[0], Exception) else 0.0
        corr_res = results[1] if not isinstance(results[1], Exception) else {}
        liq_res = results[2] if not isinstance(results[2], Exception) else None
        
        # Convert results to dictionaries for state
        pos_dict = {
            "approved": pos_result.approved if pos_result else False,
            "size_usd": pos_result.size_usd if pos_result else 0.0,
            "rejection_reason": getattr(pos_result, "rejection_reason", "No size returned") if pos_result else "No result"
        }
        
        liq_dict = {}
        if liq_res:
            liq_dict = {
                "approved": liq_res.approved,
                "tier": liq_res.liquidity_tier,
                "rejection_reason": liq_res.rejection_reason
            }
            
        return {
            "position_result": pos_dict,
            "var_result_usd": var_usd,
            "correlation_result": corr_res,
            "liquidity_result": liq_dict,
            "drawdown_pct": dd_pct,
            "final_position_size_usd": initial_size_usd
        }
        
    except Exception as e:
        logger.error(f"Error running risk checks: {e}")
        return {"error": str(e)}

async def evaluate_checks_node(state: RiskGateState) -> dict[str, Any]:
    """Evaluate results from all agents against Gate limits."""
    if state.get("error"): return {}
    
    signal = state.get("signal", {})
    pos = state.get("position_result", {})
    var_usd = state.get("var_result_usd", 0.0)
    corr = state.get("correlation_result", {})
    liq = state.get("liquidity_result", {})
    dd_pct = state.get("drawdown_pct", 0.0)
    
    individual_checks = {
        "sizing": True,
        "liquidity": True,
        "var": True,
        "correlation": True,
        "concentration": True
    }
    
    rejections = []
    conditions = []
    final_size = state.get("final_position_size_usd", 0.0)
    
    # Check 1: Trading Halt
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        if r.get("risk:trading:halted") == "True":
            rejections.append("TRADING_HALTED")
            # Return early state
            return {
                "individual_checks": individual_checks,
                "rejection_reasons": rejections,
                "approval_conditions": conditions,
                "final_position_size_usd": final_size
            }
            
        # Get portfolio value for VaR check
        pf_str = r.get("portfolio:current:state")
        portfolio_value = json.loads(pf_str).get("total_value", 100000.0) if pf_str else 100000.0
    except Exception:
        portfolio_value = 100000.0
        
    # Check 2: Sizing
    if not pos.get("approved", False):
        rejections.append(f"POSITION_SIZE_REJECTED: {pos.get('rejection_reason')}")
        individual_checks["sizing"] = False
        
    # Check 3: Liquidity
    if not liq.get("approved", False):
        rejections.append(f"LIQUIDITY_REJECTED: {liq.get('rejection_reason')}")
        individual_checks["liquidity"] = False
        
    # Check 4: VaR
    var_limit = portfolio_value * 0.02
    if var_usd > var_limit:
        rejections.append("VAR_LIMIT_EXCEEDED")
        individual_checks["var"] = False
        
    # Check 5: Correlation
    high_pairs = corr.get("new_high_corr_pairs", [])
    if high_pairs:
        max_corr = max([p.get("correlation", 0.0) for p in high_pairs])
        if max_corr > 0.85:
            rejections.append("HIGH_CORRELATION_DUPLICATE")
        elif max_corr > 0.70:
            conditions.append("MONITOR_CORRELATION")
    
    # Check 6: Sector concentration
    corr_recs = corr.get("recommendations", [])
    for rec in corr_recs:
        if "Reduce" in str(rec) and "exposure" in str(rec):
            rejections.append("SECTOR_CONCENTRATION_BREACH")
            individual_checks["concentration"] = False
            break
            
    # Check 7: Drawdown state
    if dd_pct <= -0.15:
        rejections.append("PORTFOLIO_IN_SEVERE_DRAWDOWN")
    elif dd_pct <= -0.07:
        conditions.append("REDUCE_SIZE_50PCT_DRAWDOWN")
        
    # Check 8: Signal quality
    score = signal.get("composite_score", signal.get("conviction_score", 0.0))
    if score < 0.65:
        rejections.append("CONVICTION_BELOW_MINIMUM")
        
    return {
        "individual_checks": individual_checks,
        "rejection_reasons": rejections,
        "approval_conditions": conditions,
        "final_position_size_usd": final_size
    }

async def calculate_risk_score_node(state: RiskGateState) -> dict[str, Any]:
    """Compute 0-1 composite risk score."""
    if state.get("error"): return {}
    
    checks = state.get("individual_checks", {})
    if not checks:
        return {"risk_score": 0.0}
        
    checks_passed = sum([1 for v in checks.values() if v])
    base_score = checks_passed / len(checks)
    
    # Penalties
    var_usd = state.get("var_result_usd", 0.0)
    # Using 100k assumption for penalty if unknown
    if var_usd > 2000 * 0.8: base_score -= 0.1 
    
    liq = state.get("liquidity_result", {})
    if liq.get("tier") == "low": base_score -= 0.1
    
    dd_pct = state.get("drawdown_pct", 0.0)
    if dd_pct < -0.05: base_score -= 0.15
    
    corr = state.get("correlation_result", {})
    high_pairs = corr.get("new_high_corr_pairs", [])
    if high_pairs:
        max_corr = max([p.get("correlation", 0.0) for p in high_pairs])
        if max_corr > 0.70: base_score -= 0.05
        
    risk_score = max(0.0, min(1.0, base_score))
    return {"risk_score": risk_score}

async def make_final_decision_node(state: RiskGateState) -> dict[str, Any]:
    """Determine final approval and apply adjustments."""
    if state.get("error"): return {}
    
    rejections = state.get("rejection_reasons", [])
    score = state.get("risk_score", 1.0)
    conditions = state.get("approval_conditions", [])
    final_size = state.get("final_position_size_usd", 0.0)
    
    if len(rejections) > 0:
        approved = False
    elif score < 0.5:
        approved = False
        rejections.append("RISK_SCORE_TOO_LOW")
    else:
        approved = True
        
    if approved:
        for cond in conditions:
            if "REDUCE_SIZE_50PCT" in cond:
                final_size *= 0.5
                
    return {
        "overall_approved": approved,
        "rejection_reasons": rejections,
        "final_position_size_usd": final_size
    }

async def generate_risk_summary_node(state: RiskGateState) -> dict[str, Any]:
    """Use LLM to generate a concise summary for approved signals."""
    if state.get("error"): return {}
    
    llm = LLMFactory.get_risk_llm()
    signal = state.get("signal", {})
    
    prompt = f"""
    Summarize this risk assessment in 2 sentences:
    Signal: {signal.get('ticker')} {signal.get('direction')}
    Risk score: {state.get('risk_score', 0.0):.2f}/1.0
    Checks passed: {sum([1 for v in state.get('individual_checks', {}).values() if v])}/{len(state.get('individual_checks', {}))}
    Position size: ${state.get('final_position_size_usd', 0.0):,.0f}
    VaR 95%: ${state.get('var_result_usd', 0.0):,.0f}/day
    Conditions: {state.get('approval_conditions', [])}
    
    Be direct and quantitative. No pleasantries.
    """
    
    try:
        messages = [
            SystemMessage(content="You are a quantitative risk management summarizer."),
            HumanMessage(content=prompt)
        ]
        response = await llm.ainvoke(messages)
        return {"risk_summary": response.content.strip()}
    except Exception as e:
        logger.error(f"Failed to generate summary: {e}")
        return {"risk_summary": "Risk summary generation failed."}

async def store_decision_node(state: RiskGateState) -> dict[str, Any]:
    """Store the final decision to DB and Redis PubSub."""
    if state.get("error"): return {}
    
    signal = state.get("signal", {})
    signal_id = signal.get("id")
    ticker = signal.get("ticker", "UNKNOWN")
    approved = state.get("overall_approved", False)
    reasons = state.get("rejection_reasons", [])
    
    engine = create_engine(settings.postgres_url)
    Session = sessionmaker(bind=engine)
    
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        
        with Session() as session:
            # 1. Update TradingSignal status
            if signal_id:
                try:
                    stmt = update(TradingSignal).where(TradingSignal.id == signal_id).values(
                        status="approved" if approved else "rejected"
                    )
                    session.execute(stmt)
                except Exception as e:
                    logger.warning(f"Failed to update TradingSignal {signal_id}: {e}")
            
            # 2. Write to ApprovedSignals if approved
            if approved:
                app_sig = ApprovedSignal(
                    signal_id=signal_id if signal_id else UUID(int=0),
                    ticker=ticker,
                    approved_position_size_pct=state.get("final_position_size_usd", 0.0) / 100000.0,
                    approved_position_size_usd=state.get("final_position_size_usd", 0.0),
                    risk_score=state.get("risk_score", 0.0),
                    approval_reason=state.get("risk_summary", "Approved by Risk Gate"),
                    status="approved",
                    conditions={"conditions": state.get("approval_conditions", [])},
                    valid_until=datetime.now(timezone.utc),
                    approved_at=datetime.now(timezone.utc)
                )
                session.add(app_sig)
                
                # Publish approved
                r.publish("risk.signal.approved", json.dumps({
                    "signal_id": str(signal_id),
                    "ticker": ticker,
                    "size_usd": state.get("final_position_size_usd", 0.0),
                    "risk_score": state.get("risk_score", 0.0)
                }))
            else:
                # 3. Publish rejected
                r.publish("risk.signal.rejected", json.dumps({
                    "signal_id": str(signal_id),
                    "ticker": ticker,
                    "reasons": reasons
                }))
                
                # 4. Write RiskEvent for critical rejections
                if "TRADING_HALTED" in reasons or "PORTFOLIO_IN_SEVERE_DRAWDOWN" in reasons:
                    event = RiskEvent(
                        event_type="signal_rejected",
                        severity="high",
                        description=f"Signal for {ticker} rejected due to: {reasons}",
                        current_value=0.0,
                        threshold_value=0.0,
                        action_taken="reject_signal"
                    )
                    session.add(event)
                    
            session.commit()
            
    except Exception as e:
        logger.error(f"Store decision error: {e}")
        return {"error": str(e)}
        
    return {}

# Routing functions
def route_after_evaluate(state: RiskGateState) -> str:
    rejections = state.get("rejection_reasons", [])
    if "TRADING_HALTED" in rejections:
        return "store_decision_node"
    return "calculate_risk_score_node"

def route_after_decide(state: RiskGateState) -> str:
    if state.get("overall_approved", False):
        return "generate_risk_summary_node"
    return "store_decision_node"

def build_risk_gate_graph() -> StateGraph:
    workflow = StateGraph(RiskGateState)
    
    workflow.add_node("run_all_risk_checks_node", run_all_risk_checks_node)
    workflow.add_node("evaluate_checks_node", evaluate_checks_node)
    workflow.add_node("calculate_risk_score_node", calculate_risk_score_node)
    workflow.add_node("make_final_decision_node", make_final_decision_node)
    workflow.add_node("generate_risk_summary_node", generate_risk_summary_node)
    workflow.add_node("store_decision_node", store_decision_node)
    
    workflow.set_entry_point("run_all_risk_checks_node")
    workflow.add_edge("run_all_risk_checks_node", "evaluate_checks_node")
    
    # Conditional route after evaluate
    workflow.add_conditional_edges("evaluate_checks_node", route_after_evaluate, {
        "store_decision_node": "store_decision_node",
        "calculate_risk_score_node": "calculate_risk_score_node"
    })
    
    workflow.add_edge("calculate_risk_score_node", "make_final_decision_node")
    
    # Conditional route after decide
    workflow.add_conditional_edges("make_final_decision_node", route_after_decide, {
        "generate_risk_summary_node": "generate_risk_summary_node",
        "store_decision_node": "store_decision_node"
    })
    
    workflow.add_edge("generate_risk_summary_node", "store_decision_node")
    workflow.add_edge("store_decision_node", END)
    
    return workflow.compile()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class RiskDecision:
    signal_id: str
    ticker: str
    approved: bool
    final_position_size_usd: float
    risk_score: float
    individual_checks: dict
    approval_conditions: list[str]
    rejection_reasons: list[str]
    risk_summary: str
    evaluated_at: datetime

class RiskGateAgent:
    """The master Risk Orchestrator that combines all individual risk checks."""
    
    def __init__(self):
        self.graph = build_risk_gate_graph()
        
    async def evaluate(self, signal: dict) -> RiskDecision:
        """Run a signal through the entire Risk Gate."""
        initial_state: RiskGateState = {
            "signal": signal,
            "position_result": {},
            "var_result_usd": 0.0,
            "correlation_result": {},
            "liquidity_result": {},
            "drawdown_pct": 0.0,
            "individual_checks": {},
            "overall_approved": False,
            "final_position_size_usd": 0.0,
            "risk_score": 1.0,
            "approval_conditions": [],
            "rejection_reasons": [],
            "risk_summary": "",
            "error": None
        }
        
        try:
            final_state = await self.graph.ainvoke(initial_state)
            
            return RiskDecision(
                signal_id=str(signal.get("id", "")),
                ticker=signal.get("ticker", ""),
                approved=final_state.get("overall_approved", False),
                final_position_size_usd=final_state.get("final_position_size_usd", 0.0),
                risk_score=final_state.get("risk_score", 0.0),
                individual_checks=final_state.get("individual_checks", {}),
                approval_conditions=final_state.get("approval_conditions", []),
                rejection_reasons=final_state.get("rejection_reasons", []),
                risk_summary=final_state.get("risk_summary", ""),
                evaluated_at=datetime.now(timezone.utc)
            )
        except Exception as e:
            logger.exception("Risk Gate evaluation failed completely")
            return RiskDecision(
                signal_id=str(signal.get("id", "")),
                ticker=signal.get("ticker", ""),
                approved=False,
                final_position_size_usd=0.0,
                risk_score=0.0,
                individual_checks={},
                approval_conditions=[],
                rejection_reasons=[f"SYSTEM_ERROR: {str(e)}"],
                risk_summary="",
                evaluated_at=datetime.now(timezone.utc)
            )

    async def evaluate_batch(self, signals: list[dict]) -> list[RiskDecision]:
        """Process a batch of signals concurrently."""
        tasks = [self.evaluate(sig) for sig in signals]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, RiskDecision)]

    def get_approved_signals(self) -> list[dict]:
        """Fetch all currently approved signals awaiting execution."""
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        with Session() as session:
            stmt = select(ApprovedSignal).where(ApprovedSignal.executed == False) # Assuming an 'executed' boolean flag, or just return all recent
            # Fallback to returning all if no execution state is implemented yet
            signals = session.execute(stmt).scalars().all()
            return [
                {
                    "id": str(s.id),
                    "signal_id": str(s.signal_id),
                    "ticker": s.ticker,
                    "approved_size_usd": float(s.approved_size_usd),
                    "risk_score": float(s.risk_score),
                    "created_at": s.created_at.isoformat()
                } for s in signals
            ]

    def override_approval(self, signal_id: str, reason: str) -> bool:
        """Manual human override to approve a rejected signal."""
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        try:
            with Session() as session:
                # Update status in TradingSignals
                stmt = update(TradingSignal).where(TradingSignal.id == signal_id).values(status="approved_override")
                session.execute(stmt)
                
                # Write an audit risk event
                event = RiskEvent(
                    event_type="human_override",
                    severity="medium",
                    description=f"Signal {signal_id} manually approved. Reason: {reason}",
                    current_value=0.0,
                    threshold_value=0.0,
                    action_taken="approve"
                )
                session.add(event)
                session.commit()
                
            # Publish to execution engine
            r = redis.from_url(settings.redis_url, decode_responses=True)
            r.publish("risk.signal.approved", json.dumps({
                "signal_id": signal_id,
                "ticker": "OVERRIDE",
                "size_usd": 10000.0, # Default safe size
                "risk_score": 1.0,
                "override_reason": reason
            }))
            return True
        except Exception as e:
            logger.error(f"Failed override: {e}")
            return False
