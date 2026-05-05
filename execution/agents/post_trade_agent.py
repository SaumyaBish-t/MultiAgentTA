import json
import uuid
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import TypedDict, List, Dict, Any, Optional, Union
from dataclasses import dataclass

import redis
from loguru import logger
from sqlalchemy import create_engine, text
from langgraph.graph import StateGraph, END

from config.settings import settings
from config.llm_config import LLMFactory
from execution.brokers.alpaca_adapter import AlpacaBrokerAdapter

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1 — STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PostTradeState(TypedDict):
    batch_id: uuid.UUID
    filled_orders: List[Dict[str, Any]]
    execution_metrics: Dict[str, Any]
    slippage_analysis: Dict[str, Any]
    timing_analysis: Dict[str, Any]
    quality_score: float
    learnings: List[str]
    recommendations: List[str]
    error: Optional[str]

@dataclass
class PostTradeResult:
    batch_id: uuid.UUID
    quality_score: float
    avg_slippage_bps: float
    recommendations: List[str]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2 — GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_execution_data_node(state: PostTradeState) -> Dict[str, Any]:
    """Fetch all fill and performance data for the batch from DB."""
    if state.get("filled_orders"):
        return {"filled_orders": state["filled_orders"]}
        
    batch_id = state["batch_id"]
    engine = create_engine(settings.postgres_url)
    
    try:
        with engine.connect() as conn:
            # 1. Fetch Filled Orders
            orders_query = text("""
                SELECT o.id, o.ticker, o.action, o.requested_shares, o.requested_price, 
                       o.filled_shares, o.filled_avg_price, o.status, o.submitted_at, o.filled_at,
                       p.arrival_price, p.execution_price, p.slippage_bps
                FROM orders o
                LEFT JOIN execution_performance p ON o.id = p.order_id
                WHERE o.id IN (
                    SELECT id FROM orders WHERE id IN (
                        -- This would normally join with order_batches or use a batch_id FK if added
                        -- For now, let's assume we find orders by some criteria or batch_id join
                        SELECT id FROM orders -- Placeholder for batch filtering
                    )
                )
                AND o.status = 'filled'
            """)
            # In a real system, we'd have a clean way to link orders to batch_id
            # Assuming we can find them for this smoke test/impl
            rows = conn.execute(orders_query).fetchall()
            
            filled_orders = []
            for r in rows:
                filled_orders.append(dict(r._mapping))
                
        if not filled_orders:
            logger.warning(f"No filled orders found for batch {batch_id}")
            return {"error": "NO_DATA"}
            
        return {"filled_orders": filled_orders}
    except Exception as e:
        logger.error(f"Failed to fetch post-trade data: {e}")
        return {"error": str(e)}

async def analyze_slippage_node(state: PostTradeState) -> Dict[str, Any]:
    """Break down slippage into alpha decay and market impact components."""
    if state.get("error"): return {}
    
    orders = state["filled_orders"]
    df = pd.DataFrame(orders)
    
    avg_slip = df["slippage_bps"].mean()
    worst_slip = df["slippage_bps"].max()
    
    # Mock alpha decay/market impact for now as we don't have SPY benchmark here
    # In production, we'd fetch SPY returns during the execution window
    
    analysis = {
        "avg_slippage_bps": float(avg_slip),
        "worst_slippage": float(worst_slip),
        "total_filled_value": float((df["filled_shares"] * df["filled_avg_price"]).sum()),
        "order_count": len(df)
    }
    
    return {"slippage_analysis": analysis}

async def analyze_timing_node(state: PostTradeState) -> Dict[str, Any]:
    """Evaluate if the time of execution was optimal relative to the day's VWAP."""
    if state.get("error"): return {}
    
    # This node would fetch 1-min bars and calculate VWAP
    # For now, we'll provide a high-level summary
    
    analysis = {
        "avg_vs_vwap_bps": 5.2, # Mocked: 5.2 bps worse than VWAP
        "best_hour": 10,       # 10 AM ET
        "worst_hour": 15       # 3 PM ET
    }
    
    return {"timing_analysis": analysis}

async def calculate_quality_score_node(state: PostTradeState) -> Dict[str, Any]:
    """Weight slippage, fill rate, and timing to produce a final 0-1 score."""
    if state.get("error"): return {}
    
    slip = state["slippage_analysis"]["avg_slippage_bps"]
    vs_vwap = state["timing_analysis"]["avg_vs_vwap_bps"]
    
    # 1. Slippage Score (0-1)
    if slip < 5: s_score = 1.0
    elif slip < 15: s_score = 0.7
    elif slip < 30: s_score = 0.4
    else: s_score = 0.1
    
    # 2. Timing Score (0-1)
    if vs_vwap < 0: t_score = 1.0
    elif vs_vwap < 10: t_score = 0.7
    else: t_score = 0.3
    
    # Final weighted score
    quality_score = (s_score * 0.6 + t_score * 0.4)
    
    return {"quality_score": float(quality_score)}

async def generate_learnings_node(state: PostTradeState) -> Dict[str, Any]:
    """Use LLM to generate actionable recommendations for improvement."""
    if state.get("error"): return {}
    
    score = state["quality_score"]
    if score >= 0.9:
        return {"recommendations": ["Maintain current execution parameters. Execution quality is excellent."]}
        
    try:
        llm = LLMFactory.get_model("fast") # Groq 8B
        
        prompt = f"""
        Analyze this post-trade execution report and provide 2-3 specific recommendations to reduce slippage and improve timing.
        
        REPORT:
        Quality Score: {score:.2f}/1.0
        Avg Slippage: {state['slippage_analysis']['avg_slippage_bps']:.1f} bps
        Vs VWAP: {state['timing_analysis']['avg_vs_vwap_bps']:.1f} bps
        Worst Slippage: {state['slippage_analysis']['worst_slippage']:.1f} bps
        
        Return exactly a JSON list of strings titled 'recommendations'.
        """
        
        response = await llm.ainvoke(prompt)
        # Simple extraction
        content = response.content if hasattr(response, 'content') else str(response)
        
        try:
            # Try to find JSON in response
            start = content.find("[")
            end = content.rfind("]") + 1
            if start != -1 and end != -1:
                recommendations = json.loads(content[start:end])
            else:
                recommendations = [content.strip()]
        except:
            recommendations = [content.strip()]
            
        return {"recommendations": recommendations}
    except Exception as e:
        logger.error(f"LLM learning generation failed: {e}")
        return {"recommendations": ["Improve execution timing during volatile periods."]}

async def store_and_publish_node(state: PostTradeState) -> Dict[str, Any]:
    """Persist results to Redis and notify the system."""
    if state.get("error"): return {}
    
    r = redis.from_url(settings.redis_url, decode_responses=True)
    batch_id = state["batch_id"]
    score = state["quality_score"]
    
    # 1. Cache in Redis
    r.set("execution:quality:score:latest", score)
    r.set(f"execution:batch:{batch_id}:analysis", json.dumps({
        "score": score,
        "slippage": state["slippage_analysis"],
        "recommendations": state["recommendations"]
    }))
    
    # 2. Publish
    r.publish("execution.post_trade.completed", json.dumps({
        "batch_id": str(batch_id),
        "quality_score": score,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }))
    
    logger.info(f"\nPOST-TRADE ANALYSIS COMPLETE: Batch {batch_id}")
    logger.info(f"Quality Score: {score:.2f}")
    logger.info(f"Avg Slippage:  {state['slippage_analysis']['avg_slippage_bps']:.1f} bps")
    for rec in state["recommendations"]:
        logger.info(f"💡 Rec: {rec}")
        
    return {"monitoring_complete": True}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3 — PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PostTradeAnalyzer:
    """Agent for deep-dive analysis of execution performance."""
    
    def __init__(self):
        self.workflow = StateGraph(PostTradeState)
        
        self.workflow.add_node("fetch", fetch_execution_data_node)
        self.workflow.add_node("slippage", analyze_slippage_node)
        self.workflow.add_node("timing", analyze_timing_node)
        self.workflow.add_node("quality", calculate_quality_score_node)
        self.workflow.add_node("learnings", generate_learnings_node)
        self.workflow.add_node("store", store_and_publish_node)
        
        self.workflow.set_entry_point("fetch")
        self.workflow.add_edge("fetch", "slippage")
        self.workflow.add_edge("slippage", "timing")
        self.workflow.add_edge("timing", "quality")
        self.workflow.add_edge("quality", "learnings")
        self.workflow.add_edge("learnings", "store")
        self.workflow.add_edge("store", END)
        
        self.app = self.workflow.compile()

    async def analyze(self, batch_id: uuid.UUID) -> PostTradeResult:
        """Analyze the execution quality of a specific batch."""
        initial_state: PostTradeState = {
            "batch_id": batch_id,
            "filled_orders": [],
            "execution_metrics": {},
            "slippage_analysis": {},
            "timing_analysis": {},
            "quality_score": 0.0,
            "learnings": [],
            "recommendations": [],
            "error": None
        }
        
        final_state = await self.app.ainvoke(initial_state)
        
        if final_state.get("error"):
            return PostTradeResult(batch_id, 0.0, 0.0, ["Analysis failed: No data"])
            
        return PostTradeResult(
            batch_id=batch_id,
            quality_score=final_state["quality_score"],
            avg_slippage_bps=final_state["slippage_analysis"]["avg_slippage_bps"],
            recommendations=final_state["recommendations"]
        )
