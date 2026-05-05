"""
Technical Analysis Agent — LangGraph Pipeline
=============================================

Calculates technical indicators using pure pandas/numpy and detects
trading signals using a hybrid rule-based and LLM approach.

Graph flow::

    fetch_price_data → calculate_indicators → detect_signals →
        assess_bias → store_signals → END
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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

from alpha_research.storage.research_models import TechnicalSignal
from config.llm_config import reasoning_llm
from config.settings import settings

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State & Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TechnicalState(TypedDict):
    """Mutable state passed through the LangGraph pipeline."""
    ticker: str
    timeframe: str
    price_data: dict[str, Any]
    indicators: dict[str, Any]
    signals: list[dict[str, Any]]
    overall_bias: str
    key_levels: dict[str, float]
    error: str | None


@dataclass(frozen=True, slots=True)
class TechnicalResult:
    """Immutable output returned to callers."""
    ticker: str
    timeframe: str
    bias: str
    key_levels: dict[str, float]
    signals: list[dict[str, Any]]
    error: str | None = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_API_BASE = "http://localhost:8000"
_API_HEADERS = {"x-api-key": settings.internal_api_key}

_SIGNAL_SYSTEM_PROMPT = (
    "You are an expert technical analyst. Given these indicators for {ticker} "
    "on {timeframe} timeframe, identify the top 3-5 most significant technical signals.\n"
    "Return JSON array: [{{\n"
    '  "signal_type": "momentum" | "trend" | "volatility" | "volume" | "breakout",\n'
    '  "indicator_name": "string",\n'
    '  "signal_direction": "bullish" | "bearish" | "neutral",\n'
    '  "strength": 0.0 to 1.0,\n'
    '  "description": "string (1 sentence)",\n'
    '  "actionable": boolean\n'
    "}}]\n"
    "Focus on high-conviction signals only. Return ONLY valid JSON array."
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Nodes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_price_data_node(state: TechnicalState) -> dict[str, Any]:
    ticker = state["ticker"]
    timeframe = state["timeframe"]
    start_date = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{_API_BASE}/prices/{ticker}/bars",
                params={"timeframe": timeframe, "start": start_date, "adjusted": "true"},
                headers=_API_HEADERS,
            )
            resp.raise_for_status()
            bars = resp.json()
    except Exception as exc:
        logger.warning("fetch_price_data failed for {}: {}", ticker, exc)
        return {"error": f"API error: {exc}"}

    if len(bars) < 50:
        return {"error": f"Insufficient bars ({len(bars)} < 50)"}

    # Convert to DataFrame
    df = pd.DataFrame(bars)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)

    # Convert timestamp index to strings for JSON serialization in state
    df_dict = df.reset_index().to_dict(orient="list")
    # Convert timestamps to ISO format strings
    df_dict["timestamp"] = [ts.isoformat() for ts in df_dict["timestamp"]]

    return {"price_data": df_dict}


async def calculate_indicators_node(state: TechnicalState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    try:
        df = pd.DataFrame(state["price_data"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)
        
        close = df["close"]
        high = df["high"]
        low = df["low"]
        vol = df["volume"]

        ind = {}
        
        # Trend: EMAs
        for p in [9, 21, 50, 200]:
            df[f"ema_{p}"] = close.ewm(span=p, adjust=False).mean()
            ind[f"ema_{p}"] = df[f"ema_{p}"].iloc[-1]

        ind["price_vs_ema200"] = "above" if close.iloc[-1] > df["ema_200"].iloc[-1] else "below"
        ind["ema9_vs_ema21"] = "above" if df["ema_9"].iloc[-1] > df["ema_21"].iloc[-1] else "below"

        # Momentum: RSI
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        ind["rsi_14"] = df["rsi"].iloc[-1]

        # MACD
        ema_12 = close.ewm(span=12, adjust=False).mean()
        ema_26 = close.ewm(span=26, adjust=False).mean()
        df["macd"] = ema_12 - ema_26
        df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["macd_hist"] = df["macd"] - df["macd_signal"]
        ind["macd"] = df["macd"].iloc[-1]
        ind["macd_signal"] = df["macd_signal"].iloc[-1]
        ind["macd_hist"] = df["macd_hist"].iloc[-1]

        # ROC
        df["roc_10"] = close.pct_change(periods=10) * 100
        ind["roc_10"] = df["roc_10"].iloc[-1]

        # Volatility: Bollinger Bands
        sma_20 = close.rolling(window=20).mean()
        std_20 = close.rolling(window=20).std()
        df["bb_upper"] = sma_20 + (std_20 * 2)
        df["bb_lower"] = sma_20 - (std_20 * 2)
        ind["bb_upper"] = df["bb_upper"].iloc[-1]
        ind["bb_lower"] = df["bb_lower"].iloc[-1]
        ind["bb_bandwidth"] = (df["bb_upper"].iloc[-1] - df["bb_lower"].iloc[-1]) / sma_20.iloc[-1]

        # ATR
        tr = np.maximum(high - low, np.maximum(abs(high - close.shift(1)), abs(low - close.shift(1))))
        df["atr"] = pd.Series(tr).rolling(window=14).mean()
        ind["atr_14"] = df["atr"].iloc[-1]

        # Historical Volatility
        df["hv_20"] = close.pct_change().rolling(20).std() * np.sqrt(252) * 100
        ind["hv_20"] = df["hv_20"].iloc[-1]

        # Volume
        df["vol_sma_20"] = vol.rolling(20).mean()
        ind["vol_sma_20"] = df["vol_sma_20"].iloc[-1]
        ind["vol_ratio"] = vol.iloc[-1] / df["vol_sma_20"].iloc[-1] if df["vol_sma_20"].iloc[-1] > 0 else 1.0
        
        df["obv"] = (np.sign(delta) * vol).fillna(0).cumsum()
        ind["obv"] = df["obv"].iloc[-1]

        # Levels
        last_20_low = low.tail(20).min()
        last_20_high = high.tail(20).max()
        ind["support_20"] = last_20_low
        ind["resistance_20"] = last_20_high
        
        pivot = (high.iloc[-2] + low.iloc[-2] + close.iloc[-2]) / 3 if len(df) > 1 else close.iloc[-1]
        ind["pivot_point"] = pivot

        # Current Price
        ind["current_price"] = close.iloc[-1]
        ind["current_volume"] = vol.iloc[-1]
        
        # Clean up NaNs in ind
        clean_ind = {k: (float(v) if not pd.isna(v) else 0.0) for k, v in ind.items() if isinstance(v, (int, float, np.number))}
        for k, v in ind.items():
            if isinstance(v, str):
                clean_ind[k] = v

        return {"indicators": clean_ind}
        
    except Exception as exc:
        logger.error("Indicator calculation failed for {}: {}", state["ticker"], exc)
        return {"error": f"Calc error: {exc}"}


async def detect_signals_node(state: TechnicalState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    ind = state["indicators"]
    ticker = state["ticker"]
    signals = []

    # Rule-based detection
    if ind.get("rsi_14", 50) > 70:
        signals.append({"signal_type": "momentum", "indicator_name": "RSI", "signal_direction": "bearish", "strength": 0.8, "description": "RSI > 70 (Overbought)", "actionable": True})
    elif ind.get("rsi_14", 50) < 30:
        signals.append({"signal_type": "momentum", "indicator_name": "RSI", "signal_direction": "bullish", "strength": 0.8, "description": "RSI < 30 (Oversold)", "actionable": True})

    if ind.get("current_price", 0) > ind.get("ema_200", float("inf")) and ind.get("price_vs_ema200") == "above":
        signals.append({"signal_type": "trend", "indicator_name": "EMA_200", "signal_direction": "bullish", "strength": 0.7, "description": "Price above EMA 200", "actionable": False})

    if ind.get("macd_hist", 0) > 0 and ind.get("macd", 0) > ind.get("macd_signal", 0):
        signals.append({"signal_type": "momentum", "indicator_name": "MACD", "signal_direction": "bullish", "strength": 0.6, "description": "MACD crossed above signal", "actionable": True})

    if ind.get("bb_bandwidth", 1) < 0.10:
        signals.append({"signal_type": "volatility", "indicator_name": "BollingerBands", "signal_direction": "neutral", "strength": 0.9, "description": "BB squeeze detected", "actionable": True})

    if ind.get("vol_ratio", 1) > 2.0:
        signals.append({"signal_type": "volume", "indicator_name": "Volume", "signal_direction": "neutral", "strength": 0.7, "description": "Volume > 2x average", "actionable": True})

    # LLM-based detection
    prompt = _SIGNAL_SYSTEM_PROMPT.format(ticker=ticker, timeframe=state["timeframe"])
    messages = [
        SystemMessage(content=prompt),
        HumanMessage(content=f"Indicators: {json.dumps(ind)}")
    ]

    try:
        response = await reasoning_llm.ainvoke(messages)
        text = response.content if hasattr(response, "content") else str(response)
        
        # Clean code fences
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
            
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1:
            llm_signals = json.loads(text[start:end+1])
            if isinstance(llm_signals, list):
                signals.extend(llm_signals)
    except Exception as exc:
        logger.warning("LLM signal detection failed for {}: {}", ticker, exc)

    return {"signals": signals}


async def assess_bias_node(state: TechnicalState) -> dict[str, Any]:
    if state.get("error"):
        return {"overall_bias": "neutral", "key_levels": {}}

    signals = state.get("signals", [])
    bull_score = 0.0
    bear_score = 0.0

    for s in signals:
        direction = s.get("signal_direction", "neutral").lower()
        strength = float(s.get("strength", 0.5))
        if direction == "bullish":
            bull_score += strength
        elif direction == "bearish":
            bear_score += strength

    if bull_score > bear_score + 1.0:
        bias = "bullish"
    elif bear_score > bull_score + 1.0:
        bias = "bearish"
    else:
        bias = "neutral"

    ind = state.get("indicators", {})
    key_levels = {
        "support": ind.get("support_20", 0.0),
        "resistance": ind.get("resistance_20", 0.0),
        "ema50": ind.get("ema_50", 0.0),
        "ema200": ind.get("ema_200", 0.0),
        "bb_upper": ind.get("bb_upper", 0.0),
        "bb_lower": ind.get("bb_lower", 0.0),
    }

    return {"overall_bias": bias, "key_levels": key_levels}


async def store_signals_node(state: TechnicalState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    ticker = state["ticker"]
    timeframe = state["timeframe"]
    now = datetime.now(timezone.utc)
    ind = state.get("indicators", {})

    # DB Persistence
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        with session_factory() as session:
            for s in state.get("signals", []):
                record = TechnicalSignal(
                    id=uuid.uuid4(),
                    ticker=ticker,
                    timeframe=timeframe,
                    signal_type=s.get("signal_type", "trend"),
                    indicator_name=s.get("indicator_name", "Unknown"),
                    signal_value=ind.get(s.get("indicator_name", "").lower(), 0.0),
                    signal_direction=s.get("signal_direction", "neutral"),
                    strength=s.get("strength", 0.5),
                    price_at_signal=ind.get("current_price", 0.0),
                    volume_at_signal=ind.get("current_volume", 0.0),
                    detected_at=now
                )
                session.add(record)
            session.commit()
        engine.dispose()
    except Exception as exc:
        logger.error("Failed to store TechnicalSignal to DB for {}: {}", ticker, exc)

    # Redis Cache and Pub/Sub
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        payload = {
            "bias": state.get("overall_bias", "neutral"),
            "strength": max(0.0, float(len(state.get("signals", [])) * 0.2)), # crude proxy
            "signals_count": len(state.get("signals", []))
        }
        r.setex(f"technical:bias:{ticker}:{timeframe}", 300, json.dumps(payload))
        
        pub_payload = {
            "ticker": ticker,
            "timeframe": timeframe,
            "bias": state.get("overall_bias", "neutral"),
            "timestamp": now.isoformat()
        }
        r.publish("research.technical.updated", json.dumps(pub_payload))
        r.close()
    except Exception as exc:
        logger.warning("Redis store/publish failed for {}: {}", ticker, exc)

    return {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Graph Construction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_technical_graph() -> StateGraph:
    graph = StateGraph(TechnicalState)
    graph.add_node("fetch_price_data", fetch_price_data_node)
    graph.add_node("calculate_indicators", calculate_indicators_node)
    graph.add_node("detect_signals", detect_signals_node)
    graph.add_node("assess_bias", assess_bias_node)
    graph.add_node("store_signals", store_signals_node)

    graph.set_entry_point("fetch_price_data")
    graph.add_edge("fetch_price_data", "calculate_indicators")
    graph.add_edge("calculate_indicators", "detect_signals")
    graph.add_edge("detect_signals", "assess_bias")
    graph.add_edge("assess_bias", "store_signals")
    graph.add_edge("store_signals", END)
    
    return graph


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TechnicalAgent:
    def __init__(self) -> None:
        self._graph = _build_technical_graph().compile()
        logger.info("TechnicalAgent initialised")

    async def analyze(self, ticker: str, timeframe: str = "1d") -> TechnicalResult:
        logger.info(f"Analyzing technicals for {ticker} ({timeframe})")
        initial_state: TechnicalState = {
            "ticker": ticker.upper(),
            "timeframe": timeframe,
            "price_data": {},
            "indicators": {},
            "signals": [],
            "overall_bias": "neutral",
            "key_levels": {},
            "error": None
        }
        
        final_state = await self._graph.ainvoke(initial_state)
        
        return TechnicalResult(
            ticker=ticker.upper(),
            timeframe=timeframe,
            bias=final_state.get("overall_bias", "neutral"),
            key_levels=final_state.get("key_levels", {}),
            signals=final_state.get("signals", []),
            error=final_state.get("error")
        )

    async def analyze_batch(self, tickers: list[str], timeframe: str = "1d") -> dict[str, TechnicalResult]:
        tasks = [self.analyze(t, timeframe) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        output = {}
        for t, r in zip(tickers, results):
            if isinstance(r, Exception):
                logger.error(f"Failed {t}: {r}")
                output[t.upper()] = TechnicalResult(ticker=t.upper(), timeframe=timeframe, bias="neutral", key_levels={}, signals=[], error=str(r))
            else:
                output[t.upper()] = r
        return output

    async def get_key_levels(self, ticker: str) -> dict[str, float]:
        res = await self.analyze(ticker, "1d")
        return res.key_levels

    async def scan_for_breakouts(self, tickers: list[str]) -> list[str]:
        results = await self.analyze_batch(tickers, "1d")
        breakouts = []
        for t, res in results.items():
            if res.error:
                continue
            for s in res.signals:
                if s.get("signal_type") == "breakout" or "squeeze" in str(s.get("description", "")).lower():
                    breakouts.append(t)
                    break
        return breakouts
