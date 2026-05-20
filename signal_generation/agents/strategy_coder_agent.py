import ast
import json
import uuid
import asyncio
from typing import Any, TypedDict, Optional
import pandas as pd
import numpy as np

# Suppress pandas future warnings from dynamically generated scripts
pd.set_option('future.no_silent_downcasting', True)

from langgraph.graph import StateGraph, END
from langchain_core.messages import SystemMessage, HumanMessage
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import redis

from config.settings import settings
from config.llm_config import reasoning_llm, document_llm

from signal_generation.storage.signal_models import TradingSignal, SignalParameter

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BUILT-IN STRATEGY TEMPLATES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TEMPLATE_EMA_CROSSOVER = """import pandas as pd
import numpy as np
import vectorbt as vbt

def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    # Regime-filtered momentum. A bare EMA crossover has no edge after costs;
    # the fix is to only take momentum signals while the market is in an
    # established uptrend, and to exit when that regime breaks.
    fast = int(params.get('fast_period', 20))
    slow = int(params.get('slow_period', 50))
    trend = int(params.get('trend_period', 200))

    close = price_data['close']
    ema_fast = close.ewm(span=fast).mean()
    ema_slow = close.ewm(span=slow).mean()
    trend_sma = close.rolling(trend, min_periods=1).mean()

    uptrend = close > trend_sma                                   # regime filter
    golden = (ema_fast > ema_slow) & (ema_fast.shift(1) <= ema_slow.shift(1))
    death = (ema_fast < ema_slow) & (ema_fast.shift(1) >= ema_slow.shift(1))

    entries = (golden & uptrend).fillna(False)
    exits = (death | (close < trend_sma)).fillna(False)           # explicit exit
    return entries, exits
"""

TEMPLATE_RSI_MEAN_REVERSION = """import pandas as pd
import numpy as np
import vectorbt as vbt

def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    # Confluence mean-reversion. Buy oversold dips, but ONLY while the long
    # trend is still up (never catch a falling knife in a bear market) and
    # require a real dislocation below the lower Bollinger band.
    period = int(params.get('rsi_period', 14))
    oversold = float(params.get('oversold', 35))
    bb_period = int(params.get('bb_period', 20))
    bb_std = float(params.get('bb_std', 2.0))
    trend = int(params.get('trend_period', 200))

    close = price_data['close']
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    ma = close.rolling(bb_period).mean()
    sd = close.rolling(bb_period).std()
    lower_band = ma - bb_std * sd
    trend_sma = close.rolling(trend, min_periods=1).mean()

    entries = ((rsi < oversold) & (close < lower_band) & (close > trend_sma)).fillna(False)
    exits = (close >= ma).fillna(False)                           # revert to mean
    return entries, exits
"""

TEMPLATE_BREAKOUT = """import pandas as pd
import numpy as np
import vectorbt as vbt

def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
    # Volatility breakout with trend + volume confirmation. A breakout only
    # counts if it happens in an uptrend AND on above-average volume — this
    # filters out the false breakouts that sink naive breakout systems.
    lookback = int(params.get('lookback', 20))
    exit_lookback = int(params.get('exit_lookback', 10))
    trend = int(params.get('trend_period', 100))
    vol_mult = float(params.get('volume_mult', 1.2))

    close = price_data['close']
    high = price_data['high']
    low = price_data['low']
    volume = price_data['volume']

    breakout = close > high.rolling(lookback).max().shift(1)
    trend_sma = close.rolling(trend, min_periods=1).mean()
    avg_vol = volume.rolling(lookback).mean()
    vol_ok = volume > (avg_vol * vol_mult)

    entries = (breakout & (close > trend_sma) & vol_ok).fillna(False)
    exits = (close < low.rolling(exit_lookback).min().shift(1)).fillna(False)
    return entries, exits
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class StrategyCoderState(TypedDict):
    hypothesis: dict          # from research_hypotheses
    ticker: str
    timeframe: str
    strategy_attempts: list[dict]
    current_code: str
    validation_errors: list[str]
    parameters: dict
    strategy_type: str
    attempt_count: int
    success: bool
    error: str | None
    id: str | None            # The stored signal ID


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONSTANTS & PROMPTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SYSTEM_PROMPT = """You are an expert quantitative trader writing Python trading strategies using VectorBT.

Generate a complete strategy for {ticker} based on:
Hypothesis: {hypothesis_description}
Direction: {expected_direction}
Type: {strategy_type}
Timeframe: {timeframe}

The strategy must have a genuine, economically-motivated edge tied to the
hypothesis above — NOT a generic textbook indicator crossover (a bare
moving-average crossover has no edge after costs). The backtest charges
0.1% fees + 0.1% slippage per trade and REJECTS any strategy that fails
to beat a buy-and-hold benchmark.

STRATEGY DESIGN PRINCIPLES:
- Trade the SPECIFIC inefficiency named in the hypothesis, not a bare crossover.
- Combine 2+ non-redundant conditions (e.g. a trend/regime filter PLUS a
  timing trigger) so entries are selective, not constant.
- Always include an explicit exit rule; never rely on a single condition.
- Aim for ~20-60 trades over 2 years of daily bars: enough for statistical
  significance, few enough that transaction costs don't dominate.
- Use only 3-5 parameters (more invites overfitting); prefer logic that is
  robust to small parameter changes.

STRICT REQUIREMENTS:
1. Use ONLY these imports: pandas, numpy, vectorbt as vbt
2. VectorBT Indicator Syntax Examples:
   - SMA: vbt.MA.run(close, window).ma
   - RSI: vbt.RSI.run(close, window).rsi
   - Bollinger: vbt.BBANDS.run(close, window).upper / .lower
3. The strategy function MUST follow this EXACT signature:
   def strategy(price_data: pd.DataFrame, params: dict) -> tuple[pd.Series, pd.Series]:
       # HELPER: Extract values safely (handles both numbers and range-dicts)
       def get_val(p_name, default):
           v = params.get(p_name, default)
           if isinstance(v, dict): return v.get('default', v.get('min', default))
           return v
       
       # Use get_val for all parameters
       # Example: window = int(get_val('window', 20))
       ...
       return entries, exits
3. price_data columns: open, high, low, close, volume
4. Include 3-5 tunable parameters in params dict
5. Add comments explaining each step
6. Handle edge cases (insufficient data, NaN values)

Return ONLY the Python code, no explanation.
No markdown code blocks. Raw Python only."""

FIX_PROMPT = """Fix these errors in the trading strategy code:
Errors: {validation_errors}

Original code:
{current_code}

Return ONLY the corrected Python code. No markdown code blocks. Raw Python only."""

PARAM_EXTRACT_PROMPT = """Analyze this Python trading strategy and identify the dictionary of hyperparameters it expects in the `params` argument.
For each parameter, provide a sensible min, max, and step value for optimization based on its name and typical usage.

Return JSON in this exact format:
{{
  "param_name_1": {{"min": 5, "max": 50, "step": 1}},
  "param_name_2": {{"min": 0.1, "max": 2.0, "step": 0.1}}
}}

Strategy Code:
{code}

Return ONLY valid JSON."""

FORBIDDEN_IMPORTS = [
    'import os', 'import sys', 'open(', 'subprocess', 'eval(', 'exec(',
    '__import__', 'requests', 'http', 'urllib', 'socket'
]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def clean_code(text: str) -> str:
    """Removes markdown code blocks if the LLM adds them despite instructions."""
    text = text.strip()
    if text.startswith("```python"):
        text = text[9:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()

async def classify_strategy_type_node(state: StrategyCoderState) -> dict[str, Any]:
    htype = state["hypothesis"].get("hypothesis_type", "").lower()
    
    if htype == "fundamental":
        stype = "trend" # or mean_reversion
    elif htype == "technical":
        stype = "momentum" # or breakout
    elif htype == "sentiment":
        stype = "event_driven"
    elif htype == "macro":
        stype = "trend"
    else:
        stype = "momentum"
        
    logger.info(f"Classified strategy type for {state['ticker']} as {stype}")
    return {"strategy_type": stype}

async def generate_strategy_code_node(state: StrategyCoderState) -> dict[str, Any]:
    ticker = state["ticker"]
    hypo = state["hypothesis"]
    
    prompt = SYSTEM_PROMPT.format(
        ticker=ticker,
        hypothesis_description=hypo.get("description", "No description"),
        expected_direction=hypo.get("expected_direction", "long"),
        strategy_type=state.get("strategy_type", "momentum"),
        timeframe=state["timeframe"]
    )
    
    messages = [SystemMessage(content=prompt)]
    
    try:
        res = await reasoning_llm.ainvoke(messages)
        code = clean_code(res.content)
        
        attempts = state.get("strategy_attempts", [])
        attempts.append({"code": code, "stage": "generate"})
        
        return {"current_code": code, "strategy_attempts": attempts, "attempt_count": state["attempt_count"] + 1}
    except Exception as e:
        logger.error(f"Failed to generate code for {ticker}: {e}")
        return {"error": str(e), "current_code": "", "attempt_count": state["attempt_count"] + 1}

def _validate_safety(code: str) -> list[str]:
    errors = []
    # Simple substring check
    for f in FORBIDDEN_IMPORTS:
        if f in code:
            errors.append(f"Forbidden pattern '{f}' found in code.")
            
    # AST check
    try:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names:
                    if name.name not in ["pandas", "numpy", "vectorbt"]:
                        errors.append(f"Forbidden import: {name.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module not in ["pandas", "numpy", "vectorbt"]:
                    errors.append(f"Forbidden import from: {node.module}")
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ["eval", "exec", "open"]:
                        errors.append(f"Forbidden function call: {node.func.id}")
    except Exception as e:
        errors.append(f"AST parsing failed during safety check: {e}")
    return errors

async def validate_code_node(state: StrategyCoderState) -> dict[str, Any]:
    code = state["current_code"]
    if not code:
        return {"validation_errors": ["Empty code"], "success": False}
        
    errors = []
    
    # 1. Syntax check
    try:
        compile(code, '<string>', 'exec')
    except SyntaxError as e:
        errors.append(f"SyntaxError: {e}")
        return {"validation_errors": errors, "success": False}
        
    # 2. Safety check
    safety_errors = _validate_safety(code)
    if safety_errors:
        errors.extend(safety_errors)
        return {"validation_errors": errors, "success": False}
        
    # 3 & 4. Signature & Dry run check
    try:
        # Create a tiny test DataFrame
        dates = pd.date_range("2020-01-01", periods=50, freq="D")
        np.random.seed(42)
        test_df = pd.DataFrame({
            "open": np.random.randn(50).cumsum() + 100,
            "high": np.random.randn(50).cumsum() + 105,
            "low": np.random.randn(50).cumsum() + 95,
            "close": np.random.randn(50).cumsum() + 100,
            "volume": np.random.randint(1000, 10000, 50)
        }, index=dates)
        
        # Execute code in safe namespace
        namespace = {}
        exec(code, namespace)
        
        if 'strategy' not in namespace:
            errors.append("Function 'strategy' not found in code.")
        else:
            strategy_func = namespace['strategy']
            # Try to run it
            entries, exits = strategy_func(test_df, {})
            
            if not isinstance(entries, pd.Series) or not isinstance(exits, pd.Series):
                errors.append(f"Strategy must return two pd.Series, got {type(entries)} and {type(exits)}")
            elif entries.dtype != bool or exits.dtype != bool:
                errors.append("Returned Series must be boolean")
                
    except Exception as e:
        errors.append(f"Dry run failed: {str(e)}")
        
    if errors:
        return {"validation_errors": errors, "success": False}
        
    return {"validation_errors": [], "success": True}

async def fix_code_node(state: StrategyCoderState) -> dict[str, Any]:
    prompt = FIX_PROMPT.format(
        validation_errors="; ".join(state["validation_errors"]),
        current_code=state["current_code"]
    )
    messages = [HumanMessage(content=prompt)]
    
    try:
        res = await reasoning_llm.ainvoke(messages)
        code = clean_code(res.content)
        
        attempts = state.get("strategy_attempts", [])
        attempts.append({"code": code, "stage": "fix"})
        
        return {"current_code": code, "strategy_attempts": attempts, "attempt_count": state["attempt_count"] + 1}
    except Exception as e:
        logger.error(f"Failed to fix code for {state['ticker']}: {e}")
        return {"error": str(e), "attempt_count": state["attempt_count"] + 1}

async def extract_parameters_node(state: StrategyCoderState) -> dict[str, Any]:
    prompt = PARAM_EXTRACT_PROMPT.format(code=state["current_code"])
    
    # Use document_llm (faster, typically gemini-flash) for extraction
    messages = [HumanMessage(content=prompt)]
    
    try:
        res = await document_llm.ainvoke(messages)
        text = clean_code(res.content)
        
        # Simple extraction between first { and last }
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            params_dict = json.loads(text[start:end+1])
            return {"parameters": params_dict}
            
        return {"parameters": {}}
    except Exception as e:
        logger.warning(f"Parameter extraction failed: {e}")
        return {"parameters": {}}

async def store_strategy_node(state: StrategyCoderState) -> dict[str, Any]:
    ticker = state["ticker"]
    
    if not state.get("success"):
        logger.warning(f"Strategy for {ticker} failed validation. Using fallback template.")
        # Fallback to templates based on strategy type
        stype = state.get("strategy_type", "")
        if stype == "trend" or stype == "event_driven":
            code = TEMPLATE_EMA_CROSSOVER
        elif stype == "mean_reversion":
            code = TEMPLATE_RSI_MEAN_REVERSION
        else:
            code = TEMPLATE_BREAKOUT
            
        state["current_code"] = code
        state["success"] = True
        state["parameters"] = {} # Default parameters will be used

    # Database Persistence
    try:
        engine = create_engine(settings.postgres_url)
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        
        signal_id = uuid.uuid4()
        hypo_id = state["hypothesis"].get("id")
        
        # Format hypothesis_id safely
        if isinstance(hypo_id, str):
            hypo_id = uuid.UUID(hypo_id)
        elif not hypo_id:
            hypo_id = uuid.uuid4() # Mock if missing
            
        new_signal = TradingSignal(
            id=signal_id,
            hypothesis_id=hypo_id,
            ticker=ticker,
            signal_name=f"{state.get('strategy_type', 'custom').capitalize()} Strategy for {ticker}",
            signal_type=state.get("strategy_type", "momentum"),
            entry_condition="Defined in code",
            exit_condition="Defined in code",
            strategy_code=state["current_code"],
            timeframe=state["timeframe"],
            parameters=state.get("parameters", {}),
            status="draft",
            created_by="strategy_coder_agent"
        )
        
        session.add(new_signal)
        session.flush() # Ensure signal_id is committed for foreign key constraints
        
        # Add parameter records
        for param_name, bounds in state.get("parameters", {}).items():
            param_record = SignalParameter(
                id=uuid.uuid4(),
                signal_id=signal_id,
                parameter_name=param_name,
                optimal_value=bounds.get("min", 0.0), # Will be optimized later
                search_range=bounds,
                optimization_method="grid",
                stability_score=0.0
            )
            session.add(param_record)
            
        session.commit()
        session.close()
        engine.dispose()
        logger.info(f"Stored trading signal {signal_id} for {ticker}")
        state["id"] = str(signal_id)
        
        # Publish to Redis
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            r.publish("signals.strategy.generated", json.dumps({
                "ticker": ticker,
                "signal_id": str(signal_id),
                "strategy_type": state.get("strategy_type", "momentum")
            }))
            r.close()
        except Exception as e:
            logger.warning(f"Failed to publish to Redis: {e}")
            
    except Exception as e:
        logger.error(f"Failed to store strategy: {e}")
        state["error"] = str(e)
        state["success"] = False

    return state


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def route_validation(state: StrategyCoderState) -> str:
    if state["success"]:
        return "extract_parameters_node"
    if state["attempt_count"] < 3:
        return "fix_code_node"
    return "store_strategy_node"

def build_strategy_coder_graph() -> StateGraph:
    graph = StateGraph(StrategyCoderState)
    
    graph.add_node("classify", classify_strategy_type_node)
    graph.add_node("generate", generate_strategy_code_node)
    graph.add_node("validate", validate_code_node)
    graph.add_node("fix_code_node", fix_code_node)
    graph.add_node("extract_parameters_node", extract_parameters_node)
    graph.add_node("store_strategy_node", store_strategy_node)
    
    graph.set_entry_point("classify")
    graph.add_edge("classify", "generate")
    graph.add_edge("generate", "validate")
    graph.add_edge("fix_code_node", "validate")
    graph.add_conditional_edges("validate", route_validation)
    graph.add_edge("extract_parameters_node", "store_strategy_node")
    graph.add_edge("store_strategy_node", END)
    
    return graph


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class StrategyCoderAgent:
    def __init__(self):
        self._graph = build_strategy_coder_graph().compile()
        logger.info("StrategyCoder initialised")
        
    async def generate(self, hypothesis: dict) -> dict:
        """
        Takes a research hypothesis dictionary and generates a trading strategy.
        """
        initial_state: StrategyCoderState = {
            "hypothesis": hypothesis,
            "ticker": hypothesis.get("ticker", "UNKNOWN"),
            "timeframe": hypothesis.get("expected_timeframe", "swing"),
            "strategy_attempts": [],
            "current_code": "",
            "validation_errors": [],
            "parameters": {},
            "strategy_type": "",
            "attempt_count": 0,
            "success": False,
            "error": None,
            "id": None
        }
        
        logger.info(f"Running StrategyCoder for {initial_state['ticker']}")
        final_state = await self._graph.ainvoke(initial_state)
        # The graph stores the generated code internally under 'current_code'.
        # Downstream consumers (backtester, signal pipeline) expect the
        # canonical key 'strategy_code' — expose it so the backtest node
        # actually receives the code instead of an empty string.
        if not final_state.get("strategy_code") and final_state.get("current_code"):
            final_state["strategy_code"] = final_state["current_code"]
        return final_state
        
    async def generate_batch(self, hypotheses: list[dict]) -> list[dict]:
        tasks = [self.generate(h) for h in hypotheses]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_results = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Batch generation error: {r}")
            else:
                valid_results.append(r)
                
        return valid_results
        
    async def regenerate(self, signal_id: uuid.UUID) -> Optional[dict]:
        """
        Future implementation: fetch existing signal from DB, extract its parent 
        hypothesis, and rerun generation.
        """
        logger.warning("regenerate method not yet implemented")
        return None
