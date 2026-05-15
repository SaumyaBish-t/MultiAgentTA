import asyncio
import json
import uuid
import itertools
import random
from typing import TypedDict, Any
from datetime import datetime, timezone, date

import pandas as pd
import numpy as np
import vectorbt as vbt
import httpx
from loguru import logger
from langgraph.graph import StateGraph, END
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import optuna

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal, SignalParameter

optuna.logging.set_verbosity(optuna.logging.WARNING)

# ==========================================
# STATE DEFINITION
# ==========================================
class OptimizerState(TypedDict):
    signal: dict
    ticker: str
    price_data: dict           # Includes optimization_df and validation_df
    param_ranges: dict
    optimization_method: str   # grid/bayesian/random
    n_trials: int
    results: list[dict]        # all trial results (optional)
    best_params: dict
    best_sharpe: float
    stability_scores: dict     # per parameter stability
    oos_sharpe_drop: float
    overfit_warning: bool
    error: str | None

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def safe_float(val: float) -> float:
    if pd.isna(val) or val == float('inf') or val == float('-inf'):
        return 0.0
    return float(val)

def generate_param_values(param_name: str, bounds: dict, max_samples: int = 100) -> list:
    """Generate discrete values from a parameter range dict (min, max, step)."""
    min_val = bounds.get('min', 0)
    max_val = bounds.get('max', min_val)
    step = bounds.get('step', 1)
    
    if step == 0:
        return [min_val]
        
    vals = []
    curr = min_val
    while curr <= max_val and len(vals) < max_samples:
        vals.append(curr)
        curr += step
    return vals

def execute_strategy_sandbox(code: str, df: pd.DataFrame, params: dict):
    exec_globals = {
        "pd": pd,
        "np": np,
        "vbt": vbt,
        "__builtins__": __builtins__
    }
    # Use original code since built-ins are restored
    exec(code, exec_globals)
    strategy_func = exec_globals['strategy']
    return strategy_func(df, params)

# ==========================================
# GRAPH NODES
# ==========================================
async def setup_optimization_node(state: OptimizerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    ticker = state["ticker"]
    param_ranges = state["param_ranges"]
    
    # Determine Method
    num_params = len(param_ranges)
    method = state.get("optimization_method")
    
    if not method:
        if num_params <= 2:
            method = "grid"
        elif num_params <= 4:
            method = "random"
        else:
            method = "bayesian"
            
    n_trials = state.get("n_trials", 100)
    if method == "random" and n_trials < 200:
        n_trials = 200 # Default to more for random
        
    try:
        # Fetch 24 months of data (~730 days)
        url = f"http://localhost:8000/prices/{ticker}/history?days=730"
        headers = {"x-api-key": settings.internal_api_key}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            data = response.json()
            
        if not data or len(data) < 200:
            raise ValueError(f"Insufficient data for {ticker}: {len(data)} bars. Need at least 200 for Optimization.")
            
        df = pd.DataFrame(data)
        if "timestamp" in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601', utc=True)
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
        else:
            raise ValueError("No timestamp column in price data")
            
        if df.index.tz is not None:
            df.index = df.index.tz_convert(None)
            
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df.ffill(inplace=True)
            
        # Split: First 18 months Optimization, Last 6 months Validation
        total_days = len(df)
        split_idx = int(total_days * 0.75)
        
        opt_df = df.iloc[:split_idx]
        val_df = df.iloc[split_idx:]
        
        return {
            "optimization_method": method,
            "n_trials": n_trials,
            "price_data": {
                "optimization_df": opt_df,
                "validation_df": val_df
            }
        }
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        return {"error": str(e)}

def run_grid_search_node(state: OptimizerState) -> dict[str, Any]:
    if state.get("error") or state.get("optimization_method") != "grid":
        return {}
        
    try:
        code = state["signal"]["strategy_code"]
        opt_df = state["price_data"]["optimization_df"]
        param_ranges = state["param_ranges"]
        
        keys = list(param_ranges.keys())
        values = [generate_param_values(k, param_ranges[k]) for k in keys]
        
        combos = list(itertools.product(*values))
        if not combos:
            return {"best_params": {}, "best_sharpe": 0.0}
            
        entries_list, exits_list, col_names = [], [], []
        
        for combo in combos:
            params = dict(zip(keys, combo))
            # Recast
            for k in keys:
                is_float = isinstance(param_ranges[k].get("step", 1), float)
                params[k] = float(params[k]) if is_float else int(params[k])
                
            entries, exits = execute_strategy_sandbox(code, opt_df, params)
            combo_str = "_".join(f"{k}={v}" for k, v in params.items())
            entries_list.append(entries.rename(combo_str))
            exits_list.append(exits.rename(combo_str))
            col_names.append(params)
            
        entries_df = pd.concat(entries_list, axis=1)
        exits_df = pd.concat(exits_list, axis=1)
        
        portfolio = vbt.Portfolio.from_signals(
            close=opt_df['close'],
            entries=entries_df,
            exits=exits_df,
            init_cash=100_000,
            fees=0.001,
            freq='1D'
        )
        
        sharpes = portfolio.sharpe_ratio()
        best_idx = sharpes.idxmax()
        
        if pd.isna(best_idx):
            return {"best_params": col_names[0], "best_sharpe": 0.0}
            
        best_combo_idx = list(entries_df.columns).index(best_idx)
        best_params = col_names[best_combo_idx]
        best_sharpe = safe_float(sharpes.iloc[best_combo_idx])
        
        return {"best_params": best_params, "best_sharpe": best_sharpe}
    except Exception as e:
        logger.error(f"Grid search failed: {e}")
        return {"error": str(e)}

def run_random_search_node(state: OptimizerState) -> dict[str, Any]:
    if state.get("error") or state.get("optimization_method") != "random":
        return {}
        
    try:
        code = state["signal"]["strategy_code"]
        opt_df = state["price_data"]["optimization_df"]
        param_ranges = state["param_ranges"]
        n_trials = state["n_trials"]
        
        keys = list(param_ranges.keys())
        values = [generate_param_values(k, param_ranges[k]) for k in keys]
        all_combos = list(itertools.product(*values))
        
        if not all_combos:
            return {"best_params": {}, "best_sharpe": 0.0}
            
        # Sample combinations
        if len(all_combos) > n_trials:
            sampled_combos = random.sample(all_combos, n_trials)
        else:
            sampled_combos = all_combos
            
        entries_list, exits_list, col_names = [], [], []
        
        for combo in sampled_combos:
            params = dict(zip(keys, combo))
            for k in keys:
                is_float = isinstance(param_ranges[k].get("step", 1), float)
                params[k] = float(params[k]) if is_float else int(params[k])
                
            entries, exits = execute_strategy_sandbox(code, opt_df, params)
            combo_str = "_".join(f"{k}={v}" for k, v in params.items())
            entries_list.append(entries.rename(combo_str))
            exits_list.append(exits.rename(combo_str))
            col_names.append(params)
            
        entries_df = pd.concat(entries_list, axis=1)
        exits_df = pd.concat(exits_list, axis=1)
        
        portfolio = vbt.Portfolio.from_signals(
            close=opt_df['close'],
            entries=entries_df,
            exits=exits_df,
            init_cash=100_000,
            fees=0.001,
            freq='1D'
        )
        
        sharpes = portfolio.sharpe_ratio()
        best_idx = sharpes.idxmax()
        
        if pd.isna(best_idx):
            return {"best_params": col_names[0], "best_sharpe": 0.0}
            
        best_combo_idx = list(entries_df.columns).index(best_idx)
        best_params = col_names[best_combo_idx]
        best_sharpe = safe_float(sharpes.iloc[best_combo_idx])
        
        return {"best_params": best_params, "best_sharpe": best_sharpe}
    except Exception as e:
        logger.error(f"Random search failed: {e}")
        return {"error": str(e)}

def run_bayesian_search_node(state: OptimizerState) -> dict[str, Any]:
    if state.get("error") or state.get("optimization_method") != "bayesian":
        return {}
        
    try:
        code = state["signal"]["strategy_code"]
        opt_df = state["price_data"]["optimization_df"]
        param_ranges = state["param_ranges"]
        n_trials = state["n_trials"]
        
        def objective(trial):
            params = {}
            for k, bounds in param_ranges.items():
                min_v = bounds.get('min', 0)
                max_v = bounds.get('max', 1)
                step = bounds.get('step', 1)
                
                if isinstance(step, float) and step > 0:
                    params[k] = trial.suggest_float(k, min_v, max_v, step=step)
                else:
                    params[k] = trial.suggest_int(k, int(min_v), int(max_v), step=int(step) if step > 0 else 1)
                    
            try:
                entries, exits = execute_strategy_sandbox(code, opt_df, params)
                portfolio = vbt.Portfolio.from_signals(
                    close=opt_df['close'],
                    entries=entries,
                    exits=exits,
                    init_cash=100_000,
                    fees=0.001,
                    freq='1D'
                )
                sharpe = portfolio.sharpe_ratio()
                return safe_float(sharpe)
            except Exception:
                return -10.0 # Heavy penalty for failure
                
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, n_jobs=1) # Sequential for sandbox safety
        
        best_params = study.best_params
        best_sharpe = study.best_value
        
        return {"best_params": best_params, "best_sharpe": best_sharpe}
    except Exception as e:
        logger.error(f"Bayesian search failed: {e}")
        return {"error": str(e)}

def assess_parameter_stability_node(state: OptimizerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        code = state["signal"]["strategy_code"]
        opt_df = state["price_data"]["optimization_df"]
        best_params = state["best_params"]
        
        stability_scores = {}
        
        for p_name, p_val in best_params.items():
            # Perturb +/- 20%
            is_float = isinstance(p_val, float)
            
            if is_float:
                perturbations = [p_val * 0.8, p_val * 0.9, p_val * 1.1, p_val * 1.2]
            else:
                perturbations = [
                    int(p_val * 0.8), int(p_val * 0.9), 
                    int(p_val * 1.1), int(p_val * 1.2)
                ]
            perturbations = list(set(perturbations)) # unique
            
            sharpes = []
            for pert in perturbations:
                test_params = best_params.copy()
                test_params[p_name] = pert
                
                try:
                    entries, exits = execute_strategy_sandbox(code, opt_df, test_params)
                    portfolio = vbt.Portfolio.from_signals(
                        close=opt_df['close'],
                        entries=entries,
                        exits=exits,
                        init_cash=100_000,
                        fees=0.001,
                        freq='1D'
                    )
                    sharpes.append(safe_float(portfolio.sharpe_ratio()))
                except Exception:
                    sharpes.append(0.0)
                    
            if not sharpes:
                stability_scores[p_name] = 0.0
                continue
                
            sharpes.append(state["best_sharpe"]) # include base
            s_mean = np.mean(sharpes)
            s_std = np.std(sharpes)
            
            # High std relative to mean = low stability
            if s_mean <= 0:
                score = 0.0
            else:
                score = max(0.0, 1.0 - (s_std / s_mean))
                
            stability_scores[p_name] = float(score)
            
        return {"stability_scores": stability_scores}
    except Exception as e:
        logger.error(f"Stability assessment failed: {e}")
        return {"error": str(e)}

def final_validation_node(state: OptimizerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        code = state["signal"]["strategy_code"]
        val_df = state["price_data"]["validation_df"]
        best_params = state["best_params"]
        best_opt_sharpe = state["best_sharpe"]
        
        entries, exits = execute_strategy_sandbox(code, val_df, best_params)
        portfolio = vbt.Portfolio.from_signals(
            close=val_df['close'],
            entries=entries,
            exits=exits,
            init_cash=100_000,
            fees=0.001,
            freq='1D'
        )
        
        oos_sharpe = safe_float(portfolio.sharpe_ratio())
        
        # Check for significant drop
        drop = 0.0
        warning = False
        if best_opt_sharpe > 0:
            drop = (best_opt_sharpe - oos_sharpe) / best_opt_sharpe
            if drop > 0.5:
                warning = True
                logger.warning(f"Strategy {state['ticker']} failed final validation. OOS Sharpe {oos_sharpe:.2f} vs IS {best_opt_sharpe:.2f}")
                
        return {"oos_sharpe_drop": drop, "overfit_warning": warning}
    except Exception as e:
        logger.error(f"Final validation failed: {e}")
        return {"error": str(e)}

def store_results_node(state: OptimizerState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        signal_id = state["signal"]["id"]
        
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        with Session() as session:
            # 1. Update TradingSignal
            signal_record = session.query(TradingSignal).filter_by(id=signal_id).first()
            if signal_record:
                # Merge new optimized params
                current_params = dict(signal_record.parameters) if signal_record.parameters else {}
                current_params.update(state["best_params"])
                signal_record.parameters = current_params
                
                if state["overfit_warning"]:
                    signal_record.status = "rejected"
                    
            # 2. Update SignalParameters
            sp_records = session.query(SignalParameter).filter_by(signal_id=signal_id).all()
            for record in sp_records:
                p_name = record.parameter_name
                if p_name in state["best_params"]:
                    record.optimal_value = state["best_params"][p_name]
                    record.optimization_method = state["optimization_method"]
                    record.stability_score = state["stability_scores"].get(p_name, 0.0)
                    record.optimized_at = datetime.now(timezone.utc)
                    
            session.commit()
            
        # Log successful completion
        logger.info(f"Optimization finished for {state['ticker']}. Method: {state['optimization_method']}. Warning: {state['overfit_warning']}")
        return {}
    except Exception as e:
        logger.error(f"Store optimization results failed: {e}")
        return {"error": str(e)}

def route_optimization(state: OptimizerState) -> str:
    return state.get("optimization_method", "grid")

class OptimizerAgent:
    def __init__(self):
        workflow = StateGraph(OptimizerState)
        
        workflow.add_node("setup", setup_optimization_node)
        workflow.add_node("grid", run_grid_search_node)
        workflow.add_node("random", run_random_search_node)
        workflow.add_node("bayesian", run_bayesian_search_node)
        workflow.add_node("stability", assess_parameter_stability_node)
        workflow.add_node("validation", final_validation_node)
        workflow.add_node("store", store_results_node)
        
        workflow.add_conditional_edges("setup", route_optimization)
        
        workflow.add_edge("grid", "stability")
        workflow.add_edge("random", "stability")
        workflow.add_edge("bayesian", "stability")
        
        workflow.add_edge("stability", "validation")
        workflow.add_edge("validation", "store")
        workflow.add_edge("store", END)
        
        workflow.set_entry_point("setup")
        self.app = workflow.compile()
        logger.info("ParameterOptimizer agent initialised")
        
    async def optimize(self, signal: dict, param_ranges: dict, n_trials: int = 100, method: str = None) -> dict:
        state: OptimizerState = {
            "signal": signal,
            "ticker": signal["ticker"],
            "price_data": {},
            "param_ranges": param_ranges,
            "optimization_method": method,
            "n_trials": n_trials,
            "results": [],
            "best_params": {},
            "best_sharpe": 0.0,
            "stability_scores": {},
            "oos_sharpe_drop": 0.0,
            "overfit_warning": False,
            "error": None
        }
        
        final_state = await self.app.ainvoke(state)
        return final_state
        
    async def quick_optimize(self, signal: dict, param_ranges: dict) -> dict:
        return await self.optimize(signal, param_ranges, n_trials=20)
