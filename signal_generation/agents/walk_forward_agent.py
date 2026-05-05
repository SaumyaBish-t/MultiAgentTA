import asyncio
import json
import uuid
import itertools
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

from config.settings import settings
from config.llm_config import document_llm
from signal_generation.storage.signal_models import WalkForwardResult, SignalParameter, TradingSignal

# ==========================================
# STATE DEFINITION
# ==========================================
class WalkForwardState(TypedDict):
    signal: dict
    ticker: str
    price_data: dict
    n_splits: int
    train_pct: float
    splits: list[dict]
    in_sample_results: list[dict]
    out_sample_results: list[dict]
    consistency_score: float
    overfit_score: float
    passed: bool
    recommendation: str
    error: str | None

# ==========================================
# GRAPH NODES
# ==========================================
async def fetch_price_data_node(state: WalkForwardState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    ticker = state["ticker"]
    try:
        url = f"http://localhost:8000/prices/{ticker}/history?days=1095" # 3 years
        headers = {"x-api-key": settings.internal_api_key}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            data = response.json()
            
        if not data or len(data) < 200:
            raise ValueError(f"Insufficient data for {ticker}: {len(data)} bars.")
            
        df = pd.DataFrame(data)
        if "timestamp" in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
        else:
            raise ValueError("No timestamp column in price data")
        
        # Strip tz to avoid VectorBT issues
        if df.index.tz is not None:
            df.index = df.index.tz_convert(None)
            
        return {"price_data": {ticker: df}}
    except Exception as e:
        logger.error(f"Failed to fetch data for {ticker}: {e}")
        return {"error": str(e)}

def prepare_splits_node(state: WalkForwardState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        ticker = state["ticker"]
        price_df = state["price_data"][ticker]
        n_splits = state.get("n_splits", 5)
        train_pct = state.get("train_pct", 0.7)
        
        total_days = len(price_df)
        window = total_days / n_splits
        
        splits = []
        for i in range(n_splits):
            train_end_idx = int(window * (i + train_pct))
            test_start_idx = train_end_idx
            test_end_idx = int(window * (i + 1))
            
            # Bound checks
            train_end_idx = min(train_end_idx, total_days - 1)
            test_start_idx = min(test_start_idx, total_days - 1)
            test_end_idx = min(test_end_idx, total_days)
            
            train_df = price_df.iloc[:train_end_idx] # Expanding window for training, as typical
            test_df = price_df.iloc[test_start_idx:test_end_idx]
            
            if len(train_df) == 0 or len(test_df) == 0:
                continue
                
            splits.append({
                "split_id": i + 1,
                "train_start": str(train_df.index[0].date()),
                "train_end": str(train_df.index[-1].date()),
                "test_start": str(test_df.index[0].date()),
                "test_end": str(test_df.index[-1].date()),
                "train_df": train_df,
                "test_df": test_df
            })
            
        if not splits:
            raise ValueError("No valid splits could be generated from the data.")
            
        return {"splits": splits}
    except Exception as e:
        logger.error(f"Failed to prepare splits: {e}")
        return {"error": str(e)}

def run_in_sample_node(state: WalkForwardState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        code = state["signal"]["strategy_code"]
        params = state["signal"].get("parameters", {})
        splits = state["splits"]
        
        in_sample_results = []
        
        for split in splits:
            train_df = split["train_df"]
            
            # Sandbox setup
            exec_globals = {
                "pd": pd,
                "np": np,
                "vbt": vbt,
                "__builtins__": __builtins__
            }
            
            # Use original code since built-ins are restored
            exec(code, exec_globals)
            strategy_func = exec_globals['strategy']
            
            entries, exits = strategy_func(train_df, params)
            
            portfolio = vbt.Portfolio.from_signals(
                close=train_df['close'],
                entries=entries,
                exits=exits,
                init_cash=100_000,
                fees=0.001,
                freq='1D'
            )
            
            sharpe = portfolio.sharpe_ratio()
            if pd.isna(sharpe):
                sharpe = 0.0
                
            in_sample_results.append({
                "split_id": split["split_id"],
                "sharpe": float(sharpe),
                "return_pct": float(portfolio.total_return()) * 100,
                "trades": int(portfolio.trades.count()),
                "max_drawdown_pct": float(portfolio.max_drawdown()) * 100
            })
            
        return {"in_sample_results": in_sample_results}
    except Exception as e:
        logger.error(f"In-sample run failed: {e}")
        return {"error": str(e)}

def generate_param_grid(parameters: dict) -> dict:
    grid = {}
    for param_name, value in parameters.items():
        if isinstance(value, bool):
            continue
        elif isinstance(value, int):
            low  = max(1, int(value * 0.80))
            high = int(value * 1.20)
            step = max(1, (high - low) // 8)
            grid[param_name] = {
                "min": low,
                "max": high,
                "step": step
            }
        elif isinstance(value, float):
            low  = round(value * 0.80, 4)
            high = round(value * 1.20, 4)
            step = round((high - low) / 8, 4)
            if step == 0:
                step = 0.01
            grid[param_name] = {
                "min": low,
                "max": high,
                "step": step
            }
        else:
            continue
    return grid

def optimize_parameters_node(state: WalkForwardState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        code = state["signal"]["strategy_code"]
        base_params = state["signal"].get("parameters", {})
        splits = state["splits"]
        
        # Sandbox setup
        exec_globals = {
            "pd": pd,
            "np": np,
            "vbt": vbt,
            "__builtins__": __builtins__
        }
        # Use original code since built-ins are restored
        exec(code, exec_globals)
        strategy_func = exec_globals['strategy']
        
        param_grid = generate_param_grid(base_params)
        
        keys = list(param_grid.keys())
        values = []
        for k in keys:
            min_val = param_grid[k]['min']
            max_val = param_grid[k]['max']
            step = param_grid[k]['step']
            vals = []
            curr = min_val
            while curr <= max_val:
                vals.append(curr)
                curr += step
            values.append(vals)
            
        combos = list(itertools.product(*values)) if keys else [tuple()]
        
        # For each split, find the optimal combo
        for split in splits:
            train_df = split["train_df"]
            
            if not keys or not combos:
                split["best_params"] = base_params
                split["param_grid"] = param_grid
                continue
                
            entries_list = []
            exits_list = []
            col_names = []
            
            for combo in combos:
                current_params = base_params.copy()
                for i, k in enumerate(keys):
                    current_params[k] = type(base_params[k])(combo[i]) # recast to int/float
                    
                entries, exits = strategy_func(train_df, current_params)
                
                combo_str = "_".join(f"{k}={v}" for k, v in zip(keys, combo))
                entries_list.append(entries.rename(combo_str))
                exits_list.append(exits.rename(combo_str))
                col_names.append(combo)
                
            # Vectorized evaluation
            entries_df = pd.concat(entries_list, axis=1)
            exits_df = pd.concat(exits_list, axis=1)
            
            portfolio = vbt.Portfolio.from_signals(
                close=train_df['close'],
                entries=entries_df,
                exits=exits_df,
                init_cash=100_000,
                fees=0.001,
                freq='1D'
            )
            
            sharpes = portfolio.sharpe_ratio()
            best_idx = sharpes.idxmax()
            
            if pd.isna(best_idx):
                split["best_params"] = base_params
            else:
                # Find the combo dict
                best_combo_idx = list(entries_df.columns).index(best_idx)
                best_combo_tuple = col_names[best_combo_idx]
                
                best_params = base_params.copy()
                for i, k in enumerate(keys):
                    best_params[k] = type(base_params[k])(best_combo_tuple[i])
                split["best_params"] = best_params
                
            split["param_grid"] = param_grid
                
        return {"splits": splits}
    except Exception as e:
        logger.error(f"Parameter optimization failed: {e}")
        return {"error": str(e)}

def run_out_sample_node(state: WalkForwardState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        code = state["signal"]["strategy_code"]
        splits = state["splits"]
        
        out_sample_results = []
        
        # Sandbox setup
        exec_globals = {
            "pd": pd,
            "np": np,
            "vbt": vbt,
            "__builtins__": __builtins__
        }
        # Use original code since built-ins are restored
        exec(code, exec_globals)
        strategy_func = exec_globals['strategy']
        
        for split in splits:
            test_df = split["test_df"]
            best_params = split.get("best_params", state["signal"].get("parameters", {}))
            
            entries, exits = strategy_func(test_df, best_params)
            
            portfolio = vbt.Portfolio.from_signals(
                close=test_df['close'],
                entries=entries,
                exits=exits,
                init_cash=100_000,
                fees=0.001,
                freq='1D'
            )
            
            sharpe = portfolio.sharpe_ratio()
            if pd.isna(sharpe):
                sharpe = 0.0
                
            out_sample_results.append({
                "split_id": split["split_id"],
                "sharpe": float(sharpe),
                "return_pct": float(portfolio.total_return()) * 100,
                "trades": int(portfolio.trades.count()),
                "max_drawdown_pct": float(portfolio.max_drawdown()) * 100,
                "best_params": best_params
            })
            
        return {"out_sample_results": out_sample_results}
    except Exception as e:
        logger.error(f"Out-of-sample run failed: {e}")
        return {"error": str(e)}

async def compute_robustness_node(state: WalkForwardState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        oos_results = state["out_sample_results"]
        is_results = state["in_sample_results"]
        n_splits = state.get("n_splits", 5)
        
        oos_sharpes = [res["sharpe"] for res in oos_results]
        is_sharpes = [res["sharpe"] for res in is_results]
        
        positive_splits = sum(1 for s in oos_sharpes if s > 0)
        consistency_score = positive_splits / n_splits
        
        def safe_float(val):
            if pd.isna(val) or val == float('inf') or val == float('-inf'):
                return 0.0
            return float(val)

        avg_is_sharpe = safe_float(np.mean(is_sharpes))
        avg_oos_sharpe = safe_float(np.mean(oos_sharpes))
        
        overfit_score = safe_float(avg_is_sharpe / max(avg_oos_sharpe, 0.01))
        
        # Criteria
        passed = bool(
            avg_oos_sharpe >= 0.5 and
            consistency_score >= 0.6 and
            overfit_score <= 2.0
        )
        
        # LLM Recommendation
        metrics_summary = {
            "avg_is_sharpe": avg_is_sharpe,
            "avg_oos_sharpe": avg_oos_sharpe,
            "consistency": consistency_score,
            "overfit_score": overfit_score
        }
        
        llm = document_llm # Use fast LLM
        prompt = f"Given these walk-forward results: {metrics_summary}. Give a 1-sentence recommendation on whether to proceed with this strategy. Be direct."
        
        try:
            response = await llm.ainvoke(prompt)
            recommendation = response.content.strip()
        except Exception as e:
            logger.warning(f"LLM Recommendation failed: {e}")
            recommendation = "Proceed" if passed else "Reject due to poor walk-forward metrics."
            
        return {
            "consistency_score": consistency_score,
            "overfit_score": overfit_score,
            "passed": passed,
            "recommendation": recommendation
        }
    except Exception as e:
        logger.error(f"Compute robustness failed: {e}")
        return {"error": str(e)}

def store_results_node(state: WalkForwardState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        signal_id = state["signal"]["id"]
        ticker = state["ticker"]
        
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        with Session() as session:
            def safe_float(val):
                if pd.isna(val) or val == float('inf') or val == float('-inf'):
                    return 0.0
                return float(val)

            # 1. Update trading_signals
            signal_record = session.query(TradingSignal).filter_by(id=signal_id).first()
            if signal_record:
                if not state["passed"]:
                    signal_record.status = "rejected"
                # If passed, keep it 'validated'
                
            # 2. Insert WalkForwardResult
            avg_is = safe_float(np.mean([r["sharpe"] for r in state["in_sample_results"]]))
            avg_oos = safe_float(np.mean([r["sharpe"] for r in state["out_sample_results"]]))
            
            # Prepare splits detail JSON
            splits_detail = []
            for split, is_res, oos_res in zip(state["splits"], state["in_sample_results"], state["out_sample_results"]):
                splits_detail.append({
                    "split_id": split["split_id"],
                    "train_period": f"{split['train_start']} to {split['train_end']}",
                    "test_period": f"{split['test_start']} to {split['test_end']}",
                    "is_sharpe": safe_float(is_res["sharpe"]),
                    "oos_sharpe": safe_float(oos_res["sharpe"]),
                    "best_params": oos_res.get("best_params", {})
                })
                
            wf_record = WalkForwardResult(
                id=uuid.uuid4(),
                signal_id=signal_id,
                ticker=ticker,
                n_splits=state.get("n_splits", 5),
                train_pct=state.get("train_pct", 0.7),
                in_sample_sharpe=avg_is,
                out_sample_sharpe=avg_oos,
                consistency_score=state["consistency_score"],
                overfit_score=state["overfit_score"],
                passed=state["passed"],
                splits_detail=splits_detail,
                tested_at=datetime.now(timezone.utc)
            )
            session.add(wf_record)
            
            # 3. Insert SignalParameters (optimal params across all splits, taking the mode or last split)
            # We'll store the optimal parameters from the final split
            final_split = state["splits"][-1]
            param_grid = final_split.get("param_grid", {})
            best_params = final_split.get("best_params", {})
            
            for param_name, opt_val in best_params.items():
                sp_record = SignalParameter(
                    id=uuid.uuid4(),
                    signal_id=signal_id,
                    parameter_name=param_name,
                    optimal_value=float(opt_val) if isinstance(opt_val, (int, float)) else 0.0,
                    search_range=param_grid.get(param_name, {}),
                    optimization_method="auto_grid_±20pct",
                    stability_score=state["consistency_score"],
                    optimized_at=datetime.now(timezone.utc)
                )
                session.add(sp_record)
                
            session.commit()
            
        logger.info(f"Walk-forward completed for {ticker}. Passed: {state['passed']}")
        return {}
    except Exception as e:
        logger.error(f"Store results failed: {e}")
        return {"error": str(e)}


class WalkForwardValidator:
    def __init__(self):
        # Build LangGraph
        workflow = StateGraph(WalkForwardState)
        
        workflow.add_node("fetch_data", fetch_price_data_node)
        workflow.add_node("prepare_splits", prepare_splits_node)
        workflow.add_node("run_is", run_in_sample_node)
        workflow.add_node("optimize", optimize_parameters_node)
        workflow.add_node("run_oos", run_out_sample_node)
        workflow.add_node("robustness", compute_robustness_node)
        workflow.add_node("store", store_results_node)
        
        workflow.add_edge("fetch_data", "prepare_splits")
        workflow.add_edge("prepare_splits", "run_is")
        workflow.add_edge("run_is", "optimize")
        workflow.add_edge("optimize", "run_oos")
        workflow.add_edge("run_oos", "robustness")
        workflow.add_edge("robustness", "store")
        workflow.add_edge("store", END)
        
        workflow.set_entry_point("fetch_data")
        self.app = workflow.compile()
        logger.info("WalkForwardValidator agent initialised")
        
    async def validate(self, signal: dict, n_splits: int = 5, train_pct: float = 0.7) -> dict:
        state: WalkForwardState = {
            "signal": signal,
            "ticker": signal["ticker"],
            "price_data": {},
            "n_splits": n_splits,
            "train_pct": train_pct,
            "splits": [],
            "in_sample_results": [],
            "out_sample_results": [],
            "consistency_score": 0.0,
            "overfit_score": 0.0,
            "passed": False,
            "recommendation": "",
            "error": None
        }
        
        final_state = await self.app.ainvoke(state)
        return final_state
        
    async def validate_batch(self, signals: list[dict]) -> list[dict]:
        tasks = [self.validate(sig) for sig in signals]
        return await asyncio.gather(*tasks)

    def get_robustness_report(self, signal_id: uuid.UUID) -> dict:
        engine = create_engine(settings.postgres_url)
        Session = sessionmaker(bind=engine)
        
        with Session() as session:
            record = session.query(WalkForwardResult).filter_by(signal_id=signal_id).first()
            if not record:
                return {}
                
            return {
                "n_splits": record.n_splits,
                "in_sample_sharpe": record.in_sample_sharpe,
                "out_sample_sharpe": record.out_sample_sharpe,
                "consistency": record.consistency_score,
                "overfit_score": record.overfit_score,
                "passed": record.passed,
                "splits": record.splits_detail
            }
