import uuid
import json
import httpx
import asyncio
from datetime import datetime, timezone
from typing import Any, TypedDict, Optional

import pandas as pd
import numpy as np
import vectorbt as vbt
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import redis
from langgraph.graph import StateGraph, END

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal, BacktestResult

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class BacktesterState(TypedDict):
    signal: dict              # from trading_signals
    ticker: str
    price_data: dict          # OHLCV as dict containing target ticker and SPY
    strategy_code: str
    parameters: dict
    entries: list[bool]
    exits: list[bool]
    portfolio: Any | None     # vectorbt portfolio stats
    metrics: dict             # computed performance metrics
    benchmark_metrics: dict   # SPY buy-hold comparison
    passed_filters: bool
    rejection_reasons: list[str]
    error: str | None

_API_BASE = "http://localhost:8000"
_API_HEADERS = {"x-api-key": settings.internal_api_key}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GRAPH NODES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def fetch_price_data_node(state: BacktesterState) -> dict[str, Any]:
    ticker = state["ticker"]
    
    # We need to fetch data for the target ticker and for the benchmark (SPY)
    price_data = {}
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            # Fetch target ticker
            res_target = await client.get(f"{_API_BASE}/prices/{ticker}/history", params={"days": 756}, headers=_API_HEADERS)
            res_target.raise_for_status()
            target_data = res_target.json()
            
            if not target_data or len(target_data) < 200:
                logger.warning(f"Insufficient data for {ticker}: {len(target_data)} bars. Need >= 200.")
                return {"error": f"Insufficient data: {len(target_data)} bars", "rejection_reasons": ["INSUFFICIENT_DATA"]}
                
            price_data[ticker] = target_data
            
            # Fetch benchmark
            res_spy = await client.get(f"{_API_BASE}/prices/SPY/history", params={"days": 756}, headers=_API_HEADERS)
            res_spy.raise_for_status()
            spy_data = res_spy.json()
            
            if not spy_data or len(spy_data) < 200:
                logger.warning(f"Insufficient benchmark data for SPY. Continuing without benchmark.")
            else:
                price_data["SPY"] = spy_data
            
            return {"price_data": price_data}
            
        except Exception as e:
            logger.error(f"Failed to fetch price data via API: {e}")
            return {"error": str(e), "rejection_reasons": ["API_FETCH_ERROR"]}

async def prepare_data_node(state: BacktesterState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        raw_data = state["price_data"]
        ticker = state["ticker"]
        
        # Convert to pandas DataFrame and set datetime index
        target_df = pd.DataFrame(raw_data[ticker])
        if "timestamp" in target_df.columns:
            target_df["timestamp"] = pd.to_datetime(target_df["timestamp"])
            target_df.set_index("timestamp", inplace=True)
            
        # Clean and forward fill missing values (max 3 days)
        target_df.ffill(limit=3, inplace=True)
        # Drop rows that still have NaNs (e.g. at the beginning)
        target_df.dropna(inplace=True)
        
        # Prepare SPY data if available
        if "SPY" in raw_data:
            spy_df = pd.DataFrame(raw_data["SPY"])
            if "timestamp" in spy_df.columns:
                spy_df["timestamp"] = pd.to_datetime(spy_df["timestamp"])
                spy_df.set_index("timestamp", inplace=True)
            spy_df.ffill(limit=3, inplace=True)
            spy_df.dropna(inplace=True)
            
            # Align SPY data with target ticker index to ensure matching shapes
            spy_df = spy_df.reindex(target_df.index).ffill()
            return {"price_data": {ticker: target_df, "SPY": spy_df}}
            
        return {"price_data": {ticker: target_df}}
    except Exception as e:
        logger.error(f"Failed to prepare data: {e}")
        return {"error": str(e), "rejection_reasons": ["DATA_PREP_ERROR"]}

async def run_strategy_node(state: BacktesterState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        code = state["strategy_code"]
        params = state["parameters"]
        ticker = state["ticker"]
        price_df = state["price_data"][ticker]
        
        exec_globals = {
            "pd": pd,
            "np": np,
            "vbt": vbt,
            "__builtins__": __builtins__
        }
        
        # We don't strip imports here because the StrategyCoder's safety check 
        # already validated them. Removing imports can break code that relies on them.
        exec(code, exec_globals)
        
        if 'strategy' not in exec_globals:
            raise ValueError("Function 'strategy' missing from code namespace")
            
        strategy_fn = exec_globals['strategy']
        
        entries, exits = strategy_fn(price_df, params)
        
        if not isinstance(entries, pd.Series) or not isinstance(exits, pd.Series):
            raise TypeError("Strategy must return two pd.Series")
            
        if len(entries) != len(price_df) or len(exits) != len(price_df):
            raise ValueError("Returned Series length does not match price data length")
            
        # Ensure no overlap (vectorbt clean logic: if entering, can't enter again; if entering, can't exit on same bar optionally)
        entries = entries.fillna(False).astype(bool)
        exits = exits.fillna(False).astype(bool)
        
        # Clean overlapping signals
        # If both are true on the same bar, exit wins (conservative)
        entries = entries & (~exits)
        
        return {
            "entries": entries.tolist(),
            "exits": exits.tolist()
        }
    except Exception as e:
        logger.error(f"Strategy execution failed: {e}")
        logger.debug(f"Failed code snippet:\n{state.get('strategy_code', '')}")
        return {"error": str(e), "rejection_reasons": ["EXECUTION_ERROR"]}

async def run_vectorbt_backtest_node(state: BacktesterState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        ticker = state["ticker"]
        price_df = state["price_data"][ticker]
        # Reconstruct Series
        entries = pd.Series(state["entries"], index=price_df.index)
        exits = pd.Series(state["exits"], index=price_df.index)
        
        # Run VectorBT portfolio simulation
        portfolio = vbt.Portfolio.from_signals(
            close=price_df['close'],
            entries=entries,
            exits=exits,
            init_cash=100_000,
            fees=0.001,       # 10 bps
            slippage=0.001,   # 10 bps
            freq='1D'
        )
        
        # Run SPY benchmark simulation (buy and hold) if available
        if "SPY" in state["price_data"]:
            spy_df = state["price_data"]["SPY"]
            benchmark_entries = pd.Series(False, index=spy_df.index)
            benchmark_entries.iloc[0] = True # Buy on first day
            
            benchmark = vbt.Portfolio.from_signals(
                close=spy_df['close'],
                entries=benchmark_entries,
                init_cash=100_000,
                freq='1D'
            )
            return {"portfolio": portfolio, "benchmark_metrics": {"portfolio": benchmark}}
            
        return {"portfolio": portfolio, "benchmark_metrics": {"portfolio": None}}
    except Exception as e:
        logger.error(f"VectorBT backtest failed: {e}")
        return {"error": str(e), "rejection_reasons": ["BACKTEST_ERROR"]}

def extract_trade_log(portfolio: Any) -> list[dict[str, Any]]:
    try:
        trades_df = portfolio.trades.records_readable
    except Exception:
        return []
        
    log = []
    
    # Map common column names
    col_map = {
        'Entry Timestamp': ['Entry Timestamp', 'Entry Date', 'entry_date'],
        'Exit Timestamp': ['Exit Timestamp', 'Exit Date', 'exit_date'],
        'Entry Price': ['Entry Price', 'Avg Entry Price', 'entry_price'],
        'Exit Price': ['Exit Price', 'Avg Exit Price', 'exit_price'],
        'Return': ['Return', 'PnL %', 'return'],
        'Duration': ['Duration', 'holding_period', 'duration'],
        'Direction': ['Direction', 'Side', 'direction']
    }
    
    def get_col(row, key):
        for col in col_map[key]:
            if col in row:
                return row[col]
        return None

    # Sort and cap at 500 trades to avoid DB blob overflow
    for _, row in trades_df.tail(500).iterrows():
        entry_ts = get_col(row, 'Entry Timestamp')
        exit_ts = get_col(row, 'Exit Timestamp')
        
        entry_date = entry_ts.isoformat() if hasattr(entry_ts, 'isoformat') else str(entry_ts)
        exit_date = exit_ts.isoformat() if hasattr(exit_ts, 'isoformat') else str(exit_ts)
        
        duration = get_col(row, 'Duration')
        holding_days = 0
        if hasattr(duration, 'total_seconds'):
            holding_days = float(duration.total_seconds() / 86400)
        elif isinstance(duration, (int, float)):
            holding_days = float(duration)
            
        log.append({
            "entry_date": entry_date,
            "exit_date": exit_date,
            "entry_price": float(get_col(row, 'Entry Price') or 0),
            "exit_price": float(get_col(row, 'Exit Price') or 0),
            "return_pct": float(get_col(row, 'Return') or 0) * 100,
            "holding_days": holding_days,
            "direction": str(get_col(row, 'Direction') or "long").lower()
        })
    return log

async def compute_metrics_node(state: BacktesterState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    try:
        portfolio = state["portfolio"]
        benchmark = state["benchmark_metrics"].get("portfolio")
        
        # VectorBT metrics can raise if no trades occurred.
        # So we wrap some stats
        try:
            total_trades = portfolio.trades.count()
            if total_trades > 0:
                win_rate = portfolio.trades.win_rate()
                profit_factor = portfolio.trades.profit_factor()
                avg_trade_ret = portfolio.trades.returns.mean() * 100
                avg_hold = portfolio.trades.duration.mean().total_seconds() / 86400
                best_trade = portfolio.trades.returns.max() * 100
                worst_trade = portfolio.trades.returns.min() * 100
            else:
                win_rate = 0.0
                profit_factor = 0.0
                avg_trade_ret = 0.0
                avg_hold = 0.0
                best_trade = 0.0
                worst_trade = 0.0
        except Exception:
            total_trades = 0
            win_rate = 0.0
            profit_factor = 0.0
            avg_trade_ret = 0.0
            avg_hold = 0.0
            best_trade = 0.0
            worst_trade = 0.0

        # Max drawdown duration parsing
        try:
            mdd_dur = portfolio.max_drawdown_duration().total_seconds() / 86400
        except Exception:
            mdd_dur = 0
            
        benchmark_return_pct = 0.0
        alpha = 0.0
        if benchmark is not None:
            benchmark_return_pct = float(benchmark.total_return()) * 100
            alpha = float(portfolio.sharpe_ratio() - benchmark.sharpe_ratio())
            
        metrics = {
            "total_return_pct": float(portfolio.total_return()) * 100,
            "annualized_return_pct": float(portfolio.annualized_return()) * 100,
            "sharpe_ratio": float(portfolio.sharpe_ratio()),
            "sortino_ratio": float(portfolio.sortino_ratio()),
            "calmar_ratio": float(portfolio.calmar_ratio()),
            "max_drawdown_pct": float(portfolio.max_drawdown()) * 100,
            "max_drawdown_duration_days": int(mdd_dur),
            "win_rate": float(win_rate),
            "profit_factor": float(profit_factor),
            "total_trades": int(total_trades),
            "avg_trade_return_pct": float(avg_trade_ret),
            "avg_holding_days": float(avg_hold),
            "best_trade_pct": float(best_trade),
            "worst_trade_pct": float(worst_trade),
            "volatility_annualized": float(portfolio.annualized_volatility()) * 100,
            "benchmark_return_pct": benchmark_return_pct,
            "alpha": alpha,
            "beta": 1.0, # Approximate or compute proper covariance later
            "equity_curve": [{"date": str(idx.date()), "value": float(v)} for idx, v in portfolio.value().items()],
            "monthly_returns": {str(idx.date()): float(v * 100) for idx, v in portfolio.returns().resample('ME').sum().items()},
            "trade_log": extract_trade_log(portfolio)
        }
        
        # Cleanup np.inf / np.nan in JSON outputs
        for k, v in metrics.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                metrics[k] = 0.0
        
        return {"metrics": metrics}
    except Exception as e:
        logger.error(f"Metrics computation failed: {e}")
        return {"error": str(e), "rejection_reasons": ["METRICS_ERROR"]}

async def apply_quality_filters_node(state: BacktesterState) -> dict[str, Any]:
    if state.get("error"):
        return {"passed_filters": False}
        
    metrics = state["metrics"]
    reasons = state.get("rejection_reasons", [])
    
    if metrics["sharpe_ratio"] < 0.8:
        reasons.append("LOW_SHARPE")
    if metrics["total_return_pct"] <= 0:
        reasons.append("NEGATIVE_RETURN")
    if metrics["max_drawdown_pct"] < -25:
        reasons.append("EXCESSIVE_DRAWDOWN")
    if metrics["total_trades"] < 20:
        reasons.append("INSUFFICIENT_TRADES")
    if metrics["win_rate"] < 0.4:
        reasons.append("LOW_WIN_RATE")
    if metrics["total_return_pct"] <= metrics["benchmark_return_pct"]:
        reasons.append("UNDERPERFORMS_BENCHMARK")
    if metrics["profit_factor"] < 1.2:
        reasons.append("LOW_PROFIT_FACTOR")
        
    passed = len(reasons) == 0
    return {"passed_filters": passed, "rejection_reasons": reasons}

async def store_results_node(state: BacktesterState) -> dict[str, Any]:
    signal_id = state["signal"]["id"]
    ticker = state["ticker"]
    passed = state.get("passed_filters", False)
    
    # Database persistence
    try:
        engine = create_engine(settings.postgres_url)
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        
        # 1. Update TradingSignal status
        signal_record = session.query(TradingSignal).filter(TradingSignal.id == signal_id).first()
        if signal_record:
            signal_record.status = "validated" if passed else "rejected"
            
        # 2. Insert BacktestResult
        if not state.get("error"):
            metrics = state["metrics"]
            
            # Use real start/end dates from equity curve
            eq_curve = metrics.get("equity_curve", [])
            if eq_curve:
                start_dt = datetime.strptime(eq_curve[0]["date"], "%Y-%m-%d").date()
                end_dt = datetime.strptime(eq_curve[-1]["date"], "%Y-%m-%d").date()
            else:
                start_dt = date.today()
                end_dt = date.today()
            
            bt_record = BacktestResult(
                id=uuid.uuid4(),
                signal_id=signal_id,
                ticker=ticker,
                start_date=start_dt,
                end_date=end_dt,
                initial_capital=100_000.0,
                final_capital=float(metrics.get("equity_curve", [{"value": 100000}])[-1]["value"]),
                total_return_pct=float(metrics.get("total_return_pct", 0.0)),
                annualized_return_pct=float(metrics.get("annualized_return_pct", 0.0)),
                sharpe_ratio=float(metrics.get("sharpe_ratio", 0.0)),
                sortino_ratio=float(metrics.get("sortino_ratio", 0.0)),
                calmar_ratio=float(metrics.get("calmar_ratio", 0.0)),
                max_drawdown_pct=float(metrics.get("max_drawdown_pct", 0.0)),
                max_drawdown_duration_days=int(metrics.get("max_drawdown_duration_days", 0)),
                win_rate=float(metrics.get("win_rate", 0.0)),
                profit_factor=float(metrics.get("profit_factor", 0.0)),
                total_trades=int(metrics.get("total_trades", 0)),
                avg_trade_return_pct=float(metrics.get("avg_trade_return_pct", 0.0)),
                avg_holding_days=float(metrics.get("avg_holding_days", 0.0)),
                best_trade_pct=float(metrics.get("best_trade_pct", 0.0)),
                worst_trade_pct=float(metrics.get("worst_trade_pct", 0.0)),
                volatility_annualized=float(metrics.get("volatility_annualized", 0.0)),
                benchmark_return_pct=float(metrics.get("benchmark_return_pct", 0.0)),
                alpha=float(metrics.get("alpha", 0.0)),
                beta=float(metrics.get("beta", 1.0)),
                equity_curve=metrics.get("equity_curve", []),
                monthly_returns=metrics.get("monthly_returns", {}),
                trade_log=metrics.get("trade_log", []),
                engine="vectorbt",
                backtested_at=datetime.now(timezone.utc)
            )
            session.add(bt_record)
            
        session.commit()
        session.close()
        engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to store backtest results for {ticker}: {e}")
        state["error"] = str(e)
        return state

    # Redis persistence
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        # Cache top metrics
        if not state.get("error"):
            cache_payload = {
                "sharpe": state["metrics"]["sharpe_ratio"],
                "return": state["metrics"]["total_return_pct"],
                "drawdown": state["metrics"]["max_drawdown_pct"],
                "passed": passed
            }
            r.setex(f"signal:backtest:{signal_id}", 3600, json.dumps(cache_payload))
            
            # Pub/Sub
            r.publish("signals.backtest.completed", json.dumps({
                "signal_id": str(signal_id),
                "ticker": ticker,
                "sharpe": state["metrics"]["sharpe_ratio"],
                "passed": passed
            }))
        r.close()
    except Exception as e:
        logger.warning(f"Failed Redis operations for backtest {signal_id}: {e}")

    logger.info(f"Backtest completed for {ticker}. Passed: {passed}. Rejections: {state.get('rejection_reasons', [])}")
    return state


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def route_errors(state: BacktesterState) -> str:
    if state.get("error"):
        return "store_results_node"
    return "prepare_data_node"

def route_run_errors(state: BacktesterState) -> str:
    if state.get("error"):
        return "store_results_node"
    return "run_vectorbt_backtest_node"

def route_bt_errors(state: BacktesterState) -> str:
    if state.get("error"):
        return "store_results_node"
    return "compute_metrics_node"

def route_metrics_errors(state: BacktesterState) -> str:
    if state.get("error"):
        return "store_results_node"
    return "apply_quality_filters_node"

def build_backtester_graph() -> StateGraph:
    graph = StateGraph(BacktesterState)
    
    graph.add_node("fetch", fetch_price_data_node)
    graph.add_node("prepare_data_node", prepare_data_node)
    graph.add_node("run_strategy_node", run_strategy_node)
    graph.add_node("run_vectorbt_backtest_node", run_vectorbt_backtest_node)
    graph.add_node("compute_metrics_node", compute_metrics_node)
    graph.add_node("apply_quality_filters_node", apply_quality_filters_node)
    graph.add_node("store_results_node", store_results_node)
    
    graph.set_entry_point("fetch")
    
    # Conditional error routing
    graph.add_conditional_edges("fetch", route_errors)
    graph.add_edge("prepare_data_node", "run_strategy_node")
    graph.add_conditional_edges("run_strategy_node", route_run_errors)
    graph.add_conditional_edges("run_vectorbt_backtest_node", route_bt_errors)
    graph.add_conditional_edges("compute_metrics_node", route_metrics_errors)
    graph.add_edge("apply_quality_filters_node", "store_results_node")
    graph.add_edge("store_results_node", END)
    
    return graph

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PUBLIC INTERFACE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class Backtester:
    def __init__(self):
        self._graph = build_backtester_graph().compile()
        logger.info("Backtester agent initialised")
        
    async def backtest(self, signal: dict) -> dict:
        """
        Takes a dict representation of a TradingSignal, executes the 
        code against history, and returns the BacktestResult dict.
        """
        initial_state: BacktesterState = {
            "signal": signal,
            "ticker": signal.get("ticker", ""),
            "price_data": {},
            "strategy_code": signal.get("strategy_code", ""),
            "parameters": signal.get("parameters", {}),
            "entries": [],
            "exits": [],
            "portfolio": None,
            "metrics": {},
            "benchmark_metrics": {},
            "passed_filters": False,
            "rejection_reasons": [],
            "error": None
        }
        
        final_state = await self._graph.ainvoke(initial_state)
        return final_state
        
    async def backtest_batch(self, signals: list[dict]) -> list[dict]:
        tasks = [self.backtest(s) for s in signals]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Batch backtest error: {r}")
            else:
                valid.append(r)
        return valid
        
    def get_equity_curve(self, signal_id: uuid.UUID) -> pd.Series:
        engine = create_engine(settings.postgres_url)
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        res = session.query(BacktestResult).filter(BacktestResult.signal_id == signal_id).first()
        session.close()
        engine.dispose()
        
        if res and res.equity_curve:
            df = pd.DataFrame(res.equity_curve)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                return df['value']
        return pd.Series()
        
    def compare_signals(self, signal_ids: list[uuid.UUID]) -> pd.DataFrame:
        engine = create_engine(settings.postgres_url)
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        
        metrics_list = []
        for sid in signal_ids:
            res = session.query(BacktestResult).filter(BacktestResult.signal_id == sid).first()
            if res:
                metrics_list.append({
                    "signal_id": sid,
                    "ticker": res.ticker,
                    "sharpe": res.sharpe_ratio,
                    "return_pct": res.total_return_pct,
                    "win_rate": res.win_rate,
                    "drawdown": res.max_drawdown_pct
                })
                
        session.close()
        engine.dispose()
        return pd.DataFrame(metrics_list)
