"""
signal_generation.tracking.mlflow_tracker
=========================================

Lightweight wrapper around MLflow so the Backtester (and anything else
that runs an experiment) can log params + metrics with a single call,
*and* cross-reference the MLflow run in our PostgreSQL
``strategy_experiments`` table so it's queryable from the same JOINs as
``trading_signals`` / ``backtest_results``.

Optional dependency — if ``mlflow`` isn't installed, the wrapper turns
into a no-op so the rest of the pipeline keeps working.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg2
from loguru import logger

from config.settings import settings

try:
    import mlflow  # type: ignore[import-untyped]
except Exception:  # pragma: no cover
    mlflow = None  # type: ignore[assignment]


def _ensure_tracking_uri() -> None:
    if mlflow is None:
        return
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)


def log_backtest(
    *,
    signal_id: str | None,
    ticker: str,
    strategy_type: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    experiment_name: str = "forge-backtests",
) -> dict[str, str | None]:
    """Log one backtest to MLflow and cross-reference it in PostgreSQL.

    Safe to call when ``mlflow`` is missing — returns ``{"skipped": ...}``.
    """
    if mlflow is None:
        logger.debug("mlflow not installed — log_backtest is a no-op")
        return {"skipped": "mlflow_not_installed"}

    _ensure_tracking_uri()
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run() as run:
        mlflow.log_params(params)
        for k, v in metrics.items():
            if v is not None:
                mlflow.log_metric(k, float(v))
        mlflow.set_tag("ticker", ticker)
        mlflow.set_tag("strategy_type", strategy_type)
        run_id = run.info.run_id
        experiment_id = run.info.experiment_id

    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategy_experiments
                    (signal_id, ticker, mlflow_run_id, mlflow_experiment_id,
                     strategy_type, params, sharpe, total_return, max_drawdown)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    signal_id, ticker, run_id, experiment_id,
                    strategy_type, json.dumps(params),
                    metrics.get("sharpe"),
                    metrics.get("total_return"),
                    metrics.get("max_drawdown"),
                ),
            )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("strategy_experiments insert failed: {}", exc)

    return {"run_id": run_id, "experiment_id": experiment_id}
