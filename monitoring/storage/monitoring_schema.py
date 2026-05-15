"""
Phase 8: Monitoring & Feedback Loop Schema
==========================================
Database tables for tracking system health, performance metrics,
pnl attribution, signal quality, market regimes, and automated feedback.
"""

import uuid
from sqlalchemy import (
    Table, Column, String, Float, Integer, Boolean, 
    DateTime, Date, JSON, MetaData, ForeignKey, Text, Index
)
from sqlalchemy.dialects.postgresql import UUID

metadata = MetaData()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 1: system_health_snapshots
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
system_health_snapshots = Table(
    "system_health_snapshots",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("snapshot_time", DateTime(timezone=True), nullable=False, index=True),
    Column("overall_status", String(20), nullable=False), # healthy/degraded/critical/offline
    Column("phase_statuses", JSON, nullable=False),
    Column("db_health", JSON, nullable=False),
    Column("api_health", JSON, nullable=False),
    Column("llm_health", JSON, nullable=False),
    Column("last_data_ingestion", DateTime(timezone=True), nullable=True),
    Column("last_research_run", DateTime(timezone=True), nullable=True),
    Column("last_signal_generation", DateTime(timezone=True), nullable=True),
    Column("last_risk_evaluation", DateTime(timezone=True), nullable=True),
    Column("last_execution", DateTime(timezone=True), nullable=True),
    Column("active_positions", Integer, default=0),
    Column("portfolio_value", Float, default=0.0),
    Column("current_drawdown", Float, default=0.0),
    Column("alert_level", String(20), default="NORMAL"),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 2: performance_metrics
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
performance_metrics = Table(
    "performance_metrics",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("metric_date", Date, nullable=False, index=True),
    Column("metric_type", String(20), nullable=False), # daily/weekly/monthly/ytd/inception
    Column("portfolio_value", Float, nullable=False),
    Column("total_return", Float, nullable=False),
    Column("annualized_return", Float, nullable=False),
    Column("sharpe_ratio", Float, nullable=False),
    Column("sortino_ratio", Float, nullable=False),
    Column("calmar_ratio", Float, nullable=False),
    Column("max_drawdown", Float, nullable=False),
    Column("volatility", Float, nullable=False),
    Column("beta_to_spy", Float, nullable=False),
    Column("alpha", Float, nullable=False),
    Column("information_ratio", Float, nullable=False),
    Column("benchmark_return", Float, nullable=False),
    Column("excess_return", Float, nullable=False),
    Column("win_days", Integer, nullable=False),
    Column("loss_days", Integer, nullable=False),
    Column("win_day_rate", Float, nullable=False),
    Column("avg_win_day", Float, nullable=False),
    Column("avg_loss_day", Float, nullable=False),
    Column("best_day", Float, nullable=False),
    Column("worst_day", Float, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 3: pnl_attribution
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
pnl_attribution = Table(
    "pnl_attribution",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("attribution_date", Date, nullable=False, index=True),
    Column("ticker", String(20), nullable=False),
    Column("signal_id", UUID(as_uuid=True), nullable=True),
    Column("strategy_type", String(50), nullable=False),
    Column("contribution_pct", Float, nullable=False),
    Column("position_return_pct", Float, nullable=False),
    Column("position_weight", Float, nullable=False),
    Column("alpha_contribution", Float, nullable=False),
    Column("holding_days", Integer, nullable=False),
    Column("entry_date", Date, nullable=False),
    Column("exit_date", Date, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 4: signal_live_performance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
signal_live_performance = Table(
    "signal_live_performance",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("signal_id", UUID(as_uuid=True), nullable=False, index=True),
    Column("ticker", String(20), nullable=False),
    Column("tracking_date", Date, nullable=False, index=True),
    Column("predicted_direction", String(20), nullable=False),
    Column("actual_direction", String(20), nullable=False),
    Column("predicted_return", Float, nullable=False),
    Column("actual_return", Float, nullable=False),
    Column("hit", Boolean, nullable=False),
    Column("rolling_hit_rate_20", Float, nullable=True),
    Column("rolling_hit_rate_60", Float, nullable=True),
    Column("signal_strength", Float, nullable=False),
    Column("decay_detected", Boolean, default=False),
    Column("decay_severity", String(20), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 5: regime_detections
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
regime_detections = Table(
    "regime_detections",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("detection_date", Date, nullable=False, index=True),
    Column("regime", String(50), nullable=False), # bull/bear/sideways etc
    Column("confidence", Float, nullable=False),
    Column("duration_days", Integer, nullable=False),
    Column("indicators_used", JSON, nullable=False),
    Column("previous_regime", String(50), nullable=True),
    Column("regime_change", Boolean, default=False),
    Column("implications", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 6: alerts
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
alerts = Table(
    "alerts",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("alert_type", String(50), nullable=False, index=True),
    Column("severity", String(20), nullable=False), # info/warning/critical/emergency
    Column("title", String(255), nullable=False),
    Column("message", Text, nullable=False),
    Column("data", JSON, nullable=True),
    Column("channel", String(50), nullable=False), # redis/log/dashboard
    Column("acknowledged", Boolean, default=False),
    Column("acknowledged_at", DateTime(timezone=True), nullable=True),
    Column("acknowledged_by", String(100), nullable=True),
    Column("auto_resolved", Boolean, default=False),
    Column("resolved_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 7: feedback_actions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
feedback_actions = Table(
    "feedback_actions",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("trigger_type", String(50), nullable=False),
    Column("trigger_details", JSON, nullable=False),
    Column("action_type", String(50), nullable=False),
    Column("action_details", JSON, nullable=False),
    Column("target_phase", String(20), nullable=False),
    Column("target_agent", String(100), nullable=False),
    Column("status", String(20), nullable=False), # pending/applied/failed/rejected
    Column("applied_at", DateTime(timezone=True), nullable=True),
    Column("result", JSON, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 8: retraining_triggers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
retraining_triggers = Table(
    "retraining_triggers",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("trigger_date", Date, nullable=False),
    Column("trigger_reason", String(255), nullable=False),
    Column("affected_signals", JSON, nullable=True),
    Column("affected_tickers", JSON, nullable=True),
    Column("metrics_at_trigger", JSON, nullable=False),
    Column("retrain_type", String(20), nullable=False), # full/incremental/recalibrate
    Column("retrain_status", String(20), nullable=False), # pending/running/completed/failed
    Column("triggered_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True), nullable=True),
)
