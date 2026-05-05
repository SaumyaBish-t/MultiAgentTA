"""
Phase 8: Monitoring & Feedback Loop Models
==========================================
SQLAlchemy ORM models for the monitoring system.
"""

import uuid
from datetime import datetime, date
from typing import Optional, Dict, Any
from sqlalchemy import String, Float, Integer, Boolean, DateTime, Date, JSON, Text, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID

class Base(DeclarativeBase):
    pass

class SystemHealthSnapshot(Base):
    __tablename__ = "system_health_snapshots"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    overall_status: Mapped[str] = mapped_column(String(20), nullable=False)
    phase_statuses: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    db_health: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    api_health: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    llm_health: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    last_data_ingestion: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_research_run: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_signal_generation: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_risk_evaluation: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_execution: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    active_positions: Mapped[int] = mapped_column(Integer, default=0)
    portfolio_value: Mapped[float] = mapped_column(Float, default=0.0)
    current_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    alert_level: Mapped[str] = mapped_column(String(20), default="NORMAL")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class PerformanceMetrics(Base):
    __tablename__ = "performance_metrics"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    metric_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    metric_type: Mapped[str] = mapped_column(String(20), nullable=False)
    portfolio_value: Mapped[float] = mapped_column(Float, nullable=False)
    total_return: Mapped[float] = mapped_column(Float, nullable=False)
    annualized_return: Mapped[float] = mapped_column(Float, nullable=False)
    sharpe_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    sortino_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    calmar_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    max_drawdown: Mapped[float] = mapped_column(Float, nullable=False)
    volatility: Mapped[float] = mapped_column(Float, nullable=False)
    beta_to_spy: Mapped[float] = mapped_column(Float, nullable=False)
    alpha: Mapped[float] = mapped_column(Float, nullable=False)
    information_ratio: Mapped[float] = mapped_column(Float, nullable=False)
    benchmark_return: Mapped[float] = mapped_column(Float, nullable=False)
    excess_return: Mapped[float] = mapped_column(Float, nullable=False)
    win_days: Mapped[int] = mapped_column(Integer, nullable=False)
    loss_days: Mapped[int] = mapped_column(Integer, nullable=False)
    win_day_rate: Mapped[float] = mapped_column(Float, nullable=False)
    avg_win_day: Mapped[float] = mapped_column(Float, nullable=False)
    avg_loss_day: Mapped[float] = mapped_column(Float, nullable=False)
    best_day: Mapped[float] = mapped_column(Float, nullable=False)
    worst_day: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class PnLAttribution(Base):
    __tablename__ = "pnl_attribution"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    attribution_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    signal_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    strategy_type: Mapped[str] = mapped_column(String(50), nullable=False)
    contribution_pct: Mapped[float] = mapped_column(Float, nullable=False)
    position_return_pct: Mapped[float] = mapped_column(Float, nullable=False)
    position_weight: Mapped[float] = mapped_column(Float, nullable=False)
    alpha_contribution: Mapped[float] = mapped_column(Float, nullable=False)
    holding_days: Mapped[int] = mapped_column(Integer, nullable=False)
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    exit_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class SignalLivePerformance(Base):
    __tablename__ = "signal_live_performance"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    tracking_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    predicted_direction: Mapped[str] = mapped_column(String(10), nullable=False)
    actual_direction: Mapped[str] = mapped_column(String(10), nullable=False)
    predicted_return: Mapped[float] = mapped_column(Float, nullable=False)
    actual_return: Mapped[float] = mapped_column(Float, nullable=False)
    hit: Mapped[bool] = mapped_column(Boolean, nullable=False)
    rolling_hit_rate_20: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rolling_hit_rate_60: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    signal_strength: Mapped[float] = mapped_column(Float, nullable=False)
    decay_detected: Mapped[bool] = mapped_column(Boolean, default=False)
    decay_severity: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class RegimeDetection(Base):
    __tablename__ = "regime_detections"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    detection_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    regime: Mapped[str] = mapped_column(String(50), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    duration_days: Mapped[int] = mapped_column(Integer, nullable=False)
    indicators_used: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    previous_regime: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    regime_change: Mapped[bool] = mapped_column(Boolean, default=False)
    implications: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class Alert(Base):
    __tablename__ = "alerts"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    alert_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    channel: Mapped[str] = mapped_column(String(50), nullable=False)
    acknowledged: Mapped[bool] = mapped_column(Boolean, default=False)
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    auto_resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class FeedbackAction(Base):
    __tablename__ = "feedback_actions"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trigger_type: Mapped[str] = mapped_column(String(50), nullable=False)
    trigger_details: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    action_type: Mapped[str] = mapped_column(String(50), nullable=False)
    action_details: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    target_phase: Mapped[str] = mapped_column(String(20), nullable=False)
    target_agent: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    applied_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class RetrainingTrigger(Base):
    __tablename__ = "retraining_triggers"
    
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trigger_date: Mapped[date] = mapped_column(Date, nullable=False)
    trigger_reason: Mapped[str] = mapped_column(String(255), nullable=False)
    affected_signals: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    affected_tickers: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    metrics_at_trigger: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    retrain_type: Mapped[str] = mapped_column(String(20), nullable=False)
    retrain_status: Mapped[str] = mapped_column(String(20), nullable=False)
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
