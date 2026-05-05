from typing import Optional, Dict, Any
from datetime import datetime, date
from uuid import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Float, Integer, Boolean, DateTime, Date, ForeignKey, Text, JSON
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

class Base(DeclarativeBase):
    pass

class AuditLog(Base):
    __tablename__ = "audit_log"
    
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(50), index=True)
    entity_type: Mapped[str] = mapped_column(String(50))
    entity_id: Mapped[Optional[UUID]] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    ticker: Mapped[Optional[str]] = mapped_column(String(10), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(255))
    actor: Mapped[str] = mapped_column(String(100))
    details: Mapped[Dict[str, Any]] = mapped_column(JSON)
    previous_state: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    new_state: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    ip_address: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    session_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    immutable_hash: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class ComplianceRule(Base):
    __tablename__ = "compliance_rules"
    
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    rule_id: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    rule_name: Mapped[str] = mapped_column(String(255))
    rule_category: Mapped[str] = mapped_column(String(50), index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    rule_logic: Mapped[Dict[str, Any]] = mapped_column(JSON)
    severity: Mapped[str] = mapped_column(String(20))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    auto_action: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class ComplianceCheck(Base):
    __tablename__ = "compliance_checks"
    
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    rule_id: Mapped[str] = mapped_column(String(100), ForeignKey("compliance_rules.rule_id"))
    entity_type: Mapped[str] = mapped_column(String(50))
    entity_id: Mapped[Optional[UUID]] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    ticker: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    check_result: Mapped[str] = mapped_column(String(20))
    current_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    threshold_value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    auto_action_taken: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class RuleViolation(Base):
    __tablename__ = "rule_violations"
    
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    compliance_check_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("compliance_checks.id"))
    rule_id: Mapped[str] = mapped_column(String(100))
    violation_type: Mapped[str] = mapped_column(String(50))
    ticker: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    severity: Mapped[str] = mapped_column(String(20))
    description: Mapped[str] = mapped_column(Text)
    current_value: Mapped[float] = mapped_column(Float)
    allowed_value: Mapped[float] = mapped_column(Float)
    excess: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(20), default="open")
    resolution: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    waived_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    waived_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class RestrictedList(Base):
    __tablename__ = "restricted_list"
    
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    restriction_type: Mapped[str] = mapped_column(String(50))
    reason: Mapped[str] = mapped_column(String(255))
    added_by: Mapped[str] = mapped_column(String(100))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    removed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

class DailyReport(Base):
    __tablename__ = "daily_reports"
    
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    report_date: Mapped[date] = mapped_column(Date, index=True)
    report_type: Mapped[str] = mapped_column(String(50))
    portfolio_value: Mapped[float] = mapped_column(Float)
    daily_pnl: Mapped[float] = mapped_column(Float)
    daily_pnl_pct: Mapped[float] = mapped_column(Float)
    total_trades: Mapped[int] = mapped_column(Integer)
    total_traded_value: Mapped[float] = mapped_column(Float)
    violations_count: Mapped[int] = mapped_column(Integer)
    warnings_count: Mapped[int] = mapped_column(Integer)
    report_data: Mapped[Dict[str, Any]] = mapped_column(JSON)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    delivered: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class WashSaleTracker(Base):
    __tablename__ = "wash_sale_tracker"
    
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    sold_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    sold_price: Mapped[float] = mapped_column(Float)
    sold_shares: Mapped[int] = mapped_column(Integer)
    loss_amount: Mapped[float] = mapped_column(Float)
    wash_sale_window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    replacement_purchase: Mapped[bool] = mapped_column(Boolean, default=False)
    replacement_purchase_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    disallowed_loss: Mapped[float] = mapped_column(Float, default=0)
    status: Mapped[str] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class PatternDayTradeTracker(Base):
    __tablename__ = "pattern_day_trade_tracker"
    
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    account_id: Mapped[str] = mapped_column(String(100))
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    ticker: Mapped[str] = mapped_column(String(10))
    buy_order_id: Mapped[Optional[UUID]] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    sell_order_id: Mapped[Optional[UUID]] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    is_day_trade: Mapped[bool] = mapped_column(Boolean, default=False)
    rolling_5day_count: Mapped[int] = mapped_column(Integer, default=0)
    pdt_limit_reached: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
