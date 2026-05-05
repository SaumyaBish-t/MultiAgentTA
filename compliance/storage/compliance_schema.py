from sqlalchemy import (
    Table, Column, String, Float, Integer, Boolean, DateTime, Date, ForeignKey, Index, Text, MetaData, JSON
)
from sqlalchemy.dialects.postgresql import UUID
import uuid

metadata = MetaData()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 1: audit_log
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
audit_log = Table(
    "audit_log",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("event_type", String(50), nullable=False, index=True), 
    # enum: order_submitted/order_filled/order_cancelled/position_opened/position_closed/signal_approved/signal_rejected/risk_breach/circuit_breaker/rebalance/hypothesis_generated/backtest_completed/parameter_changed/system_startup/system_shutdown/compliance_check/rule_violation/alert_sent/human_override/emergency_action
    Column("entity_type", String(50), nullable=False), # order/position/signal/portfolio/risk/system
    Column("entity_id", UUID(as_uuid=True), nullable=True),
    Column("ticker", String(10), nullable=True, index=True),
    Column("action", String(255), nullable=False),
    Column("actor", String(100), nullable=False), # which agent/pipeline did this
    Column("details", JSON, nullable=False),
    Column("previous_state", JSON, nullable=True),
    Column("new_state", JSON, nullable=True),
    Column("ip_address", String(45), nullable=True),
    Column("session_id", String(100), nullable=True),
    Column("immutable_hash", String(64), nullable=False), # SHA256 of all fields
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 2: compliance_rules
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
compliance_rules = Table(
    "compliance_rules",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("rule_id", String(100), unique=True, nullable=False, index=True),
    Column("rule_name", String(255), nullable=False),
    Column("rule_category", String(50), nullable=False, index=True), # position_limit/concentration/trading_restriction/reporting/risk_limit/wash_sale/pattern_day_trading/leverage
    Column("description", Text, nullable=True),
    Column("rule_logic", JSON, nullable=False), # thresholds and conditions
    Column("severity", String(20), nullable=False), # info/warning/violation/critical
    Column("enabled", Boolean, default=True, nullable=False),
    Column("auto_action", String(100), nullable=True), # what to do on violation
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 3: compliance_checks
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
compliance_checks = Table(
    "compliance_checks",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("rule_id", String(100), ForeignKey("compliance_rules.rule_id"), nullable=False),
    Column("entity_type", String(50), nullable=False),
    Column("entity_id", UUID(as_uuid=True), nullable=True),
    Column("ticker", String(10), nullable=True),
    Column("check_result", String(20), nullable=False), # pass/warning/violation
    Column("current_value", Float, nullable=True),
    Column("threshold_value", Float, nullable=True),
    Column("details", Text, nullable=True),
    Column("auto_action_taken", String(100), nullable=True),
    Column("checked_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 4: rule_violations
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
rule_violations = Table(
    "rule_violations",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("compliance_check_id", UUID(as_uuid=True), ForeignKey("compliance_checks.id"), nullable=False),
    Column("rule_id", String(100), nullable=False),
    Column("violation_type", String(50), nullable=False),
    Column("ticker", String(10), nullable=True),
    Column("severity", String(20), nullable=False),
    Column("description", Text, nullable=False),
    Column("current_value", Float, nullable=False),
    Column("allowed_value", Float, nullable=False),
    Column("excess", Float, nullable=False), # how much over limit
    Column("status", String(20), nullable=False, default="open"), # open/acknowledged/resolved/waived
    Column("resolution", Text, nullable=True),
    Column("resolved_by", String(100), nullable=True),
    Column("resolved_at", DateTime(timezone=True), nullable=True),
    Column("waived_by", String(100), nullable=True),
    Column("waived_reason", String(255), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 5: restricted_list
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
restricted_list = Table(
    "restricted_list",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("ticker", String(10), nullable=False, index=True),
    Column("restriction_type", String(50), nullable=False), # no_trade/no_buy/no_sell/reduce_only/enhanced_monitoring
    Column("reason", String(255), nullable=False),
    Column("added_by", String(100), nullable=False), # system/human
    Column("active", Boolean, default=True, nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("removed_at", DateTime(timezone=True), nullable=True),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 6: daily_reports
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
daily_reports = Table(
    "daily_reports",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("report_date", Date, nullable=False, index=True),
    Column("report_type", String(50), nullable=False), # daily_pnl/risk_summary/compliance_summary/execution_report/full_portfolio_report
    Column("portfolio_value", Float, nullable=False),
    Column("daily_pnl", Float, nullable=False),
    Column("daily_pnl_pct", Float, nullable=False),
    Column("total_trades", Integer, nullable=False),
    Column("total_traded_value", Float, nullable=False),
    Column("violations_count", Integer, nullable=False),
    Column("warnings_count", Integer, nullable=False),
    Column("report_data", JSON, nullable=False), # full report content
    Column("generated_at", DateTime(timezone=True), nullable=False),
    Column("delivered", Boolean, default=False, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 7: wash_sale_tracker
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
wash_sale_tracker = Table(
    "wash_sale_tracker",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("ticker", String(10), nullable=False, index=True),
    Column("sold_at", DateTime(timezone=True), nullable=False),
    Column("sold_price", Float, nullable=False),
    Column("sold_shares", Integer, nullable=False),
    Column("loss_amount", Float, nullable=False),
    Column("wash_sale_window_end", DateTime(timezone=True), nullable=False), # 30 days after sale
    Column("replacement_purchase", Boolean, default=False, nullable=False),
    Column("replacement_purchase_at", DateTime(timezone=True), nullable=True),
    Column("disallowed_loss", Float, default=0, nullable=False),
    Column("status", String(20), nullable=False), # monitoring/triggered/expired
    Column("created_at", DateTime(timezone=True), nullable=False),
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TABLE 8: pattern_day_trade_tracker
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
pattern_day_trade_tracker = Table(
    "pattern_day_trade_tracker",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("account_id", String(100), nullable=False),
    Column("trade_date", Date, nullable=False, index=True),
    Column("ticker", String(10), nullable=False),
    Column("buy_order_id", UUID(as_uuid=True), nullable=True),
    Column("sell_order_id", UUID(as_uuid=True), nullable=True),
    Column("is_day_trade", Boolean, default=False),
    Column("rolling_5day_count", Integer, default=0),
    Column("pdt_limit_reached", Boolean, default=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
