import uuid
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from loguru import logger

from config.settings import settings
from compliance.storage.compliance_schema import metadata

def init_compliance_db():
    """Initializes the Phase 7 Compliance & Audit database tables and seeds default rules."""
    logger.info(f"Initializing Compliance database at {settings.postgres_url.split('@')[-1]}")
    
    engine = create_engine(settings.postgres_url)
    
    try:
        with engine.begin() as conn:
            # Create tables
            logger.info("Creating compliance tables...")
            metadata.create_all(conn)
            
            # Seed default compliance rules
            logger.info("Seeding default compliance rules...")
            
            rules = [
                {
                    "id": uuid.uuid4(),
                    "rule_id": "MAX_POSITION_SIZE_5PCT",
                    "rule_name": "Maximum Position Size 5%",
                    "rule_category": "position_limit",
                    "description": "No single position can exceed 5% of total portfolio value.",
                    "rule_logic": {"max_pct": 0.05},
                    "severity": "violation",
                    "auto_action": "reduce_position",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "MAX_SECTOR_30PCT",
                    "rule_name": "Maximum Sector Concentration 30%",
                    "rule_category": "concentration",
                    "description": "Total exposure to any single sector cannot exceed 30%.",
                    "rule_logic": {"max_sector_pct": 0.30},
                    "severity": "violation",
                    "auto_action": "alert_only",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "MAX_PORTFOLIO_DRAWDOWN_10PCT",
                    "rule_name": "Maximum Portfolio Drawdown 10%",
                    "rule_category": "risk_limit",
                    "description": "Trading halts if portfolio drawdown exceeds 10% from peak.",
                    "rule_logic": {"max_drawdown": -0.10},
                    "severity": "critical",
                    "auto_action": "halt_trading",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "PDT_WARNING_3TRADES",
                    "rule_name": "PDT Warning: 3 Day Trades",
                    "rule_category": "pattern_day_trading",
                    "description": "Warn when account reaches 3 day trades in 5 business days.",
                    "rule_logic": {"max_day_trades": 3, "window_days": 5},
                    "severity": "warning",
                    "auto_action": "alert_only",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "WASH_SALE_30DAY",
                    "rule_name": "Wash Sale 30-Day Window",
                    "rule_category": "wash_sale",
                    "description": "Flag purchases of the same security within 30 days of a loss sale.",
                    "rule_logic": {"window_days": 30},
                    "severity": "warning",
                    "auto_action": "flag_and_alert",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "MAX_DAILY_LOSS_3PCT",
                    "rule_name": "Maximum Daily Loss 3%",
                    "rule_category": "risk_limit",
                    "description": "Trading halts if daily loss exceeds 3% of starting equity.",
                    "rule_logic": {"max_daily_loss": -0.03},
                    "severity": "critical",
                    "auto_action": "halt_trading",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "NO_LEVERAGE",
                    "rule_name": "No Leverage Constraint",
                    "rule_category": "leverage",
                    "description": "Total gross exposure cannot exceed 1.0x (100% of equity).",
                    "rule_logic": {"max_leverage": 1.0},
                    "severity": "critical",
                    "auto_action": "reject_order",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "MIN_CASH_5PCT",
                    "rule_name": "Minimum Cash Reserve 5%",
                    "rule_category": "position_limit",
                    "description": "Maintain at least 5% of portfolio in cash.",
                    "rule_logic": {"min_cash_pct": 0.05},
                    "severity": "warning",
                    "auto_action": "alert_only",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "MAX_GROSS_EXPOSURE_95PCT",
                    "rule_name": "Maximum Gross Exposure 95%",
                    "rule_category": "risk_limit",
                    "description": "Block new trades if gross exposure exceeds 95% of buying power.",
                    "rule_logic": {"max_gross_exposure": 0.95},
                    "severity": "violation",
                    "auto_action": "block_new_trades",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "RESTRICTED_LIST_CHECK",
                    "rule_name": "Restricted List Trading Block",
                    "rule_category": "trading_restriction",
                    "description": "Reject orders for tickers currently on the restricted list.",
                    "rule_logic": {},
                    "severity": "critical",
                    "auto_action": "reject_order",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "MARKET_HOURS_CHECK",
                    "rule_name": "Market Hours Check",
                    "rule_category": "trading_restriction",
                    "description": "Reject orders when market is closed.",
                    "rule_logic": {},
                    "severity": "critical",
                    "auto_action": "reject_order",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                },
                {
                    "id": uuid.uuid4(),
                    "rule_id": "DUPLICATE_ORDER_CHECK",
                    "rule_name": "Duplicate Order Check",
                    "rule_category": "trading_restriction",
                    "description": "Reject duplicate pending orders for same ticker and action.",
                    "rule_logic": {},
                    "severity": "violation",
                    "auto_action": "reject_order",
                    "enabled": True,
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc)
                }
            ]
            
            # Insert rules using UPSERT style (or just check existence)
            for rule in rules:
                conn.execute(
                    text("""
                        INSERT INTO compliance_rules 
                        (id, rule_id, rule_name, rule_category, description, rule_logic, severity, enabled, auto_action, created_at, updated_at)
                        VALUES (:id, :rule_id, :rule_name, :rule_category, :description, :rule_logic, :severity, :enabled, :auto_action, :created_at, :updated_at)
                        ON CONFLICT (rule_id) DO UPDATE SET
                            rule_name = EXCLUDED.rule_name,
                            description = EXCLUDED.description,
                            rule_logic = EXCLUDED.rule_logic,
                            severity = EXCLUDED.severity,
                            auto_action = EXCLUDED.auto_action,
                            updated_at = EXCLUDED.updated_at
                    """),
                    {
                        "id": str(rule["id"]),
                        "rule_id": rule["rule_id"],
                        "rule_name": rule["rule_name"],
                        "rule_category": rule["rule_category"],
                        "description": rule["description"],
                        "rule_logic": json_dumps(rule["rule_logic"]),
                        "severity": rule["severity"],
                        "enabled": rule["enabled"],
                        "auto_action": rule["auto_action"],
                        "created_at": rule["created_at"],
                        "updated_at": rule["updated_at"]
                    }
                )
            
            logger.success("Compliance database initialized successfully.")
            
    except Exception as e:
        logger.error(f"Failed to initialize compliance database: {e}")
        raise

def json_dumps(data):
    import json
    return json.dumps(data)

if __name__ == "__main__":
    init_compliance_db()
