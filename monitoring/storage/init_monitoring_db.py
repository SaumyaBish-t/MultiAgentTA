"""
Phase 8: Monitoring Database Initialization
==========================================
Creates tables and seeds initial health snapshots and performance metrics.
"""

import uuid
import json
from datetime import datetime, timezone, date
from sqlalchemy import create_engine, text
from loguru import logger
from config.settings import settings
from monitoring.storage.monitoring_schema import metadata

def init_monitoring_db():
    """Initializes the monitoring database tables."""
    logger.info(f"Initializing Monitoring database...")
    
    engine = create_engine(settings.postgres_url)
    
    try:
        # Create all tables defined in metadata
        logger.info("Creating monitoring tables...")
        metadata.create_all(engine)
        
        # Seed initial data
        with engine.begin() as conn:
            logger.info("Seeding initial system health snapshot...")
            now = datetime.now(timezone.utc)
            
            # 1. Initial health snapshot
            conn.execute(text("""
                INSERT INTO system_health_snapshots 
                (id, snapshot_time, overall_status, phase_statuses, db_health, api_health, llm_health, created_at)
                VALUES (:id, :snapshot_time, :overall_status, :phase_statuses, :db_health, :api_health, :llm_health, :created_at)
            """), {
                "id": uuid.uuid4(),
                "snapshot_time": now,
                "overall_status": "healthy",
                "phase_statuses": json.dumps({
                    "phase1_data": "healthy",
                    "phase2_research": "healthy",
                    "phase3_signals": "healthy",
                    "phase4_risk": "healthy",
                    "phase5_portfolio": "healthy",
                    "phase6_execution": "healthy",
                    "phase7_compliance": "healthy"
                }),
                "db_health": json.dumps({"timescaledb": "healthy", "postgresql": "healthy", "redis": "healthy", "chromadb": "healthy"}),
                "api_health": json.dumps({"fastapi": "healthy", "alpaca": "healthy", "polygon": "healthy", "fmp": "healthy", "fred": "healthy"}),
                "llm_health": json.dumps({"groq": "healthy", "cerebras": "healthy", "openrouter": "healthy", "mistral": "healthy"}),
                "created_at": now
            })
            
            # 2. Initial performance metrics (zeros)
            logger.info("Seeding initial performance metrics...")
            conn.execute(text("""
                INSERT INTO performance_metrics 
                (id, metric_date, metric_type, portfolio_value, total_return, annualized_return, sharpe_ratio, 
                 sortino_ratio, calmar_ratio, max_drawdown, volatility, beta_to_spy, alpha, information_ratio, 
                 benchmark_return, excess_return, win_days, loss_days, win_day_rate, avg_win_day, avg_loss_day, 
                 best_day, worst_day, created_at)
                VALUES (:id, :metric_date, :metric_type, :portfolio_value, :total_return, :annualized_return, :sharpe_ratio, 
                        :sortino_ratio, :calmar_ratio, :max_drawdown, :volatility, :beta_to_spy, :alpha, :information_ratio, 
                        :benchmark_return, :excess_return, :win_days, :loss_days, :win_day_rate, :avg_win_day, :avg_loss_day, 
                        :best_day, :worst_day, :created_at)
            """), {
                "id": uuid.uuid4(),
                "metric_date": date.today(),
                "metric_type": "inception",
                "portfolio_value": 0.0,
                "total_return": 0.0,
                "annualized_return": 0.0,
                "sharpe_ratio": 0.0,
                "sortino_ratio": 0.0,
                "calmar_ratio": 0.0,
                "max_drawdown": 0.0,
                "volatility": 0.0,
                "beta_to_spy": 1.0,
                "alpha": 0.0,
                "information_ratio": 0.0,
                "benchmark_return": 0.0,
                "excess_return": 0.0,
                "win_days": 0,
                "loss_days": 0,
                "win_day_rate": 0.0,
                "avg_win_day": 0.0,
                "avg_loss_day": 0.0,
                "best_day": 0.0,
                "worst_day": 0.0,
                "created_at": now
            })
                
        logger.success("Monitoring database initialized successfully.")
        
    except Exception as e:
        logger.error(f"Failed to initialize monitoring database: {e}")
        raise

if __name__ == "__main__":
    init_monitoring_db()
