"""
Test Anomaly Detection Agent
"""

import asyncio
import json
import redis
import uuid
from datetime import datetime, timezone, date, timedelta
from monitoring.agents.anomaly_detection_agent import AnomalyDetectionAgent
from config.settings import settings
from sqlalchemy import create_engine, text

async def test_anomaly_detection():
    r = redis.from_url(settings.redis_url)
    engine = create_engine(settings.postgres_url)
    
    # 1. Mock Performance Data to trigger Volatility Spike
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM performance_metrics"))
        # 25 days of 0.1% return, then 5 days of 5% return
        for i in range(30):
            day = date.today() - timedelta(days=i)
            ret = 0.05 if i < 5 else 0.001
            conn.execute(text("""
                INSERT INTO performance_metrics 
                (id, metric_date, metric_type, portfolio_value, total_return, annualized_return, sharpe_ratio, 
                 sortino_ratio, calmar_ratio, max_drawdown, volatility, beta_to_spy, alpha, information_ratio, 
                 benchmark_return, excess_return, win_days, loss_days, win_day_rate, avg_win_day, avg_loss_day, 
                 best_day, worst_day, created_at)
                VALUES (gen_random_uuid(), :metric_date, 'daily', 100000.0, :ret, 0.1, 1.0, 1.0, 1.0, -0.05, 0.1, 1.0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, now())
            """), {"metric_date": day, "ret": ret})

    # 2. Mock Execution Data to trigger High Slippage
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM execution_performance"))
        conn.execute(text("DELETE FROM orders"))
        order_id = uuid.uuid4()
        conn.execute(text("""
            INSERT INTO orders (id, ticker, order_type, action, requested_shares, status, created_at, updated_at, time_in_force, filled_shares, commission_paid, extended_hours)
            VALUES (:id, 'AAPL', 'market', 'buy', 100, 'filled', now(), now(), 'day', 100, 0, false)
        """), {"id": order_id})
        
        for i in range(5):
            conn.execute(text("""
                INSERT INTO execution_performance 
                (id, order_id, ticker, slippage_bps, arrival_price, execution_price, measured_at)
                VALUES (gen_random_uuid(), :order_id, 'AAPL', 65, 150.0, 150.1, now())
            """), {"order_id": order_id})

    # 3. Run Agent
    agent = AnomalyDetectionAgent()
    report = await agent.run()
    
    print("Anomaly Detection Report:")
    print(f"  Total Anomalies: {report.total}")
    print(f"  Critical Anomalies: {report.critical}")
    
    print("\nData Anomalies:")
    for a in report.data: print(f"  - {a['type']} ({a['severity']})")
    
    print("\nExecution Anomalies:")
    for a in report.execution: print(f"  - {a['type']} ({a['severity']}): avg {a.get('avg_bps', 0):.1f} bps")
    
    print("\nPerformance Anomalies:")
    for a in report.performance: print(f"  - {a['type']} ({a['severity']})")
    
    print("\nSystem Anomalies:")
    for a in report.system: print(f"  - {a['type']} ({a['severity']})")

if __name__ == "__main__":
    asyncio.run(test_anomaly_detection())
