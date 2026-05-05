"""
Test P&L Monitor Agent
"""

import asyncio
import json
import redis
from datetime import datetime, timezone, date
from monitoring.agents.pnl_monitor_agent import PnLMonitor
from config.settings import settings
from sqlalchemy import create_engine, text

async def test_pnl_monitor():
    r = redis.from_url(settings.redis_url)
    
    # 1. Mock Portfolio in Redis
    portfolio = {
        "cash": 50000.0,
        "positions": [
            {"ticker": "AAPL", "shares": 100, "entry_price": 150.0, "prev_close": 155.0},
            {"ticker": "TSLA", "shares": 50, "entry_price": 200.0, "prev_close": 210.0}
        ]
    }
    r.set("portfolio:current:state", json.dumps(portfolio))
    
    # 2. Mock Yesterday's metrics in DB
    engine = create_engine(settings.postgres_url)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM performance_metrics"))
        conn.execute(text("""
            INSERT INTO performance_metrics 
            (id, metric_date, metric_type, portfolio_value, total_return, annualized_return, sharpe_ratio, 
             sortino_ratio, calmar_ratio, max_drawdown, volatility, beta_to_spy, alpha, information_ratio, 
             benchmark_return, excess_return, win_days, loss_days, win_day_rate, avg_win_day, avg_loss_day, 
             best_day, worst_day, created_at)
            VALUES (gen_random_uuid(), :metric_date, 'daily', 75000.0, 0.01, 0.15, 1.2, 1.5, 2.0, -0.05, 0.12, 1.0, 0.02, 0.5, 0.005, 0.005, 10, 5, 0.66, 0.01, -0.008, 0.03, -0.02, now())
        """), {"metric_date": date.today() - timedelta(days=1)})

    # 3. Run Monitor
    monitor = PnLMonitor()
    # Note: Phase 1 FastAPI might not be running, so it will fallback to entry_price
    result = await monitor.calculate()
    
    print("P&L Calculation Result:")
    print(f"  Portfolio Value: {result.portfolio_value}")
    print(f"  Daily P&L: {result.daily_pnl} ({result.daily_pnl_pct*100:.2f}%)")
    print(f"  Excess Return: {result.excess_return*100:.2f}%")
    print(f"  Drawdown: {result.drawdown*100:.2f}%")
    print(f"  Attribution items: {len(result.attribution)}")
    print(f"  Alerts: {result.alerts_triggered}")

if __name__ == "__main__":
    from datetime import timedelta
    asyncio.run(test_pnl_monitor())
