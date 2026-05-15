import os
import glob
from sqlalchemy import create_engine, text

def replace_in_files():
    files = glob.glob('**/*.py', recursive=True)
    count = 0
    for f in files:
        if 'venv' in f: continue
        try:
            with open(f, 'r', encoding='utf-8') as file:
                content = file.read()
            if 'String(20)' in content:
                content = content.replace('String(20)', 'String(20)')
                with open(f, 'w', encoding='utf-8') as file:
                    file.write(content)
                count += 1
                print(f"Updated {f}")
        except Exception as e:
            pass
    print(f"Total files updated: {count}")

def alter_databases():
    timescale_url = "postgresql://trader:password@localhost:5435/market_data"
    postgres_url = "postgresql://trader:password@localhost:5434/fundamentals"

    tables = [
        "ohlcv_bars", "market_events", "fundamental_metrics", "macro_indicators", 
        "news_articles", "sentiment_scores", "research_hypotheses", "trading_signals",
        "portfolio_snapshots", "portfolio_positions", "portfolio_allocations",
        "live_orders", "order_fills", "historical_trades",
        "risk_limits", "risk_snapshots", "drawdown_events",
        "monitoring_alerts", "signal_accuracy_logs"
    ]

    for url in [timescale_url, postgres_url]:
        engine = create_engine(url)
        with engine.begin() as conn:
            for table in tables:
                try:
                    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN ticker TYPE VARCHAR(20);"))
                    print(f"Successfully altered {table} in {url.split('/')[-1]}")
                except Exception as e:
                    # Ignore if table doesn't exist or doesn't have ticker
                    pass

if __name__ == '__main__':
    replace_in_files()
    alter_databases()
