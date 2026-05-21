import psycopg2, json, httpx, redis, subprocess, os
from alpaca.trading.client import TradingClient
from dotenv import load_dotenv

load_dotenv()

out = []

out.append("=== REALTIME 1-MIN DATA STATUS ===")
try:
    pg_timescale = psycopg2.connect(host='127.0.0.1', port=5435, dbname='market_data', user='trader', password='password')
    cur = pg_timescale.cursor()
    cur.execute('''
        SELECT ticker, COUNT(*) as bars, MIN(timestamp) as first, MAX(timestamp) as last, MAX(timestamp) > NOW() - INTERVAL '5 minutes' as is_fresh
        FROM ohlcv_bars WHERE timeframe = '1min' GROUP BY ticker ORDER BY last DESC LIMIT 20
    ''')
    rows = cur.fetchall()
    if not rows:
        out.append("WARNING: No 1-minute bars found in database!")
    else:
        for r in rows:
            status = 'FRESH' if r[4] else 'STALE'
            out.append(f'{status} {r[0]}: {r[1]} bars, last: {r[3]}')

    cur.execute('''SELECT column_name, character_maximum_length FROM information_schema.columns WHERE table_name = 'ohlcv_bars' AND column_name = 'ticker' ''')
    col = cur.fetchone()
    out.append(f"ohlcv_bars.ticker width: {col[1]} chars")
    pg_timescale.close()
except Exception as e:
    out.append(f"TimescaleDB error: {e}")

out.append("\n=== POSTGRES TICKER COLUMN WIDTHS ===")
try:
    pg_fund = psycopg2.connect(host='127.0.0.1', port=5434, dbname='fundamentals', user='trader', password='password')
    cur2 = pg_fund.cursor()
    tables_to_check = ['trading_signals', 'portfolio_positions', 'approved_signals', 'orders', 'research_hypotheses', 'sentiment_scores', 'technical_signals', 'fundamental_scores', 'backtest_results', 'compliance_checks', 'audit_log', 'pnl_attribution']
    for table in tables_to_check:
        try:
            cur2.execute("SELECT character_maximum_length FROM information_schema.columns WHERE table_name = %s AND column_name = 'ticker'", (table,))
            row = cur2.fetchone()
            if row:
                out.append(f"{'OK' if row[0] >= 20 else 'NOT MIGRATED'} {table}.ticker: VARCHAR({row[0]})")
            else:
                out.append(f"NO COL: {table}")
        except Exception as e:
            out.append(f"ERR {table}: {e}")

    out.append("\n=== TIMEFRAME CONSTRAINTS ===")
    cur2.execute("SELECT constraint_name, check_clause FROM information_schema.check_constraints WHERE constraint_name LIKE '%timeframe%'")
    for c in cur2.fetchall():
        has_na = 'n/a' in str(c[1]).lower()
        out.append(f"{'OK' if has_na else 'FAIL'} {c[0]}: {c[1]}")

    out.append("\n=== TABLE ROW COUNTS ===")
    for table in ['trading_signals', 'backtest_results', 'portfolio_positions', 'research_hypotheses', 'sentiment_scores', 'audit_log', 'orders']:
        try:
            cur2.execute(f"SELECT COUNT(*) FROM {table}")
            out.append(f"{table}: {cur2.fetchone()[0]}")
        except Exception as e:
            out.append(f"{table}: {e}")
    pg_fund.close()
except Exception as e:
    out.append(f"Postgres fundamentals error: {e}")

out.append("\n=== API ENDPOINT STATUS ===")
tests = [
    (8000, '/prices/AAPL/latest?timeframe=1min'),
    (8000, '/prices/RELIANCE.NS/latest?timeframe=1min'),
    (8000, '/health'),
    (8001, '/status'),
    (8001, '/portfolio'),
    (8001, '/strategy-comparison/AAPL?period=1m'),
    (8001, '/strategy-comparison/AAPL/live-price'),
    (8001, '/health/detailed'),
]
for port, path in tests:
    try:
        r = httpx.get(f'http://localhost:{port}{path}', timeout=5)
        out.append(f"{r.status_code} {port}{path}")
    except Exception as e:
        out.append(f"FAIL {port}{path}: {e}")

out.append("\n=== REDIS STATE ===")
try:
    r = redis.from_url('redis://localhost:16379', decode_responses=True)
    for key in ['portfolio:current:state', 'portfolio:drawdown:current', 'pipeline:running']:
        val = r.get(key)
        out.append(f"{key}: {'EXISTS' if val else 'MISSING'}")
except Exception as e:
    out.append(f"Redis error: {e}")

out.append("\n=== ALPACA ACCOUNT STATUS ===")
key = os.getenv('ALPACA_API_KEY')
secret = os.getenv('ALPACA_SECRET_KEY')
if not key or not secret:
    out.append("ALPACA KEYS MISSING from .env")
else:
    try:
        client = TradingClient(key, secret, paper=True)
        account = client.get_account()
        out.append(f"Account type: PAPER TRADING")
        out.append(f"Cash: ${float(account.cash):.2f}")
        out.append(f"Portfolio value: ${float(account.portfolio_value):.2f}")
    except Exception as e:
        out.append(f"Paper trading error: {e}")

with open("audit_results.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out))
