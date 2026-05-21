import psycopg2, redis, json
from datetime import datetime, timezone, timedelta
import uuid, random, math

pg = psycopg2.connect(
    host='127.0.0.1', port=5434,
    dbname='fundamentals', user='trader', password='password'
)
cur = pg.cursor()

# ── Sync positions from Alpaca ────────────────────────────
try:
    from alpaca.trading.client import TradingClient
    import os
    from dotenv import load_dotenv
    load_dotenv()
    client = TradingClient(
        os.getenv('ALPACA_API_KEY'),
        os.getenv('ALPACA_SECRET_KEY'),
        paper=True
    )
    acct = client.get_account()
    positions = client.get_all_positions()

    # Get portfolio ID
    cur.execute('SELECT id FROM portfolios LIMIT 1')
    row = cur.fetchone()
    if not row:
        port_id = str(uuid.uuid4())
        cur.execute('''
            INSERT INTO portfolios
            (id, name, strategy, total_capital, invested_capital,
             cash, status, created_at, updated_at)
            VALUES (%s,'main_portfolio','black_litterman',
                    %s, %s, %s, 'active', NOW(), NOW())
        ''', (port_id,
              float(acct.portfolio_value),
              float(acct.portfolio_value) - float(acct.cash),
              float(acct.cash)))
        print(f'Created portfolio: {port_id}')
    else:
        port_id = str(row[0])
        cur.execute('''
            UPDATE portfolios SET
                total_capital=%s,
                invested_capital=%s,
                cash=%s,
                updated_at=NOW()
            WHERE id=%s
        ''', (float(acct.portfolio_value),
              float(acct.portfolio_value) - float(acct.cash),
              float(acct.cash), port_id))

    # Sync positions
    for p in positions:
        cur.execute('''
            INSERT INTO portfolio_positions
            (id, portfolio_id, ticker, target_weight, current_weight,
             target_shares, current_shares, target_value_usd,
             current_value_usd, entry_price, current_price,
             unrealized_pnl, unrealized_pnl_pct, status,
             opened_at, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    'active', NOW()-interval'7 days', NOW(), NOW())
            ON CONFLICT (portfolio_id, ticker) DO UPDATE SET
                current_shares=EXCLUDED.current_shares,
                current_price=EXCLUDED.current_price,
                current_value_usd=EXCLUDED.current_value_usd,
                unrealized_pnl=EXCLUDED.unrealized_pnl,
                unrealized_pnl_pct=EXCLUDED.unrealized_pnl_pct,
                updated_at=NOW()
        ''', (
            str(uuid.uuid4()), port_id, p.symbol,
            float(p.market_value)/float(acct.portfolio_value),
            float(p.market_value)/float(acct.portfolio_value),
            int(float(p.qty)), int(float(p.qty)),
            float(p.market_value), float(p.market_value),
            float(p.avg_entry_price), float(p.current_price),
            float(p.unrealized_pl), float(p.unrealized_plpc),
        ))
        print(f'Synced position: {p.symbol}')

    pg.commit()
    print(f'Portfolio synced from Alpaca')
    print(f'Cash: ${float(acct.cash):,.2f}')
    print(f'Portfolio: ${float(acct.portfolio_value):,.2f}')
    print(f'Positions: {len(positions)}')

    if len(positions) == 0:
        raise Exception("Alpaca has 0 positions, falling back to dummy seeds")

except Exception as e:
    print(f'Alpaca sync issue: {e}')
    print('Seeding dummy paper positions...')
    pg.rollback() # Reset transaction

    # Fallback: seed realistic paper positions
    tickers = [
        ('AAPL', 100, 175.50, 182.30, 0.20),
        ('MSFT', 50, 415.20, 425.80, 0.15),
        ('NVDA', 20, 875.30, 895.10, 0.12),
        ('SPY',  80, 525.40, 531.20, 0.30),
        ('GOOGL',40, 175.10, 179.90, 0.10),
    ]

    cur.execute('SELECT id FROM portfolios LIMIT 1')
    row = cur.fetchone()
    port_id = str(row[0]) if row else str(uuid.uuid4())

    if not row:
        cur.execute('''
            INSERT INTO portfolios (id,name,strategy,total_capital,
            invested_capital,cash,status,created_at,updated_at)
            VALUES (%s,'main_portfolio','black_litterman',
                   100000,93758,6242,'active',NOW(),NOW())
        ''', (port_id,))
        
    cur.execute('SELECT id FROM approved_signals LIMIT 1')
    signal_row = cur.fetchone()
    dummy_signal_id = str(signal_row[0]) if signal_row else None

    for ticker, qty, entry, current, weight in tickers:
        pnl = (current - entry) * qty
        pnl_pct = (current - entry) / entry
        cur.execute('''
            INSERT INTO portfolio_positions
            (id,portfolio_id,signal_id,ticker,target_weight,current_weight,
             target_shares,current_shares,target_value_usd,
             current_value_usd,entry_price,current_price,
             unrealized_pnl,unrealized_pnl_pct,status,
             opened_at,created_at,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                   'active',NOW()-interval'14 days',NOW(),NOW())
            ON CONFLICT DO NOTHING
        ''', (str(uuid.uuid4()),port_id,dummy_signal_id,ticker,weight,weight,
              qty,qty,current*qty,current*qty,
              entry,current,pnl,pnl_pct))
    print('Fallback positions seeded')

# ── Seed portfolio performance history ────────────────────
cur.execute('''
    SELECT COUNT(*) FROM portfolio_performance
    WHERE date >= CURRENT_DATE - 30
''')
if cur.fetchone()[0] < 20:
    print('Seeding portfolio performance history...')
    cur.execute('SELECT id FROM portfolios LIMIT 1')
    port_id = str(cur.fetchone()[0])

    base = 100000.0
    spy_base = 525.0
    for i in range(90, 0, -1):
        d = datetime.now().date() - timedelta(days=i)
        if d.weekday() >= 5: continue  # skip weekends
        daily = random.gauss(0.0007, 0.012)
        base *= (1 + daily)
        spy_price = spy_base * (1 + random.gauss(0.0004, 0.009) * (90-i))
        benchmark_ret = (spy_price - spy_base) / spy_base
        cur.execute('''
            INSERT INTO portfolio_performance
            (id,portfolio_id,date,portfolio_value,daily_return,
             cumulative_return,benchmark_return,excess_return,
             rolling_sharpe_30d,rolling_volatility_30d,
             rolling_max_drawdown,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ''', (str(uuid.uuid4()),port_id,d,round(base,2),
              round(daily,6),round((base-100000)/100000,6),
              round(benchmark_ret,6),round(daily-benchmark_ret,6),
              round(random.gauss(1.4,0.2),3),
              round(random.gauss(0.15,0.02),4),
              round(random.gauss(-0.05,0.02),4)))
    print('Performance history seeded')

# ── Seed orders/blotter ───────────────────────────────────
cur.execute('SELECT COUNT(*) FROM orders')
if cur.fetchone()[0] < 10:
    print('Seeding order blotter...')
    cur.execute('SELECT id FROM portfolios LIMIT 1')
    port_id = str(cur.fetchone()[0])
    order_data = [
        ('AAPL','buy',100,175.50,175.55,7,'smart_order_router'),
        ('MSFT','buy',50,415.20,415.28,3,'smart_order_router'),
        ('NVDA','buy',20,875.30,875.45,8,'smart_order_router'),
        ('SPY','buy',80,525.40,525.43,2,'smart_order_router'),
        ('GOOGL','buy',40,175.10,175.17,4,'smart_order_router'),
        ('TSLA','sell',30,245.80,245.71,-5,'smart_order_router'),
    ]
    for ticker,action,qty,req_price,fill_price,slippage,actor in order_data:
        oid = str(uuid.uuid4())
        cur.execute('''
            INSERT INTO orders
            (id,ticker,order_type,action,time_in_force,extended_hours,commission_paid,requested_shares,
             filled_shares,filled_avg_price,status,
             submitted_at,filled_at,slippage_pct,created_at,updated_at)
            VALUES (%s,%s,'market',%s,'day',false,0.0,%s,%s,%s,'filled',
                   NOW()-interval'20 days',NOW()-interval'20 days',
                   %s,NOW()-interval'20 days',NOW()-interval'20 days')
            ON CONFLICT DO NOTHING
        ''', (oid,ticker,action,qty,qty,fill_price,
              slippage/10000))
    print('Orders seeded')

# ── Seed circuit breakers ─────────────────────────────────
cur.execute('SELECT COUNT(*) FROM circuit_breakers')
if cur.fetchone()[0] == 0:
    print('Seeding circuit breakers...')
    breakers = [
        ('portfolio_drawdown', -0.10, 'halt_new_trades'),
        ('daily_loss', -0.03, 'halt_new_trades'),
        ('single_position', -0.15, 'close_position'),
        ('gross_exposure', 0.95, 'block_new_trades'),
        ('sector_concentration', 0.30, 'alert_only'),
        ('volatility', 0.40, 'reduce_50pct'),
    ]
    for btype, threshold, action in breakers:
        cur.execute('''
            INSERT INTO circuit_breakers
            (id,breaker_type,threshold,current_value,triggered,
             auto_reset,action,created_at)
            VALUES (%s,%s,%s,%s,false,true,%s,NOW())
            ON CONFLICT DO NOTHING
        ''', (str(uuid.uuid4()),btype,threshold,threshold*0.2,action))
    print('Circuit breakers seeded')

pg.commit()
pg.close()

# ── Update Redis ──────────────────────────────────────────
r = redis.from_url('redis://localhost:16379')
r.set('portfolio:drawdown:current', '0.023')
r.set('portfolio:peak:value', '102450.0')
r.set('portfolio:alert:level', 'green')
r.set('risk:trading:halted', 'False')
r.set('monitoring:health:latest', json.dumps({
    'overall': 'healthy',
    'checked_at': datetime.now(timezone.utc).isoformat()
}))
print('Redis updated')
print('All seeding complete!')
