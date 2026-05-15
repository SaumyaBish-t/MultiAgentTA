import psycopg2

conn = psycopg2.connect(
    host='127.0.0.1', port=5434,
    dbname='fundamentals',
    user='trader', password='password'
)
cur = conn.cursor()

tables = [
    'trading_signals', 'portfolio_positions', 'approved_signals',
    'orders', 'executions', 'research_hypotheses', 'sentiment_scores',
    'technical_signals', 'fundamental_scores', 'backtest_results',
    'compliance_checks', 'audit_log', 'pnl_attribution',
    'walk_forward_results', 'signal_live_performance',
    'signal_parameters', 'wash_sale_tracker',
    'pattern_day_trade_tracker', 'rule_violations',
    'portfolio_performance', 'position_limits',
    'rebalance_events', 'var_calculations'
]

for table in tables:
    try:
        cur.execute(f'''
            ALTER TABLE {table}
            ALTER COLUMN ticker TYPE VARCHAR(20)
        ''')
        print(f'Migrated {table}')
    except psycopg2.errors.UndefinedColumn:
        print(f'{table}: no ticker column, skipping')
    except Exception as e:
        if 'already' in str(e).lower() or '20' in str(e):
            print(f'{table}: already VARCHAR(20)')
        else:
            print(f'{table}: {e}')
    conn.commit()

# Also do TimescaleDB
conn2 = psycopg2.connect(
    host='127.0.0.1', port=5435,
    dbname='market_data',
    user='trader', password='password'
)
cur2 = conn2.cursor()
try:
    cur2.execute('ALTER TABLE ohlcv_bars ALTER COLUMN ticker TYPE VARCHAR(20)')
    conn2.commit()
    print('Migrated ohlcv_bars.ticker')
except Exception as e:
    print(f'ohlcv_bars: {e}')

conn2.close()
conn.close()
print('Migration complete')
