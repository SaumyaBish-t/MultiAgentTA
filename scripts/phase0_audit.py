import psycopg2, redis, json, requests
import os

with open("AUDIT.md", "w", encoding="utf-8") as out:
    def write(s):
        out.write(s + "\n")

    # ── Database audit ──────────────────────────────────────────
    try:
        pg = psycopg2.connect(
            host='127.0.0.1', port=5434,
            dbname='fundamentals',
            user='trader', password='password'
        )
        cur = pg.cursor()

        tables = {
            'portfolio_positions': 'Portfolio page - active positions',
            'orders':              'Portfolio page - order blotter',
            'executions':          'Portfolio page - fill details',
            'execution_performance': 'Portfolio page - slippage',
            'portfolio_performance': 'Portfolio page - equity curve',
            'pnl_attribution':     'Portfolio page - PnL breakdown',
            'portfolios':          'Portfolio page - account summary',
            'trading_signals':     'Signals page - signal feed',
            'research_hypotheses': 'Signals page - hypotheses',
            'sentiment_scores':    'Signals page - agent consensus',
            'technical_signals':   'Signals page - technical agent',
            'fundamental_scores':  'Signals page - fundamental agent',
            'macro_signals':       'Signals page - macro agent',
            'backtest_results':    'Signals page - backtest data',
            'walk_forward_results':'Signals page - WF validation',
            'portfolio_risk_snapshots': 'Risk page - VaR/snapshots',
            'var_calculations':    'Risk page - VaR per position',
            'correlation_matrix_snapshots': 'Risk page - heatmap',
            'circuit_breakers':    'Risk page - kill switch status',
            'risk_events':         'Risk page - breach history',
            'audit_log':           'Audit page - event stream',
            'compliance_rules':    'Audit page - rules',
            'rule_violations':     'Audit page - violations',
            'daily_reports':       'Audit page - reports',
            'approved_signals':    'Cross-page - approved signals',
            'rebalance_events':    'Cross-page - rebalancing',
            'signal_generation_runs': 'Cross-page - pipeline runs',
            'research_runs':       'Cross-page - research runs',
        }

        write('=== TABLE CONTENTS ===')
        for table, purpose in tables.items():
            try:
                cur.execute(f'SELECT COUNT(*) FROM {table}')
                count = cur.fetchone()[0]
                flag = '⚠️  EMPTY' if count == 0 else '✅'
                write(f'{flag} {table}: {count} rows — {purpose}')
            except Exception as e:
                write(f'❌ {table}: MISSING/ERROR — {e}')
                pg.rollback()

        pg.close()
    except Exception as e:
        write(f"DB Error: {e}")

    # ── Redis audit ─────────────────────────────────────────────
    r = redis.from_url('redis://localhost:6379')
    redis_keys = [
        'portfolio:current:state',
        'portfolio:drawdown:current',
        'portfolio:peak:value',
        'portfolio:alert:level',
        'risk:var:portfolio:current',
        'risk:trading:halted',
        'monitoring:regime:current',
        'signals:rankings:current',
        'monitoring:pnl:latest',
        'monitoring:health:latest',
    ]
    write('\n=== REDIS KEYS ===')
    for key in redis_keys:
        val = r.get(key)
        flag = '✅' if val else '❌ MISSING'
        preview = str(val)[:60] if val else 'null'
        write(f'{flag} {key}: {preview}')

    # ── Alpaca paper account ─────────────────────────────────────
    write('\n=== ALPACA PAPER ACCOUNT ===')
    try:
        from alpaca.trading.client import TradingClient
        from dotenv import load_dotenv
        load_dotenv()
        client = TradingClient(
            os.getenv('ALPACA_API_KEY'),
            os.getenv('ALPACA_SECRET_KEY'),
            paper=True
        )
        acct = client.get_account()
        positions = client.get_all_positions()
        orders = client.get_orders()
        write(f'✅ Paper cash:     ${float(acct.cash):>12,.2f}')
        write(f'✅ Portfolio val:  ${float(acct.portfolio_value):>12,.2f}')
        write(f'✅ Buying power:   ${float(acct.buying_power):>12,.2f}')
        write(f'✅ Open positions: {len(positions)}')
        write(f'✅ Open orders:    {len(orders)}')
        write(f'   Trading blocked: {acct.trading_blocked}')
        for p in positions:
            pnl = float(p.unrealized_pl)
            pct = float(p.unrealized_plpc) * 100
            write(f'   {p.symbol}: {p.qty} shares | '
                  f'avg ${float(p.avg_entry_price):.2f} | '
                  f'now ${float(p.current_price):.2f} | '
                  f'PnL ${pnl:+.2f} ({pct:+.1f}%)')
    except Exception as e:
        write(f'❌ Alpaca error: {e}')

    # ── API endpoints ────────────────────────────────────────────
    write('\n=== API STATUS ===')
    endpoints = [
        (8000, '/health'),
        (8001, '/status'),
        (8001, '/portfolio/full'),
        (8001, '/signals/full'),
        (8001, '/risk/full'),
        (8001, '/audit/full'),
        (8001, '/compliance/status'),
        (8001, '/performance?period=30d'),
        (8001, '/health/detailed'),
    ]
    for port, path in endpoints:
        try:
            r2 = requests.get(f'http://localhost:{port}{path}', timeout=4)
            empty = not r2.json() or (
                isinstance(r2.json(), dict) and
                all(not v for v in r2.json().values())
            )
            flag = '✅' if r2.status_code == 200 else f'❌ {r2.status_code}'
            note = ' (EMPTY DATA)' if empty else ''
            write(f'{flag} :{port}{path}{note}')
        except Exception as e:
            write(f'❌ :{port}{path} — {e}')
