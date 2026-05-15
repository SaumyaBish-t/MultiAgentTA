print('=== FINAL SYSTEM STATUS ===')
import requests, json

# Backend checks
tests = [
    (8000, '/health', 'Phase 1 Data API'),
    (8001, '/status', 'Phase 8 Monitor API'),
    (8001, '/portfolio', 'Portfolio data'),
    (8001, '/signals', 'Signal data'),
    (8001, '/risk/snapshot', 'Risk data'),
    (8001, '/regime', 'Regime data'),
    (8001, '/strategy-comparison/AAPL?period=1m', 'US Chart Data'),
    (8001, '/strategy-comparison/RELIANCE.NS?period=1m', 'India Chart Data'),
    (8001, '/pipeline/status/AAPL', 'Pipeline status'),
    (8001, '/account/status', 'Paper account status'),
    (8001, '/compliance/status', 'Compliance'),
    (8001, '/audit?limit=3', 'Audit log'),
]

for port, path, name in tests:
    try:
        r = requests.get(f'http://localhost:{port}{path}', timeout=5)
        status = '✅' if r.status_code == 200 else f'❌ HTTP {r.status_code}'
        # Check data isn't empty
        data = r.json()
        has_data = bool(data) and (
            isinstance(data, list) and len(data) > 0
            or isinstance(data, dict) and any(v for v in data.values() if v)
        )
        data_status = '(has data)' if has_data else '(EMPTY - needs seeding)'
        print(f'{status} {name}: {path} {data_status}')
    except Exception as e:
        print(f'💥 {name}: {e}')

# Check SSE endpoints
print('\n=== SSE STREAMING ===')
import threading, time

def test_sse(url, name, timeout=3):
    try:
        r = requests.get(url, stream=True, timeout=timeout)
        chunk = next(r.iter_lines(timeout=timeout))
        print(f'✅ {name}: streaming works - got {len(str(chunk))} bytes')
        r.close()
    except Exception as e:
        print(f'❌ {name}: {e}')

test_sse('http://localhost:8001/realtime/stream/portfolio', 'Portfolio SSE')
test_sse('http://localhost:8001/realtime/stream/prices/AAPL?timeframe=1min', 'AAPL Price SSE')

print('\n=== PAPER TRADING ===')
from alpaca.trading.client import TradingClient
import os
from dotenv import load_dotenv
load_dotenv()
try:
    client = TradingClient(
        os.getenv('ALPACA_API_KEY'),
        os.getenv('ALPACA_SECRET_KEY'),
        paper=True
    )
    acct = client.get_account()
    print(f'✅ Paper account active')
    print(f'   Cash: ${float(acct.cash):,.2f}')
    print(f'   Portfolio: ${float(acct.portfolio_value):,.2f}')
    print(f'   This is 100% simulated money')
except Exception as e:
    print(f'❌ Alpaca paper account: {e}')
