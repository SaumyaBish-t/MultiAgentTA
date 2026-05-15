from fastapi import APIRouter
from fastapi.responses import StreamingResponse
import asyncio
import json
import httpx
import yfinance as yf
from datetime import datetime, timezone
from sqlalchemy import text
from data_ingestion.storage.init_db import DatabaseManager
from config.settings import settings
from loguru import logger

router = APIRouter()
db = DatabaseManager(settings.timescale_url, settings.postgres_url)


def _is_indian_ticker(ticker: str) -> bool:
    return '.NS' in ticker or '.BO' in ticker or '.BSE' in ticker


async def _fetch_live_bar_yahoo(ticker: str) -> dict | None:
    """Fetch the latest 1-minute bar directly from Yahoo Finance v8 chart API."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {'interval': '1m', 'range': '1d'}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code != 200:
                return None
            data = resp.json()
            result = data.get('chart', {}).get('result', [])
            if not result:
                return None
            meta = result[0]
            timestamps = meta.get('timestamp', [])
            quotes = meta.get('indicators', {}).get('quote', [{}])[0]
            if not timestamps:
                return None
            # Get the latest bar
            i = len(timestamps) - 1
            c = quotes.get('close', [None] * len(timestamps))[i]
            if c is None:
                # Try second-to-last if latest is None (market just opened a new candle)
                if i > 0:
                    i = i - 1
                    c = quotes.get('close', [None] * len(timestamps))[i]
                if c is None:
                    return None
            return {
                'time': int(timestamps[i]),
                'open': float(quotes.get('open', [0] * len(timestamps))[i] or 0),
                'high': float(quotes.get('high', [0] * len(timestamps))[i] or 0),
                'low': float(quotes.get('low', [0] * len(timestamps))[i] or 0),
                'close': float(c),
                'volume': int(quotes.get('volume', [0] * len(timestamps))[i] or 0),
            }
    except Exception as e:
        logger.debug(f"Yahoo live bar fetch failed for {ticker}: {e}")
        return None


async def _fetch_live_bar_indian(ticker: str) -> dict | None:
    """Fetch the latest price from the free Indian Stock Market API."""
    clean = ticker.replace('.NS', '').replace('.BO', '').replace('.BSE', '')
    url = f"http://65.0.104.9/stock?symbol={clean}"
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return None
            body = resp.json()
            if body.get('status') != 'success':
                return None
            d = body['data']
            return {
                'time': int(datetime.now(timezone.utc).timestamp()),
                'open': float(d.get('open', {}).get('value', 0)),
                'high': float(d.get('day_high', {}).get('value', 0)),
                'low': float(d.get('day_low', {}).get('value', 0)),
                'close': float(d.get('last_price', {}).get('value', 0)),
                'volume': int(d.get('volume', {}).get('value', 0)),
            }
    except Exception as e:
        logger.debug(f"Indian Stock API live bar failed for {ticker}: {e}")
        return None


async def _fetch_live_bar_db(ticker: str, timeframe: str) -> dict | None:
    """Fallback: fetch the latest bar from TimescaleDB."""
    try:
        with db.timescale_session() as session:
            result = session.execute(
                text('''
                    SELECT timestamp, open, high, low, close, volume
                    FROM ohlcv_bars
                    WHERE ticker = :ticker
                    AND timeframe = :timeframe
                    ORDER BY timestamp DESC
                    LIMIT 1
                '''),
                {'ticker': ticker, 'timeframe': timeframe}
            )
            row = result.fetchone()
            if row:
                return {
                    'time': int(row[0].timestamp()),
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': int(row[5]),
                }
    except Exception as e:
        logger.debug(f"DB live bar fetch failed for {ticker}: {e}")
    return None


async def price_event_stream(ticker: str, timeframe: str):
    """
    Stream live price bars to the frontend via SSE.
    Primary source: Yahoo Finance / Indian Stock API (real-time).
    Fallback: TimescaleDB (if available).
    """
    last_time = None
    last_close = None
    is_indian = _is_indian_ticker(ticker)

    while True:
        try:
            bar = None

            # Primary: fetch live data directly from market APIs
            if is_indian:
                bar = await _fetch_live_bar_indian(ticker)
            
            if not bar:
                bar = await _fetch_live_bar_yahoo(ticker)
            
            # Fallback: TimescaleDB
            if not bar:
                bar = await _fetch_live_bar_db(ticker, timeframe)

            if bar and (bar['time'] != last_time or bar['close'] != last_close):
                last_time = bar['time']
                last_close = bar['close']
                bar['ticker'] = ticker
                bar['timeframe'] = timeframe
                yield f"data: {json.dumps(bar)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

        await asyncio.sleep(5)


@router.get('/stream/prices/{ticker}')
async def stream_prices(ticker: str, timeframe: str = '1min'):
    return StreamingResponse(
        price_event_stream(ticker, timeframe),
        media_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
        }
    )

async def portfolio_event_stream():
    import redis.asyncio as aioredis
    r = aioredis.from_url('redis://localhost:6379')

    while True:
        try:
            state_raw = await r.get('portfolio:current:state')
            drawdown_raw = await r.get('portfolio:drawdown:current')
            alert_raw = await r.get('portfolio:alert:level')

            if state_raw:
                state = json.loads(state_raw)
                event = {
                    'portfolio_value': state.get('total_value', 0),
                    'cash': state.get('cash', 0),
                    'drawdown': float(drawdown_raw or 0),
                    'alert_level': (alert_raw or b'green').decode(),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

        await asyncio.sleep(5)

@router.get('/stream/portfolio')
async def stream_portfolio():
    return StreamingResponse(
        portfolio_event_stream(),
        media_type='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'}
    )

async def pipeline_event_stream(run_id: str):
    import redis.asyncio as aioredis
    r = aioredis.from_url('redis://localhost:6379')

    while True:
        try:
            status_raw = await r.get(f'pipeline:status:{run_id}')
            if status_raw:
                status = json.loads(status_raw)
                yield f"data: {json.dumps(status)}\n\n"
                if status.get('stage') == 'completed' or status.get('stage') == 'failed':
                    break
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            break
        await asyncio.sleep(1)

@router.get('/stream/pipeline/{run_id}')
async def stream_pipeline(run_id: str):
    return StreamingResponse(
        pipeline_event_stream(run_id),
        media_type='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'}
    )
