import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

import yfinance as yf
import httpx
from fastapi import APIRouter, Query, HTTPException
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import redis

from config.settings import settings
from signal_generation.storage.signal_models import TradingSignal, BacktestResult
from data_ingestion.storage.init_db import DatabaseManager
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

def _is_indian_ticker(ticker: str) -> bool:
    return '.NS' in ticker or '.BO' in ticker or '.BSE' in ticker

async def _fetch_indian_stock_api(ticker: str):
    """Fetch current price from the free Indian Stock Market API (0xramm)."""
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
                'open': float(d.get('open', {}).get('value', 0)),
                'high': float(d.get('day_high', {}).get('value', 0)),
                'low': float(d.get('day_low', {}).get('value', 0)),
                'close': float(d.get('last_price', {}).get('value', 0)),
                'volume': int(d.get('volume', {}).get('value', 0)),
                'prev_close': float(d.get('prev_close', {}).get('value', 0)),
            }
    except Exception as e:
        logger.debug(f"Indian Stock API failed for {ticker}: {e}")
        return None

async def _fetch_yahoo_direct_chart(ticker: str, period_days: int = 1, interval: str = '1d'):
    """
    Fetch OHLCV directly from Yahoo Finance v8 chart API.
    More reliable than the yfinance library which often gets rate-limited.
    Returns list of (timestamp, open, high, low, close, volume) tuples.
    """
    range_map = {1: '1d', 7: '5d', 30: '1mo', 90: '3mo', 365: '1y'}
    yf_range = range_map.get(period_days, '3mo')
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {'interval': interval, 'range': yf_range}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code != 200:
                return []
            data = resp.json()
            result = data.get('chart', {}).get('result', [])
            if not result:
                return []
            meta = result[0]
            timestamps = meta.get('timestamp', [])
            quotes = meta.get('indicators', {}).get('quote', [{}])[0]
            bars = []
            for i, ts in enumerate(timestamps):
                c = quotes.get('close', [None] * len(timestamps))[i]
                if c is None:
                    continue
                bars.append((
                    ts,
                    float(quotes.get('open', [0] * len(timestamps))[i] or 0),
                    float(quotes.get('high', [0] * len(timestamps))[i] or 0),
                    float(quotes.get('low', [0] * len(timestamps))[i] or 0),
                    float(c),
                    int(quotes.get('volume', [0] * len(timestamps))[i] or 0),
                ))
            return bars
    except Exception as e:
        logger.debug(f"Yahoo direct chart API failed for {ticker}: {e}")
        return []

router = APIRouter(prefix="/strategy-comparison", tags=["Strategy Comparison"])

# Shared components
r = redis.from_url(settings.redis_url, decode_responses=True)
engine = create_engine(settings.postgres_url)
db_manager = DatabaseManager(settings.timescale_url, settings.postgres_url)

def get_timescale_session():
    return db_manager.timescale_session()

def timeframe_to_bucket(tf: str) -> str:
    mapping = {'1min': '1 minute', '5m': '5 minutes', '30m': '30 minutes', '1h': '1 hour', '6h': '6 hours', '12h': '12 hours', '1d': '1 day'}
    return mapping.get(tf, '1 day')

@router.get("/tickers/available")
async def get_available_tickers():
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT DISTINCT ticker FROM trading_signals")).fetchall()
            signal_tickers = [row[0] for row in res]
            all_tickers = list(set(settings.tickers + signal_tickers))
            tickers_data = []
            for ticker in all_tickers:
                tickers_data.append({
                    "symbol": ticker,
                    "company_name": f"{ticker} Corporation",
                    "has_signals": ticker in signal_tickers,
                    "has_history": True,
                    "last_updated": datetime.now(timezone.utc).isoformat()
                })
            return {"tickers": tickers_data}
    except Exception as e:
        logger.error(f"Failed to get available tickers: {e}")
        return {"tickers": []}

@router.get("/{ticker}")
async def get_strategy_comparison(
    ticker: str,
    period: str = Query("3m", regex="^(5m|30m|6h|12h|1d|1w|1m|3m|1y)$"),
    timeframe: str = Query("1d")
):
    period_days = {'1d': 1, '1w': 7, '1m': 30, '3m': 90, '1y': 365}.get(period, 30)

    db_timeframe = '1min' if timeframe in ['5m', '30m', '1h', '6h', '12h'] else '1d'

    bars = []
    try:
        with get_timescale_session() as session:
            result = session.execute(
                text('''
                    SELECT
                        time_bucket(:bucket, timestamp) as bar_time,
                        FIRST(open, timestamp) as open,
                        MAX(high) as high,
                        MIN(low) as low,
                        LAST(close, timestamp) as close,
                        SUM(volume) as volume
                    FROM ohlcv_bars
                    WHERE ticker = :ticker
                    AND timestamp >= NOW() - (:days * INTERVAL '1 day')
                    AND timeframe = '1min'
                    GROUP BY bar_time
                    ORDER BY bar_time ASC
                ''').bindparams(bucket=timeframe_to_bucket(timeframe), ticker=ticker, days=period_days)
            )
            bars = result.fetchall()
    except Exception as e:
        logger.warning(f"TimescaleDB fetch failed for {ticker}: {e}")

    # Fallback 1: Yahoo Finance direct v8 API (more reliable than yfinance lib)
    if not bars:
        try:
            yf_interval_map = {'5m':'5m','30m':'30m','1h':'1h','6h':'1h','12h':'1h','1d':'1d','1min':'1m'}
            yf_int = yf_interval_map.get(timeframe, '1d')
            yahoo_bars = await _fetch_yahoo_direct_chart(ticker, period_days, yf_int)
            if yahoo_bars:
                bars = yahoo_bars
                logger.info(f"Yahoo direct API returned {len(bars)} bars for {ticker}")
        except Exception as e:
            logger.warning(f"Yahoo direct API fallback failed for {ticker}: {e}")

    # Fallback 2: Indian Stock API for NSE/BSE tickers (real-time single bar)
    if not bars and _is_indian_ticker(ticker):
        try:
            logger.info(f"Attempting Indian Stock API fallback for {ticker}")
            indian_data = await _fetch_indian_stock_api(ticker)
            if indian_data and indian_data['close'] > 0:
                ts_now = datetime.now(timezone.utc).timestamp()
                bars.append((ts_now, indian_data['open'], indian_data['high'],
                             indian_data['low'], indian_data['close'], indian_data['volume']))
        except Exception as e:
            logger.warning(f"Indian Stock API fallback failed for {ticker}: {e}")

    # Fallback 3: Alpaca (US equities only)
    if not bars and not _is_indian_ticker(ticker):
        try:
            logger.info(f"Attempting Alpaca fallback for {ticker}")
            client = StockHistoricalDataClient(settings.alpaca_api_key.get_secret_value(), settings.alpaca_secret_key.get_secret_value())
            tf_mapping = {
                '1min': TimeFrame.Minute,
                '5m': TimeFrame(5, TimeFrameUnit.Minute),
                '30m': TimeFrame(30, TimeFrameUnit.Minute),
                '1h': TimeFrame.Hour,
                '6h': TimeFrame(6, TimeFrameUnit.Hour),
                '12h': TimeFrame(12, TimeFrameUnit.Hour),
                '1d': TimeFrame.Day,
            }
            alpaca_tf = tf_mapping.get(timeframe, TimeFrame.Day)
            req = StockBarsRequest(
                symbol_or_symbols=ticker,
                timeframe=alpaca_tf,
                start=datetime.now(timezone.utc) - timedelta(days=period_days)
            )
            alpaca_bars = client.get_stock_bars(req)
            if alpaca_bars and ticker in alpaca_bars.data:
                for bar in alpaca_bars.data[ticker]:
                    bars.append((bar.timestamp.timestamp(), bar.open, bar.high, bar.low, bar.close, bar.volume))
        except Exception as e:
            logger.warning(f"Alpaca fallback failed for {ticker}: {e}")

    market_data = []
    if bars:
        start_price = float(bars[0][4]) if bars[0][4] else 1.0
        for bar in bars:
            if not bar[4]: continue
            ts = bar[0].timestamp() if hasattr(bar[0], 'timestamp') else bar[0]
            market_data.append({
                'time': int(ts),
                'open': float(bar[1]),
                'high': float(bar[2]),
                'low': float(bar[3]),
                'close': float(bar[4]),
                'volume': int(bar[5] or 0),
                'normalized': ((float(bar[4]) - start_price) / start_price) * 100
            })

    # Strategy Data
    validated_signal = None
    with Session(engine) as session:
        validated_signal = session.query(TradingSignal).filter(
            TradingSignal.ticker == ticker
        ).order_by(TradingSignal.created_at.desc()).first()

    strategy_data = []
    trade_markers = []
    if validated_signal:
        with Session(engine) as session:
            backtest = session.query(BacktestResult).filter(
                BacktestResult.signal_id == validated_signal.id
            ).first()
            if backtest and backtest.equity_curve:
                equity = backtest.equity_curve
                for pt in equity:
                    try:
                        ts = datetime.fromisoformat(pt['date'].replace('Z', '+00:00')).timestamp()
                        strategy_data.append({
                            'time': int(ts),
                            'strategy_value': ((pt.get('value', 100000) - 100000) / 100000) * 100
                        })
                    except:
                        pass
                
            if backtest and backtest.trade_log:
                for trade in backtest.trade_log:
                    try:
                        ts = datetime.fromisoformat(trade['entry_date'].replace('Z', '+00:00')).timestamp()
                        trade_markers.append({
                            'time': int(ts),
                            'action': 'entry',
                            'price': trade.get('entry_price', 0)
                        })
                        if 'exit_date' in trade:
                            ts_exit = datetime.fromisoformat(trade['exit_date'].replace('Z', '+00:00')).timestamp()
                            trade_markers.append({
                                'time': int(ts_exit),
                                'action': 'exit',
                                'price': trade.get('exit_price', 0),
                                'trade_return_pct': trade.get('return_pct', 0)
                            })
                    except:
                        pass

    return {
        'ticker': ticker,
        'company_name': f"{ticker} Corporation",
        'market': 'IN' if '.NS' in ticker or '.BSE' in ticker else 'US',
        'current_price': market_data[-1]['close'] if market_data else 0,
        'price_change': market_data[-1]['close'] - market_data[0]['close'] if market_data else 0,
        'price_change_pct': market_data[-1]['normalized'] if market_data else 0,
        'market_data': market_data,
        'strategy_data': strategy_data,
        'trade_markers': trade_markers,
        'has_strategy': validated_signal is not None,
        'strategy_needs_generation': validated_signal is None,
        'data_source': 'timescaledb_realtime' if bars else 'yfinance_delayed',
        'timeframe': timeframe,
        'period': period
    }

@router.get("/{ticker}/live-price")
async def get_live_price(ticker: str):
    try:
        with get_timescale_session() as session:
            result = session.execute(
                text('''
                    SELECT open, close, timestamp
                    FROM ohlcv_bars
                    WHERE ticker = :ticker
                    ORDER BY timestamp DESC
                    LIMIT 1
                ''').bindparams(ticker=ticker)
            )
            last_bar = result.fetchone()
            
            if last_bar:
                # Get first bar of the day for change calculation
                first_bar_res = session.execute(
                    text('''
                        SELECT open
                        FROM ohlcv_bars
                        WHERE ticker = :ticker
                        AND timestamp::date = :today
                        ORDER BY timestamp ASC
                        LIMIT 1
                    ''').bindparams(ticker=ticker, today=last_bar.timestamp.date())
                )
                first_bar = first_bar_res.fetchone()
                start_open = float(first_bar.open) if first_bar else float(last_bar.open)
                
                change = float(last_bar.close) - start_open
                return {
                    "ticker": ticker,
                    "price": float(last_bar.close),
                    "change": change,
                    "change_pct": (change / start_open) * 100 if start_open > 0 else 0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
    except Exception as e:
        logger.error(f"Live price DB error for {ticker}: {e}")
    
    # Fallback to Indian Stock API for NSE/BSE tickers
    if _is_indian_ticker(ticker):
        try:
            indian_data = await _fetch_indian_stock_api(ticker)
            if indian_data and indian_data['close'] > 0:
                prev = indian_data.get('prev_close', indian_data['open'])
                change = indian_data['close'] - prev
                return {
                    "ticker": ticker,
                    "price": indian_data['close'],
                    "change": change,
                    "change_pct": (change / prev) * 100 if prev > 0 else 0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception:
            pass

    # Fallback to yfinance
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d", interval="1m")
        if not hist.empty:
            last = hist.iloc[-1]
            first = hist.iloc[0]
            change = float(last["Close"] - first["Open"])
            return {
                "ticker": ticker,
                "price": float(last["Close"]),
                "change": change,
                "change_pct": (change / float(first["Open"])) * 100,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    except Exception:
        pass
    
    return {"ticker": ticker, "price": 0, "change": 0, "change_pct": 0, "timestamp": datetime.now(timezone.utc).isoformat()}
