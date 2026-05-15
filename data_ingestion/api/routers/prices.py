from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_ingestion.api.cache import cache_response, redis_client
from data_ingestion.api.dependencies import get_timescale_db
from data_ingestion.api.schemas import BatchPriceRequest, PriceBar
from data_ingestion.storage.models import OhlcvBar

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    HAS_ALPACA = True
except ImportError:
    HAS_ALPACA = False

def _is_indian(ticker: str) -> bool:
    return '.NS' in ticker or '.BO' in ticker or '.BSE' in ticker

def _yahoo_direct_bars(ticker: str, period: str = '2y', interval: str = '1d') -> list[dict]:
    """
    Fetch OHLCV from Yahoo Finance v8 chart API directly.
    More reliable than the yfinance library which gets rate-limited.
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {'interval': interval, 'range': period}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        resp = httpx.get(url, params=params, headers=headers, timeout=20)
        if resp.status_code != 200:
            return []
        data = resp.json()
        result = data.get('chart', {}).get('result', [])
        if not result:
            return []
        meta = result[0]
        timestamps = meta.get('timestamp', [])
        if not timestamps:
            return []
        quotes = meta.get('indicators', {}).get('quote', [{}])[0]
        bars = []
        for i, ts in enumerate(timestamps):
            c = quotes.get('close', [None]*len(timestamps))[i]
            if c is None:
                continue
            bars.append({
                'timestamp': datetime.fromtimestamp(ts, tz=timezone.utc),
                'open': float(quotes.get('open', [0]*len(timestamps))[i] or 0),
                'high': float(quotes.get('high', [0]*len(timestamps))[i] or 0),
                'low': float(quotes.get('low', [0]*len(timestamps))[i] or 0),
                'close': float(c),
                'volume': int(quotes.get('volume', [0]*len(timestamps))[i] or 0),
            })
        return bars
    except Exception as e:
        logger.warning(f"Yahoo direct API failed for {ticker}: {e}")
        return []

router = APIRouter(prefix="/prices", tags=["Prices"])


@router.get("/{ticker}/bars", response_model=List[PriceBar])
@cache_response(ttl_seconds=300)  # 5 minutes
async def get_price_bars(
    request: Request,
    ticker: str,
    timeframe: str = Query(..., description="e.g., 1min, 5min, 1h, 1d"),
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    adjusted: bool = True,
    db: Session = Depends(get_timescale_db)
):
    """Get OHLCV bars for a specific ticker and timeframe."""
    ticker = ticker.upper()
    query = select(OhlcvBar).where(
        OhlcvBar.ticker == ticker,
        OhlcvBar.timeframe == timeframe
    )

    if start:
        query = query.where(OhlcvBar.timestamp >= start)
    if end:
        query = query.where(OhlcvBar.timestamp <= end)
        
    query = query.order_by(OhlcvBar.timestamp.asc())
    
    results = db.execute(query).scalars().all()
    
    bars = list(results)
    
    # If DB has data, return it
    if bars:
        return bars
    
    # ── Fallback: fetch from external sources ──────────────────
    from loguru import logger
    logger.info(f"No TimescaleDB bars for {ticker}/{timeframe}, attempting external fallback")
    
    is_indian = '.NS' in ticker or '.BO' in ticker or '.BSE' in ticker
    fallback_bars = []
    
    # Map timeframe to days of history needed
    days_needed = 120  # Enough for 200-day indicators
    
    # For US stocks: try Alpaca first
    if not is_indian and HAS_ALPACA and timeframe == '1d':
        try:
            from config.settings import settings as _settings
            client = StockHistoricalDataClient(
                _settings.alpaca_api_key.get_secret_value(),
                _settings.alpaca_secret_key.get_secret_value()
            )
            req = StockBarsRequest(
                symbol_or_symbols=ticker,
                timeframe=TimeFrame.Day,
                start=datetime.now() - timedelta(days=int(days_needed * 1.5))
            )
            alpaca_data = client.get_stock_bars(req)
            if alpaca_data and ticker in alpaca_data.data:
                for b in alpaca_data.data[ticker]:
                    fallback_bars.append(PriceBar(
                        ticker=ticker,
                        timestamp=b.timestamp,
                        open=float(b.open),
                        high=float(b.high),
                        low=float(b.low),
                        close=float(b.close),
                        volume=int(b.volume),
                        timeframe=timeframe,
                        is_adjusted=True
                    ))
                logger.info(f"Alpaca returned {len(fallback_bars)} bars for {ticker}/{timeframe}")
        except Exception as e:
            logger.warning(f"Alpaca bars fallback failed for {ticker}: {e}")
    
    # For all tickers: try Yahoo Direct v8 API (works for both US and Indian stocks)
    if not fallback_bars:
        try:
            yf_interval_map = {'1min': '1m', '5min': '5m', '15min': '15m', '1h': '1h', '1d': '1d'}
            yf_int = yf_interval_map.get(timeframe, '1d')
            if yf_int in ['1m', '5m', '15m']:
                yf_period = '5d'
            elif yf_int == '1h':
                yf_period = '1mo'
            else:
                yf_period = '2y'
            raw_bars = _yahoo_direct_bars(ticker, period=yf_period, interval=yf_int)
            for b in raw_bars:
                fallback_bars.append(PriceBar(
                    ticker=ticker,
                    timestamp=b['timestamp'],
                    open=b['open'],
                    high=b['high'],
                    low=b['low'],
                    close=b['close'],
                    volume=b['volume'],
                    timeframe=timeframe,
                    is_adjusted=True
                ))
            if fallback_bars:
                logger.info(f"Yahoo direct API returned {len(fallback_bars)} bars for {ticker}/{timeframe}")
        except Exception as e:
            logger.warning(f"Yahoo direct bars fallback failed for {ticker}: {e}")
    
    return fallback_bars


@router.get("/{ticker}/latest", response_model=Optional[PriceBar])
@cache_response(ttl_seconds=30)  # 30 seconds
async def get_latest_price(
    request: Request,
    ticker: str,
    timeframe: str = Query("1min"),
    db: Session = Depends(get_timescale_db)
):
    """Get the absolute latest bar, hits Redis first via cache_response decorator."""
    ticker = ticker.upper()
    
    # If cache misses, we query DB
    query = select(OhlcvBar).where(
        OhlcvBar.ticker == ticker,
        OhlcvBar.timeframe == timeframe
    ).order_by(OhlcvBar.timestamp.desc()).limit(1)
    
    result = db.execute(query).scalar_one_or_none()
    if not result:
        raise HTTPException(status_code=404, detail=f"No bars found for {ticker}")
        
    return result


@router.get("/{ticker}/history", response_model=List[PriceBar])
@cache_response(ttl_seconds=300)
async def get_price_history(
    request: Request,
    ticker: str,
    days: int = Query(252, description="Trading days history"),
    db: Session = Depends(get_timescale_db)
):
    """Get daily adjusted closes for the past N days."""
    ticker = ticker.upper()
    start_date = datetime.now() - timedelta(days=int(days * 1.5)) # Buffer for weekends
    
    query = select(OhlcvBar).where(
        OhlcvBar.ticker == ticker,
        OhlcvBar.timeframe == "1d",
        OhlcvBar.timestamp >= start_date
    ).order_by(OhlcvBar.timestamp.desc()).limit(days)
    
    results = db.execute(query).scalars().all()
    bars = list(reversed(results))
    
    # If TimescaleDB has data, return it
    if bars:
        return bars
    
    # ── Fallback: fetch from external sources ──────────────────
    from loguru import logger
    logger.info(f"No TimescaleDB data for {ticker}, attempting external fallback")
    
    is_indian = '.NS' in ticker or '.BO' in ticker or '.BSE' in ticker
    fallback_bars = []
    
    # For US stocks: try Alpaca first (free with API key)
    if not is_indian and HAS_ALPACA:
        try:
            from config.settings import settings as _settings
            client = StockHistoricalDataClient(
                _settings.alpaca_api_key.get_secret_value(),
                _settings.alpaca_secret_key.get_secret_value()
            )
            req = StockBarsRequest(
                symbol_or_symbols=ticker,
                timeframe=TimeFrame.Day,
                start=start_date
            )
            alpaca_data = client.get_stock_bars(req)
            if alpaca_data and ticker in alpaca_data.data:
                for b in alpaca_data.data[ticker]:
                    fallback_bars.append(PriceBar(
                        ticker=ticker,
                        timestamp=b.timestamp,
                        open=float(b.open),
                        high=float(b.high),
                        low=float(b.low),
                        close=float(b.close),
                        volume=int(b.volume),
                        timeframe='1d',
                        is_adjusted=True
                    ))
                logger.info(f"Alpaca returned {len(fallback_bars)} daily bars for {ticker}")
        except Exception as e:
            logger.warning(f"Alpaca fallback failed for {ticker}: {e}")
    
    # For all tickers: try Yahoo Direct v8 API (works for both US and Indian stocks)
    if not fallback_bars:
        try:
            raw_bars = _yahoo_direct_bars(ticker, period='2y', interval='1d')
            for b in raw_bars:
                fallback_bars.append(PriceBar(
                    ticker=ticker,
                    timestamp=b['timestamp'],
                    open=b['open'],
                    high=b['high'],
                    low=b['low'],
                    close=b['close'],
                    volume=b['volume'],
                    timeframe='1d',
                    is_adjusted=True
                ))
            if fallback_bars:
                logger.info(f"Yahoo direct API returned {len(fallback_bars)} daily bars for {ticker}")
        except Exception as e:
            logger.warning(f"Yahoo direct fallback failed for {ticker}: {e}")
    
    return fallback_bars[-days:] if fallback_bars else []


@router.post("/batch", response_model=Dict[str, List[PriceBar]])
@cache_response(ttl_seconds=300)
async def get_batch_prices(
    request: Request,
    batch_request: BatchPriceRequest,
    db: Session = Depends(get_timescale_db)
):
    """Fetch prices for multiple tickers at once."""
    tickers = [t.upper() for t in batch_request.tickers]
    
    query = select(OhlcvBar).where(
        OhlcvBar.ticker.in_(tickers),
        OhlcvBar.timeframe == batch_request.timeframe
    )
    
    if batch_request.start:
        query = query.where(OhlcvBar.timestamp >= batch_request.start)
    if batch_request.end:
        query = query.where(OhlcvBar.timestamp <= batch_request.end)
        
    query = query.order_by(OhlcvBar.timestamp.asc())
    results = db.execute(query).scalars().all()
    
    output = {ticker: [] for ticker in tickers}
    for r in results:
        output[r.ticker].append(r)
        
    return output
