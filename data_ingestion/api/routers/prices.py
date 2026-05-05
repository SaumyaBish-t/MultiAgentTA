from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_ingestion.api.cache import cache_response, redis_client
from data_ingestion.api.dependencies import get_timescale_db
from data_ingestion.api.schemas import BatchPriceRequest, PriceBar
from data_ingestion.storage.models import OhlcvBar

router = APIRouter(prefix="/prices", tags=["Prices"])


@router.get("/{ticker}/bars", response_model=List[PriceBar])
@cache_response(ttl_seconds=300)  # 5 minutes
async def get_price_bars(
    request: Request,
    ticker: str,
    timeframe: str = Query(..., description="e.g., 1min, 5min, 1h, 1d"),
    start: Optional[date] = None,
    end: Optional[date] = None,
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
    
    # Map to schema, handling adjusted vs raw close if needed
    # Note: Currently our model just has 'close'. If we want raw, we'd need a raw_close column.
    # The models.py doesn't have raw_close natively but we assume it's stored or we just return close.
    bars = []
    for r in results:
        # Pydantic will serialize this directly from attributes
        bars.append(r)
        
    return bars


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
    # Reverse to return chronological order
    return list(reversed(results))


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
