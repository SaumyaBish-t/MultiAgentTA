from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from data_ingestion.api.cache import cache_response
from data_ingestion.api.dependencies import get_postgres_db
from data_ingestion.api.schemas import MacroObservation
from data_ingestion.storage.models import MacroSeries

router = APIRouter(prefix="/macro", tags=["Macro"])


@router.get("/snapshot", response_model=Dict[str, MacroObservation])
@cache_response(ttl_seconds=3600)  # 1 hour
async def get_macro_snapshot(
    request: Request,
    db: Session = Depends(get_postgres_db)
):
    """Get latest values for all tracked macroeconomic series."""
    # Subquery to get the latest observation_date per series_id
    subq = (
        select(
            MacroSeries.series_id,
            func.max(MacroSeries.observation_date).label("max_date")
        )
        .group_by(MacroSeries.series_id)
        .subquery()
    )

    # Join back to get the full row
    query = (
        select(MacroSeries)
        .join(
            subq,
            (MacroSeries.series_id == subq.c.series_id) &
            (MacroSeries.observation_date == subq.c.max_date)
        )
    )

    results = db.execute(query).scalars().all()
    
    snapshot = {}
    for r in results:
        snapshot[r.series_id] = r
        
    return snapshot


@router.get("/{series_id}", response_model=List[MacroObservation])
@cache_response(ttl_seconds=3600)
async def get_macro_series(
    request: Request,
    series_id: str,
    start_date: Optional[date] = None,
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_postgres_db)
):
    """Get historical data for a specific FRED series."""
    series_id = series_id.upper()
    query = select(MacroSeries).where(MacroSeries.series_id == series_id)
    
    if start_date:
        query = query.where(MacroSeries.observation_date >= start_date)
        
    query = query.order_by(MacroSeries.observation_date.desc()).limit(limit)
    
    results = db.execute(query).scalars().all()
    # Reverse to return chronological order
    return list(reversed(results))
