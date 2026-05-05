from datetime import datetime

from fastapi import APIRouter
from sqlalchemy import func, select

from data_ingestion.api.cache import redis_client
from data_ingestion.api.schemas import HealthResponse
from data_ingestion.storage.init_db import get_db_manager
from data_ingestion.storage.models import MacroSeries, NewsArticle, OhlcvBar

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("", response_model=HealthResponse)
async def health_check():
    """System health, DB status, and latest update timestamps."""
    manager = get_db_manager()
    
    # 1. DB Connections
    db_status = manager.check_connections()
    overall_status = "healthy" if all(db_status.values()) else "degraded"
    
    # 2. Cache Stats
    cache_stats = {"status": "unavailable", "hits": 0, "misses": 0, "hit_rate": 0.0}
    if redis_client:
        try:
            if redis_client.ping():
                cache_stats["status"] = "connected"
                stats = redis_client.hgetall("cache:stats")
                hits = int(stats.get("hits", 0))
                misses = int(stats.get("misses", 0))
                total = hits + misses
                
                cache_stats["hits"] = hits
                cache_stats["misses"] = misses
                if total > 0:
                    cache_stats["hit_rate"] = round(hits / total, 4)
        except Exception:
            cache_stats["status"] = "error"

    # 3. Last updates (approximated via latest timestamps in DB)
    last_updates = {}
    try:
        if db_status.get("timescale"):
            with manager.timescale_session() as ts_session:
                last_price = ts_session.execute(select(func.max(OhlcvBar.timestamp))).scalar()
                last_updates["prices"] = last_price
                
        if db_status.get("postgres"):
            with manager.postgres_session() as pg_session:
                last_news = pg_session.execute(select(func.max(NewsArticle.published_at))).scalar()
                last_macro = pg_session.execute(select(func.max(MacroSeries.observation_date))).scalar()
                last_updates["news"] = last_news
                last_updates["macro"] = last_macro
    except Exception:
        pass

    return HealthResponse(
        status=overall_status,
        databases=db_status,
        cache=cache_stats,
        last_updates=last_updates
    )
