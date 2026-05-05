"""
Prefect Workflow for System Health Monitoring.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from loguru import logger
from prefect import flow, task
from sqlalchemy import func, select

from config.settings import settings
from data_ingestion.storage.init_db import get_db_manager
from data_ingestion.storage.models import NewsArticle, OhlcvBar
from data_ingestion.storage.storage_manager import StorageManager

storage = StorageManager()


def _is_market_hours() -> bool:
    """Helper to determine if it is currently US market hours."""
    now = datetime.now(timezone.utc)
    # Weekends
    if now.weekday() >= 5:
        return False
    # Simplified market hours (13:30 to 20:00 UTC)
    market_open = now.replace(hour=13, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=20, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close


@task(retries=3, retry_delay_seconds=30)
async def check_timescale_health_task() -> list[str]:
    logger.info("Starting check_timescale_health_task")
    errors = []
    manager = get_db_manager()
    
    health = manager.check_connections()
    if not health.get("timescale", False):
        errors.append("TimescaleDB connection is down.")
        return errors

    if _is_market_hours():
        try:
            with manager.timescale_session() as session:
                last_price = session.execute(select(func.max(OhlcvBar.timestamp))).scalar()
                
                if not last_price:
                    errors.append("No OHLCV bars found in TimescaleDB.")
                else:
                    # Make sure last_price is tz-aware for comparison
                    if last_price.tzinfo is None:
                        last_price = last_price.replace(tzinfo=timezone.utc)
                    
                    age = datetime.now(timezone.utc) - last_price
                    if age > timedelta(minutes=5):
                        errors.append(f"Last price bar is too old: {age.total_seconds() / 60:.1f} mins.")
        except Exception as e:
            errors.append(f"Failed to query TimescaleDB last price: {e}")
            
    logger.info(f"Completed check_timescale_health_task: {len(errors)} errors")
    return errors


@task(retries=3, retry_delay_seconds=30)
async def check_postgres_health_task() -> list[str]:
    logger.info("Starting check_postgres_health_task")
    errors = []
    manager = get_db_manager()
    
    health = manager.check_connections()
    if not health.get("postgres", False):
        errors.append("PostgreSQL connection is down.")
        
    logger.info(f"Completed check_postgres_health_task: {len(errors)} errors")
    return errors


@task(retries=3, retry_delay_seconds=30)
async def check_redis_health_task() -> list[str]:
    logger.info("Starting check_redis_health_task")
    errors = []
    if storage.redis is None:
        errors.append("Redis client is not initialized.")
    else:
        try:
            if not storage.redis.ping():
                errors.append("Redis ping failed.")
        except Exception as e:
            errors.append(f"Redis connection error: {e}")
            
    logger.info(f"Completed check_redis_health_task: {len(errors)} errors")
    return errors


@task(retries=3, retry_delay_seconds=30)
async def check_news_freshness_task() -> list[str]:
    logger.info("Starting check_news_freshness_task")
    errors = []
    
    if _is_market_hours():
        manager = get_db_manager()
        try:
            with manager.postgres_session() as session:
                last_article = session.execute(select(func.max(NewsArticle.published_at))).scalar()
                
                if not last_article:
                    errors.append("No news articles found in PostgreSQL.")
                else:
                    if last_article.tzinfo is None:
                        last_article = last_article.replace(tzinfo=timezone.utc)
                        
                    age = datetime.now(timezone.utc) - last_article
                    if age > timedelta(minutes=30):
                        errors.append(f"Last news article is too old: {age.total_seconds() / 60:.1f} mins.")
        except Exception as e:
            errors.append(f"Failed to query PostgreSQL last news article: {e}")
            
    logger.info(f"Completed check_news_freshness_task: {len(errors)} errors")
    return errors


@task(retries=3, retry_delay_seconds=30)
async def alert_evaluator_task(all_errors: list[str]) -> None:
    logger.info("Starting alert_evaluator_task")
    if all_errors:
        error_str = " | ".join(all_errors)
        logger.error(f"Health Monitor Checks Failed: {error_str}")
        
        # Publish alert to Redis
        alert_payload = {
            "source": "health_monitor_flow",
            "failure_rate": 1.0,
            "details": error_str,
            "timestamp": str(datetime.now(timezone.utc))
        }
        if storage.redis:
            try:
                storage.redis.publish("data.quality.alert", json.dumps(alert_payload))
            except Exception as e:
                logger.error(f"Failed to publish health alert to Redis: {e}")
                
    logger.info("Completed alert_evaluator_task")


@flow(name="Data Health Monitor")
async def health_monitor_flow():
    """
    Schedule: every 10 minutes.
    """
    ts_errors = await check_timescale_health_task()
    pg_errors = await check_postgres_health_task()
    redis_errors = await check_redis_health_task()
    news_errors = await check_news_freshness_task()
    
    all_errors = ts_errors + pg_errors + redis_errors + news_errors
    await alert_evaluator_task(all_errors)
