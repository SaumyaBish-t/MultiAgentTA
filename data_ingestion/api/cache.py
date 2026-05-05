"""
Redis Cache & Rate Limiting Utilities.
"""

import functools
import json
from typing import Any, Callable

import redis
from fastapi import HTTPException, Request
from fastapi.encoders import jsonable_encoder
from loguru import logger

from config.settings import settings

try:
    redis_client = redis.from_url(
        settings.redis_url, 
        decode_responses=True,
        socket_timeout=1.0,
        socket_connect_timeout=1.0
    )
except Exception as e:
    logger.error("Redis connection failed in API layer: {}", e)
    redis_client = None


def check_rate_limit(request: Request, limit: int = 100, window: int = 60) -> None:
    """Token-bucket style rate limiting per client ID."""
    if not redis_client:
        return

    client_id = request.headers.get("x-api-key", "internal")
    key = f"rate_limit:{client_id}"
    
    try:
        current = redis_client.incr(key)
        if current == 1:
            redis_client.expire(key, window)
        
        if current > limit:
            raise HTTPException(status_code=429, detail=f"Rate limit exceeded ({limit} req/min)")
    except redis.RedisError as e:
        logger.warning("Rate limit check failed: {}", e)


def cache_response(ttl_seconds: int):
    """
    Decorator to cache FastAPI endpoint responses.
    Tracks hit/miss ratios in Redis hashes.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            if not redis_client:
                return await func(request, *args, **kwargs)

            # Generate cache key from URL and query params
            key = f"cache:{request.url.path}"
            if request.url.query:
                key += f"?{request.url.query}"
            
            stats_key = "cache:stats"
            
            try:
                cached_data = redis_client.get(key)
                if cached_data:
                    redis_client.hincrby(stats_key, "hits", 1)
                    return json.loads(cached_data)
            except Exception as e:
                logger.warning("Cache read failed: {}", e)
                
            # Cache miss - record and execute
            try:
                redis_client.hincrby(stats_key, "misses", 1)
            except Exception:
                pass
                
            # Execute endpoint
            result = await func(request, *args, **kwargs)
            
            # Serialize and cache the result
            try:
                encoded_result = jsonable_encoder(result)
                redis_client.setex(key, ttl_seconds, json.dumps(encoded_result))
            except Exception as e:
                logger.warning("Failed to cache response: {}", e)
                
            return result
        return wrapper
    return decorator
