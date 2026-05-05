"""
Trading System — Internal Data Access API
===========================================

Centralized internal FastAPI application providing data access to all 
downstream trading system pipelines (Research, Signal, Risk, Execution).

Enforces read-only DB access, Redis response caching, token-bucket 
rate limiting, and centralized observability logging.
"""

import time
from typing import Callable

import uvicorn
from fastapi import Depends, FastAPI, Request, Response
from loguru import logger

from data_ingestion.api.cache import check_rate_limit
from data_ingestion.api.dependencies import verify_api_key
from data_ingestion.api.routers import fundamentals, health, macro, news, prices
from data_ingestion.storage.init_db import init_databases
from config.settings import settings

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  APPLICATION SETUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

app = FastAPI(
    title="Data Access Layer API",
    description="Internal data access gateway for the Algorithmic Trading System.",
    version="1.0.0",
    # Global dependency enforcing API Key for all endpoints
    dependencies=[Depends(verify_api_key)]
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MIDDLEWARE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.middleware("http")
async def observe_and_limit_requests(request: Request, call_next: Callable) -> Response:
    """
    Middleware that enforces rate limits and logs structured 
    observability data for every request.
    """
    start_time = time.perf_counter()
    
    # 1. Rate Limiting (throws 429 if exceeded)
    # Exclude health checks from strict rate limits if desired, but we limit globally here.
    if request.url.path != "/health":
        check_rate_limit(request, limit=100, window=60)
        
    # 2. Process Request
    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as exc:
        logger.exception("Unhandled exception during request processing")
        raise exc
        
    # 3. Observability Logging
    process_time = time.perf_counter() - start_time
    
    logger.info(
        "API Request | {method} {path} | status={status} | time={time:.4f}s | client={client}",
        method=request.method,
        path=request.url.path,
        status=status_code,
        time=process_time,
        client=request.headers.get("x-api-key", "unknown")
    )
    
    # Add custom header for debugging
    response.headers["X-Process-Time"] = str(process_time)
    return response


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ROUTERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Health endpoint is unprotected — accessible without API key for monitoring
app.include_router(health.router, dependencies=[])
app.include_router(prices.router)
app.include_router(fundamentals.router)
app.include_router(news.router)
app.include_router(macro.router)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LIFECYCLE EVENTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_event("startup")
async def startup_event():
    """Initialise database connections and verify health at startup."""
    logger.info("Starting Data Access Layer API...")
    logger.info("Initializing databases...")
    init_databases()
    logger.info("Validating LLM connections...")
    settings.validate_llm_connections()
    logger.info("API Startup Complete.")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down Data Access Layer API...")
    from data_ingestion.storage.init_db import get_db_manager
    get_db_manager().dispose()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ENTRY POINT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    uvicorn.run(
        "data_ingestion.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
