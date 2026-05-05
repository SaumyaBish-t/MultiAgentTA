"""
FastAPI Dependencies for the Internal Data Access API.
"""

from typing import Generator

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session

from config.settings import settings
from data_ingestion.storage.init_db import get_db_manager

# Expect an "x-api-key" header
api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


def verify_api_key(request: Request = None, api_key: str = Security(api_key_header)) -> str:
    """
    Simple API Key authentication.
    Requires INTERNAL_API_KEY to be set in .env
    Skips auth for /health endpoint (monitoring).
    """
    # Allow health checks without API key
    if request and request.url.path.startswith("/health"):
        return api_key or "health-check"

    # Assuming we add internal_api_key to settings, or just use a dummy one if missing.
    expected_key = getattr(settings, "internal_api_key", "secret-internal-token-123")
    
    if api_key != expected_key:
        raise HTTPException(
            status_code=403,
            detail="Forbidden: Invalid internal API key"
        )
    return api_key


def get_timescale_db() -> Generator[Session, None, None]:
    """Dependency: Yields a TimescaleDB (market_data) session."""
    manager = get_db_manager()
    # Using the context manager ensures rollback on exception and close
    with manager.timescale_session() as session:
        yield session


def get_postgres_db() -> Generator[Session, None, None]:
    """Dependency: Yields a PostgreSQL (fundamentals) session."""
    manager = get_db_manager()
    with manager.postgres_session() as session:
        yield session
