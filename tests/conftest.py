import asyncio
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

# Configure pytest-asyncio to handle async tests cleanly
@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_db_connection(mocker):
    """Mocks the SQLAlchemy database manager."""
    mock_manager = mocker.patch("data_ingestion.storage.init_db.get_db_manager")
    return mock_manager

@pytest.fixture
def mock_redis(mocker):
    """Mocks the Redis client."""
    return mocker.patch("data_ingestion.api.cache.redis_client")

@pytest.fixture
def sample_ohlcv_df():
    """Returns a sample DataFrame formatted like OHLCV price data."""
    now = datetime.now(timezone.utc)
    data = {
        "ticker": ["AAPL", "AAPL", "AAPL"],
        "timestamp": [now - timedelta(minutes=2), now - timedelta(minutes=1), now],
        "open": [150.0, 150.5, 151.0],
        "high": [151.0, 151.5, 152.0],
        "low": [149.5, 150.0, 150.5],
        "close": [150.5, 151.0, 151.5],
        "volume": [1000, 1500, 2000],
        "vwap": [150.2, 150.8, 151.2],
        "transactions": [100, 150, 200],
        "timeframe": ["1min", "1min", "1min"],
        "source": ["polygon", "polygon", "polygon"],
        "is_adjusted": [True, True, True]
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_news_df():
    """Returns a sample DataFrame for News articles."""
    return pd.DataFrame({
        "tickers": [["AAPL"], ["MSFT", "GOOGL"]],
        "headline": ["Apple releases new iPhone", "Tech giants report earnings"],
        "summary": ["The new iPhone features an updated camera.", "Microsoft and Google exceed expectations."],
        "source": ["Bloomberg", "Reuters"],
        "url": ["http://bloomberg.com/apple", "http://reuters.com/tech"],
        "published_at": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
        "sentiment_score": [0.8, 0.6]
    })

@pytest.fixture
def sample_fundamentals_df():
    """Returns a sample DataFrame for Income Statements."""
    return pd.DataFrame({
        "ticker": ["AAPL", "AAPL"],
        "fiscal_date": ["2023-09-30", "2023-12-31"],
        "period_type": ["annual", "quarterly"],
        "revenue": [383285000000, 119575000000],
        "net_income": [96995000000, 33916000000]
    })

@pytest.fixture
def mock_polygon_response():
    """Sample JSON response mimicking Polygon.io aggregates API."""
    return {
        "ticker": "AAPL",
        "status": "OK",
        "resultsCount": 2,
        "results": [
            {"v": 1000, "vw": 150.2, "o": 150.0, "c": 150.5, "h": 151.0, "l": 149.5, "t": 1672531200000, "n": 100},
            {"v": 1500, "vw": 150.8, "o": 150.5, "c": 151.0, "h": 151.5, "l": 150.0, "t": 1672531260000, "n": 150}
        ]
    }
