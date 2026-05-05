import pytest
from fastapi.testclient import TestClient

from config.settings import settings
from data_ingestion.api.main import app

# Create a test client using the FastAPI app
client = TestClient(app)

# All requests require the internal API key
HEADERS = {"x-api-key": settings.internal_api_key}

def test_health_endpoint(mocker):
    """Test the /health status endpoint."""
    mocker.patch(
        "data_ingestion.storage.init_db.DatabaseManager.check_connections",
        return_value={"timescale": True, "postgres": True}
    )
    
    response = client.get("/health", headers=HEADERS)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["databases"]["timescale"] is True

def test_get_latest_price(mocker, sample_ohlcv_df):
    """Test the latest price endpoint hits Redis or falls back to DB."""
    # 1. Mock DB fallback
    mock_db = mocker.patch("data_ingestion.api.routers.prices.get_timescale_db")
    
    # 2. Mock Redis cache to return a value
    mocker.patch("data_ingestion.api.cache.redis_client.get", return_value='{"ticker": "AAPL", "close": 150.0, "timeframe": "1min", "timestamp": "2023-01-01T10:00:00Z", "open": 150.0, "high": 150.0, "low": 150.0, "volume": 100, "is_adjusted": true}')
    
    response = client.get("/prices/AAPL/latest?timeframe=1min", headers=HEADERS)
    assert response.status_code == 200
    data = response.json()
    assert data["ticker"] == "AAPL"
    assert data["close"] == 150.0

def test_get_historical_bars(mocker):
    """Test retrieving historical OHLCV bars."""
    # Mock the SQLAlchemy session and query execution
    class MockResult:
        def scalars(self):
            class MockScalars:
                def all(self):
                    return [
                        {"ticker": "AAPL", "timestamp": "2023-01-01T10:00:00Z", "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 1000, "timeframe": "1min", "is_adjusted": True}
                    ]
            return MockScalars()
            
    mock_session = mocker.Mock()
    mock_session.execute.return_value = MockResult()
    
    # Override dependency
    from data_ingestion.api.dependencies import get_timescale_db
    app.dependency_overrides[get_timescale_db] = lambda: mock_session
    
    response = client.get("/prices/AAPL/bars?timeframe=1min", headers=HEADERS)
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["close"] == 100.5
    
    # Cleanup override
    app.dependency_overrides.clear()

def test_search_news_semantic(mocker):
    """Test ChromaDB semantic search endpoint."""
    # Mock ChromaDB
    mock_chroma = mocker.patch("chromadb.PersistentClient")
    mock_collection = mocker.Mock()
    mock_chroma.return_value.get_collection.return_value = mock_collection
    
    mock_collection.query.return_value = {
        "ids": [["url1", "url2"]],
        "distances": [[0.1, 0.2]],
        "metadatas": [[{"url": "url1", "source": "Bloomberg"}, {"url": "url2", "source": "Reuters"}]],
        "documents": [["Apple news article about the latest product release.", "Microsoft news article about their quarterly earnings."]]
    }
    
    # Mock the LangChain embeddings used by the actual endpoint
    mock_embeddings = mocker.Mock()
    mock_embeddings.embed_query.return_value = [0.1] * 1536
    
    # Patch config.llm_config.embeddings (where it's defined)
    mocker.patch("config.llm_config.embeddings", mock_embeddings)
    # Patch data_ingestion.api.routers.news.embeddings (where it's used/imported)
    mocker.patch("data_ingestion.api.routers.news.embeddings", mock_embeddings, create=True)
    
    payload = {"query": "tech giants", "tickers": ["AAPL", "MSFT"], "limit": 2}
    response = client.post("/news/search", json=payload, headers=HEADERS)
    
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 2
    assert data["results"][0]["url"] == "url1"
    # Similarity = 1 - distance = 1 - 0.1 = 0.9
    assert data["results"][0]["similarity"] == pytest.approx(0.9, rel=0.01)
