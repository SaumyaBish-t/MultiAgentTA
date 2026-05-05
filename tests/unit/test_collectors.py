import asyncio
import pandas as pd
import pytest

from data_ingestion.collectors.market_data_collector import MarketDataCollector, OUTPUT_COLUMNS


@pytest.fixture
def collector():
    return MarketDataCollector()


@pytest.mark.asyncio
async def test_rate_limiter_respects_limits(collector, mocker):
    """Test that the token bucket delays execution when exhausted."""
    # Mock sleep to run fast but track calls
    mock_sleep = mocker.patch("asyncio.sleep")
    
    # Exhaust all tokens in the limiter
    collector._limiter._tokens = 0.0
    
    # This should trigger asyncio.sleep internally
    await collector._limiter.acquire()
    
    mock_sleep.assert_called()


@pytest.mark.asyncio
async def test_retry_on_api_failure(collector, mocker):
    """Test that tenacity retries on failures before falling back."""
    # Mock the synchronous Polygon fetch to fail then succeed
    call_count = 0
    def mock_polygon_sync(ticker, start, end, tf):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("Simulated API failure")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    
    mocker.patch.object(collector, "_fetch_polygon_sync", side_effect=mock_polygon_sync)
    
    # Mock yfinance fallback to succeed
    mock_yf_df = pd.DataFrame({
        "ticker": ["AAPL"],
        "timestamp": [pd.Timestamp("2023-01-01", tz="UTC")],
        "open": [100.0], "high": [105.0], "low": [95.0], "close": [102.0],
        "volume": [1000], "vwap": [101.0], "transactions": [10],
        "timeframe": ["1d"], "source": ["yfinance"],
    })
    mocker.patch.object(collector, "_fetch_yfinance_sync", return_value=mock_yf_df)
    
    # Also mock the limiter to not wait
    mocker.patch.object(collector._limiter, "acquire", new_callable=mocker.AsyncMock)
    
    df = await collector.fetch_latest("AAPL", "1d")
    
    # Polygon should have been retried (call_count >= 2) before falling back
    assert call_count >= 2


@pytest.mark.asyncio
async def test_fallback_to_yfinance(collector, mocker):
    """Test that it falls back to yfinance when Polygon fails completely."""
    # Mock Polygon to always fail
    mocker.patch.object(
        collector, "_fetch_polygon_sync",
        side_effect=Exception("Polygon down")
    )
    
    # Mock yfinance to succeed
    mock_yf_df = pd.DataFrame({
        "ticker": ["AAPL"],
        "timestamp": [pd.Timestamp("2023-01-01", tz="UTC")],
        "open": [100.0], "high": [105.0], "low": [95.0], "close": [102.0],
        "volume": [1000], "vwap": [101.0], "transactions": [10],
        "timeframe": ["1d"], "source": ["yfinance"],
    })
    mocker.patch.object(collector, "_fetch_yfinance_sync", return_value=mock_yf_df)
    
    # Mock the limiter to not wait
    mocker.patch.object(collector._limiter, "acquire", new_callable=mocker.AsyncMock)
    
    df = await collector.fetch_latest("AAPL", "1d")
    
    assert not df.empty
    assert df["source"].iloc[0] == "yfinance"


@pytest.mark.asyncio
async def test_output_format_matches_schema(collector, mocker, mock_polygon_response):
    """Test that returned DataFrame matches the expected standard format."""
    # Mock the Polygon REST client to return our fixture data
    mocker.patch.object(
        collector._polygon, "get_aggs",
        return_value=[
            type("Agg", (), {"timestamp": r["t"], "open": r["o"], "high": r["h"],
                             "low": r["l"], "close": r["c"], "volume": r["v"],
                             "vwap": r["vw"], "transactions": r["n"]})()
            for r in mock_polygon_response["results"]
        ]
    )
    
    # Mock the limiter to not wait
    mocker.patch.object(collector._limiter, "acquire", new_callable=mocker.AsyncMock)
    
    df = await collector.fetch_latest("AAPL", "1min")
    
    # OUTPUT_COLUMNS is the canonical list of columns
    assert all(col in df.columns for col in OUTPUT_COLUMNS)
    assert len(df) >= 2
    assert df["ticker"].iloc[0] == "AAPL"
    assert df["timeframe"].iloc[0] == "1min"
    assert df["source"].iloc[0] == "polygon"
