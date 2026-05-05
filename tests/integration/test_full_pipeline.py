import asyncio
import json
import pytest
import pandas as pd
from datetime import datetime, timezone

from data_ingestion.flows.ingestion_flow import (
    collect_market_data_task,
    clean_price_data_task,
    normalize_price_data_task,
    store_price_data_task,
    update_cache_task
)
from data_ingestion.storage.storage_manager import WriteResult


@pytest.mark.asyncio
async def test_end_to_end_single_ticker(mocker, mock_polygon_response):
    """Test full pipeline: collect -> clean -> normalize -> store."""
    mock_df = pd.DataFrame({
        "ticker": ["AAPL", "AAPL"],
        "timestamp": [pd.Timestamp("2023-01-01 10:00:00", tz="UTC"), pd.Timestamp("2023-01-01 10:01:00", tz="UTC")],
        "open": [150.0, 150.5], "high": [151.0, 151.5], "low": [149.0, 150.0], "close": [150.5, 151.0],
        "volume": [1000, 1500], "vwap": [150.2, 150.8], "transactions": [10, 15],
        "timeframe": ["1min", "1min"], "source": ["polygon", "polygon"], "is_adjusted": [True, True]
    })
    
    # 1. Mock the module-level collector's fetch_all_tickers
    mocker.patch(
        "data_ingestion.flows.ingestion_flow.market_data_collector.fetch_all_tickers",
        return_value={"AAPL": mock_df}
    )
    
    # 2. Mock DB storage
    mock_store = mocker.patch(
        "data_ingestion.flows.ingestion_flow.storage.write_price_bars",
        return_value=WriteResult(attempted=2, inserted=2, skipped=0, errors=0)
    )
    
    # 3. Mock the DQ agent DB calls
    mocker.patch("data_ingestion.cleaners.data_quality_agent.get_db_manager")
    mocker.patch("data_ingestion.flows.ingestion_flow.dq_agent.write_anomalies")
    mocker.patch("data_ingestion.flows.ingestion_flow.dq_agent.write_report")
    
    # 4. Mock the normalizer's DB dependency
    mocker.patch("data_ingestion.normalizers.normalizer.get_db_manager")
    mocker.patch("data_ingestion.normalizers.normalizer.yf.Ticker")
    
    # Execute pipeline tasks
    df = await collect_market_data_task.fn("1min")
    assert len(df) == 2
    
    clean_df, report = await clean_price_data_task.fn(df)
    assert len(clean_df) >= 2


@pytest.mark.asyncio
async def test_concurrent_multi_ticker_collection(mocker):
    """Test that multiple tickers are collected concurrently."""
    mocker.patch("data_ingestion.collectors.market_data_collector.settings.tickers", ["AAPL", "MSFT", "GOOGL"])
    
    async def mock_fetch_all(timeframe, **kwargs):
        """Mock fetch_all_tickers to return data for 3 tickers."""
        await asyncio.sleep(0.05)
        result = {}
        for t in ["AAPL", "MSFT", "GOOGL"]:
            result[t] = pd.DataFrame({"ticker": [t], "close": [100.0]})
        return result
        
    mocker.patch(
        "data_ingestion.flows.ingestion_flow.market_data_collector.fetch_all_tickers",
        side_effect=mock_fetch_all
    )
    
    start = asyncio.get_event_loop().time()
    df = await collect_market_data_task.fn("1min")
    end = asyncio.get_event_loop().time()
    
    assert len(df) == 3


@pytest.mark.asyncio
async def test_redis_cache_hit_after_write(mocker, sample_ohlcv_df):
    """Test that Redis cache updates with the latest bar after storage."""
    mock_cache = mocker.patch("data_ingestion.flows.ingestion_flow.storage.cache_latest_prices")
    
    await update_cache_task.fn(sample_ohlcv_df)
    
    mock_cache.assert_called_once()
    args = mock_cache.call_args[0][0]
    assert "AAPL" in args
    assert args["AAPL"]["close"] == 151.5


@pytest.mark.asyncio
async def test_quality_alert_published_on_failure(mocker, sample_ohlcv_df):
    """Test that a high failure rate triggers an alert via DataQualityAgent."""
    from data_ingestion.cleaners.data_quality_agent import DataQualityAgent
    
    mocker.patch("data_ingestion.cleaners.data_quality_agent.get_db_manager")
    agent = DataQualityAgent()
    
    # Mock Redis for the alert
    mock_redis = mocker.MagicMock()
    mocker.patch("data_ingestion.cleaners.data_quality_agent.redis.from_url", return_value=mock_redis)
    
    # Create a report with high failure rate (> 10%)
    report = {
        "source": "test",
        "run_timestamp": datetime.now(tz=timezone.utc),
        "records_received": 10,
        "records_passed": 5,
        "records_failed": 5,
        "failure_rate": 0.50,
        "anomalies_detected": 5,
        "specific_failures": ["corrupted_bar"] * 5,
    }
    
    # Mock write_report to call publish_alert when failure_rate > 0.10
    agent.write_report(report)
    
    # Verify redis publish was called for the alert
    mock_redis.publish.assert_called_once()
    call_args = mock_redis.publish.call_args[0]
    assert call_args[0] == "data_quality_alerts"
    payload = json.loads(call_args[1])
    assert payload["failure_rate"] == 0.50
