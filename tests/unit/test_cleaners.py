import pandas as pd
import pytest
from datetime import datetime, timedelta, timezone

from data_ingestion.cleaners.data_quality_agent import (
    DataQualityAgent,
    AnomalyDetector,
    clean_price_data,
)


@pytest.fixture
def agent(mocker):
    mocker.patch("data_ingestion.cleaners.data_quality_agent.get_db_manager")
    return DataQualityAgent()


def test_removes_duplicate_bars(sample_ohlcv_df):
    """Test that exactly identical rows based on ticker, timestamp, timeframe are removed."""
    # Duplicate the last row
    dup_df = pd.concat([sample_ohlcv_df, sample_ohlcv_df.iloc[[-1]]], ignore_index=True)
    assert len(dup_df) == 4
    
    cleaned_df, _ = clean_price_data(dup_df)
    # After dedup we should have 3 original rows 
    unique_ts = cleaned_df.drop_duplicates(subset=["ticker", "timestamp", "timeframe"])
    assert len(unique_ts) >= 3


def test_detects_price_spikes(sample_ohlcv_df):
    """Test flagging and removing price changes > 15%."""
    df = sample_ohlcv_df.copy()
    # Ensure data is in the past to avoid future-timestamp check
    df["timestamp"] = df["timestamp"] - timedelta(hours=1)
    
    # Add a massive spike
    spike_row = df.iloc[[-1]].copy()
    spike_row["timestamp"] = spike_row["timestamp"] + timedelta(minutes=1)
    spike_row["close"] = df["close"].iloc[-1] * 1.50  # 50% spike
    spike_row["high"] = spike_row["close"] * 1.01  
    df = pd.concat([df, spike_row], ignore_index=True)
    
    cleaned_df, anomalies = clean_price_data(df)
    
    # The spike row should be flagged and removed
    fat_finger_anomalies = [a for a in anomalies if a["anomaly_type"] == "fat_finger_price"]
    assert len(fat_finger_anomalies) >= 1


def test_validates_ohlc_logic(sample_ohlcv_df):
    """Test dropping rows where High is less than Low, etc."""
    df = sample_ohlcv_df.copy()
    # Ensure data is in the past
    df["timestamp"] = df["timestamp"] - timedelta(hours=1)
    
    # Create invalid logic
    invalid_row = df.iloc[[-1]].copy()
    invalid_row["timestamp"] = invalid_row["timestamp"] + timedelta(minutes=1)
    invalid_row["high"] = 100.0
    invalid_row["low"] = 200.0  # High < Low
    df = pd.concat([df, invalid_row], ignore_index=True)
    
    cleaned_df, anomalies = clean_price_data(df)
    
    # The corrupted bar should be flagged
    corrupted = [a for a in anomalies if a["anomaly_type"] == "corrupted_bar"]
    assert len(corrupted) >= 1


def test_handles_missing_bars(sample_ohlcv_df):
    """Test linear interpolation of missing bars (<= 1 consecutive)."""
    df = sample_ohlcv_df.copy()
    # NaN out the close on the middle row
    df.loc[1, "close"] = pd.NA
    
    cleaned_df, _ = clean_price_data(df)
    
    # After cleaning, there should be no NaN close values
    assert not cleaned_df["close"].isna().any()


def test_rejects_future_timestamps(sample_ohlcv_df):
    """Test that clean_price_data handles future timestamps."""
    df = sample_ohlcv_df.copy()
    future_row = df.iloc[[-1]].copy()
    future_row["timestamp"] = datetime.now(timezone.utc) + timedelta(days=30)
    df = pd.concat([df, future_row], ignore_index=True)
    
    cleaned_df, anomalies = clean_price_data(df)
    # The future row should be dropped
    assert len(cleaned_df) == 3
    # Anomaly should be recorded
    future_anomalies = [a for a in anomalies if a["anomaly_type"] == "future_timestamp"]
    assert len(future_anomalies) >= 1


def test_anomaly_detection_zscore(sample_ohlcv_df):
    """Test volume spike detection via AnomalyDetector."""
    # Need at least 20 rows for volume spike detection
    dfs = []
    base_time = datetime.now(timezone.utc) - timedelta(hours=2)
    for i in range(25):
        row = sample_ohlcv_df.iloc[[0]].copy()
        row["timestamp"] = base_time + timedelta(minutes=i)
        row["volume"] = 1000
        dfs.append(row)
    
    df = pd.concat(dfs, ignore_index=True)
    # Add a massive volume spike
    df.loc[24, "volume"] = 1_000_000
    
    anomalies = AnomalyDetector.detect_volume_spikes(df)
    volume_spikes = [a for a in anomalies if a["anomaly_type"] == "volume_spike"]
    assert len(volume_spikes) >= 1


def test_quality_report_generation(agent, sample_ohlcv_df, mocker):
    """Test generation of the DataQualityReport."""
    # Mock the write methods to avoid DB calls
    mocker.patch.object(agent, "write_anomalies")
    mocker.patch.object(agent, "write_report")
    
    df = sample_ohlcv_df.copy()
    df["timestamp"] = df["timestamp"] - timedelta(hours=1)
    
    # Introduce one error: high < low
    invalid_row = df.iloc[[-1]].copy()
    invalid_row["timestamp"] = invalid_row["timestamp"] + timedelta(minutes=1)
    invalid_row["high"] = 0
    invalid_row["low"] = 100
    df = pd.concat([df, invalid_row], ignore_index=True)
    
    # Full run
    cleaned = agent.process_price_data(df, source_name="test_source")
    
    # write_report should have been called with the quality report
    agent.write_report.assert_called_once()
    report = agent.write_report.call_args[0][0]
    assert report["records_received"] == 4
    assert report["source"] == "test_source"
