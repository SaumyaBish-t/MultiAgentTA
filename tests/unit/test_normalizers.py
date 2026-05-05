import pandas as pd
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from data_ingestion.normalizers.normalizer import DataNormalizer


@pytest.fixture
def normalizer(mocker):
    mocker.patch("data_ingestion.normalizers.normalizer.get_db_manager")
    return DataNormalizer()


def test_timestamp_to_utc_from_et(normalizer):
    """Test converting America/New_York timestamps to UTC."""
    # Create a Series with ET tz
    et_time = pd.Timestamp("2023-01-03 09:30:00", tz="America/New_York")
    df = pd.DataFrame({"timestamp": [et_time]})
    
    norm_df = normalizer.normalize_timestamps(df)
    assert norm_df["timestamp"].dt.tz == timezone.utc
    # 09:30 ET in Jan is 14:30 UTC
    assert norm_df["timestamp"].iloc[0].hour == 14


def test_timestamp_to_utc_from_naive(normalizer):
    """Test converting naive timestamps assuming they are UTC."""
    naive_time = pd.Timestamp("2023-01-03 14:30:00")
    df = pd.DataFrame({"timestamp": [naive_time]})
    
    norm_df = normalizer.normalize_timestamps(df)
    assert norm_df["timestamp"].dt.tz == timezone.utc
    assert norm_df["timestamp"].iloc[0].hour == 14


def test_split_adjustment_calculation(normalizer, mocker):
    """Test backward split adjustments using yfinance actions."""
    # Create a mock yfinance actions DataFrame with a 2:1 split
    split_date = pd.Timestamp("2023-01-05", tz="UTC")
    actions_df = pd.DataFrame(
        {"Dividends": [0.0], "Stock Splits": [2.0]},
        index=pd.DatetimeIndex([split_date], name="Date")
    )
    
    mock_yf_ticker = MagicMock()
    mock_yf_ticker.actions = actions_df
    mocker.patch("data_ingestion.normalizers.normalizer.yf.Ticker", return_value=mock_yf_ticker)
    
    df = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-03", periods=4, tz="UTC"),
        "open": [100.0, 100.0, 50.0, 50.0],
        "close": [100.0, 100.0, 50.0, 50.0],
        "high": [100.0, 100.0, 50.0, 50.0],
        "low": [100.0, 100.0, 50.0, 50.0],
        "volume": [1000, 1000, 2000, 2000],
        "is_adjusted": [False, False, False, False]
    })
    
    adj_df = normalizer.adjust_for_corporate_actions(df, "AAPL")
    
    # Pre-split dates should be adjusted: prices divided by split factor (2)
    # So pre-split close of 100.0 / 2 = 50.0
    assert adj_df.loc[0, "close"] == pytest.approx(50.0, rel=0.01)
    assert adj_df.loc[3, "close"] == pytest.approx(50.0, rel=0.01)


def test_dividend_adjustment(normalizer, mocker):
    """Test backward dividend adjustments using yfinance actions."""
    # Create a mock yfinance actions DataFrame with a $1 dividend
    div_date = pd.Timestamp("2023-01-05", tz="UTC")
    actions_df = pd.DataFrame(
        {"Dividends": [1.0], "Stock Splits": [0.0]},
        index=pd.DatetimeIndex([div_date], name="Date")
    )
    
    mock_yf_ticker = MagicMock()
    mock_yf_ticker.actions = actions_df
    mocker.patch("data_ingestion.normalizers.normalizer.yf.Ticker", return_value=mock_yf_ticker)
    
    df = pd.DataFrame({
        "timestamp": pd.date_range("2023-01-03", periods=4, tz="UTC"),
        "open": [101.0, 101.0, 100.0, 100.0],
        "close": [101.0, 101.0, 100.0, 100.0],
        "high": [101.0, 101.0, 100.0, 100.0],
        "low": [101.0, 101.0, 100.0, 100.0],
        "volume": [1000, 1000, 1000, 1000],
        "is_adjusted": [False, False, False, False]
    })
    
    adj_df = normalizer.adjust_for_corporate_actions(df, "AAPL")
    
    # The dividend adjustment should modify pre-dividend prices
    # Post-dividend (rows 2,3) should remain unchanged
    assert adj_df.loc[3, "close"] == pytest.approx(100.0, rel=0.01)
    # Pre-dividend rows should be adjusted downward
    assert adj_df.loc[0, "close"] < 101.0


def test_ticker_symbol_normalization(normalizer, mocker):
    """Test mapping variations to standard format."""
    df = pd.DataFrame({"ticker": ["AAPL.US", "NASDAQ:MSFT", "GOOGL"]})
    
    # Set the internal valid tickers cache directly
    normalizer._valid_tickers = {"AAPL", "MSFT", "GOOGL"}
    
    norm_df = normalizer.normalize_ticker_symbols(df)
    
    assert norm_df["ticker"].iloc[0] == "AAPL"
    assert norm_df["ticker"].iloc[1] == "MSFT"
    assert norm_df["ticker"].iloc[2] == "GOOGL"


def test_ttm_calculation_from_quarters(normalizer):
    """Test calculation of Trailing Twelve Months (TTM) from quarterly statements."""
    df = pd.DataFrame({
        "ticker": ["AAPL"] * 5,
        "fiscal_date": pd.date_range("2022-12-31", periods=5, freq="QE").tz_localize("UTC"),
        "period_type": ["quarterly"] * 5,
        "revenue": [100, 110, 120, 130, 140],
        "net_income": [10, 11, 12, 13, 14]
    })
    
    # normalize_fundamentals computes TTM as rolling(4).sum() columns
    ttm_df = normalizer.normalize_fundamentals(df)
    
    # TTM at Q5 = Q5 + Q4 + Q3 + Q2 = 140 + 130 + 120 + 110 = 500
    assert "revenue_ttm" in ttm_df.columns
    assert "net_income_ttm" in ttm_df.columns
    assert ttm_df.iloc[-1]["revenue_ttm"] == 500
    assert ttm_df.iloc[-1]["net_income_ttm"] == 50
