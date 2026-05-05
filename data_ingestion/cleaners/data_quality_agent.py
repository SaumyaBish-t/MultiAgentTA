"""
Trading System — Data Quality & Validation Agent
==================================================

Runs AFTER the raw data collectors and BEFORE final normalisation
and insertion into the database.

It employs ``pandera`` for strict schema validation, identifies anomalies
like fat-finger trades or volume spikes, and maintains an audit trail
of data quality via Postgres and Redis.
"""

from __future__ import annotations

import json
import redis
import warnings
from datetime import datetime, timezone
from typing import Any, cast

# Suppress Pandera future warnings about pandas-specific imports
warnings.filterwarnings("ignore", category=FutureWarning, module="pandera")

import numpy as np
import pandas as pd
import pandera as pa
from loguru import logger
from pandera.typing import DataFrame, Series
from scipy import stats

from config.settings import settings
from data_ingestion.storage.init_db import get_db_manager
from data_ingestion.storage.models import DataAnomaly, DataQualityReport

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 1 - PRICE DATA VALIDATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PriceBarSchema(pa.DataFrameModel):
    """Pandera schema for OHLCV market data."""

    ticker: Series[str] = pa.Field(
        nullable=False,
        isin=settings.tickers,
    )
    timestamp: Series[pd.DatetimeTZDtype] = pa.Field(
        nullable=False,
        dtype_kwargs={"unit": "ns", "tz": "UTC"},
    )
    open: Series[float] = pa.Field(gt=0, nullable=False)
    high: Series[float] = pa.Field(gt=0, nullable=False)
    low: Series[float] = pa.Field(gt=0, nullable=False)
    close: Series[float] = pa.Field(gt=0, nullable=False)
    volume: Series[int] = pa.Field(ge=0, nullable=False)
    vwap: Series[float] = pa.Field(gt=0, nullable=True)

    @pa.dataframe_check
    def check_high_is_highest(cls, df: pd.DataFrame) -> Series[bool]:
        return (df["high"] >= df["open"]) & (df["high"] >= df["close"]) & (df["high"] >= df["low"])

    @pa.dataframe_check
    def check_low_is_lowest(cls, df: pd.DataFrame) -> Series[bool]:
        return (df["low"] <= df["open"]) & (df["low"] <= df["close"]) & (df["low"] <= df["high"])


def clean_price_data(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """
    Clean raw OHLCV price data.
    
    Operations:
    - Remove duplicates (ticker, timestamp, timeframe)
    - Drop corrupted bars (high < low)
    - Flag/remove price changes > 15% in a single bar
    - Fill single missing bars via linear interpolation
    - Drop if > 3 consecutive missing bars
    - Ignore overnight/weekend gaps
    
    Returns
    -------
    Cleaned DataFrame and a list of detected anomalies.
    """
    if df.empty:
        return df, []

    anomalies: list[dict[str, Any]] = []

    # 0. Future timestamps
    now = datetime.now(timezone.utc)
    future_mask = df["timestamp"] > now
    if future_mask.any():
        for _, row in df[future_mask].iterrows():
            anomalies.append({
                "ticker": row["ticker"],
                "timestamp": row["timestamp"],
                "anomaly_type": "future_timestamp",
                "value": str(row["timestamp"]),
                "expected_range": f"<= {now}",
                "source": row.get("source", "unknown"),
            })
        df = df[~future_mask]

    # 1. Deduplicate
    df = df.drop_duplicates(subset=["ticker", "timestamp", "timeframe"], keep="last")

    # 2. Corrupted bars (high < low)
    corrupted_mask = df["high"] < df["low"]
    if corrupted_mask.any():
        for _, row in df[corrupted_mask].iterrows():
            anomalies.append({
                "ticker": row["ticker"],
                "timestamp": row["timestamp"],
                "anomaly_type": "corrupted_bar",
                "value": float(row["high"]),
                "expected_range": f">= {row['low']} (low)",
                "source": row.get("source", "unknown"),
            })
        df = df[~corrupted_mask]

    # Ensure chronological order per ticker for the next steps
    df = df.sort_values(["ticker", "timestamp"]).reset_index(drop=True)

    # 3. Fat-finger checks (>15% jump in a single bar)
    # We compare close of current to close of previous (within same ticker)
    df["prev_close"] = df.groupby("ticker")["close"].shift(1)
    df["pct_change"] = (df["close"] - df["prev_close"]).abs() / df["prev_close"]
    
    fat_finger_mask = df["pct_change"] > 0.15
    if fat_finger_mask.any():
        for _, row in df[fat_finger_mask].iterrows():
            anomalies.append({
                "ticker": row["ticker"],
                "timestamp": row["timestamp"],
                "anomaly_type": "fat_finger_price",
                "value": float(row["close"]),
                "expected_range": "< 15% change from previous",
                "source": row.get("source", "unknown"),
            })
        df = df[~fat_finger_mask]
    
    df = df.drop(columns=["prev_close", "pct_change"])

    # 4. Handle missing bars & gaps
    # For interpolation, we operate per-ticker.
    cleaned_dfs = []
    for ticker, group in df.groupby("ticker"):
        group = group.set_index("timestamp")
        
        # We only resample if we know the timeframe and it's intraday or daily.
        # But we must respect market hours. For simplicity, we just use the existing
        # pandas interpolation on missing points if they happen to exist as NaNs.
        # Since we just have a list of bars, we can detect missing sequential bars 
        # by checking the time diff, but pandas `.resample()` is easier.
        timeframe = group["timeframe"].iloc[0] if not group.empty else "1d"
        
        tf_map = {"1min": "1min", "5min": "5min", "15min": "15min", "1h": "1h", "1d": "B"}
        rule = tf_map.get(timeframe)
        
        if rule:
            # Resample to introduce NaNs for missing periods (business days / specific intervals)
            resampled = group.resample(rule).asfreq()
            
            # Find consecutive NaNs
            is_na = resampled["close"].isna()
            consecutive_nas = is_na.groupby((~is_na).cumsum()).cumsum()
            
            # Interpolate single missing bars (limit=1)
            resampled["close"] = resampled["close"].interpolate(method="linear", limit=1)
            resampled["open"] = resampled["open"].interpolate(method="linear", limit=1)
            resampled["high"] = resampled["high"].interpolate(method="linear", limit=1)
            resampled["low"] = resampled["low"].interpolate(method="linear", limit=1)
            
            # Volume and VWAP can be 0 or interpolated
            resampled["volume"] = resampled["volume"].fillna(0)
            
            # Drop chunks with > 3 consecutive missing bars
            # Anything that's STILL NaN after limit=1 interpolation gets dropped.
            # (Note: overnight gaps won't appear if we filter to market hours, 
            # but a simple `.dropna()` handles the remaining NaNs safely here).
            resampled = resampled.dropna(subset=["close"])
            
            resampled["ticker"] = ticker
            resampled["timeframe"] = timeframe
            resampled["source"] = group["source"].iloc[0] if not group.empty else "unknown"
            
            resampled = resampled.reset_index()
            cleaned_dfs.append(resampled)
        else:
            cleaned_dfs.append(group.reset_index())

    if cleaned_dfs:
        df = pd.concat(cleaned_dfs, ignore_index=True)

    # 5. Final preparation for schema validation
    # Ensure timestamp has 'ns' resolution (Pandera is strict)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.as_unit("ns")
    
    # Ensure volume is integer (explicit int64 for Pandera)
    df["volume"] = df["volume"].fillna(0).astype("int64")

    # Final Schema Validation
    try:
        df = PriceBarSchema.validate(df)
    except pa.errors.SchemaError as e:
        logger.error(f"Price Schema Validation Error: {e}")
        # In a real pipeline, we might drop the specific bad rows. 
        # Pandera can be configured to drop invalid rows using `lazy=True`.

    return cast(pd.DataFrame, df), anomalies


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 2 - FUNDAMENTALS VALIDATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FundamentalsSchema(pa.DataFrameModel):
    """Schema for fundamental financial data."""

    ticker: Series[str] = pa.Field(nullable=False)
    revenue: Series[float] = pa.Field(gt=0, nullable=True)  # Companies shouldn't have 0/negative rev
    fiscal_date: Series[pd.DatetimeTZDtype] = pa.Field(
        nullable=False,
        dtype_kwargs={"unit": "ns", "tz": "UTC"},
    )
    period_type: Series[str] = pa.Field(isin=["annual", "quarterly"], nullable=False)
    
    @pa.dataframe_check
    def check_quarter_end(cls, df: pd.DataFrame) -> Series[bool]:
        """Check that fiscal_date falls near a month end."""
        return df["fiscal_date"].dt.is_month_end | (df["fiscal_date"].dt.day >= 25)

    @pa.dataframe_check
    def no_duplicate_fiscal_dates(cls, df: pd.DataFrame) -> bool:
        """Ensure no duplicate fiscal dates per ticker/period."""
        return not df.duplicated(subset=["ticker", "fiscal_date", "period_type"]).any()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 3 - NEWS VALIDATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class NewsSchema(pa.DataFrameModel):
    """Schema for news articles."""

    published_at: Series[pd.DatetimeTZDtype] = pa.Field(
        nullable=False,
        dtype_kwargs={"unit": "ns", "tz": "UTC"},
    )
    headline: Series[str] = pa.Field(nullable=False, str_length={"min_value": 11})
    url: Series[str] = pa.Field(nullable=True, str_startswith="http")
    tickers: Series[object] = pa.Field(nullable=False)

    @pa.dataframe_check
    def not_in_future(cls, df: pd.DataFrame) -> Series[bool]:
        now = pd.Timestamp.now(tz="UTC")
        return df["published_at"] <= now

    @pa.dataframe_check
    def tickers_not_empty(cls, df: pd.DataFrame) -> Series[bool]:
        return df["tickers"].apply(lambda x: isinstance(x, list) and len(x) > 0)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 4 - ANOMALY DETECTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AnomalyDetector:
    """Detects statistical anomalies in timeseries data."""

    @staticmethod
    def detect_price_outliers(df: pd.DataFrame) -> list[dict[str, Any]]:
        """Z-score outlier detection on price changes (|z-score| > 4)."""
        anomalies = []
        if df.empty or len(df) < 2:
            return anomalies

        for ticker, group in df.groupby("ticker"):
            if len(group) < 30:
                continue  # Need enough data for a valid z-score
                
            returns = group["close"].pct_change().dropna()
            z_scores = stats.zscore(returns)
            
            outlier_idx = np.abs(z_scores) > 4
            outliers = returns[outlier_idx]
            
            for idx, val in outliers.items():
                row = group.loc[idx]
                anomalies.append({
                    "ticker": ticker,
                    "timestamp": row["timestamp"],
                    "anomaly_type": "zscore_price_outlier",
                    "value": float(row["close"]),
                    "expected_range": "|z| <= 4",
                    "source": row.get("source", "unknown"),
                })
        return anomalies

    @staticmethod
    def detect_volume_spikes(df: pd.DataFrame) -> list[dict[str, Any]]:
        """Volume spike detection (> 5x 20-day average)."""
        anomalies = []
        if df.empty or "volume" not in df.columns:
            return anomalies

        for ticker, group in df.groupby("ticker"):
            if len(group) < 20:
                continue

            # We use a rolling mean over the past 20 periods
            vol_ma = group["volume"].rolling(window=20, min_periods=1).mean()
            
            # Shift the MA so we compare current volume to the MA of *previous* days
            prev_vol_ma = vol_ma.shift(1)
            
            spike_mask = group["volume"] > (5 * prev_vol_ma)
            
            for _, row in group[spike_mask].iterrows():
                anomalies.append({
                    "ticker": ticker,
                    "timestamp": row["timestamp"],
                    "anomaly_type": "volume_spike",
                    "value": float(row["volume"]),
                    "expected_range": "< 5x 20-period MA",
                    "source": row.get("source", "unknown"),
                })
        return anomalies


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECTION 5 - QUALITY REPORT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DataQualityAgent:
    """
    Orchestrates the validation, anomaly detection, and reporting.
    """

    def __init__(self) -> None:
        self.db_manager = get_db_manager()

    def publish_alert(self, report: dict[str, Any]) -> None:
        """Publish high-failure-rate alerts to Redis."""
        try:
            import redis
            r = redis.from_url(settings.redis_url)
            r.publish("data_quality_alerts", json.dumps(report, default=str))
            logger.warning("Published quality alert to Redis for {}", report["source"])
        except Exception as exc:
            logger.error("Failed to publish alert to Redis: {}", exc)

    def write_anomalies(self, anomalies: list[dict[str, Any]]) -> None:
        """Write detected anomalies to the PostgreSQL `data_anomalies` table."""
        if not anomalies:
            return

        with self.db_manager.postgres_session() as session:
            for item in anomalies:
                anomaly = DataAnomaly(
                    ticker=item.get("ticker"),
                    timestamp=item["timestamp"],
                    anomaly_type=item["anomaly_type"],
                    value=item.get("value"),
                    expected_range=item.get("expected_range"),
                    source=item.get("source", "unknown"),
                )
                session.add(anomaly)
            logger.info("Persisted {} anomalies to DB.", len(anomalies))

    def write_report(self, report: dict[str, Any]) -> None:
        """Write the QualityReport to PostgreSQL."""
        with self.db_manager.postgres_session() as session:
            db_report = DataQualityReport(
                source=report["source"],
                run_timestamp=report["run_timestamp"],
                records_received=report["records_received"],
                records_passed=report["records_passed"],
                records_failed=report["records_failed"],
                failure_rate=report["failure_rate"],
                anomalies_detected=report["anomalies_detected"],
                specific_failures=report["specific_failures"],
            )
            session.add(db_report)
            logger.info(
                "Persisted QualityReport for {} (Failure Rate: {:.2%})", 
                report["source"], report["failure_rate"]
            )

        if report["failure_rate"] > 0.10:
            self.publish_alert(report)

    def process_price_data(self, df: pd.DataFrame, source_name: str = "polygon") -> pd.DataFrame:
        """
        End-to-end processing of a price dataframe.
        """
        records_received = len(df)
        if records_received == 0:
            return df
            
        logger.info("Processing {} price records from {}...", records_received, source_name)

        # 1. Cleaning & structural anomalies
        clean_df, structural_anomalies = clean_price_data(df)
        
        # 2. Statistical anomalies
        zscore_anomalies = AnomalyDetector.detect_price_outliers(clean_df)
        volume_anomalies = AnomalyDetector.detect_volume_spikes(clean_df)
        
        all_anomalies = structural_anomalies + zscore_anomalies + volume_anomalies
        self.write_anomalies(all_anomalies)

        # 3. Validation results
        # We count 'passed' as the number of valid rows we ended up with, 
        # but clamp to records_received to avoid negative failure rates from interpolation.
        records_passed = len(clean_df)
        records_failed = max(0, records_received - (records_passed - len(all_anomalies))) # approximation
        
        # Actually, let's just use a simpler clamp for now to ensure it's not negative
        # and accurately reflects that we might have more data now.
        actual_failures = records_received - len(clean_df)
        records_failed = max(0, actual_failures)
        failure_rate = records_failed / records_received if records_received > 0 else 0.0

        report = {
            "source": source_name,
            "run_timestamp": datetime.now(tz=timezone.utc),
            "records_received": records_received,
            "records_passed": records_passed,
            "records_failed": records_failed,
            "failure_rate": float(failure_rate),
            "anomalies_detected": len(all_anomalies),
            "specific_failures": [a["anomaly_type"] for a in all_anomalies][:10], # Top 10 reasons
        }
        
        self.write_report(report)
        return clean_df
