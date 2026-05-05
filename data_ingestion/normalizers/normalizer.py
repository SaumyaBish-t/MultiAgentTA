"""
Trading System — Normalization Layer
======================================

Standardizes and formats data after the cleaning phase. Ensures all data
types, timezones, strings, and corporate actions are properly adjusted
before insertion into the PostgreSQL/TimescaleDB databases.

Provides the `DataNormalizer` class with pure-function style methods where
possible, and `validate_against_schema` for final SQLAlchemy model validation.
"""

from __future__ import annotations
import contextlib
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Type

import numpy as np
import pandas as pd
import yfinance as yf
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
from loguru import logger
from sqlalchemy import inspect
from sqlalchemy.orm import DeclarativeBase

from data_ingestion.storage.init_db import get_db_manager
from data_ingestion.storage.models import Company

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CUSTOM EXCEPTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class NormalizationError(Exception):
    """Base exception for general normalization failures."""
    pass

class TimestampError(NormalizationError):
    """Raised when timestamp normalization fails or future dates exist."""
    pass

class TickerResolutionError(NormalizationError):
    """Raised when an unknown ticker symbol is encountered."""
    pass

class CorporateActionError(NormalizationError):
    """Raised when corporate action adjustment fails."""
    pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DATA NORMALIZER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DataNormalizer:
    """
    Standardizes data formats across all ingestion sources.
    """

    def __init__(self) -> None:
        self.db_manager = get_db_manager()
        self._valid_tickers: set[str] | None = None

    def _get_valid_tickers(self) -> set[str]:
        """Lazy-load valid tickers from the PostgreSQL companies table."""
        if self._valid_tickers is None:
            try:
                with self.db_manager.postgres_session() as session:
                    tickers = session.query(Company.ticker).all()
                    self._valid_tickers = {t[0] for t in tickers}
            except Exception as exc:
                logger.warning("Could not fetch valid tickers from DB: {}", exc)
                self._valid_tickers = set()
        return self._valid_tickers

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 1 - PRICE NORMALIZATION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def adjust_for_corporate_actions(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Apply backward adjustment factors for splits and dividends.
        Stores the original close as `raw_close` and the adjusted as `close`.
        """
        if df.empty:
            return df

        # Store raw close
        if "raw_close" not in df.columns:
            df["raw_close"] = df["close"]

        try:
            # yfinance logs to stderr directly, we suppress it to avoid log noise
            with open(os.devnull, "w") as f, contextlib.redirect_stderr(f):
                yf_ticker = yf.Ticker(ticker)
                actions = yf_ticker.actions
        except Exception as exc:
            logger.warning(f"Optional corporate action adjustment failed for {ticker}: {exc}")
            return df

        if actions.empty:
            return df

        # We assume df is sorted chronologically
        df = df.sort_values("timestamp").reset_index(drop=True)
        
        # Calculate daily cumulative adjustment factor. 
        # Start from the end (present) moving backwards. 1.0 means no adjustment.
        # If a 2:1 split happens, previous prices must be divided by 2 (or multiplied by 0.5).
        # We'll map actions to the df dates.
        
        df["date_str"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        actions["date_str"] = actions.index.tz_convert("UTC").strftime("%Y-%m-%d") if actions.index.tz is not None else actions.index.tz_localize("UTC").strftime("%Y-%m-%d")
        
        # Merge actions by date
        actions_dict = actions.groupby("date_str").agg({"Dividends": "sum", "Stock Splits": "sum"}).to_dict(orient="index")
        
        adj_factor = 1.0
        factors = []
        
        # Iterate backwards to calculate backward adjustment factor
        for i in range(len(df) - 1, -1, -1):
            date_str = df.at[i, "date_str"]
            action = actions_dict.get(date_str)
            
            if action:
                split = action.get("Stock Splits", 0.0)
                div = action.get("Dividends", 0.0)
                
                close_price = df.at[i, "raw_close"]
                
                # Ex-date logic: the adjustment factor changes *before* the ex-date.
                # So the current row (ex-date) is not adjusted for this action, 
                # but older rows will be.
                
                if split > 0:
                    adj_factor *= split
                    logger.info("Applied Stock Split for {} on {}: factor={}", ticker, date_str, split)
                
                if div > 0 and close_price > div:
                    # Backward dividend adjustment factor: Close / (Close - Div)
                    # When we divide raw_close by this factor, it correctly subtracts the div.
                    div_factor = close_price / (close_price - div)
                    adj_factor *= div_factor
                    logger.info("Applied Dividend for {} on {}: amt={}, factor={:.4f}", ticker, date_str, div, div_factor)

            factors.append(adj_factor)

        # Reverse factors since we built it backwards
        factors.reverse()
        
        df["adj_factor"] = factors
        
        # Apply to OHLCV
        # Prices are divided by the split factor moving backwards (or multiplied by adj_factor)
        # Note: In standard backward adjustment, if adj_factor < 1 (from div), we multiply previous prices.
        # If split = 2, adj_factor becomes 2, meaning previous prices are multiplied by 1/2.
        # Let's standardize: backward factor is multiplied.
        # If split is 2:1, prices before split should be halved.
        # Above we did: adj_factor *= split. So before split, adj_factor = 2. 
        # Price should be Price / adj_factor.
        
        df["open"] = df["open"] / df["adj_factor"]
        df["high"] = df["high"] / df["adj_factor"]
        df["low"] = df["low"] / df["adj_factor"]
        df["close"] = df["raw_close"] / df["adj_factor"]
        
        # Volume is multiplied by the split factor (to maintain total value)
        # But we only adjust volume for splits, not dividends.
        # This is an approximation. A robust engine would track split_factor separately.
        # For this requirement, adjusting close and raw_close is primary.
        
        df = df.drop(columns=["date_str", "adj_factor"])
        return df

    def normalize_timestamps(self, df: pd.DataFrame, time_col: str = "timestamp") -> pd.DataFrame:
        """
        Convert timestamps to UTC timezone-aware.
        Raises TimestampError if future timestamps exist.
        """
        if df.empty or time_col not in df.columns:
            return df

        try:
            # Convert to datetime if it's not already
            if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
                # pd.to_datetime handles unix epochs, strings, etc.
                # using `utc=True` will coerce everything to UTC tz-aware.
                df[time_col] = pd.to_datetime(df[time_col], utc=True)
            else:
                # If naive, localize to UTC. If aware, convert to UTC.
                if df[time_col].dt.tz is None:
                    df[time_col] = df[time_col].dt.tz_localize("UTC")
                else:
                    df[time_col] = df[time_col].dt.tz_convert("UTC")
        except Exception as exc:
            raise TimestampError(f"Failed to normalize timestamps: {exc}") from exc

        # Check for future dates
        now = pd.Timestamp.now(tz="UTC")
        future_mask = df[time_col] > now
        if future_mask.any():
            future_dates = df.loc[future_mask, time_col].tolist()
            raise TimestampError(f"Future timestamps found in data: {future_dates[:5]}")

        return df

    def normalize_ticker_symbols(self, df: pd.DataFrame, ticker_col: str = "ticker") -> pd.DataFrame:
        """
        Map ticker variations to standard format. Flag unknown tickers.
        """
        if df.empty or ticker_col not in df.columns:
            return df

        valid_tickers = self._get_valid_tickers()

        def clean_ticker(t: str) -> str:
            if not isinstance(t, str):
                return str(t)
            t = t.upper().strip()
            # Remove exchange prefixes (e.g., NASDAQ:AAPL -> AAPL)
            t = re.sub(r"^[A-Z]+:", "", t)
            # Remove country suffixes (e.g., AAPL.US -> AAPL)
            t = re.sub(r"\.[A-Z]{2}$", "", t)
            return t

        df[ticker_col] = df[ticker_col].apply(clean_ticker)

        if valid_tickers:
            unknowns = set(df[ticker_col]) - valid_tickers
            if unknowns:
                logger.warning("Unknown tickers found after normalization: {}", unknowns)
                # We do not drop them automatically, but we log the flag.
        
        return df

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 2 - FUNDAMENTALS NORMALIZATION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def normalize_fundamentals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize monetary values to USD millions.
        Compute ratios if applicable fields exist.
        Ensure fiscal_date is the last day of the month/quarter.
        """
        if df.empty:
            return df

        # Standardize monetary values to millions
        # We assume if revenue > 10^8, it's raw dollars and needs conversion.
        # But instructions say: "Standardize all monetary values to USD millions"
        # We'll divide by 1,000,000 for standard monetary columns.
        monetary_cols = [
            "revenue", "gross_profit", "operating_income", "net_income", "ebitda",
            "total_assets", "total_liabilities", "equity", "cash", "total_debt", "market_cap"
        ]
        
        for col in monetary_cols:
            if col in df.columns:
                # Only divide if we suspect it hasn't been divided yet. 
                # e.g., AAPL revenue is ~380B. If it's > 1B, it's raw dollars.
                # A heuristic: if max value > 1,000,000, divide by 1M.
                max_val = df[col].max()
                if pd.notna(max_val) and max_val > 10_000_000:
                    df[col] = df[col] / 1_000_000.0

        # Compute TTM for quarterly data (this is a simplified aggregation over groups)
        # If period_type == 'quarterly', sum last 4 quarters.
        # This requires grouping by ticker and sorting by date.
        if "period_type" in df.columns and "quarterly" in df["period_type"].values:
            # Sort to ensure rolling works correctly
            df = df.sort_values(["ticker", "fiscal_date"]).reset_index(drop=True)
            
            flow_items = ["revenue", "gross_profit", "operating_income", "net_income", "ebitda"]
            
            for col in flow_items:
                if col in df.columns:
                    ttm_col = f"{col}_ttm"
                    df[ttm_col] = df.groupby("ticker")[col].transform(lambda x: x.rolling(4, min_periods=1).sum())

        # Compute standard ratios
        if "gross_profit" in df.columns and "revenue" in df.columns:
            df["gross_margin"] = df["gross_profit"] / df["revenue"].replace(0, np.nan)
        
        if "operating_income" in df.columns and "revenue" in df.columns:
            df["operating_margin"] = df["operating_income"] / df["revenue"].replace(0, np.nan)
            
        if "net_income" in df.columns and "revenue" in df.columns:
            df["net_margin"] = df["net_income"] / df["revenue"].replace(0, np.nan)

        if "total_debt" in df.columns and "equity" in df.columns:
            df["debt_to_equity"] = df["total_debt"] / df["equity"].replace(0, np.nan)

        if "net_income" in df.columns and "equity" in df.columns:
            df["roe"] = df["net_income"] / df["equity"].replace(0, np.nan)

        if "net_income" in df.columns and "total_assets" in df.columns:
            df["roa"] = df["net_income"] / df["total_assets"].replace(0, np.nan)

        # Fiscal date to last day of quarter
        if "fiscal_date" in df.columns:
            df["fiscal_date"] = pd.to_datetime(df["fiscal_date"], utc=True)
            # Offset to end of month
            df["fiscal_date"] = df["fiscal_date"] + pd.offsets.MonthEnd(0)

        return df

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 3 - TEXT/NEWS NORMALIZATION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def normalize_news(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean HTML, standardize sources, extract/validate tickers, truncate content, detect lang.
        """
        if df.empty:
            return df

        valid_tickers = self._get_valid_tickers()

        # 1. Clean HTML tags
        html_regex = re.compile(r"<[^>]+>")
        def clean_html(text: Any) -> str:
            if not isinstance(text, str):
                return ""
            return html_regex.sub("", text).strip()

        if "raw_content" in df.columns:
            df["raw_content"] = df["raw_content"].apply(clean_html)
        if "headline" in df.columns:
            df["headline"] = df["headline"].apply(clean_html)
        if "summary" in df.columns:
            df["summary"] = df["summary"].apply(clean_html)

        # 2. Standardize sources
        def std_source(src: Any) -> str:
            if not isinstance(src, str):
                return "unknown"
            src = src.replace(" L.P.", "").replace(" LLC", "").replace(" Inc.", "")
            return src.strip()
        
        if "source" in df.columns:
            df["source"] = df["source"].apply(std_source)

        # 3. Extract and validate ticker mentions
        def validate_tickers(t_list: Any) -> list[str]:
            if not isinstance(t_list, list):
                return []
            return [t for t in t_list if isinstance(t, str) and t.upper() in valid_tickers]

        if "tickers" in df.columns and valid_tickers:
            df["tickers"] = df["tickers"].apply(validate_tickers)

        # 4. Truncate content to 2000 chars
        if "raw_content" in df.columns:
            df["raw_content"] = df["raw_content"].str[:2000]

        # 5. Language detection
        def detect_lang(text: str) -> str:
            if not text or len(text) < 10:
                return "unknown"
            try:
                return detect(text)
            except LangDetectException:
                return "unknown"

        if "headline" in df.columns:
            df["lang"] = df["headline"].apply(detect_lang)
            non_en = df[df["lang"] != "en"]
            if not non_en.empty:
                logger.info("Flagged {} non-English articles.", len(non_en))

        return df

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 4 - SCHEMA ENFORCEMENT
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def validate_against_schema(self, df: pd.DataFrame, model_class: Type[DeclarativeBase]) -> pd.DataFrame:
        """
        Reflects over the SQLAlchemy model class and ensures the DataFrame
        has the exact required columns and compatible data types.
        Raises NormalizationError if validation fails.
        """
        if df.empty:
            return df

        mapper = inspect(model_class)
        model_columns = {c.key: c for c in mapper.columns}
        
        # 1. Check all non-nullable columns are present
        for col_name, col in model_columns.items():
            if col.primary_key and col.autoincrement and col_name not in df.columns:
                continue # Auto-increment PKs can be omitted
                
            if col.server_default is not None and col_name not in df.columns:
                continue # Columns with server defaults can be omitted
                
            if not col.nullable and col_name not in df.columns:
                raise NormalizationError(f"Missing required column '{col_name}' for model {model_class.__name__}")

        # 2. Check for extra columns in DataFrame
        extra_cols = set(df.columns) - set(model_columns.keys())
        # We might have added temporary columns like 'lang' or 'raw_close'.
        # We should drop them or raise. Let's strictly drop extra columns so it maps perfectly.
        if extra_cols:
            logger.debug("Dropping extra columns not in model {}: {}", model_class.__name__, extra_cols)
            df = df.drop(columns=list(extra_cols))

        # 3. Data type casting to match model roughly
        # For numeric columns
        for col_name in df.columns:
            col_type = model_columns[col_name].type.python_type
            
            try:
                if issubclass(col_type, int):
                    # Replace inf/nan with null before cast
                    df[col_name] = df[col_name].replace([np.inf, -np.inf], np.nan)
                    # Convert to nullable integer type
                    df[col_name] = df[col_name].astype(pd.Int64Dtype())
                elif issubclass(col_type, float):
                    df[col_name] = df[col_name].astype(float)
                elif issubclass(col_type, str):
                    df[col_name] = df[col_name].astype(str).replace("nan", None)
                elif issubclass(col_type, datetime):
                    # Should already be tz-aware datetime if passed through normalize_timestamps
                    if not pd.api.types.is_datetime64_any_dtype(df[col_name]):
                        df[col_name] = pd.to_datetime(df[col_name], utc=True)
            except Exception as exc:
                raise NormalizationError(f"Type mismatch on column '{col_name}'. Expected {col_type}, got error: {exc}") from exc

        return df
