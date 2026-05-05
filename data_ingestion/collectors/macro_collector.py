"""
Trading System — Macro Economic Data Collector
================================================

Collects economic indicator time-series from **FRED** (Federal Reserve
Economic Data) via the ``fredapi`` library.

Tracked series: FEDFUNDS, CPIAUCSL, GDP, UNRATE, DGS10, T10Y2Y, VIXCLS

Schedule: weekly on Sunday night (02:00 UTC Monday).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from fredapi import Fred
from loguru import logger
from tenacity import (
    retry, retry_if_exception_type,
    stop_after_attempt, wait_exponential,
)

from config.settings import PROJECT_ROOT, settings

STAGING_DIR: Path = PROJECT_ROOT / "data_ingestion" / "storage" / "staging"

# Default FRED series to track
DEFAULT_SERIES: list[str] = [
    "FEDFUNDS",   # Federal Funds Rate
    "CPIAUCSL",   # Consumer Price Index (All Urban)
    "GDP",        # Gross Domestic Product
    "UNRATE",     # Unemployment Rate
    "DGS10",      # 10-Year Treasury Yield
    "T10Y2Y",     # 10Y-2Y Treasury Spread (yield curve)
    "VIXCLS",     # CBOE VIX Index
]

# Human-readable names
SERIES_NAMES: dict[str, str] = {
    "FEDFUNDS": "Federal Funds Effective Rate",
    "CPIAUCSL": "Consumer Price Index for All Urban Consumers",
    "GDP":      "Gross Domestic Product",
    "UNRATE":   "Unemployment Rate",
    "DGS10":    "10-Year Treasury Constant Maturity Rate",
    "T10Y2Y":   "10-Year Treasury Minus 2-Year Treasury",
    "VIXCLS":   "CBOE Volatility Index (VIX)",
}

MACRO_COLUMNS: list[str] = [
    "series_id", "series_name", "value",
    "observation_date", "source",
]


@dataclass
class MacroMetrics:
    success: dict[str, int] = field(default_factory=dict)
    failure: dict[str, int] = field(default_factory=dict)
    total_rows: dict[str, int] = field(default_factory=dict)

    def record_success(self, series_id: str, rows: int) -> None:
        self.success[series_id] = self.success.get(series_id, 0) + 1
        self.total_rows[series_id] = self.total_rows.get(series_id, 0) + rows

    def record_failure(self, series_id: str) -> None:
        self.failure[series_id] = self.failure.get(series_id, 0) + 1

    def summary(self) -> dict[str, Any]:
        return {"success": dict(self.success), "failure": dict(self.failure),
                "total_rows": dict(self.total_rows)}


def _stage_parquet(df: pd.DataFrame, tag: str) -> Path | None:
    if df.empty: return None
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    out_dir = STAGING_DIR / "type=macro"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{tag}_{ts}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.debug("Staged {} rows -> {}", len(df), path.name)
    return path


class MacroCollector:
    """
    FRED economic indicator collector.

    Fetches time-series observations for a configurable list of
    FRED series identifiers. Designed to run weekly.

    Parameters
    ----------
    fred_api_key : str, optional
        Overrides ``settings.fred_api_key``.
    series_ids : list[str], optional
        FRED series to track (default: ``DEFAULT_SERIES``).
    """

    def __init__(
        self,
        fred_api_key: str | None = None,
        series_ids: list[str] | None = None,
        retry_attempts: int | None = None,
    ) -> None:
        api_key = fred_api_key or settings.fred_api_key.get_secret_value()
        self._fred = Fred(api_key=api_key)
        self._series_ids = series_ids or DEFAULT_SERIES
        self._retry_attempts = retry_attempts or settings.collector_retry_attempts
        self._backoff = settings.collector_retry_backoff
        self.metrics = MacroMetrics()
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(
            "MacroCollector initialised  series={}  retries={}",
            len(self._series_ids), self._retry_attempts,
        )

    # ── Single series fetch ─────────────────────────────────────

    async def fetch_series(
        self,
        series_id: str,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
    ) -> pd.DataFrame:
        """
        Fetch observations for a single FRED series.

        Parameters
        ----------
        series_id : str
            FRED identifier (e.g. ``"UNRATE"``).
        start_date : str | date, optional
            Start of range (default: 2 years ago).
        end_date : str | date, optional
            End of range (default: today).

        Returns DataFrame matching ``MacroSeries`` model columns.
        """
        if start_date is None:
            start_date = date.today() - timedelta(days=730)
        if end_date is None:
            end_date = date.today()

        logger.info(
            "fetch_series  id={}  range={}/{}",
            series_id, start_date, end_date,
        )

        @retry(
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_exponential(multiplier=1, min=2, max=60, exp_base=self._backoff),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        def _get_data() -> pd.Series:
            return self._fred.get_series(
                series_id,
                observation_start=str(start_date),
                observation_end=str(end_date),
            )

        try:
            loop = asyncio.get_event_loop()
            raw_series: pd.Series = await loop.run_in_executor(None, _get_data)

            if raw_series is None or raw_series.empty:
                logger.warning("FRED returned 0 observations for {}", series_id)
                self.metrics.record_failure(series_id)
                return pd.DataFrame(columns=MACRO_COLUMNS)

            # Drop NaN observations (FRED uses "." for missing)
            raw_series = raw_series.dropna()

            series_name = SERIES_NAMES.get(series_id, series_id)

            df = pd.DataFrame({
                "series_id":        series_id,
                "series_name":      series_name,
                "value":            raw_series.values,
                "observation_date": [
                    datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                    for d in raw_series.index
                ],
                "source":           "fred",
            })

            df = df[MACRO_COLUMNS]
            self.metrics.record_success(series_id, len(df))
            _stage_parquet(df, series_id)
            logger.info(
                "FRED OK  series={}  rows={}  latest={}",
                series_id, len(df),
                df["observation_date"].iloc[-1].strftime("%Y-%m-%d") if len(df) > 0 else "N/A",
            )
            return df

        except Exception as exc:
            logger.error("FRED FAILED  series={}  err={}", series_id, exc)
            self.metrics.record_failure(series_id)
            return pd.DataFrame(columns=MACRO_COLUMNS)

    # ── Bulk fetch ──────────────────────────────────────────────

    async def fetch_all_series(
        self,
        start_date: str | date | None = None,
        max_concurrency: int = 3,
    ) -> dict[str, pd.DataFrame]:
        """
        Fetch all configured FRED series in parallel.

        Returns ``{series_id: DataFrame}``.
        """
        sem = asyncio.Semaphore(max_concurrency)

        async def _one(sid: str) -> tuple[str, pd.DataFrame]:
            async with sem:
                df = await self.fetch_series(sid, start_date=start_date)
                return sid, df

        logger.info(
            "fetch_all_series  count={}  concurrency={}",
            len(self._series_ids), max_concurrency,
        )

        results = await asyncio.gather(*[_one(s) for s in self._series_ids])
        result_dict = dict(results)

        ok = sum(1 for df in result_dict.values() if not df.empty)
        total = sum(len(df) for df in result_dict.values())
        logger.info(
            "fetch_all_series DONE  ok={}/{}  total_rows={}",
            ok, len(self._series_ids), total,
        )
        return result_dict

    # ── Latest value ────────────────────────────────────────────

    async def get_latest_value(self, series_id: str) -> float | None:
        """Return the most recent observation value for a FRED series."""
        try:
            loop = asyncio.get_event_loop()
            val = await loop.run_in_executor(
                None,
                lambda: self._fred.get_series(series_id).dropna().iloc[-1],
            )
            return float(val)
        except Exception as exc:
            logger.warning("get_latest_value FAILED  series={}  err={}", series_id, exc)
            return None

    def print_metrics(self) -> None:
        logger.info("MacroCollector Metrics:\n{}", json.dumps(self.metrics.summary(), indent=2))


macro_collector = MacroCollector()


async def _main() -> None:
    """Smoke-test: fetch UNRATE and DGS10."""
    for sid in ["UNRATE", "DGS10"]:
        df = await macro_collector.fetch_series(sid, start_date="2024-01-01")
        print(f"\n{sid}: {len(df)} observations")
        if not df.empty:
            print(df.tail(3).to_string())
    macro_collector.print_metrics()

if __name__ == "__main__":
    asyncio.run(_main())
