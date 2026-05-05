"""
Trading System — Market Data Collector
=======================================

Collects OHLCV price bars from **Polygon.io** (primary) with
**yfinance** as an automatic fallback.

Features
--------
* Token-bucket rate limiter tuned for Polygon free tier (5 req/min)
* Tenacity-based retry with exponential back-off (configurable)
* Async parallel fetching across all tickers via ``asyncio``
* Redis-cached ``get_current_price`` for hot-path reads
* Raw data staged as Parquet before the cleaning pipeline
* Per-ticker success / failure metrics for observability

Usage
-----
::

    from data_ingestion.collectors.market_data_collector import collector

    # Historical back-fill
    df = await collector.fetch_historical("AAPL", "2024-01-01", "2024-06-01", "1d")

    # Incremental update — all tickers, 1-min bars
    results = await collector.fetch_all_tickers("1min")

    # Single latest price (Redis first, then API)
    price = await collector.get_current_price("TSLA")
"""

from __future__ import annotations

import asyncio
import json
import time as _time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf
from loguru import logger
from polygon import RESTClient as PolygonClient
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import PROJECT_ROOT, Timeframe, settings

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants & Mappings
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STAGING_DIR: Path = PROJECT_ROOT / "data_ingestion" / "storage" / "staging"
"""Directory for raw Parquet files before the cleaning pipeline."""

# Our Timeframe enum → Polygon (multiplier, timespan)
_POLYGON_TF_MAP: dict[str, tuple[int, str]] = {
    "1min":  (1,  "minute"),
    "5min":  (5,  "minute"),
    "15min": (15, "minute"),
    "1h":    (1,  "hour"),
    "4h":    (4,  "hour"),
    "1d":    (1,  "day"),
    "1w":    (1,  "week"),
}

# Our Timeframe enum → yfinance interval string
_YF_TF_MAP: dict[str, str] = {
    "1min":  "1m",
    "5min":  "5m",
    "15min": "15m",
    "1h":    "1h",
    "4h":    "1h",   # yfinance has no 4h — we'll resample from 1h
    "1d":    "1d",
    "1w":    "1wk",
}

# yfinance limits max history by interval
_YF_MAX_PERIOD: dict[str, str] = {
    "1m":  "7d",
    "5m":  "60d",
    "15m": "60d",
    "1h":  "730d",
    "1d":  "max",
    "1wk": "max",
}

# Standardised output columns — exact match to OHLCVBar model
OUTPUT_COLUMNS: list[str] = [
    "ticker",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",
    "timeframe",
    "source",
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Token-Bucket Rate Limiter
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TokenBucketRateLimiter:
    """
    Thread-safe, async-compatible token-bucket rate limiter.

    Parameters
    ----------
    rate : float
        Tokens added per second.
    capacity : int
        Maximum burst capacity.
    """

    def __init__(self, rate: float, capacity: int) -> None:
        self._rate = rate
        self._capacity = capacity
        self._tokens: float = float(capacity)
        self._last_refill: float = _time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until a token is available, then consume one."""
        while True:
            async with self._lock:
                now = _time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self._capacity,
                    self._tokens + elapsed * self._rate,
                )
                self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

            # Not enough tokens — wait a fraction of the refill period
            await asyncio.sleep(1.0 / self._rate)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Metrics Tracker
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class CollectorMetrics:
    """Simple in-memory counters for fetch success / failure per ticker."""

    success: dict[str, int] = field(default_factory=dict)
    failure: dict[str, int] = field(default_factory=dict)
    total_rows: dict[str, int] = field(default_factory=dict)

    def record_success(self, ticker: str, rows: int) -> None:
        self.success[ticker] = self.success.get(ticker, 0) + 1
        self.total_rows[ticker] = self.total_rows.get(ticker, 0) + rows

    def record_failure(self, ticker: str) -> None:
        self.failure[ticker] = self.failure.get(ticker, 0) + 1

    def summary(self) -> dict[str, Any]:
        return {
            "success": dict(self.success),
            "failure": dict(self.failure),
            "total_rows": dict(self.total_rows),
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Market Data Collector
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MarketDataCollector:
    """
    Production-grade OHLCV collector.

    Primary source : Polygon.io REST API
    Fallback       : yfinance (Yahoo Finance)

    Parameters
    ----------
    polygon_api_key : str
        Polygon.io API key.
    rate_limit : float
        Requests per minute for the Polygon free tier (default 5).
    retry_attempts : int
        Max retries before falling back (default from settings).
    timeout : int
        HTTP timeout in seconds (default from settings).
    """

    def __init__(
        self,
        polygon_api_key: str | None = None,
        rate_limit: float = 5.0,
        retry_attempts: int | None = None,
        timeout: int | None = None,
    ) -> None:
        # Polygon client
        api_key = polygon_api_key or settings.polygon_api_key.get_secret_value()
        self._polygon = PolygonClient(api_key=api_key)

        # Rate limiter — 5 calls / 60 s = 1/12 tokens per second
        self._limiter = TokenBucketRateLimiter(
            rate=rate_limit / 60.0,
            capacity=int(rate_limit),
        )

        # Retry / timeout from settings
        self._retry_attempts = retry_attempts or settings.collector_retry_attempts
        self._timeout = timeout or settings.collector_timeout
        self._backoff = settings.collector_retry_backoff

        # Redis (lazy)
        self._redis = None

        # Metrics
        self.metrics = CollectorMetrics()

        # Ensure staging dir exists
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

        logger.info(
            "MarketDataCollector initialised  "
            "rate_limit={}/min  retries={}  timeout={}s",
            rate_limit, self._retry_attempts, self._timeout,
        )

    # ── Redis (lazy init) ──────────────────────────────────────

    def _get_redis(self):
        """Lazy-init Redis connection — avoids import-time failures."""
        if self._redis is None:
            try:
                import redis
                self._redis = redis.from_url(
                    settings.redis_url,
                    socket_connect_timeout=5,
                    decode_responses=True,
                )
                self._redis.ping()
                logger.debug("Redis connected for price cache")
            except Exception as exc:
                logger.warning("Redis unavailable for price cache: {}", exc)
                self._redis = False  # sentinel: don't retry
        return self._redis if self._redis is not False else None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  POLYGON — Primary Source
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _fetch_polygon_sync(
        self,
        ticker: str,
        start: str,
        end: str,
        timeframe: str,
    ) -> pd.DataFrame:
        """
        Synchronous Polygon fetch (wrapped by retry decorator).

        Returns a standardised DataFrame or raises on failure.
        """
        multiplier, timespan = _POLYGON_TF_MAP[timeframe]

        aggs = self._polygon.get_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=start,
            to=end,
            adjusted=True,
            sort="asc",
            limit=50_000,
        )

        if not aggs:
            logger.debug(
                "Polygon returned 0 bars  ticker={}  tf={}  range={}/{}",
                ticker, timeframe, start, end,
            )
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        rows: list[dict[str, Any]] = []
        for bar in aggs:
            ts = bar.timestamp  # epoch millis from Polygon
            rows.append({
                "ticker":       ticker,
                "timestamp":    pd.Timestamp(ts, unit="ms", tz="UTC"),
                "open":         bar.open,
                "high":         bar.high,
                "low":          bar.low,
                "close":        bar.close,
                "volume":       bar.volume or 0,
                "vwap":         bar.vwap,
                "transactions": bar.transactions,
                "timeframe":    timeframe,
                "source":       "polygon",
            })

        df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
        return df

    def _make_retry_decorator(self):
        """Build a tenacity retry decorator from current settings."""
        return retry(
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=1,
                max=60,
                exp_base=self._backoff,
            ),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )

    async def _fetch_polygon(
        self,
        ticker: str,
        start: str,
        end: str,
        timeframe: str,
    ) -> pd.DataFrame:
        """Rate-limited, retried async wrapper around Polygon REST."""
        await self._limiter.acquire()

        retried_fn = self._make_retry_decorator()(self._fetch_polygon_sync)

        loop = asyncio.get_event_loop()
        df = await loop.run_in_executor(
            None, retried_fn, ticker, start, end, timeframe,
        )
        return df

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  YFINANCE — Fallback
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _fetch_yfinance_sync(
        self,
        ticker: str,
        start: str,
        end: str,
        timeframe: str,
    ) -> pd.DataFrame:
        """
        Synchronous yfinance fetch.

        yfinance has no rate limit but data quality can be spotty —
        used only when Polygon is exhausted.
        """
        yf_interval = _YF_TF_MAP[timeframe]
        needs_resample = (timeframe == "4h")

        if needs_resample:
            yf_interval = "1h"

        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(
            start=start,
            end=end,
            interval=yf_interval,
            auto_adjust=True,
        )

        if hist.empty:
            logger.debug(
                "yfinance returned 0 bars  ticker={}  tf={}",
                ticker, timeframe,
            )
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        # Resample 1h → 4h if needed
        if needs_resample:
            hist = hist.resample("4h").agg({
                "Open":   "first",
                "High":   "max",
                "Low":    "min",
                "Close":  "last",
                "Volume": "sum",
            }).dropna(subset=["Open"])

        df = pd.DataFrame({
            "ticker":       ticker,
            "timestamp":    hist.index.tz_localize("UTC")
                            if hist.index.tz is None
                            else hist.index.tz_convert("UTC"),
            "open":         hist["Open"].values,
            "high":         hist["High"].values,
            "low":          hist["Low"].values,
            "close":        hist["Close"].values,
            "volume":       hist["Volume"].astype(int).values,
            "vwap":         None,
            "transactions": None,
            "timeframe":    timeframe,
            "source":       "yfinance",
        })

        return df[OUTPUT_COLUMNS]

    async def _fetch_yfinance(
        self,
        ticker: str,
        start: str,
        end: str,
        timeframe: str,
    ) -> pd.DataFrame:
        """Async wrapper around yfinance."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._fetch_yfinance_sync, ticker, start, end, timeframe,
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  Parquet Staging
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _stage_to_parquet(
        self,
        df: pd.DataFrame,
        ticker: str,
        timeframe: str,
    ) -> Path | None:
        """Write raw DataFrame to a partitioned Parquet file."""
        if df.empty:
            return None

        ts_tag = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        out_dir = STAGING_DIR / f"tf={timeframe}" / f"ticker={ticker}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{ticker}_{timeframe}_{ts_tag}.parquet"

        df.to_parquet(path, index=False, engine="pyarrow")
        logger.debug(
            "Staged {} rows -> {}",
            len(df), path.relative_to(PROJECT_ROOT),
        )
        return path

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  PUBLIC API
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def fetch_historical(
        self,
        ticker: str,
        start_date: str | date,
        end_date: str | date,
        timeframe: str = "1d",
        *,
        stage: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch historical OHLCV bars for a single ticker.

        Parameters
        ----------
        ticker : str
            Equity / ETF symbol (e.g. ``"AAPL"``).
        start_date, end_date : str | date
            Date range in ``YYYY-MM-DD`` format or ``date`` objects.
        timeframe : str
            Bar resolution — must be a key in ``_POLYGON_TF_MAP``.
        stage : bool
            If ``True``, write raw data to Parquet staging.

        Returns
        -------
        pd.DataFrame
            Standardised OHLCV DataFrame (empty on total failure).
        """
        start_str = str(start_date)
        end_str = str(end_date)
        logger.info(
            "fetch_historical  ticker={}  tf={}  range={}/{}",
            ticker, timeframe, start_str, end_str,
        )

        df = pd.DataFrame(columns=OUTPUT_COLUMNS)

        # 1. Try Polygon
        try:
            df = await self._fetch_polygon(ticker, start_str, end_str, timeframe)
            if not df.empty:
                logger.info(
                    "Polygon OK  ticker={}  tf={}  rows={}",
                    ticker, timeframe, len(df),
                )
                self.metrics.record_success(ticker, len(df))
                if stage:
                    self._stage_to_parquet(df, ticker, timeframe)
                return df
        except (RetryError, Exception) as exc:
            logger.warning(
                "Polygon FAILED after retries  ticker={}  tf={}  err={}",
                ticker, timeframe, exc,
            )

        # 2. Fallback to yfinance
        try:
            df = await self._fetch_yfinance(ticker, start_str, end_str, timeframe)
            if not df.empty:
                logger.info(
                    "yfinance fallback OK  ticker={}  tf={}  rows={}",
                    ticker, timeframe, len(df),
                )
                self.metrics.record_success(ticker, len(df))
                if stage:
                    self._stage_to_parquet(df, ticker, timeframe)
                return df
        except Exception as exc:
            logger.error(
                "yfinance ALSO FAILED  ticker={}  tf={}  err={}",
                ticker, timeframe, exc,
            )

        # 3. Total failure
        self.metrics.record_failure(ticker)
        logger.error(
            "ALL SOURCES FAILED  ticker={}  tf={}  returning empty DataFrame",
            ticker, timeframe,
        )
        return df

    async def fetch_latest(
        self,
        ticker: str,
        timeframe: str = "1min",
        n_bars: int = 100,
        *,
        stage: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch the most recent *n_bars* for incremental updates.

        Uses a look-back window based on timeframe to compute
        the ``start_date`` automatically.
        """
        # Compute look-back
        lookback_map: dict[str, timedelta] = {
            "1min":  timedelta(hours=n_bars / 60 + 1),
            "5min":  timedelta(hours=n_bars * 5 / 60 + 1),
            "15min": timedelta(hours=n_bars * 15 / 60 + 2),
            "1h":    timedelta(days=n_bars / 24 + 1),
            "4h":    timedelta(days=n_bars * 4 / 24 + 1),
            "1d":    timedelta(days=n_bars + 5),   # +weekends
            "1w":    timedelta(weeks=n_bars + 1),
        }
        lookback = lookback_map.get(timeframe, timedelta(days=n_bars))

        end = date.today()
        start = end - lookback

        logger.info(
            "fetch_latest  ticker={}  tf={}  n_bars={}  lookback={}d",
            ticker, timeframe, n_bars, lookback.days,
        )

        df = await self.fetch_historical(
            ticker, start, end, timeframe, stage=stage,
        )

        # Trim to last N bars
        if len(df) > n_bars:
            df = df.tail(n_bars).reset_index(drop=True)

        return df

    async def fetch_all_tickers(
        self,
        timeframe: str = "1d",
        *,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        n_bars: int = 100,
        stage: bool = True,
        max_concurrency: int = 3,
    ) -> dict[str, pd.DataFrame]:
        """
        Fetch OHLCV for **all configured tickers** in parallel.

        Parameters
        ----------
        timeframe : str
            Bar resolution.
        start_date, end_date : optional
            If both given, uses ``fetch_historical``; otherwise
            ``fetch_latest`` with ``n_bars``.
        n_bars : int
            Number of recent bars when doing incremental.
        stage : bool
            Write raw Parquet files.
        max_concurrency : int
            Semaphore limit for parallel requests (be kind to APIs).

        Returns
        -------
        dict[str, pd.DataFrame]
            ``{ticker: DataFrame}`` — empty DataFrames for failures.
        """
        tickers = settings.tickers
        semaphore = asyncio.Semaphore(max_concurrency)

        async def _guarded_fetch(t: str) -> tuple[str, pd.DataFrame]:
            async with semaphore:
                if start_date and end_date:
                    df = await self.fetch_historical(
                        t, start_date, end_date, timeframe, stage=stage,
                    )
                else:
                    df = await self.fetch_latest(
                        t, timeframe, n_bars=n_bars, stage=stage,
                    )
                return t, df

        logger.info(
            "fetch_all_tickers  tf={}  tickers={}  concurrency={}",
            timeframe, len(tickers), max_concurrency,
        )

        tasks = [_guarded_fetch(t) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        result_dict = {t: df for t, df in results}

        # Log summary
        total_rows = sum(len(df) for df in result_dict.values())
        successes = sum(1 for df in result_dict.values() if not df.empty)
        logger.info(
            "fetch_all_tickers DONE  "
            "ok={}/{}  total_rows={}  tf={}",
            successes, len(tickers), total_rows, timeframe,
        )
        return result_dict

    async def get_current_price(self, ticker: str) -> float | None:
        """
        Return the latest closing price for *ticker*.

        Resolution order:
        1. Redis cache (key: ``price:{ticker}``, TTL 60 s)
        2. Polygon last-trade snapshot
        3. yfinance ``fast_info["lastPrice"]``

        Returns ``None`` if all sources fail.
        """
        cache_key = f"price:{ticker}"

        # 1. Redis cache
        r = self._get_redis()
        if r is not None:
            try:
                cached = r.get(cache_key)
                if cached is not None:
                    price = float(cached)
                    logger.debug("Cache HIT  {}={}", ticker, price)
                    return price
            except Exception:
                pass

        price: float | None = None

        # 2. Polygon snapshot
        try:
            await self._limiter.acquire()
            loop = asyncio.get_event_loop()
            snapshot = await loop.run_in_executor(
                None,
                lambda: self._polygon.get_snapshot_ticker("stocks", ticker),
            )
            if snapshot and snapshot.day:
                price = snapshot.day.close
        except Exception as exc:
            logger.debug("Polygon snapshot failed for {}: {}", ticker, exc)

        # 3. yfinance fallback
        if price is None:
            try:
                loop = asyncio.get_event_loop()
                yf_ticker = await loop.run_in_executor(
                    None, lambda: yf.Ticker(ticker),
                )
                info = await loop.run_in_executor(
                    None, lambda: yf_ticker.fast_info,
                )
                price = float(info["lastPrice"])
            except Exception as exc:
                logger.warning(
                    "yfinance price failed for {}: {}", ticker, exc,
                )

        # Write back to Redis
        if price is not None and r is not None:
            try:
                r.setex(cache_key, 60, str(price))
                logger.debug("Cache SET  {}={}", ticker, price)
            except Exception:
                pass

        return price

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  Convenience helpers
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def print_metrics(self) -> None:
        """Pretty-print per-ticker success / failure counts."""
        m = self.metrics.summary()
        logger.info("Collector Metrics:\n{}", json.dumps(m, indent=2))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Module-level singleton
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

collector = MarketDataCollector()
"""
Pre-configured singleton — import this everywhere::

    from data_ingestion.collectors.market_data_collector import collector
"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CLI quick-test
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _main() -> None:
    """Smoke-test: fetch 5 days of daily bars for all tickers."""
    end = date.today()
    start = end - timedelta(days=5)
    results = await collector.fetch_all_tickers(
        "1d", start_date=start, end_date=end,
    )
    for ticker, df in results.items():
        print(f"{ticker}: {len(df)} bars")
    collector.print_metrics()


if __name__ == "__main__":
    asyncio.run(_main())
