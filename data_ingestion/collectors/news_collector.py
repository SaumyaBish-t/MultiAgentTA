"""
Trading System — News Collector
================================

Collects market/ticker news from **Alpaca News API** (primary),
**NewsAPI** (secondary), and **SEC EDGAR RSS** (8-K filings).

Schedule:
  - Every 5 min during market hours (13:30–20:00 UTC)
  - Every 30 min outside market hours

Deduplication via SHA-256 hash of (headline + source + date).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from loguru import logger
from tenacity import (
    retry, retry_if_exception_type,
    stop_after_attempt, wait_exponential,
)

from config.settings import PROJECT_ROOT, settings

STAGING_DIR: Path = PROJECT_ROOT / "data_ingestion" / "storage" / "staging"

ALPACA_NEWS_URL = "https://data.alpaca.markets/v1beta1/news"
NEWSAPI_URL = "https://newsapi.org/v2/everything"
SEC_RSS_URL = "https://efts.sec.gov/LATEST/search-index?q={query}&dateRange=custom&startdt={start}&enddt={end}&forms=8-K"
SEC_FULL_TEXT_URL = "https://efts.sec.gov/LATEST/search-index"

NEWS_COLUMNS: list[str] = [
    "hash_id", "tickers", "headline", "summary",
    "url", "source", "published_at", "raw_content",
]


@dataclass
class NewsMetrics:
    fetched: dict[str, int] = field(default_factory=dict)
    duplicates_skipped: int = 0

    def record(self, source: str, count: int) -> None:
        self.fetched[source] = self.fetched.get(source, 0) + count

    def summary(self) -> dict[str, Any]:
        return {"fetched": dict(self.fetched), "duplicates_skipped": self.duplicates_skipped}


def _hash_article(headline: str, source: str, pub_date: str) -> str:
    """SHA-256 hash for deduplication."""
    raw = f"{headline.strip().lower()}|{source.lower()}|{pub_date[:10]}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _parse_utc(raw: str | None) -> datetime | None:
    if not raw: return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S+00:00"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _stage_parquet(df: pd.DataFrame, tag: str) -> Path | None:
    if df.empty: return None
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    out_dir = STAGING_DIR / "type=news"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{tag}_{ts}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.debug("Staged {} rows -> {}", len(df), path.name)
    return path


class NewsCollector:
    """
    Multi-source news collector with deduplication.

    Sources: Alpaca News API, NewsAPI.org, SEC EDGAR RSS.
    """

    def __init__(
        self,
        alpaca_key: str | None = None,
        alpaca_secret: str | None = None,
        newsapi_key: str | None = None,
        retry_attempts: int | None = None,
        timeout: int | None = None,
    ) -> None:
        self._alpaca_key = alpaca_key or settings.alpaca_api_key.get_secret_value()
        self._alpaca_secret = alpaca_secret or settings.alpaca_secret_key.get_secret_value()
        self._newsapi_key = newsapi_key or settings.news_api_key.get_secret_value()
        self._retry_attempts = retry_attempts or settings.collector_retry_attempts
        self._timeout = timeout or settings.collector_timeout
        self._backoff = settings.collector_retry_backoff
        self.metrics = NewsMetrics()
        self._seen_hashes: set[str] = set()
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("NewsCollector initialised  retries={}  timeout={}s",
                     self._retry_attempts, self._timeout)

    def _is_duplicate(self, h: str) -> bool:
        if h in self._seen_hashes:
            self.metrics.duplicates_skipped += 1
            return True
        self._seen_hashes.add(h)
        return False

    # ── Alpaca News ─────────────────────────────────────────────

    async def _fetch_alpaca_news(
        self, symbols: str | None = None, hours_back: int = 24, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch from Alpaca News API with retry."""
        start = (datetime.now(tz=timezone.utc) - timedelta(hours=hours_back)).isoformat()
        params: dict[str, Any] = {"start": start, "limit": limit, "sort": "desc"}
        if symbols:
            params["symbols"] = symbols

        @retry(
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=30, exp_base=self._backoff),
            retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
            reraise=True,
        )
        async def _do() -> list[dict[str, Any]]:
            headers = {
                "APCA-API-KEY-ID": self._alpaca_key,
                "APCA-API-SECRET-KEY": self._alpaca_secret,
            }
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(ALPACA_NEWS_URL, params=params, headers=headers)
                resp.raise_for_status()
                return resp.json().get("news", [])

        return await _do()

    # ── NewsAPI ─────────────────────────────────────────────────

    async def _fetch_newsapi(
        self, query: str, hours_back: int = 1, page_size: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch from NewsAPI.org with retry."""
        from_dt = (datetime.now(tz=timezone.utc) - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%S")
        params = {
            "q": query, "from": from_dt, "sortBy": "publishedAt",
            "pageSize": page_size, "language": "en", "apiKey": self._newsapi_key,
        }

        @retry(
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=30, exp_base=self._backoff),
            retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
            reraise=True,
        )
        async def _do() -> list[dict[str, Any]]:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(NEWSAPI_URL, params=params)
                resp.raise_for_status()
                return resp.json().get("articles", [])

        return await _do()

    # ── SEC EDGAR RSS ───────────────────────────────────────────

    async def _fetch_sec_filings(
        self, ticker: str, form_type: str = "8-K",
    ) -> list[dict[str, Any]]:
        """Fetch recent SEC filings via EDGAR full-text search."""
        params = {
            "q": f'"{ticker}"',
            "dateRange": "custom",
            "startdt": (datetime.now(tz=timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d"),
            "enddt": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
            "forms": form_type,
        }
        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                headers={"User-Agent": "TradingSystem/1.0 (contact@example.com)"},
            ) as client:
                resp = await client.get(SEC_FULL_TEXT_URL, params=params)
                resp.raise_for_status()
                data = resp.json()
                return data.get("hits", {}).get("hits", [])
        except Exception as exc:
            logger.debug("SEC EDGAR fetch failed for {}: {}", ticker, exc)
            return []

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  PUBLIC API
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def fetch_ticker_news(
        self, ticker: str, hours_back: int = 24,
    ) -> pd.DataFrame:
        """Fetch news mentioning a specific ticker. Alpaca primary, NewsAPI fallback."""
        logger.info("fetch_ticker_news  ticker={}  hours_back={}", ticker, hours_back)
        articles: list[dict[str, Any]] = []

        # 1. Alpaca
        try:
            raw = await self._fetch_alpaca_news(symbols=ticker, hours_back=hours_back)
            for a in raw:
                pub = _parse_utc(a.get("created_at")) or datetime.now(tz=timezone.utc)
                h = _hash_article(a.get("headline", ""), "alpaca", pub.isoformat())
                if self._is_duplicate(h): continue
                articles.append({
                    "hash_id": h,
                    "tickers": [s.strip() for s in (a.get("symbols") or [])],
                    "headline": a.get("headline", ""),
                    "summary": a.get("summary", ""),
                    "url": a.get("url", ""),
                    "source": "alpaca",
                    "published_at": pub,
                    "raw_content": a.get("content", ""),
                })
            self.metrics.record("alpaca", len(articles))
        except Exception as exc:
            logger.warning("Alpaca news FAILED for {}: {}", ticker, exc)

        # 2. NewsAPI fallback
        try:
            raw = await self._fetch_newsapi(query=ticker, hours_back=hours_back)
            count = 0
            for a in raw:
                pub = _parse_utc(a.get("publishedAt")) or datetime.now(tz=timezone.utc)
                h = _hash_article(a.get("title", ""), "newsapi", pub.isoformat())
                if self._is_duplicate(h): continue
                articles.append({
                    "hash_id": h,
                    "tickers": [ticker],
                    "headline": a.get("title", ""),
                    "summary": a.get("description", ""),
                    "url": a.get("url", ""),
                    "source": "newsapi",
                    "published_at": pub,
                    "raw_content": a.get("content", ""),
                })
                count += 1
            self.metrics.record("newsapi", count)
        except Exception as exc:
            logger.warning("NewsAPI FAILED for {}: {}", ticker, exc)

        df = pd.DataFrame(articles, columns=NEWS_COLUMNS) if articles else pd.DataFrame(columns=NEWS_COLUMNS)
        _stage_parquet(df, f"news_{ticker}")
        logger.info("fetch_ticker_news DONE  ticker={}  rows={}", ticker, len(df))
        return df

    async def fetch_market_news(self, hours_back: int = 1) -> pd.DataFrame:
        """Fetch general market / macro news."""
        logger.info("fetch_market_news  hours_back={}", hours_back)
        articles: list[dict[str, Any]] = []

        # Alpaca — no symbol filter = market-wide
        try:
            raw = await self._fetch_alpaca_news(symbols=None, hours_back=hours_back)
            for a in raw:
                pub = _parse_utc(a.get("created_at")) or datetime.now(tz=timezone.utc)
                h = _hash_article(a.get("headline", ""), "alpaca", pub.isoformat())
                if self._is_duplicate(h): continue
                articles.append({
                    "hash_id": h,
                    "tickers": [s.strip() for s in (a.get("symbols") or [])],
                    "headline": a.get("headline", ""),
                    "summary": a.get("summary", ""),
                    "url": a.get("url", ""),
                    "source": "alpaca",
                    "published_at": pub,
                    "raw_content": a.get("content", ""),
                })
            self.metrics.record("alpaca_market", len(articles))
        except Exception as exc:
            logger.warning("Alpaca market news FAILED: {}", exc)

        # NewsAPI — broad market query
        try:
            raw = await self._fetch_newsapi(
                query="stock market OR S&P 500 OR Federal Reserve OR earnings",
                hours_back=hours_back,
            )
            count = 0
            for a in raw:
                pub = _parse_utc(a.get("publishedAt")) or datetime.now(tz=timezone.utc)
                h = _hash_article(a.get("title", ""), "newsapi", pub.isoformat())
                if self._is_duplicate(h): continue
                articles.append({
                    "hash_id": h, "tickers": [],
                    "headline": a.get("title", ""),
                    "summary": a.get("description", ""),
                    "url": a.get("url", ""),
                    "source": "newsapi",
                    "published_at": pub,
                    "raw_content": a.get("content", ""),
                })
                count += 1
            self.metrics.record("newsapi_market", count)
        except Exception as exc:
            logger.warning("NewsAPI market FAILED: {}", exc)

        df = pd.DataFrame(articles, columns=NEWS_COLUMNS) if articles else pd.DataFrame(columns=NEWS_COLUMNS)
        _stage_parquet(df, "news_market")
        logger.info("fetch_market_news DONE  rows={}", len(df))
        return df

    async def fetch_sec_filings(
        self, ticker: str, form_type: str = "8-K",
    ) -> pd.DataFrame:
        """Fetch latest SEC filings (default 8-K) for a ticker."""
        logger.info("fetch_sec_filings  ticker={}  form={}", ticker, form_type)
        articles: list[dict[str, Any]] = []
        try:
            hits = await self._fetch_sec_filings(ticker, form_type)
            for hit in hits:
                src = hit.get("_source", {})
                pub = _parse_utc(src.get("file_date")) or datetime.now(tz=timezone.utc)
                headline = f"SEC {form_type}: {src.get('display_names', [''])[0] if src.get('display_names') else ticker}"
                h = _hash_article(headline, "sec_edgar", pub.isoformat())
                if self._is_duplicate(h): continue
                articles.append({
                    "hash_id": h, "tickers": [ticker],
                    "headline": headline,
                    "summary": src.get("file_description", ""),
                    "url": f"https://www.sec.gov/Archives/edgar/data/{src.get('file_num', '')}",
                    "source": "sec_edgar",
                    "published_at": pub,
                    "raw_content": json.dumps(src),
                })
            self.metrics.record("sec_edgar", len(articles))
        except Exception as exc:
            logger.warning("SEC filings FAILED for {}: {}", ticker, exc)

        df = pd.DataFrame(articles, columns=NEWS_COLUMNS) if articles else pd.DataFrame(columns=NEWS_COLUMNS)
        _stage_parquet(df, f"sec_{ticker}")
        logger.info("fetch_sec_filings DONE  ticker={}  rows={}", ticker, len(df))
        return df

    def get_fetch_interval(self) -> int:
        """Return appropriate interval based on market hours."""
        now = datetime.now(tz=timezone.utc).time()
        if settings.market_open_utc <= now <= settings.market_close_utc:
            return 300   # 5 min during market hours
        return 1800      # 30 min outside

    def print_metrics(self) -> None:
        logger.info("NewsCollector Metrics:\n{}", json.dumps(self.metrics.summary(), indent=2))


news_collector = NewsCollector()


async def _main() -> None:
    df = await news_collector.fetch_ticker_news("AAPL", hours_back=24)
    print(f"AAPL news: {len(df)} articles")
    if not df.empty:
        print(df[["headline", "source", "published_at"]].head().to_string())
    news_collector.print_metrics()

if __name__ == "__main__":
    asyncio.run(_main())
