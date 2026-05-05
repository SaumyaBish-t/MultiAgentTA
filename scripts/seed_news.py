"""
News Data Seeder
================
Fetches recent news articles from NewsAPI.org and persists them
into the PostgreSQL `news_articles` table via StorageManager.

Usage:
    python scripts/seed_news.py                   # default tickers from config
    python scripts/seed_news.py AAPL MSFT TSLA    # specific tickers
"""

import sys
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
from loguru import logger

from config.settings import settings
from data_ingestion.storage.storage_manager import StorageManager


NEWS_API_BASE = "https://newsapi.org/v2/everything"


def fetch_news_for_ticker(ticker: str, api_key: str, days: int = 7) -> list[dict]:
    """Fetch articles from NewsAPI for a single ticker."""
    from_date = (datetime.now(tz=timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    to_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    params = {
        "q": ticker,
        "from": from_date,
        "to": to_date,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 50,
        "apiKey": api_key,
    }

    try:
        resp = requests.get(NEWS_API_BASE, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") != "ok":
            logger.warning("NewsAPI returned status '{}' for {}", data.get("status"), ticker)
            return []

        articles = data.get("articles", [])
        logger.info("NewsAPI returned {} articles for {}", len(articles), ticker)
        return articles

    except requests.RequestException as e:
        logger.error("NewsAPI request failed for {}: {}", ticker, e)
        return []


def normalize_articles(raw_articles: list[dict], ticker: str) -> pd.DataFrame:
    """Convert raw NewsAPI response into a DataFrame matching the NewsArticle model."""
    rows = []
    for art in raw_articles:
        # Skip articles with "[Removed]" placeholder content
        if art.get("title") == "[Removed]" or not art.get("url"):
            continue

        published = art.get("publishedAt")
        if published:
            try:
                pub_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            except ValueError:
                pub_dt = datetime.now(tz=timezone.utc)
        else:
            pub_dt = datetime.now(tz=timezone.utc)

        rows.append({
            "tickers": [ticker.upper()],
            "headline": (art.get("title") or "")[:500],
            "summary": (art.get("description") or "")[:2000],
            "source": (art.get("source", {}).get("name") or "unknown")[:50],
            "url": art.get("url", "")[:500],
            "published_at": pub_dt,
            "sentiment_score": None,  # Will be populated by the SentimentAgent later
        })

    return pd.DataFrame(rows)


def run(tickers: list[str]):
    api_key = None
    if hasattr(settings, "news_api_key") and settings.news_api_key:
        key = settings.news_api_key
        # Handle Pydantic SecretStr
        api_key = key.get_secret_value() if hasattr(key, "get_secret_value") else str(key)

    # Fallback: read directly from env
    if not api_key:
        import os
        api_key = os.getenv("NEWS_API_KEY")

    if not api_key:
        logger.error("NEWS_API_KEY not set. Cannot seed news data.")
        return

    storage = StorageManager()
    total_inserted = 0

    for ticker in tickers:
        logger.info("Fetching news for {}...", ticker)
        raw = fetch_news_for_ticker(ticker, api_key, days=7)

        if not raw:
            logger.warning("No articles returned for {}", ticker)
            continue

        df = normalize_articles(raw, ticker)
        if df.empty:
            logger.warning("All articles filtered out for {}", ticker)
            continue

        logger.info("Writing {} articles for {} to PostgreSQL...", len(df), ticker)
        result = storage.write_news(df)
        total_inserted += result.inserted
        logger.info("✅ {} — inserted: {}, errors: {}", ticker, result.inserted, result.errors)

    logger.info("═══ News seeding complete. Total articles inserted: {} ═══", total_inserted)


if __name__ == "__main__":
    args = sys.argv[1:]
    tickers = [arg.upper() for arg in args] if args else (settings.tickers if hasattr(settings, "tickers") else ["AAPL", "MSFT", "TSLA"])
    run(tickers)
