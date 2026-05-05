"""
Trading System — Storage Manager
==================================

Handles all final writes to databases (TimescaleDB, PostgreSQL, Redis, ChromaDB).
Implements connection pooling, robust retry mechanisms, caching, and pub/sub 
event distribution to notify downstream services.
"""

from __future__ import annotations

import json
import os

# Disable ChromaDB telemetry before it initializes
os.environ["ANONYMIZED_TELEMETRY"] = "False"
os.environ["CHROMA_TELEMETRY"] = "False"

import logging
logging.getLogger("chromadb.telemetry.product.posthog").setLevel(logging.CRITICAL)

from dataclasses import dataclass
from typing import Any, Type

import chromadb
from chromadb.config import Settings as ChromaSettings
import numpy as np
import pandas as pd
import redis
from loguru import logger
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase

from config.settings import settings
from config.llm_config import embeddings
from data_ingestion.storage.init_db import get_db_manager
from data_ingestion.storage.models import (
    BalanceSheet,
    Company,
    IncomeStatement,
    MacroSeries,
    NewsArticle,
    OhlcvBar,
    RawTick,
)


@dataclass
class WriteResult:
    attempted: int
    inserted: int
    skipped: int
    errors: int


class StorageManager:
    """Central gateway for persisting data and publishing events."""

    def __init__(self) -> None:
        self.db_manager = get_db_manager()
        
        # Redis connection
        try:
            self.redis = redis.from_url(settings.redis_url, decode_responses=True)
        except Exception as e:
            logger.error("Failed to connect to Redis: {}", e)
            self.redis = None

        # ChromaDB setup
        try:
            self.chroma_client = chromadb.PersistentClient(
                path=settings.chroma_path,
                settings=ChromaSettings(anonymized_telemetry=False)
            )
            self.news_collection = self.chroma_client.get_or_create_collection("news_articles")
            self.sec_collection = self.chroma_client.get_or_create_collection("sec_filings")
        except Exception as e:
            logger.error("Failed to initialize ChromaDB: {}", e)
            self.chroma_client = None


    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 1 - TIMESCALEDB WRITER
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _batch_insert_do_nothing(
        self, df: pd.DataFrame, model_class: Type[DeclarativeBase], batch_size: int = 1000
    ) -> WriteResult:
        """Helper to batch insert into TimescaleDB and ignore conflicts."""
        if df.empty:
            return WriteResult(0, 0, 0, 0)

        # Ensure we don't have duplicates in the DataFrame itself, 
        # as ON CONFLICT won't help with intra-batch duplicates.
        if model_class.__tablename__ == "ohlcv_bars":
            df = df.drop_duplicates(subset=["ticker", "timestamp", "timeframe"])
        if model_class.__tablename__ == "raw_ticks":
            df = df.drop_duplicates(subset=["ticker", "timestamp"])

        # Replace all NaN/NaT/inf values with None (SQL NULL)
        # We do this on the records list for maximum reliability with SQLAlchemy
        records = df.to_dict(orient="records")
        for record in records:
            for key, value in record.items():
                if pd.isnull(value) or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                    record[key] = None
                elif key in ["volume", "transactions"] and value is not None:
                    try:
                        record[key] = int(round(float(value)))
                    except (ValueError, TypeError):
                        record[key] = None
        
        attempted = len(records)
        inserted = 0
        skipped = 0
        errors = 0

        # Create the insert statement
        stmt = insert(model_class)
        
        # For TimescaleDB hypertables, we MUST be explicit about index elements
        # for ON CONFLICT DO NOTHING to work across chunks.
        if model_class.__tablename__ == "ohlcv_bars":
            on_conflict_stmt = stmt.on_conflict_do_nothing(
                index_elements=["ticker", "timestamp", "timeframe"]
            )
        elif model_class.__tablename__ == "raw_ticks":
            on_conflict_stmt = stmt.on_conflict_do_nothing(
                index_elements=["ticker", "timestamp"]
            )
        else:
            on_conflict_stmt = stmt.on_conflict_do_nothing()

        with self.db_manager.timescale_session() as session:
            for i in range(0, attempted, batch_size):
                batch = records[i : i + batch_size]
                try:
                    result = session.execute(on_conflict_stmt, batch)
                    # result.rowcount might not be available on all result types in SQLAlchemy 2.0
                    batch_inserted = getattr(result, "rowcount", len(batch))
                    if batch_inserted is None: # Sometimes it returns None
                        batch_inserted = len(batch)
                    
                    inserted += batch_inserted
                    skipped += (len(batch) - batch_inserted)
                except Exception as exc:
                    import traceback
                    logger.error("Batch insert failed for {}: {}\n{}", model_class.__name__, exc, traceback.format_exc())
                    errors += len(batch)
                    session.rollback()  # rollback the transaction for this batch

        logger.info(
            "Write {} -> Attempted: {}, Inserted: {}, Skipped: {}, Errors: {}",
            model_class.__tablename__, attempted, inserted, skipped, errors
        )
        return WriteResult(attempted, inserted, skipped, errors)

    def write_price_bars(self, df: pd.DataFrame) -> WriteResult:
        """Write OHLCV bars to TimescaleDB."""
        return self._batch_insert_do_nothing(df, OhlcvBar)

    def write_ticks(self, df: pd.DataFrame) -> WriteResult:
        """Write raw tick data to TimescaleDB."""
        return self._batch_insert_do_nothing(df, RawTick)


    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 2 - POSTGRESQL WRITER
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _upsert_postgres(
        self, df: pd.DataFrame, model_class: Type[DeclarativeBase], index_elements: list[str]
    ) -> WriteResult:
        """Helper to batch upsert into PostgreSQL."""
        if df.empty:
            return WriteResult(0, 0, 0, 0)

        records = df.replace({pd.NaT: None}).to_dict(orient="records")
        attempted = len(records)
        inserted = 0
        errors = 0

        with self.db_manager.postgres_session() as session:
            for record in records:
                try:
                    stmt = insert(model_class).values(**record)
                    # Exclude primary keys and index elements from update
                    update_dict = {c.name: c for c in stmt.excluded if c.name not in index_elements and c.name != "id"}
                    
                    if update_dict:
                        on_conflict_stmt = stmt.on_conflict_do_update(
                            index_elements=index_elements,
                            set_=update_dict
                        )
                    else:
                        on_conflict_stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)

                    session.execute(on_conflict_stmt)
                    inserted += 1
                except Exception as exc:
                    logger.error("Upsert failed for {}: {}", model_class.__name__, exc)
                    errors += 1
                    session.rollback()

        logger.info(
            "Upsert {} -> Attempted: {}, Success: {}, Errors: {}",
            model_class.__tablename__, attempted, inserted, errors
        )
        return WriteResult(attempted, inserted, 0, errors)

    def write_fundamentals(self, df: pd.DataFrame, table_name: str) -> WriteResult:
        """Write fundamental statements or company profiles."""
        if table_name == "companies":
            return self._upsert_postgres(df, Company, ["ticker"])
        elif table_name == "income_statements":
            return self._upsert_postgres(df, IncomeStatement, ["ticker", "fiscal_date", "period_type"])
        elif table_name == "balance_sheets":
            return self._upsert_postgres(df, BalanceSheet, ["ticker", "fiscal_date", "period_type"])
        else:
            logger.error("Unknown fundamentals table: {}", table_name)
            return WriteResult(len(df), 0, 0, len(df))

    def write_news(self, df: pd.DataFrame) -> WriteResult:
        """Write news articles, deduplicate on URL, and publish events."""
        result = self._upsert_postgres(df, NewsArticle, ["url"])
        
        # Also embed and store in ChromaDB for semantic search
        if not df.empty:
            articles_list = df.to_dict(orient="records")
            self.embed_and_store_news(articles_list)

        # After successful write, publish to Redis
        if self.redis and not df.empty:
            for _, row in df.iterrows():
                try:
                    msg = {
                        "tickers": row.get("tickers", []),
                        "headline": row.get("headline", ""),
                        "source": row.get("source", ""),
                        "url": row.get("url", "")
                    }
                    self.redis.publish("data.news.new", json.dumps(msg))
                except Exception as e:
                    logger.warning("Failed to publish news event: {}", e)

        return result

    def write_macro(self, df: pd.DataFrame) -> WriteResult:
        """Write macroeconomic series."""
        return self._upsert_postgres(df, MacroSeries, ["series_id", "observation_date"])


    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 3 - REDIS CACHE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def cache_latest_prices(self, ticker_prices: dict[str, dict[str, Any]]) -> None:
        """
        Cache latest price snapshots to Redis.
        Key: 'price:latest:{ticker}'
        Value: JSON {close, timestamp, volume}
        TTL: 65 seconds
        """
        if not self.redis:
            return

        pipeline = self.redis.pipeline()
        count = 0
        for ticker, data in ticker_prices.items():
            key = f"price:latest:{ticker}"
            pipeline.setex(key, 65, json.dumps(data, default=str))
            count += 1
            
        try:
            pipeline.execute()
            logger.debug("Cached latest prices for {} tickers", count)
        except Exception as e:
            logger.warning("Failed to cache prices to Redis: {}", e)

    def get_latest_price(self, ticker: str) -> float | None:
        """
        Read from Redis first, fallback to DB if cache miss.
        """
        # 1. Try Redis
        if self.redis:
            try:
                val = self.redis.get(f"price:latest:{ticker}")
                if val:
                    data = json.loads(val)
                    logger.debug("Cache hit for {}", ticker)
                    return float(data.get("close", 0.0))
            except Exception as e:
                logger.warning("Redis cache read failed for {}: {}", ticker, e)

        logger.debug("Cache miss for {}, falling back to DB", ticker)
        
        # 2. Fallback to TimescaleDB
        try:
            with self.db_manager.timescale_session() as session:
                result = session.execute(
                    text("SELECT close FROM ohlcv_bars WHERE ticker = :t ORDER BY timestamp DESC LIMIT 1"),
                    {"t": ticker}
                ).scalar()
                return float(result) if result is not None else None
        except Exception as e:
            logger.error("DB fallback failed for {}: {}", ticker, e)
            return None

    def cache_sentiment_score(self, ticker: str, score: float, ttl: int = 3600) -> None:
        """Cache the latest sentiment score for a ticker."""
        if self.redis:
            try:
                self.redis.setex(f"sentiment:{ticker}", ttl, str(score))
            except Exception as e:
                logger.warning("Failed to cache sentiment for {}: {}", ticker, e)


    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 4 - REDIS PUB/SUB EVENTS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def publish_event(self, channel: str, payload: dict[str, Any]) -> None:
        """Publish a JSON payload to a Redis channel."""
        if self.redis:
            try:
                self.redis.publish(channel, json.dumps(payload, default=str))
            except Exception as e:
                logger.error("Failed to publish event to {}: {}", channel, e)

    def publish_prices_updated(self, tickers: list[str], timeframe: str, timestamp: str) -> None:
        self.publish_event("data.prices.updated", {
            "tickers": tickers,
            "timeframe": timeframe,
            "timestamp": timestamp
        })

    def publish_fundamentals_updated(self, ticker: str, period: str) -> None:
        self.publish_event("data.fundamentals.updated", {
            "ticker": ticker,
            "period": period
        })


    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SECTION 5 - CHROMADB WRITER
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def embed_and_store_news(self, articles: list[dict[str, Any]]) -> None:
        """
        Embed news using OpenAI text-embedding-3-small and store in ChromaDB.
        Skips articles if their URL already exists.
        """
        if not self.chroma_client or not articles:
            return

        # Simple deduplication against ChromaDB
        try:
            existing = self.news_collection.get(include=["metadatas"])
            existing_urls = {meta.get("url") for meta in existing["metadatas"] if meta}
        except Exception as e:
            logger.warning("Failed to read ChromaDB for existing URLs: {}", e)
            existing_urls = set()

        to_embed = [a for a in articles if a.get("url") not in existing_urls and a.get("url")]
        if not to_embed:
            return

        texts = [a.get("headline", "") + " " + a.get("summary", "") for a in to_embed]
        
        try:
            # Batch embedding via NVIDIA NIM (LangChain wrapper)
            vector_embeddings = embeddings.embed_documents(texts)
            
            # Prepare for ChromaDB
            ids = [a.get("url") for a in to_embed]
            metadatas = []
            for a in to_embed:
                # ChromaDB metadatas cannot contain None or complex objects
                meta = {
                    "ticker": ",".join(a.get("tickers", [])) if a.get("tickers") else "GENERAL",
                    "source": a.get("source", "unknown"),
                    "published_at": str(a.get("published_at", "")),
                    "url": a.get("url")
                }
                metadatas.append(meta)

            self.news_collection.add(
                ids=ids,
                embeddings=vector_embeddings,
                metadatas=metadatas,
                documents=texts
            )
            logger.info("Embedded and stored {} news articles in ChromaDB.", len(to_embed))
        except Exception as e:
            logger.error("Failed to embed/store news: {}", e)

    def embed_and_store_filing(self, filing: dict[str, Any]) -> None:
        """
        Chunk and embed long SEC filings, store in ChromaDB.
        """
        if not self.chroma_client or not filing:
            return

        content = filing.get("raw_content", "")
        if not content:
            return

        url = filing.get("url", "unknown_url")
        
        # Simple character-based chunking (approximation of 1000 tokens ~ 4000 chars)
        # Using 4000 chars with 800 char overlap (200 tokens)
        chunk_size = 4000
        overlap = 800
        chunks = []
        start = 0
        while start < len(content):
            end = start + chunk_size
            chunks.append(content[start:end])
            start = end - overlap

        try:
            # Embed chunks via NVIDIA NIM (LangChain wrapper)
            vector_embeddings = embeddings.embed_documents(chunks)
            
            ids = [f"{url}#chunk{i}" for i in range(len(chunks))]
            metadatas = [{
                "ticker": filing.get("ticker", "UNKNOWN"),
                "form_type": filing.get("form_type", "UNKNOWN"),
                "filed_at": str(filing.get("filed_at", "")),
                "url": url,
                "chunk_index": i
            } for i in range(len(chunks))]

            self.sec_collection.add(
                ids=ids,
                embeddings=vector_embeddings,
                metadatas=metadatas,
                documents=chunks
            )
            logger.info("Stored {} chunks for SEC filing {} in ChromaDB.", len(chunks), filing.get("ticker"))
        except Exception as e:
            logger.error("Failed to embed/store SEC filing: {}", e)
