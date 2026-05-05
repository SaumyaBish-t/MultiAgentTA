from datetime import datetime, timedelta, timezone
from typing import List

import chromadb
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from config.settings import settings
from data_ingestion.api.cache import cache_response
from data_ingestion.api.dependencies import get_postgres_db
from data_ingestion.api.schemas import (
    NewsArticleResponse,
    NewsSearchRequest,
    NewsSearchResponse,
)
from data_ingestion.storage.models import NewsArticle

router = APIRouter(prefix="/news", tags=["News"])


@router.get("/market", response_model=List[NewsArticleResponse])
@cache_response(ttl_seconds=120)  # 2 minutes
async def get_market_news(
    request: Request,
    hours: int = Query(4, ge=1, le=72),
    db: Session = Depends(get_postgres_db)
):
    """Get general market news."""
    start_time = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    
    # We define market news as articles with either no specific tickers or tagged 'GENERAL'
    # For simplicity, we just order by published_at DESC.
    query = select(NewsArticle).where(
        NewsArticle.published_at >= start_time
    ).order_by(NewsArticle.published_at.desc()).limit(50)
    
    results = db.execute(query).scalars().all()
    return results


@router.get("/{ticker}", response_model=List[NewsArticleResponse])
@cache_response(ttl_seconds=120)
async def get_ticker_news(
    request: Request,
    ticker: str,
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_postgres_db)
):
    """Get recent news for a specific ticker."""
    ticker = ticker.upper()
    start_time = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    
    # Postgres ARRAY contains operator requires specific syntax
    # For SQLAlchemy, we can use `NewsArticle.tickers.contains([ticker])`
    from sqlalchemy import any_
    query = select(NewsArticle).where(
        ticker == any_(NewsArticle.tickers),
        NewsArticle.published_at >= start_time
    ).order_by(NewsArticle.published_at.desc()).limit(limit)
    
    results = db.execute(query).scalars().all()
    return results


@router.post("/search", response_model=NewsSearchResponse)
async def semantic_search_news(
    request: NewsSearchRequest
):
    """Semantic search via ChromaDB using OpenAI embeddings."""
    try:
        client = chromadb.PersistentClient(path=settings.chroma_path)
        collection = client.get_collection("news_articles")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ChromaDB connection failed: {e}")
        
    try:
        from config.llm_config import embeddings
        query_embedding = embeddings.embed_query(request.query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to embed query: {e}")

    # Build where clause for ChromaDB if exactly one ticker is specified
    where_clause = None
    if request.tickers and len(request.tickers) == 1:
        where_clause = {"ticker": request.tickers[0].upper()}
    # Note: ChromaDB doesn't easily support 'contains' in metadata for strings.
    # For multiple tickers or partial matches, we rely on the semantic search query.

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=request.limit,
        where=where_clause,
        include=["metadatas", "distances", "documents"]
    )
    
    hits = []
    if results and results["ids"] and len(results["ids"][0]) > 0:
        for i in range(len(results["ids"][0])):
            meta = results["metadatas"][0][i] or {}
            # Distance: lower is better (cosine distance). Similarity = 1 - distance
            distance = results["distances"][0][i]
            similarity = 1.0 - distance
            
            hits.append({
                "headline": results["documents"][0][i][:200] + "...",
                "url": meta.get("url", ""),
                "source": meta.get("source", ""),
                "published_at": meta.get("published_at", ""),
                "similarity": similarity
            })
            
    return NewsSearchResponse(query=request.query, results=hits)
