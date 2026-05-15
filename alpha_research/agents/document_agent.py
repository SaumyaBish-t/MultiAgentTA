"""
Document Intelligence Agent — LangGraph Pipeline
=================================================

Performs Retrieval-Augmented Generation (RAG) over SEC filings and
news articles in ChromaDB to extract insights, risks, and management tone.

Graph flow::

    search_filings → search_news_context → extract_insights →
        assess_management_tone → store_insights → END
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, TypedDict

import chromadb
import httpx
import redis
from chromadb.config import Settings
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from alpha_research.storage.research_models import ResearchHypothesis
from config.llm_config import document_llm, embeddings
from config.settings import settings


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State & Results
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DocumentState(TypedDict):
    """Mutable state passed through the LangGraph pipeline."""
    ticker: str
    query: str
    retrieved_docs: list[dict[str, Any]]
    extracted_insights: list[str]
    risk_mentions: list[str]
    opportunity_mentions: list[str]
    management_tone: str
    key_quotes: list[str]
    doc_types_searched: list[str]
    error: str | None


@dataclass(frozen=True, slots=True)
class DocumentResult:
    """Immutable output returned to callers."""
    ticker: str
    insights: list[str]
    risks: list[str]
    opportunities: list[str]
    management_tone: str
    key_quotes: list[str]
    summary: str
    error: str | None = None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_API_BASE = "http://localhost:8000"
_API_HEADERS = {"x-api-key": settings.internal_api_key}

_INSIGHTS_SYSTEM_PROMPT = (
    "You are a financial analyst reading company documents.\n"
    "Extract key insights from these documents about {ticker}.\n"
    "Return JSON: {{\n"
    '  "insights": ["str"],         // max 5 key insights\n'
    '  "risks": ["str"],            // mentioned risks\n'
    '  "opportunities": ["str"],    // growth opportunities\n'
    '  "management_tone": "confident" | "cautious" | "negative",\n'
    '  "key_quotes": ["str"],       // max 3 important quotes\n'
    '  "summary": "str"             // 2 sentences\n'
    "}}\n"
    "Focus on financial impact and investment relevance. Return ONLY valid JSON."
)

_CONFIDENT_WORDS = ["exceeded", "raised guidance", "record", "strong demand", "accelerating", "outperformed"]
_CAUTIOUS_WORDS = ["headwinds", "uncertain", "challenging", "moderated", "below expectations", "lowered guidance"]
_NEGATIVE_WORDS = ["declined", "loss", "restructuring", "layoffs", "missed", "deteriorating", "crisis"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Nodes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_chroma_client() -> chromadb.ClientAPI:
    """Returns a connected ChromaDB client."""
    return chromadb.PersistentClient(
        path=settings.chroma_path,
        settings=Settings(anonymized_telemetry=False)
    )

async def search_filings_node(state: DocumentState) -> dict[str, Any]:
    ticker = state["ticker"]
    query = state["query"]
    retrieved = []
    
    # 1. API Call to /news/search
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{_API_BASE}/news/search",
                json={"query": f"{ticker} financial results risks {query}", "tickers": [ticker], "limit": 10},
                headers=_API_HEADERS,
            )
            resp.raise_for_status()
            api_results = resp.json()
            articles = api_results.get("results", api_results) if isinstance(api_results, dict) else api_results
            for art in articles:
                retrieved.append({
                    "url": art.get("url"),
                    "source": art.get("source", "api"),
                    "content": art.get("summary", "") or art.get("headline", ""),
                    "timestamp": art.get("published_utc")
                })
    except Exception as exc:
        logger.warning(f"API /news/search failed for {ticker}: {exc}")

    # 2. Direct ChromaDB search for sec_filings
    try:
        client = _get_chroma_client()
        try:
            collection = client.get_collection("sec_filings")
            q_emb = embeddings.embed_query(f"{ticker} revenue earnings guidance risk {query}")
            res = collection.query(
                query_embeddings=[q_emb],
                n_results=5,
                where={"ticker": ticker}
            )
            for i, doc in enumerate(res["documents"][0]):
                meta = res["metadatas"][0][i]
                retrieved.append({
                    "url": meta.get("url", f"sec_{uuid.uuid4()}"),
                    "source": "sec_filings",
                    "content": doc,
                    "timestamp": meta.get("published_utc", "")
                })
        except ValueError:
            logger.info("Collection 'sec_filings' not found in ChromaDB, skipping.")
    except Exception as exc:
        logger.warning(f"ChromaDB sec_filings search failed for {ticker}: {exc}")

    # Deduplicate by URL
    seen_urls = set()
    deduped = []
    for doc in retrieved:
        url = doc.get("url")
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduped.append(doc)
        elif not url:
            deduped.append(doc)

    return {"retrieved_docs": deduped, "doc_types_searched": ["api_news", "sec_filings"]}


async def search_news_context_node(state: DocumentState) -> dict[str, Any]:
    ticker = state["ticker"]
    query = state["query"]
    retrieved = state.get("retrieved_docs", [])
    
    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    
    try:
        client = _get_chroma_client()
        try:
            collection = client.get_collection("news_articles")
            q_emb = embeddings.embed_query(f"{ticker} {query}")
            res = collection.query(
                query_embeddings=[q_emb],
                n_results=10,
                where={"ticker": ticker}
            )
            
            for i, doc in enumerate(res["documents"][0]):
                meta = res["metadatas"][0][i]
                # Try to parse timestamp for filtering
                ts_str = meta.get("published_utc", "")
                try:
                    ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00')).timestamp()
                    if ts < thirty_days_ago:
                        continue
                except (ValueError, AttributeError):
                    pass # Keep if we can't parse
                    
                retrieved.append({
                    "url": meta.get("url", f"news_{uuid.uuid4()}"),
                    "source": "news_articles",
                    "content": doc,
                    "timestamp": ts_str
                })
        except ValueError:
            logger.info("Collection 'news_articles' not found in ChromaDB, skipping.")
    except Exception as exc:
        logger.warning(f"ChromaDB news_articles search failed for {ticker}: {exc}")

    # Deduplicate again
    seen_urls = set()
    deduped = []
    for doc in retrieved:
        url = doc.get("url")
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduped.append(doc)
        elif not url:
            deduped.append(doc)

    doc_types = state.get("doc_types_searched", [])
    doc_types.append("news_context")
    
    return {"retrieved_docs": deduped, "doc_types_searched": doc_types}


async def extract_insights_node(state: DocumentState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    ticker = state["ticker"]
    docs = state.get("retrieved_docs", [])
    
    if not docs:
        logger.info(f"No documents retrieved for {ticker}, skipping extraction.")
        return {
            "extracted_insights": [],
            "risk_mentions": [],
            "opportunity_mentions": [],
            "management_tone": "cautious",
            "key_quotes": [],
            "summary": "No documents found."
        }

    # Prepare context
    context_chunks = []
    for i, d in enumerate(docs[:15]): # Limit to top 15 to avoid context length overflow even for 8k
        source = d.get("source", "unknown")
        content = d.get("content", "")[:1000] # truncate individual docs to 1k chars
        context_chunks.append(f"Doc {i+1} [{source}]: {content}")
        
    context_str = "\n\n".join(context_chunks)

    prompt = _INSIGHTS_SYSTEM_PROMPT.format(ticker=ticker)
    messages = [
        SystemMessage(content=prompt),
        HumanMessage(content=f"Context:\n{context_str}\n\nExtract insights.")
    ]

    insights = []
    risks = []
    opps = []
    tone = "cautious"
    quotes = []
    summary = ""

    try:
        response = await document_llm.ainvoke(messages)
        text = response.content if hasattr(response, "content") else str(response)

        # Clean code fences
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            data = json.loads(text[start:end+1])
            insights = data.get("insights", [])[:5]
            risks = data.get("risks", [])
            opps = data.get("opportunities", [])
            tone = data.get("management_tone", "cautious")
            quotes = data.get("key_quotes", [])[:3]
            summary = data.get("summary", "")
            
    except Exception as exc:
        logger.warning(f"LLM insight extraction failed for {ticker}: {exc}")
        return {"error": f"LLM extraction failed: {exc}"}

    return {
        "extracted_insights": insights,
        "risk_mentions": risks,
        "opportunity_mentions": opps,
        "management_tone": tone,
        "key_quotes": quotes,
        "summary": summary
    }


async def assess_management_tone_node(state: DocumentState) -> dict[str, Any]:
    if state.get("error"):
        return {}
        
    # We already got a tone from the LLM, but we'll augment/override with keyword scoring
    all_text = " ".join(state.get("extracted_insights", []) + state.get("key_quotes", [])).lower()
    
    conf_score = sum(1 for w in _CONFIDENT_WORDS if w in all_text)
    caut_score = sum(1 for w in _CAUTIOUS_WORDS if w in all_text)
    neg_score = sum(1 for w in _NEGATIVE_WORDS if w in all_text)
    
    # If the LLM returned confident but keywords are heavily negative, override.
    llm_tone = state.get("management_tone", "cautious").lower()
    final_tone = llm_tone
    
    if neg_score > conf_score and neg_score > caut_score:
        final_tone = "negative"
    elif conf_score > neg_score and conf_score > caut_score:
        final_tone = "confident"
    elif caut_score > conf_score and caut_score > neg_score:
        final_tone = "cautious"

    return {"management_tone": final_tone}


async def store_insights_node(state: DocumentState) -> dict[str, Any]:
    if state.get("error"):
        return {}

    ticker = state["ticker"]
    summary = state.get("summary", "")
    now = datetime.now(timezone.utc)
    
    # Format insights for the hypothesis description
    desc_lines = [f"Summary: {summary}\n", "Insights:"]
    for i in state.get("extracted_insights", []):
        desc_lines.append(f"- {i}")
        
    description = "\n".join(desc_lines)
    
    # DB Persistence
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)
        with session_factory() as session:
            record = ResearchHypothesis(
                id=uuid.uuid4(),
                ticker=ticker,
                hypothesis_type="composite",
                title=f"Document Insights for {ticker}",
                description=description[:2000],
                status="pending",
                conviction_score=0.5, # Default placeholder
                expected_direction="neutral",
                expected_timeframe="n/a",
                supporting_signals={
                    "risks": state.get("risk_mentions", []),
                    "opportunities": state.get("opportunity_mentions", []),
                    "quotes": state.get("key_quotes", []),
                    "tone": state.get("management_tone", "neutral")
                },
                created_at=now,
                updated_at=now,
                created_by_agent="DocumentAgent"
            )
            session.add(record)
            session.commit()
        engine.dispose()
    except Exception as exc:
        logger.error(f"Failed to store ResearchHypothesis for {ticker}: {exc}")

    # Redis Cache
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        cache_val = state.get("summary", "No summary.")
        r.setex(f"document:insights:{ticker}", 1800, cache_val)
        r.close()
    except Exception as exc:
        logger.warning(f"Redis publish failed for document insights: {exc}")

    return {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Graph Construction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_document_graph() -> StateGraph:
    graph = StateGraph(DocumentState)
    graph.add_node("search_filings", search_filings_node)
    graph.add_node("search_news_context", search_news_context_node)
    graph.add_node("extract_insights", extract_insights_node)
    graph.add_node("assess_management_tone", assess_management_tone_node)
    graph.add_node("store_insights", store_insights_node)

    graph.set_entry_point("search_filings")
    graph.add_edge("search_filings", "search_news_context")
    graph.add_edge("search_news_context", "extract_insights")
    graph.add_edge("extract_insights", "assess_management_tone")
    graph.add_edge("assess_management_tone", "store_insights")
    graph.add_edge("store_insights", END)
    
    return graph


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DocumentAgent:
    def __init__(self) -> None:
        self._graph = _build_document_graph().compile()
        logger.info("DocumentAgent initialised")

    async def research(self, ticker: str, query: str = "") -> DocumentResult:
        logger.info(f"Running document intelligence for {ticker}")
        initial_state: DocumentState = {
            "ticker": ticker.upper(),
            "query": query,
            "retrieved_docs": [],
            "extracted_insights": [],
            "risk_mentions": [],
            "opportunity_mentions": [],
            "management_tone": "cautious",
            "key_quotes": [],
            "doc_types_searched": [],
            "error": None
        }
        
        final_state = await self._graph.ainvoke(initial_state)
        
        return DocumentResult(
            ticker=ticker.upper(),
            insights=final_state.get("extracted_insights", []),
            risks=final_state.get("risk_mentions", []),
            opportunities=final_state.get("opportunity_mentions", []),
            management_tone=final_state.get("management_tone", "cautious"),
            key_quotes=final_state.get("key_quotes", []),
            summary=final_state.get("summary", ""),
            error=final_state.get("error")
        )

    async def find_risk_mentions(self, ticker: str) -> list[str]:
        res = await self.research(ticker, query="risks negative factors regulatory")
        return res.risks if res and not res.error else []

    async def compare_guidance(self, ticker: str) -> dict[str, Any]:
        res = await self.research(ticker, query="guidance forward outlook expected")
        if res.error:
            return {"error": res.error}
        return {
            "ticker": ticker,
            "management_tone": res.management_tone,
            "quotes": res.key_quotes
        }

    async def search_across_tickers(self, query: str) -> dict[str, DocumentResult]:
        # Simple parallel execution for active tickers in settings
        tickers = settings.tickers[:5] # limit to 5 to avoid overloading in tests
        tasks = [self.research(t, query) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        output = {}
        for t, r in zip(tickers, results):
            if isinstance(r, Exception):
                logger.error(f"Failed {t}: {r}")
            else:
                output[t.upper()] = r
        return output
