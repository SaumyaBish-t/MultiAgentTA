"""
Sentiment Analysis Agent — LangGraph Pipeline
================================================

Multi-source sentiment scoring agent that:

1. Fetches news articles and market posts from the Phase 1 FastAPI
2. Scores them with LLMs (Cerebras for news, Groq for social)
3. Aggregates into a weighted composite sentiment score
4. Persists to PostgreSQL and publishes to Redis pub/sub

Graph flow::

    fetch_news → fetch_reddit → score_news → score_reddit
       → aggregate_scores → store_results → END

Usage
-----
::

    from alpha_research.agents.sentiment_agent import SentimentAgent

    agent = SentimentAgent()

    # Single ticker
    result = await agent.analyze("AAPL")

    # Batch (parallel)
    results = await agent.analyze_batch(["AAPL", "MSFT", "NVDA"])

    # Cache-first lookup
    cached = await agent.get_latest_score("AAPL")
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypedDict

import httpx
import redis
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from alpha_research.storage.research_models import SentimentScore
from config.llm_config import research_llm, simple_llm
from config.settings import settings


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  State
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SentimentState(TypedDict):
    """Mutable state passed through the LangGraph pipeline."""

    ticker: str
    hours_back: int
    news_articles: list[dict[str, Any]]
    reddit_posts: list[dict[str, Any]]
    raw_scores: list[dict[str, Any]]
    aggregated_score: float
    magnitude: float
    sentiment_label: str          # bullish / bearish / neutral
    sample_count: int
    key_themes: list[str]
    risk_flags: list[str]
    error: str | None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Result dataclass
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass(frozen=True, slots=True)
class SentimentResult:
    """Immutable output returned to callers."""

    ticker: str
    score: float
    magnitude: float
    label: str
    key_themes: list[str] = field(default_factory=list)
    risk_flags: list[str] = field(default_factory=list)
    sample_count: int = 0
    scored_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a JSON-friendly dict."""
        return {
            "ticker": self.ticker,
            "score": round(self.score, 4),
            "magnitude": round(self.magnitude, 4),
            "label": self.label,
            "key_themes": self.key_themes,
            "risk_flags": self.risk_flags,
            "sample_count": self.sample_count,
            "scored_at": self.scored_at.isoformat(),
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Constants
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_API_BASE = "http://localhost:8000"
_API_KEY = settings.internal_api_key
_API_HEADERS: dict[str, str] = {"x-api-key": _API_KEY}

_NEWS_WEIGHT = 0.7
_REDDIT_WEIGHT = 0.3

_BULLISH_THRESHOLD = 0.2
_BEARISH_THRESHOLD = -0.2

_BATCH_SIZE = 10  # max articles per LLM call

_SCORE_SYSTEM_PROMPT = (
    "You are a financial sentiment analyzer. "
    "For each news article provided, return a JSON array with objects:\n"
    '{"headline": str, "score": float (-1 to 1), '
    '"magnitude": float (0 to 1), "themes": list[str]}.\n\n'
    "Score guide:\n"
    "  -1.0 = very bearish (bankruptcy, fraud, massive miss)\n"
    "  -0.5 = bearish (downgrades, declining revenue)\n"
    "   0.0 = neutral (routine filings, no market impact)\n"
    "  +0.5 = bullish (upgrades, strong growth)\n"
    "  +1.0 = very bullish (breakthrough product, massive beat)\n\n"
    "Magnitude: how impactful is this news on a 0–1 scale.\n"
    "Themes: 1–3 short theme labels (e.g. 'earnings_beat', 'supply_chain').\n\n"
    "Be precise, financial-context aware, and return ONLY valid JSON."
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Graph Nodes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def fetch_news_node(state: SentimentState) -> dict[str, Any]:
    """
    Node 1 — Fetch ticker-specific news from Phase 1 FastAPI.

    GET /news/{ticker}?hours={hours_back}&limit=50
    """
    ticker = state["ticker"]
    hours = state["hours_back"]

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{_API_BASE}/news/{ticker}",
                params={"hours": hours, "limit": 50},
                headers=_API_HEADERS,
            )
            resp.raise_for_status()
            articles = resp.json()
    except Exception as exc:
        logger.warning("fetch_news_node failed for {}: {}", ticker, exc)
        articles = []

    logger.info("Fetched {} news articles for {}", len(articles), ticker)
    return {"news_articles": articles}


async def fetch_reddit_node(state: SentimentState) -> dict[str, Any]:
    """
    Node 2 — Fetch market-wide news and filter for ticker mentions.

    GET /news/market?hours={hours_back}
    """
    ticker = state["ticker"]
    hours = state["hours_back"]

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{_API_BASE}/news/market",
                params={"hours": hours},
                headers=_API_HEADERS,
            )
            resp.raise_for_status()
            all_posts = resp.json()
    except Exception as exc:
        logger.warning("fetch_reddit_node failed for {}: {}", ticker, exc)
        all_posts = []

    # Filter for ticker mentions in tickers list, headline, or summary
    ticker_upper = ticker.upper()
    filtered: list[dict[str, Any]] = []
    for post in all_posts:
        tickers_in_post = post.get("tickers") or []
        headline = (post.get("headline") or "").upper()
        summary = (post.get("summary") or "").upper()

        if (
            ticker_upper in tickers_in_post
            or ticker_upper in headline
            or ticker_upper in summary
        ):
            filtered.append(post)

    logger.info(
        "Fetched {} market posts, {} mention {}",
        len(all_posts), len(filtered), ticker,
    )
    return {"reddit_posts": filtered}


def _format_articles_for_llm(articles: list[dict[str, Any]]) -> str:
    """Format a batch of articles into a numbered prompt."""
    lines: list[str] = []
    for i, art in enumerate(articles, 1):
        headline = art.get("headline", "No headline")
        summary = art.get("summary", "")
        source = art.get("source", "unknown")
        lines.append(f"{i}. [{source}] {headline}")
        if summary:
            lines.append(f"   Summary: {summary[:300]}")
    return "\n".join(lines)


def _parse_llm_scores(raw_text: str) -> list[dict[str, Any]]:
    """
    Parse the LLM JSON response, handling markdown fences and
    trailing text gracefully.
    """
    text = raw_text.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        first_newline = text.index("\n")
        text = text[first_newline + 1 :]
    if text.endswith("```"):
        text = text[: text.rfind("```")]
    text = text.strip()

    # Find the outermost JSON array
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        logger.warning("No JSON array found in LLM response")
        return []

    try:
        parsed = json.loads(text[start : end + 1])
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError as exc:
        logger.warning("JSON parse error in LLM response: {}", exc)

    return []


async def _score_batch(
    articles: list[dict[str, Any]],
    llm: Any,
    source_label: str,
) -> list[dict[str, Any]]:
    """Score a batch of articles with the given LLM."""
    if not articles:
        return []

    prompt_text = _format_articles_for_llm(articles)
    messages = [
        SystemMessage(content=_SCORE_SYSTEM_PROMPT),
        HumanMessage(
            content=(
                f"Score these {len(articles)} {source_label} articles:\n\n"
                f"{prompt_text}"
            )
        ),
    ]

    try:
        response = await llm.ainvoke(messages)
        raw_text = response.content if hasattr(response, "content") else str(response)
        scores = _parse_llm_scores(raw_text)
        logger.debug(
            "LLM scored {}/{} {} articles",
            len(scores), len(articles), source_label,
        )
        # Tag each score with source
        for s in scores:
            s["source"] = source_label
        return scores
    except Exception as exc:
        logger.error("LLM scoring failed for {}: {}", source_label, exc)
        return []


async def score_news_node(state: SentimentState) -> dict[str, Any]:
    """
    Node 3 — Score news articles with research_llm (Cerebras).

    Batches articles into groups of _BATCH_SIZE to stay within
    context limits.
    """
    articles = state["news_articles"]
    if not articles:
        logger.info("No news articles to score for {}", state["ticker"])
        return {"raw_scores": state.get("raw_scores", [])}

    all_scores: list[dict[str, Any]] = []

    # Process in batches
    for i in range(0, len(articles), _BATCH_SIZE):
        batch = articles[i : i + _BATCH_SIZE]
        batch_scores = await _score_batch(batch, research_llm, "news")
        all_scores.extend(batch_scores)

    existing = state.get("raw_scores", [])
    return {"raw_scores": existing + all_scores}


async def score_reddit_node(state: SentimentState) -> dict[str, Any]:
    """
    Node 4 — Score Reddit/market posts with simple_llm (Groq 8B).

    Reddit is lower priority so we use the cheaper, faster model.
    """
    posts = state["reddit_posts"]
    if not posts:
        logger.info("No reddit posts to score for {}", state["ticker"])
        return {"raw_scores": state.get("raw_scores", [])}

    all_scores: list[dict[str, Any]] = []

    for i in range(0, len(posts), _BATCH_SIZE):
        batch = posts[i : i + _BATCH_SIZE]
        batch_scores = await _score_batch(batch, simple_llm, "reddit")
        all_scores.extend(batch_scores)

    existing = state.get("raw_scores", [])
    return {"raw_scores": existing + all_scores}


async def aggregate_scores_node(state: SentimentState) -> dict[str, Any]:
    """
    Node 5 — Weighted aggregation of all raw scores.

    News articles are weighted at 0.7, Reddit at 0.3.
    Determines bullish / bearish / neutral label and extracts
    top themes and risk flags.
    """
    raw_scores = state.get("raw_scores", [])

    if not raw_scores:
        logger.warning("No raw scores to aggregate for {}", state["ticker"])
        return {
            "aggregated_score": 0.0,
            "magnitude": 0.0,
            "sentiment_label": "neutral",
            "sample_count": 0,
            "key_themes": [],
            "risk_flags": ["NO_DATA_AVAILABLE"],
        }

    # Separate by source
    news_scores = [s for s in raw_scores if s.get("source") == "news"]
    reddit_scores = [s for s in raw_scores if s.get("source") == "reddit"]

    def _avg(items: list[dict[str, Any]], key: str) -> float:
        vals = [s.get(key, 0.0) for s in items if isinstance(s.get(key), (int, float))]
        return sum(vals) / len(vals) if vals else 0.0

    news_avg_score = _avg(news_scores, "score")
    news_avg_mag = _avg(news_scores, "magnitude")
    reddit_avg_score = _avg(reddit_scores, "score")
    reddit_avg_mag = _avg(reddit_scores, "magnitude")

    # Weighted composite — adjust weights if one source is missing
    if news_scores and reddit_scores:
        composite_score = (news_avg_score * _NEWS_WEIGHT) + (reddit_avg_score * _REDDIT_WEIGHT)
        composite_mag = (news_avg_mag * _NEWS_WEIGHT) + (reddit_avg_mag * _REDDIT_WEIGHT)
    elif news_scores:
        composite_score = news_avg_score
        composite_mag = news_avg_mag
    elif reddit_scores:
        composite_score = reddit_avg_score
        composite_mag = reddit_avg_mag
    else:
        composite_score = 0.0
        composite_mag = 0.0

    # Clamp to valid ranges
    composite_score = max(-1.0, min(1.0, composite_score))
    composite_mag = max(0.0, min(1.0, composite_mag))

    # Label
    if composite_score > _BULLISH_THRESHOLD:
        label = "bullish"
    elif composite_score < _BEARISH_THRESHOLD:
        label = "bearish"
    else:
        label = "neutral"

    # Extract top 5 themes
    theme_counts: dict[str, int] = {}
    for s in raw_scores:
        for theme in s.get("themes", []):
            theme_str = str(theme).lower().strip()
            if theme_str:
                theme_counts[theme_str] = theme_counts.get(theme_str, 0) + 1

    top_themes = sorted(theme_counts, key=theme_counts.get, reverse=True)[:5]  # type: ignore[arg-type]

    # Risk flags
    total_samples = len(raw_scores)
    risk_flags: list[str] = []
    if composite_mag > 0.8:
        risk_flags.append("HIGH_MAGNITUDE_EVENT")
    if total_samples < 3:
        risk_flags.append("LOW_SAMPLE_COUNT")

    logger.info(
        "Aggregated sentiment for {}: score={:.3f} mag={:.3f} label={} "
        "samples={} themes={}",
        state["ticker"], composite_score, composite_mag,
        label, total_samples, top_themes,
    )

    return {
        "aggregated_score": composite_score,
        "magnitude": composite_mag,
        "sentiment_label": label,
        "sample_count": total_samples,
        "key_themes": top_themes,
        "risk_flags": risk_flags,
    }


async def store_results_node(state: SentimentState) -> dict[str, Any]:
    """
    Node 6 — Persist to PostgreSQL and publish to Redis.

    Writes a ``SentimentScore`` row and publishes a notification
    to the ``research.sentiment.updated`` channel.
    """
    ticker = state["ticker"]
    now = datetime.now(tz=timezone.utc)

    # ── PostgreSQL write ──────────────────────────────────────
    try:
        engine = create_engine(settings.postgres_url, future=True)
        session_factory = sessionmaker(bind=engine, expire_on_commit=False)

        with session_factory() as session:
            record = SentimentScore(
                id=uuid.uuid4(),
                ticker=ticker,
                source="composite",
                score=state["aggregated_score"],
                magnitude=state["magnitude"],
                sample_count=state["sample_count"],
                time_window_hours=state["hours_back"],
                raw_scores={
                    "news_count": len(state["news_articles"]),
                    "reddit_count": len(state["reddit_posts"]),
                    "raw_items": state.get("raw_scores", [])[:20],  # cap stored detail
                },
                scored_at=now,
            )
            session.add(record)
            session.commit()
            logger.info(
                "✓ SentimentScore persisted → {} score={:.3f} [{}]",
                ticker, state["aggregated_score"], state["sentiment_label"],
            )

        engine.dispose()

    except Exception as exc:
        logger.error("Failed to persist SentimentScore for {}: {}", ticker, exc)
        return {"error": f"DB write failed: {exc}"}

    # ── Redis publish ─────────────────────────────────────────
    try:
        r = redis.from_url(settings.redis_url, decode_responses=True)
        payload = json.dumps({
            "ticker": ticker,
            "score": round(state["aggregated_score"], 4),
            "label": state["sentiment_label"],
            "magnitude": round(state["magnitude"], 4),
            "sample_count": state["sample_count"],
            "timestamp": now.isoformat(),
        })
        r.publish("research.sentiment.updated", payload)

        # Also cache as latest score for fast lookups
        r.setex(
            f"sentiment:latest:{ticker}",
            3600,  # 1 hour TTL
            payload,
        )
        r.close()
        logger.debug("✓ Published sentiment update for {} to Redis", ticker)

    except Exception as exc:
        logger.warning("Redis publish failed for {}: {}", ticker, exc)

    return {"error": None}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Graph Construction
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_sentiment_graph() -> StateGraph:
    """
    Construct the LangGraph pipeline for sentiment analysis.

    Flow: fetch_news → fetch_reddit → score_news →
          score_reddit → aggregate → store → END
    """
    graph = StateGraph(SentimentState)

    # Add nodes
    graph.add_node("fetch_news", fetch_news_node)
    graph.add_node("fetch_reddit", fetch_reddit_node)
    graph.add_node("score_news", score_news_node)
    graph.add_node("score_reddit", score_reddit_node)
    graph.add_node("aggregate_scores", aggregate_scores_node)
    graph.add_node("store_results", store_results_node)

    # Linear edges
    graph.set_entry_point("fetch_news")
    graph.add_edge("fetch_news", "fetch_reddit")
    graph.add_edge("fetch_reddit", "score_news")
    graph.add_edge("score_news", "score_reddit")
    graph.add_edge("score_reddit", "aggregate_scores")
    graph.add_edge("aggregate_scores", "store_results")
    graph.add_edge("store_results", END)

    return graph


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Public Interface
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SentimentAgent:
    """
    High-level interface for sentiment analysis.

    Wraps the LangGraph pipeline and provides convenience methods
    for single-ticker analysis, batch analysis, and cache lookups.

    Examples
    --------
    ::

        agent = SentimentAgent()

        # Analyse a single ticker
        result = await agent.analyze("AAPL", hours_back=24)
        print(result.label)  # "bullish"

        # Batch analysis (parallel)
        results = await agent.analyze_batch(["AAPL", "MSFT", "NVDA"])
        for ticker, result in results.items():
            print(f"{ticker}: {result.label} ({result.score:.3f})")

        # Check Redis cache first
        cached = await agent.get_latest_score("AAPL")
    """

    def __init__(self) -> None:
        self._graph = _build_sentiment_graph().compile()
        logger.info("SentimentAgent initialised with LangGraph pipeline")

    async def analyze(
        self,
        ticker: str,
        hours_back: int = 24,
    ) -> SentimentResult:
        """
        Run the full sentiment pipeline for a single ticker.

        Parameters
        ----------
        ticker : str
            Equity symbol (e.g. ``"AAPL"``).
        hours_back : int
            Lookback window in hours for news fetching.

        Returns
        -------
        SentimentResult
            Immutable result with score, label, themes, and flags.
        """
        ticker = ticker.upper()
        logger.info("═══ SentimentAgent.analyze({}, {}h) ═══", ticker, hours_back)

        initial_state: SentimentState = {
            "ticker": ticker,
            "hours_back": hours_back,
            "news_articles": [],
            "reddit_posts": [],
            "raw_scores": [],
            "aggregated_score": 0.0,
            "magnitude": 0.0,
            "sentiment_label": "neutral",
            "sample_count": 0,
            "key_themes": [],
            "risk_flags": [],
            "error": None,
        }

        final_state = await self._graph.ainvoke(initial_state)

        result = SentimentResult(
            ticker=ticker,
            score=final_state["aggregated_score"],
            magnitude=final_state["magnitude"],
            label=final_state["sentiment_label"],
            key_themes=final_state["key_themes"],
            risk_flags=final_state["risk_flags"],
            sample_count=final_state["sample_count"],
        )

        if final_state.get("error"):
            logger.warning(
                "SentimentAgent completed with error for {}: {}",
                ticker, final_state["error"],
            )

        logger.info(
            "═══ SentimentAgent result: {} → {} ({:.3f}) ═══",
            ticker, result.label, result.score,
        )
        return result

    async def analyze_batch(
        self,
        tickers: list[str],
        hours_back: int = 24,
    ) -> dict[str, SentimentResult]:
        """
        Analyse multiple tickers in parallel.

        Parameters
        ----------
        tickers : list[str]
            List of equity symbols.
        hours_back : int
            Lookback window in hours.

        Returns
        -------
        dict[str, SentimentResult]
            Mapping of ticker → result.
        """
        logger.info(
            "SentimentAgent.analyze_batch({} tickers, {}h)",
            len(tickers), hours_back,
        )

        tasks = [self.analyze(t, hours_back) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output: dict[str, SentimentResult] = {}
        for ticker, result in zip(tickers, results):
            ticker = ticker.upper()
            if isinstance(result, Exception):
                logger.error("Batch analysis failed for {}: {}", ticker, result)
                output[ticker] = SentimentResult(
                    ticker=ticker,
                    score=0.0,
                    magnitude=0.0,
                    label="neutral",
                    risk_flags=["ANALYSIS_FAILED"],
                )
            else:
                output[ticker] = result

        return output

    async def get_latest_score(
        self,
        ticker: str,
    ) -> SentimentResult | None:
        """
        Retrieve the latest cached sentiment from Redis.

        Falls back to ``None`` if no cached data exists.

        Parameters
        ----------
        ticker : str
            Equity symbol.

        Returns
        -------
        SentimentResult | None
            Cached result or None.
        """
        ticker = ticker.upper()
        try:
            r = redis.from_url(settings.redis_url, decode_responses=True)
            cached = r.get(f"sentiment:latest:{ticker}")
            r.close()

            if not cached:
                logger.debug("No cached sentiment for {}", ticker)
                return None

            data = json.loads(cached)
            return SentimentResult(
                ticker=data["ticker"],
                score=data["score"],
                magnitude=data["magnitude"],
                label=data["label"],
                sample_count=data.get("sample_count", 0),
                scored_at=datetime.fromisoformat(data["timestamp"]),
            )

        except Exception as exc:
            logger.warning("Redis lookup failed for {}: {}", ticker, exc)
            return None
