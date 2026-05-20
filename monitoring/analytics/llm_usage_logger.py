"""
monitoring.analytics.llm_usage_logger
=====================================

Tiny utility to record one row in ``llm_usage_log`` after every LLM
call. Used as a context manager or as a decorator.

Example
-------
::

    from monitoring.analytics.llm_usage_logger import track_llm

    with track_llm(provider='groq', model='llama-3.3-70b',
                   call_type='hypothesis', agent_name='HypothesisAgent',
                   ticker='AAPL') as t:
        resp = llm.invoke(...)
        t.input_tokens  = resp.usage_metadata.get('input_tokens', 0)
        t.output_tokens = resp.usage_metadata.get('output_tokens', 0)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import psycopg2
from loguru import logger

from config.settings import settings


# Per-million-token pricing — for free-tier providers we keep 0.
COST_PER_M_TOKENS: dict[str, tuple[float, float]] = {
    "groq": (0.0, 0.0),
    "cerebras": (0.0, 0.0),
    "openrouter": (0.0, 0.0),
    "nvidia_nim": (0.0, 0.0),
    "mistral": (0.0, 0.0),
}


@dataclass
class _LLMTrack:
    provider: str
    model: str
    call_type: str
    agent_name: str = ""
    ticker: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    success: bool = True
    _t0: float = 0.0
    _latency_ms: Optional[int] = None

    def __enter__(self) -> "_LLMTrack":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._latency_ms = int((time.perf_counter() - self._t0) * 1000)
        if exc is not None:
            self.success = False
        self._persist()

    def _estimated_cost(self) -> float:
        rates = COST_PER_M_TOKENS.get(self.provider.lower(), (0.0, 0.0))
        return (self.input_tokens * rates[0] + self.output_tokens * rates[1]) / 1_000_000.0

    def _persist(self) -> None:
        try:
            conn = psycopg2.connect(settings.postgres_url, connect_timeout=3)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO llm_usage_log
                        (provider, model, call_type, input_tokens, output_tokens,
                         estimated_cost_usd, agent_name, ticker,
                         success, latency_ms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (self.provider, self.model, self.call_type,
                     self.input_tokens, self.output_tokens,
                     self._estimated_cost(),
                     self.agent_name, self.ticker,
                     self.success, self._latency_ms),
                )
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.debug("llm_usage_log insert skipped: {}", exc)


def track_llm(provider: str, model: str, call_type: str,
              agent_name: str = "", ticker: str = "") -> _LLMTrack:
    """Return a context manager / dataclass that records an LLM call row."""
    return _LLMTrack(
        provider=provider, model=model, call_type=call_type,
        agent_name=agent_name, ticker=ticker,
    )
