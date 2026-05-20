"""
alpha_research.agents.options_sentiment_agent
=============================================

L3 agent that reads the put/call ratio published by L1's
options_flow_collector and produces a directional view.

Heuristic
---------
* PCR < 0.7 → bullish (calls dominate)
* PCR 0.7–1.0 → neutral-bullish
* PCR 1.0–1.3 → neutral-bearish
* PCR > 1.3 → bearish (puts dominate; often a contrarian buy signal at extreme readings)

Note: Indian tickers don't have a usable free P/C source — agent
returns a confidence-0 output with an explanation.
"""

from __future__ import annotations

import json

import redis
from loguru import logger

from config.settings import settings
from alpha_research.utils.extended_output import ExtendedAgentOutput


def _is_indian(ticker: str) -> bool:
    return ticker.endswith(".NS") or ticker.endswith(".BSE")


def analyse(ticker: str) -> ExtendedAgentOutput:
    if not settings.options_flow_enabled:
        return ExtendedAgentOutput(confidence_basis="options_flow_enabled is False")

    if _is_indian(ticker):
        return ExtendedAgentOutput(
            confidence_basis="Indian ticker — no reliable free P/C source"
        )

    try:
        r = redis.from_url(settings.redis_url, socket_connect_timeout=2)
        raw = r.get(f"options:pcr:{ticker}")
    except Exception as exc:
        logger.warning("options_sentiment_agent Redis read failed: {}", exc)
        return ExtendedAgentOutput(confidence_basis=f"Redis error: {exc}")

    if not raw:
        return ExtendedAgentOutput(
            confidence_basis="no cached P/C ratio (run options_flow_collector first)"
        )

    payload = json.loads(raw)
    pcr = float(payload.get("ratio", 1.0))
    if pcr < 0.7:
        score, label = 0.7, "bullish"
    elif pcr < 1.0:
        score, label = 0.3, "neutral_bullish"
    elif pcr < 1.3:
        score, label = -0.3, "neutral_bearish"
    else:
        score, label = -0.7, "bearish"

    return ExtendedAgentOutput(
        score=score,
        confidence=0.6,
        evidence=[f"PCR = {pcr:.2f} ({label})"],
        contradictions=[],
        regime_fit="any",
        why_reasoning=(
            "Aggregate option open interest reflects positioning by "
            "informed institutional traders. Persistent put dominance "
            "implies hedging or directional bearishness; the reverse for calls."
        ),
        confidence_basis=f"single-snapshot CBOE-equivalent P/C ratio = {pcr:.2f}",
        extras={"raw_pcr": pcr, "label": label},
    )
