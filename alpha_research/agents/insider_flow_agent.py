"""
alpha_research.agents.insider_flow_agent
========================================

L3 agent that reads the ``insider_transactions`` table (populated by
L1's insider_collector) and produces a bullish/bearish view per ticker.

Heavy weight on C-suite *open-market* purchases (transaction_type='P'):
they are the strongest single equity signal in academic literature.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import psycopg2
from loguru import logger

from config.settings import settings
from alpha_research.utils.extended_output import ExtendedAgentOutput


def _query_recent(ticker: str, days: int = 90) -> list[tuple]:
    try:
        conn = psycopg2.connect(settings.postgres_url, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT insider_name, insider_title, transaction_type,
                       shares, price_per_share, total_value, transaction_date
                FROM insider_transactions
                WHERE ticker = %s
                  AND transaction_date >= %s
                ORDER BY transaction_date DESC
                """,
                (ticker, datetime.utcnow().date() - timedelta(days=days)),
            )
            rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as exc:
        logger.warning("insider_flow_agent DB read failed for {}: {}", ticker, exc)
        return []


def analyse(ticker: str) -> ExtendedAgentOutput:
    if not settings.insider_flow_enabled:
        return ExtendedAgentOutput(
            score=0.0, confidence=0.0,
            confidence_basis="insider_flow_enabled is False",
        )

    rows = _query_recent(ticker)
    evidence: list[str] = []
    contradictions: list[str] = []
    bull = 0.0
    bear = 0.0
    n = 0
    for name, title, txn_type, shares, price, value, txn_date in rows:
        n += 1
        is_c_suite = title and any(k in title.upper() for k in ("CEO", "CFO", "PRES", "COO"))
        weight = 1.5 if is_c_suite else 1.0
        if txn_type == "P":
            bull += weight
            evidence.append(f"{name} ({title}) bought {shares:,} @ ${price:.2f} on {txn_date}")
        elif txn_type == "S":
            bear += weight * 0.5
            contradictions.append(f"{name} ({title}) sold {shares:,} @ ${price:.2f} on {txn_date}")

    if n == 0:
        return ExtendedAgentOutput(
            score=0.0, confidence=0.0,
            confidence_basis="no Form 4 filings in 90-day window",
        )

    raw = bull - bear
    score = max(-1.0, min(1.0, raw / max(n, 3)))
    confidence = min(1.0, n / 10.0)
    return ExtendedAgentOutput(
        score=score,
        confidence=confidence,
        evidence=evidence[:5],
        contradictions=contradictions[:5],
        regime_fit="any",
        why_reasoning=(
            "Insiders have asymmetric information; open-market purchases "
            "(especially by C-suite) signal conviction that public investors "
            "lack. Sales are weaker signals — they can be tax / diversification."
        ),
        confidence_basis=f"{n} Form 4 transactions over the trailing 90 days",
    )
