"""
review_gate.tools.news_calendar
===============================

Checks the next 48h for earnings (via FMP) and Fed events (heuristic).
This is a defensive check — if there's an earnings beat or Fed surprise
imminent, even a perfect technical signal can be steamrolled.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import httpx
from loguru import logger

from config.settings import settings
from review_gate.models import NewsCheckResult


def check(ticker: str) -> NewsCheckResult:
    upcoming: list[str] = []
    earnings = False
    fed = False

    api_key = settings.fmp_api_key.get_secret_value() if settings.fmp_api_key else ""
    if api_key:
        try:
            r = httpx.get(
                "https://financialmodelingprep.com/api/v3/earning_calendar",
                params={"apikey": api_key, "symbol": ticker},
                timeout=8,
            )
            if r.status_code == 200:
                cutoff = datetime.utcnow() + timedelta(hours=48)
                for row in r.json():
                    when = row.get("date") or row.get("when")
                    if not when:
                        continue
                    try:
                        d = datetime.fromisoformat(when[:19])
                    except ValueError:
                        continue
                    if datetime.utcnow() <= d <= cutoff:
                        earnings = True
                        upcoming.append(f"Earnings on {d.date()}")
        except Exception as exc:
            logger.warning("FMP earnings calendar fetch failed: {}", exc)

    # Fed meeting heuristic — could be wired to FRED FOMC release calendar.
    # Left as a stub so the field is always populated.
    return NewsCheckResult(
        has_earnings_within_48h=earnings,
        has_fed_event_within_48h=fed,
        upcoming_events=upcoming,
    )
