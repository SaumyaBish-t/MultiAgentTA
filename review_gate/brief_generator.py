"""
review_gate.brief_generator
===========================

Rule-based recommendation scorer + plain-English headline composer.

Scoring (from doc §5 L5):
    price check still valid           +2
    calendar clear                    +2
    options aligned                   +2
    historical win rate > 50%         +2
    each negative check               −2 to −4
    score >= 4 → APPROVE
    1–3       → REDUCE SIZE
    <= 0      → REJECT
"""

from __future__ import annotations

from review_gate.models import (
    MemoryResult,
    NewsCheckResult,
    OptionsCheckResult,
    PriceCheckResult,
    ReviewRecommendation,
)


def score_and_recommend(
    *,
    ticker: str,
    direction: str,
    strategy_type: str,
    price: PriceCheckResult,
    news: NewsCheckResult,
    options: OptionsCheckResult,
    memory: MemoryResult,
) -> ReviewRecommendation:
    score = 0
    support_bits: list[str] = []
    concern_bits: list[str] = []

    if price.still_valid:
        score += 2
        support_bits.append(f"price held ({price.pct_move:+.2f}%)")
    else:
        score -= 4
        concern_bits.append(f"price moved {price.pct_move:+.2f}% — signal stale")

    if not news.has_earnings_within_48h and not news.has_fed_event_within_48h:
        score += 2
        support_bits.append("clean calendar")
    else:
        score -= 3
        concern_bits.append("event risk in next 48h: " + ", ".join(news.upcoming_events))

    if options.aligned_with_signal is True:
        score += 2
        support_bits.append(f"P/C {options.put_call_ratio:.2f} agrees with {direction}")
    elif options.aligned_with_signal is False:
        score -= 2
        concern_bits.append(f"P/C {options.put_call_ratio:.2f} contradicts {direction}")

    if memory.historical_win_rate is not None:
        if memory.historical_win_rate >= 0.55:
            score += 2
            support_bits.append(
                f"{memory.similar_setups_found} similar setups, "
                f"{memory.historical_win_rate:.0%} win rate"
            )
        elif memory.historical_win_rate < 0.4:
            score -= 2
            concern_bits.append(
                f"{memory.similar_setups_found} similar setups, "
                f"only {memory.historical_win_rate:.0%} win rate"
            )

    if score >= 4:
        rec = "APPROVE"
    elif score >= 1:
        rec = "REDUCE_SIZE"
    else:
        rec = "REJECT"

    headline = f"{rec}: {direction.upper()} {ticker} ({strategy_type})"
    confidence = max(0.0, min(1.0, (score + 4) / 12))

    return ReviewRecommendation(
        recommendation=rec,
        score=score,
        headline=headline,
        key_support="; ".join(support_bits) if support_bits else "",
        key_concern="; ".join(concern_bits) if concern_bits else "",
        confidence=confidence,
    )


def brief_body(rec: ReviewRecommendation, price, news, options, memory) -> str:
    """Compose the markdown body of the trade brief."""
    lines = [
        f"# {rec.headline}",
        "",
        f"**Recommendation:** {rec.recommendation}  (score {rec.score})",
        f"**Confidence:** {rec.confidence:.2f}",
        "",
        "## Support",
        rec.key_support or "_none_",
        "",
        "## Concerns",
        rec.key_concern or "_none_",
        "",
        "## Check Detail",
        f"- Price: {price.price_at_signal} → {price.current_price} ({price.pct_move:+.2f}%) — valid={price.still_valid}",
        f"- News: earnings_48h={news.has_earnings_within_48h}, fed_48h={news.has_fed_event_within_48h}",
        f"- Options: pcr={options.put_call_ratio}, aligned={options.aligned_with_signal}",
        f"- Memory: {memory.similar_setups_found} similar setups, win_rate={memory.historical_win_rate}",
        "",
        "## Review Notes",
        "_operator-supplied notes will be appended here on decision_",
    ]
    return "\n".join(lines)
