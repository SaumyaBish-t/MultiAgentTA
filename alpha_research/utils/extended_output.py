"""
alpha_research.utils.extended_output
====================================

Additive output schema for L3 research agents.

The doc requires every agent to expose five new fields — evidence,
contradictions, regime_fit, why_reasoning, confidence_basis. To honour
the "additive only" architecture rule, existing agent return types are
**not modified**. Instead, agents (or wrappers around them) can build
an ``ExtendedAgentOutput`` alongside whatever they already return.

Old code paths see the original output and keep working. New consumers
(HypothesisAgent v2, L5 review gate, L11 meta-analysis) read the
extended fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExtendedAgentOutput:
    """The extra reasoning fields every agent should ultimately populate.

    All fields have safe defaults so older code that doesn't fill them in
    won't break downstream consumers.
    """

    score: float = 0.0
    confidence: float = 0.0
    evidence: list[str] = field(default_factory=list)
    contradictions: list[str] = field(default_factory=list)
    regime_fit: str = "unknown"           # e.g. "bull_low_vol"
    why_reasoning: str = ""                # economic mechanism, NOT statistics
    confidence_basis: str = ""             # what the confidence number is based on

    # Free-form attachments — anything an agent wants to surface.
    extras: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "contradictions": list(self.contradictions),
            "regime_fit": self.regime_fit,
            "why_reasoning": self.why_reasoning,
            "confidence_basis": self.confidence_basis,
            **({"extras": self.extras} if self.extras else {}),
        }
