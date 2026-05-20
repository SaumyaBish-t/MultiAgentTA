"""
alpha_research.utils.disagreement_tracker
=========================================

Quantifies agent disagreement so the HypothesisAgent can modulate
conviction.

Rule of thumb (doc §5 L3):
* Unanimous agreement (low std)   → boost conviction
* High disagreement (high std)    → cut conviction

The score returned is a multiplier in ``[0.5, 1.2]`` applied on top of
whatever conviction the hypothesis agent already computed.
"""

from __future__ import annotations

import statistics
from typing import Iterable


def conviction_multiplier(agent_scores: Iterable[float]) -> float:
    """Return a conviction multiplier based on agent score dispersion.

    Parameters
    ----------
    agent_scores : iterable of floats in roughly [-1, 1] or [0, 1]
    """
    scores = list(agent_scores)
    if len(scores) < 2:
        return 1.0

    stdev = statistics.pstdev(scores)
    # Heuristic mapping. Tune later from L11 meta-analysis.
    if stdev < 0.05:
        return 1.2        # near-unanimous → +20%
    if stdev < 0.15:
        return 1.05
    if stdev < 0.30:
        return 0.85
    return 0.5             # severe disagreement → halve conviction


def describe_disagreement(agent_scores: Iterable[float]) -> dict:
    scores = list(agent_scores)
    if not scores:
        return {"stdev": 0.0, "label": "no_data", "multiplier": 1.0}
    stdev = statistics.pstdev(scores) if len(scores) > 1 else 0.0
    mult = conviction_multiplier(scores)
    if stdev < 0.05:
        label = "unanimous"
    elif stdev < 0.15:
        label = "aligned"
    elif stdev < 0.30:
        label = "mixed"
    else:
        label = "conflicted"
    return {"stdev": stdev, "label": label, "multiplier": mult, "n": len(scores)}
