"""
core.channel_router
===================

Redis pub/sub channel routing for the 14-layer upgrade.

The upgrade rule is **additive only** — existing phases keep publishing
to the same channels they always have. New layers subscribe to those
channels and republish to *new* channels read by the next phase. This
module is the single place where "which channel does Phase N read from
right now?" is decided, based on feature flags in ``config.settings``.

Existing pipeline files import from here when they need to know their
input channel; if a flag is off, the getter returns the original channel
name so the system is byte-identical to its pre-upgrade behaviour.

Example
-------
>>> from core.channel_router import get_phase4_input_channel
>>> get_phase4_input_channel()
'signals.pipeline.completed'    # review_gate_enabled == False
"""

from __future__ import annotations

from config.settings import settings


class Channels:
    """Canonical Redis pub/sub channel names.

    Existing channels (DO NOT rename — they are wired into running code):
    """

    # ── Existing channels (pre-upgrade) ──────────────────────────
    SIGNALS_PIPELINE_COMPLETED = "signals.pipeline.completed"
    RESEARCH_PIPELINE_COMPLETED = "research.pipeline.completed"
    RISK_PIPELINE_COMPLETED = "risk.pipeline.completed"
    EXECUTION_PIPELINE_COMPLETED = "execution.pipeline.completed"
    COMPLIANCE_DAILY_COMPLETED = "compliance.daily.completed"
    MONITORING_PNL_UPDATED = "monitoring.pnl.updated"
    MONITORING_REGIME_CHANGED = "monitoring.regime.changed"
    MONITORING_SIGNALS_DECAY = "monitoring.signals.decay"

    # ── New channels (added by the 14-layer upgrade) ─────────────
    # L5 Review Gate
    SIGNALS_REVIEW_APPROVED = "signals.review.approved"
    SIGNALS_REVIEW_REJECTED = "signals.review.rejected"
    REVIEW_PENDING = "review.pending"
    PIPELINE_REVIEW_APPROVED = "pipeline.review.approved"
    PIPELINE_REVIEW_REJECTED = "pipeline.review.rejected"

    # L2 Feature Engine
    FEATURES_COMPUTED = "features.computed"


# ── Routing helpers ─────────────────────────────────────────────
# Each helper answers "given current feature flags, which channel
# should phase X subscribe to / publish to?"  Helpers are pure
# functions of settings — call them at runtime, do NOT cache.


def get_phase4_input_channel() -> str:
    """Phase 4 (Risk Management) input.

    With the Review Gate enabled, Phase 4 reads approved signals from
    L5's output channel. Without it, Phase 4 reads directly from
    Phase 3's output as before.
    """
    if settings.review_gate_enabled:
        return Channels.SIGNALS_REVIEW_APPROVED
    return Channels.SIGNALS_PIPELINE_COMPLETED


def get_review_gate_input_channel() -> str:
    """L5 Review Gate input. Always Phase 3's existing output channel."""
    return Channels.SIGNALS_PIPELINE_COMPLETED


def get_review_gate_output_channel(decision: str) -> str:
    """L5 Review Gate output, switched on the human decision.

    Parameters
    ----------
    decision : {"approved", "rejected"}
    """
    if decision == "approved":
        return Channels.SIGNALS_REVIEW_APPROVED
    if decision == "rejected":
        return Channels.SIGNALS_REVIEW_REJECTED
    raise ValueError(f"Unknown review decision: {decision!r}")


def get_research_features_channel() -> str | None:
    """L3 research agents — channel announcing that L2 features are ready.

    Returns ``None`` if the Feature Engine is disabled, so callers can
    skip subscribing entirely.
    """
    if settings.feature_engine_enabled:
        return Channels.FEATURES_COMPUTED
    return None


def is_review_gate_active() -> bool:
    """Convenience flag check — used by L5 listeners on startup."""
    return settings.review_gate_enabled


def is_feature_engine_active() -> bool:
    """Convenience flag check — used by L2 + L3 wiring."""
    return settings.feature_engine_enabled


__all__ = [
    "Channels",
    "get_phase4_input_channel",
    "get_review_gate_input_channel",
    "get_review_gate_output_channel",
    "get_research_features_channel",
    "is_review_gate_active",
    "is_feature_engine_active",
]
