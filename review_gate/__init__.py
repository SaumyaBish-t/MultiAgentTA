"""
review_gate
===========

L5 — Human Review Gate.

Converts FORGE from autonomous to human-assisted. Sits between Phase 3
(signal generation) and Phase 4 (risk management), runs four automated
sanity checks in parallel, writes a trade brief to the Obsidian vault,
and PAUSES on a LangGraph interrupt until the operator approves /
reduces / rejects via the dashboard.

Disabled when ``settings.review_gate_enabled`` is ``False``.
"""
