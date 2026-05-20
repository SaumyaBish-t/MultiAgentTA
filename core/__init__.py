"""
core
====

Shared infrastructure for the 14-layer FORGE architecture upgrade.

This package holds *additive* plumbing — channel routing, idempotent
migrations, pipeline health checks — that lets new layers (L2 Feature
Engine, L5 Review Gate, L11 Meta-Analysis, L12 Memory) slot in between
existing phases without touching their code.

Nothing in ``core/`` should ever modify an existing pipeline file.
"""
