"""
review_gate.tools.memory_lookup
===============================

Searches the Obsidian vault for similar past setups by scanning the
YAML frontmatter on every markdown file under ``trade-briefs/``.

The check is intentionally simple and dependency-free: parse front
matter with PyYAML if available, otherwise fall back to a regex on
``ticker:`` / ``strategy_type:`` / ``outcome_pct:`` lines.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from loguru import logger

from config.settings import settings
from review_gate.models import MemoryResult

try:
    import yaml  # type: ignore[import-untyped]
except Exception:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---", re.DOTALL)
FIELD_RE = re.compile(r"^(\w+):\s*(.+?)\s*$", re.MULTILINE)


def _parse_frontmatter(text: str) -> dict:
    m = FRONTMATTER_RE.match(text)
    if not m:
        return {}
    block = m.group(1)
    if yaml is not None:
        try:
            return yaml.safe_load(block) or {}
        except Exception:
            return {}
    return dict(FIELD_RE.findall(block))


def check(ticker: str, strategy_type: str) -> MemoryResult:
    vault = settings.obsidian_vault_path
    if not vault or not os.path.isdir(vault):
        return MemoryResult(notes=["vault not configured"])

    root = Path(vault) / "trade-briefs"
    if not root.exists():
        return MemoryResult(notes=["no trade-briefs folder"])

    similar = 0
    wins = 0
    losses = 0
    examples: list[str] = []
    try:
        for path in root.rglob("*.md"):
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            fm = _parse_frontmatter(text)
            if not fm:
                continue
            if str(fm.get("ticker", "")).upper() != ticker.upper():
                continue
            if str(fm.get("strategy_type", "")) != strategy_type:
                continue
            similar += 1
            try:
                outcome = float(fm.get("outcome_pct", 0))
                if outcome > 0:
                    wins += 1
                elif outcome < 0:
                    losses += 1
                examples.append(f"{path.stem}: {outcome:+.2f}%")
            except (TypeError, ValueError):
                pass
    except Exception as exc:
        logger.warning("memory_lookup walk failed: {}", exc)

    decided = wins + losses
    win_rate = (wins / decided) if decided else None
    return MemoryResult(
        similar_setups_found=similar,
        historical_win_rate=win_rate,
        notes=examples[:5],
    )
