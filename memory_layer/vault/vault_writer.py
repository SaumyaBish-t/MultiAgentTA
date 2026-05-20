"""
memory_layer.vault.vault_writer
===============================

Higher-level vault operations than ``review_gate.vault_writer``:
the standard folder skeleton, outcome recording on closed trades,
and daily-observation appender.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from config.settings import settings

STANDARD_FOLDERS = [
    "trade-briefs",
    "trade-briefs/approved",
    "trade-briefs/rejected",
    "trade-briefs/graveyard",
    "market-regimes",
    "strategy-notes",
    "stock-profiles",
    "performance-journal",
    "postmortems",
    "daily-observations",
    "meta-analysis",
]


def _root() -> Path | None:
    if not settings.obsidian_vault_path:
        return None
    p = Path(settings.obsidian_vault_path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def ensure_structure() -> list[str]:
    """Create every folder in ``STANDARD_FOLDERS``. Returns created paths."""
    root = _root()
    if root is None:
        return []
    created: list[str] = []
    for sub in STANDARD_FOLDERS:
        p = root / sub
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created.append(str(p))
    return created


def record_outcome(brief_path: str, outcome_pct: float) -> bool:
    """Open an existing trade brief and update its ``outcome_pct`` frontmatter."""
    p = Path(brief_path)
    if not p.exists():
        return False
    try:
        text = p.read_text(encoding="utf-8")
    except Exception as exc:
        logger.warning("vault record_outcome read failed: {}", exc)
        return False

    if "outcome_pct:" in text:
        lines = []
        for line in text.splitlines():
            if line.startswith("outcome_pct:"):
                lines.append(f"outcome_pct: {outcome_pct:.4f}")
            else:
                lines.append(line)
        new_text = "\n".join(lines)
    else:
        # Inject after the opening --- frontmatter marker
        new_text = text.replace(
            "---\n", f"---\noutcome_pct: {outcome_pct:.4f}\n", 1,
        )

    try:
        p.write_text(new_text, encoding="utf-8")
        return True
    except Exception as exc:
        logger.warning("vault record_outcome write failed: {}", exc)
        return False


def append_daily_observation(text: str) -> str | None:
    root = _root()
    if root is None:
        return None
    folder = root / "daily-observations"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{datetime.utcnow().date()}.md"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(f"\n\n## {datetime.utcnow().isoformat()}\n\n{text}\n")
    return str(path)


def write_postmortem(strategy: str, body: str, frontmatter: dict[str, Any]) -> str | None:
    root = _root()
    if root is None:
        return None
    folder = root / "postmortems"
    folder.mkdir(parents=True, exist_ok=True)
    fm_lines = ["---"] + [f"{k}: {v}" for k, v in frontmatter.items()] + ["---", ""]
    text = "\n".join(fm_lines) + body
    path = folder / f"{datetime.utcnow().date()}_{strategy}.md"
    path.write_text(text, encoding="utf-8")
    return str(path)
