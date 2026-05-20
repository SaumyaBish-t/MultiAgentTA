"""
review_gate.vault_writer
========================

Reads/writes Obsidian-flavoured markdown notes. One trade brief =
one ``.md`` file with YAML frontmatter under ``trade-briefs/`` in the
configured Obsidian vault.

Designed to be a no-op (with a warning) when no vault path is set, so
the review gate still functions even before L12 is fully online.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from config.settings import settings


def _vault_root() -> Path | None:
    path = settings.obsidian_vault_path
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        try:
            p.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning("Could not create vault dir {}: {}", p, exc)
            return None
    return p


def _to_yaml(d: dict[str, Any]) -> str:
    lines = ["---"]
    for k, v in d.items():
        if isinstance(v, list):
            lines.append(f"{k}:")
            for item in v:
                lines.append(f"  - {item}")
        elif v is None:
            lines.append(f"{k}: ")
        else:
            lines.append(f"{k}: {v}")
    lines.append("---")
    return "\n".join(lines)


def write_trade_brief(review: dict[str, Any], body: str = "") -> str | None:
    """Persist a trade brief. Returns the absolute path, or ``None``
    if the vault isn't configured.
    """
    root = _vault_root()
    if root is None:
        logger.warning("Vault not configured — skipping trade brief write")
        return None

    folder = root / "trade-briefs"
    folder.mkdir(parents=True, exist_ok=True)
    ticker = review.get("ticker", "UNKNOWN")
    rid = review.get("review_id", datetime.utcnow().strftime("%Y%m%dT%H%M%S"))
    filename = f"{datetime.utcnow().date()}_{ticker}_{rid}.md"
    path = folder / filename

    frontmatter = {
        "ticker": ticker,
        "date": datetime.utcnow().date().isoformat(),
        "direction": review.get("direction", ""),
        "strategy_type": review.get("strategy_type", ""),
        "hurst_class": review.get("hurst_class", ""),
        "conviction": review.get("recommendation_confidence", 0),
        "regime": review.get("regime", ""),
        "decision": review.get("human_decision", "pending"),
        "decision_notes": review.get("human_notes", ""),
        "outcome_pct": review.get("outcome_pct", ""),
    }
    text = _to_yaml(frontmatter) + "\n\n" + (body or "")
    path.write_text(text, encoding="utf-8")
    return str(path)


def move_brief_to(outcome_folder: str, brief_path: str) -> str | None:
    """Move an existing brief to ``approved/``, ``rejected/``, or ``graveyard/``."""
    root = _vault_root()
    if root is None or not brief_path:
        return None
    src = Path(brief_path)
    if not src.exists():
        return None
    dest_dir = root / "trade-briefs" / outcome_folder
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    try:
        os.replace(src, dest)
    except OSError as exc:
        logger.warning("vault move failed: {}", exc)
        return None
    return str(dest)
