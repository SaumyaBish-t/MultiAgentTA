"""
scripts/run_safe_migrations.py
==============================

Thin entry point so the safe migrations can be invoked the same way
other operational scripts in ``scripts/`` are.

Usage
-----
$ python scripts/run_safe_migrations.py

Prerequisites
-------------
* PostgreSQL (fundamentals) on the URL configured in ``settings.postgres_url``
  must be reachable. The default is ``127.0.0.1:5434``.
* The ``pgcrypto`` extension will be created automatically if missing
  (required for ``gen_random_uuid()``).

Safe to re-run — every statement is ``CREATE TABLE IF NOT EXISTS``.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the project root is on sys.path when this script is run directly
# (matches the style of other scripts in this folder).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.safe_migrations import main  # noqa: E402


if __name__ == "__main__":
    sys.exit(main())
