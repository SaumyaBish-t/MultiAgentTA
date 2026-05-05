"""
Root conftest — ensures the project root is on sys.path so that
`import data_ingestion` and `import config` resolve correctly
when running pytest from the trading-system directory.
"""

import sys
from pathlib import Path

# Add the project root (this file's directory) to sys.path
ROOT = str(Path(__file__).resolve().parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
