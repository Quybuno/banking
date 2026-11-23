"""Cấu hình sys.path cho unit test oracle."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = str(Path(__file__).resolve().parents[3])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
