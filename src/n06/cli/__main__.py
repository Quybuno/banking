"""Cho phép thực thi CLI qua `python -m n06.cli`."""

from __future__ import annotations

import sys

from n06.cli import main

if __name__ == "__main__":
    sys.exit(main())
