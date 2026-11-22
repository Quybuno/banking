"""Smoke tests cho khung dự án N06 (TASK T00)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import n06

REPO_ROOT = Path(__file__).resolve().parents[2]
VERIFY_CONTRACT_SCRIPT = (
    REPO_ROOT / "contracts" / "N06_INPUT_CONTRACT_V1" / "tools" / "verify_contract.py"
)


def test_import_n06() -> None:
    """Kiểm tra package n06 có thể import được từ src layout."""
    assert n06.__name__ == "n06"
    assert n06.__doc__ is not None
    package_path = Path(n06.__file__).resolve()
    assert package_path == REPO_ROOT / "src" / "n06" / "__init__.py"


def test_verify_contract_subprocess_passes() -> None:
    """Chạy thật verify_contract.py qua subprocess và kiểm tra exit code 0 (14/14 PASS)."""
    assert VERIFY_CONTRACT_SCRIPT.is_file(), f"Không tìm thấy {VERIFY_CONTRACT_SCRIPT}"

    result = subprocess.run(
        [sys.executable, str(VERIFY_CONTRACT_SCRIPT)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"verify_contract.py thất bại:\n{result.stderr}\n{result.stdout}"
    assert "14 check groups passed." in result.stdout
