"""Kiểm tra quy tắc cứng của N06 trên mã nguồn. Chạy: python scripts/check_rules.py

Dùng trong CI và Stop hook. Chỉ dùng thư viện chuẩn. Exit 1 nếu vi phạm.
Các kiểm tra ở đây là lưới chặn thô; không thay review của người.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "n06"
PROTECTED = ("contracts/", "tests/fixtures/")

# Mẫu cấu hình triển khai không được nằm trong src/ (đặc tả Phần I mục 3).
HARDCODE = [
    (re.compile(r"\blocalhost\b|127\.0\.0\.1"), "hostname cố định"),
    (re.compile(r":(9092|9000|9001|5432|8080|3000)\b"), "port cố định"),
    (re.compile(r"['\"]s3a?://"), "đường dẫn bucket cố định"),
    (re.compile(r"n06-acceptance-v1"), "dataset_id của fixture"),
    (re.compile(r"['\"]20\d\d-\d\d-\d\d"), "ngày cố định"),
]
PLACEHOLDER = re.compile(r"\bTODO\b|\bFIXME\b|raise NotImplementedError")
ORACLE = re.compile(r"expected_results|ground_truth|tests[./]harness|from tests\b|import tests\b")


def changed_files() -> list[str]:
    base = os.environ.get("N06_BASE_REF", "")
    cmds = [["git", "diff", "--name-only", f"{base}...HEAD"]] if base else []
    cmds += [["git", "diff", "--name-only", "HEAD"], ["git", "ls-files", "--others", "--exclude-standard"]]
    out: set[str] = set()
    for cmd in cmds:
        try:
            res = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True)
            out.update(line for line in res.stdout.splitlines() if line)
        except (OSError, subprocess.CalledProcessError):
            pass
    return sorted(out)


def main() -> int:
    errors: list[str] = []

    if os.environ.get("N06_ALLOW_CONTRACT_CHANGE") != "1":
        for f in changed_files():
            if f.startswith(PROTECTED):
                errors.append(f"{f}: thư mục được bảo vệ, chỉ đổi qua PR có nhãn contract-change")

    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        for no, line in enumerate(text.splitlines(), 1):
            code = line.split("#", 1)[0]
            for pattern, why in HARDCODE:
                if pattern.search(code):
                    errors.append(f"{rel}:{no}: {why}: {line.strip()}")
            if PLACEHOLDER.search(line):
                errors.append(f"{rel}:{no}: placeholder thay nghiệp vụ: {line.strip()}")
            # Chỉ generator được ghi ground truth; không module nào trong src đọc oracle của test.
            if ORACLE.search(code) and not rel.startswith("src/n06/generator/"):
                errors.append(f"{rel}:{no}: pipeline tham chiếu oracle/test harness: {line.strip()}")
            if "tests" in code and re.search(r"^\s*(from|import)\s+tests", code):
                errors.append(f"{rel}:{no}: src import tests")

    if errors:
        print("Vi phạm quy tắc N06:", file=sys.stderr)
        for e in errors:
            print("  -", e, file=sys.stderr)
        return 1
    print("check_rules: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
