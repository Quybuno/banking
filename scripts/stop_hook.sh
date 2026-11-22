#!/usr/bin/env bash
# Stop hook cho Claude Code: chặn agent kết thúc lượt khi kiểm tra còn đỏ.
# Exit 2 = chặn dừng; nội dung stderr được trả lại cho agent để tự sửa.
cd "${CLAUDE_PROJECT_DIR:-$(dirname "$0")/..}" || exit 0
fail=0; log=$(mktemp)
run(){ echo "\$ $*" >>"$log"; "$@" >>"$log" 2>&1 || fail=1; }
run python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py
run python scripts/check_rules.py
if [ -f pyproject.toml ]; then
  run ruff check .
  run ruff format --check .
  run pytest tests/unit -q
fi
if [ "$fail" -ne 0 ]; then
  { echo "Kiểm tra chưa đạt. Sửa nguyên nhân gốc, không sửa contracts/ hay tests/fixtures/, rồi chạy lại."; tail -n 80 "$log"; } >&2
  rm -f "$log"; exit 2
fi
rm -f "$log"; exit 0
