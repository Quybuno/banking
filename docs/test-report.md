# Báo cáo kiểm thử — N06

Báo cáo tổng hợp trạng thái kiểm thử trung thực theo `docs/spec.md` Phần I mục 15–16. Chỉ ghi `PASS` cho lệnh đã chạy thật; các phần chưa chạy ghi rõ `NOT_RUN`.

## 1. Cổng tĩnh & Hợp đồng dữ liệu

| Hạng mục | Lệnh | Kết quả | Ghi chú |
|---|---|---|---|
| Kiểm tra hợp đồng dữ liệu | `python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py` | **PASS** (14/14) | Sinh `VERIFICATION_REPORT.json` |
| Kiểm tra quy tắc cứng | `python scripts/check_rules.py` | **PASS** | Không rò ground truth, không import `tests/` từ `src/` |
| Lint & Format | `ruff check . && ruff format --check .` | **PASS** | 0 lỗi |
| Unit tests | `pytest tests/unit -q` | **PASS** | Toàn bộ bộ unit test trong `tests/unit/` |

## 2. Kiểm thử tích hợp (Docker)

| Hạng mục | Lệnh | Kết quả | Ghi chú |
|---|---|---|---|
| Backfill end-to-end & idempotent (T42) | `pytest tests/integration/test_backfill_e2e.py -v` | **PASS** | Chạy sạch 50 giao dịch + chạy lại giữ nguyên số dòng mart |
| Dashboard SQL role chỉ đọc (T43) | `pytest tests/integration/test_dashboard_queries.py -v` | **PASS** | 6 truy vấn chạy bằng `n06_mart_ro`, chặn `DELETE` |
| E1 smoke benchmark (T51) | `pytest tests/integration/test_benchmark_e1_smoke.py -v` | **PASS** | Mức 1.000 giao dịch, sinh đủ CSV/JSON/charts |
| Toàn bộ `tests/integration` cùng lúc | `pytest tests/integration -q` | **NOT_RUN** | Không chạy gộp để tránh tranh chấp RAM/Docker (chạy tách từng nhóm) |
| Đợt đo dài benchmark E1–E8 | `n06 benchmark --experiment E1..E8` | **NOT_RUN** | Thuộc T52, T63–T65 (ngoài phạm vi T53 phần tài liệu/demo) |
| Dựng dashboard Metabase trên giao diện & chụp ảnh | Thủ công theo `sql/dashboard_queries/README.md` | **NOT_RUN** | Cần thao tác giao diện trình duyệt sau khi bật profile `bi` |
