# Kịch bản Demo cho Giảng viên — N06

Tài liệu hướng dẫn 5 kịch bản demo cốt lõi bám sát `docs/spec.md` Phần I mục 12 & 15.

> **Lưu ý:** Giả định đã hoàn tất bước khởi tạo môi trường (`uv sync --extra dev`, `n06 init-local --profile demo`, `docker compose --profile core up -d --wait`, `n06 init-local --profile demo --require-buckets`, `n06 doctor --profile demo`).

## Kịch bản 1: Chạy sạch — Mọi khóa đều `MATCHED`
1. Sinh dữ liệu sạch từ `contracts/N06_INPUT_CONTRACT_V1/config/generator.demo-clean.json`:
   ```bash
   n06 generate --generator-config contracts/N06_INPUT_CONTRACT_V1/config/generator.demo-clean.json --out data/demo-clean
   ```
2. Đẩy sự kiện lên Kafka, ingest Raw/Bronze và chờ đủ dòng:
   ```bash
   n06 publish --profile test --source-dir data/demo-clean/source --allow-non-empty-topic
   n06 stream --profile default --available-now
   n06 wait-ingestion --profile default --dataset-id n06-demo-clean --source-dir data/demo-clean/source
   ```
3. Nạp sao kê + `control_report.json` và chạy `backfill`:
   ```bash
   n06 ingest-statements --profile default --source-dir data/demo-clean/source --dataset-id n06-demo-clean --as-of-time 2026-01-05T00:00:00Z
   n06 backfill --profile default --dataset-id n06-demo-clean --from-date 2026-01-01 --to-date 2026-01-03 --as-of-time 2026-01-05T00:00:00Z
   ```
4. **Kỳ vọng:** `reconciliation.status_counts` có 100% `MATCHED`, `quarantine_records = 0`, `key_conflicts = 0`. Chạy lại `n06 backfill` cùng `as_of_time` giữ nguyên kết quả.

## Kịch bản 2: `demo-faults` — Hiện đủ 8 trạng thái đối soát
- Dùng cấu hình `contracts/N06_INPUT_CONTRACT_V1/config/generator.demo-faults.json` với `--profile demo` (có policy `[quality]` công khai cho phép cách ly lỗi trong ngưỡng: `max_quarantine_rate_bps = 100`, `max_conflict_count = 50`).
- Chạy `generate` → `publish` → `stream --available-now` → `wait-ingestion` → `ingest-statements` → `backfill --profile demo --as-of-time 2026-01-04T00:00:00Z`.
- Kiểm tra bằng truy vấn `sql/dashboard_queries/04_reconciliation_status_breakdown.sql`: hiển thị đầy đủ 8 trạng thái (`MATCHED`, `PENDING`, `MISSING_EXTERNAL`, `MISSING_INTERNAL`, `AMOUNT_MISMATCH`, `CURRENCY_MISMATCH`, `STATUS_MISMATCH`, `DUPLICATE_OR_CONFLICT`).

## Kịch bản 3: Negative suite vào Quarantine
- Bộ dữ liệu lỗi nghiêm trọng (JSON hỏng, tiền âm, thiếu trường bắt buộc, bút toán mất cân bằng) được kiểm chứng tự động qua `tests/integration/test_quarantine_pipeline.py`:
  ```bash
  pytest tests/integration/test_quarantine_pipeline.py -v
  ```
- **Kỳ vọng:** Bản ghi hỏng bị cách ly sang tầng `quarantine/records` kèm `reason_code`, không lọt vào Silver/Gold.

## Kịch bản 4: Cổng chất lượng chặn publish (`QUALITY_GATE_BLOCKED`)
- Chạy `n06 process-day` (hoặc `n06 backfill`) trên dataset `demo-faults` nhưng dùng `--profile default` (ngưỡng chặt `max_quarantine_rate_bps = 0`, `max_conflict_count = 0`).
- **Kỳ vọng:** `process-day` ghi `quality_metrics`, in `N06_PROCESS_DAY_ERROR={"code": "QUALITY_GATE_BLOCKED", ...}`, thoát mã `14`, **không** cập nhật `current_snapshot` và `backfill` dừng ngay, không chạy `load-mart`.

## Kịch bản 5: Ba mốc `as_of_time` (T1 / T2 / T3)
- Trên cùng dataset có sao kê đến trễ:
  - **T1** (trong hạn `allowed_lateness_seconds`, chưa có sao kê): các khóa chưa thấy sao kê ở trạng thái `PENDING`.
  - **T2** (quá hạn `allowed_lateness_seconds`, vẫn chưa có sao kê): chuyển sang `MISSING_EXTERNAL` với `discrepancy_age_seconds > 0`.
  - **T3** (sau khi sao kê trễ được giao qua `ingest-statements --as-of-time <T3>` và chạy lại `backfill --as-of-time <T3>`): khóa được cập nhật sang `MATCHED` (hoặc sai lệch tương ứng), tổng số match key không tăng sai.
