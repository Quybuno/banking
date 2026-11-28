# Runbook vận hành — Restart, Backfill & Drill-down (T62)

Tài liệu vận hành cho N06 theo `docs/spec.md` Phần I mục 8.3, 9, 10 và `docs/adr/0013-metabase-airflow.md`.

## 1. Quản lý dịch vụ & Ngân sách RAM (Restart / Stop)

> **Quy tắc RAM (ADR-0013):** Không bật đồng thời `bi` (Metabase) và `orchestration` (Airflow) cùng Spark nặng trên máy 9 GB RAM.

- **Khởi động hạ tầng lõi (`core`):**
  ```bash
  docker compose --profile core up -d --wait
  ```
- **Bật Metabase xem dashboard (`bi`), dừng sau khi dùng:**
  ```bash
  docker compose --profile core --profile bi up -d --wait
  # Sau khi xem xong:
  docker compose --profile bi stop
  ```
- **Bật Airflow chạy DAG (`orchestration`), dừng `bi` trước khi bật:**
  ```bash
  docker compose --profile bi stop
  docker compose --profile core --profile orchestration up -d --wait
  # Sau khi DAG chạy xong:
  docker compose --profile orchestration stop
  ```
- **Restart một service lỗi (giữ nguyên volume dữ liệu):**
  ```bash
  docker compose restart postgres   # hoặc kafka / minio / spark
  ```
- **Reset sạch toàn bộ dữ liệu (CẢNH BÁO: xóa volume Postgres/MinIO/Kafka, mất dashboard Metabase):**
  ```bash
  docker compose --profile core --profile bi --profile orchestration down -v
  ```

## 2. Xử lý sự cố & Backfill

1. **Khi `process-day` bị `QUALITY_GATE_BLOCKED` (mã 14):**
   - `current_snapshot` không đổi và `backfill` đã dừng, mart giữ nguyên bản hợp lệ trước đó.
   - Kiểm tra bảng `quality_metrics` trong `n06_control` và bản ghi `quarantine/records` trên MinIO.
   - Nếu là dataset demo có lỗi chủ đích, chạy lại với `--profile demo` (ngưỡng `[quality]` công khai).
2. **Khi `load-mart` lỗi sau khi Gold đã commit:**
   - Không cần chạy lại Spark `process-day`; chạy lại chỉ bước mart trên đúng Gold đã commit:
     ```bash
     n06 backfill --profile demo --dataset-id <dataset_id> \
       --from-date <YYYY-MM-DD> --to-date <YYYY-MM-DD> \
       --as-of-time <YYYY-MM-DDTHH:MM:SSZ> --mart-only
     ```
3. **Khi có sao kê đến trễ (late statement):**
   - Nạp sao kê tại mốc `as_of_time` mới rồi chạy lại `n06 backfill` cho khoảng ngày ảnh hưởng:
     ```bash
     n06 ingest-statements --profile demo --dataset-id <dataset_id> --as-of-time <T_new>
     n06 backfill --profile demo --dataset-id <dataset_id> \
       --from-date <from> --to-date <to> --as-of-time <T_new>
     ```

## 3. Drill-down một sai lệch đối soát

1. Mở câu hỏi **`07_discrepancy_drilldown.sql`** trên Metabase (collection `N06`), nhập bộ lọc `external_reference` (hoặc `dataset_id` / `recon_status`).
2. Xem các cột truy vết:
   - `recon_source_pointers`: danh sách đường dẫn file sao kê / bản ghi nguồn tham gia đối soát.
   - `transaction_id`, `account_from`, `account_to`, `transaction_source_pointer`: giao dịch nội bộ và tọa độ nguồn (Bronze/Kafka).
   - `internal_*` vs `statement_*`, `issue_codes`, `discrepancy_age_seconds`: nguyên nhân và tuổi sai lệch.
