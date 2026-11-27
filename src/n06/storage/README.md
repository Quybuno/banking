# src/n06/storage/

**Mục đích:** Object store, manifest, giao thức commit batch (run_id, current_snapshot).

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 8.1

**Quy tắc:** Reader chỉ đọc snapshot do current_snapshot trỏ tới.

## Control store qua `psql` (ADR-0012)

- Mọi giá trị đi vào SQL bằng một biến JSON của psql, đặt bằng `\set` ở đầu kịch bản gửi qua **stdin** (không nằm trong argv). Câu lệnh chỉ dùng `:'params'`; khối `DO` đọc giá trị từ bảng tạm. Đây là trích dẫn phía client của psql, không phải tham số phía server.
- `upsert_ingest_batch` dùng `ON CONFLICT (query_name, batch_id) DO UPDATE`; `list_committed_ingest_batches` lọc theo `query_name` vì streaming ghi `dataset_id` NULL (ADR-0004 Q1, migration `0002`).
- Trong đường `psql`, `after_run_update_hook` chạy **trước** khi gửi SQL; kiểm rollback giữa transaction phải gây lỗi ngay trong SQL (`tests/integration/test_control_store_in_image.py`).
