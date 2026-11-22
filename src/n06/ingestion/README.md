# src/n06/ingestion/

**Mục đích:** Lệnh `stream` (một query foreachBatch ghi Raw + Bronze), `wait-ingestion`, `ingest-statements`.

**Người sở hữu:** A (Mạnh); giao thức commit do B duyệt

**Đặc tả liên quan (docs/spec.md):** Phần I mục 6, 8.1 (streaming)

**Quy tắc:** Ghi theo đường dẫn xác định bởi batch_id; upsert `ingest_batches` sau khi mọi output đã ghi.
