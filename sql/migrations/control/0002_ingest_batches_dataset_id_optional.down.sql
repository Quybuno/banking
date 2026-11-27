-- Thất bại rõ ràng nếu còn dòng `dataset_id` NULL; không xóa dữ liệu (ADR-0004 mục 8).
ALTER TABLE ingest_batches ALTER COLUMN dataset_id SET NOT NULL;
