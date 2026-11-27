-- ADR-0004 Q1: một microbatch streaming có thể chứa nhiều dataset hoặc record không đọc được,
-- nên `dataset_id` không bắt buộc ở `ingest_batches` (dataset_id nằm ở từng dòng Raw và Bronze).
ALTER TABLE ingest_batches ALTER COLUMN dataset_id DROP NOT NULL;
