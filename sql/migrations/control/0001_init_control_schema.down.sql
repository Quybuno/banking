-- Migration 0001 (DOWN): Gỡ bỏ lược đồ Control DB theo thứ tự phụ thuộc ngược

DROP INDEX IF EXISTS idx_quality_metrics_dataset_date;
DROP TABLE IF EXISTS quality_metrics;

DROP TABLE IF EXISTS current_snapshot;

DROP INDEX IF EXISTS idx_runs_dataset_status;
DROP TABLE IF EXISTS runs;

DROP INDEX IF EXISTS idx_ingest_batches_dataset_date;
DROP TABLE IF EXISTS ingest_batches;

DROP TABLE IF EXISTS schema_migrations;
