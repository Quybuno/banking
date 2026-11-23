-- Migration 0001 (UP): Khởi tạo lược đồ Control DB theo ADR-0003 và docs/spec.md Phần I mục 8.1

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingest_batches (
    id BIGSERIAL PRIMARY KEY,
    query_name TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    dataset_id TEXT NOT NULL,
    ingestion_date DATE NOT NULL,
    topic_offsets JSONB NOT NULL,
    record_count BIGINT NOT NULL CHECK (record_count >= 0),
    raw_path TEXT NOT NULL,
    bronze_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'COMMITTED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ingest_batches_query_batch UNIQUE (query_name, batch_id)
);

CREATE INDEX IF NOT EXISTS idx_ingest_batches_dataset_date
    ON ingest_batches (dataset_id, ingestion_date);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    profile TEXT NOT NULL,
    business_date_from DATE,
    business_date_to DATE,
    status TEXT NOT NULL CHECK (
        status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'GOLD_COMMITTED', 'MART_PUBLISHED')
    ),
    row_count BIGINT NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    input_versions JSONB NOT NULL DEFAULT '[]'::jsonb,
    config_hash TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    code_version TEXT NOT NULL,
    semantic_hash TEXT,
    error_summary TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_runs_dataset_status
    ON runs (dataset_id, status);

CREATE TABLE IF NOT EXISTS current_snapshot (
    id BIGSERIAL PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    entity TEXT NOT NULL,
    business_date DATE NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs (run_id),
    output_path TEXT NOT NULL,
    manifest_path TEXT,
    semantic_hash TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_current_snapshot_key UNIQUE (dataset_id, layer, entity, business_date)
);

CREATE TABLE IF NOT EXISTS quality_metrics (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs (run_id) ON DELETE CASCADE,
    dataset_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    entity TEXT NOT NULL,
    business_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PASS' CHECK (status IN ('PASS', 'WARN', 'FAIL')),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_quality_metrics_key UNIQUE (run_id, layer, entity, business_date, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_quality_metrics_dataset_date
    ON quality_metrics (dataset_id, business_date);
