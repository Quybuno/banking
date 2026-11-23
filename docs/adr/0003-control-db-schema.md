# ADR-0003: Lược đồ Control DB và cơ chế migration

- Trạng thái: Accepted
- Người viết: B (Quang)
- Người duyệt: A (Mạnh), C (Quý)
- Ngày: 2026-11-23
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 8.1, 8.2, 10, 11

## Bối cảnh
Đặc tả Phần I mục 8.1 quy định cơ chế nhất quán cho object storage dựa trên control DB PostgreSQL (`n06_control`):
- **Streaming (Raw, Bronze):** *"Sau khi mọi output của batch đã ghi xong, upsert một dòng vào bảng control `ingest_batches` với khóa `(query_name, batch_id)`, kèm khoảng offset theo topic-partition và số bản ghi."*
- **Batch (Silver, Gold):** *"Chỉ sau thành công mới cập nhật bảng control PostgreSQL trong một transaction để trỏ `current_snapshot` của `(dataset, tầng, entity, business_date)` tới `run_id` mới; có unique constraint trên khóa đó."* và *"Mỗi run lưu row count, danh sách input version, config hash, schema/rule/code version."*
- **Mart & Vận hành:** Mục 8.2 yêu cầu lưu trạng thái `gold_committed`, `mart_published`, snapshot id trong bảng run; mục 10 yêu cầu bảng `runs` và `quality_metrics`.
- **Thư mục migration:** `sql/migrations/README.md` đã chỉ định `sql/migrations/` chứa migration cho control DB (B) và mart (C).

Đặc tả chưa chốt chi tiết tên cột, kiểu dữ liệu, khóa duy nhất của `quality_metrics` và cách tổ chức file migration/runner.

## Quyết định

### 1. Vị trí và định dạng migration
- Thư mục migration của control DB đặt tại `sql/migrations/control/`.
- Mỗi phiên bản gồm cặp file thuần SQL PostgreSQL 16.6:
  - `NNNN_<ten_ngan>.up.sql`
  - `NNNN_<ten_ngan>.down.sql`
- Tuyệt đối không lưu secret, credential hay giá trị môi trường trong file migration.
- Kết nối control DB luôn lấy từ `load_config().postgres.control` (`PostgresDbConfig` trong `src/n06/config.py`).

### 2. Lược đồ các bảng control DB
1. **`schema_migrations`**: theo dõi phiên bản migration đã áp dụng (`version TEXT PRIMARY KEY`, `name TEXT NOT NULL`, `applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`).
2. **`ingest_batches`** (streaming Raw/Bronze):
   - `id BIGSERIAL PRIMARY KEY`
   - `query_name TEXT NOT NULL`
   - `batch_id BIGINT NOT NULL`
   - `dataset_id TEXT NOT NULL`
   - `ingestion_date DATE NOT NULL`
   - `topic_offsets JSONB NOT NULL`
   - `record_count BIGINT NOT NULL CHECK (record_count >= 0)`
   - `raw_path TEXT NOT NULL`
   - `bronze_path TEXT NOT NULL`
   - `status TEXT NOT NULL DEFAULT 'COMMITTED'`
   - `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
   - `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
   - Ràng buộc duy nhất: `CONSTRAINT uq_ingest_batches_query_batch UNIQUE (query_name, batch_id)`.
3. **`runs`** (theo dõi mỗi lần chạy batch):
   - `run_id TEXT PRIMARY KEY`
   - `dataset_id TEXT NOT NULL`
   - `job_name TEXT NOT NULL`
   - `profile TEXT NOT NULL`
   - `business_date_from DATE`
   - `business_date_to DATE`
   - `status TEXT NOT NULL` (`RUNNING`, `SUCCEEDED`, `FAILED`, `GOLD_COMMITTED`, `MART_PUBLISHED`)
   - `row_count BIGINT NOT NULL DEFAULT 0 CHECK (row_count >= 0)`
   - `input_versions JSONB NOT NULL DEFAULT '[]'::jsonb`
   - `config_hash TEXT NOT NULL`
   - `schema_version TEXT NOT NULL`
   - `rule_version TEXT NOT NULL`
   - `code_version TEXT NOT NULL`
   - `semantic_hash TEXT`
   - `error_summary TEXT`
   - `started_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
   - `finished_at TIMESTAMPTZ`
4. **`current_snapshot`** (con trỏ phiên bản hiện hành của Silver/Gold):
   - `id BIGSERIAL PRIMARY KEY`
   - `dataset_id TEXT NOT NULL`
   - `layer TEXT NOT NULL`
   - `entity TEXT NOT NULL`
   - `business_date DATE NOT NULL`
   - `run_id TEXT NOT NULL REFERENCES runs(run_id)`
   - `output_path TEXT NOT NULL`
   - `manifest_path TEXT`
   - `semantic_hash TEXT`
   - `updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
   - Ràng buộc duy nhất: `CONSTRAINT uq_current_snapshot_key UNIQUE (dataset_id, layer, entity, business_date)`.
5. **`quality_metrics`** (chỉ số chất lượng theo run/tầng/entity/ngày):
   - `id BIGSERIAL PRIMARY KEY`
   - `run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE`
   - `dataset_id TEXT NOT NULL`
   - `layer TEXT NOT NULL`
   - `entity TEXT NOT NULL`
   - `business_date DATE NOT NULL`
   - `metric_name TEXT NOT NULL`
   - `metric_value BIGINT NOT NULL`
   - `status TEXT NOT NULL DEFAULT 'PASS'` (`PASS`, `WARN`, `FAIL`)
   - `details JSONB NOT NULL DEFAULT '{}'::jsonb`
   - `created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`
   - Ràng buộc duy nhất: `CONSTRAINT uq_quality_metrics_key UNIQUE (run_id, layer, entity, business_date, metric_name)`.

### 3. Runner Python (`src/n06/control/`)
- Cung cấp `src/n06/control/migrations.py` đọc các cặp `.up.sql` / `.down.sql` từ `sql/migrations/control/`, xây dựng DSN từ `PostgresDbConfig` (không lưu mật khẩu trong log/lỗi), và thực thi `migrate_up` / `migrate_down` / `get_applied_versions` trong transaction.
- Hỗ trợ nhận executor/connection trừu tượng (tương thích `psycopg` hoặc `psql` CLI) để không khóa cứng vào một thư viện duy nhất khi kiểm thử đơn vị.

## Phương án đã cân nhắc
- **Dùng Alembic / SQLAlchemy**: Không chọn ở giai đoạn này vì dự án đang giữ `pyproject.toml` tối giản không thêm ORM nặng, trong khi schema control chỉ có 5 bảng cần kiểm soát DDL tường minh theo mục 8.1.
- **Đặt migration ở `migrations/control/` ở root**: Không chọn vì repo đã có `sql/migrations/` và `sql/migrations/README.md` cho mục đích này.

## Hệ quả
- **T21**: Tạo migration SQL và runner tại `sql/migrations/control/` và `src/n06/control/`.
- **T22 (`n06 stream`)**: Upsert vào `ingest_batches` dùng `ON CONFLICT (query_name, batch_id) DO UPDATE`.
- **T24 (commit batch)**: Ghi `runs` và upsert `current_snapshot` dùng `ON CONFLICT (dataset_id, layer, entity, business_date) DO UPDATE` trong một transaction.
- **T30–T34 (quality)**: Ghi các chỉ số chất lượng vào `quality_metrics`.
