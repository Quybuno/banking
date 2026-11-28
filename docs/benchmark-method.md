# Phương pháp đo benchmark (`n06 benchmark`)

Tài liệu này mô tả cách vận hành lệnh `n06 benchmark` theo `docs/spec.md` Phần I mục 14, `docs/adr/0007-do-benchmark.md` (Accepted) và `docs/adr/0013-metabase-airflow.md` (Accepted).

## 1. Điều kiện môi trường khi đo (ADR-0007 B2, ADR-0013)

- **Chỉ bật profile `core`** (`kafka`, `minio`, `postgres`, `spark`). `airflow-*` (profile `orchestration`) và `metabase` (profile `bi`) phải dừng trước khi đo để tránh tranh chấp RAM trên máy 9 GB.
- Profile `benchmark` (`config/benchmark.toml`) đặt `services.airflow_enabled = false`, `services.metabase_enabled = false`, dùng bucket riêng `n06-benchmark-lakehouse`.
- Mặc định `n06 benchmark` từ chối chạy nếu cây Git bẩn (`git_dirty = true`); chỉ bỏ qua khi truyền `--allow-dirty` (trạng thái bẩn được ghi vào cột `git_dirty`).

## 2. Lệnh `n06 benchmark`

```bash
uv run --extra benchmark n06 benchmark --experiment E1 --level 1000 --repeat 1
```

Tham số chính:
- `--experiment`: mã thí nghiệm (`E1`..`E8`).
- `--level` / `--levels`: một hoặc nhiều mức biến thử nghiệm (mặc định lấy `benchmark.smoke_level` trong `config/default.toml`).
- `--repeat` / `--repetitions`: số lần đo (`benchmark.repetitions`, mặc định `3`).
- `--warmup`: số lần khởi động cold cache (`benchmark.warmup`, mặc định `1`).
- `--profile`: mặc định `benchmark`.
- `--seed`, `--as-of-time`, `--output-dir`, `--generator-config`, `--allow-dirty`, `--share-baseline`, `--json`.

## 3. Tách giai đoạn và quy tắc đếm

- **Thời gian sinh dữ liệu** (`generation_seconds`, `generator_peak_rss_mb`) được đo riêng một lần cho mỗi mức và ghi vào `metrics.csv`, **không** cộng vào `wall_seconds`.
- **Chuỗi thời gian tài nguyên**: `ResourceSampler` lấy mẫu mỗi `benchmark.sample_interval_seconds` giây cho CPU/RAM/swap của host, từng container (`docker stats --no-stream`), và RSS cây tiến trình (`process_tree_rss_mb`), lưu vào `timeseries/<run_id>.csv`.
- **Spark Event Log**: khi `spark.event_log_enabled = true`, parser trích `input_rows`, `output_rows`, `shuffle_read_bytes`, `shuffle_write_bytes`, `spill_memory_bytes`, `spill_disk_bytes`, `stage_duration_total_ms`, `task_duration_median_ms`, `task_duration_max_ms`, `task_max_median_ratio`, `task_count`, `spark_event_log_ref`.
- **Quy tắc đếm (ADR-0007 B6, Q9)**: chỉ các lần `phase = measured`, `status = ok`, `correctness_ok = true`, `swap_peak_mb == 0`, và `ram_available_start_mb >= driver_memory + noise_min_free_ram_overhead_mb` mới có `counted = true` và được đưa vào `summary.csv` (trung vị, min–max) và `charts/*.png`.

## 4. Trạng thái workload trên nhánh hiện tại

- **Đã nối thật**: `E1` (chạy `generate` tách riêng + `publish` + `stream --available-now` + `wait-ingestion` + `ingest-statements` + `process-day`), `E2` (`publish` rate=0 + `stream --available-now` + `wait-ingestion`).
- **Chưa có workload** (`E3`–`E8`): trả mã thoát `17` (`WORKLOAD_UNAVAILABLE`) kèm thông báo rõ ràng, không giả số liệu.

## 5. Thư mục đầu ra (`reports/benchmark/<batch_id>/`)

- `runs.csv`, `runs.json`: chi tiết từng lần chạy (gồm cả warm-up, lỗi, sai, nhiễu).
- `metrics.csv`: metric dài (`run_id, metric, value, unit, scope`).
- `summary.csv`, `summary.json`: tổng hợp trung vị, min–max trên `counted = true`.
- `not_run.csv`: các mức bị từ chối hoặc không chạy được kèm bằng chứng đo được.
- `timeseries/<run_id>.csv`: chuỗi thời gian CPU/RAM/swap/container.
- `environment.json`: thông số phần cứng, container limits và phiên bản lúc bắt đầu phiên.
- `charts/*.png`: biểu đồ vẽ bằng `matplotlib` từ `summary.csv`, ghi `batch_id` và `commit_sha`.
