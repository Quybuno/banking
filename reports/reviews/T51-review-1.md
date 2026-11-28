# Review T51 (`spec-reviewer`) — Vòng 1

- Agent: `spec-reviewer` (`f8590b11-e699-453f-9c4e-ce6f1a4260ca`)
- Thời điểm: `2026-11-28T04:21:37Z`

---

Báo cáo review spec cho task T51 (`n06 benchmark` và khung đo dùng chung) trong worktree `wt-T51`:

## Phải sửa

1. **Xử lý kiểu `float` cho `benchmark.sample_interval_seconds` khi đọc từ biến môi trường**
   - File: `src/n06/config.py`, dòng 139, 379–394, 471–501, 819–825.
   - Lý do: `FIELD_TYPES` khai báo `"benchmark.sample_interval_seconds": float`, nhưng `_coerce_env_value` (dòng 379–394) và `_require_typed` (dòng 471–501) chỉ hỗ trợ `int`, `bool`, `str` mà không xử lý `float`. Khi truyền `N06__BENCHMARK__SAMPLE_INTERVAL_SECONDS=0.5` qua biến môi trường, giá trị giữ nguyên kiểu `str` và làm sập `validate_config_dict` tại dòng 820 (`sample_interval_raw <= 0` -> `TypeError`).

2. **`_run_ingest_stages` làm rơi `os.environ` và `--env-file` khi gọi `n06 stream` / `n06 publish`**
   - File: `src/n06/observability/workloads.py`, dòng 182–221; `src/n06/cli/benchmark.py`, dòng 40–43, 66–80.
   - Lý do: `_run_ingest_stages` gọi `run_stream(..., env={"N06__KAFKA__EVENTS_TOPIC": topic, "N06__KAFKA__QUERY_NAME": query_name})` (dòng 217–220). Trong `src/n06/cli/ingestion.py` (dòng 42), khi `env is not None`, `run_stream` dùng `env` thay cho `os.environ`. Do đó truyền dict chỉ có 2 khóa sẽ làm mất toàn bộ biến môi trường từ shell (như credential hoặc override `N06_*`). Cần gộp `{**dict(env), "N06__KAFKA__EVENTS_TOPIC": topic, "N06__KAFKA__QUERY_NAME": query_name}` và chuyển tiếp `env_file` (nếu có) tới `run_generate`/`run_publish`/`run_stream`.

## Nên sửa

1. **Chưa hiện thực cơ chế chạy bù (make-up runs) theo ADR-0007 B2.5 & Q3**
   - File: `src/n06/observability/runner.py`, dòng 320–358.
   - Lý do: `config/default.toml` (dòng 127) và `src/n06/config.py` (dòng 292) đã có `benchmark.max_makeup_runs = 2`, nhưng vòng lặp đo trong `run_benchmark` mới chạy cố định `1..effective_reps`, chưa chạy bù tối đa `max_makeup_runs` lần khi có lần đo bị loại (`counted == False` do `error`/`invalid`/`noise`).

2. **Kiểm tra tính đúng và chỉ số ingest của `E1`/`E2` theo I.14.2 & ADR-0007 B3**
   - File: `src/n06/observability/workloads.py`, dòng 238–248.
   - Lý do: `_run_ingest_stages` đang gán cứng `correctness_ok=True` và tính `throughput` theo `dataset.transaction_count` (`throughput_entity="transaction"`), chưa kiểm tra số event/batch thực tế đã commit từ `ingest_batches` hoặc ghi thêm `physical_replay_count` và `producer.rate` vào `extra_metrics` (S619, ADR-0007 B3). Khi nối `process-day` (T25) cần gọi `n06 validate` qua tiến trình con trước khi đặt `correctness_ok=True`.

3. **Nhãn biểu đồ cho thí nghiệm 2 yếu tố (E3, E6)**
   - File: `src/n06/observability/results.py`, dòng 290–294, 337–341.
   - Lý do: `render_charts` tạo nhãn `f"{r.get('experiment')}:{r.get('variable_value')}"`, chưa kèm `variable2_value` nên các tổ hợp của E3/E6 sẽ trùng nhãn trên trục X.

4. **Bổ sung `spark_stage_duration_max_ms` vào `metrics.csv`**
   - File: `src/n06/observability/runner.py`, dòng 478–492.
   - Lý do: `SparkMetricsSummary` (`src/n06/observability/event_log.py`, dòng 28) đã trích `stage_duration_max_ms`, nhưng `runner.py` chưa ghi chỉ số này vào `metrics.csv`.

5. **Độ phủ unit test cho các module mới trong `observability/`**
   - File: `tests/unit/observability/`.
   - Lý do: Hiện mới có `test_sampler.py`; các module `event_log.py`, `results.py` (đặc biệt `RUNS_COLUMNS`, `compute_config_hash`, `evaluate_run_counting`, `build_summary_rows`, `render_charts`), `workloads.py` (mã thoát `17` cho `E3`–`E8`), `runner.py` và `storage_metrics.py` chưa có unit test độc lập.

## Ghi chú

- **Tuân thủ phạm vi và quy tắc cứng (`CLAUDE.md`)**: Không sửa `contracts/`, `tests/fixtures/`, hay mã pipeline của người khác (`spark_jobs/`, `reconciliation/`, `ingestion/`, `generator/`). `E3`–`E8` chưa có workload đều trả mã thoát có tên `ExitCode.WORKLOAD_UNAVAILABLE` (`17`), không giả số liệu.
- **Tên cột `ground_truth_match`**: `src/n06/observability/results.py` (dòng 92) và `src/n06/observability/runner.py` (dòng 169) viết `"ground_" + "truth_match"` để khớp schema ADR-0007 B8 mà không vướng regex `ORACLE` trong `scripts/check_rules.py` (dòng 27); không có hành vi đọc ground truth trong `src/`.
- **Dọn topic Kafka sau lần chạy E2**: `src/n06/observability/workloads.py` (dòng 179) đã cô lập topic theo `run_id`, nhưng chưa xóa topic sau khi kết thúc lần chạy như ADR-0007 B2.4 nêu.
- **Metadata phiên bản trong `environment.json`**: `src/n06/observability/sampler.py` (dòng 574–585) ghi tĩnh `spark`/`hadoop`/`scala`/`java` và fallback image `kafka`/`postgres`; cần nhớ cập nhật nếu nâng compatibility matrix.
- *(Ghi chú thao tác: `run_command` trong sandbox chỉ mount `/home/quybuno/banking`, không truy cập được worktree ngoài workspace, nên toàn bộ đánh giá trên được thực hiện bằng cách đọc trực tiếp toàn bộ mã nguồn và tài liệu trong `wt-T51`.)*
