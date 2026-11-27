# Báo cáo Review độc lập — TASK T22b (`task/T22b-stream`)

> **Lưu ý về phạm vi kiểm tra:** Môi trường reviewer hiện tại chỉ có công cụ đọc file (`read_file`), không có công cụ chạy lệnh shell (`git diff`, `pytest`, `ruff`, `verify_contract.py`, `check_rules.py`). Mọi nhận xét dưới đây dựa trên việc đọc trực tiếp mã nguồn, cấu hình, đặc tả và ADR trong worktree `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/wt-22b`.

---

## Kiểm tra 7 hạng mục (`docs/TASKS.md` mục 6.3)

### 1. Các điều kiện hoàn thành của T22b
- **Một query `foreachBatch` ghi Raw + Bronze theo `batch_id`, persist microbatch, upsert `ingest_batches` là bước cuối:** **Đạt (kiểm bằng đọc code).**
  - `src/n06/ingestion/stream_job.py:93-182`: `process_batch` persist `source` (dòng 110), `unpersist` trong `finally` (dòng 182), ghi Raw mode `overwrite` vào `raw_uri` (dòng 133-136), xóa `bronze_uri` cũ rồi ghi từng entity Bronze có dữ liệu (dòng 140-147), sau cùng mới gọi `ctx.store.upsert_ingest_batch(...)` (dòng 153-165).
  - `src/n06/ingestion/stream_job.py:199-218`: một `readStream` (`startingOffsets=earliest`, `maxOffsetsPerTrigger`, `groupIdPrefix`) + một `writeStream.foreachBatch(...)` với `checkpointLocation = layout.checkpoint_uri(cfg, query_name)`.
- **Schema Raw & Bronze (`transaction`, `ledger_entry`, `parse_error`) khớp ADR-0004 (cột, kiểu, nullability):** **Đạt (kiểm bằng đọc code).**
  - Raw (9 cột): `key`, `value`, `topic`, `partition`, `offset`, `kafka_timestamp`, `ingested_at`, `dataset_id`, `ingestion_run_id` (`stream_job.py:99-109, 133-136`). Khớp bảng mục 2 ADR-0004.
  - Bronze `transaction` (26 cột = 6 cột chung + 7 cột envelope có tiền tố `envelope_` cho tên trùng + 13 cột payload, `amount_minor` kiểu `LongType`) và `ledger_entry` (23 cột = 6 + 7 + 10, `amount_minor` kiểu `LongType`): `records.py:25-61`, `stream_job.py:40-54, 79-90`. Khớp mục 3, 4, 5 ADR-0004 và các schema hợp đồng.
  - Bronze `parse_error` (12 cột = 6 cột chung + `dataset_id` + `entity`, `business_key`, `error_code`, `error_detail`, `issue_codes: array<string>`): `stream_job.py:36-37, 81-83`. Khớp mục 6 ADR-0004.
  - *Lưu ý về nullability:* các cột non-null ở dữ liệu thực tế đều không bao giờ mang `None` (cột từ Kafka, `F.lit`, hoặc đã qua JSON Schema); tuy nhiên schema Spark trong `_parsed_schema()` đặt `nullable=True` cho tất cả field vì dùng chung 1 struct phẳng cho cả 3 route (xem mục Ghi nhận).
- **Quy tắc Q1–Q8 và G1–G22 (ADR-0004) và đường dẫn (ADR-0002 mục 1, 2, 6):** **Đạt (kiểm bằng đọc code).**
  - Q1: `IngestBatchRecord(..., dataset_id=None, status="COMMITTED")` (`stream_job.py:157, 163`).
  - Q2/Q6: ghi đè đúng `raw_uri` và xóa/ghi đè đúng `bronze_uri` (`batch_id=<id>/`, không đụng thư mục cha); entity không có dòng không tạo thư mục (`stream_job.py:136, 140-147`).
  - Q3: `layout.topic_offsets` sinh `{"<topic>": {"<partition>": [first, last + 1]}}` (`layout.py:52-59`).
  - Q4: `records._dataset_id` chỉ lấy `envelope.dataset_id` nếu là `str` khớp `DATASET_ID_PATTERN`, không lấy từ payload (`records.py:77-82`).
  - Q5: `records._entity_and_key` suy `entity` từ `event_type` hợp lệ và `business_key` chỉ khi `transaction_id`/`entry_id` là UUID hợp lệ (`records.py:85-97`).
  - Q7: `ingested_at = ctx.clock()` lấy một lần đầu callback, dùng chung cho Raw và Bronze (`stream_job.py:96, 108`).
  - Q8: chỉ gọi `validate_event_envelope(text)` cho dòng `parse_error`, chỉ lọc `ErrorCode.ENVELOPE_PAYLOAD_MISMATCH`, bọc `try/except`, chọn mã chính theo `sort_issue_codes` (`records.py:147-151, 170-176`).
  - G1: xử lý `value is None` -> `SCHEMA_TYPE` trước `bytes(value)`, decode UTF-8 nghiêm ngặt -> `INVALID_JSON`, bắt `ValueError`/`RecursionError` -> `INVALID_JSON`, làm sạch lone surrogate (`records.py:64-66, 119-143`).
  - G9: batch rỗng (`not ranges`) trả về ngay, không ghi file và không upsert (`stream_job.py:123-124`).
  - `ingestion_date`: lấy từ `unix_micros(min(kafka_timestamp))` đổi sang `Asia/Ho_Chi_Minh` (`stream_job.py:119, 126-129`, `layout.py:45-49`), tránh lỗi múi giờ local của PySpark.
  - Checkpoint: `s3a://<bucket>/<checkpoints_prefix>/<query_name>/` (`layout.py:41-42`, `stream_job.py:210`).
- **Cách chạy `n06 stream` qua `docker compose run --rm spark`, bind mount chỉ đọc, hook `--crash-at`:** **Đạt (kiểm bằng đọc code).**
  - `launcher.py:44-73`: `docker compose -f ... --profile core run --rm --no-deps -T`, mount `src`, `config`, `contracts`, `sql` ở chế độ `:ro` vào `/opt/n06/...`, truyền biến `N06*` chỉ bằng tên (`-e NAME`) nên secret không nằm trong argv.
  - `--crash-at` (`after_raw`, `before_upsert`, `after_upsert`) và `--crash-batch` chỉ nằm trong `stream_job.py:228-229`, không lộ ra CLI công khai `n06 stream`; `maybe_crash` dùng `os.kill(os.getppid(), signal.SIGKILL)` + `os._exit(86)` để tắt nóng cả JVM lẫn driver Python.
- **Chạy test thực tế (`ruff`, `pytest`, `verify_contract.py`, `test_stream_recovery.py`):** **Không kiểm chứng được** trong phiên này do không có shell tool.

### 2. Thay đổi trong `contracts/` hoặc `tests/fixtures/`; test bị xóa/skip/nới expected
- **Không kiểm chứng trọn vẹn được** bằng `git diff` vì không có shell tool. Trong các file đã đọc (`tests/unit/ingestion/test_records.py`, `tests/unit/ingestion/test_layout_and_launcher.py`, `tests/integration/test_stream_recovery.py`) không có `pytest.skip` hay nới expected. Cần xác nhận lại bằng `git diff --stat task/T22a-control-store...HEAD` trước khi merge.

### 3. Hardcode endpoint / ngày / seed / quy mô / partition / bucket / topic
- **Đạt (trong các file `src/` đã đọc):** `config.py`, `layout.py`, `records.py`, `launcher.py`, `spark_session.py`, `stream_job.py`, `cli/ingestion.py` đều đọc endpoint, bucket, prefix, topic, `query_name`, `max_offsets_per_trigger`, `trigger_interval_seconds`, `business_timezone`, Spark master/memory/shuffle_partitions từ `AppConfig`. Không có hardcode cấu hình triển khai.

### 4. Dùng `float` cho tiền; đọc ground truth trong pipeline
- **Đạt (kiểm bằng đọc code):**
  - `amount_minor` kiểm chặt `type(field) is not int` (`records.py:159`) và ánh xạ `LongType()` (`stream_job.py:49`). Không dùng `float`.
  - Không có tham chiếu nào tới `ground_truth`, `expected_results`, `fault_assignment`, `canonical`, hay `tests/` trong `src/n06/ingestion/` và `src/n06/cli/ingestion.py`.

### 5. Làm việc ngoài phạm vi task
- **Đạt (trong các file đã đọc):** Các file chạm tới (`config/default.toml`, `config/README.md`, `src/n06/config.py`, `src/n06/cli/__init__.py`, `src/n06/cli/ingestion.py`, `src/n06/ingestion/*`, `tests/unit/ingestion/*`, `tests/integration/test_stream_recovery.py`) đều thuộc phạm vi T22b (kể cả việc thêm `kafka.query_name` theo ADR-0002 Hệ quả). `wait-ingestion` (T22c) được tách rõ ràng.

### 6. Chỗ đặc tả / ADR bị hiểu khác văn bản
- Không phát hiện sai lệch nghiêm trọng với `docs/spec.md` (I.6, I.8.1), ADR-0002, ADR-0004, ADR-0012.
- Ghi chú minh bạch đã có sẵn trong `src/n06/ingestion/README.md:21`: đặc tả Phần I mục 2 nêu *"Có một image job chứa Java, PySpark, các JAR đã ghim và gói `n06`"*, trong khi hiện tại `src/`, `config/`, `contracts/`, `sql/` được bind mount chỉ đọc vào `/opt/n06` (ADR-0012 mục Tác động dòng 83 đã để mở việc này cho kế hoạch T22 và trước khi Airflow gọi job).

### 7. Test có expected tính độc lập không
- **Đạt (kiểm bằng đọc code):**
  - `tests/unit/ingestion/test_records.py`: expected viết tay (route, tên cột, mã lỗi, `dataset_id`, `entity`, `business_key`, `ENVELOPE_PAYLOAD_MISMATCH`), không gọi lại `classify_record` để tính expected.
  - `tests/unit/ingestion/test_layout_and_launcher.py`: URI S3A, mốc ngày VN 16:59:59 vs 17:00:00 UTC, `topic_offsets`, lệnh Docker viết tay độc lập.
  - `tests/integration/test_stream_recovery.py`: tính `expected_values` độc lập bằng `hashlib.sha256` trên byte gốc của file nguồn + 4 message hỏng tự tạo; `error_codes` viết tay `{"INVALID_JSON": 2, "SCHEMA_TYPE": 1, "UNSUPPORTED_SCHEMA_VERSION": 1}`; kiểm tra liên tục offset từ 0, không trùng `(partition, offset)`, G11 và Q7.

---

## Phải sửa (chặn merge)

Không có.

---

## Nên sửa

1. **Validate `--query-name` sớm ngay trên host trong `n06 stream` (`src/n06/cli/ingestion.py:49-52, 83-100`):**
   - Hiện tại `run_stream` không truyền `query_name` vào `cli_overrides` của `load_config` trên host. Nếu người dùng truyền `--query-name "../x"`, lệnh vẫn qua bước kiểm cấu hình host, khởi chạy container Docker + JVM Spark rồi mới thất bại với mã `10` tại `stream_job.py:237`. Nên truyền `cli_overrides={"kafka.query_name": query_name}` (khi `query_name is not None`) để fail-fast ngay trên host.
2. **Bổ sung assert schema cột Bronze trong `tests/integration/test_stream_recovery.py:75, 84, 276-298`:**
   - Script `_INSPECT` đã thu thập `"columns": sorted(frame.columns)` cho từng thư mục Bronze (`entity=transaction`, `entity=ledger_entry`, `entity=parse_error`), nhưng phần kiểm tra ở dòng 276-298 chỉ assert `raw_columns` mà **chưa assert `bronze[folder]["columns"]`**. Nên thêm assert danh sách cột viết tay của cả 3 entity Bronze theo ADR-0004 (bắt được lỗi thiếu/sai tên cột ngay trên Parquet thật).
3. **Dọn dữ liệu MinIO trong `finally` và không hardcode tên prefix trong `tests/integration/test_stream_recovery.py:308-311`:**
   - Dòng 308 đang viết cứng `prefixes = ("raw", "bronze", "checkpoints")` thay vì đọc `(stack.cfg.paths.raw_prefix, stack.cfg.paths.bronze_prefix, stack.cfg.paths.checkpoints_prefix)`, và đặt ở cuối thân test nên nếu một assert trước đó fail, dữ liệu test trên MinIO sẽ không được dọn. Nên đưa bước dọn vào `finally` (tương tự bước xóa dòng Postgres và topic Kafka ở fixture `stack`).
4. **Cải thiện `host_only_addresses` cho chuỗi `bootstrap_servers` nhiều broker hoặc IPv6 (`src/n06/ingestion/launcher.py:37`):**
   - `cfg.kafka.bootstrap_servers.partition(":")[0]` chỉ kiểm tra phần trước dấu `:` đầu tiên, nên bỏ sót trường hợp nhiều broker (`kafka:9092,localhost:19092`) hoặc địa chỉ IPv6 dạng `[::1]:9092`. Nên tách theo dấu phẩy rồi parse host (hỗ trợ `[ipv6]:port`).
5. **Tránh parse lại JSON hỏng không cần thiết trong `records.py:145-150`:**
   - Khi `result.record is None` (ví dụ JSON lỗi cú pháp hoặc mảng lồng 200.000 cấp đã bị `parse_and_validate_json_schema` bắt), `classify_record` vẫn gọi tiếp `_load_object(text)` rồi `_envelope_mismatch(text)` (chạy `validate_event_envelope(text)` thêm lần nữa dù chắc chắn không có `ENVELOPE_PAYLOAD_MISMATCH`). Chỉ cần gọi `_envelope_mismatch(text)` khi `record is not None`.
6. **Sửa comment nhầm và thiếu export trong `src/n06/cli/__init__.py`:**
   - Dòng 241 ghi `# Lệnh 4: n06 publish` ngay trên `stream_parser` (nên sửa lại chú thích cho `stream` và `publish`).
   - `run_stream` đã import ở dòng 32 nhưng chưa có trong `__all__` (dòng 42-64), trong khi `run_generate` và `run_publish` đều có.

---

## Ghi nhận

- **Nullability trong `_parsed_schema()` (`src/n06/ingestion/stream_job.py:40-54`):** Tất cả field của struct UDF đều `nullable=True` để dùng chung cho 3 route; khi ghi ra Parquet, Spark mặc định đọc lại mọi cột Parquet với `nullable=True`. Dữ liệu thực tế vẫn bảo đảm không null cho các cột bắt buộc của ADR-0004.
- **Quyền ghi `event_log_dir` khi UID host khác 1000 (`src/n06/cli/ingestion.py:62-64`, `infra/spark/Dockerfile:79`):** Container chạy user `spark` (UID 1000). Nếu chạy trên máy có UID host khác 1000 với `event_log_enabled = true`, bind mount `data/spark-events` có thể gặp lỗi quyền ghi.
- **Độ phủ ca kiểm thử phục hồi / biên:** `test_stream_recovery.py` đã kiểm tốt 2 mốc quan trọng nhất (`before_upsert` và `after_upsert`) theo I.8.1 bước 5; các ca phụ như `after_raw`, batch rỗng (G9), hay phát lại xóa thư mục entity thừa (Q6) hiện chỉ kiểm ở mức đọc code / unit test riêng lẻ.

---

## Chưa kiểm

Do môi trường reviewer không có công cụ chạy lệnh shell, các mục sau **chưa được thực thi/xác minh độc lập**:
1. `git -C .../wt-22b diff --stat task/T22a-control-store...HEAD` (để khẳng định 100% không có file nào ngoài danh sách đã đọc, đặc biệt trong `contracts/` và `tests/fixtures/`, bị chỉnh sửa).
2. `ruff check . && ruff format --check .`
3. `python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py`
4. `check_rules.py` (kiểm tra cấm hardcode / import `tests/` trong `src/`).
5. `pytest tests/unit -q`
6. `pytest tests/integration/test_stream_recovery.py -v` trên hạ tầng Docker Compose thật (Kafka, MinIO, Postgres, Spark).
