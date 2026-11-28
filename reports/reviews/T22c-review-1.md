# Báo cáo Review độc lập — TASK T22c (`n06 wait-ingestion`)

Nhánh xem xét: `task/T22c-wait-ingestion` tại `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/wt-22c` (so chiếu với `docs/TASKS.md`, `docs/spec.md` I.8.1 & I.10, `docs/adr/0004-schema-raw-bronze.md` mục 9 & G17-G21).
**Phạm vi kiểm tra:** Chỉ đọc mã nguồn và tài liệu (môi trường reviewer không có công cụ chạy lệnh shell).

---

## Đối chiếu 7 hạng mục `docs/TASKS.md` mục 6.3

1. **Điều kiện hoàn thành:**
   - Thuật toán so đa tập `N[h] >= M[h]` trên Raw của các batch có trong `ingest_batches` của `query_name`: **Đạt (qua đọc code)** (`src/n06/ingestion/wait_job.py:32-57`, `src/n06/storage/control_store.py:525-546`).
   - Kiểm tra nguồn bằng bước của producer (manifest, `sha256`, `physical_rows`, từ chối file events ngoài manifest; không dùng chọn key): **Chưa đạt một điểm biên** — `src/n06/ingestion/source_lines.py:35` không từ chối dòng cuối thiếu LF như `producer/source.py:163-164` (xem mục Phải sửa #2).
   - Gắn chỉ đọc riêng `source/` vào `/opt/n06/source-input`, không đọc thư mục gốc chứa ground truth: **Đạt (qua đọc code)** (`src/n06/cli/ingestion.py:66-76`, `src/n06/ingestion/launcher.py:18,62-64`, `src/n06/ingestion/README.md:37`).
   - Lỗi cấu hình thoát ngay mã `10`, timeout thoát mã `14` nêu `missing_lines` và `parse_error_rows`: **Đạt ở đường chính (qua đọc code)** (`src/n06/cli/ingestion.py:55-76,134-136`, `src/n06/ingestion/wait_job.py:95-102,113-126,146-147`), nhưng còn lỗ hổng khi Bronze đang bị ghi đè lúc timeout (xem mục Phải sửa #1).
   - Xử lý thư mục batch đang bị ghi đè dở dang (coi lỗi đọc là chưa sẵn sàng và thử lại): **Chưa đạt** — chỉ bắt `AnalysisException`, bỏ lọt `Py4JJavaError`/`SparkException` khi file Parquet bị xóa giữa pha listing và pha đọc trên S3A (xem mục Phải sửa #1).
   - Unit test & integration test: **Đạt về tính độc lập của expected (qua đọc code)**; **không tự chạy được** để xác nhận PASS/FAIL.
2. **Thay đổi trong `contracts/` hoặc `tests/fixtures/`, xóa/skip/nới expected:** **Không kiểm chứng được toàn diện** vì không chạy được `git diff --stat task/T22b-stream...HEAD` (xem mục Chưa kiểm). Trong các file test mới đã đọc không có `skip` hay nới expected.
3. **Hardcode endpoint/ngày/seed/quy mô/partition/bucket/topic:** **Đạt (qua đọc code)** — không có hardcode trong `src/n06/cli/ingestion.py`, `src/n06/ingestion/launcher.py`, `src/n06/ingestion/source_lines.py`, `src/n06/ingestion/wait_job.py`. `DEFAULT_WAIT_TIMEOUT_SECONDS = 300.0` override được qua `--timeout-seconds`; `/opt/n06/source-input` là điểm mount nội bộ trong container.
4. **Dùng float cho tiền / đọc ground truth trong pipeline:** **Đạt (qua đọc code)** — không xử lý tiền; chỉ gắn và đọc `source/` (`manifests/` + `events/`).
5. **Ngoài phạm vi task:** **Đạt (qua đọc code)** — các thay đổi đã đọc bám đúng `wait-ingestion`; phần đọc fixture nghiệm thu được giữ là việc mở đúng theo ADR-0004 (`src/n06/ingestion/README.md:41`).
6. **Chỗ hiểu khác đặc tả/ADR:** Có 2 điểm chính (mục Phải sửa #1, #2) và 1 điểm về UTF-8/raw bytes (mục Nên sửa #2).
7. **Expected test độc lập:** **Đạt (qua đọc code)** — `tests/unit/ingestion/test_wait_source_and_cli.py` viết tay expected; `tests/integration/test_wait_ingestion.py:104-105` đếm dòng trực tiếp bằng `p.read_bytes().count(b"\n")`, kiểm tra cả ca thêm dòng mới lẫn thêm 1 bản sao giống từng byte (`one-more-copy`) để chứng minh so đa tập.

---

## Phải sửa (chặn merge)

1. **Bắt thiếu ngoại lệ khi thư mục batch đang bị ghi đè dở dang trên S3A (lệch ADR-0004 mục 9 dòng 192):**
   - **Trích ADR-0004 mục 9 (dòng 192):** *"Khi query phát lại một batch đã commit, thư mục bị ghi đè nên tạm thời dở dang với bên đọc (ADR-0002 mục 2); lệnh coi lỗi đọc là 'chưa sẵn sàng' và thử lại"*.
   - **Code hiện tại (`src/n06/ingestion/wait_job.py:88-91`):**
     ```python
     try:
         missing = missing_lines(expected, spark, [batch.raw_path for batch in batches])
     except AnalysisException as exc:  # batch đang được phát lại: thư mục tạm thời dở dang
         print(f"N06_WAIT chưa đọc được Raw ({type(exc).__name__}), thử lại", flush=True)
     ```
   - **Vấn đề:** Khi `n06 stream` phát lại batch và chạy `.mode("overwrite").parquet(raw_uri)` trên MinIO/S3A, `AnalysisException` chỉ xảy ra nếu thư mục trống/chưa có schema ngay lúc driver lập kế hoạch. Trường hợp race phổ biến hơn là driver đã liệt kê danh sách `part-*.parquet`, ngay sau đó `overwrite` xóa file cũ đi trước khi executor kịp đọc -> S3A trả `FileNotFoundException` và PySpark ném `Py4JJavaError` (bọc `org.apache.spark.SparkException: Job aborted due to stage failure... FileNotFoundException`), **không phải `AnalysisException`**. Ngoại lệ này thoát thẳng ra `main()` làm lệnh chết sớm trước timeout mà không thử lại.
   - Ngoài ra, ở `src/n06/ingestion/wait_job.py:101`:
     ```python
     "parse_error_rows": None if missing == 0 else parse_error_rows(spark, batches),
     ```
     Nếu hết timeout đúng lúc `n06 stream` đang xóa và ghi lại `bronze_uri` (`stream_job.py:141`), `parse_error_rows` không bắt lỗi đọc sẽ văng ngoại lệ ra `main()`, làm mất luôn output `N06_WAIT_RESULT=` (không báo được `missing_lines` và `parse_error_rows`).
   - **Yêu cầu sửa:** Mở rộng bắt lỗi đọc tạm thời trong vòng lặp `wait()` (bao gồm `Py4JJavaError` khi thông điệp/nguyên nhân là lỗi đọc file S3A/Parquet như `FileNotFoundException` / `Path does not exist` / `SparkException` khi đọc stage, hoặc tách rõ hàm kiểm tra lỗi đọc tạm thời so với lỗi kết nối/cấu hình), và bảo vệ `parse_error_rows()` để không làm mất báo cáo `missing_lines` khi timeout.

2. **Không từ chối dòng cuối thiếu LF trong `dataset_event_files`, gây lệch giữa `total` và `expected_lines` (lệch ADR-0004 mục 9 bước 1):**
   - **Trích ADR-0004 mục 9 bước 1 (dòng 176):** *"Đọc và kiểm nguồn bằng đúng bước của producer (liệt kê theo manifest, kiểm `sha256`, `physical_rows`, từ chối file events không có trong manifest)"*.
   - **So sánh code:**
     - `src/n06/producer/source.py:163-165`:
       ```python
       if not raw.endswith(b"\n"):
           raise SourceDataError(f"Dòng cuối thiếu ký tự LF tại {where}.")
       rows += 1
       ```
     - `src/n06/ingestion/source_lines.py:33-35`:
       ```python
       for line in handle:
           digest.update(line)
           rows += line.endswith(b"\n")
       ```
   - **Vấn đề:** Nếu một file events có nội dung `b"a\nb"` (dòng cuối thiếu LF) và manifest ghi `physical_rows = 1`, `producer` từ chối nhưng `dataset_event_files` chấp nhận với `total = 1`. Sau đó `wait_job.py:35-36` gọi `spark.read.option("lineSep", "\n").text(paths)` sẽ đọc ra **2 dòng** (`"a"` và `"b"`), làm `expected` chứa 2 dòng trong khi `expected_lines` báo `1` và `missing_lines` có thể lớn hơn `expected_lines`.
   - **Yêu cầu sửa:** Kiểm tra `if not line.endswith(b"\n"): raise SourceDataError(...)` trước khi tăng `rows` trong `src/n06/ingestion/source_lines.py`, khớp với `producer/source.py`.

---

## Nên sửa

1. **Validate `timeout_seconds` và `--query-name ""` ngay trong `src/n06/ingestion/wait_job.py:111-121`:**
   - Hiện chỉ `src/n06/cli/ingestion.py:134` kiểm `0 < timeout_seconds < float("inf")`. Nếu gọi trực tiếp `wait_job.py` với `--timeout-seconds nan` hoặc `<= 0`, `wait()` sẽ xử lý sai (`nan` làm `clock() >= deadline` luôn `False`).
   - Dòng 118 dùng `args.query_name or cfg.kafka.query_name`: nếu truyền `--query-name ""`, chuỗi rỗng bị âm thầm chuyển sang default thay vì báo lỗi `CONFIG_ERROR_EXIT` (lệch tinh thần `docs/spec.md` I.3: *"không âm thầm chuyển sang default"*). Nên đổi thành `args.query_name if args.query_name is not None else cfg.kafka.query_name`.
2. **Đọc dòng nguồn bằng `spark.read.text(...).cast("binary")` có thể làm lệch hash với byte không hợp lệ UTF-8 (`src/n06/ingestion/wait_job.py:35-37`):**
   - ADR-0004 mục 9 bước 2 nêu `sha256` của **byte dòng** (không gồm LF). `spark.read.option("lineSep", "\n").text(paths)` giải mã UTF-8 rồi `.cast("binary")` mã hóa lại, nên nếu file nguồn có byte UTF-8 hỏng (như ca nêu ở G1), byte lỗi bị thay bằng U+FFFD (`\xef\xbf\xbd`) và không bao giờ khớp `sha2(value, 256)` của Raw. (Tuy `n06 publish` hiện cũng từ chối dòng không phải JSON hợp lệ, nhưng ghi nhận/xử lý bằng đọc nhị phân hoặc nêu rõ giới hạn sẽ chặt chẽ hơn.)
3. **Đường dẫn mặc định `paths.generator_output_dir` tương đối dùng CWD thay vì `REPO_ROOT` (`src/n06/cli/ingestion.py:66-67`):**
   - Dòng 67: `source_dir = Path(cfg.paths.generator_output_dir) / DEFAULT_SOURCE_DIRNAME`, trong khi ngay dòng 78-79 `event_log_dir` tương đối lại ghép `REPO_ROOT / cfg.spark.event_log_dir`. Nếu người dùng chạy `n06 wait-ingestion` không truyền `--source-dir` từ thư mục con khác `REPO_ROOT`, đường dẫn mặc định `"data/generated/source"` sẽ không tìm thấy.
4. **Bổ sung độ phủ kiểm thử cho các nhánh cốt lõi:**
   - Chưa có unit test cho hàm `wait()` (`src/n06/ingestion/wait_job.py:70-102`) dùng `clock`/`sleep` giả để kiểm tra retry khi gặp lỗi đọc Raw, thời gian chờ cắt theo `deadline`, và gọi `parse_error_rows` khi timeout.
   - `tests/integration/test_wait_ingestion.py:148-174` dùng `generator.demo-faults.json` (có phát sinh `parse_error`) nhưng chưa assert `result["parse_error_rows"] > 0` (hoặc giá trị cụ thể), nên nhánh `spark.read.parquet(*paths).count()` của `parse_error_rows` chưa được xác nhận trả kết quả khác 0.
   - `tests/unit/ingestion/test_wait_source_and_cli.py` chưa có test ca file events nằm ngoài manifest bị `dataset_event_files` từ chối, ca dòng cuối thiếu LF, và test `main(["wait-ingestion", "--help"])`.
5. **Giải phóng cache `expected` trong `wait()` (`src/n06/ingestion/wait_job.py:82`):**
   - Bọc vòng lặp `while True` trong `try ... finally: expected.unpersist()` để tránh giữ cache khi gọi `wait()` trong test hoặc tiến trình dùng chung `SparkSession`.

---

## Ghi nhận

- Thuật toán so đa tập trong `missing_lines` (`src/n06/ingestion/wait_job.py:43-57`) dùng `left join` theo `h`, lọc `value IS NOT NULL`, tính `sum(greatest(expected - coalesce(observed, 0), 0))`: đúng và gọn, không `collect()` dữ liệu giao dịch lên driver.
- `tests/integration/test_wait_ingestion.py:147-174` kiểm tra cả hai ca `new-line` và `one-more-copy`, xác minh đúng bản chất đa tập (multiset) của thuật toán.
- Quyết định gắn chỉ đọc riêng `source/` vào `/opt/n06/source-input:ro` an toàn về ranh giới ground truth và đã được ghi chép rõ trong `src/n06/ingestion/README.md:31-41` (kèm lưu ý quyền đọc của uid 1000).
- `src/n06/cli/__init__.py:46-68` chưa thêm `run_stream`, `run_wait_ingestion` vào `__all__` (nhỏ, không ảnh hưởng chạy CLI).

---

## Chưa kiểm

Do môi trường reviewer không có công cụ chạy lệnh shell, các mục sau **chưa được thực thi/xác minh**:
1. `git -C .../wt-22c diff task/T22b-stream...HEAD` và danh sách đầy đủ toàn bộ file thay đổi trong nhánh (chưa xác nhận được 100% rằng diff không chạm `contracts/`, `tests/fixtures/`, hay xóa/sửa test ngoài các file đã đọc).
2. `ruff check . && ruff format --check .`
3. `pytest tests/unit -q`
4. `python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py`
5. `pytest tests/integration/test_wait_ingestion.py` trên hạ tầng Docker Compose thật (Kafka, MinIO, PostgreSQL, Spark).
