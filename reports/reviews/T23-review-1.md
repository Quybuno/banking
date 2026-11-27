# Báo cáo Review PR — TASK T23 (`n06 ingest-statements` & dời SigV4 sang `src/n06/storage/object_store.py`)

Worktree đã kiểm tra: `/home/quybuno/.gemini/antigravity/brain/f2137caa-086e-4d85-99f1-20d3cc6ebca5/scratch/wt/wt-t23`
Phương thức kiểm tra: **Chỉ đọc trực tiếp mã nguồn và tài liệu** (không có shell tool để chạy lệnh).

---

## Phải sửa (chặn merge)

Không có.

---

## Nên sửa

1. **Bổ sung bước đọc lại nội dung (`GET`) trên MinIO thật trong `tests/integration/test_ingest_statements_minio.py` (dòng 106–129):**
   - **Trích ADR-0012 quyết định 2, mục Kiểm thử (dòng 116):** *"Test tích hợp MinIO thật: ghi, đọc lại, so `sha256`, tải lại cùng nội dung không đổi, khác nội dung bị từ chối, file lớn nhất theo ngoại suy."*
   - **Hiện trạng:** `test_minio_object_put_head_immutability_and_large_file` mới chỉ gọi `head_object` để kiểm `x-amz-meta-sha256` và `content_length`, chưa gọi `_raw_s3(target, "GET", f"/{bucket}/obj/small.jsonl")` (và `obj/large.jsonl`) để tải lại bytes thật từ MinIO và so `hashlib.sha256(body).hexdigest()` với file gốc. Dù MinIO xác thực `x-amz-content-sha256` khi nhận `PUT`, bổ sung `GET` sẽ bám sát nguyên văn yêu cầu kiểm thử của ADR-0012 hơn.

2. **Tăng tính độc lập của expected và liệt kê toàn bộ bucket trong `test_ingest_statements_end_to_end_on_minio_respects_simulated_clock` (`tests/integration/test_ingest_statements_minio.py`, dòng 146–192):**
   - Test hiện gọi `discover_statement_deliveries(source_dir)` (dòng 146) và `build_statement_keys(cfg, batch)` (dòng 178, 184) — là các hàm thuộc module đang kiểm thử — để dựng danh sách khóa cần kiểm, và chỉ `HEAD` các khóa dự kiến.
   - Nên liệt kê toàn bộ object thực có trên MinIO bằng `_raw_s3(target, "GET", f"/{bucket}", [("list-type", "2")])` để khẳng định số object trên bucket bằng đúng `3 * len(expected_early)` và không có bất kỳ khóa nào rò rỉ (file tương lai, `statement_deliveries.jsonl` ở vị trí bất kỳ, hoặc file ngoài ý muốn).

---

## Ghi nhận

### Đối chiếu đầy đủ 7 hạng mục (`docs/TASKS.md` mục 6.3)

1. **Điều kiện hoàn thành (thẻ T23, ADR-0002 mục 4 & 6, ADR-0009 QĐ3, ADR-0012 QĐ2):**
   - **Không file nào xuất hiện trước `file_available_at` theo đồng hồ giả lập:** Đạt (đọc code). `src/n06/ingestion/statements.py` dòng 340–347 lọc `parse_utc_timestamp(batch.file_available_at) <= cutoff`; `as_of_time` là tham số bắt buộc ở `ingest_statements` (dòng 323), không gọi `now()` trong logic nghiệp vụ. Unit test (`test_ingest_statements.py` dòng 99–149) và integration test (`test_ingest_statements_minio.py` dòng 149–225) kiểm cả mốc sớm, chạy lại idempotent và mốc tiến dần.
   - **Khóa MinIO, biên nhận `_ingest/<delivery_order:09d>.json`, thứ tự tải `file -> manifest -> biên nhận`:** Đạt (đọc code). `build_statement_keys` (dòng 292–299) dựng đúng `<statements_prefix>/<dataset_id>/<file_relpath>`, `<manifest_relpath>`, và `_ingest/<delivery_order:09d>.json`; `build_receipt_payload` (dòng 302–315) đủ các trường tối thiểu theo ADR-0002 mục 4 và chuẩn hóa bằng `canonical_json` + LF; `ingest_statements` (dòng 356–381) tải đúng thứ tự file dữ liệu → manifest → biên nhận, sắp xếp theo `delivery_order` tăng dần (dòng 288). Cơ chế khôi phục khi rớt giữa 3 bước cũng an toàn và idempotent (dòng 382–388).
   - **Không tải `statement_deliveries.jsonl` hay ground truth:** Đạt (đọc code). Chỉ tải 3 khóa của từng batch hợp lệ trong `eligible`.
   - **ADR-0012 quyết định 2 (dời SigV4 sang `src/n06/storage/object_store.py`, `PUT`/`HEAD` streaming với `Content-Length`, `x-amz-meta-sha256`, từ chối ghi đè khi thiếu/khác `sha256`, retry có giới hạn, known-answer test `PUT Object (test$file.text)`, cập nhật `cli/README.md` và `cli/object_store.py`, không thêm phụ thuộc):** Đạt (đọc code).
     - `src/n06/storage/object_store.py`: `_send_request` gửi `body_file` theo luồng từng chunk 1 MiB với `content-length` và `x-amz-content-sha256` (dòng 324–359); `_Response` đã mở rộng giữ `headers` (dòng 297–300); `_check_existing_immutable` từ chối ghi đè bằng `ObjectIntegrityError` nếu thiếu `x-amz-meta-sha256` hoặc lệch `sha256` (dòng 653–665); `_send_with_retry` thử lại tối đa `DEFAULT_MAX_RETRIES = 3` với lỗi mạng và HTTP `{408, 429, 500, 502, 503, 504}` (dòng 38, 479–502).
     - `src/n06/cli/object_store.py` (dòng 1–64) chỉ tái xuất từ `n06.storage.object_store` và đã cập nhật docstring; `src/n06/cli/README.md` (dòng 13) và `src/n06/storage/README.md` (dòng 11–15) đã cập nhật quy tắc.
     - `tests/unit/storage/test_object_store_sigv4.py` dùng lại vector `PUT Object (test$file.text)`; `tests/unit/storage/test_object_store_objects.py` có fake S3 server đọc đủ thân theo `Content-Length` và kiểm metadata/immutability/retry/không lộ secret.
     - `pyproject.toml` (dòng 11) giữ nguyên `dependencies = ["confluent-kafka==2.16.2"]`, không thêm phụ thuộc.
   - **Không lộ secret trong thông báo lỗi:** Đạt (đọc code). `ObjectStoreTarget.secret_key = field(repr=False)` (dòng 82); `target_from_config` chỉ nêu tên trường; `_transport_failure`, `_object_http_error`, `_send_with_retry` chỉ nêu mã HTTP, `<Code>` S3 (lọc regex), hoặc `type(exc).__name__`; `src/n06/cli/statements.py` dùng `exc.public_message` cho `ConfigValidationError`.

2. **Thay đổi `contracts/`, `tests/fixtures/`, hoặc xóa/skip/nới test:**
   - Trong các file đã đọc: không có test nào bị xóa, skip hay nới expected (`tests/unit/cli/test_object_store_sigv4.py` vẫn được giữ nguyên song song với `tests/unit/storage/test_object_store_sigv4.py`).
   - *(Lưu ý: chưa chạy `git diff --stat` để xác minh toàn nhánh — xem mục Chưa kiểm).*

3. **Hardcode endpoint/ngày/seed/quy mô/partition/bucket/topic:**
   - Không phát hiện hardcode cấu hình triển khai trong `src/n06/storage/object_store.py`, `src/n06/ingestion/statements.py`, `src/n06/cli/statements.py`. Bucket, prefix, endpoint/port, credential đều lấy từ `AppConfig` và biến môi trường.

4. **Dùng `float` cho tiền / đọc ground truth trong pipeline:**
   - Không có. Không xử lý tiền tệ; không đọc `ground_truth`, `expected_results`, `fault_assignment`, `canonical`, hay `tests/` từ `src/n06/`.

5. **Phạm vi task:**
   - Đúng phạm vi T23 + ADR-0012 quyết định 2.
   - Lưu ý quy trình duyệt sở hữu (`docs/TASKS.md` mục 2): PR chạm `src/n06/storage/` (B), `src/n06/cli/` (C) và `src/n06/ingestion/` (A), cần người sở hữu tương ứng duyệt trước khi merge.

6. **Chỗ đặc tả/ADR bị hiểu khác văn bản:**
   - Không phát hiện sai lệch logic cốt lõi. Chỉ có điểm nhỏ về việc thiếu `GET` đọc lại bytes trong integration test MinIO (đã nêu ở mục **Nên sửa** #1) và kích thước file lớn trong test là ~5 MiB (vượt ngoại suy 4,3 MB ở 1M tx; dưới mức 21,5 MB ở 5M tx — chấp nhận được cho thời gian chạy test và đã chú thích rõ).

7. **Tính độc lập của expected trong test:**
   - Unit test (`test_object_store_sigv4.py`, `test_object_store_objects.py`, `test_ingest_statements.py`): expected tính độc lập hoàn toàn (AWS vector cố định, `hashlib.sha256`, đường dẫn tường minh theo ADR-0002).
   - Integration test (`test_ingest_statements_minio.py`): `test_ingest_statements_end_to_end_on_minio_respects_simulated_clock` gọi lại `discover_statement_deliveries` và `build_statement_keys` để dựng kỳ vọng (đã nêu ở mục **Nên sửa** #2).

---

## Chưa kiểm

Do không có công cụ chạy lệnh shell trong phiên review này, các kiểm tra sau **chưa được thực thi** và người merge cần xác minh trước khi merge:

1. `git diff --stat main...HEAD` — xác minh toàn bộ danh sách file sửa trên nhánh `task/T23-ingest-statements`, đảm bảo không chạm `contracts/`, `tests/fixtures/`, `uv.lock` hoặc file ngoài phạm vi.
2. `ruff check . && ruff format --check .`
3. `pytest tests/unit -q`
4. `python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py` (14/14 PASS)
5. `pytest tests/integration/test_ingest_statements_minio.py` trên MinIO thật.
