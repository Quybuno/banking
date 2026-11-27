# ADR-0004b: Schema Silver, `key_conflicts` và `quarantine` (phần Silver của ADR-0004)

- Trạng thái: Proposed
- Người viết: agent (soạn theo chỉ thị ưu tiên thấp của chủ repo để chuẩn bị mở khóa T25/T30/T31), 2026-11-27
- Người duyệt: B (Quang — người sở hữu `quality`, `spark_jobs` Silver) và A (Mạnh — người duyệt theo `docs/TASKS.md` mục 3)
- Ngày: 2026-11-27
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 6 (Silver, Kiểm tra chất lượng), mục 7.1, mục 8.1 (batch); Phần II mục 1, 3, 8. Ngoài đặc tả: ADR-0002 (Accepted), ADR-0003 (Accepted), ADR-0004 (Accepted, phần Raw/Bronze).

> **Lưu ý:** Tài liệu này ở trạng thái **Proposed**, chưa được dùng làm căn cứ code cho tới khi được chủ repo/người duyệt chuyển sang **Accepted**. Phần Raw và Bronze đã chốt nằm tại `docs/adr/0004-schema-raw-bronze.md` (Accepted).

## Bối cảnh

`docs/adr/0004-schema-raw-bronze.md` (Accepted) chỉ chốt Raw và Bronze của luồng Kafka, để mở phần Silver cho trước T25 (`docs/TASKS.md` mục 3). T25 (`n06 process-day` bản sạch), T30 (quarantine), T31 (dedup/conflict/`key_conflicts`) và T32 (quality metrics) cần cùng một quy ước tên entity, cột, kiểu dữ liệu, khóa nghiệp vụ và cách lưu lineage/quarantine.

Căn cứ đặc tả (`docs/spec.md`):
- Phần I mục 6 (Silver): *"Chuẩn hóa kiểu, thời gian, currency, business key và kiểm tra quality."* *"Silver và Gold lưu Parquet, phân vùng theo `business_date` bên dưới tiền tố dataset/entity/run_id."*
- Phần I mục 6 (dedup/conflict): *"Cùng event_id + cùng payload: giữ một business event nhưng lưu lineage các lần nhận. Khác payload: quarantine conflict, không chọn ngẫu nhiên."* *"Khác event_id + cùng business ID + cùng nội dung: một business record, vẫn thống kê duplicate; cùng business ID khác nội dung: conflict."* *"Statement replay cùng statement_record_id/nội dung loại trùng ở Silver; nhiều statement_record_id cho cùng match key là duplicate nghiệp vụ để đối soát phát hiện."*
- Phần I mục 6 (quarantine): *"Quarantine gồm error_code, error_detail, entity/business key, source pointer, dataset_id, run_id và raw reference. Không ghi đè lỗi cũ mất dấu vết."*
- Phần I mục 7.1: *"Xuất thêm `key_conflicts` từ bước quality với business key/match key còn khôi phục được. Reconciliation đọc chỉ mục này để tạo `DUPLICATE_OR_CONFLICT` và đánh dấu phía bị quarantine."*

## Quyết định (đề xuất)

### 1. Danh sách entity và tầng trong `current_snapshot` (ADR-0002 mục 3)

| Tầng (`layer`) | Entity (`entity`) | Đường dẫn Parquet | Ý nghĩa |
|---|---|---|---|
| `silver` | `transactions` | `<silver_prefix>/<dataset_id>/transactions/run_id=<run_id>/business_date=<d>/` | Giao dịch hợp lệ sau chuẩn hóa và dedup |
| `silver` | `ledger_entries` | `<silver_prefix>/<dataset_id>/ledger_entries/run_id=<run_id>/business_date=<d>/` | Bút toán sổ cái hợp lệ sau chuẩn hóa và dedup |
| `silver` | `statements` | `<silver_prefix>/<dataset_id>/statements/run_id=<run_id>/business_date=<d>/` | Sao kê hợp lệ đã loại trùng theo `statement_record_id` |
| `silver` | `accounts` | `<silver_prefix>/<dataset_id>/accounts/run_id=<run_id>/business_date=<d>/` | Bản chụp tài khoản hợp lệ của dataset tại ngày xử lý |
| `silver` | `key_conflicts` | `<silver_prefix>/<dataset_id>/key_conflicts/run_id=<run_id>/business_date=<d>/` | Chỉ mục xung đột khóa (`EVENT_ID_CONFLICT`, `BUSINESS_KEY_CONFLICT`, quarantine còn khôi phục được match key) cho T33 |
| `quarantine` | `records` | `<quarantine_prefix>/<dataset_id>/records/run_id=<run_id>/business_date=<d>/` | Mọi bản ghi bị từ chối (gộp từ Bronze `parse_error` có thể quy ngày và lỗi nghiệp vụ/xung đột ở Silver) |

### 2. Quy tắc chuẩn hóa kiểu

- **Tiền (`amount_minor`, `opening_balance_minor`):** luôn là `BIGINT` (`LongType`), kiểm miền trước khi ghi, tuyệt đối không `float`/`double`.
- **Thời gian (`event_time`, `emitted_at`, `available_at`, `file_available_at`, `kafka_timestamp`, `ingested_at`):** `TIMESTAMP` (UTC).
- **Ngày nghiệp vụ (`business_date`, `opening_date`):** `DATE` (kiểm khớp ngày theo `Asia/Ho_Chi_Minh` của `event_time` cho transaction/ledger/statement).
- **Cột phân vùng `business_date`:** không lặp trùng hay mâu thuẫn với thư mục `business_date=<YYYY-MM-DD>/`; mọi reader đọc qua `current_snapshot.output_path`.

### 3. Schema các bảng Silver

#### 3.1. `silver.transactions` — Khóa duy nhất: `(dataset_id, transaction_id)`

| Cột | Kiểu Spark | Null | Ghi chú |
|---|---|---|---|
| `dataset_id` | string | không | Mã dataset |
| `transaction_id` | string | không | UUID giao dịch (khóa nghiệp vụ) |
| `external_reference` | string | không | Tham chiếu đối soát với kênh |
| `channel` | string | không | `SIM_CHANNEL_A` / `SIM_CHANNEL_B` |
| `account_from`, `account_to` | string | không | UUID tài khoản gửi/nhận |
| `amount_minor` | long | không | Số tiền nguyên dương |
| `currency` | string | không | `VND` / `USD` |
| `status` | string | không | `SUCCESS` / `FAILED` |
| `transaction_type` | string | không | `INTERNAL_TRANSFER` |
| `event_time` | timestamp | không | UTC |
| `business_date` | date | không | Ngày theo `Asia/Ho_Chi_Minh` |
| `first_event_id` | string | không | `event_id` nhỏ nhất theo thứ tự từ điển trong các bản trùng nghiệp vụ |
| `event_ids` | array&lt;string&gt; | không | Tập `event_id` duy nhất (đã sắp tăng dần) cho cùng business record |
| `replay_count` | int | không | Tổng số bản ghi vật lý đã gộp (`>= 1`) |
| `source_pointers` | array&lt;string&gt; | không | Danh sách `<topic>:<partition>:<offset>` (đã sắp tăng dần) |
| `first_kafka_timestamp` | timestamp | không | `min(kafka_timestamp)` của các lần nhận |
| `first_ingested_at` | timestamp | không | `min(ingested_at)` của các lần nhận |
| `run_id` | string | không | `run_id` của lần chạy batch tạo snapshot |

#### 3.2. `silver.ledger_entries` — Khóa duy nhất: `(dataset_id, entry_id)`

| Cột | Kiểu Spark | Null | Ghi chú |
|---|---|---|---|
| `dataset_id` | string | không | Mã dataset |
| `entry_id` | string | không | UUID bút toán (khóa nghiệp vụ) |
| `transaction_id` | string | không | UUID giao dịch cha |
| `ledger_account_id` | string | không | UUID tài khoản bị ghi nợ/có |
| `side` | string | không | `DEBIT` / `CREDIT` |
| `amount_minor` | long | không | Số tiền nguyên dương |
| `currency` | string | không | `VND` / `USD` |
| `event_time` | timestamp | không | UTC |
| `business_date` | date | không | Ngày theo `Asia/Ho_Chi_Minh` |
| `first_event_id` | string | không | `event_id` đại diện |
| `event_ids` | array&lt;string&gt; | không | Tập `event_id` duy nhất đã sắp |
| `replay_count` | int | không | Số bản ghi vật lý đã gộp (`>= 1`) |
| `source_pointers` | array&lt;string&gt; | không | Danh sách `<topic>:<partition>:<offset>` |
| `run_id` | string | không | `run_id` của lần chạy batch |

#### 3.3. `silver.statements` — Khóa duy nhất: `(dataset_id, statement_record_id)`

| Cột | Kiểu Spark | Null | Ghi chú |
|---|---|---|---|
| `dataset_id` | string | không | Mã dataset |
| `statement_record_id` | string | không | UUID dòng sao kê (loại trùng replay cùng ID) |
| `external_reference` | string | không | Khóa ghép đối soát |
| `channel` | string | không | Kênh sao kê |
| `amount_minor` | long | không | Số tiền nguyên dương |
| `currency` | string | không | `VND` / `USD` |
| `status` | string | không | `SUCCESS` / `FAILED` |
| `event_time` | timestamp | không | UTC |
| `business_date` | date | không | Ngày nghiệp vụ gốc (không thay bằng `available_at`) |
| `available_at` | timestamp | không | UTC |
| `file_available_at` | timestamp | không | Lấy từ biên nhận `_ingest/` |
| `delivery_order` | int | không | Từ biên nhận `_ingest/` |
| `source_file` | string | không | Đường dẫn tương đối file sao kê |
| `source_sha256` | string | không | SHA-256 của file sao kê |
| `source_line` | long | không | Số dòng vật lý (1-based) trong file |
| `run_id` | string | không | `run_id` của lần chạy batch |

*(Ghi chú: nhiều `statement_record_id` khác nhau cho cùng `(dataset_id, channel, external_reference)` **được giữ lại** ở `silver.statements` để T33 đối soát phát hiện `DUPLICATE_OR_CONFLICT` theo Phần I mục 6 và 7.1.)*

#### 3.4. `silver.accounts` — Khóa duy nhất: `(dataset_id, account_id)`

Các cột theo `contracts/N06_INPUT_CONTRACT_V1/schemas/account.schema.json`: `dataset_id`, `account_id`, `account_code`, `account_type`, `currency`, `opening_balance_minor` (long), `opening_date` (date), `status`, kèm `source_file`, `run_id`.

#### 3.5. `silver.key_conflicts`

| Cột | Kiểu Spark | Null | Ghi chú |
|---|---|---|---|
| `dataset_id` | string | không | Mã dataset |
| `business_date` | date | không | Ngày nghiệp vụ |
| `side_origin` | string | không | `INTERNAL` / `STATEMENT` |
| `entity` | string | không | `transaction` / `ledger_entry` / `statement` |
| `business_key` | string | được | `transaction_id` / `entry_id` / `statement_record_id` nếu khôi phục được |
| `channel` | string | được | Nếu khôi phục được |
| `external_reference` | string | được | Nếu khôi phục được (để T33 gắn `DUPLICATE_OR_CONFLICT`) |
| `conflict_code` | string | không | `EVENT_ID_CONFLICT` / `BUSINESS_KEY_CONFLICT` / mã lỗi quarantine khác |
| `issue_codes` | array&lt;string&gt; | không | Mọi mã lỗi phát hiện được |
| `source_pointers` | array&lt;string&gt; | không | Mọi source pointer liên quan |
| `run_id` | string | không | `run_id` tạo snapshot |

#### 3.6. `quarantine.records` (`layer = 'quarantine'`, `entity = 'records'`)

| Cột | Kiểu Spark | Null | Ghi chú |
|---|---|---|---|
| `dataset_id` | string | được | NULL nếu không parse được từ `parse_error` |
| `business_date` | date | không | Ngày xử lý (hoặc ngày `ingestion_date` với `parse_error` không có `business_date`) |
| `run_id` | string | không | `run_id` của lần chạy batch (bất biến, không ghi đè lỗi run cũ) |
| `stage` | string | không | `BRONZE_PARSE` / `SILVER_DEDUP` / `SILVER_BUSINESS` |
| `entity` | string | được | `transaction` / `ledger_entry` / `statement` / `account` / NULL |
| `business_key` | string | được | Khóa nghiệp vụ nếu khôi phục được |
| `error_code` | string | không | Mã chính theo thứ tự ưu tiên Phần II mục 8 |
| `issue_codes` | array&lt;string&gt; | không | Toàn bộ mã lỗi phát hiện được |
| `error_detail` | string | không | Mô tả lỗi |
| `source_pointer` | string | không | `<topic>:<partition>:<offset>` hoặc `<file>:<sha256>:<line>` |
| `raw_reference` | string | không | URI `raw_path` hoặc khóa file sao kê trên MinIO |

## Phương án đã cân nhắc

- **Gộp `quarantine` vào `layer = 'silver'`:** trái với ADR-0002 mục 3 (đã chốt `paths.quarantine_prefix` và `layer = 'quarantine'`). Không chọn.
- **Đặt cột `source_pointers` thành bảng con riêng ngay ở Silver:** tăng số entity và giao dịch `current_snapshot` ở P0; lưu `array<string>` (đã sắp xác định để giữ semantic hash ổn định) đủ gọn cho P0 và vẫn giữ đủ dấu vết phát lại.

## Câu hỏi mở cần người duyệt chốt trước khi Accepted

1. **Cách gán `business_date` cho `parse_error` không đọc được `business_date`:** dùng `ingestion_date` của batch Raw để phân vùng `quarantine.records`, hay tách riêng một phân vùng cố định / bảng không phân vùng?
2. **Vị trí nạp `accounts.jsonl` trên MinIO** (được để mở từ ADR-0002 mục 9 và ADR-0004 mục Việc mở): đọc từ `<statements_prefix>/<dataset_id>/accounts.jsonl` hay một lệnh nạp accounts riêng trước T25?

## Hệ quả

- Sau khi Accepted: T25, T30, T31, T32 dùng đúng tên entity, cột và kiểu trên; mọi lần ghi bắt buộc gọi `require_unused_run_prefix` và `build_run_output_path` theo ADR-0002 mục 3 & 6.
