# ADR-0004: Schema Raw và Bronze (entity, cột, kiểu, partition, khóa, quan hệ với `ingest_batches`)

- Trạng thái: Proposed
- Người viết: agent (soạn theo yêu cầu của chủ dự án để mở khóa T22), 2026-11-25
- Người duyệt: B (người viết theo `docs/TASKS.md` mục 3) và A (người duyệt theo bảng đó)
- Ngày: 2026-11-25
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 4.1, 6, 8.1 (streaming); Phần II mục 3, 4, 8

**Phạm vi: chỉ Raw và Bronze của luồng sự kiện Kafka. Silver và Gold: chưa quyết, B bổ sung trước T25.**
ADR này chưa được dùng làm căn cứ code cho tới khi chuyển Accepted và các câu hỏi ở mục "Câu hỏi phải chốt" có đáp án.

## Bối cảnh

ADR-0002 (Accepted) mục 2 chốt đường dẫn và ghi: *"Tên entity cụ thể và schema thuộc ADR-0004"*, *"Định dạng file bên trong Raw thuộc T22/ADR-0004"*. `docs/TASKS.md` xếp ADR-004 trước T25, trong khi T22 đã ghi Raw và Bronze, nên phần Raw/Bronze cần chốt trước T22.

Nguồn trích (số dòng của `docs/spec.md`):

- S298: *"Giữ Kafka key/value nguyên dạng, topic, partition, offset, timestamps, dataset_id nếu parse được, ingestion_run_id. Record JSON sai vẫn phải lưu được."*
- S299: *"Khóa kỹ thuật Kafka: topic + partition + offset. Không dedup Raw bằng transaction_id; Raw cần giữ dấu vết phát lại và lỗi."*
- S304: *"Parse theo schema version rõ ràng; tách entity, lưu Parquet, giữ source pointer. Record parse lỗi vẫn có đường đến quarantine, không bị filter bỏ im lặng."*
- S305: *"Partition theo ingestion_date để bắt được sự kiện đến muộn. [...]"*
- S236: *"[...] Sai kiểu, âm, zero hoặc vượt miền hợp lệ vào quarantine."*
- S238: *"Phân biệt `event_time` [...], `emitted_at` [...], `kafka_timestamp` (thời điểm producer gửi thật, lấy từ record timestamp của Kafka), `ingested_at` (thời điểm pipeline ghi nhận thật), `business_date`, `run_id`, `source_batch_id`. Chỉ `kafka_timestamp` và `ingested_at` được dùng để đo độ trễ hệ thống."*
- S311: *"Chuẩn hóa kiểu, thời gian, currency, business key và kiểm tra quality."* (thuộc Silver)
- S317: *"Quarantine gồm error_code, error_detail, entity/business key, source pointer, dataset_id, run_id và raw reference. Không ghi đè lỗi cũ mất dấu vết."*
- S384, S385, S387 (mục 8.1 streaming): ghi Raw rồi Bronze (kèm bản ghi parse lỗi) theo `batch_id`; upsert `ingest_batches` khóa (query_name, batch_id) kèm khoảng offset theo topic-partition và số bản ghi; *"Chết sau bước 3 nhưng trước khi checkpoint ghi nhận: phát lại ghi đè cùng nội dung và upsert không tạo dòng mới."*
- S746: *"[...] source pointer gồm file/checksum/line number hoặc topic/partition/offset."*
- S876 (Phần II mục 8): thứ tự ưu tiên mã lỗi; *"Nếu một record có nhiều lỗi cùng mức, lưu tất cả issue_codes, chọn primary theo thứ tự mã đã tài liệu hóa."*
- Phần II mục 3: schema chốt kiểu, miền giá trị, enum, trường bắt buộc, cấm trường ngoài schema; các kiểm tra join giữa file (account tồn tại, ledger khớp transaction, envelope khớp payload, ...) cần domain validator.

## Quyết định

### 1. Entity

| Tầng | Entity (thư mục `entity=` theo ADR-0002) | Nội dung | Nguồn tên |
|---|---|---|---|
| Raw | không chia entity | mọi record Kafka của batch, kể cả JSON hỏng | S298, S299 |
| Bronze | `transaction` | record có `event_type = TRANSACTION_CREATED` hợp lệ schema | `event_envelope.schema.json` (`event_type`), Phần II mục 4 (entity `transaction`, `ledger_entry` của control report) |
| Bronze | `ledger_entry` | record có `event_type = LEDGER_ENTRY_CREATED` hợp lệ schema | như trên |
| Bronze | `parse_error` | record không qua được bước parse theo schema | S304; tên `parse_error` do ADR-0002 điểm 4 chốt |

Bất biến phải kiểm thử: mỗi record Raw nằm đúng **một** dòng ở đúng **một** entity Bronze, nên `count(Raw) = count(transaction) + count(ledger_entry) + count(parse_error)` trong mỗi batch (S304 "không bị filter bỏ im lặng", S322 tách các đếm). Bronze không dedup (dedup thuộc Silver, spec dòng 313-316).

Accounts và statement không đi qua Kafka nên không thuộc ADR này (xem "Ngoài phạm vi").

### 2. Raw (Parquet)

| Cột | Kiểu Spark | Null | Nguồn |
|---|---|---|---|
| `key` | binary | được | S298 "Kafka key/value nguyên dạng" |
| `value` | binary | được | S298 |
| `topic` | string | không | S298, S299 |
| `partition` | int | không | S298, S299 |
| `offset` | long | không | S298, S299 |
| `kafka_timestamp` | timestamp (UTC) | không | S298 "timestamps", S238 |
| `ingested_at` | timestamp (UTC) | không | S298 "timestamps", S238 |
| `dataset_id` | string | được (khi không parse được) | S298 "dataset_id nếu parse được"; `event_envelope.schema.json` |
| `ingestion_run_id` | string | không | S298 |

Khóa kỹ thuật: `(topic, partition, offset)` (S299). Không có khóa theo nghiệp vụ.

### 3. Bronze: cột chung của `transaction` và `ledger_entry`

| Cột | Kiểu Spark | Nguồn |
|---|---|---|
| `topic`, `partition`, `offset` | string, int, long | S304 "giữ source pointer", S746 |
| `kafka_timestamp`, `ingested_at` | timestamp (UTC) | S238 |
| `ingestion_run_id` | string | S238 `run_id`, S298 (giả định G5) |
| `envelope_schema_version` | string | `event_envelope.schema.json: schema_version` |
| `event_id` | string | `event_envelope.schema.json` (uuid) |
| `event_type` | string | `event_envelope.schema.json` |
| `envelope_dataset_id` | string | `event_envelope.schema.json: dataset_id` |
| `source_batch_id` | string | `event_envelope.schema.json` (uuid), S238 |
| `envelope_event_time` | string | `event_envelope.schema.json: event_time` |
| `emitted_at` | string | `event_envelope.schema.json`, S238 |

### 4. Bronze `transaction` (thêm vào cột chung)

Mỗi cột lấy tên trường của `contracts/N06_INPUT_CONTRACT_V1/schemas/transaction.schema.json` (`payload`).

| Cột | Kiểu Spark | Ràng buộc trong contract |
|---|---|---|
| `schema_version` | string | const `1.0.0` |
| `dataset_id` | string | pattern `^[a-z][a-z0-9-]{2,63}$` |
| `transaction_id` | string | uuid |
| `external_reference` | string | pattern `^REF-[A-Z0-9-]{1,80}$` |
| `account_from`, `account_to` | string | uuid |
| `amount_minor` | long | integer 1 đến 10^12 (số nguyên, không float: S234) |
| `currency` | string | `VND`, `USD` |
| `status` | string | `SUCCESS`, `FAILED` |
| `transaction_type` | string | const `INTERNAL_TRANSFER` |
| `channel` | string | `SIM_CHANNEL_A`, `SIM_CHANNEL_B` |
| `event_time` | string | UTC `YYYY-MM-DDTHH:MM:SSZ` |
| `business_date` | string | date |

### 5. Bronze `ledger_entry` (thêm vào cột chung)

Theo `schemas/ledger_entry.schema.json`.

| Cột | Kiểu Spark | Ràng buộc trong contract |
|---|---|---|
| `schema_version` | string | const `1.0.0` |
| `dataset_id` | string | pattern như trên |
| `entry_id`, `transaction_id`, `ledger_account_id` | string | uuid |
| `side` | string | `DEBIT`, `CREDIT` |
| `amount_minor` | long | integer 1 đến 10^12 |
| `currency` | string | `VND`, `USD` |
| `event_time` | string | UTC |
| `business_date` | string | date |

Mọi cột `payload` là **không null** vì chỉ record hợp lệ schema mới vào hai entity này (giả định G1).

### 6. Bronze `parse_error`

| Cột | Kiểu Spark | Null | Nguồn |
|---|---|---|---|
| `topic`, `partition`, `offset` | string, int, long | không | S317 "source pointer", "raw reference"; S746 |
| `kafka_timestamp`, `ingested_at` | timestamp | không | S238 |
| `ingestion_run_id` | string | không | S317 `run_id` (giả định G5) |
| `dataset_id` | string | được | S317 |
| `entity` | string | được | S317 "entity/business key" |
| `business_key` | string | được | S317; Phần II mục 8: *"Nếu key không khôi phục được, giữ quarantine và quality counts; không tạo reference giả."* |
| `error_code` | string | không | S317; bảng mã Phần II mục 8; mã chính chọn theo S876 |
| `error_detail` | string | không | S317 |
| `issue_codes` | array&lt;string&gt; | không | S876 "lưu tất cả issue_codes" |

Raw reference của S317 là `(topic, partition, offset)` cộng `ingest_batches.raw_path` của batch; không chép lại `value` (giả định G6).

### 7. Partition và đường dẫn

Theo ADR-0002: `<raw_prefix>/<query_name>/ingestion_date=<d>/batch_id=<id>/` và `<bronze_prefix>/<query_name>/ingestion_date=<d>/batch_id=<id>/entity=<entity>/`. `ingestion_date` là ngày theo `Asia/Ho_Chi_Minh` của Kafka timestamp nhỏ nhất trong batch (S384, ADR-0002 điểm 3). `ingestion_date`, `batch_id`, `query_name` **không** nằm trong file Parquet; job batch lấy đường dẫn từ `ingest_batches` (S386; giả định G4).

### 8. Quan hệ với `ingest_batches` (ADR-0003, `0001_init_control_schema.up.sql`)

Một batch hoàn tất tương ứng đúng một dòng khóa `(query_name, batch_id)`, upsert **sau** khi mọi output đã ghi (S385).

| Cột bảng | Giá trị |
|---|---|
| `query_name`, `batch_id` | của query `foreachBatch` |
| `dataset_id` | **chưa quyết**, xem câu hỏi Q1 |
| `ingestion_date` | như mục 7 |
| `topic_offsets` | khoảng offset theo topic-partition của batch (S385); dạng JSON: xem Q3 |
| `record_count` | số record Raw của batch (giả định G7) |
| `raw_path`, `bronze_path` | URI `s3a://` theo ADR-0002 |
| `status` | `COMMITTED` |

## Giả định (mỗi mục cần người duyệt đồng ý hoặc sửa)

- **G1.** Bước parse của Bronze là kiểm **JSON Schema của contract** (envelope và payload). Record vi phạm bất kỳ mã mức schema nào (`INVALID_JSON`, `UNSUPPORTED_SCHEMA_VERSION`, `MISSING_REQUIRED_FIELD`, `SCHEMA_TYPE`, `UNSUPPORTED_CURRENCY`, `AMOUNT_OUT_OF_RANGE`, `SCHEMA_VALUE`) vào `parse_error`; kiểm tra ngữ nghĩa (Phần II mục 3) thuộc Silver. Đặc tả không nói rõ ranh giới giữa "parse lỗi" và "quality".
- **G2.** Tên cột trùng nhau giữa envelope và payload (`schema_version`, `dataset_id`, `event_time`) được giữ cả hai, bản của envelope có tiền tố `envelope_`, để Silver kiểm `ENVELOPE_PAYLOAD_MISMATCH`.
- **G3.** Trường thời gian và ngày của contract giữ nguyên kiểu `string` ở Bronze; chuẩn hóa kiểu là việc của Silver (S311).
- **G4.** Cột phân vùng không lặp lại trong file.
- **G5.** `run_id` của S238/S317 ở tầng streaming chính là `ingestion_run_id`; giá trị do lệnh `n06 stream` sinh mỗi lần khởi động tiến trình.
- **G6.** `parse_error` không chép `value`.
- **G7.** `record_count` đếm record Raw (vật lý), không phải số record Bronze hợp lệ.
- **G8.** Raw lưu dạng Parquet (đặc tả chỉ nói rõ Parquet cho Bronze; Raw giữ `key`/`value` dạng binary).
- **G9.** Batch không có record nào thì không ghi file và không tạo dòng `ingest_batches` (khi đó không tính được `ingestion_date`).
- **G10.** Tên cột `key`, `value`, `topic`, `partition`, `offset` trùng tên cột nguồn Kafka của Spark; `kafka_timestamp` là cột `timestamp` của nguồn đó, đổi tên theo S238. Không giữ `timestampType` và header Kafka vì đặc tả không yêu cầu và producer không gửi header.

## Câu hỏi phải chốt trước khi Accepted

- **Q1 (chặn T22). `ingest_batches.dataset_id` là một giá trị `NOT NULL`, nhưng một microbatch có thể chứa nhiều `dataset_id` hoặc record không parse được `dataset_id`.** Đặc tả (S385) không nhắc `dataset_id`; cột này do ADR-0003 thêm, còn `n06 wait-ingestion --dataset-id` (spec mục 12) cần biết theo dataset.
  - (a) Lấy `dataset.dataset_id` từ cấu hình của lệnh `stream`; record của dataset khác vẫn được lưu; đếm theo dataset phải đọc Bronze.
  - (b) Migration mới thêm bảng con `(query_name, batch_id, dataset_id, record_count)`; `ingest_batches.dataset_id` bỏ `NOT NULL`. Sửa ADR-0003.
  - (c) Quy định một topic chỉ chứa một dataset; batch có record dataset khác thì thất bại (một record hỏng có thể làm query dừng mãi).
  - Đề xuất của người viết: (b), vì chỉ (b) cho `wait-ingestion` đếm đúng mà không đọc object store. Cần B và C quyết vì đụng schema control.
- **Q2. Phát lại batch ghi đè "cùng nội dung" (S387), nhưng `ingested_at` và `ingestion_run_id` khác nhau giữa lần chạy đầu và lần phát lại.** Đề xuất: "cùng nội dung" được hiểu là mọi cột trừ `ingested_at` và `ingestion_run_id` (tương tự mục 8.1 bước 6 của batch, cho phép khác run_id/timestamp); test restart so sánh bỏ hai cột đó.
- **Q3. Dạng `topic_offsets`.** Đề xuất: `{"<topic>": {"<partition>": {"first": <offset đầu>, "last": <offset cuối, tính cả>}}}`.

## Phương án đã cân nhắc

- Bronze giữ một bảng duy nhất với `payload` dạng chuỗi JSON: không "tách entity" theo S304.
- Chuyển kiểu `date`/`timestamp` ngay ở Bronze: làm mất giá trị gốc khi sai định dạng và lấn việc chuẩn hóa của Silver (S311).
- Đưa record sai miền (ví dụ `amount_minor <= 0`) vào bảng entity để Silver lọc: cột entity phải cho null và kiểu lỏng hơn; xem G1.

## Hệ quả

- T22 dùng được domain validator sẵn có (`src/n06/domain/`, chỉ dùng thư viện chuẩn); bộ kiểm đọc schema từ `contracts/` nên tiến trình trong container Spark phải thấy thư mục đó (cách đưa mã và `contracts/` vào container do kế hoạch T22 quyết).
- T25 đọc Bronze theo `ingest_batches`; T30 dựng quarantine từ `parse_error` và lỗi ngữ nghĩa của Silver.
- Thêm cột sau này cần ADR mới.

## Ngoài phạm vi (chưa quyết)

- **Silver và Gold: chưa quyết, B bổ sung trước T25.**
- Bronze của statement (file sao kê không đi qua streaming query; ADR-0002 mục 4 mới chốt đường dẫn file gốc).
- Nơi lưu `accounts.jsonl`.
