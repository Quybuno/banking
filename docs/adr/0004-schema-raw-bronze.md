# ADR-0004: Schema Raw và Bronze (entity, cột, kiểu, partition, khóa, quan hệ với `ingest_batches`)

- Trạng thái: Proposed
- Người viết: agent (soạn theo yêu cầu của chủ dự án để mở khóa T22), 2026-11-25
- Người duyệt: B (người viết theo `docs/TASKS.md` mục 3) và A (người duyệt theo bảng đó)
- Ngày: 2026-11-25
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 2, 4.1, 6, 8.1 (streaming), 10, 12, 14.2; Phần II mục 3, 4, 8

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
- Các dòng dùng thêm: S234 (tiền là số nguyên), S300 (*"Lưu statement nguyên bản cùng manifest/checksum [...]"*), S309 (tiêu đề `#### Silver`, S317 nằm dưới mục này), S313-S316 (dedup ở Silver), S322 (*"Tách các đếm: physical Kafka records, unique event, unique business record, rejected record, nghiệp vụ hợp lệ."*), S386 (*"Job batch chỉ đọc các `batch_id` có trong `ingest_batches`."*), S432 (chờ *"offset/record kỳ vọng đã commit"*), S621 (độ trễ = `ingested_at` − `kafka_timestamp`), S740 (*"Accounts và statements đều là JSONL trong v1 [...] Bronze chuyển sang Parquet là bước pipeline."*), S891 (`ENVELOPE_PAYLOAD_MISMATCH`: *"event_type/dataset/time không khớp payload"*).
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

Quy tắc đề xuất (giả định G11, đặc tả không phát biểu): mỗi record Raw nằm đúng **một** dòng ở đúng **một** entity Bronze, nên `count(Raw) = count(transaction) + count(ledger_entry) + count(parse_error)` trong mỗi batch. Căn cứ gần nhất là S304 ("không bị filter bỏ im lặng") và S322 (tách các đếm); cả hai không nêu đẳng thức này. Bronze không dedup (dedup thuộc Silver, S313-S316).

Accounts và statement: xem "Ngoài phạm vi".

### 2. Raw (Parquet)

| Cột | Kiểu Spark | Null (G12) | Nguồn của cột |
|---|---|---|---|
| `key` | binary | được | S298 "Kafka key/value nguyên dạng" |
| `value` | binary | được | S298 |
| `topic` | string | không | S298, S299 |
| `partition` | int | không | S298, S299 |
| `offset` | long | không | S298, S299 |
| `kafka_timestamp` | timestamp (UTC) | không | S298 "timestamps", S238 |
| `ingested_at` | timestamp (UTC) | không | S298 "timestamps", S238 |
| `dataset_id` | string | được (khi không parse được; Q4) | S298 "dataset_id nếu parse được"; `event_envelope.schema.json` |
| `ingestion_run_id` | string | không | S298 |

Khóa kỹ thuật: `(topic, partition, offset)` (S299). Không có khóa theo nghiệp vụ.

### 3. Bronze: khóa và cột chung của `transaction` và `ledger_entry`

**Khóa kỹ thuật của cả ba entity Bronze:** `(topic, partition, offset)` (giả định G13: S299 chốt khóa này cho Kafka; đặc tả không nói khóa của bảng Bronze). Khóa nghiệp vụ (`transaction_id`, `entry_id`, `event_id`) **không duy nhất** ở Bronze vì còn bản phát lại.

| Cột | Kiểu Spark | Null | Nguồn của cột |
|---|---|---|---|
| `topic`, `partition`, `offset` | string, int, long | không | S304 "giữ source pointer", S746 |
| `kafka_timestamp`, `ingested_at` | timestamp (UTC) | không | S238 |
| `ingestion_run_id` | string | không | S238 `run_id`, S298 (G5) |
| `envelope_schema_version` | string | không | `event_envelope.schema.json: schema_version` |
| `event_id` | string | không | `event_envelope.schema.json` (uuid) |
| `event_type` | string | không | `event_envelope.schema.json` |
| `envelope_dataset_id` | string | không | `event_envelope.schema.json: dataset_id` |
| `source_batch_id` | string | không | `event_envelope.schema.json` (uuid), S238 |
| `envelope_event_time` | string | không | `event_envelope.schema.json: event_time` |
| `emitted_at` | string | không | `event_envelope.schema.json`, S238 |

Các cột envelope không null vì chúng đều bắt buộc trong schema và chỉ record hợp lệ schema vào hai entity này (G1).

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

Mọi cột `payload` là **không null** vì chỉ record hợp lệ schema mới vào hai entity này (G1).

### 6. Bronze `parse_error`

S317 liệt kê trường của **quarantine** và nằm dưới mục Silver; ADR-0002 điểm 4 cũng tách `parse_error` (Bronze) khỏi quarantine (Silver). Việc dùng bộ trường đó cho `parse_error` là **giả định G14**.

| Cột | Kiểu Spark | Null | Nguồn của cột (qua G14) |
|---|---|---|---|
| `topic`, `partition`, `offset` | string, int, long | không | S317 "source pointer", "raw reference"; S746 |
| `kafka_timestamp`, `ingested_at` | timestamp | không | S238 |
| `ingestion_run_id` | string | không | S317 `run_id` (G5) |
| `dataset_id` | string | được (Q4) | S317 |
| `entity` | string | được | S317 "entity/business key" (G15, Q5) |
| `business_key` | string | được | S317 (G15, Q5) |
| `error_code` | string | không | S317; bảng mã Phần II mục 8; mã chính chọn theo S876 |
| `error_detail` | string | không | S317 |
| `issue_codes` | array&lt;string&gt; | không | S876 "lưu tất cả issue_codes" (G16) |

Raw reference của S317 là `(topic, partition, offset)` cộng `ingest_batches.raw_path` của batch; không chép lại `value` (G6).

### 7. Partition và đường dẫn

Theo ADR-0002: `<raw_prefix>/<query_name>/ingestion_date=<d>/batch_id=<id>/` và `<bronze_prefix>/<query_name>/ingestion_date=<d>/batch_id=<id>/entity=<entity>/`. `ingestion_date` là ngày theo `Asia/Ho_Chi_Minh` của Kafka timestamp nhỏ nhất trong batch (S384, ADR-0002 điểm 3). `ingestion_date`, `batch_id`, `query_name` **không** nằm trong file Parquet; job batch lấy đường dẫn từ `ingest_batches` (người viết suy từ S386, vốn chỉ nói job batch chỉ đọc các `batch_id` có trong bảng; giả định G4).

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

- **G1.** Bước parse của Bronze là **chỉ kiểm JSON Schema của contract**, tức hàm `parse_and_validate_json_schema(raw, "event_envelope")` của `src/n06/domain/schema_validator.py`; **không** dùng `validate_event_envelope` hay `validate_transaction_record` vì hai hàm này trộn cả mã ngữ nghĩa. Record vi phạm bất kỳ mã mức schema nào (`INVALID_JSON`, `UNSUPPORTED_SCHEMA_VERSION`, `MISSING_REQUIRED_FIELD`, `SCHEMA_TYPE`, `UNSUPPORTED_CURRENCY`, `AMOUNT_OUT_OF_RANGE`, `SCHEMA_VALUE`) vào `parse_error`; kiểm tra ngữ nghĩa (Phần II mục 3) thuộc Silver. Đặc tả không nói rõ ranh giới giữa "parse lỗi" và "quality". Value `null` (tombstone) cho `SCHEMA_TYPE`, value rỗng cho `INVALID_JSON`; cả hai vào `parse_error`. Đã chạy thử ngày 2026-11-25: `business_date` sai vẫn qua schema (Silver bắt), `event_type` lệch payload cho `MISSING_REQUIRED_FIELD, SCHEMA_VALUE` (xem Q8). Hàm này giải mã bytes với `errors="replace"`, nên T22 phải tự từ chối UTF-8 không hợp lệ (`INVALID_JSON`) trước khi gọi.
- **G2.** Tên cột trùng nhau giữa envelope và payload (`schema_version`, `dataset_id`, `event_time`) được giữ cả hai, bản của envelope có tiền tố `envelope_`, để Silver kiểm `ENVELOPE_PAYLOAD_MISMATCH`.
- **G3.** Trường thời gian và ngày của contract giữ nguyên kiểu `string` ở Bronze; chuẩn hóa kiểu là việc của Silver (S311).
- **G4.** Cột phân vùng không lặp lại trong file.
- **G5.** `run_id` của S238/S317 ở tầng streaming chính là `ingestion_run_id`; giá trị do lệnh `n06 stream` sinh mỗi lần khởi động tiến trình.
- **G6.** `parse_error` không chép `value`.
- **G7.** `record_count` đếm record Raw (vật lý), không phải số record Bronze hợp lệ.
- **G8.** Raw lưu dạng Parquet (đặc tả chỉ nói rõ Parquet cho Bronze; Raw giữ `key`/`value` dạng binary).
- **G9.** Batch không có record nào thì không ghi file và không tạo dòng `ingest_batches` (khi đó không tính được `ingestion_date`).
- **G10.** Tên cột `key`, `value`, `topic`, `partition`, `offset` trùng tên cột nguồn Kafka của Spark; `kafka_timestamp` là cột `timestamp` của nguồn đó, đổi tên theo S238. Không giữ `timestampType` và header Kafka vì đặc tả không yêu cầu và producer không gửi header.
- **G11.** Mỗi record Raw cho đúng một dòng Bronze (mục 1).
- **G12.** Tính null của cột Raw (`key`, `value` được null; còn lại như bảng). S298 không nói tính null.
- **G13.** Khóa kỹ thuật của bảng Bronze là `(topic, partition, offset)`.
- **G14.** `parse_error` dùng bộ trường quarantine của S317.
- **G15.** `entity` là `transaction` hoặc `ledger_entry` suy từ `event_type` khi đọc được, ngược lại null; `business_key` là `transaction_id` (transaction) hoặc `entry_id` (ledger) chỉ khi đọc được và là UUID hợp lệ, ngược lại null (không tạo key giả). Cần chốt ở Q5.
- **G16.** `issue_codes` lưu mã ở **mọi** mức (đúng hành vi validator hiện có); S876 chỉ yêu cầu khi *"nhiều lỗi cùng mức"*.

## Câu hỏi phải chốt trước khi Accepted

- **Q1 (chặn T22). `ingest_batches.dataset_id` là một giá trị `NOT NULL`, nhưng một microbatch có thể chứa nhiều `dataset_id` hoặc record không đọc được `dataset_id`.** S385 không nhắc `dataset_id`; cột này do ADR-0003 thêm. Kịch bản nghiệm thu lại gọi `n06 wait-ingestion --dataset-id acceptance-01` (dòng 549; cú pháp được phép điều chỉnh theo dòng 540).
  - (a) Lấy `dataset.dataset_id` từ cấu hình của lệnh `stream`; record của dataset khác vẫn lưu; đếm theo dataset phải đọc Bronze.
  - (b) Migration mới thêm bảng con `(query_name, batch_id, dataset_id, record_count)`; `ingest_batches.dataset_id` bỏ `NOT NULL`. Hệ quả: sửa ADR-0003; `IngestBatchRecord.dataset_id` và `list_committed_ingest_batches(dataset_id)` của T24 (đã DONE) đổi; test đang kỳ vọng đúng một migration `0001` sẽ vỡ. Record không có `dataset_id` vẫn không quy được về dataset nào, cần nói bảng con lưu chúng ra sao.
  - (c) Một topic chỉ chứa một dataset; batch có record dataset khác thì thất bại (một record hỏng có thể làm query dừng mãi).
  - (d) `wait-ingestion` chờ theo offset hoặc số record kỳ vọng đã commit (S432), dựa vào `topic_offsets`/`record_count`; `ingest_batches.dataset_id` lấy như (a). Phải nói rõ `--dataset-id` ánh xạ sang offset hoặc số record kỳ vọng nào (ví dụ từ manifest nguồn của dataset), và `record_count` gộp cả record của dataset khác lẫn bản phát lại (G7) nên không so trực tiếp được khi topic dùng chung.
  - Đề xuất của người viết: cân nhắc (d) trước vì không đổi schema, với điều kiện trả lời được hai điểm trên; nếu không thì (b). B (control store) và C quyết.
- **Q2. Phát lại batch ghi đè "cùng nội dung" (S387) trong khi `ingested_at` và `ingestion_run_id` khác nhau giữa hai lần chạy; S317 lại yêu cầu *"Không ghi đè lỗi cũ mất dấu vết"* mà overwrite `parse_error` khi phát lại (S384) làm mất hai giá trị của lần đầu.** Đề xuất: "cùng nội dung" là mọi cột trừ `ingested_at`, `ingestion_run_id` (tương tự mục 8.1 bước 6 của batch); yêu cầu "không ghi đè lỗi" áp cho quarantine của Silver, còn `parse_error` là output của batch nên được ghi đè.
- **Q3. Dạng `topic_offsets`.** Tiền lệ trong `tests/integration/test_control_migrations.py`: `{"events": {"0": [0, 10]}}`. Cần chốt đầu cuối có tính cả hay không. Trong tiền lệ đó `[0, 10]` đi cùng `record_count = 10`, gợi ý đầu cuối **không** tính. Đề xuất: `{"<topic>": {"<partition>": [<offset đầu>, <offset cuối, tính cả>]}}`; nếu người duyệt muốn theo tiền lệ thì đổi thành đầu cuối không tính.
- **Q4. `dataset_id` của Raw và `parse_error` lấy từ envelope hay payload, và xử lý thế nào khi sai kiểu, sai pattern, hoặc hai bên lệch nhau.** Đề xuất: lấy từ envelope nếu là chuỗi khớp pattern, ngược lại null.
- **Q5. `entity` và `business_key` của `parse_error`** (G15) có đúng ý người duyệt không; có cần `event_id` không.
- **Q6. Batch không có `parse_error` (hoặc không có entity nào đó) thì ghi thư mục rỗng hay không ghi.** T25 đọc đường dẫn không tồn tại sẽ khác. Đề xuất: không ghi; T25 coi thiếu thư mục là rỗng. Khi đó lần phát lại phải ghi đè **cả** thư mục `batch_id=<id>/` (ADR-0002 mục 2 và 6), không ghi đè từng entity, để không còn thư mục cũ.
- **Q7. `ingested_at` gán một lần cho cả batch hay mỗi lần ghi; Raw và Bronze của cùng record có bằng nhau không** (S621 dùng nó đo độ trễ). Đề xuất: một giá trị cho cả batch, dùng chung cho Raw và Bronze.
- **Q8. Với G1, `event_type` lệch payload vào `parse_error` nên mã `ENVELOPE_PAYLOAD_MISMATCH` cho `event_type` (S891) không bao giờ xuất hiện ở Silver.** Chấp nhận, hay Bronze phải gắn mã đó?

## Phương án đã cân nhắc

- Bronze giữ một bảng duy nhất với `payload` dạng chuỗi JSON: không "tách entity" theo S304.
- Chuyển kiểu `date`/`timestamp` ngay ở Bronze: lấn việc chuẩn hóa của Silver (S311). (Record sai định dạng không vào bảng entity theo G1 và Raw vẫn giữ nguyên bytes, nên lý do không phải là mất dữ liệu.)
- Đưa record sai miền (ví dụ `amount_minor <= 0`) vào bảng entity để Silver lọc: cột entity phải cho null và kiểu lỏng hơn; xem G1.

## Hệ quả

- T22 dùng được `parse_and_validate_json_schema` (chỉ thư viện chuẩn); bộ kiểm đọc schema từ `contracts/`. Phần I mục 2 (S201) yêu cầu image job chứa Java, PySpark, JAR đã ghim và gói `n06`; image hiện tại chưa chứa `n06` và `contracts/` (kế hoạch T22 quyết).
- T25 đọc Bronze theo `ingest_batches`; T30 dựng quarantine từ `parse_error` và lỗi ngữ nghĩa của Silver.
- Thêm cột sau này cần ADR mới.

## Ngoài phạm vi (chưa quyết)

- **Silver và Gold: chưa quyết, B bổ sung trước T25.** Lưu ý: `docs/TASKS.md` giao schema **Gold** cho ADR-005 (C viết, trước T41), không phải ADR này; chủ dự án quyết giữ theo bảng đó hay gộp.
- **Bronze của accounts và statement: ADR này thu hẹp phạm vi "Raw và Bronze" thành luồng sự kiện Kafka**, dù S740 nói accounts và statement cũng chuyển sang Parquet ở Bronze và S300 yêu cầu lưu statement nguyên bản. Đề xuất: B bổ sung trước T25 (T25 cần đầu vào sao kê); cần người duyệt đồng ý.
- Nơi lưu `accounts.jsonl`.
