# ADR-0004: Schema Raw và Bronze (entity, cột, kiểu, partition, khóa, quan hệ với `ingest_batches`)

- Trạng thái: Accepted
- Người viết: agent (soạn theo yêu cầu của chủ dự án để mở khóa T22), 2026-11-25
- Người duyệt: B (người viết theo `docs/TASKS.md` mục 3) và A (người duyệt theo bảng đó)
- Đã duyệt (Accepted): QUY (chủ repo), 2026-11-27 (PR #15, #16)
- Ngày: 2026-11-25
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 2, 4.1, 6, 8.1 (streaming), 10, 12, 14.2; Phần II mục 3, 4, 8

**Phạm vi: chỉ Raw và Bronze của luồng sự kiện Kafka.** Bronze của accounts và statement là việc mở (mục "Việc mở"). Silver ngoài phạm vi. Schema Gold thuộc ADR-005 theo `docs/TASKS.md`, không thuộc ADR này.
Từ 2026-11-27 ADR này là căn cứ code. Chủ repo đã trả lời Q1 đến Q8 (mục "Quyết định đã chốt").

*Lịch sử:* soạn ở trạng thái Proposed (PR #9, 2026-11-25); chuyển Accepted 2026-11-27 (PR #15, #16); cùng ngày ghi câu trả lời Q1-Q8 theo chỉ thị của chủ repo (dùng đúng đề xuất; Q1: `dataset_id` không bắt buộc, qua migration mới `0002`).

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
- S379 (mục 8.1, batch, bước 6): *"[...] run_id/timestamp vận hành được phép khác."*; S384: ghi theo `batch_id`, *"chế độ overwrite"*; S385 gồm câu *"Callback chỉ trả thành công sau bước này."*; S724: *"Envelope dataset_id/event_time phải trùng payload; event_type đúng entity, source_batch_id nằm trong manifest nguồn."*
- S224 (*"Dùng Spark SQL functions thay Python UDF khi có thể."*), S240 (*"[...] Các lần truyền cùng event khác nhau ở Kafka offset/ingested_at."*), S215 (clock được inject), S334 (*"[...] business key/match key còn khôi phục được."*), S352 (*"Lưu `issue_codes` cho mọi vấn đề phát hiện được [...]"*), S546 và S548 (kịch bản chạy `publish` trước `stream`), S861 (*"[...] publish events.jsonl một lần, chờ pipeline commit hết offsets của batch."*).
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

Accounts và statement: xem "Việc mở".

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

**Quyết định (Q1): `dataset_id` không bắt buộc trong `ingest_batches`.** S385 liệt kê nội dung của dòng (khóa, khoảng offset, số bản ghi) và không có `dataset_id`; S298 đặt `dataset_id` ở từng record Raw (*"nếu parse được"*). Một microbatch có thể chứa nhiều dataset hoặc record không đọc được. Vì vậy:

- Migration **mới** `0002` (không sửa `0001`): `ALTER TABLE ingest_batches ALTER COLUMN dataset_id DROP NOT NULL`. Bước `down` đặt lại `NOT NULL` và **thất bại rõ ràng nếu còn dòng NULL** (không xóa dữ liệu).
- Streaming ghi `dataset_id = NULL`; `dataset_id` nằm ở từng dòng Raw và Bronze.

| Cột bảng | Giá trị |
|---|---|
| `query_name`, `batch_id` | của query `foreachBatch` (S385) |
| `dataset_id` | `NULL` (cần migration 0002) |
| `ingestion_date` | như mục 7 |
| `topic_offsets` | khoảng offset theo topic-partition của batch (S385); dạng: Q3 |
| `record_count` | số record Raw của batch (G7) |
| `raw_path`, `bronze_path` | URI `s3a://` theo ADR-0002 |
| `status` | `COMMITTED` (mặc định của cột, ADR-0003 mục 2 và `0001_init_control_schema.up.sql`) |

Quyết định này **sửa ADR-0003 mục 2** (cột `dataset_id`). Hệ quả phải làm cùng migration 0002 (thuộc task hiện thực sau khi Accepted, cần B duyệt vì đụng mã của T24):

- `IngestBatchRecord.dataset_id` đổi sang `str | None`.
- `list_committed_ingest_batches(dataset_id=...)` đang lọc `WHERE dataset_id = %s` nên sẽ **không trả dòng nào**; phải liệt kê theo `query_name`. Đổi cả giao diện `ControlStore` (`control_store.py`), hai hiện thực, `src/n06/storage/commit.py` và store giả trong `tests/unit/storage/test_commit_protocol.py`.
- `tests/unit/control/test_control_migrations_unit.py` (dòng 119, 126, 131) kỳ vọng `["0001"]` trên thư mục migration thật nên sẽ đỏ khi thêm `0002`.
- `tests/integration/test_control_migrations.py` đang kỳ vọng đúng migration `0001` và gọi `migrate_down` trên DB control dùng chung: khi đã có dòng `dataset_id` NULL, bước `down` của 0002 làm test thất bại, nên test cần DB riêng hoặc dọn dòng của chính nó.
- Chỉ mục `idx_ingest_batches_dataset_date` không còn hữu ích.

### 9. `n06 wait-ingestion --dataset-id X` làm được khi `ingest_batches.dataset_id` là NULL

**Thuật toán** (so byte của nguồn với byte đã lưu ở Raw):

1. Thư mục nguồn lấy từ `--source-dir`, mặc định `<paths.generator_output_dir>/source` (**không** dùng thư mục gốc, vì gốc còn chứa ground truth). Đọc và kiểm nguồn bằng đúng bước của producer (liệt kê theo manifest, kiểm `sha256`, `physical_rows`, từ chối file events không có trong manifest), chỉ lấy manifest có `dataset_id = X`. Phép kiểm dùng lại là: liệt kê theo manifest, `sha256`, `physical_rows`, và từ chối file events không có trong manifest; **không** dùng phần chọn key của producer (phần đó còn từ chối dòng không phải object JSON, `event_type` lạ hay thiếu `transaction_id`, nên nguồn có dòng như vậy cũng không publish được bằng `n06 publish`). Không có manifest của `X`, không có file events, lệch manifest, hoặc không có dòng nào (`M` rỗng): thoát ngay với lỗi cấu hình, không chờ.
2. Đa tập kỳ vọng `M`: với mỗi dòng của các file events đó, `sha256` của byte dòng (không gồm LF) và số lần xuất hiện.
3. Đa tập quan sát `N`: `sha256(value)` và số lần, trên các dòng **Raw** của những batch có trong `ingest_batches` của `query_name` đang chờ (S386).
4. Sẵn sàng khi và chỉ khi với mọi giá trị băm `h` trong `M`: `N[h] ≥ M[h]` (join hai bảng đã gộp trong Spark). Chưa đủ thì đợi batch mới; quá `--timeout-seconds` thì thoát mã khác 0, nêu số dòng còn thiếu và số dòng `parse_error`.

Raw được dùng vì giữ **mọi** record và byte `value` nguyên dạng (S298); bảng entity Bronze không giữ byte và `parse_error` không chép `value`. Cột `dataset_id` của Raw và Bronze (S298, Q4) phục vụ bên đọc và chẩn đoán; lệnh này chọn dataset bằng manifest của `X`, nên không cần `ingest_batches.dataset_id`.

**Lập luận.**

- *Sẵn sàng nghĩa là đủ nội dung.* Dòng `ingest_batches` chỉ có sau khi output của batch đã ghi xong (S385). `N ≥ M` nghĩa là mỗi dòng nguồn của `X`, kể cả bản lặp, có ít nhất bấy nhiêu bản **giống từng byte** trong Raw đã commit. Nguồn khác phiên bản (cùng `event_id`, khác nội dung) cho byte khác nên không được tính; dòng vi phạm schema vẫn được tính vì Raw giữ nó.
- *Tiến tới sẵn sàng.* Producer gửi đúng byte từng dòng (`tests/integration/test_publish_kafka.py::test_default_key_count_matches_manifest_and_bytes_identical` so đa tập byte của topic với file nguồn); Raw giữ nguyên `value`. Với các giả định G17-G21, khi query đã commit hết phần vừa publish thì `N ≥ M`.
- *Vì sao không đếm hay so `event_id`.* Số dòng bị bản lặp đánh lừa (đo 2026-11-25 trên `generator.demo-faults.json` 20.000 giao dịch: 57.727 dòng, 57.538 `event_id` phân biệt). So tập `event_id` báo sẵn sàng sai khi bản cũ của cùng dataset đã được ingest, hoặc khi hai dòng cùng `event_id` khác nội dung (`fixtures/negative/event_id_conflict.jsonl`) rơi vào hai batch; và không bao giờ đúng với dòng không đọc được `event_id`.

**Giới hạn đã biết.**

- Hai dòng **giống từng byte** (event replay, hoặc phát lại bằng `--allow-non-empty-topic`) không phân biệt được: bản chép thừa có thể làm đủ số lần trước khi bản gốc thứ hai tới. Nội dung vẫn đủ.
- Kết luận đúng tại thời điểm đọc. Khi query phát lại một batch đã commit, thư mục bị ghi đè nên tạm thời dở dang với bên đọc (ADR-0002 mục 2); lệnh coi lỗi đọc là "chưa sẵn sàng" và thử lại, còn bên xử lý tiếp theo không nên chạy khi query đang phục hồi.
- S861 nói chờ "commit hết offsets": tiêu chí trên tương đương khi topic chỉ nhận lần publish của `X`; nếu người duyệt muốn đúng nghĩa offset thì thêm điều kiện offset commit đạt offset cuối của topic ghi sau khi publish.
- Fixture nghiệm thu dùng bố cục khác (`source_manifest.json`, `fixtures/acceptance/events.jsonl`) mà producer hiện cũng chưa đọc được; cách nạp fixture là việc mở.
- Lệnh chạy như job Spark trong image job (S201) và phải thấy được thư mục nguồn; service `spark` của Compose hiện gắn volume riêng tại `/workspace/data`, không thấy dữ liệu trên host. **Chưa hiện thực, chưa đo** chi phí.

## Giả định (được chấp nhận cùng ADR ngày 2026-11-27)

- **G1.** Bước parse của Bronze là **chỉ kiểm JSON Schema của contract**, tức hàm `parse_and_validate_json_schema(raw, "event_envelope")` của `src/n06/domain/schema_validator.py`; kiểm tra ngữ nghĩa (Phần II mục 3) thuộc Silver, **không** dùng `validate_event_envelope` hay `validate_transaction_record` để định tuyến. Record vi phạm bất kỳ mã mức schema nào (`INVALID_JSON`, `UNSUPPORTED_SCHEMA_VERSION`, `MISSING_REQUIRED_FIELD`, `SCHEMA_TYPE`, `UNSUPPORTED_CURRENCY`, `AMOUNT_OUT_OF_RANGE`, `SCHEMA_VALUE`) vào `parse_error`. Đặc tả không nói rõ ranh giới giữa "parse lỗi" và "quality".
  **Một record xấu không được làm hỏng batch (S298, S304).** Đo ngày 2026-11-25, bộ kiểm hiện tại: số nguyên 5000 chữ số ném `ValueError`; mảng lồng 200.000 cấp ném `RecursionError`; `bytearray` của một dòng hợp lệ trả `SCHEMA_TYPE`; byte UTF-8 hỏng bị thay thế rồi cho mã khác (`errors="replace"`). Vì vậy T22 phải: xử lý `value` null (tombstone, Raw cho phép null) **trước** khi ép kiểu, ghi vào `parse_error` với mã `SCHEMA_TYPE` (`bytes(None)` ném `TypeError`); ép `value` còn lại về `bytes`; giải mã UTF-8 nghiêm ngặt, thất bại là `INVALID_JSON`; bắt `ValueError` và `RecursionError` quanh bộ kiểm và ghi `INVALID_JSON`. Đây là thiếu sót của bộ kiểm T10, nên sửa ở task riêng.
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
- **G15.** Cách suy `entity` và `business_key` theo Q5.
- **G16.** `issue_codes` lưu mã ở **mọi** mức (đúng hành vi validator hiện có); S876 chỉ yêu cầu khi *"nhiều lỗi cùng mức"*.
- **G17.** Query đọc topic từ offset **đầu** (`startingOffsets = earliest`), vì kịch bản chạy `publish` trước `stream` (S546, S548). Mặc định của Spark cho streaming là `latest` theo tài liệu Spark (chưa chạy trong repo).
- **G18.** Kafka chưa xóa message theo retention trước khi query đọc tới.
- **G19.** `--source-dir` là đúng nguồn đã publish, và query đọc đúng topic đó.
- **G20.** `wait-ingestion` biết `query_name` từ cấu hình (khóa do T22 thêm theo ADR-0002); mỗi topic một query.
- **G21.** Nguồn đã sinh xong: manifest events được ghi sau dữ liệu, và bước kiểm nguồn từ chối file events không có trong manifest.
- **G22.** Bộ kiểm Python chạy trong `foreachBatch` (UDF), dù S224 ưu tiên Spark SQL functions, để dùng lại đúng bộ kiểm của contract.

## Quyết định đã chốt (Q1 đến Q8)

Chủ repo chốt ngày 2026-11-27: dùng đúng câu trả lời đề xuất. "Spec im lặng" nghĩa là đã chọn phương án nhỏ nhất và dễ đảo ngược.

| # | Câu hỏi | Quyết định | Nguồn |
|---|---|---|---|
| Q1 | `ingest_batches.dataset_id` đang `NOT NULL`; một batch có thể chứa nhiều dataset hoặc record hỏng | Cho `NULL` bằng migration mới `0002` (sửa ADR-0003 mục 2); `dataset_id` ở từng dòng Raw/Bronze; `wait-ingestion` so byte nguồn với Raw (mục 9, giả định G17-G21) | S385 (không có `dataset_id`), S298, S432, S861 |
| Q2 | Phát lại batch ghi đè "cùng nội dung" nhưng `ingested_at`, `ingestion_run_id` khác | "Cùng nội dung" là mọi cột trừ hai cột đó. Yêu cầu "không ghi đè lỗi cũ" áp cho quarantine của Silver; `parse_error` là output của batch nên bị ghi đè khi phát lại. Hệ quả: sau sự cố, độ trễ tính từ `ingested_at` (S621) phồng lên cho batch phát lại | S387; S379 (nói cho batch); S317 nằm dưới mục Silver (S309); S384. Spec im lặng cho streaming |
| Q3 | Dạng `topic_offsets` | `{"<topic>": {"<partition>": [<đầu>, <cuối, không tính>]}}`, như tiền lệ `{"events": {"0": [0, 10]}}` đi cùng `record_count = 10` | S385 (chỉ nói "khoảng offset"); `tests/integration/test_control_migrations.py:175-176`. Spec im lặng về dạng. Quy ước offset cuối không tính của Spark: chưa kiểm trong repo |
| Q4 | `dataset_id` của Raw và `parse_error` lấy ở đâu | `envelope.dataset_id` nếu là chuỗi khớp pattern của contract, ngược lại `NULL`; không lấy từ payload. Lệch envelope và payload để Silver kiểm | S298 "nếu parse được"; S724; `event_envelope.schema.json` |
| Q5 | `entity`, `business_key` của `parse_error` | `entity`: theo `event_type` nếu nhận ra, ngược lại `NULL`. `business_key`: `transaction_id` hoặc `entry_id` nếu đọc được và là UUID, ngược lại `NULL`; không tạo key giả. Match key `(dataset_id, channel, external_reference)` **không** chép vào `parse_error`; bước tạo `key_conflicts` đọc lại Raw qua source pointer | S317, S334, S352, S898 (không tạo reference giả). Spec im lặng về cách suy ở Bronze |
| Q6 | Entity không có dòng nào trong batch | Không ghi thư mục; phát lại ghi đè **cả** thư mục `batch_id=<id>/` nên không sót thư mục cũ; bên đọc coi thiếu thư mục là rỗng | S384; ADR-0002 mục 2 (*"Chế độ overwrite chỉ áp dụng cho đúng thư mục `batch_id=<id>/`"*). Spec im lặng |
| Q7 | `ingested_at` gán lúc nào | Một giá trị cho cả batch, lấy khi callback bắt đầu từ clock được inject, giống nhau ở Raw và Bronze. Hai lần truyền trong cùng batch khác nhau ở offset, bằng nhau ở `ingested_at` | S238, S240, S215, S621. Spec im lặng về độ hạt |
| Q8 | Mã `ENVELOPE_PAYLOAD_MISMATCH` khi `event_type` lệch payload | Định tuyến vẫn theo schema (G1). Riêng dòng `parse_error`, gọi thêm `validate_event_envelope` (không truyền accounts) và **chỉ lấy mã `ENVELOPE_PAYLOAD_MISMATCH`** thêm vào `issue_codes`; không lấy mã ngữ nghĩa khác. Lời gọi này phải được bọc bắt ngoại lệ (đo được `OverflowError` với `event_time` năm 9999), lỗi thì giữ mã schema. Mã chính theo thứ tự ưu tiên. Lệch `dataset_id`/`event_time` vẫn qua schema nên Silver gắn mã | S891, S724, S876. Đã chạy thử 2026-11-25: validator domain thêm mã này, mã chính là `MISSING_REQUIRED_FIELD` |

## Việc mở

| Việc | Chủ | Hạn |
|---|---|---|
| Schema Bronze của accounts và statement (S740: *"Accounts và statements đều là JSONL trong v1 [...] Bronze chuyển sang Parquet là bước pipeline."*; S300 yêu cầu lưu statement nguyên bản), gồm cả nơi lưu `accounts.jsonl` | B (Quang) | trước khi bắt đầu T25 |
| Schema Silver | B (Quang) | trước khi bắt đầu T25 |
| Cách `wait-ingestion` đọc fixture nghiệm thu (S861) | A (T22) | trước khi kết thúc T22 |
| Sửa bộ kiểm T10 để không ném ngoại lệ trên record xấu (G1) | A (T10) | trước khi kết thúc T22 |

## Phương án đã cân nhắc

- Q1, lấy `dataset_id` từ cấu hình của lệnh `stream`: gán sai khi topic có record của dataset khác hoặc record hỏng.
- Q1, bảng con `(query_name, batch_id, dataset_id, record_count)`: thêm bảng control và vẫn không quy được record không có `dataset_id`; nặng hơn một lần `DROP NOT NULL`.
- Q1, một topic chỉ chứa một dataset và batch lạ thì thất bại: một record hỏng làm query dừng mãi.
- Q1, `wait-ingestion` chờ theo `record_count`: không ánh xạ được `--dataset-id` khi topic dùng chung, và bản lặp làm số đếm sai (mục 9).
- Q1, so tập `event_id` của nguồn với Bronze: báo sẵn sàng sai khi nguồn khác phiên bản hoặc khi `event_id` xung đột (mục 9).
- Bronze giữ một bảng duy nhất với `payload` dạng chuỗi JSON: không "tách entity" theo S304.
- Chuyển kiểu `date`/`timestamp` ngay ở Bronze: lấn việc chuẩn hóa của Silver (S311). (Record sai định dạng không vào bảng entity theo G1 và Raw vẫn giữ nguyên bytes, nên lý do không phải là mất dữ liệu.)
- Đưa record sai miền (ví dụ `amount_minor <= 0`) vào bảng entity để Silver lọc: cột entity phải cho null và kiểu lỏng hơn; xem G1.

## Hệ quả

- T22 dùng được `parse_and_validate_json_schema` và `validate_event_envelope` (chỉ thư viện chuẩn, nhưng phải bọc theo G1); bộ kiểm đọc schema từ `contracts/`. Phần I mục 2 (S201) yêu cầu image job chứa Java, PySpark, JAR đã ghim và gói `n06`; image hiện tại chưa chứa `n06` và `contracts/` (kế hoạch T22 quyết).
- Migration `0002` và thay đổi mã control store (mục 8) làm cùng T22 sau khi Accepted.
- T25 đọc Bronze theo `ingest_batches`; T30 dựng quarantine từ `parse_error` và lỗi ngữ nghĩa của Silver.
- Thêm cột sau này cần ADR mới.

## Ngoài phạm vi

- Silver (xem "Việc mở").
- Gold: ADR-005 theo `docs/TASKS.md`.
