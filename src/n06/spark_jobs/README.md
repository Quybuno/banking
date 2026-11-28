# src/n06/spark_jobs/

**Mục đích:** Job Spark Silver, Gold, số dư; lệnh `process-day`.

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 6, 9

**Quy tắc:** Đọc input theo bảng control, không glob toàn bucket.

## `n06 process-day` (T25, bản sạch)

```bash
n06 process-day --profile demo --dataset-id <id> [--as-of-time 2026-01-04T00:00:00Z] \
    [--business-date 2026-01-01 ...] [--query-name <query>]
```

Chuỗi đứng trước: `generate` -> `publish` -> `stream --available-now` -> `wait-ingestion` ->
`ingest-statements`. Lệnh chạy như `n06 stream`: host kiểm cấu hình rồi mở một container dùng một
lần của image Spark (`spark-submit`, `spark.master` và `spark.driver_memory` từ cấu hình).

**Đầu vào (không glob):**

- Bronze `transaction` và `ledger_entry` của mọi dòng `ingest_batches` `COMMITTED` thuộc
  `kafka.query_name` (hoặc `--query-name`), lọc theo `dataset_id`. Entity không có trong một batch
  thì bỏ qua thư mục đó.
- Sao kê: biên nhận `<statements_prefix>/<dataset_id>/_ingest/<9 chữ số>.json`. Mỗi biên nhận phải có
  file dữ liệu và manifest; `sha256` và số dòng của file được so với biên nhận trước khi đọc nội dung
  (lệch thì job dừng). Silver sao kê mang `file_available_at` và `delivery_order` của biên nhận
  (ADR-0006 D3); đối soát chỉ dùng sao kê có `file_available_at <= as_of_time`.
- Tài khoản: `<statements_prefix>/<dataset_id>/accounts.jsonl` (do `n06 ingest-statements` tải lên,
  ADR-0004b Q2).
- `--as-of-time` là đồng hồ giả lập; không đặt thì CLI đọc đồng hồ một lần ở host rồi truyền tường
  minh cho job. Mã Spark không bao giờ đọc đồng hồ.

**Đầu ra** (Parquet, mỗi `business_date` một thư mục, cột `business_date` nằm trong file):

| Tầng | Entity | Ghi chú |
|---|---|---|
| silver | `transactions`, `ledger_entries` | gộp bản trùng theo khóa nghiệp vụ, giữ `event_ids`, `replay_count`, `source_pointers` |
| silver | `statements` | loại trùng theo `statement_record_id`; nhiều `statement_record_id` cho cùng match key được giữ |
| silver | `accounts` | bản chụp ghi dưới mọi `business_date` của run (ADR-0002 điểm 5) |
| gold | `fact_transactions` | ADR-0005 mục 2.1 |
| gold | `fact_reconciliation` | chỉ khóa MATCHED (xem dưới) |

**Giao thức commit (đặc tả I.8.1 batch, ADR-0002 mục 3 và 6).**

1. `start_run`, rồi gọi `require_unused_run_prefix` cho MỌI gốc `run_id=<id>/` của run với bản liệt kê
   S3 thật, trước khi ghi object đầu tiên. Run đã có object thì job dừng, `COMMIT_ERROR`, không ghi
   thêm.
2. Mỗi (tầng, entity, ngày) ghi bằng `mode("errorifexists")` vào đường dẫn của `build_run_output_path`,
   đọc lại và đối chiếu số dòng.
3. Sau khi mọi output đã ghi và kiểm tra mới `commit_run` từng `current_snapshot`. Lỗi ở bất kỳ bước
   nào thì `fail_run`; `current_snapshot` không bị chạm tới trước bước 3.
4. `commit_run` đòi mọi lần commit của cùng `run_id` mang cùng `semantic_hash`, nên hash là hash
   của **cả run**: băm nội dung từng entity (không phụ thuộc thứ tự dòng và `run_id`), danh sách
   input (`ingest_batches`, biên nhận, `as_of_time`, ngày được chọn), `config_hash`, phiên bản schema,
   quy tắc và mã (`plan.py`).

**Liệt kê S3 trong job Spark.** `s3_listing.make_list_objects` dùng `FileSystem.listFiles` của
Hadoop S3A trên chính cấu hình S3A của phiên Spark (không thêm phụ thuộc). `require_unused_run_prefix`
nhận hàm này, nên kiểm tra chạy trên bucket thật chứ không trên danh sách giả.

**Chưa làm (task khác).** Quarantine đầy đủ (T30), `key_conflicts` và xung đột khóa (T31), 8 trạng
thái đối soát (T33), số dư (T40), mart (T26, T41). Hệ quả cho bản T25:

- Bản ghi mà T30 hoặc T31 sẽ xử lý làm job **dừng** với một dòng `N06_PROCESS_DAY_ERROR={"code":...}`
  (mã `BUSINESS_KEY_CONFLICT`, `EVENT_ID_CONFLICT`, `BUSINESS_DATE_INVALID`, `STATEMENT_INVALID_ROWS`,
  `STATEMENT_ID_CONFLICT`, `ACCOUNT_INVALID_ROWS`, `ACCOUNT_ID_CONFLICT`, `STATEMENT_FILE_MISMATCH`,
  `RECEIPT_WITHOUT_FILE`, `RECEIPT_WITHOUT_MANIFEST`), mã thoát 14; không có dữ liệu nào bị bỏ im
  lặng.
- `fact_reconciliation` chỉ chứa khóa MATCHED (đủ một giao dịch độc lập và một sao kê độc lập, trùng
  `currency`, `amount_minor`, `status`). Khóa còn lại chỉ được đếm trong
  `reconciliation.unclassified_keys` của kết quả, chưa được ghi và chưa có trạng thái (T33).
- Một run commit nhiều `current_snapshot` bằng nhiều lần gọi `commit_run` (API của T24 chỉ nhận một
  snapshot mỗi transaction): chết giữa chừng có thể để một phần snapshot trỏ sang run mới.

**Cột `accounts`.** Theo `account.schema.json` của hợp đồng (`opening_business_date`); danh sách cột
trong ADR-0004b 3.4 có `account_code`, `status`, `opening_date` mà hợp đồng không có.

**Kiểm tra.** `tests/unit/spark_jobs/` (thuần Python: biên nhận, đường dẫn, kiểm run chưa dùng, hash,
CLI) và `tests/integration/test_process_day_clean.py` (Kafka, MinIO, PostgreSQL, Spark thật).
