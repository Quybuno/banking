# Data Dictionary — N06 Banking Data Pipeline

Tài liệu mô tả lược đồ và độ hạt (grain) các tầng dữ liệu theo `docs/spec.md` bản 1.2, `docs/adr/0004-schema-raw-va-bronze.md`, `docs/adr/0004b-schema-silver.md`, `docs/adr/0005-schema-gold-va-mart.md`, và `docs/adr/0014-cong-chat-luong-va-control-report.md`.

## Quy ước chung
- **Tiền tệ:** số nguyên `BIGINT` tính theo đơn vị nhỏ nhất (`amount_minor`, `*_minor`), không dùng `FLOAT`/`DOUBLE`.
- **Ngày nghiệp vụ (`business_date`):** múi giờ `Asia/Ho_Chi_Minh` (`DATE`).
- **Timestamp:** UTC dạng ISO-8601 `...Z` (`TIMESTAMPTZ` trong PostgreSQL).

## 1. Lakehouse (MinIO / S3A)

| Tầng | Thực thể | Grain / Khóa | Nội dung chính |
|---|---|---|---|
| Raw | `events` | 1 dòng vật lý Kafka | Payload JSON nguyên bản, `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingested_at` |
| Bronze | `transactions`, `ledger_entries` | `event_id` (vật lý, còn replay) | Bản ghi đã parse envelope + metadata Kafka/ingestion |
| Silver | `transactions` | `(dataset_id, transaction_id)` | Giao dịch đã khử replay, chuẩn hóa kiểu, `replay_count`, `source_pointer` |
| Silver | `ledger_entries` | `(dataset_id, entry_id)` | Bút toán kế toán kép (`DEBIT`/`CREDIT`) |
| Silver | `statements` | `(dataset_id, statement_record_id)` | Bản ghi sao kê từ kênh đối tác |
| Silver | `accounts` | `(dataset_id, account_id)` | Danh mục tài khoản và số dư mở đầu |
| Silver | `key_conflicts` | `(dataset_id, entity, channel, external_reference)` | Bằng chứng xung đột khóa nghiệp vụ khác payload |
| Quarantine | `records` | 1 bản ghi vật lý bị loại | Bản ghi lỗi schema/parse/âm tiền/mất cân bằng kèm `reason_code` |
| Gold | `fact_transactions` | `(dataset_id, transaction_id)` | Giao dịch chuẩn hóa phục vụ mart |
| Gold | `fact_daily_balances` | `(dataset_id, account_id, business_date, currency)` | `opening_balance_minor`, `debit_minor`, `credit_minor`, `closing_balance_minor` |
| Gold | `fact_reconciliation` | `(dataset_id, channel, external_reference)` | `recon_status` (8 trạng thái), `issue_codes`, `anchor_event_time`, `discrepancy_age_seconds`, `source_pointers` |

## 2. PostgreSQL Mart (`n06_mart`, schema `public`)

| Bảng | Khóa chính | Ghi chú |
|---|---|---|
| `dim_date` | `date_key` (`YYYYMMDD`) | Lịch ngày/tháng/quý/năm |
| `dim_account` | `(dataset_id, account_id)` | Thuộc tính tài khoản (SCD1); `fact_transactions` nối qua 2 role `account_from` và `account_to` |
| `dim_channel` | `channel` | Kênh giao dịch (`SIM_CHANNEL_A`, `SIM_CHANNEL_B`) |
| `dim_transaction_type` | `transaction_type` | Loại giao dịch |
| `fact_transactions` | `(dataset_id, transaction_id)` | Giao dịch đã commit |
| `fact_daily_balances` | `(dataset_id, account_id, business_date, currency)` | Bất biến `opening + credit - debit = closing` |
| `fact_reconciliation` | `(dataset_id, channel, external_reference)` | Kết quả đối soát hiện hành theo `as_of_time` |
| `mart_publish_metadata` | `(dataset_id, gold_run_id, business_date_from, business_date_to)` | `source_watermark_at`, `mart_published_at` đo freshness |

## 3. PostgreSQL Control (`n06_control`)

- `ingest_batches`: theo dõi từng batch streaming Raw/Bronze đã commit.
- `pipeline_runs`: vòng đời từng lần chạy (`RUNNING` → `GOLD_COMMITTED` → `MART_PUBLISHED` / `FAILED`).
- `current_snapshot`: con trỏ atomic tới phiên bản Parquet đã commit cho từng `(dataset_id, layer, entity, business_date)`.
- `quality_metrics`: số đếm tầng, tỷ lệ quarantine, xung đột khóa và đối chiếu `control_report.json` (ADR-0014).
