# src/n06/warehouse/

**Mục đích:** Lệnh `load-mart`, `backfill`: staging, publish mart trong một transaction.

**Người sở hữu:** C (Quý)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 8.2, 8.3, 9

**Quy tắc:** Không Spark JDBC append thẳng vào bảng cuối. Publish phải xóa được dòng cũ không còn trong output.

## Nội dung hiện có (T41, theo ADR-0005)

| File | Việc |
|---|---|
| `schema.py` | Mô tả 4 dim + 3 fact: cột, kiểu, khóa chính. Phải khớp `sql/migrations/mart/0001_*`; integration test so với `information_schema`. |
| `migrations.py` | `migrate_mart_up/down` dùng lại trình chạy của `n06.control`, thư mục `sql/migrations/mart`. |
| `runner.py` | `SqlRunner` (`psql` qua stdin, `ON_ERROR_STOP=1`), `make_psql_runner`, `with_params`. |
| `publish.py` | `MartPublisher`: staging, kiểm tra, publish một transaction. |
| `load_mart.py` | Logic thuần của `n06 load-mart` (T26): lập kế hoạch đọc `current_snapshot`, dựng `dim_date`/`dim_channel`/`dim_transaction_type`, chuẩn hóa dòng Spark. |
| `load_mart_job.py` | Job Spark chạy trong container (`spark-submit`): đọc Gold Parquet đã commit theo `current_snapshot`, nạp staging + publish qua `MartPublisher`, cập nhật `runs.status = 'MART_PUBLISHED'`. |

### Migration mart

- `0001_init_mart_schema`: `dim_date`, `dim_account`, `dim_channel`, `dim_transaction_type`, `fact_transactions`, `fact_daily_balances`, `fact_reconciliation`, `mart_publish_metadata`. Bảng được chuyển về chủ sở hữu database (user mart) để ứng dụng publish bằng quyền của chính nó.
- `0002_read_only_role`: role `n06_mart_ro` chỉ `SELECT` (ADR-0013 Q6). Mật khẩu **không** nằm trong migration; T43 đặt bằng `ALTER ROLE n06_mart_ro PASSWORD ...` từ `.env`. `down` thu hồi quyền nhưng không `DROP ROLE` (role thuộc cả cluster).
- Lần chạy đầu phải do role có quyền tạo role (superuser của Compose), vì role là đối tượng của cả cluster; các lần sau idempotent. User mart thường không có `CREATEROLE`.

### Cột bổ sung so với ADR-0005

- `fact_reconciliation.input_snapshot_id TEXT NULL`: theo đặc tả mục 7.3 ("P0 lưu `last_evaluated_at`, input snapshot id và recon rule version") và T33 (PR #32) xuất cột này. Quyết định của giám sát; ADR-0005 chưa liệt kê.
- `mart_publish_metadata`: ADR-0005 mục 3 và spec mục 8.2 ("publish metadata" trong cùng transaction). Khóa `(dataset_id, gold_run_id, business_date_from, business_date_to)`; publish lại cùng run và phạm vi thì cập nhật dòng.

- `dim_account` theo hợp đồng đầu vào, không theo bảng cột của ADR-0005: `dataset_id`, `account_id`, `account_type`, `currency`, `opening_balance_minor`, `opening_business_date`, `run_id`. Không có `account_code`, `status`, `opening_date` vì `account.schema.json` không có các trường đó (hợp đồng thắng; giám sát đang sửa ADR-0004b 3.4 và ADR-0005 bằng PR riêng).

### Giao thức publish (`MartPublisher`)

1. `create_staging`: bảng `stg_<bảng>_<16 hex của sha256(run_id)>` (tên suy từ danh sách bảng cho phép, không từ chuỗi người dùng), tạo bằng `CREATE TABLE ... AS SELECT * ... WITH NO DATA` nên không có ràng buộc.
2. `stage_rows` nạp theo lô qua tham số JSON (cho khối lượng nhỏ và test), hoặc nạp bằng đường khác (Spark JDBC vào đúng tên staging) rồi gọi `publish_staged`.
3. `validate_staging`: không null PK/cột bắt buộc, không trùng PK, đúng `dataset_id`, `business_date` nằm trong khoảng, `date_key` khớp `business_date`, `opening + credit - debit = closing` (tính bằng `numeric`). Lỗi ném `MartValidationError`, mart chưa bị đụng.
4. `publish_staged`: một `BEGIN ... COMMIT` upsert 4 dimension, mỗi fact `DELETE` đúng `(dataset_id, business_date BETWEEN from AND to)` rồi `INSERT` từ staging, ghi metadata. Lỗi giữa chừng thì `psql` thoát không COMMIT: rollback toàn bộ, kể cả dimension.
5. `publish` bọc cả luồng và luôn `DROP` staging.

Giá trị dữ liệu đi qua biến psql `:'params'` (JSON); tiền phải là `int` Python trong miền BIGINT (không `float`, không `bool`).

### Điểm cần biết khi nối T26/T40

- Phạm vi thay thế chỉ theo `business_date` của dòng. Nếu output mới đưa một khóa về một `business_date` trong phạm vi trong khi dòng cũ của khóa đó nằm ngoài phạm vi, `INSERT` vi phạm PK và cả transaction rollback (không tự xóa dòng ngoài phạm vi). ADR-0005 mục 4 chưa quy định trường hợp này.
- `dim_date`, `dim_channel`, `dim_transaction_type` được `n06 load-mart` (T26) dựng tự động từ dữ liệu fact trong phạm vi ngày và upsert trong cùng transaction.
- Cập nhật trạng thái `runs.status = 'MART_PUBLISHED'` ở control DB (ADR-0005 mục 4.4) được `n06 load-mart` (T26) thực hiện **sau khi** transaction mart commit thành công.
