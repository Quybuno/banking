# Báo cáo Review PR — TASK T41 (`task/T41-mart`)

Worktree đã đọc: `/home/quybuno/banking/.claude/worktrees/agent-a8f21deb6d1508652`
Phương pháp: đọc mã nguồn tĩnh (không sửa file, không chạy lệnh shell trong phiên này — xem mục **Chưa kiểm**).

---

## Phải sửa (chặn merge)

1. **Lỗi assert sai kiểu `boolean::text` trong integration test role chỉ đọc (`tests/integration/test_mart_postgres.py:533-538`):**
   ```python
   role = _scalar(
       mart.admin,
       "SELECT rolcanlogin::text || rolsuper::text || rolcreaterole::text || rolcreatedb::text "
       f"FROM pg_roles WHERE rolname = '{RO_ROLE}';",
   )
   assert role == "tfalsefalsefalse"
   ```
   Trong PostgreSQL 16, ép kiểu `boolean::text` trả về `'true'` / `'false'` (không phải `'t'` / `'f'` như hiển thị cột boolean thô của `psql`). Vì vậy biểu thức trên trả về `"truefalsefalsefalse"`, khiến `assert role == "tfalsefalsefalse"` **FAIL** khi chạy trên PostgreSQL thật. (Chính dòng 664–667 trong cùng tệp cũng xác nhận `(...)::text` cho `"true"`.)
   - **Cách sửa:** đổi expected thành `"truefalsefalsefalse"` (hoặc nối bằng `'|'` như `"true|false|false|false"` cho dễ đọc).

2. **Lỗi test kiểm tra `PK NULL` ở mức DB truyền chuỗi `'NULL'` thay vì `SQL NULL` (`tests/integration/test_mart_postgres.py:574-586, 605`):**
   ```python
   def _direct_fact_tx(dataset_id: str, tx_id: str, **override: Any) -> str:
       values = {
           "dataset_id": f"'{dataset_id}'", "transaction_id": f"'{tx_id}'",
           ...
       }
       values.update({k: str(v) for k, v in override.items()})
   ...
   reject(_direct_fact_tx(dataset_id, "NULL"), "null value")  # PK null
   ```
   Tham số vị trí `"NULL"` bị bọc nháy đơn thành `"'NULL'"` (chuỗi 4 ký tự hợp lệ), nên câu `INSERT` thành công thay vì báo lỗi `null value`, làm `pytest.raises(_PsqlFailed, match="null value")` **FAIL** trên PostgreSQL thật.
   - **Cách sửa:** truyền qua `override`, ví dụ `_direct_fact_tx(dataset_id, "tx-null", transaction_id="NULL")`.

> Hai lỗi trên cho thấy `tests/integration/test_mart_postgres.py` chưa chạy xanh ở bản hiện tại trên nhánh. Cần sửa cả hai lỗi và chạy lại `pytest tests/integration/test_mart_postgres.py` trên PostgreSQL thật, dán output thật trước khi merge.

---

## Nên sửa

1. **Truy vấn `LIMIT 1` không xác định trong `tests/integration/test_mart_postgres.py:915-920`:**
   `fact_reconciliation` có 2 dòng cho `(dataset_id, 'CH_A')` (`tx-0-1` và `tx-0-2`), chỉ `tx-0-1` có `source_pointers = [nasty, "b"]`. Câu `WHERE dataset_id = '...' AND channel = 'CH_A' LIMIT 1` không có `AND transaction_id = 'tx-0-1'` hay `ORDER BY`, phụ thuộc thứ tự vật lý của heap. Nên lọc tường minh `AND transaction_id = 'tx-0-1'`.

2. **Phụ thuộc thứ tự chạy test trên DB dùng chung `scope="module"` (`tests/integration/test_mart_postgres.py:793-796, 899`):**
   `test_values_travel_as_parameters_not_sql_text` (dòng 899) ghi đè `dim_channel.channel_label` của `CH_A` thành `nasty` trên cùng DB `mart` dùng chung của module, trong khi `test_failure_inside_transaction_rolls_back_to_previous_version` (dòng 794–796) assert `CH_A` có nhãn `CHANNELS["CH_A"]`. Nếu chạy test không theo thứ tự file sẽ lỗi. Nên dùng mã kênh riêng cho test injection hoặc khôi phục lại nhãn.

3. **Chuẩn hóa dữ liệu lặp 2 lần trong `MartPublisher.publish` (`src/n06/warehouse/publish.py:354-360`):**
   `publish()` gọi `normalize_rows()` cho cả 7 bảng ở dòng 355 để kiểm kiểu trước khi chạm DB, rồi `stage_rows()` ở dòng 359 lại gọi `normalize_rows()` một lần nữa (dòng 228). Có thể tách helper nội bộ nhận danh sách đã chuẩn hóa để tránh xử lý lặp.

---

## Ghi nhận

### Đối chiếu 7 hạng mục `docs/TASKS.md` mục 6.3 và 4 điều kiện cốt lõi T41

1. **Điều kiện hoàn thành (kiểm bằng đọc mã nguồn):**
   - **Migration mart up/down idempotent, đủ 4 dim + 3 fact đúng cột/kiểu/khóa ADR-0005 (+ đính chính `dim_account` + `input_snapshot_id` S361), kiểm miền BIGINT, role `n06_mart_ro` chỉ SELECT:** **Đạt về thiết kế mã nguồn** (chưa chạy kiểm chứng runtime, xem bug integration test ở mục Phải sửa).
     - `sql/migrations/mart/0001_init_mart_schema.up.sql` & `src/n06/warehouse/schema.py`: đủ `dim_date`, `dim_account` (đã bỏ `account_code`, `status`, đổi `opening_date` → `opening_business_date` khớp `account.schema.json`), `dim_channel`, `dim_transaction_type`, `fact_transactions` (16 cột), `fact_daily_balances` (11 cột + constraint `opening::numeric + credit::numeric - debit::numeric = closing::numeric` chống tràn), `fact_reconciliation` (25 cột ADR-0005 + `input_snapshot_id TEXT` theo S361 + constraint `fact_reconciliation_matched_clean`, không đặt FK cứng sang `dim_account` theo ADR-0005 Q2), `mart_publish_metadata` (ADR-0005 §3), `schema_migrations`, index theo `(dataset_id, business_date)`, chuyển owner bảng về `db_owner`.
     - `0001_init_mart_schema.down.sql`: `DROP TABLE IF EXISTS` đúng thứ tự ngược phụ thuộc.
     - `0002_read_only_role.up.sql` & `.down.sql`: tạo `n06_mart_ro` idempotent (`LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT`, bắt `duplicate_object`), không chứa mật khẩu, chỉ cấp `CONNECT`, `USAGE`, `SELECT` + `ALTER DEFAULT PRIVILEGES ... GRANT SELECT`; `.down.sql` thu hồi quyền mà không `DROP ROLE` (an toàn cho role cấp cluster).
   - **Publish: staging → validate → 1 transaction thay thế phạm vi ngày → rollback thật khi lỗi:** **Đạt về thiết kế mã nguồn** (`src/n06/warehouse/publish.py:199-362`).
     - Tên staging `stg_<table_name>_<sha256(run_id)[:16]>` qua allowlist `TABLES_BY_NAME` và regex `STAGING_NAME_RE`.
     - `validate_staging`: kiểm `null_primary_key`, `null_required_column`, `duplicate_primary_key`, `wrong_dataset_id`, `outside_business_date_range`, `date_key_mismatch`, `balance_invariant_violation` trước khi đụng bảng đích.
     - `_transaction_sql`: 1 khối `BEGIN; ... COMMIT;` qua `psql -v ON_ERROR_STOP=1`: upsert 4 dim → `DELETE` đúng `(dataset_id, business_date BETWEEN from AND to)` rồi `INSERT` cho 3 fact → upsert `mart_publish_metadata`.
   - **Test "output mới bỏ một record cũ", ngoài phạm vi không bị đụng, chạy lại không tăng dòng:** **Đạt về kịch bản test** (`tests/integration/test_mart_postgres.py:639-745`, cần chạy lại sau khi sửa 2 lỗi ở mục Phải sửa).
   - **Tham số hóa SQL & tiền là `int` trong miền BIGINT:** **Đạt** (`src/n06/warehouse/runner.py:28-36` dùng `\set params` + `:'params'`; `src/n06/warehouse/publish.py:117-128` kiểm `type(value) is not int` loại `float`/`bool` và kiểm `[-2**63, 2**63 - 1]`).

2. **`contracts/`, `tests/fixtures/`, xóa/skip/nới test:** Không phát hiện `skip`/`xfail` hay nới expected trong các file test đã đọc (`test_mart_postgres.py:57` dùng `pytest.fail` khi thiếu Docker). `pyproject.toml` không thêm dependency mới. (Việc xác nhận toàn nhánh không chạm `contracts/`/`tests/fixtures/`/`src/n06/control/` xem mục Chưa kiểm.)

3. **Hardcode:** Không phát hiện hardcode endpoint, credential, ngày, seed, quy mô, partition, bucket hay topic trong `src/n06/warehouse/` và `sql/migrations/mart/`.

4. **Float cho tiền & ground truth:** Không dùng float cho tiền (toàn bộ cột `*_minor` là `BIGINT`, kiểm `type(value) is int`, tổng số dư ép `::numeric`). Không import `tests/` hay đọc ground truth trong `src/n06/warehouse/`.

5. **Phạm vi task:** Đúng phạm vi T41; giữ đúng ranh giới với T26 (`load-mart` / `MART_PUBLISHED`), T42 (`backfill`), T43 (Metabase & đặt password cho `n06_mart_ro`). Tái sử dụng `n06.control` qua adapter `RunnerSqlExecutor`.

6. **Đối chiếu đặc tả / ADR:**
   - Hai điểm khác bản ADR-0005 đang có trên nhánh (`dim_account` theo `account.schema.json` và `fact_reconciliation.input_snapshot_id TEXT` theo spec S361) đều khớp quyết định đã chốt của giám sát và được ghi rõ trong `src/n06/warehouse/README.md`.
   - Lưu ý nhỏ: `tests/integration/test_mart_postgres.py:268` đặt `"account_type": "CHECKING"` thay vì `"CUSTOMER_DEPOSIT"` theo `account.schema.json` (không ảnh hưởng DDL vì không có CHECK constraint trên `account_type`).
   - Lưu ý nhỏ: `dim_account.opening_balance_minor` chưa có `CHECK (opening_balance_minor >= 0)` (theo `account.schema.json` minimum là 0; ADR-0005 không yêu cầu).

7. **Expected test độc lập:** Đạt. `tests/integration/test_mart_postgres.py:340-442` định nghĩa bảng `ADR_0005` độc lập hoàn toàn với `n06.warehouse.schema` để đối chiếu `information_schema` và `pg_index`; unit test của `publish.py` và `migrations.py` dùng expected tính độc lập.

- **Nhắc mục 6.4 `docs/TASKS.md`:** T41 (*"Publish mart trong một transaction"*) nằm trong danh sách mã lõi yêu cầu người (C và B) tự đọc trước khi merge.

---

## Chưa kiểm

Phiên reviewer này không có công cụ chạy lệnh shell, nên các hạng mục sau **chưa được thực thi trực tiếp**:
- `git diff --name-status origin/main` / `git status` để xác nhận danh sách đầy đủ mọi tệp bị sửa trong nhánh (bao gồm xác nhận không có thay đổi ngoài ý muốn ở `contracts/`, `tests/fixtures/`, `src/n06/control/`, `sql/migrations/control/`).
- `ruff check . && ruff format --check .`
- `python scripts/check_rules.py`
- `python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py`
- `pytest tests/unit -q`
- `pytest tests/integration/test_mart_postgres.py -q` (bao gồm kiểm chứng thực tế `with_params` với `psql` PostgreSQL 16 và kiểm chứng sau khi sửa 2 lỗi ở mục **Phải sửa**).

