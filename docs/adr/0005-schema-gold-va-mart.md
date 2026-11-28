# ADR-0005: Schema Gold và PostgreSQL Mart (3 fact + 4 dim, grain, khóa, publish nguyên tử)

- Trạng thái: Accepted
- Người viết: agent (soạn theo chỉ thị ưu tiên thấp của chủ repo), 2026-11-27
- Người duyệt: C (Quý — người viết theo `docs/TASKS.md` mục 3) và B (Quang — người duyệt theo bảng đó)
- Ngày: 2026-11-27
- Đã duyệt (Accepted): chủ repo (giám sát được chủ repo giao toàn quyền), 2026-11-27
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 7.2, 7.3, 8.1, 8.2, 9; Phần II mục 6. Ngoài đặc tả: ADR-0002 (Accepted), ADR-0003 (Accepted), ADR-0004b (Proposed).

> **Lưu ý:** Tài liệu này ở trạng thái **Proposed**, chưa được dùng làm căn cứ code cho tới khi được chủ repo/người duyệt chuyển sang **Accepted**.

## Bối cảnh

`docs/TASKS.md` mục 3 yêu cầu chốt ADR-005 (schema Gold và mart: grain, khóa, cột của 3 fact + 4 dim theo đặc tả Phần I mục 9) trước T41 (và làm đầu vào thiết kế sớm cho T25, T26, T40, T41, T43). Đặc tả Phần I mục 9 đã nêu danh sách bảng và grain tổng quát, nhưng cần chốt tên cột, kiểu dữ liệu khớp giữa Gold Parquet (Spark) và PostgreSQL Mart (`n06_mart`), quy ước NULL, và cơ chế publish qua staging table trong một transaction (Phần I mục 8.2).

## Quyết định (đề xuất)

### 1. Bốn bảng Dimension (`layer = 'gold'` trên MinIO và schema `public` trên DB `n06_mart`)

Theo ADR-0002 điểm 5, trên MinIO mọi entity Gold (kể cả dimension) đều ghi dưới `<gold_prefix>/<dataset_id>/<entity>/run_id=<run_id>/business_date=<YYYY-MM-DD>/` và được `current_snapshot` trỏ tới. Trên PostgreSQL mart, bảng đích là bản hiện hành (SCD1 cho `dim_account`).

| Bảng | Grain / Primary Key trên Mart | Cột (Spark / PostgreSQL) |
|---|---|---|
| `dim_date` | `date_key` (`INT`, dạng `YYYYMMDD`) | `date_key` (int/INT PK), `calendar_date` (date/DATE UNIQUE NOT NULL), `year` (int/SMALLINT), `quarter` (int/SMALLINT), `month` (int/SMALLINT), `day` (int/SMALLINT), `day_of_week` (int/SMALLINT, 1=Mon..7=Sun) |
| `dim_account` | `(dataset_id, account_id)` | `dataset_id` (string/TEXT), `account_id` (string/TEXT), `account_type` (string/TEXT NOT NULL), `currency` (string/TEXT NOT NULL), `opening_balance_minor` (long/BIGINT NOT NULL), `opening_business_date` (date/DATE NOT NULL), `run_id` (string/TEXT NOT NULL). *Đính chính 2026-11-28:* bỏ `account_code`, `status`, đổi `opening_date` thành `opening_business_date` cho khớp `account.schema.json` của hợp đồng (phát hiện ở T25, PR #33) |
| `dim_channel` | `channel` (`TEXT`) | `channel` (string/TEXT PK), `channel_label` (string/TEXT NOT NULL) |
| `dim_transaction_type` | `transaction_type` (`TEXT`) | `transaction_type` (string/TEXT PK), `description` (string/TEXT NOT NULL) |

### 2. Ba bảng Fact lõi (P0)

#### 2.1. `fact_transactions` — Grain / PK: `(dataset_id, transaction_id)`

| Cột | Spark | PostgreSQL | Null | Ghi chú |
|---|---|---|---|---|
| `dataset_id` | string | TEXT | không | PK (1) |
| `transaction_id` | string | TEXT | không | PK (2) |
| `business_date` | date | DATE | không | Phân vùng Gold; phạm vi thay thế trên Mart |
| `date_key` | int | INT | không | FK -> `dim_date(date_key)` |
| `external_reference` | string | TEXT | không | Khóa đối soát |
| `account_from` | string | TEXT | không | Role 1 nối `dim_account(dataset_id, account_id)` |
| `account_to` | string | TEXT | không | Role 2 nối `dim_account(dataset_id, account_id)` |
| `channel` | string | TEXT | không | FK -> `dim_channel(channel)` |
| `transaction_type` | string | TEXT | không | FK -> `dim_transaction_type(transaction_type)` |
| `currency` | string | TEXT | không | `VND` / `USD` |
| `status` | string | TEXT | không | `SUCCESS` / `FAILED` |
| `amount_minor` | long | BIGINT | không | Số tiền nguyên dương (không float) |
| `event_time` | timestamp | TIMESTAMPTZ | không | UTC |
| `replay_count` | int | INT | không | Số bản ghi vật lý đã gộp ở Silver |
| `source_pointer` | string | TEXT | không | Source pointer đại diện để drill-down |
| `run_id` | string | TEXT | không | `run_id` của Gold snapshot |

#### 2.2. `fact_daily_balances` — Grain / PK: `(dataset_id, account_id, business_date, currency)`

| Cột | Spark | PostgreSQL | Null | Ghi chú |
|---|---|---|---|---|
| `dataset_id` | string | TEXT | không | PK (1) |
| `account_id` | string | TEXT | không | PK (2); gồm cả tài khoản không phát sinh trong ngày |
| `business_date` | date | DATE | không | PK (3) |
| `currency` | string | TEXT | không | PK (4) |
| `date_key` | int | INT | không | FK -> `dim_date(date_key)` |
| `opening_balance_minor` | long | BIGINT | không | Số dư đầu ngày |
| `debit_minor` | long | BIGINT | không | Tổng ghi nợ trong ngày (`>= 0`) |
| `credit_minor` | long | BIGINT | không | Tổng ghi có trong ngày (`>= 0`) |
| `closing_balance_minor` | long | BIGINT | không | Bất biến: `opening + credit - debit = closing` |
| `ledger_entry_count` | int | INT | không | Số bút toán trong ngày (`>= 0`) |
| `run_id` | string | TEXT | không | `run_id` của Gold snapshot |

#### 2.3. `fact_reconciliation` — Grain / PK: `(dataset_id, channel, external_reference)`

| Cột | Spark | PostgreSQL | Null | Ghi chú |
|---|---|---|---|---|
| `dataset_id` | string | TEXT | không | PK (1) |
| `channel` | string | TEXT | không | PK (2) |
| `external_reference` | string | TEXT | không | PK (3) |
| `business_date` | date | DATE | không | Ngày nghiệp vụ của khóa đối soát |
| `date_key` | int | INT | không | FK -> `dim_date(date_key)` |
| `recon_status` | string | TEXT | không | 1 trong 8 trạng thái theo thứ tự ưu tiên mục 7.2 |
| `issue_codes` | array&lt;string&gt; | TEXT[] | không | Toàn bộ mã vấn đề phát hiện được (rỗng nếu `MATCHED`) |
| `transaction_id` | string | TEXT | được | NULL nếu `MISSING_INTERNAL` hoặc nhiều transaction conflict |
| `internal_amount_minor` | long | BIGINT | được | Tiền phía nội bộ (NULL nếu thiếu/conflict) |
| `internal_currency` | string | TEXT | được | Currency phía nội bộ |
| `internal_status` | string | TEXT | được | Status phía nội bộ |
| `statement_amount_minor` | long | BIGINT | được | Tiền phía sao kê (NULL nếu thiếu/conflict) |
| `statement_currency` | string | TEXT | được | Currency phía sao kê |
| `statement_status` | string | TEXT | được | Status phía sao kê |
| `internal_record_count` | int | INT | không | Số bản ghi nội bộ cho match key |
| `statement_record_count` | int | INT | không | Số bản ghi sao kê độc lập cho match key |
| `anchor_event_time` | timestamp | TIMESTAMPTZ | được | NULL khi `MATCHED`; theo mục 7.3 |
| `discrepancy_age_seconds` | long | BIGINT | được | NULL khi `MATCHED`; `>= 0` khi khác `MATCHED` |
| `as_of_time` | timestamp | TIMESTAMPTZ | không | Mốc đánh giá của snapshot |
| `last_evaluated_at` | timestamp | TIMESTAMPTZ | không | Thời điểm tính snapshot |
| `first_seen_at` | timestamp | TIMESTAMPTZ | được | Để NULL ở P0; dùng ở P1 (T61) |
| `resolved_at` | timestamp | TIMESTAMPTZ | được | Để NULL ở P0; dùng ở P1 (T61) |
| `recon_rule_version` | string | TEXT | không | Phiên bản quy tắc đối soát |
| `source_pointers` | array&lt;string&gt; | TEXT[] | không | Danh sách source pointer hai phía để drill-down |
| `run_id` | string | TEXT | không | `run_id` của Gold snapshot |

### 3. Bảng mở rộng (P2) và metadata độ tươi (freshness)

- `reconciliation_history` (P2): PK `(dataset_id, channel, external_reference, semantic_version)`, chỉ chèn khi nội dung kết quả thay đổi.
- `mart_publish_metadata` (hoặc truy vấn kết hợp `runs` trong control DB): lưu `dataset_id`, `business_date_from`, `business_date_to`, `gold_run_id`, `mart_published_at`, `source_watermark_at` để dashboard hiển thị freshness đúng định nghĩa mục 9.

### 4. Giao thức publish từ Gold sang PostgreSQL Mart (Phần I mục 8.2)

1. `n06 load-mart` chỉ đọc các đường dẫn Gold đã commit từ `current_snapshot` (`layer = 'gold'`).
2. Nạp dữ liệu vào bảng staging có tên kiểm soát theo allowlist (`stg_<table_name>_<safe_run_hex>`).
3. Kiểm tra đếm dòng và ràng buộc trên staging (không null ở PK, không trùng PK, `opening + credit - debit = closing` trên `fact_daily_balances`).
4. Trong **một transaction PostgreSQL duy nhất**:
   - Upsert 4 bảng dimension (`ON CONFLICT ... DO UPDATE`).
   - Với 3 bảng fact: `DELETE` toàn bộ dòng cũ thuộc `(dataset_id, business_date BETWEEN from_date AND to_date)` rồi `INSERT` từ staging — bảo đảm xóa sạch stale rows khi output mới bỏ một bản ghi cũ (điều kiện T41).
   - Cập nhật trạng thái run sang `MART_PUBLISHED` trong control DB sau khi commit mart thành công.

## Phương án đã cân nhắc

- **Chỉ dùng `UPSERT` cho bảng fact trên Mart:** không xóa được dòng cũ không còn trong output mới (trái trực tiếp Phần I mục 8.2 và điều kiện hoàn thành T41). Không chọn.
- **Tách `fact_reconciliation_sources` thành bảng con ngay từ P0:** đầy đủ chuẩn hóa 3NF hơn, nhưng làm phức tạp publish transaction và câu truy vấn Metabase ở P0; cột `source_pointers TEXT[]` đủ phục vụ drill-down P0 và có thể tách bảng con ở P2 nếu số pointer lớn.

## Quyết định đã chốt (2026-11-27)

- **Q1** không còn là câu mở: `docs/spec.md` dòng 264 đã chốt *"Số dư cuối ngày = số dư đầu ngày + tổng CREDIT − tổng DEBIT theo tài khoản"* (dòng 262: DEBIT bên gửi, CREDIT bên nhận). `fact_daily_balances` dùng đúng công thức này.
- **Q2.** Không đặt FK cứng từ `fact_reconciliation` sang `dim_account` (để không chặn `MISSING_INTERNAL`).
- Role chỉ đọc `n06_mart_ro` tạo bằng migration idempotent của database mart trong T41 (ADR-0013 Q6).

Lịch sử: soạn Proposed (PR #24, merge vào main 2026-11-27 khi còn Proposed); Accepted 2026-11-27 với các câu trả lời trên.

## Câu hỏi mở (đã trả lời ở mục trên, giữ làm lịch sử)

1. **Chiều dấu của `debit_minor` / `credit_minor` trong `fact_daily_balances`:** chốt quy ước `closing = opening + credit - debit` (tài khoản nhận là `CREDIT`, tài khoản gửi là `DEBIT` — khớp trực tiếp fixture T40: A gửi 6.400 → 100.000 - 6.400 = 93.600; B nhận 6.400 → 50.000 + 6.400 = 56.400).
2. **`fact_reconciliation` khi một khóa bị sửa từ `business_date` cũ hoặc có nhiều ngày:** phạm vi `DELETE` theo `(dataset_id, business_date BETWEEN from_date AND to_date)` có cần thêm khóa ngoại kiểm tra với `dim_account` hay để mềm để không chặn `MISSING_INTERNAL` (khi giao dịch nội bộ không tồn tại)? Đề xuất: không đặt FK cứng từ `fact_reconciliation` sang `dim_account`.

## Hệ quả

- Sau khi Accepted: T26 (`load-mart` tối thiểu), T40 (`fact_daily_balances`), T41 (DDL mart đủ 3 fact + 4 dim), T43 (truy vấn dashboard Metabase) triển khai trực tiếp theo bảng cột và khóa ở trên.
