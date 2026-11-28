# sql/dashboard_queries/

**Mục đích:** Truy vấn SQL không tham số cho từng câu hỏi/biểu đồ Metabase (TASK T43, ADR-0013).

**Người sở hữu:** C (Quý)

**Đặc tả liên quan (`docs/spec.md`):** Phần I mục 9; `docs/adr/0013-metabase-airflow.md`; `docs/adr/0005-schema-gold-va-mart.md`; `docs/adr/0014-cong-chat-luong-va-control-report.md`.

**Quy tắc:**
- Mỗi file một truy vấn SQL thuần PostgreSQL không tham số (chạy được bằng `psql` và Metabase).
- Kết nối database `n06_mart` bằng role chỉ đọc `n06_mart_ro` (không dùng `n06_mart_user`).
- **Không tự động provision dashboard** (provision qua API là P2); các bước bên dưới là **tái lập thủ công**.
- Phần `quality_metrics` nằm ở database `n06_control` tạm dừng do ADR-0013 chưa cấp quyền đọc `n06_control` cho Metabase — xem đề xuất `docs/adr/0017-metabase-doc-quality-metrics.md`.

## Danh sách truy vấn

| File | Tiêu đề Metabase | Loại biểu đồ | Ghi chú |
|---|---|---|---|
| `01_transactions_by_day_channel.sql` | Số giao dịch theo ngày và kênh | Stacked Bar Chart | `fact_transactions` + `dim_channel` |
| `02_amount_by_currency_status.sql` | Tổng tiền giao dịch theo loại tiền và trạng thái | Table / Bar Chart | Số nguyên `amount_minor` (không float) |
| `03_daily_closing_balances.sql` | Số dư cuối ngày theo ngày và loại tiền | Line Chart / Table | `fact_daily_balances` (có dữ liệu sau T40) |
| `04_reconciliation_status_breakdown.sql` | Số lượng và tỷ lệ 8 trạng thái đối soát | Bar Chart / Table | Mẫu số = tổng match key; `PENDING` tách riêng |
| `05_discrepancy_age_buckets.sql` | Tuổi sai lệch chưa xử lý theo trạng thái và khoảng tuổi | Pivot / Stacked Bar | Nhóm theo `discrepancy_age_seconds` |
| `06_pipeline_freshness.sql` | Độ tươi pipeline (Freshness) | Table | `source_watermark_at` và `mart_published_at` |

## Hướng dẫn tái lập thủ công trên Metabase (`v0.64.9`)

> Lưu ý RAM (ADR-0013 mục 4): không bật `bi` cùng lúc với `orchestration` hoặc Spark nặng. Dừng `metabase` trước khi chạy benchmark (`docker compose --profile bi stop`).

1. **Đặt mật khẩu cho role chỉ đọc `n06_mart_ro`** (sau khi đã chạy migration mart của T41):
   ```bash
   docker compose exec -T postgres psql -U "${N06_POSTGRES_SUPERUSER:-n06_admin}" -d "${N06_POSTGRES_MART_DB:-n06_mart}" \
     -c "ALTER ROLE n06_mart_ro PASSWORD '${N06_POSTGRES_MART_RO_PASSWORD}';"
   ```
2. **Khởi động Metabase (profile `bi`)**:
   ```bash
   docker compose --profile core --profile bi up -d --wait
   ```
   Mở `http://localhost:${N06_METABASE_HOST_PORT:-13000}`.
3. **Thiết lập lần đầu (thủ công)**: tạo tài khoản quản trị Metabase, bỏ qua bước thêm dữ liệu mẫu.
4. **Thêm kết nối PostgreSQL mart**:
   - **Admin → Databases → Add database → PostgreSQL**
   - Host: `postgres`, Port: `5432`, Database name: `n06_mart`
   - Username: `n06_mart_ro`, Password: giá trị `N06_POSTGRES_MART_RO_PASSWORD` trong `.env`
   - Bấm **Save** và **Sync database schema now**.
5. **Tạo từng câu hỏi**:
   - **New → SQL query → chọn database `n06_mart`** → dán nội dung từng file `01_*.sql` .. `06_*.sql` → **Visualize** theo loại biểu đồ ghi ở đầu file → **Save** vào collection `N06`.
6. **Tạo dashboard `N06`**:
   - **New → Dashboard**, đặt tên `N06`, thêm 6 câu hỏi theo thứ tự `01` → `06`, sắp xếp bố cục và **Save**.
7. **Tắt Metabase sau khi demo/nghiệm thu**:
   ```bash
   docker compose --profile bi stop
   ```
