# ADR-0017: Cách Metabase đọc `quality_metrics` từ `n06_control`

- Trạng thái: Proposed
- Người viết: C (agent thực hiện T43)
- Người duyệt: A, B, chủ repo
- Ngày: 2026-11-28
- Mục đặc tả liên quan (`docs/spec.md`): Phần I mục 2, mục 6, mục 9; ADR-0013; ADR-0014

## Bối cảnh và trích dẫn nguyên văn chỗ mâu thuẫn

1. **ADR-0013** (`docs/adr/0013-metabase-airflow.md`, mục 5, dòng 188 & 196):
   > *"Metabase không được dùng `n06_mart_user` (chủ sở hữu, ghi được). Cần một role chỉ đọc cho database mart (đề xuất tên `n06_mart_ro`, `CONNECT` trên database vì script init đã `REVOKE CONNECT ... FROM PUBLIC`, `USAGE` trên schema mart, `SELECT` trên bảng và `ALTER DEFAULT PRIVILEGES` cho bảng mới)."*
   > *"3. Admin, Databases, thêm PostgreSQL: Host `postgres`, Port `5432`, Database `n06_mart`, Username và Password của role chỉ đọc; lọc schema chỉ gồm schema mart; chạy 'Sync database schema'."*

2. **ADR-0014** (`docs/adr/0014-cong-chat-luong-va-control-report.md`, mục Hệ quả, dòng 28):
   > *"T43 đọc `quality_metrics` để hiển thị; T50 kiểm khớp control report trên fixture acceptance qua pipeline."*

Hiện tại `quality_metrics` nằm trong database `n06_control`, còn ADR-0013 chỉ cấp quyền kết nối cho Metabase vào database `n06_mart` qua role `n06_mart_ro` (không có role chỉ đọc trên `n06_control`). Theo quy tắc dừng khi đặc tả/ADR chưa chốt, T43 **tạm dừng** phần truy vấn `quality_metrics` trên Metabase và đề xuất ADR này.

## Phương án đề xuất

### Phương án A (Khuyến nghị): Tạo role chỉ đọc `n06_control_ro` chỉ cấp `SELECT` trên `quality_metrics`
- Thêm migration idempotent trong `sql/migrations/control/` tạo role `n06_control_ro` chỉ có `CONNECT` trên `n06_control`, `USAGE` trên `public`, và `SELECT` duy nhất trên bảng `quality_metrics` (không cho đọc `pipeline_runs` hay `ingest_batches` nếu không cần).
- Thêm kết nối thứ hai trong Metabase (`n06_control` bằng `n06_control_ro`) cho câu hỏi `07_quality_metrics.sql`.
- **Ưu điểm:** Không nhân bản dữ liệu, hiển thị được cả các run bị `QUALITY_GATE_BLOCKED` (chưa publish sang mart).

### Phương án B: Publish bảng tổng hợp `mart_quality_metrics` sang `n06_mart` trong `load-mart`
- Metabase chỉ cần một kết nối duy nhất tới `n06_mart` bằng `n06_mart_ro`.
- **Nhược điểm:** Run bị `QUALITY_GATE_BLOCKED` không chạy `load-mart` nên không xuất hiện trong bảng mart.

## Hệ quả

- Sau khi ADR-0017 được duyệt (Accepted), bổ sung `07_quality_metrics.sql` vào `sql/dashboard_queries/` và cập nhật hướng dẫn Metabase.
