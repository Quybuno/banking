# src/n06/reconciliation/

**Mục đích:** Đối soát 8 trạng thái, issue_codes, cutoff theo as_of_time, tuổi sai lệch.

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 7

**Quy tắc:** Không gọi đồng hồ hệ thống; mọi phép so thời gian dùng as_of_time được truyền vào.

## Thành phần (T33)

- `rules.py`: tám trạng thái và thứ tự ưu tiên (bảng 7.2), thứ tự `issue_codes`, cột Silver bắt buộc. Không cần Spark.
- `settings.py`: `ReconciliationSettings` (thời gian chờ, múi giờ, tập status/channel) lấy từ `AppConfig`, mục `[reconciliation]` (ADR-0006 Q10). Không cần Spark.
- `reconcile.py`: `reconcile(transactions, statements, key_conflicts, as_of_time=..., settings=..., run_id=..., input_snapshot_id=...)` nhận DataFrame Silver theo ADR-0004b và trả một dòng cho mỗi match key theo cột `fact_reconciliation` (ADR-0005 mục 2.3, thêm `input_snapshot_id` do spec S361 yêu cầu). Cần PySpark và `spark.sql.session.timeZone = UTC`; chạy trong image Spark. Unit test Spark nằm ở `tests/integration/test_recon_spark.py`.

## Quy tắc đã hiện thực (ADR-0006)

- Sao kê có mặt khi `file_available_at <= as_of_time`, với `file_available_at` lấy từ biên nhận `_ingest/` (cột của `silver.statements`); giao dịch không bị lọc theo thời gian (Q4).
- Mỗi phía gom về một dòng cho mỗi match key trước khi full outer join (không many-to-many); khóa có nhiều bản ghi độc lập ở một phía là `DUPLICATE_OR_CONFLICT`.
- Cutoff = 00:00 ngày kế tiếp theo `business_timezone` + `allowed_lateness_seconds`; `as_of_time >= cutoff` là quá hạn.
- `discrepancy_age_seconds` và `anchor_event_time` chỉ cho khóa khác `MATCHED`; tuổi âm bị kẹp về 0 và được đếm (`quality_counters`).
- `first_seen_at`, `resolved_at` để NULL (P1, T61). `last_evaluated_at` = `as_of_time` (Q9).
- `key_conflicts`: `conflict_code` là `EVENT_ID_CONFLICT`/`BUSINESS_KEY_CONFLICT` thì khóa là `DUPLICATE_OR_CONFLICT`; mã khác là bản ghi bị quarantine còn khôi phục được key, chỉ thêm mã `COUNTERPART_QUARANTINED` (Q8, mã đối soát bổ sung, không thuộc mã lỗi hợp đồng II.8).

## Chưa làm / ghi nhận

- Khóa chỉ có trong `key_conflicts` (không còn bản ghi Silver nào) vẫn là `DUPLICATE_OR_CONFLICT` nhưng `anchor_event_time` và tuổi để NULL vì `key_conflicts` không có `event_time`.
- `quarantine` ở dòng `key_conflicts` đọc theo ADR-0004b mục 3.5 (gộp chung bảng); ADR-0006 Q8 nói "đầu vào tùy chọn" riêng. Cần T30–T32 xác nhận khi `key_conflicts` thật có.
- Ghi mart, history, `first_seen_at`/`resolved_at`: ngoài phạm vi T33.
