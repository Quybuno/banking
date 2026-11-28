# ADR-0014: Cổng chất lượng trước publish và đối chiếu control report

- Trạng thái: Proposed
- Người viết: agent giám sát (Claude), task T32
- Người duyệt: chủ repo
- Ngày: 2026-11-28
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 6 ("Kiểm tra chất lượng và control totals"), Phần II mục 4

## Bối cảnh
Đặc tả yêu cầu: "So các tổng này với `control_report` của nguồn", "Báo cáo cân đối phải giải thích chênh lệch do replay/quarantine/filter", "Ngưỡng quality cấu hình được. Lỗi nghiêm trọng chặn publish Gold; dữ liệu lỗi có chủ đích trong demo được xử lý theo policy công khai". Đặc tả không nói: control report đặt ở đâu trên object store, thế nào là chênh lệch đã giải thích, ngưỡng bằng bao nhiêu, và chặn publish phạm vi nào. ADR-0004b Q2 chỉ quyết định vị trí `accounts.jsonl`.

## Quyết định
1. **Vị trí.** `n06 ingest-statements` tải `control_report.json` lên `<statements_prefix>/<dataset_id>/control_report.json`, không bị chặn theo `as_of_time`. Trước khi tải bất kỳ object nào, lệnh kiểm `sha256` với source manifest, schema, quy tắc dòng và `dataset_id`.
2. **Đếm theo `business_date`** cho `transaction`, `ledger_entry`, `statement`: `physical = unique_business + replay + rejected`. Bản ghi vật lý bị loại khi khóa của nó không có trong Silver hoặc chính dòng đó vào quarantine. Record không đọc được (`parse_error`) tính vào ngày ingestion (ADR-0004b Q1).
3. **Chênh lệch đã giải thích.** Với một nhóm `(entity, business_date, currency, status_or_side)`: Silver thiếu so với nguồn, số khóa thiếu không vượt số khóa bị loại của đúng nhóm đó, và số tiền thiếu nằm trong khoảng nhỏ nhất đến lớn nhất mà ngần ấy khóa bị loại tạo ra được. Silver thừa, hoặc đủ số bản ghi nhưng lệch tiền, là không giải thích được. Khoảng cần cho khóa xung đột, vì pipeline không biết biến thể nào là bản nguồn đã công bố.
4. **Chặn publish** khi một trong ba điều xảy ra: bản ghi vật lý bị loại vượt `quality.max_quarantine_rate_bps`; số dòng `key_conflicts` vượt `quality.max_conflict_count`; có chênh lệch không giải thích được và `quality.fail_on_unexplained_control_mismatch = true`. Thiếu control report với `quality.require_control_report = true` thì job dừng trước khi tạo run.
5. **Phạm vi chặn là cả run**: không entity nào được trỏ `current_snapshot` sang run bị chặn (tránh snapshot lẫn nhiều run, đặc tả I.8.1). Output vẫn nằm dưới prefix `run_id`, `quality_metrics` được ghi trước khi chặn, run chuyển `FAILED`, job thoát mã khác 0 với mã `QUALITY_GATE_BLOCKED`.
6. **Ngưỡng.** Mặc định (`config/default.toml`): 0 bps, 0 xung đột. Policy demo công khai (`config/demo.toml`): 100 bps (1 %), 50 xung đột; kiểm tra control report vẫn bật.
7. **Tên metric** trong `quality_metrics` (khóa unique của ADR-0003): metric theo nhóm có hậu tố `.<currency>.<status_or_side>`, ví dụ `record_count.VND.SUCCESS`, `control_amount_diff.VND.DEBIT`. `layer` nhận `bronze`, `silver`, `quarantine`, `control`, `run`; metric `layer='run'` gắn `business_date` nhỏ nhất của run.

## Phương án đã cân nhắc
- Chỉ chặn Gold, vẫn commit Silver và quarantine: làm `current_snapshot` lẫn hai run.
- Ngưỡng tỷ lệ kiểu float: mục cấu hình hiện chỉ có str, int, bool; dùng bps số nguyên.
- Đòi chênh lệch tiền khớp tuyệt đối với bản ghi quarantine: không làm được cho khóa xung đột.

## Hệ quả
- Chạy negative suite trên dataset nhỏ sẽ bị chặn theo policy mặc định; muốn publish phải nâng ngưỡng bằng cấu hình.
- T43 đọc `quality_metrics` để hiển thị; T50 kiểm khớp control report trên fixture acceptance qua pipeline.
