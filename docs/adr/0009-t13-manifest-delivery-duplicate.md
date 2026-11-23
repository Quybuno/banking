# ADR-0009: Manifest ordinal 0, bản trùng sao kê và thứ tự lịch giao (T13)

- Trạng thái: Proposed
- Người viết: agent (task T13), chờ người duyệt chuyển sang Accepted
- Người duyệt:
- Ngày: 2026-11-23
- Mục đặc tả liên quan (docs/spec.md): Phần II mục 2, mục 4, mục 5.2, mục 5.3

## Bối cảnh
Ba chỗ đặc tả im lặng, được người duyệt chốt trong phiên lập kế hoạch T13.

**1. File nào nằm trong manifest của source batch.**
- Phần II mục 4: "Mỗi source batch có một manifest đúng `source_manifest.schema.json`: dataset_id, source_batch_id và danh sách file với path, sha256, physical_rows."
- Phần II mục 5.2: "source_batch dùng ordinal `floor(transaction_ordinal/10000)` và discriminator `original`" và "Mỗi file có một manifest riêng với source_batch_id = UUIDv5 của entity `source_batch`, ordinal w, discriminator `statement:<channel>`" (sao kê).
- Phần đầu v1.2, thay đổi 4: fixture `control_report.json` "nằm trong source manifest". Fixture `source_manifest.json` liệt kê cả `accounts.jsonl` và `control_report.json`.
- Spec không nói `accounts.jsonl` và `control_report.json` thuộc batch nào.

**2. Bản `statement_duplicate`.**
- Phần II mục 5.3: "statement_duplicate | Thêm statement có ID khác, cùng reference và nội dung đối chiếu."
- Mục 5.2: "Mỗi cặp (w, channel) có đúng một file". Spec không nói `available_at` của bản trùng.

**3. Thứ tự `delivery_order`.**
- Mục 5.2: "Generator xuất lịch giao file đúng `statement_delivery.schema.json`." Schema chỉ yêu cầu `delivery_order` là số nguyên >= 0; fixture đánh số theo thứ tự file.

## Quyết định
1. Manifest của source batch ordinal 0, discriminator `original`, liệt kê (sắp theo path) `accounts.jsonl`, `control_report.json` và file events của shard 0. Không tạo discriminator mới. Manifest các shard khác chỉ liệt kê file events của shard đó. `ledger_entries.jsonl` (tập tham chiếu kiểm toán) và `statement_deliveries.jsonl` (lịch điều phối) không nằm trong manifest. Vì `control_report.json` chỉ tính xong sau khi đi hết các shard, manifest ordinal 0 được ghi cuối cùng.
2. Bản trùng dùng discriminator `duplicate`, cùng `available_at` và cùng file với bản gốc.
3. `delivery_order` xếp theo `(file_available_at, channel)` tăng dần, đánh số từ 0, mỗi file một số; các dòng cùng file cùng `delivery_order`.

## Phương án đã cân nhắc
- Manifest riêng cho accounts/control_report: cần discriminator mới, trái "không tạo discriminator mới".
- Bản trùng ở cửa sổ khác: làm "truyền lại cùng một dòng" và "nhiều statement_record_id" khó phân biệt trong một file.
- `delivery_order` theo từng dòng: fixture dùng một số cho cả file.

## Hệ quả
- T13: `manifest.py`, `dataset.py`. T20/T23 đọc manifest theo thứ tự shard và file giao theo `delivery_order`.
- T14/T33: nhãn `kind` (original/duplicate) chỉ có trong ground truth.
