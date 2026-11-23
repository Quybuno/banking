# src/n06/generator/

**Mục đích:** canonical_json, draw(), UUIDv5, generator chunked, fault injection, statement simulator, manifest, control report, ground truth.

**Người sở hữu:** A (Mạnh)

**Đặc tả liên quan (docs/spec.md):** Phần II mục 4, 5

**Quy tắc:** Ground truth ghi ra vị trí tách khỏi prefix operational. Statement simulator không đọc Raw/Bronze/Silver/Gold.

## Module

| File | Vai trò | Task |
|---|---|---|
| `determinism/` | `canonical_json`, `draw()`, UUIDv5, `external_reference` | T11 |
| `scenario.py`, `external_sort.py` | Kịch bản gốc (accounts, transactions, ledger) và external sort `(event_time, ordinal)` | T12 |
| `params.py` | `DatasetParams` (định danh, delay, cửa sổ), `FaultConfigError`, `check_statement_delays` (normal delay phải giữ statement trước cutoff, mục 5.1) | T13 |
| `layout.py` | `OutputLayout` / `DEFAULT_LAYOUT`: nơi duy nhất định nghĩa tên thư mục và file đầu ra; `build_dataset(..., layout=...)` | T13 |
| `canonical.py` | Đọc lại kịch bản gốc kèm ordinal, đối chiếu `transaction_id` | T13 |
| `fault.py` | Số lỗi `floor(S * bp / 10000)`, xếp hạng SHA-256, cắt đoạn theo thứ tự cố định (mục 5.3) | T13 |
| `events.py` | Envelope Kafka, replay, `missing_internal` | T13 |
| `statements.py` | Dòng sao kê, biến đổi lỗi, mốc D (00:00 `Asia/Ho_Chi_Minh` của ngày sau), cửa sổ file | T13 |
| `manifest.py`, `control_report.py` | Manifest từng source batch; `control_report.json` (replay tính một lần) | T13 |
| `ground_truth.py` | Ghi ground truth tách khỏi `source/` | T13 |
| `dataset.py` | `build_dataset(config, out_root)`: ghép các bước trên | T13 |

Đầu ra mặc định: `<out_root>/source/` (pipeline được đọc) và `<out_root>/ground_truth/` (chỉ test harness).
`ground_truth/canonical/` do T12 ghi trực tiếp, không sao chép.

## Quy ước

- `fault_assignment.jsonl` chỉ ghi giao dịch bị tiêm lỗi; vắng mặt nghĩa là không lỗi (ADR-0009, quyết định 4).
- Tên part file trong `ground_truth/canonical/` (do T12 đặt) phụ thuộc `generation_chunk_rows`;
  `canonical_index.json` hash theo luồng nối nên không đổi.

## Tồn đọng đã biết

- Bộ nhớ của bộ chia file sao kê (`_StatementFileSplitter._pending` trong `dataset.py`) tỷ lệ với
  số dòng của một cửa sổ `statement_file_window_seconds`, chưa bị chặn bởi `generation_chunk_rows`.
  Cửa sổ lớn ở quy mô lớn có thể giữ hàng trăm nghìn tới hàng triệu dòng trong RAM.
- Chưa chạy ở quy mô 5 triệu giao dịch; lần chạy lớn nhất là 12.000 giao dịch.
