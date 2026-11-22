# src/n06/storage/

**Mục đích:** Object store, manifest, giao thức commit batch (run_id, current_snapshot).

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 8.1

**Quy tắc:** Reader chỉ đọc snapshot do current_snapshot trỏ tới.
