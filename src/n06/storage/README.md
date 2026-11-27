# src/n06/storage/

**Mục đích:** Object store, manifest, giao thức commit batch (run_id, current_snapshot).

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 8.1

**Quy tắc:** Reader chỉ đọc snapshot do current_snapshot trỏ tới.

## Object store S3/MinIO (`object_store.py`, ADR-0012 quyết định 2)

- Bộ ký AWS SigV4 và client `http.client` không thêm dependency, hỗ trợ `HEAD`/`PUT` bucket (dùng lại bởi `n06.cli`) và `HEAD`/`PUT` object bất biến (dùng bởi `n06 ingest-statements`, T23).
- `PUT` object gửi thân theo luồng với `Content-Length`, lưu `sha256` vào metadata `x-amz-meta-sha256`, thử lại có giới hạn khi lỗi mạng/5xx.
- Ghi bất biến (`put_object_immutable_*`): `HEAD` trước; nếu đã có và trùng `x-amz-meta-sha256` thì bỏ qua, nếu thiếu hoặc khác `sha256` thì ném `ObjectIntegrityError` (không ghi đè).
