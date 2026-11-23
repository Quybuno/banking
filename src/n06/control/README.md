# src/n06/control/

**Mục đích:** Quản lý migration và truy cập Control DB (`n06_control`) theo ADR-0003.

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 8.1

**Quy tắc:**
- Lấy cấu hình kết nối từ `load_config().postgres.control`, không hardcode endpoint/credential.
- Không lưu secret trong migration hoặc log.
