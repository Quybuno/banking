# src/n06/domain/

**Mục đích:** Model nghiệp vụ, validator schema Draft 2020-12, validator nghiệp vụ, mã lỗi.

**Người sở hữu:** A (Mạnh)

**Đặc tả liên quan (docs/spec.md):** Phần II mục 1, 3, 8

**Quy tắc:** Tiền là số nguyên. Không đọc file trong `tests/` hay `contracts/.../expected_results.json`.
- Bộ kiểm **không ném ngoại lệ vì dữ liệu**: byte không phải UTF-8, số nguyên quá dài, JSON lồng quá sâu cho `INVALID_JSON`; value `null` cho `SCHEMA_TYPE`; `bytes`, `bytearray`, `memoryview` được xử lý như nhau; ngày nghiệp vụ vượt miền lịch cho `BUSINESS_DATE_MISMATCH`. Lỗi lập trình (schema chứa từ khóa chưa hỗ trợ, tên schema sai) vẫn ném.
