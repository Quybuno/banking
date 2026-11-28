# src/n06/quality/

**Mục đích:** Quarantine, dedup, conflict, key_conflicts, quality metrics, control totals.

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 6; Phần II mục 8; ADR-0004b

**Quy tắc:** Không filter bỏ bản ghi lỗi im lặng.

## Nội dung hiện có (T30)

| File | Việc |
|---|---|
| `quarantine.py` | Chuẩn hóa `QuarantineRecord` (11 cột ADR-0004b §3.6), phân loại message Kafka thô, dòng sao kê, dòng tài khoản và nhóm sổ cái bằng validator T10 theo thứ tự ưu tiên 17 mã lỗi Phần II mục 8. |
| `spark_quarantine.py` | Hàm Spark SQL thuần (không Python UDF) chuyển Bronze `parse_error`, kiểm tra nghiệp vụ Silver `transactions`/`ledger_entries`/nhóm sổ cái và tách phân vùng theo `business_date` (fallback `ingestion_date` với `business_date = NULL` theo ADR-0004b Q1). |
