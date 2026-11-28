# src/n06/quality/

**Mục đích:** Quarantine, dedup, conflict, key_conflicts, quality metrics, control totals.

**Người sở hữu:** B (Quang)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 6; Phần II mục 8; ADR-0004b

**Quy tắc:** Không filter bỏ bản ghi lỗi im lặng.

## Nội dung hiện có (T30, T31, T32)

| File | Việc |
|---|---|
| `quarantine.py` | Chuẩn hóa `QuarantineRecord` (11 cột ADR-0004b §3.6), phân loại message Kafka thô, dòng sao kê, dòng tài khoản và nhóm sổ cái bằng validator T10 theo thứ tự ưu tiên 17 mã lỗi Phần II mục 8. |
| `spark_quarantine.py` | Hàm Spark SQL thuần (không Python UDF) chuyển Bronze `parse_error`, kiểm tra nghiệp vụ Silver `transactions`/`ledger_entries`/nhóm sổ cái và tách phân vùng theo `business_date` (fallback `ingestion_date` với `business_date = NULL` theo ADR-0004b Q1). |
| `conflicts.py` | Phân biệt event replay, business replay (T009), duplicate nghiệp vụ (T010, T011), và xung đột khóa (`EVENT_ID_CONFLICT`, `BUSINESS_KEY_CONFLICT`) xuất `KeyConflictRecord` (11 cột ADR-0004b §3.5). |
| `spark_conflicts.py` | Hàm Spark SQL thuần tách xung đột khóa khỏi Silver sang `quarantine.records` + `silver.key_conflicts`, ưu tiên `EVENT_ID_CONFLICT` trước `BUSINESS_KEY_CONFLICT`. |
| `metrics.py` | Tính control totals tách riêng `transaction`/`ledger_entry`, đối chiếu từng dòng với `control_report.json`, sinh `QualityMetricRecord` cho Control DB và đánh giá ngưỡng chặn publish (`QualityThresholds`). |
| `spark_metrics.py` | Tính control totals từ DataFrame Silver bằng Spark SQL thuần (số nguyên `int`, không float). |
