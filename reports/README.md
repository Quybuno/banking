# reports/

**Mục đích:** Kết quả benchmark máy đọc được (CSV/JSON), biểu đồ, test report.

**Người sở hữu:** C (Quý)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 14.3

**Quy tắc:** Chỉ ghi số liệu do lệnh `n06 benchmark` sinh ra, kèm commit SHA, seed, config hash. Không commit dữ liệu thô lớn.

## `reports/reviews/`

Theo chỉ thị của chủ dự án ngày 2026-11-25: mọi báo cáo của reviewer được lưu **nguyên văn** vào `reports/reviews/<ID>-review-<n>.md` (phần đầu file do người điều phối thêm: agent, thời điểm, SHA-256 của phần nguyên văn). Đây là ngoại lệ của quy tắc "chỉ `n06 benchmark`" ở trên và ở `CLAUDE.md`; dòng tương ứng trong `CLAUDE.md` chưa được sửa. Các file này không phải số liệu đo của dự án và không được trích làm kết quả benchmark.

