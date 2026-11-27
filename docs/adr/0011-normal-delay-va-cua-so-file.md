# ADR-0011: Ràng buộc normal delay phải tính cả cửa sổ file sao kê (T13 follow-up)

- Trạng thái: Accepted
- Người viết: agent (task T13 follow-up), chờ người duyệt chuyển sang Accepted
- Người duyệt: chủ repo
- Đã duyệt (Accepted): chủ repo (giám sát được chủ repo giao toàn quyền), 2026-11-27
- Ngày: 2026-11-23
- Mục đặc tả liên quan (docs/spec.md): Phần II mục 5.1 (dòng 761), mục 5.3 (dòng 787), mục 1.10

## Bối cảnh
Hai câu của đặc tả có thể đọc ra hai ràng buộc khác nhau cho normal delay.

Dòng 761 (mục 5.1):
> "Kiểm tra ngày, timezone, tỷ lệ, normal delay, late delay ngay lúc startup; late-within delay + statement_file_window_seconds < allowed_lateness; late-after delay >= allowed_lateness. Normal delay phải giữ statement trước cutoff."

Dòng 787 (mục 5.3):
> "Xuất ground truth gồm fault assignment của từng giao dịch, `available_at` và `file_available_at` của từng dòng sao kê, và số lượng theo loại lỗi. Ground truth **không** lưu sẵn trạng thái đối soát: test harness tính trạng thái kỳ vọng cho một `as_of_time` cho trước theo đúng bảng ở Phần I, mục 7.2, coi một dòng sao kê là đã có khi `file_available_at <= as_of_time`. Nếu dataset quá nhỏ khiến count = 0, báo rõ; nghiệm thu dùng fixture cố định thay vì tin demo random sẽ bao phủ đủ lỗi."

- Đọc sát dòng 761, "giữ statement trước cutoff" có thể hiểu là `available_at` của statement bình thường phải < cutoff.
- Dòng 787 nói oracle coi một dòng sao kê là đã có khi `file_available_at <= as_of_time`, và `file_available_at = (w + 1) × W` lớn hơn `available_at` tối đa một cửa sổ W (mục 5.2). Ràng buộc late_within ở dòng 761 cũng đã cộng cửa sổ: `late-within delay + statement_file_window_seconds < allowed_lateness`.
- Khoảng hở được review độc lập T13 tái hiện bằng lệnh chạy: `normal_statement_delay_seconds=86000`, `statement_file_window_seconds=5000`, `allowed_lateness_seconds=86400`, `late_within=43200` qua được validator T10 và cả kiểm tra normal delay cũ (chỉ so `available_at`), nhưng build 1500 giao dịch cho 41 dòng sao kê không lỗi có `file_available_at` sau cutoff (ví dụ `available_at 2026-01-02T16:12:57Z`, `file_available_at 2026-01-02T17:30:00Z`, cutoff `2026-01-02T17:00:00Z`). Oracle ở `as_of_time` trong khoảng đó sẽ báo `MISSING_EXTERNAL` sai cho giao dịch không có lỗi.

## Quyết định
Phương án (a): statement bình thường phải có **`file_available_at` trước cutoff**, kiểm lúc startup bằng bất đẳng thức bảo toàn:

`normal_statement_delay_seconds + statement_file_window_seconds <= allowed_lateness_seconds`

Cách suy ra: giao dịch muộn nhất của ngày có `event_time = D - 1` giây; `file_available_at <= available_at + W`; cần `D - 1 + normal_delay + W < D + allowed_lateness` (đúng cutoff đã quá hạn, mục 1.10). Vi phạm thì `build_dataset` ném `FaultConfigError` nêu `normal_statement_delay_seconds`, `statement_file_window_seconds` và cutoff, trước khi tạo thư mục đầu ra. Cấu hình mặc định (60, 3600, 86400) hợp lệ. Ca biên: `normal + W == allowed` được chấp nhận, `normal + W == allowed + 1` bị từ chối.

## Phương án đã cân nhắc và loại
- **(b) Chỉ kiểm `available_at`** (`normal_delay <= allowed_lateness`, như bản đầu của T13): đúng sát chữ của dòng 761 nhưng cho phép cấu hình tái hiện được ở trên, tạo `MISSING_EXTERNAL` sai ở oracle. Loại vì làm oracle sai khi ground truth không có lỗi.
- **(c) Tính chính xác `file_available_at` theo từng `business_date`** (so `(floor((D - 1 + normal_delay) / W) + 1) × W` với cutoff từng ngày): cho phép nhiều cấu hình hơn khi D thẳng hàng với cửa sổ, nhưng kết quả phụ thuộc `W` có chia hết cho 86400 hay không và thay đổi theo ngày, khó giải thích và khó kiểm. Loại vì phức tạp không cần thiết; (a) chỉ chặt hơn (c) trong các ca D thẳng hàng với cửa sổ.

## Hệ quả
- Sửa `check_statement_delays` trong `src/n06/generator/params.py`.
- Đổi expected của ba test của T13 (tên giữ nguyên): `test_check_matches_independent_cutoff_arithmetic`, `test_boundary_cases_are_exactly_at_and_just_below_cutoff`, `test_build_accepts_delay_just_below_cutoff`; thêm `test_config_normal_delay_file_window.py`.
- Cấu hình có `normal_delay` sát `allowed_lateness` mà trước đây hợp lệ (ví dụ 86400 với W=3600) nay bị từ chối. Không có cấu hình mặc định hay profile trong `contracts/config/` bị ảnh hưởng.
