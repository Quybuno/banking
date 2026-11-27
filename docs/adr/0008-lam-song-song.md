# ADR-0008: Cách làm song song giữa các milestone

- Trạng thái: Accepted
- Đã duyệt (Accepted): chủ repo (giám sát được chủ repo giao toàn quyền), 2026-11-27
- Người viết: cả nhóm
- Người duyệt: cả nhóm
- Mục đặc tả liên quan: Phần I mục 12 ("Không bắt đầu milestone sau khi cổng của milestone trước chưa đạt")

## Bối cảnh
Áp dụng nguyên văn quy tắc trên thì ba thành viên phải làm tuần tự và chờ nhau.

## Quyết định
1. Cổng milestone áp dụng cho việc merge vào `main`.
2. Được chuẩn bị task của milestone sau trên nhánh riêng nếu chỉ dùng fixture cố định trong `contracts/` thay cho đầu ra của task chưa xong.
3. PR của milestone sau chỉ merge khi cổng milestone trước đã đạt trên `main`.

## Ngoại lệ đã chấp nhận

T20, T21, T24 (M2) đã merge vào `main` trước khi cổng M1 đạt. Chủ repo chấp nhận ngoại lệ này ngày 2026-11-27. Từ ngày này các PR tiếp theo theo đúng mục 3 ở trên.

Lịch sử: Proposed từ đầu dự án; Accepted 2026-11-27.

## Hệ quả
Lịch song song theo `docs/TASKS.md` mục 5. Nếu đầu ra thật khác giả định trên fixture, task chuẩn bị trước phải sửa lại trước khi merge.
