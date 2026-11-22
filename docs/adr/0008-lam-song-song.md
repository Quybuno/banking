# ADR-0008: Cách làm song song giữa các milestone

- Trạng thái: Proposed
- Người viết: cả nhóm
- Người duyệt: cả nhóm
- Mục đặc tả liên quan: Phần I mục 12 ("Không bắt đầu milestone sau khi cổng của milestone trước chưa đạt")

## Bối cảnh
Áp dụng nguyên văn quy tắc trên thì ba thành viên phải làm tuần tự và chờ nhau.

## Quyết định
1. Cổng milestone áp dụng cho việc merge vào `main`.
2. Được chuẩn bị task của milestone sau trên nhánh riêng nếu chỉ dùng fixture cố định trong `contracts/` thay cho đầu ra của task chưa xong.
3. PR của milestone sau chỉ merge khi cổng milestone trước đã đạt trên `main`.

## Hệ quả
Lịch song song theo `docs/TASKS.md` mục 5. Nếu đầu ra thật khác giả định trên fixture, task chuẩn bị trước phải sửa lại trước khi merge.
