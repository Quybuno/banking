# src/n06/producer/

**Mục đích:** Lệnh `publish`: producer Kafka, tốc độ phát, chiến lược key.

**Người sở hữu:** A (Mạnh)

**Đặc tả liên quan (docs/spec.md):** Phần II mục 4; Phần I mục 14 (E2, E6)

**Quy tắc:** Key mặc định `transaction_id`. Đổi key không đổi value.

## Module (TASK T20, ADR-0010)

| File | Vai trò |
|---|---|
| `source.py` | Đọc manifest, chọn file events, kiểm sha256 và số dòng trước khi gửi |
| `keys.py` | Chọn key (`transaction_id`, `account_from`); map xóa khi sang file mới |
| `rate.py` | Điều khiển tốc độ phát theo lịch tuyệt đối (`0` là không giới hạn) |
| `kafka_client.py` | `confluent-kafka`: cấu hình producer idempotent, admin, đọc watermark |
| `publisher.py` | Vòng gửi, quy tắc topic không rỗng, thống kê (độ trễ p50/p95/p99) |
| `errors.py` | Lỗi publish và ánh xạ mã thoát |

Lệnh CLI nằm ở `src/n06/cli/publish.py`: `n06 publish [--source-dir DIR] [--topic T] [--rate N] [--key-strategy {transaction_id,account_from}] [--allow-non-empty-topic] [--json]`.

## Quy tắc

- Value là đúng byte của dòng trong file; không parse rồi serialize lại.
- Thứ tự phát: lần lượt từng file events theo tên, trong file theo thứ tự dòng. Downstream không được dựa vào việc ledger đến sau transaction (Phần II mục 4: thứ tự chỉ đảm bảo trong một partition).
- Mặc định từ chối publish vào topic đã có message; chỉ `--allow-non-empty-topic` mới cho phép.
- Producer idempotent chỉ khử trùng trong một phiên. Việc retry giữ nguyên byte được bảo đảm theo cấu hình và cấu trúc, chưa kiểm chứng bằng chèn lỗi mạng (ADR-0010 mục 8).
- Mã thoát: 0 thành công; 10 cấu hình hoặc dữ liệu nguồn sai; 14 không kết nối được broker; 15 lỗi giao nhận.
- Không đọc thư mục ground truth và không import oracle; chỉ đọc manifests/ và events/ của nguồn.
- `config/` chưa có khóa riêng cho flush timeout và tên thư mục con; mặc định nằm trong `source.py` và `cli/publish.py`, ghi đè bằng tham số CLI.
