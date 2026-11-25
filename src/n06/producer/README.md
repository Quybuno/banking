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
- Producer idempotent chỉ khử trùng trong một phiên. Idempotence có hiệu lực thật trên broker được kiểm bằng thống kê `eos` của librdkafka (broker cấp Producer ID; producer đối chứng không có) ở `tests/integration/test_publish_kafka.py::test_real_producer_is_idempotent_on_the_broker_and_control_is_not`. Việc retry giữ nguyên byte được bảo đảm theo cấu hình và cấu trúc, chưa kiểm chứng bằng chèn lỗi mạng (ADR-0010 mục 8).
- Mã thoát: 0 thành công; 10 cấu hình hoặc dữ liệu nguồn sai (phát hiện trước khi gửi, hoặc file nguồn không đọc được giữa lúc đang gửi, khi đó thông báo nêu số message đã giao); 14 không kết nối được broker; 15 lỗi giao nhận.
- Không đọc thư mục ground truth và không import oracle; chỉ đọc manifests/ và events/ của nguồn.
- Thời gian chờ và giới hạn nằm trong `config/default.toml` mục `[producer]`: `flush_timeout_seconds`, `max_message_bytes`, `metadata_timeout_seconds`, `leader_wait_seconds` (ghi đè bằng `N06__PRODUCER__...`); `--flush-timeout-seconds` (phải > 0 và hữu hạn) thắng cấu hình. Hằng số mặc định trong `kafka_client.py` chỉ là mặc định của thư viện và được test chống lệch với cấu hình (`tests/unit/publish/test_review_fixes.py`). Tên thư mục con `source`, `manifests`, `events` lấy mặc định từ `DEFAULT_LAYOUT` của generator (có test chống lệch).
- Toàn bộ nguồn được kiểm trong một lượt đọc trước khi gửi message đầu tiên: sha256 và số dòng khớp manifest, mọi dòng có LF cuối, không trống, phân tích được thành một object JSON (producer **không** kiểm theo schema envelope: BOM, CRLF, khóa trùng, `NaN`, `transaction_id` không phải UUID vẫn được gửi nguyên byte, việc kiểm đó thuộc Bronze), đủ `payload.transaction_id` (và `account_from` khi dùng key đó), `event_type` hợp lệ, không có orphan ledger, kích thước key + value không vượt `message.max.bytes` trừ biên, đường dẫn trong manifest nằm trong thư mục nguồn. Lỗi dữ liệu nguồn (gồm JSON hỏng, số nguyên vượt giới hạn của Python, JSON lồng quá sâu, key không mã hóa được UTF-8) thoát mã 10 mà topic chưa nhận message nào. File không được kiểm lại sha256 lúc gửi: file bị đổi giữa lượt kiểm và lượt gửi chỉ bị phát hiện qua `delivered != expected` sau khi đã gửi.
- Lỗi producer từ chối đồng bộ (ví dụ `MSG_SIZE_TOO_LARGE`) và lỗi giao nhận thoát mã 15; `retries < 1` hoặc cấu hình producer bị librdkafka từ chối thoát mã 10; `replication_factor` hoặc số partition cụm broker từ chối thoát mã 10; lỗi metadata tạm thời được thử lại có hạn rồi thoát 14.

## Giả định về bố cục nguồn (quyết định T20)

1. **File events được chọn theo tiền tố thư mục.** Manifest không phân loại file nên `source.py` chỉ nhận
   mục có đường dẫn bắt đầu bằng `<events-dir>/` (tham số `--events-dir`, mặc định `events`); đường dẫn
   file vẫn lấy nguyên từ manifest. `accounts.jsonl`, `control_report.json` và sao kê không được phát.
   Tiền tố thư mục manifest và nguồn là `--manifests-dir` và `--source-dir`. Mặc định được test chống lệch
   với `DEFAULT_LAYOUT` của generator T13.
2. **Thứ tự phát theo số shard (sắp tự nhiên).** File được sắp theo khóa tự nhiên của đường dẫn (các dãy chữ số so sánh như số), nên `events_batch_99999` đứng trước `events_batch_100000` và tên không đệm 0 vẫn đúng thứ tự (trước đây sắp theo chuỗi sẽ sai im lặng từ shard 100000). Hai tên chỉ khác số 0 đầu (`events_batch_01` và `events_batch_1`) coi là cùng số shard và được phá hòa theo chuỗi đường dẫn; tên của T13 (5 chữ số cố định) không rơi vào ca này. Các ca biên được kiểm trong `tests/unit/publish/test_source_shard_order.py`.

## Chạy test và đọc kết quả

- **Test integration cần `.env`.** `tests/integration/test_publish_kafka.py` nạp `.env` ở gốc repo vì cấu hình T01 bắt buộc secret của MinIO và Postgres ngay cả khi chỉ publish (`n06 init-local` sinh file này). Không có `.env` thì lệnh thoát 10 và test fail. Test cũng cần Kafka chạy (`docker compose --profile core up -d`) và fail, không skip, nếu thiếu.
- **E6: partitioner mặc định của librdkafka là `consistent_random` (CRC32), khác murmur2 của client Java.** Phân bố key theo partition do đó khác một client Java dùng cùng key; phải nêu rõ khi báo cáo E6.
- **`RateLimiter` bám lịch tuyệt đối nên bù bằng cách bắn dồn:** sau khi bị trễ, các message đến hạn được gửi liên tiếp, tốc độ trung bình đúng nhưng tốc độ tức thời có thể vượt mục tiêu (`test_rate.py::test_limiter_catches_up_after_slow_send`).
- **Độ trễ p50/p95/p99 là độ trễ giao nhận của producer**, không phải độ trễ end-to-end của pipeline. Khi giới hạn tốc độ làm producer chờ, vòng chờ gọi `poll` theo lát 5 ms để callback giao nhận chạy đúng lúc broker ack (trước đây chờ bằng `sleep` làm độ trễ cộng thêm khoảng 1/rate; mô phỏng ở rate 20/s cho p50 50,1 ms, nay 1,2 ms).

## Tồn đọng

- Kiểm topic rỗng dùng tổng high watermark (ADR-0010 mục 6), không phải high trừ low: topic có message đã bị retention xóa vẫn bị coi là không rỗng (hướng sai an toàn). Hai tiến trình `n06 publish` cùng chạy trên một topic mới có thể đua nhau; chưa có khóa.
- Ràng buộc "transaction đứng trước ledger trong cùng file" của chiến lược `account_from` sẽ chặn cứng nếu T13 đổi thứ tự trong shard.
- Chưa so `dataset_id` giữa các manifest và `source_batch_id` của envelope với manifest.
- Chưa kiểm chứng retry của producer bằng chèn lỗi mạng trong test tự động (xem ADR-0010 mục 8); đã kiểm tay một lần bằng `docker pause`.
- Một lượt đọc kiểm nguồn rồi một lượt đọc để gửi: dữ liệu bị parse JSON hai lần. Chấp nhận được ở quy mô demo, chưa đo ở quy mô lớn.
