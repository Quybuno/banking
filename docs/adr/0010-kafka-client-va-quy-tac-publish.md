# ADR-0010: Thư viện Kafka client và quy tắc của `n06 publish` (T20)

- Trạng thái: Accepted
- Người viết: agent (task T20), duyệt bởi <TÊN NGƯỜI DUYỆT>, 2026-11-23
- Người duyệt:
- Ngày: 2026-11-23
- Mục đặc tả liên quan (docs/spec.md): Phần II mục 4; Phần I mục 5 (dòng 255), mục 14.1 (E2, E6)

## Bối cảnh
Repo chưa có Kafka client cho Python: `pyproject.toml` có `dependencies = []`. ADR-0001 chỉ ghim `kafka-clients 3.4.1` phía JVM của Spark, không dùng được cho `n06 publish`. Cần một thư viện hỗ trợ producer idempotent (thẻ T20, điều kiện 2), tốc độ phát điều khiển được (điều kiện 4) và tạo topic với số partition cấu hình (E2).

**Đính chính nhận định ban đầu.** Kế hoạch T20 lúc đầu giả định `kafka-python` không hỗ trợ idempotent producer. Kiểm chứng ngày 2026-11-23 cho thấy giả định này **sai**. Cả hai thư viện đều hỗ trợ idempotence, nên idempotence không phân biệt được hai lựa chọn.

Nguồn kiểm chứng (đã mở và đọc ngày 2026-11-23):
- kafka-python, `CHANGES.md`: https://raw.githubusercontent.com/dpkp/kafka-python/master/CHANGES.md
  - Mục `2.2.0 (Apr 28, 2025)`: "KIP-98: Add idempotent producer support (#2569)".
  - Mục `3.0.0 (Jun 11, 2026)`: "KIP-679: Update Producer defaults -- enable_idempotence=True, acks='all' (#3013)".
  - Mục `3.0.13 (Nov 5, 2026)`: "fix: transition idempotent producer to UNINITIALIZED in `reset_producer_id` (#3182)".
- kafka-python, bản mới nhất trên PyPI: 3.0.13 (2026-11-06), thuần Python, không có wheel nhị phân: https://pypi.org/pypi/kafka-python/json
- kafka-python, tương thích broker: "tested against broker versions 4.3 through 0.8.0": https://raw.githubusercontent.com/dpkp/kafka-python/master/docs/compatibility.rst
- librdkafka, idempotent producer và các đảm bảo (exactly-once, ordering, gap-less thử nghiệm): https://raw.githubusercontent.com/confluentinc/librdkafka/master/INTRODUCTION.md (mục "Idempotent Producer"). Producer báo lỗi lúc khởi tạo nếu cấu hình không tương thích, ví dụ `acks=1` với `enable.idempotence=true`.
- librdkafka, `enable.idempotence` tự đặt `max.in.flight.requests.per.connection=5`, `retries=INT32_MAX`, `acks=all`: https://raw.githubusercontent.com/confluentinc/librdkafka/master/CONFIGURATION.md
- confluent-kafka, bản 2.16.2 (2026-11-18) có wheel cho Python 3.11: https://pypi.org/pypi/confluent-kafka/json
  - `confluent_kafka-2.16.2-cp311-cp311-manylinux_2_28_x86_64.whl`, sha256 `f4cae8bc8e5208def5cfe9ac07b85c934c07e5424011608b3bac713cbcc82c3e`.
  - `confluent_kafka-2.16.2-cp311-cp311-manylinux_2_28_aarch64.whl`, sha256 `dc47a5ce6e228f04f0e38841de50cf779e7934009d9746f4cbf7bbc06ca027fc`.
- confluent-kafka, README (lời tuyên bố của nhà cung cấp về hiệu năng, chưa phải số đo của nhóm): https://raw.githubusercontent.com/confluentinc/confluent-kafka-python/master/README.md

Broker trong Compose (T02) là `apache/kafka:3.7.2` (`docker-compose.yml:12`).

## Quyết định
**1. Thư viện: `confluent-kafka==2.16.2`** (ghim đúng, wheel nhị phân cho Python 3.11 và glibc >= 2.28; máy hiện tại là glibc 2.31, x86_64).

Hai lý do (idempotence **không** nằm trong số đó vì cả hai thư viện đều hỗ trợ):
- *Thí nghiệm E2.* E2 phát với tốc độ không giới hạn để đo thông lượng ingest. Nếu bên phát chậm hơn bên nhận thì số đo phản ánh producer chứ không phải Spark. Producer của librdkafka (viết bằng C) ít có nguy cơ làm nghẽn hơn producer thuần Python. **Đây là rủi ro cần kiểm tra ở T52, không phải kết luận:** nhà cung cấp tuyên bố thư viện C nhanh hơn, nhóm chưa đo.
- *Kiểm tra cấu hình lúc khởi tạo.* librdkafka từ chối khởi tạo producer nếu `acks` không tương thích với idempotence (ví dụ `acks=1` với `enable.idempotence=true`), khớp với kiểm tra `producer.acks == "all"` của lệnh publish.

Quan sát thêm, không dùng làm lý do: kafka-python thêm idempotent producer ở 2.2.0 (04/2025), bật mặc định từ 3.0.0 (06/2026) và có bản sửa lỗi riêng cho đường này ở 3.0.13 (11/2026). Đây là dấu hiệu đường idempotent ít thời gian kiểm nghiệm hơn, không phải bằng chứng thư viện sai.

Phương án thay thế hợp lệ: `kafka-python==3.0.13` (thuần Python, không phụ thuộc nhị phân, cũng có idempotence và `acks=all` mặc định). Hai thư viện có API gửi message khác nhau, nên đổi thư viện sau khi T20 xong tốn công hơn là chỉ đổi một dòng dependency.

Kiểm chứng phiên bản ngày 2026-11-23: wheel `confluent_kafka-2.16.2-cp311-cp311-manylinux_2_28_x86_64.whl` tải được từ PyPI, sha256 tính trên máy khớp giá trị ở trên, và chứa `confluent_kafka/cimpl.cpython-311-x86_64-linux-gnu.so`. Danh sách PyPI Simple có `2.16.0`, `2.16.1` và `2.16.2` cho cp311 manylinux x86_64. Trang changelog của Confluent không liệt kê `2.16.2`, nhưng PyPI là nguồn phát hành của gói.

**2. Tương thích.** `librdkafka` hỗ trợ mọi broker >= 0.8 (README confluent-kafka), nên không xung đột với Kafka 3.7.2. Tôi chưa tìm thấy dòng tài liệu riêng nêu phiên bản broker tối thiểu cho idempotence; việc này được xác nhận bằng test tích hợp trên broker của Compose. Với ADR-0001: `kafka-clients 3.4.1` là JAR của Spark, hoàn toàn tách khỏi client Python. Dependency mới chỉ thuộc môi trường `uv sync`; `scripts/smoke.sh` được chạy lại sau khi sửa lockfile để chứng minh không phá bộ đã ghim.

**3. Một topic cho mọi event**, dùng `event_type` trong envelope để phân biệt, tên topic lấy từ `kafka.events_topic`. Căn cứ spec Phần I mục 5 (dòng 255): "Topic theo loại event hoặc một topic với `event_type`: agent chọn một cách và ghi ADR", và key mặc định `transaction_id` có mục đích liên kết giao dịch/bút toán "trong cùng topic".

**4. Key.** Căn cứ Phần II mục 4: "Key mặc định là transaction_id (cả transaction event và ledger event). … giá trị hỗ trợ là `transaction_id` và `account_from`; `account_from` chỉ dùng cho thí nghiệm E6. Đổi key không làm đổi envelope hay payload."
- `transaction_id` (mặc định): key là `payload.transaction_id` cho cả hai loại event.
- `account_from`: transaction event dùng `payload.account_from`. Ledger event không có trường này nên dùng `account_from` của transaction tương ứng. Spec im lặng ở điểm này; đây là quyết định của người duyệt trong phiên lập kế hoạch T20.
  - Giữ map `transaction_id -> account_from` trong bộ nhớ, xóa toàn bộ khi chuyển sang file events của shard mới (ledger luôn cùng shard với transaction).
  - Ledger không tìm thấy transaction trong shard hiện tại thì dừng với mã 10 (`CONFIG_ERROR`) và thông báo ghi rõ đây là **dữ liệu nguồn không nhất quán, không phải lỗi cấu hình**, kèm `event_id`, `transaction_id`, tên file và shard.
- Value luôn là đúng byte của dòng trong file (bỏ LF cuối), không parse rồi serialize lại. Nhờ đó đổi key không thể đổi value.

**5. Thứ tự phát.** Lần lượt từng shard theo tên file trong manifest, trong file theo thứ tự dòng do T13 ghi; tất định. Căn cứ spec II.4: "Thứ tự Kafka chỉ đảm bảo trong một partition, không suy luận ledger đã đủ từ một message." Pipeline downstream không được dựa vào việc ledger đến sau transaction.

**6. Chạy lại.** Producer idempotent chỉ khử trùng trong một phiên producer, không giữa hai lần chạy lệnh. Mặc định lệnh từ chối publish vào topic không rỗng (tổng end offset của mọi partition > 0) với mã 10; thông báo nêu tên topic và số message hiện có. Chỉ khi có cờ `--allow-non-empty-topic` mới cho phép phát tiếp (phục vụ kiểm thử phát lại); thống kê in số message đã có trước khi gửi. Replay do T13 tiêm là message riêng, không bị loại ở bước publish.

**7. Mã thoát.** Thêm `KAFKA_DELIVERY_ERROR = 15` vào `ExitCode` (`src/n06/cli/doctor.py`) cho lỗi giao nhận không retry được hoặc message chưa giao sau `flush`. Mã 14 (`SERVICE_ERROR`) vẫn dành cho không kết nối được dịch vụ. Chỉ sửa đúng một dòng ở `doctor.py` và một dòng epilog ở `src/n06/cli/__init__.py`.

**8. Giới hạn của điều kiện 2 (retry giữ nguyên byte).** Điều kiện này được bảo đảm theo cấu hình và cấu trúc: `enable.idempotence=true`, `acks=all`, và value là cùng một đối tượng byte đọc từ file, không serialize lại. **Chưa kiểm chứng bằng chèn lỗi mạng** (không có proxy chèn lỗi trong môi trường). Test tích hợp chỉ xác nhận số message và byte trên broker thật.

## Phương án đã cân nhắc
- `kafka-python==3.0.13`: xem mục 1. Hợp lệ, không chọn vì độ chín của đường idempotent và thông lượng chưa được đo (xem lý do ở trên).
- `kafka-console-producer.sh` trong container: không điều khiển được tốc độ và không có callback giao nhận từng message, không đáp ứng điều kiện 4.
- Một topic cho mỗi loại event: tách `transaction_id` giữa hai topic làm mất ý nghĩa "liên kết trong cùng topic" của key mặc định.

## Hệ quả
- Sửa `pyproject.toml` (đúng một dòng dependency) và `uv.lock` (chỉ phần thêm của gói này). Chỉ làm sau khi ADR này Accepted.
- Thêm một dòng vào `docs/compatibility-matrix.md`.
- T20: `src/n06/producer/`, `src/n06/cli/publish.py`, đăng ký subcommand tối thiểu ở `cli/__init__.py`. T22/T23 và downstream không được giả định thứ tự ledger sau transaction.
- Nếu người duyệt chọn `kafka-python`: đổi dòng dependency và phiên bản trong ADR này; phần quyết định 3-8 không đổi.
