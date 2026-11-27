# PR #12 (T20: chờ metadata thấy topic mới): review độc lập, một vòng

Phần trên đường kẻ do người điều phối thêm; phần dưới đường kẻ là **nguyên văn** báo cáo của reviewer (subagent `spec-reviewer`), trích bằng script từ bản ghi phiên, không chỉnh sửa. Nội dung là nhận định của reviewer tại thời điểm viết, có thể chứa lỗi (ví dụ số dòng).

- Agent: `aa1ed0678f721e4e7`
- Thời điểm: 2026-11-27T07:02:37.756Z
- Lượt báo cáo: 1/1 của agent này
- SHA-256 phần nguyên văn: `c023c20cc2eae55e2ec6722bad4ee0eff73b6ed2a10bd664752fb3a9e613628c`

---

## Review PR #12 (`git diff 3cd08fb..8d3f77a`, 3 file), task T20

Kết luận: không có mục chặn merge. Bản sửa xử lý đúng lỗi đã nêu và thời gian chờ có hạn. Có 3 mục nên sửa.

### Phải sửa
Không có.

### Nên sửa

**N1. Số partition không được kiểm lại sau khi tạo topic hoặc gặp TOPIC_ALREADY_EXISTS** (`/home/quybuno/banking/src/n06/producer/publisher.py:90-98`, `/home/quybuno/banking/src/n06/producer/kafka_client.py:177-178`, `:206-214`)
- Chuỗi xảy ra: `partition_count` trả None, `create_topic` nuốt ALREADY_EXISTS, `end_offsets` chờ thấy topic rồi publish. Không chỗ nào so `count` với `settings.partitions`; `PublishStats.partitions` (`publisher.py:204`) vẫn báo số cấu hình. Số này dùng cho E2.
- Output với admin giả (topic có 1 partition, cấu hình 3): `[J exists-with-1-partition, config 3] prepare_topic -> 0 (khong bao loi)`
- Lỗ hổng có từ trước. Bản sửa làm đường này dễ đi tới đích hơn: trước đây nếu metadata còn trễ thì thoát 14, nay chờ rồi publish. `docker-compose.yml:33` bật `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"`, nên client khác cũng có thể tạo topic với số partition mặc định xen giữa. README `:55` mới ghi chung là "đua nhau; chưa có khóa".
- Cùng gốc: topic hiện ra không lỗi nhưng 0 partition thì `end_offsets` trả `{}`, tức bị coi là topic rỗng (`[E zero partitions] t=0.0s list_calls=1 -> OK {}`). Hành vi này có từ trước, và tôi chưa thấy bằng chứng broker thật trả về như vậy.
- Đề xuất: trong `prepare_topic`, lấy `offsets = broker.end_offsets(...)`, nếu `len(offsets) != settings.partitions` thì ném `PublishConfigError` (mã 10). Phải sửa theo `FakeBroker` (`tests/unit/publish/conftest.py:170-174`) vì mặc định nó trả `{}`.

**N2. Tổng thời gian chờ vượt `leader_wait_seconds`; thông báo và README chưa nói** (`kafka_client.py:189-204`, `:143-168`; `README.md:33`)
- Hạn ngoài (`:195`) bọc `partition_count`, hàm này tự đặt hạn mới bằng `leader_wait` ở mỗi lần gọi (`:146`), và mỗi `list_topics` tốn tới `metadata_timeout`.
- Đo bằng đồng hồ giả, L=20, M=10:
```
[A never, fast] t=20.2s list_calls=102 -> ... Topic t không tồn tại khi đọc offset (đã chờ 20 s).
[B never, slow 10s] t=20.2s list_calls=2 -> ... (đã chờ 20 s).
[C last call starts at deadline] t=30.0s list_calls=101 -> ... (đã chờ 20 s).
[D2 nested worst] t=49.8s list_calls=200 -> ... Không lấy được metadata của topic t (lỗi LEADER_NOT_AVAILABLE)
```
- Vậy chờ vẫn có hạn, xấu nhất cỡ 2L + M (khoảng 50 s) cho riêng bước chờ topic xuất hiện. Ca C in "đã chờ 20 s" trong khi đã qua 30 s.
- Sau bước này, mỗi partition còn có hạn watermark riêng (`:220` trở đi, có từ trước). Tôi chưa đo tổng này, cũng chưa đo phần `partition_count` và `create_topic` đứng trước.
- Đề xuất tối thiểu: README ghi trần thật, và đổi chữ thành "hạn chờ 20 s". Hoặc truyền hạn chót chung xuống `partition_count`.

**N3. Không test nào bắt việc bỏ qua cấu hình `leader_wait_seconds` ở vòng chờ mới** (`tests/unit/publish/test_kafka_client.py:168-173`)
- Đột biến thay `self._leader_wait` bằng `20.0` vẫn `129 passed`, vì test chỉ dùng giá trị mặc định.
- Đề xuất: thêm ca `leader_wait_seconds=3`, assert `3.0 <= clock.now <= 3.5`.

### Ghi nhận

**Đột biến** (trên bản sao, chạy `tests/unit/publish`):

| Đột biến | Kết quả |
|---|---|
| M1 bỏ bản sửa | bắt: `2 failed, 127 passed` (đúng 2 test mới) |
| M5 dùng `metadata_timeout` làm hạn | bắt: 1 failed |
| M6 hết hạn trả 0 | bắt: 2 failed |
| M9 tối đa 3 lần đọc | bắt: 1 failed |
| M11 `time.monotonic` thay `self._clock` | bắt: 1 failed |
| M3 bỏ kiểm hạn; M7 bỏ sleep; M10 `time.sleep` thật; M12 tính lại hạn trong vòng | treo quá 60 s, không báo fail |
| M2 `>=` thành `>` | sống: 129 passed |
| M4 hardcode 20.0 | sống: 129 passed (xem N3) |
| M8 sleep 5 s | sống: 129 passed |

- Bốn đột biến treo cho thấy hồi quy "chờ vô hạn" chỉ lộ ra dưới dạng CI treo. Có thể cho `FakeAdmin` ném lỗi sau N lần gọi.
- Expected của hai test mới là hằng viết tay (`{0: 0}`, 3 lần gọi, 2 lần sleep, khoảng 20.0-20.5), không gọi hàm đang test để tính. Chưa có test đi trọn `prepare_topic` với `ConfluentBroker` (None, tạo hoặc ALREADY_EXISTS, trễ metadata, đọc offset).
- Đổi hành vi: `end_offsets` với topic không tồn tại nay chờ ít nhất `leader_wait_seconds` rồi mới thoát 14, trước đây thoát ngay. Nơi gọi duy nhất trong `src` là `publisher.py:98`. `leader_wait_seconds` tối thiểu là 1 theo config; với 1 s thì thoát sau 1.0 s.
- `list_topics` ném `KafkaException` trong lúc chờ thì thoát 14 ngay, không thử lại (ca H: `t=0.2s list_calls=2`). Vẫn đúng mã 14 của ADR-0010.
- Topic đã hiện nhưng leader chưa sẵn sàng: lỗi cấp topic được `partition_count` thử lại, lỗi watermark thử lại theo `_TRANSIENT_ERRORS`. Trên mock cluster trong tiến trình của librdkafka 2.16.0, watermark của topic hoặc partition chưa biết trả `_UNKNOWN_PARTITION -190`, nằm trong tập này. Mock không thay được broker thật.
- Producer sau đó dùng client riêng nên metadata của nó có thể còn trễ. Mã không đặt gì thêm, dựa vào hành vi của librdkafka. Tôi chưa kiểm phần này.
- Mã thoát 10/14/15 không đổi. Tôi không thấy chỗ nào của diff trái ADR-0010 hoặc spec I.5 (dòng 255) và II.4 (dòng 738-739). Câu mới ở README `:33` khớp mã về mã thoát, thiếu trần thời gian (N2).
- Không có thay đổi trong `contracts/` hay `tests/fixtures/`. Không test nào bị xóa, skip hoặc nới expected (diff test chỉ thêm dòng). Không thấy hardcode mới, float cho tiền, đọc ground truth trong `src/`, hay việc ngoài phạm vi.
- `PROGRESS.md` không đổi trong PR này; nên có dòng đề xuất cho bản sửa theo bước 5 của `CLAUDE.md`.

### Điều kiện hoàn thành thẻ T20 (`docs/TASKS.md:83`)

| Điều kiện | Trạng thái |
|---|---|
| Đếm message trên topic khớp manifest | không kiểm chứng được trong lượt này: test ở `tests/integration/test_publish_kafka.py:146`, không được chạy |
| Đổi key không đổi value | integration (`:173`) chưa chạy; unit `tests/unit/publish` pass; diff không đụng vòng gửi hay chọn key |

### Output lệnh kiểm tra
`check_rules.py` chạy trên repo với `N06_BASE_REF=main`. `verify_contract.py` và pytest chạy trên bản `git archive 8d3f77a` trong scratchpad, vì verify ghi `VERIFICATION_REPORT.json` vào `contracts/` và để không đụng `.env`.
```
check_rules: OK

PASS: Manifest SHA-256, physical rows, UTF-8/LF and safe paths
PASS: All positive fixture records satisfy package schema subset with format checks
PASS: Generator config schemas and cross-field constraints
PASS: Business keys, account references, currencies, dates and chronology
PASS: Envelope consistency; replay dedup yields exact reference business records
PASS: Ledger completeness, balanced sides and transaction linkage
PASS: Hand-authored expected counts and per-currency control totals
PASS: Hand-authored control report: schema, row rules and per-group totals
PASS: Two-day opening/movement/closing arithmetic and no overdraft
PASS: Actual statement batches and delivery schedule equal reference without early release
PASS: All three reconciliation snapshots match every key and expected histograms
PASS: Before, exactly at and after cutoff use an unambiguous inclusive deadline
PASS: Negative records, ID conflicts, incomplete/unbalanced ledger and malformed JSON
PASS: Golden vectors for draw(), UUIDv5 identities and external_reference
14 check groups passed.   (báo cáo sinh ra giống hệt bản đã commit)

ruff check: All checks passed!
ruff format --check: 113 files already formatted
pytest tests/unit/publish: 129 passed in 1.76s
```

### Chưa kiểm
- `tests/integration`, `scripts/smoke.sh`, toàn bộ `tests/unit` ngoài `publish` (theo ràng buộc).
- Chưa tái hiện lỗi chập chờn hay xác nhận bản sửa trên Kafka thật; chưa tạo topic nào, chưa đụng container, tôi không mở `.env`.
- Hành vi metadata của producer ngay sau khi tạo topic, và cụm nhiều broker.

Script đo (ngoài repo; `git status` của repo sạch):
- `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review12/scripts/probe.py`
- `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review12/scripts/probe2.py`
- `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review12/scripts/mock.py`
- `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review12/scripts/mutate.py`
