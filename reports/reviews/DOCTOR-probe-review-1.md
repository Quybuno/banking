# Review độc lập bản sửa probe của n06 doctor, lượt 1

Phần trên đường kẻ do người điều phối thêm; phần dưới đường kẻ là **nguyên văn** báo cáo của reviewer (subagent `spec-reviewer`), trích bằng script từ bản ghi phiên, không chỉnh sửa. Nội dung là nhận định của reviewer tại thời điểm viết, có thể chứa lỗi (ví dụ số dòng).

- Agent: `aa92807f6ce52f2ad`
- Thời điểm: 2026-11-25T03:01:40.801Z
- Lượt báo cáo: 1/1 của agent này
- SHA-256 phần nguyên văn: `a18600f89f94935642ae18d2d30a4bd11ebe4612259c5d748ac7021b6bbc2e50`

---

## Review nhánh `task/DOCTOR-probe-timeouts` (commit `ac27f05`)

Kết luận: còn 2 mục PHẢI SỬA. Trường hợp chính của yêu cầu (máy chủ im lặng, từ chối, ngắt kết nối, phản hồi rác) đã đạt. Điều kiện "không văng ngoại lệ" chưa đạt. Tôi không sửa file trong repo; `git status` vẫn sạch.

Các file liên quan:
- `/home/quybuno/banking/src/n06/cli/doctor.py`
- `/home/quybuno/banking/src/n06/cli/README.md`
- `/home/quybuno/banking/tests/unit/cli/test_doctor_probes.py`
- `/home/quybuno/banking/src/n06/cli/object_store.py`

### PHẢI SỬA

**1. Ngoại lệ còn lọt khỏi `_probe_tcp` và `_probe_http`** (`doctor.py:521`, `doctor.py:540`)

Cả hai chỉ bắt `(OSError, http.client.HTTPException)`. `UnicodeError`, `ValueError` và `OverflowError` đi thẳng ra ngoài.

Tái hiện đầu cuối bằng `uv run n06 doctor --profile demo` trên dịch vụ thật. Cả ba lệnh đều ra traceback, `exit=1`, không có mã 14 hay tên dịch vụ:
```
N06__KAFKA__BOOTSTRAP_SERVERS=a..b:19092
N06__OBJECT_STORE__ENDPOINT=http://a..b:19000
N06__POSTGRES__MART__HOST=a..b
-> UnicodeError: encoding with 'idna' codec failed (UnicodeError: label empty or too long)
```
Phản hồi sai từ máy chủ cũng làm `_probe_http` ném (gọi hàm trực tiếp, máy chủ giả trả 302):
```
Location: http://[abc                      -> ValueError: Invalid IPv6 URL
Location: http://a..b/                     -> UnicodeError
Location: http://127.0.0.1:999...(20 số)/  -> OverflowError
```
Gọi trực tiếp với cổng `2**63` hoặc `10**30` cũng ra `OverflowError` ở cả hai hàm. Qua `doctor` thì tôi không tới được đường này, vì `compose ps` lỗi trước (xem NHẬN XÉT).

Ba lỗi đầu cần cấu hình sai, lỗi redirect cần máy chủ cư xử sai. Gợi ý: bắt thêm `ValueError` (gồm `UnicodeError`) và `OverflowError`, kèm test.

Câu ở `README.md:68` "Không probe nào được ném ngoại lệ" hiện sai.

Probe bucket: tôi không thấy đường lọt. `head_bucket` bắt cả `ValueError` (`object_store.py:358`) và cổng đã được kiểm miền 1-65535.

**2. Probe HTTP đi qua `http_proxy`: FAIL/PASS sai, traceback lộ mật khẩu** (`doctor.py:535`)

`urllib.request.urlopen` đọc proxy từ `os.environ`, kể cả cho `localhost`. Hành vi này đã có trên main (cũng dùng `urlopen`), nhánh chưa sửa.

- FAIL sai: `http_proxy=http://127.0.0.1:1` làm `service.minio` và `service.spark` FAIL mã 14 dù cả hai healthy. Thêm `no_proxy=localhost,127.0.0.1` thì PASS 12/12.
- PASS sai: proxy giả trả 200, đích là cổng không ai nghe. `_probe_http` trả `(True, '... -> 200 OK')`, proxy nhận `GET http://localhost:37563/minio/health/live`.
- Traceback lộ secret: `http_proxy=http:/probeuser:S3cretProxyPw@proxyhost` cho `ValueError: proxy URL with no authority: 'http:/probeuser:S3cretProxyPw@proxyhost'`, `exit=1`.
- Lộ trong báo cáo: `http_proxy=http://probeuser:S3cretProxyPw` (thiếu `@host`) cho `InvalidURL: nonnumeric port: 'S3cretProxyPw'` trong `detail`. `redact_secrets` không che vì tên biến không thuộc danh sách.

Gợi ý: dùng `urllib.request.build_opener(urllib.request.ProxyHandler({}))` cho probe. Probe bucket dùng `http.client` nên vốn không qua proxy.

### NÊN SỬA

**3. Không có hạn thời gian tổng, máy chủ nhỏ giọt treo probe** (`doctor.py:473-480`, `515`, `535`; `object_store.py:302`)

Timeout chỉ áp cho từng thao tác socket. Đo được, tất cả đều trả PASS/exists:

| Probe | Kịch bản | Thời gian |
|---|---|---|
| Kafka (timeout 2 s) | 1 byte mỗi 1,8 s | 18,00 s |
| HTTP (timeout mặc định 2,5 s) | 15 byte, mỗi byte cách 2 s | 30,01 s |
| HTTP (timeout 0,3 s) | 100 byte, mỗi byte cách 0,2 s | 20,03 s |
| Bucket (timeout 0,3 s) | như trên | 20,03 s |

Kafka bị chặn quanh 20 s (10 byte). HTTP và bucket gần như không chặn trên: mỗi dòng header tới 65536 byte, tối đa khoảng 100 header.

**4. README nói chưa đúng mã**
- `README.md:71` "thời gian chờ mỗi probe là 2-5 giây" sai theo mục 3; đó là thời gian chờ mỗi thao tác socket.
- `README.md:26`: dòng mã 14 vẫn ghi "hoặc không mở kết nối port", thiếu trường hợp im lặng, sai giao thức, thiếu bucket.

**5. Postgres PASS sai với dịch vụ trả byte đầu là `S` hoặc `N`** (`doctor.py:485-487`)

Máy chủ giả gửi `SSH-2.0-OpenSSH\r\n` (sau khi nhận yêu cầu) cho kết quả `(True, '... dịch vụ trả lời đúng giao thức')`. Gợi ý: `recv` với bộ đệm lớn hơn 1 và yêu cầu nhận đúng một byte `S`/`N`.

**6. Test chưa khóa hành vi bắt tay**

Đột biến trên bản sao trong scratchpad. Các thay đổi sau vẫn cho `21 passed`:
- `api_key` 18 đổi thành 3.
- Phiên bản 0 đổi thành 3.
- Độ dài `client_id` đổi thành 0.
- `_recv_exactly` bỏ vòng lặp (chỉ `recv` một lần).
- Bỏ điều kiện `size < 6`.
- `_probe_tcp` bỏ `HTTPException` (đường này không phát sinh được, ít quan trọng).

Ba đột biến khác bị bắt: HTTP bỏ `HTTPException` (1 fail), bỏ handshake trong `check_services` (2 fail), Kafka dùng handshake Postgres (3 fail).

Postgres có assert đúng byte yêu cầu (`test:200`), Kafka thì không (`test:85-88`, `203-206`; `_kafka_ok` chỉ đọc `request[8:12]`). Cần assert nguyên byte yêu cầu Kafka viết tay, và một test trả phản hồi chia nhiều mảnh.

**7. Test phụ thuộc môi trường**: chạy file test mới với `http_proxy=http://127.0.0.1:1` cho `5 failed, 16 passed`. Sửa mục 2 thì khả năng cao hết.

### NHẬN XÉT

- **Giao thức Kafka**: header v1 và thứ tự trường đúng cho `ApiVersions` v0. Broker thật 3.7.2 trả `size 352, corr 0x4e3036, err 0, apikeys 57`. `error_code != 0` (kể cả 35) báo "phản hồi không phải của Kafka broker" (`doctor.py:500`) là gây hiểu nhầm, nên in mã lỗi. Không chặn trên `size`: `2**31-1` vẫn PASS. Listener TLS/SASL chưa kiểm; compose chỉ dùng PLAINTEXT.
- **Giao thức Postgres**: `SSLRequest` đúng, server thật trả `N`. Theo hiểu biết giao thức (chưa kiểm), `SSLRequest` được trả lời cả khi DB đang khởi động hoặc hết kết nối; nếu bật SSL (`S`) thì đóng ngay có thể sinh log.
- **Tác dụng phụ**: sau 20 vòng probe vào mỗi dịch vụ, `docker logs --since` cho 0 dòng mới ở cả Kafka và Postgres, với cấu hình log hiện tại.
- **Cổng ngoài miền**: `_probe_tcp("localhost", 84628, ...)` trả PASS (84628 = 19092 + 65536, cổng bị quấn vòng). Qua `doctor`, thử `N06_SPARK_UI_HOST_PORT=84616` và cổng Kafka 20 chữ số: cả bốn dịch vụ báo "chưa được tạo trong Docker Compose" (mã 14), không probe. Thông báo đó gây hiểu nhầm; theo mã (`doctor.py:430`) là do `compose ps` thất bại.
- `N06_MINIO_API_HOST_PORT=abc` vẫn ra traceback (`doctor.py:562`), đúng như tồn đọng ghi ở `README.md:72`.
- **Mã thoát và tên dịch vụ**: các đường FAIL đã bắt đều là 14 và `summary` có tên dịch vụ. Bucket thêm "MinIO". Mã thoát cuối vẫn lấy mục FAIL đầu tiên (ví dụ 12 đứng trước 14), như trước.
- **Secret**: phần probe của cấu hình dự án không đưa credential vào thông báo. `{exc}` đưa nguyên chuỗi do máy chủ gửi, kể cả ký tự điều khiển như `\x1b[31m`, ra output.
- **Chập chờn**: file test mới đạt 21/21 trong 5 lần lặp và thêm 1 lần với `-W error::pytest.PytestUnhandledThreadExceptionWarning`. `reply_server` đọc bằng một lần `recv`, và test "refused" dùng lại cổng vừa nhả: rủi ro lý thuyết, chưa gặp.
- **Phạm vi**: diff chỉ gồm 3 file; không đổi `contracts/` hay `tests/fixtures/`; không xóa, skip hay nới test cũ. Không thấy thêm endpoint hardcode, float cho tiền hay đọc ground truth. `docs/TASKS.md` không có thẻ cho ID này (grep "DOCTOR" không ra), nên đối chiếu theo yêu cầu bạn nêu; `PROGRESS.md` chưa đổi trong diff.

### Output kiểm tra
```
pytest tests/unit          580 passed in 134.90s
ruff check .               All checks passed!
ruff format --check .      113 files already formatted
check_rules.py             check_rules: OK
verify_contract.py         14 check groups passed.
n06 doctor --profile demo  PASS (có 1 cảnh báo) 12/12, exit_code=0
```
Ngoài danh sách được phép, tôi đã chạy `verify_contract.py`, `ruff format --check`, các lệnh chỉ đọc `docker compose ps`, `docker logs`, `docker exec ... grep postgresql.conf`, và đột biến trên bản sao ở `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/mut_49068` (còn nguyên, không xóa). Không chạy test tích hợp, không đụng trạng thái container.
