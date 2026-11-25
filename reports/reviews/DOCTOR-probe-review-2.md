# Review độc lập bản sửa probe của n06 doctor, lượt 2 (kiểm lại lượt 1)

Phần trên đường kẻ do người điều phối thêm; phần dưới đường kẻ là **nguyên văn** báo cáo của reviewer (subagent `spec-reviewer`), trích bằng script từ bản ghi phiên, không chỉnh sửa. Nội dung là nhận định của reviewer tại thời điểm viết, có thể chứa lỗi (ví dụ số dòng).

- Agent: `aa92807f6ce52f2ad`
- Thời điểm: 2026-11-25T03:31:10.666Z
- Lượt báo cáo: 2/2 của agent này
- SHA-256 phần nguyên văn: `6876c1bbb7323a58d7816c331301057d4bc605715e0222a61091e896e52e17a3`

---

## Xác nhận lại bản sửa probe: `git diff ac27f05..57516dd`

**Về mã nguồn: không còn mục PHẢI SỬA.** Còn 1 mục PHẢI SỬA thuộc quy tắc kho (không phải lỗi chạy).

Các file:
- `/home/quybuno/banking/src/n06/cli/doctor.py`
- `/home/quybuno/banking/src/n06/cli/README.md`
- `/home/quybuno/banking/tests/unit/cli/test_doctor_probes.py`
- `/home/quybuno/banking/reports/reviews/DOCTOR-probe-review-1.md`

### PHẢI SỬA

**1. File review đặt trong `reports/`, trái quy tắc đã ghi**
- Commit này thêm `reports/reviews/DOCTOR-probe-review-1.md` (120 dòng).
- `CLAUDE.md`: "`reports/` — chỉ chứa kết quả do `n06 benchmark` sinh."
- `reports/README.md`: "Chỉ ghi số liệu do lệnh `n06 benchmark` sinh ra".
- Trên nhánh `task/ADR-004-012`, commit `9c9881d` từng chuyển một báo cáo review ra khỏi PR (lý do ghi trong tiêu đề: "PR chỉ docs").
- Cần chuyển file ra ngoài `reports/`. Nếu chủ dự án muốn giữ, hãy để họ quyết và sửa quy tắc tương ứng.

### NÊN SỬA

**2. Test proxy không bắt được hồi quy** (`test_doctor_probes.py:339`)
- Bỏ `ProxyHandler({})` khỏi `_PROBE_OPENER` vẫn cho `51 passed`.
- Lý do: opener dựng lúc import, nên `ProxyHandler` mặc định đọc môi trường khi đó. `monkeypatch.setenv` chạy sau không có tác dụng.
- Chạy cùng đột biến với `http_proxy` đặt trước khi chạy pytest thì cả 3 ca fail.
- Mã hiện tại đúng. Nên chạy probe trong tiến trình con với env đặt sẵn.

**3. Probe HTTP chưa chặn cổng ngoài miền** (`doctor.py:598-623`; chỉ `_probe_tcp` chặn ở `:562`)
- Với máy chủ giả ở cổng `p`, `_probe_http` tới `p+65536` trả `(True, '... -> 200 OK')`, cổng `p+131072` cũng vậy.
- Với Spark thật ở cổng 83616 thì trả mã 400.
- Qua `doctor` hiện không tới được đường này: env ngoài miền làm `compose ps` thất bại, endpoint cổng 99999 bị chặn khi parse.
- Nửa HTTP của `test_port_outside_range_fails_without_connecting` (`:306-309`) không đúng tên. Với 84628 nó quấn về 19092 và nối tới cổng Kafka thật trên máy này; assert `False` đạt không phải nhờ chặn cổng.

**4. README và thông báo cấu hình chưa khớp hành vi**
- `README.md:68` nói URL hỏng hoặc cổng ngoài miền là mã `14`. Chạy thật, `N06__OBJECT_STORE__ENDPOINT=http://[abc` và `http://localhost:99999` ra mã `10`.
- Khi chỉ endpoint MinIO hỏng, cả 4 dịch vụ (kể cả Kafka) FAIL "Không suy được địa chỉ của service ...". `detail` (`ValueError: Invalid IPv6 URL`, `invalid literal for int() ... 'abc'`) không nêu biến hay trường nào sai (`doctor.py:676-687`).

### NHẬN XÉT

**Ca cũ đã chạy lại, đạt**
- `a..b` qua ba biến `N06__...`: FAIL 14 đúng dịch vụ, không traceback.
- Redirect hỏng, mã 301/302/303/307/308: FAIL, không đi theo.
- `N06_{KAFKA,MINIO_API,POSTGRES,SPARK_UI}_HOST_PORT=abc`: mã 10.
- `_probe_tcp` với cổng 70000, -1, 84628, `2**63`, `10**30`: FAIL.
- Các biến thể proxy: không còn traceback, không thấy `S3cretProxyPw` trong các dòng đã lọc; proxy giả trả 200 không còn tạo PASS sai.
- Banner SSH và `NN` bị từ chối; Kafka mã 35 nêu mã lỗi; ký tự điều khiển trong `BadStatusLine` thành `?`.

**Hạn tổng, chạy đầu cuối với máy chủ giả cục bộ** (stderr rỗng, tiến trình thoát bình thường)

| Kịch bản | Kết quả | Thời gian cả lệnh |
|---|---|---|
| Cả 4 dịch vụ im lặng | 4 FAIL, mã 14 | 12,3 s |
| Nhỏ giọt (Postgres im lặng) | 4 FAIL, mã 14; MinIO và Spark do hạn 8 s | 27,3 s |
| Chỉ bucket `HEAD` nhỏ giọt | bucket FAIL, mã 14, hạn 8 s | 10,8 s |

**Luồng nền**
- Luồng quá hạn còn sống khi máy chủ vẫn gửi (3 lần probe để lại 3 luồng sau 1 s) và giữ socket. Với CLI một lần chạy thì vô hại; nếu gọi trong tiến trình dài thì nên đóng socket khi hết hạn.
- Ngoại lệ phát sinh sau hạn bị nuốt im lặng (`excepthook` không được gọi). Ngoại lệ trước hạn, gồm `SystemExit`, được ném lại đúng.
- `head_bucket` gọi trực tiếp vẫn kéo 20 s (ngoài `doctor` không có hạn tổng; `init-local` ngoài phạm vi).

**Dịch vụ khỏe**
- `doctor --profile demo` PASS 12/12.
- 60 lần probe mỗi dịch vụ: 0 FAIL, max Spark 210 ms, ba dịch vụ còn lại dưới 15 ms.
- Một lần chạy `doctor` (với `http_proxy` hỏng) báo `service.spark` `TimeoutError: timed out`. Chạy lại 6 lần cùng env đều PASS, nên chưa giải thích được. Hạn socket 2,5 s không đổi trong diff, và tôi không có bằng chứng nó liên quan bản sửa.

**Đột biến còn sống** (ngoài proxy)
- Bỏ ánh xạ 301/303/307/308 vẫn `51 passed`: test chỉ dùng 302.
- Đổi `PROBE_DEADLINE_SECONDS` thành 3600 vẫn `51 passed`: không test nào khóa hạn mặc định.
- Các đột biến khác tôi thử đều bị bắt: byte yêu cầu Kafka, đọc chia mảnh, `size<6`, `recv(1)`, `ValueError`, `OverflowError`, chặn cổng TCP, `_NoRedirect`, lọc ký tự điều khiển, hạn tổng, ném lại lỗi, hạn bucket.

**Nhỏ**
- `N06_KAFKA_HOST_PORT=` (rỗng) ra mã 10, trong khi Compose coi rỗng là mặc định (`${...:-19092}`).
- Hai test nhỏ giọt HTTP và bucket mất 5,6 s mỗi test, vì máy chủ giả còn gửi sau khi test xong.
- Kafka vẫn không chặn trên `size` (`2**31-1` PASS).
- Listener TLS/SASL chưa kiểm.

### Output kiểm tra
```
pytest tests/unit          610 passed in 141.75s
ruff check .               All checks passed!
ruff format --check .      113 files already formatted
check_rules.py             check_rules: OK
n06 doctor --profile demo  PASS (có 1 cảnh báo) 12/12, exit_code=0
test_doctor_probes.py      51/51 ở các lần lặp, cả khi đặt http_proxy và khi tải CPU (3 lần)
```
`contracts/` và `tests/fixtures/` không đổi (diff so với main chỉ 4 file); lần này tôi không chạy lại `verify_contract.py`. Không sửa file trong repo (`git status` sạch), không chạy test tích hợp, không đụng container, không rm. Ngoài danh sách được phép, tôi đã tạo tải CPU bằng 24 tiến trình Python tự hết sau 90 s, và chạy đột biến trên bản sao trong `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad` (còn nguyên).
