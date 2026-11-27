# PR #13 (T20: assertion thông lượng): review độc lập, một vòng

Phần trên đường kẻ do người điều phối thêm; phần dưới đường kẻ là **nguyên văn** báo cáo của reviewer (subagent `spec-reviewer`), trích bằng script từ bản ghi phiên, không chỉnh sửa. Nội dung là nhận định của reviewer tại thời điểm viết, có thể chứa lỗi (ví dụ số dòng).

- Agent: `a3dad482392d03a0e`
- Thời điểm: 2026-11-27T07:30:03.051Z
- Lượt báo cáo: 1/1 của agent này
- SHA-256 phần nguyên văn: `f44c405c20307941c6f93fb06dc21436b450ca7130729069d988fef2d3474b6b`

---

# Review PR #13 (T20, commit 05469fd, diff `origin/main...05469fd`, 3 file test)

Kết luận: **chưa nên merge ngay**. Còn một chỗ hụt phép kiểm cần sửa và một quyết định cần chủ dự án.

## Trả lời 1-4

### 1. Hai assertion bị bỏ có nằm trong điều kiện chủ dự án nêu không?
- `limited.achieved_rate <= rate * 1.03`: **xác nhận**. Đây đúng là so thông lượng đo được với số cố định. Lưu ý nó không phụ thuộc máy: nó được suy ra từ cận dưới thời gian (xem câu 2).
- `free.achieved_rate > limited.achieved_rate`: **không xác nhận nằm trong điều kiện nguyên văn**. Hai vế đều là số đo, assertion không có số cố định nào.
  - Lập luận "ngầm đòi máy vượt 3000 msg/s" chỉ giải thích vì sao assertion không đáng tin. Theo số PR tự nêu (1664 so với 2203, cả hai dưới 3000), đó là so hai số đo cùng bị giới hạn bởi máy. Điều này không biến nó thành "so với số cố định".
  - Về kỹ thuật, bỏ nó là hợp lý và cách thay đúng dạng chủ dự án yêu cầu. Nhưng đây là mở rộng phạm vi cho phép, nên chủ dự án phải quyết. Mô tả PR đã nêu trung thực điểm này.

### 2. Phép kiểm có bị nới không?
- Toán của "cận trên là thừa khi total >= 108": **xác nhận**. total=107 cho 3090,10 (không nhỏ hơn 3090); total=108 cho 3089,83. Bộ tích hợp có total=4330, cận suy ra khoảng 3061,9.
- Lập luận này cần thêm tiền đề `achieved_rate == delivered / elapsed_seconds` (`src/n06/producer/publisher.py:212`). PR không nêu tiền đề này và test tích hợp mới không assert nó, thực tế không còn đọc `achieved_rate` lần nào.
- "Phép kiểm không bị nới": **không xác nhận**. Ngoài mất mát đã khai, còn một mất mát chưa khai: không test nào kiểm `achieved_rate` khi `rate=0`, tức đường đo của E2 (xem P1).

### 3. Test đơn vị mới
- Tất định: **xác nhận** cho hai test publisher. Dùng đồng hồ giả; `perf_counter_ns` chỉ dùng cho độ trễ và không bị assert. Riêng `test_rate_flag_overrides...[None-5]` chạy đồng hồ thật (chờ thật 0,41 s), nhưng assert `target_rate` không phụ thuộc thời gian.
- Expected độc lập: **xác nhận** (`k/7`, `[0.0]*7`, bảng parametrize).
- Bắt hồi quy: **một phần**. **Có đường treo vô hạn** (M07, M08).

Đột biến trên bản sao, khác 5 đột biến của PR. Cột "test của main" là 129 test publish tại `origin/main`.

| Đột biến | 3 test mới | Cả publish (134) | Test của main (129) |
|---|---|---|---|
| M01 chờ gấp đôi (`end = clock() + 2 * seconds`) | pass | 134 pass | 129 pass |
| M02 gửi sớm một nhịp | fail | 5 fail | 4 fail |
| M03 `acquire` sau `produce` | fail | 1 fail | 129 pass |
| M04 `achieved_rate = 0` khi rate 0 | pass | 134 pass | 129 pass |
| M05 `elapsed_seconds` báo dư 100 s | pass | 134 pass | 129 pass |
| M06 rate 0 gọi `time.sleep(0.001)` | pass | 134 pass | 129 pass |
| M07 vòng chờ `poll(0)` | **TREO** (timeout 20 s) | **TREO** (40 s) | 129 pass |
| M08 `remaining <= 0` thành `< 0` | **TREO** | **TREO** | 129 pass |
| M09 chỉ chờ một lát 5 ms | fail | 2 fail | 1 fail |
| M10 mặc định `time.sleep` | fail | 2 fail | 1 fail |
| M11 limiter nhận `rate*2` | fail | 3 fail | 2 fail |
| M12 `started` đặt sau vòng gửi | fail | 3 fail | 2 fail |
| M13 rate 0 bị giới hạn 10000/s | fail | 1 fail | 129 pass |
| M14 vòng chờ dùng `time.monotonic` | pass | 134 pass | 129 pass |

M03 và M13 chỉ bị test mới bắt.

### 4. Tiêu chí T20 và ADR-0010
**Chưa đủ ở phía "đo"; "điều khiển" chỉ chặt một phía.**
- Cụm "tốc độ phát đo và điều khiển được" tôi không tìm thấy nguyên văn. `docs/TASKS.md:83` ghi việc "tốc độ phát", cột kiểm tra chỉ có "đếm message trên topic khớp manifest; đổi key không đổi value". Gần nhất là `docs/adr/0010-...md:10` ("tốc độ phát điều khiển được (điều kiện 4)") và dòng 64.
- Không nhanh hơn `rate`: được kiểm tất định (test mới và `test_rate.py`).
- Không chậm hơn `rate`: đường `sleep` tiêm vào được kiểm chặt (`test_publisher.py:206-207`). Đường chờ mặc định qua `poll` không có kiểm tất định (M01).
- Đo: `target_rate` được kiểm. `achieved_rate` chỉ còn ở rate=7 (`test_publisher.py:209`) và rate=20 bằng đồng hồ thật (`test_review_fixes.py:214`). rate=0 và tích hợp không có.
- `p50 <= p95 <= p99` giữ nguyên. ADR-0010 không đặt ngưỡng test; không sửa `src/` nên không thấy mâu thuẫn. Spec dòng 435 ("0 = không giới hạn") có test tương ứng.
- Hai tiêu chí ở cột kiểm tra: "đổi key không đổi value" đạt ở unit (`test_publisher.py:93-100`, pass). "Đếm message khớp manifest" không kiểm chứng được vì tôi không chạy tích hợp; PR không sửa các test đó.

## Phải sửa

**P1. Mất phép kiểm `achieved_rate` khi `--rate 0`** (`tests/integration/test_publish_kafka.py:243-248`).
Với M04, tôi tính hai biểu thức assert trên kết quả `publish_events` với producer giả (không chạy file tích hợp):
```
M04: limited.achieved=3000.4 free.achieved=0.0
  assert cu  'free.achieved_rate > limited.achieved_rate' -> False
  assert moi 'free target_rate==0 va delivered==total'    -> True
```
Đề xuất, không phụ thuộc máy:
- Tích hợp: `assert limited["achieved_rate"] == pytest.approx(total / limited["elapsed_seconds"])`, tương tự cho `free`. Chưa chạy trên broker.
- Đơn vị, trong `test_zero_rate_never_waits`: `assert stats.achieved_rate == pytest.approx(7 / stats.elapsed_seconds)`. Đã thử trên bản sao: bản sạch 134 pass, M04 fail. Đây là kiểm nhất quán giữa hai trường.
- Mô tả PR khai thêm mất mát và tiền đề ở câu 2.

**P2. Cần chủ dự án đồng ý rõ việc bỏ assertion thứ hai trước khi merge** (không phải sửa code). Nếu không đồng ý, phương án khác là giữ phép so sánh nhưng hạ rate của lượt giới hạn (ví dụ 500). Phương án đó vẫn phụ thuộc máy, chỉ ở ngưỡng thấp hơn nhiều.

## Nên sửa

**N1. Test treo thay vì fail** (`tests/unit/publish/test_publisher.py:228-231`). Đồng hồ giả không tiến khi `poll(0)`. M07, M08 làm test treo; không thấy `timeout-minutes` hay `pytest-timeout` trong `.github` và `pyproject.toml`. M08 vô hại với đồng hồ thật nhưng vẫn treo CI. Sửa: trong `_TimedProducer.poll` thêm `assert len(self.polls) < 10_000, "vong cho khong ket thuc"`. Đã thử: M07, M08 chuyển thành fail.

**N2. Kiểm hai phía** (`test_publisher.py:239-240`): thay vòng `>=` bằng `assert producer.sent_at == pytest.approx([k / 7 for k in range(7)], abs=1e-6)`. Đã thử cùng N1: bản sạch 134 pass; M01, M14 fail.

**N3. Cận dưới ở tích hợp rỗng trên máy chậm** (`test_publish_kafka.py:229,238-240`). total=4330, minimum=1,4141 s: máy dưới khoảng 3062 msg/s tự đạt dù limiter tắt. Mô phỏng bằng producer giả chạy đồng hồ thật:
```
limiter BI TAT, may 2423 msg/s: elapsed=1.7872 achieved=2423 | moi: elapsed>=min -> True | cu: achieved<=3090 -> True
limiter BI TAT, may 8000 msg/s: elapsed=0.5414 achieved=7998 | moi: elapsed>=min -> False | cu: achieved<=3090 -> False
```
Các số PR nêu (2423, 1664) nằm dưới ngưỡng. Điểm này đã có từ trước. Hạ `rate` xuống 500 cho minimum khoảng 8,5 s, có hiệu lực trên máy nhanh hơn khoảng 511 msg/s (chưa chạy trên broker).

**N4. `rate=0 không chờ` chỉ kiểm đường chờ qua `poll`** (`test_publisher.py:244-249`). M06 lọt. Có thể monkeypatch `time.sleep` cho ném lỗi (chưa thử).

**N5. Commit message nói quá**: "Hai assertion cũ phụ thuộc máy". Assertion `<= rate*1.03` không phụ thuộc máy, chính lập luận của PR cho thấy vậy. Nên chỉnh ở mô tả hoặc merge message.

## Ghi nhận
- `tests/unit/publish/test_review_fixes.py:214` vẫn có `assert 15.0 < stats.achieved_rate < 25.0` với đồng hồ thật (dòng 212-213 có ngưỡng độ trễ cố định). Cùng loại "Lỗi B", ngoài diff này, chủ dự án nên biết. Hiện nó cũng góp phần bắt M09-M12.
- `test_publish_kafka.py:240` `wall >= minimum` yếu: `wall` gồm cả kiểm nguồn và tạo topic. Chiều hữu ích hơn là `elapsed_seconds <= wall` (bắt M05, lỗ hổng có từ trước; chưa thử).
- `tests/integration/` thuộc B theo README và CODEOWNERS (CODEOWNERS còn để placeholder `<github-B>`).
- Không có thay đổi trong `contracts/` hay `tests/fixtures/`; không test nào bị xóa hoặc skip; hai assert bị bỏ như trên. Chỉ sửa 3 file test, không thấy việc ngoài phạm vi, không đụng `src/` (không phát sinh float cho tiền, hardcode triển khai hay đọc ground truth trong `src/`).

## Output cổng
`check_rules.py` (chạy bằng `.venv/bin/python`, cả khi đặt `N06_BASE_REF=origin/main`):
```
check_rules: OK
exit=0
```
`verify_contract.py` chạy trên bản `git archive 05469fd` trong scratchpad, vì script ghi `VERIFICATION_REPORT.json`; sau khi chạy, `contracts/` của bản sao giống hệt repo:
```
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
14 check groups passed.
exit=0
```
Trên bản sao: `pytest tests/unit/publish` cho `134 passed in 1.32s`; `ruff check` "All checks passed!", `ruff format --check` "113 files already formatted". `gh pr checks 13`: `checks pass`. Repo sạch sau review, HEAD vẫn 05469fd.

## Chưa kiểm
- `tests/integration`, `smoke.sh`, container: không chạy. Các con số 22/22, 2/2 và unit 636/636 trong PR chưa tự xác nhận.
- Số đo 2423/2992 và 1664/2203, cùng 5 đột biến PR đã nêu: không chạy lại.
- Không đọc `.env`.

Script đột biến: `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review13/mutate.py`
