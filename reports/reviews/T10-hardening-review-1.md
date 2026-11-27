## Review PR #21, `task/T10-hardening` (5cc543f so với origin/main)

Đây là vòng review duy nhất. Không sửa file nào trong repo hay worktree. Script thử và bản sao nằm trong `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review21/`.
Các đường dẫn `src/` và `tests/` dưới đây tính từ `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/wt-t10/`.

### Điều kiện của thẻ

| Điều kiện | Kết quả |
|---|---|
| Không ném trên số nguyên 5000 chữ số | Đạt. Bản cũ `RAISE ValueError`, bản mới `INVALID_JSON` |
| Không ném trên mảng lồng sâu | Đạt. Bản cũ `RecursionError`, bản mới `INVALID_JSON` |
| value null | Đạt. `SCHEMA_TYPE` (bản cũ cũng vậy) |
| UTF-8 hỏng | Đạt. `INVALID_JSON` |
| Có test với các record này | Đạt, còn lỗ hổng ở mục "Nên sửa" 3 |
| Không record xấu nào làm ném | Đạt với mọi đầu vào str/bytes trong fuzz. Còn ngoại lệ với Mapping dựng trong Python (mục 1, 4) |

Thẻ T10-hardening không thấy trong `docs/TASKS.md` (chỉ có T10 gốc). Tôi lấy nội dung thẻ từ yêu cầu của bạn.

### Phải sửa

Không có phát hiện chặn merge.

### Nên sửa

**1. Mapping có khóa khác kiểu vẫn ném `TypeError`**, `src/n06/domain/schema_validator.py:281`
```
extra_keys = sorted(set(value.keys()) - set(props.keys()))
mixed keys tx          -> RAISE TypeError '<' not supported between instances of 'str' and 'int'
mixed keys env payload -> RAISE TypeError ... (qua validate_event_envelope)
```
- Đầu vào là `{..., 1: 2, "x": 3}`. Fuzz gặp 939 lần.
- Lỗi có từ bản cũ và không xảy ra được từ JSON (khóa luôn là str), nên không chặn đường Kafka.
- Nó trái docstring "Không ném ngoại lệ vì dữ liệu" và dòng mới trong README. Gợi ý: trả mã lỗi cho khóa không phải str, và thêm test.

**2. Bắt ngoại lệ có nuốt lỗi lập trình**, `schema_validator.py:490-493`
```
self-$ref schema   cũ: RAISE RecursionError    mới: False INVALID_JSON record=None
```
- Schema `$ref` vòng, hoặc một `ValueError` do bug trong `validate_instance`, sẽ biến mọi record thành `INVALID_JSON`. Message chỉ giữ `type(exc).__name__` nên khó truy nguyên.
- README ghi "Lỗi lập trình ... vẫn ném". Các trường hợp tôi thử còn lại vẫn ném: từ khóa lạ, `type` sai, regex hỏng, `$ref` không tồn tại, `minimum` là chuỗi, tên schema sai, tên timezone sai.
- Schema hợp đồng hiện không có vòng, nên đây là rủi ro tiềm ẩn.
- Gợi ý: dùng một hàm repr an toàn cho `{value!r}` (dòng 217, 231, 247, 262, 332, 342) để thu hẹp hoặc bỏ khối `try` này. Tối thiểu đưa `exc` vào message.

**3. Test chưa bắt hết hồi quy** (đột biến trên bản sao, chạy `tests/unit/domain`)
```
baseline => 43 dấu chấm, đạt hết
errors=replace => FAILED
tuple -ValueError => FAILED
tuple -RecursionError => FAILED
tuple -OverflowError => đạt hết   (sống sót)
no except at loads => FAILED
no except at validate => FAILED
no bytearray/memoryview => FAILED
revert date transaction => FAILED
revert date ledger => FAILED
revert date statement => đạt hết   (sống sót)
src cua origin/main voi test moi => FAILED
```
- Sửa đổi ở `business_validator.py:331` (statement) không có test. Hoàn nguyên dòng này thì `validate_statement_record` lại ném `OverflowError` mà test vẫn xanh. Probe xác nhận hành vi mới đúng: `statement 9999 -> BUSINESS_DATE_MISMATCH`.
- `OverflowError` trong `_BAD_DATA_ERRORS` không có test, và fuzz không tìm được đầu vào nào cần nó. Nên bỏ hoặc nêu ca cụ thể.
- `tests/unit/domain/test_bad_records.py:84-86` chỉ assert `is_valid is False`, không assert mã, dù docstring của file nói mỗi record cho một mã lỗi.

**4. README nói rộng hơn thực tế với validator nhóm ledger** (hành vi cũ, không đổi), `business_validator.py:499-516`
```
group None tx        -> RAISE TypeError 'NoneType' object is not subscriptable
group null amount    -> RAISE TypeError Trường 'amount_minor' phải là số nguyên (int) ...
group missing key    -> RAISE KeyError 'schema_version'
tracker null payload -> RAISE TypeError   (ConflictTracker.check_and_register_envelope)
```
Hai hàm này nhận Mapping nhưng giả định record đã qua kiểm. Ghi rõ tiền điều kiện đó trong README hoặc docstring, hoặc mở việc riêng.

### Ghi nhận

- **Mã lỗi và ưu tiên với Mapping có số nguyên quá dài:** `{amount_minor: 10**5000}`, kể cả khi kèm thiếu trường hoặc `schema_version` 2.0.0, cho duy nhất `INVALID_JSON` với `record=None`. Trong khi `10**4299` cho `AMOUNT_OUT_OF_RANGE`. Đặc tả II.8 định nghĩa `INVALID_JSON` là "Không parse được JSON" và yêu cầu "lưu tất cả issue_codes"; Mapping thì không qua bước parse. Đường str/bytes khớp ADR-0004 G1. README đã ghi quy ước cho số nguyên quá dài.
- **Ngưỡng "lồng quá sâu" phụ thuộc độ sâu stack của bên gọi:** cùng bytes, stack nông chuyển sang `INVALID_JSON` ở khoảng 991 cấp, thêm 600 frame thì ở 391. Ở cấp 992, `parse_and_validate_json_schema` cho `SCHEMA_TYPE` còn `validate_event_envelope` cho `INVALID_JSON`. Điểm vỡ là `json.loads`; bản cũ ném ở cùng vùng. Theo G1 cả hai mã đều vào `parse_error`, chỉ khác mã chính. Muốn tất định thì cần hằng độ sâu tối đa.
- **Còn ném trong ca nhân tạo:**
  - `Decimal("sNaN")` đặt vào trường `const` của Mapping gây `decimal.InvalidOperation` (36 lần trong fuzz).
  - `memoryview` đã `release()` nay ném `ValueError` (bản cũ trả `SCHEMA_TYPE`).
  - Mapping tự viết ném lỗi khi đọc vẫn ném, đây là hành vi đúng.
- **Đổi hành vi so với bản cũ** (so sánh 248.510 lời gọi, gồm record hợp lệ và biến đổi từ fixture/config):
  - str, dict, MappingProxy: mọi khác biệt đều là "trước ném, nay trả mã". Đa số là `INVALID_JSON`; còn lại là các ca ngày năm 9999.
  - bytes: ngoài các ca trước ném, 4.594 kết quả đổi, tất cả sang `INVALID_JSON` (giải mã UTF-8 nghiêm ngặt, đúng G1). Trong đó có record trước đây được coi là hợp lệ nhờ U+FFFD.
  - bytearray, memoryview: trước là `SCHEMA_TYPE`, nay được kiểm như bytes.
  - Không thấy record hợp lệ nào dạng str, dict hay bytes UTF-8 đúng bị đổi kết quả.
- Chữ ký kiểu trong `business_validator.py` (dòng 114, 127, 226, 308, 369, 794, 865, 970) vẫn là `str | bytes | Mapping`, trong khi README hứa nhận cả `bytearray` và `memoryview`.
- Ngoài phạm vi, có từ trước: `dataset_id` thêm `"\n"` ở cuối vẫn hợp lệ (`dataset_id newline -> True None`), do `re.search` với `$` tại `schema_validator.py` (kiểm `pattern`). Đáng mở việc riêng.
- Test `null value` và nhánh `bytes` hợp lệ đã đạt trên bản cũ theo logic code, nên chỉ là test khóa hành vi. Expected trong file test được viết tay, record mẫu lấy từ fixture, không gọi hàm đang test để sinh đáp án.
- Diff chỉ gồm 4 file (README domain, 2 validator, 1 file test mới). Không đổi `contracts/` hay `tests/fixtures/`, không sửa, xóa hay skip test cũ. Không thấy hardcode mới, float cho tiền, đọc ground truth, hay việc ngoài phạm vi.

### Fuzz
248.510 lời gọi qua `validate_record` (10 entity), `parse_and_validate_json_schema` và 7 hàm `validate_*` trực tiếp. Gồm thay hoặc xóa từng trường, khóa lạ, giá trị cực trị, và phá byte ngẫu nhiên. Chỉ còn hai loại ngoại lệ: `TypeError` (`schema_validator.py:281`) và `InvalidOperation` (sNaN).

### Output kiểm tra (trên worktree)
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
14 check groups passed.
ruff check: All checks passed!
ruff format --check: 116 files already formatted
```
`pytest tests/unit -q`: chạy đến 100%, phần đuôi tôi xem (`tail -6`) không có F hay E; dòng tổng số test không được in ra. Có log `Connection refused` tới `localhost:1` từ các test producer. `verify_contract.py` ghi `VERIFICATION_REPORT.json`; `git status` sau đó vẫn sạch.

### Chưa kiểm
- `pytest tests/integration` và `scripts/smoke.sh` (không đụng container).
- Chạy trong Spark executor, nơi độ sâu stack khác.
- Đầu vào rất lớn (bộ nhớ, thời gian; message lỗi chép nguyên `repr` của value).
- Registry hay schema tùy biến ngoài các ca đã nêu.
- CI của PR.
