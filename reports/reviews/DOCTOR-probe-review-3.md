# DOCTOR-probe: review độc lập lượt 3 (diff 86f612a..b37bd64)

Phần trên đường kẻ do người điều phối thêm; phần dưới đường kẻ là **nguyên văn** báo cáo của reviewer (subagent `spec-reviewer`), trích bằng script từ bản ghi phiên, không chỉnh sửa. Nội dung là nhận định của reviewer tại thời điểm viết, có thể chứa lỗi (ví dụ số dòng).

- Agent: `a22f1c18172ee7e58`
- Thời điểm: 2026-11-25T07:03:49.427Z
- Lượt báo cáo: 1/1 của agent này
- SHA-256 phần nguyên văn: `d9dfbc62b6dbdb83950e02ca80f7ad3a5a8e9e4975a8f4774eeaba9a4412f723`

---

## Review lượt 3: `git diff 86f612a..b37bd64` (PR #11)

**Kết luận: chưa đạt yêu cầu "không in secret ở bất kỳ đường lỗi nào".** Các dạng mà test mới phủ đã bị chặn, nhưng còn nhiều đường lộ. Theo đọc diff, các đường dưới đây có từ trước, không do b37bd64 tạo ra (tôi không chạy lại ở 86f612a). Mọi secret đều là giả.

File: `/home/quybuno/banking/src/n06/cli/doctor.py`, `/home/quybuno/banking/src/n06/cli/README.md`, `/home/quybuno/banking/tests/unit/cli/test_doctor_secrets.py`.
Script tái hiện (chạy trên bản sao): `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review3/scripts/leak.py`, `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review3/scripts/mutate.py`, `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review3/scripts/empty_port.py`.

## Phải sửa

**1. Giá trị `N06_PROFILE` / `N06__PROFILE` bị in nguyên (text và JSON)**
- `doctor.py:295-296` in `exc.reason`, mà reason của trường `profile` chứa đường dẫn ghép từ giá trị (`/home/quybuno/banking/src/n06/config.py:402`, `:831`).
- Tái hiện bằng CLI thật: `N06_PROFILE=SentinelPw9137 python -m n06.cli doctor --env-file <env giả> --skip-services --json`, rc=10:
  `"summary": "Cấu hình không hợp lệ: Lỗi cấu hình tại trường 'profile': Không tìm thấy file cấu hình '.../config/SentinelPw9137.toml'."`
- `N06__PROFILE` và giá trị có dấu `/` cũng lộ.

**2. Profile dài thì traceback chứa giá trị**
- `doctor.py:311` chỉ bắt `ConfigValidationError` và `FileNotFoundError`; `path.is_file()` trong `_load_toml_file` ném `OSError`.
- `N06_PROFILE` = "SentinelPw9137" lặp 30 lần, cả text và `--json`, rc=1, stderr:
  `OSError: [Errno 36] File name too long: '.../config/SentinelPw9137SentinelPw9137...`
- Sửa cho 1 và 2: với `field_path == "profile"` dùng câu cố định, không dùng `reason`; bắt `OSError` và chỉ in tên loại lỗi.

**3. `object_store.bucket`: in giá trị sai, và `check_id` không qua bộ lọc secret**
- `doctor.py:855`, `:876`; `/home/quybuno/banking/src/n06/cli/object_store.py:191` trích `{name!r}`; vòng lọc cuối giữ `check_id=item.check_id` (`doctor.py:1039`).
- Điều kiện: `service.minio` PASS (tôi dùng MinIO giả trong tiến trình).
- `N06__OBJECT_STORE__BUCKET=SentinelPw9137`:
  `[FAIL] bucket.SentinelPw9137 | Cấu hình bucket 'SentinelPw9137' không hợp lệ`
  `-> Chi tiết (exit=10): Tên bucket 'SentinelPw9137' không hợp lệ: ...`
- Đặt bucket bằng đúng secret key (do init-local giả sinh, bộ lọc có biết) vẫn lộ:
  `[FAIL] bucket.n06_g6meVdikeFxfxMFBUaOvGrLry75bhYmB | Cấu hình bucket '***REDACTED***' không hợp lệ`
  JSON: `"check_id": "bucket.n06_g6meVdikeFxfxMFBUaOvGrLry75bhYmB",`
- Tên hợp lệ chữ thường cũng được in khi bucket thiếu (xem mục "Cần chủ dự án quyết").

**4. Địa chỉ chứa mật khẩu mà không có `@` vẫn được in trong tên đích**
- `doctor.py:657` chỉ chặn `@`; tên đích ghép ở `:706` trở đi và in ở `:819`. Điều kiện: container running/healthy (tôi giả trạng thái, cổng đóng).
- `N06__POSTGRES__MART__HOST="host=db user=n06 password=SentinelPw9137"`:
  `[FAIL] service.postgres   | Service 'postgres' (postgres) không phản hồi tại host=db user=n06 password=SentinelPw9137:1`
  `service.spark` cũng in vì dùng chung host.
- `jdbc:postgresql://db/n06?user=n06&password=SentinelPw9137`: lộ y như trên.
- `user:SentinelPw9137＠db` (dấu `＠` U+FF20): lộ.
- Secret key (đã biết) đặt làm host của endpoint bị `urlparse` hạ chữ thường nên bộ lọc không khớp: `http://n06_g6mevdikefxfxmfbuaovgrlry75bhymb:1/minio/health/live`.
- Gợi ý: kiểm host theo mẫu ký tự của hostname/IP (sai thì mã 10, chỉ nêu tên trường); lọc secret không phân biệt hoa thường.

**5. Biến cổng là số ngoài miền vẫn bị trích giá trị**
- Đây đúng đường "biến `N06_*_HOST_PORT` (trích giá trị)" đã nêu; bản sửa mới xử lý trường hợp không phải số. `doctor.py:572-575` và URL ở `_probe_http`.
- Cả bốn biến, ví dụ `N06_POSTGRES_HOST_PORT=913700012345`:
  `-> Chi tiết (exit=14): Cổng ngoài miền 1-65535: 127.0.0.1:913700012345`
- Chỉ lộ khi mật khẩu toàn chữ số. Gợi ý: kiểm 1-65535 trong `host_port` (đổi sang mã 10 thì sửa README:68-69).

**6. `limits.disk_probe_path` bị in trong `capacity.limits`**
- `/home/quybuno/banking/src/n06/limits.py:227`, `doctor.py:955-956`.
- CLI thật, `N06__LIMITS__DISK_PROBE_PATH=/nonexistent/SentinelPw9137 ... --skip-services`, rc=16:
  `[FAIL] capacity.limits    | Chỗ trống ? (đọc theo limits.disk_probe_path, /nonexistent/SentinelPw9137); ...`
  `... không đọc được chỗ trống ([Errno 2] No such file or directory: '/nonexistent/SentinelPw9137')`

## Nên sửa

**7. README:70 nói quá mức mã làm** (`README.md:70`)
- "không in giá trị sai, chỉ tên trường/biến và lý do": sai với mục 1, 3, 5, 6.
- "Địa chỉ ... không được chứa `@`": chỉ kiểm trong `check_services`, và chỉ trên cấu hình profile `test`. `N06__OBJECT_STORE__ENDPOINT=http://user:SentinelPw9137@127.0.0.1:1 ... --profile test --skip-services` cho `[PASS] config.schema`, `Kết quả: PASS (có 1 cảnh báo...)`, rc=0 (không in mật khẩu). Theo đọc mã, Docker hỏng thì cũng không kiểm, và `init-local` vẫn nhận địa chỉ này.

**8. README:69 "Biến cổng rỗng dùng giá trị mặc định" sai với kiểm bucket** (có từ trước)
- `object_store.py:218-219` không coi rỗng là mặc định. `N06_MINIO_API_HOST_PORT=""` với MinIO giả:
  `[PASS] service.minio ...`
  `[FAIL] bucket.n06-test-lakehouse | Cấu hình bucket 'n06-test-lakehouse' không hợp lệ`
  `-> Chi tiết (exit=10): N06_MINIO_API_HOST_PORT phải là số nguyên từ 1 đến 65535`

**9. `DOCKER_HOST` hỏng làm in mật khẩu** (`doctor.py:397`, có từ trước, cần cấu hình sai hiếm gặp)
- stderr của `docker info` được chép nguyên. `DOCKER_HOST=tcp://user:SentinelPw9137@127.0.0.1:abc`:
  `-> Chi tiết (exit=13): Failed to initialize: unable to resolve docker endpoint: parse "tcp://user:SentinelPw9137@127.0.0.1:abc": invalid port ":abc" after host`
- `DOCKER_HOST` đúng cú pháp có userinfo thì Docker không in mật khẩu. Nhánh `compose version` (`:378`) cũng chép stderr, chưa tái hiện được lộ ở đó.

**10. Test chưa phủ các đường trên**
- `test_doctor_secrets.py:43` luôn giả `_get_compose_service_states` trả `{}`, nên không ca nào đi tới chỗ in tên đích hay bucket; đó là đường lộ gốc "in trong tên đích". Các ca `@` chỉ bắt hồi quy qua mã 10 và tên trường.
- Chưa có ca cho profile, bucket, cổng số ngoài miền, disk_probe_path.
- Đột biến M12 (bỏ `redact_secrets` ở vòng lọc cuối của summary/detail) vẫn qua toàn bộ `tests/unit/cli` (rc=0).

## Ghi nhận

- **Test mới bắt được hồi quy của phần đã sửa:** M0 sạch 12 pass; M1-M11 (trả `str(exc)` ở hai nơi, bỏ kiểm `@` cho từng trường hoặc cả ba, trích giá trị biến/mặc định/lỗi urlparse/địa chỉ) đều rc=1.
- **Đường đã kiểm, không lộ:** from_date, timezone, key_strategy, trường lạ, region, credential có khoảng trắng, bốn biến cổng không phải số, endpoint `%40`, IPv6 hỏng, NFKC, generator_output_dir dài, mật khẩu Postgres đã biết đặt làm host, khóa credential trong TOML (test mới).
- **Traceback không chứa secret:** `N06__KAFKA__NUM_PARTITIONS` dài 5000 chữ số gây `ValueError: Exceeds the limit (4300 digits)` không bắt (`config.py:349`).
- **Hành vi đổi:** thông báo lỗi cấu hình không còn giá trị sai (đúng chủ đích); địa chỉ có `@` nay là mã 10 trong kiểm service. Không thấy cấu hình hợp lệ nào bị từ chối (các TOML đã đọc không có `@`; unit test pass).
- **Cần chủ dự án quyết:** một mật khẩu trần đặt vào host hoặc bucket (đúng cú pháp) không phân biệt được với giá trị thật, nên vẫn bị in (`SentinelPw9137:1`, `bucket.sentinelpw9137`). Yêu cầu "không in ở bất kỳ đường lỗi nào" va với spec dòng 492 "`doctor` báo thiếu gì cụ thể" và README:36 "thiếu bucket thì báo đúng tên". Theo CLAUDE.md nên chốt bằng ADR: hoặc không in giá trị cấu hình, hoặc chấp nhận giới hạn này và ghi vào README. Spec dòng 227: "Log có cấu trúc, không ghi secret hay toàn bộ payload hàng loạt."
- **PR #11 chưa có commit này:** `gh pr view 11` cho head `86f612a`; nhánh cục bộ ahead 1.
- **Phạm vi:** so với merge-base, `pyproject.toml`, `uv.lock`, `contracts/`, `tests/fixtures/` không đổi. `tests/` chỉ thêm 2 file, file mới không có skip/xfail. Expected của test mới độc lập (chuỗi đánh dấu, mã 10, tên trường). Diff lượt này không thêm hardcode endpoint/ngày/seed, không float tiền, không đọc ground truth. Tôi không thấy thẻ task DOCTOR trong `docs/TASKS.md`/`PROGRESS.md` (grep "DOCTOR|probe" rỗng) nên không đối chiếu điều kiện hoàn thành.

### Output kiểm tra
```
$ python scripts/check_rules.py
check_rules: OK                      (rc=0)
$ python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py
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
14 check groups passed.              (rc=0)
$ ruff check --no-cache .            All checks passed!
$ ruff format --no-cache --check .   114 files already formatted
$ pytest tests/unit (bản sao)        641 passed in 138.54s
```
`verify_contract.py` ghi lại `VERIFICATION_REPORT.json`; `git status` sau đó sạch. Tôi không sửa file nào khác trong repo; đột biến và pytest chạy trên bản sao `git archive` trong scratchpad.

### Chưa kiểm
- `tests/integration`, Docker Compose và container thật (trạng thái service được giả; không dừng/khởi động gì).
- `.env` thật (không đọc).
- Endpoint https, Windows gốc, `DOCKER_CONTEXT`/`DOCKER_CONFIG` hỏng, lỗi quyền đọc file.
- `init-local` về lộ secret; `generate`/`publish` (đã biết, ngoài phạm vi).
- CI trên GitHub.
