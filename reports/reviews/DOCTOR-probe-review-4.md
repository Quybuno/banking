# DOCTOR-probe: review độc lập lượt 4 (diff b37bd64..3fa8d46)

Phần trên đường kẻ do người điều phối thêm; phần dưới đường kẻ là **nguyên văn** báo cáo của reviewer (subagent `spec-reviewer`), trích bằng script từ bản ghi phiên, không chỉnh sửa. Nội dung là nhận định của reviewer tại thời điểm viết, có thể chứa lỗi (ví dụ số dòng).

- Agent: `a96f83f930a7517c0`
- Thời điểm: 2026-11-25T07:14:34.600Z
- Lượt báo cáo: 1/1 của agent này
- SHA-256 phần nguyên văn: `e71eea9851160b54e3c142a504f92326fe5860cab8d7d0ce6ab62368cdf1f675`

---

## Review lượt 4: `git diff b37bd64..3fa8d46` (PR #11)

File: `/home/quybuno/banking/src/n06/cli/doctor.py`, `/home/quybuno/banking/src/n06/cli/README.md`, `/home/quybuno/banking/tests/unit/cli/test_doctor_secrets.py`.
Tái hiện trên bản sao 3fa8d46, mọi secret là giả: `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review4/scripts/exp.py` (Docker, trạng thái service và probe được giả) và `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review4/scripts/mutate.py`.

## Phải sửa

**1. Host của `object_store.endpoint` không được kiểm, chuỗi chứa mật khẩu vẫn được in** (`doctor.py:696-700`, in qua URL ở `:737`; `README.md:69`)
- `_HOST_PATTERN` chỉ áp cho Kafka và PostgreSQL.
- `N06__OBJECT_STORE__ENDPOINT="http://host=db password=SentinelPw9137:1"` cho rc=14:
  `[FAIL] service.minio | Service 'minio' (fake-minio) không phản hồi tại http://host=db password=sentinelpw9137:1/minio/health/live`
- `http://db;password=SentinelPw9137:1` cũng in `http://db;password=sentinelpw9137:1/minio/health/live`.
- README ghi "host không phải tên máy hay địa chỉ IP" là mã 10, sai với endpoint. Các chuỗi này không đúng cú pháp host nên không thuộc giới hạn đã biết.
- Lỗi lộ có thể có từ trước; diff này tuyên bố đã chặn nhưng bỏ sót. Gợi ý: kiểm `parsed_minio.hostname` bằng cùng mẫu.

**2. Che userinfo bỏ lọt mật khẩu chứa `/`, khoảng trắng, `'`, `"`** (`doctor.py:294`; `README.md:72`)
- Docker thật (`DOCKER_HOST` hỏng, lỗi ngay khi phân tích địa chỉ):
  `Failed to initialize: unable to resolve docker endpoint: parse "tcp://user:Sentinel/Pw9137@127.0.0.1:abc": invalid port ":Sentinel" after host`
- `_sanitize` trả nguyên văn cả 4 dạng, ví dụ `sanitize: tcp://user:Sentinel/Pw9137@127.0.0.1:1`. Đưa cùng chuỗi vào detail của `docker.engine` (giả) thì report in nguyên.
- README khẳng định phần `user:mat-khau@` bị che. Mật khẩu base64 thường có `/`. Gợi ý: nới regex tới `@` cuối trên cùng dòng (chấp nhận che dư, tôi chưa thử), hoặc ghi giới hạn vào README.

**3. Vẫn còn traceback khi không đọc được tệp cấu hình** (`doctor.py:1039`, `:769`; `README.md:73`)
- CLI thật, `.env` giả không phải UTF-8, `doctor --env-file ... --skip-services --json`: rc=1, `UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff in position 6: invalid start byte` kèm traceback.
- Gọi `run_doctor` với `.env` không có quyền đọc: `PermissionError`. Nguyên nhân: `parse_dotenv_file` được gọi ngoài mọi `try`.
- `test.toml` không có quyền đọc, profile mặc định: `PermissionError`. Nguyên nhân: `check_services` chỉ bắt `(ValueError, OverflowError)`.
- Trái mục tiêu "không văng traceback" và câu README "tệp không đọc được ... chỉ nêu tên loại lỗi". Không thấy secret trong các thông báo này. Có từ trước, nhưng diff chưa phủ.

## Nên sửa

**4. Mã thoát có thể đổi do lọc `check_id` trước khi xếp hạng (lỗi mới của diff)** (`doctor.py:1135`, `:1148-1150`)
- Xếp hạng so `check_id != "capacity.limits"` trên bản đã che.
- Cùng lỗi Docker và capacity: không có biến thêm thì rc=13; thêm `X_API_TOKEN=capacity` thì `exit_code=16`, vì `capacity.limits` bị đổi tên và che mất mã hạ tầng.
- Cũng do che `check_id`: `N06_OBJECT_STORE_ACCESS_KEY=minio` cho `[FAIL] service.***REDACTED***`, người đọc JSON theo `check_id` sẽ không tìm thấy `service.minio`.
- Cả hai cần giá trị secret hiếm gặp. Gợi ý: xếp hạng trên `checks` thô; cân nhắc chỉ che phần động của `check_id` (tên bucket).

**5. `README.md:68` sai một phần sau diff**
- Câu "tên host không mã hóa được đều là FAIL mã `14`": `N06__POSTGRES__MART__HOST=máy-chủ` nay là rc=10. Theo mẫu `{1,253}`, host Kafka/PostgreSQL dài hơn 253 ký tự cũng vậy. `a..b` vẫn là 14.
- Mẫu mới cũng từ chối IPv6 có zone (`fe80::1%eth0`, mã 10), đây có thể là cấu hình hợp lệ hiếm. `[::1]` và host có khoảng trắng đầu cũng bị từ chối. `::1` và `my_service.local.` vẫn được nhận.

**6. Test chưa bắt một số hồi quy** (`test_doctor_secrets.py`)
- Đột biến bỏ nhánh IPv6 của `_HOST_PATTERN`: 26 test vẫn pass. Bỏ `_` khỏi mẫu: vẫn pass. Chưa có ca khẳng định host hợp lệ (IPv6, tên service có `_`) được chấp nhận.
- Chưa có ca cho các mục 1, 2, 3, 4.

## Ghi nhận

- **Đột biến:** bản sạch 26 pass. Bị bắt (rc=1): bỏ `IGNORECASE`; bỏ che `check_id`; bỏ lọc summary; bỏ lọc detail; bỏ che userinfo; bỏ hoặc thu hẹp `except (OSError, ValueError)` (3 biến thể); bỏ kiểm host; bỏ miền cổng; chèn nội dung ngoại lệ vào thông báo. Sống sót: hai đột biến ở mục 6.
- **Regex không chậm bất thường:** `_URL_USERINFO` trên chuỗi tới 1 triệu ký tự tối đa 0,395 s; `_HOST_PATTERN` tối đa 0,016 s.
- **Endpoint không ghi cổng** (`http://localhost`, biến cổng trống): nay cả bốn service là mã 10, không probe service nào. Theo đọc mã, trước đây chỉ MinIO FAIL 14. `object_store.resolve_host_endpoint` vốn đã đòi cổng, và thông báo có nêu đúng nguyên nhân.
- **Header `profile`** (`DoctorReport.profile`) không qua bộ lọc: `run_doctor(profile=<secret đã biết>)` in nguyên ở header và JSON `"profile"`. Qua CLI không tới được vì `--profile` có `choices` (`cli/__init__.py:160`).
- **Bất biến:** diff chỉ đổi 4 file; không đổi `contracts/`, `tests/fixtures/`; không thấy test bị xóa, skip hay nới expected (helper chỉ thêm tham số và mock probe). Expected của test mới độc lập. `reports/reviews/` được `reports/README.md:13` cho phép (README đó ghi `CLAUDE.md` chưa sửa theo).
- `.venv/bin/python scripts/check_rules.py`: `check_rules: OK`, rc=0. (`python` trần không có trên PATH.)
- Repo sạch sau review (`git status --short` rỗng, HEAD 3fa8d46).

## Chưa kiểm

- `verify_contract.py` và toàn bộ `tests/unit` (theo yêu cầu); chỉ chạy `test_doctor_secrets.py` trên bản sao. Không chạy ruff.
- `tests/integration`, container thật, `.env` thật.
- Mật khẩu chứa khoảng trắng, `'`, `"` với Docker thật: lần thử khoảng trắng với timeout 10 s bị ngắt không có kết quả, chỉ kiểm qua `_sanitize` và Docker giả. Lần thử `/` đầu cũng bị ngắt, chạy lại với 40 s thì ra output ở mục 2.
- Không chạy bản b37bd64 để so hành vi cũ; nhận định "có từ trước" dựa trên đọc diff.
