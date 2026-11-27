# PR #14 (không in giá trị trường bị từ chối): review hẹp, một vòng

Phần trên đường kẻ do người điều phối thêm; phần dưới đường kẻ là **nguyên văn** báo cáo của reviewer (subagent `spec-reviewer`), trích bằng script từ bản ghi phiên, không chỉnh sửa. Nội dung là nhận định của reviewer tại thời điểm viết, có thể chứa lỗi (ví dụ số dòng).

- Agent: `a114f5edb29c2bdf4`
- Thời điểm: 2026-11-27T08:14:27.359Z
- Lượt báo cáo: 1/1 của agent này
- SHA-256 phần nguyên văn: `1f6cb5c3801eee8fcd1e3f53326732ed095bbba53dd18a49758aea9756b2f655`

---

Review hẹp PR #14, commit 8168a92, phạm vi `git diff origin/main...8168a92 -- src tests`. Không sửa file trong repo (`git status --short` rỗng). Chỉ dùng secret giả, không đọc .env, không đụng container (docker và probe được giả lập khi chạy doctor). Script tái hiện nằm trong `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review14b/` (`r.py`, `r2.py`).

Lệnh tái hiện (chạy `r.py` trước, `r2.py` sau):
`cd /tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review14b && env -i PATH="$PATH" HOME="$HOME" PYTHONDONTWRITEBYTECODE=1 /home/quybuno/banking/.venv/bin/python r.py` (rồi `r2.py`). Giá trị giả là `FAKEpw123`.

Tóm tắt: A: CÓ, còn đường in. B: không tìm thấy đường in. C: không thấy lỗi nghiêm trọng, có vài điểm ghi nhận.

## Phải sửa (đều thuộc A)

**1. `generate` in giá trị trường bị từ chối của cấu hình generator, cả text và `--json`.** Đây là đường dễ gặp nhất.
- Vị trí: `/home/quybuno/banking/src/n06/domain/schema_validator.py:231, 262, 342` tạo thông báo có `{value!r}`; `/home/quybuno/banking/src/n06/generator/dataset.py:229-230` ghép `i.message`; `/home/quybuno/banking/src/n06/cli/generate.py:129` in `str(exc)`.
- Tái hiện: sao `generator.demo-clean.json`, đặt `seed` (hoặc `currency`, `from_date`, `account_selection`, `account_count`) = `"FAKEpw123"`, gọi `run_generate`.
```
rc 10 {"ok": false, "error": "Cấu hình generator vi phạm ràng buộc: SCHEMA_TYPE@$.seed: Sai kiểu dữ liệu tại '$.seed': yêu cầu integer, nhận 'FAKEpw123'."}
[generate] Lỗi: ... UNSUPPORTED_CURRENCY@$.currency: Giá trị tại '$.currency' ('FAKEpw123') không thuộc danh sách hợp lệ ['VND', 'USD'].
[generate] Lỗi: ... SCHEMA_VALUE@$.from_date: Giá trị tại '$.from_date' ('FAKEpw123') sai định dạng format='date'.
```
- Trái với README mới của cli ("Quy tắc: không in giá trị của trường cấu hình bị từ chối... Áp dụng cho doctor, generate, publish"). Gợi ý: ở CLI chỉ in `code@field_path`.

**2. `doctor` in tên bucket không hợp lệ khi có thêm một lỗi object store khác.**
- `/home/quybuno/banking/src/n06/cli/doctor.py:928-967`: `target_from_config` (dòng 931) chạy trước khi tên bucket được kiểm (việc kiểm nằm trong `head_bucket`), nên `except ObjectStoreConfigError` (957) in `bucket` chưa kiểm vào `check_id` và `summary`.
- Tái hiện qua `run_doctor` đầy đủ (docker và probe giả): `N06__OBJECT_STORE__BUCKET=Bad_FAKEpw123`, `N06_MINIO_API_HOST_PORT=` (rỗng; README nói kiểm service vẫn qua còn kiểm bucket báo mã 10).
```
rc 10
[PASS] service.minio      | Service 'minio' (minio): state=running, health=healthy, endpoint=http://localhost:19000/minio/health/live
[FAIL] bucket.Bad_FAKEpw123 | Cấu hình bucket 'Bad_FAKEpw123' không hợp lệ
```
- Gọi trực tiếp `check_object_store_buckets` với `N06__OBJECT_STORE__REGION="bad region"` cũng in tên bucket. Gợi ý: gọi `validate_bucket_name` trước `target_from_config`.

**3. `publish` in giá trị cấu hình bị broker từ chối, cả `--json`.** Code không đổi trong diff.
- `/home/quybuno/banking/src/n06/producer/kafka_client.py:183, 197-202` (admin giả, không chạy broker thật):
```
rc 14 {"ok": false, "error": "Không tạo được topic bad topic FAKEpw123: KafkaError{code=TOPIC_EXCEPTION,...}"}
rc 14 [publish] Lỗi: Không lấy được metadata của topic bad topic FAKEpw123 (lỗi KafkaError{code=TOPIC_EXCEPTION,...}).
rc 10 [publish] Lỗi: Cụm broker từ chối tạo topic n06.banking.events.v1 với 3 partition và replication_factor=31337 (...)
```
- `kafka_client.py:169`, `N06__KAFKA__BOOTSTRAP_SERVERS=user:FAKEpw123@127.0.0.1:1` (client thật, cổng 1). `doctor` coi địa chỉ có `@` là không hợp lệ, còn `publish` vẫn nhận rồi in, và đây không phải "host đúng cú pháp" trong giới hạn đã khai:
```
rc 14 [publish] Lỗi: Không kết nối được broker user:FAKEpw123@127.0.0.1:1: KafkaError{code=_TRANSPORT,...}
stderr: %3|...|FAIL|n06-publish#producer-3| [thrd:[user:FAKEpw123@127.0.0.1:1]:9092/bootstrap]: ... Failed to resolve ...
```
  Dòng stderr do librdkafka tự ghi, nên chỉ sửa thông báo là chưa đủ; cần từ chối `@` trước khi tạo client.

**4. Đường dẫn bị từ chối (tên quá dài) vẫn bị in, `generate` văng traceback.**
- `N06__PATHS__GENERATOR_OUTPUT_DIR=FAKEpw123` + 300 ký tự `a`.
- `generate`: `/home/quybuno/banking/src/n06/cli/generate.py:32` (`candidate.exists()`, gọi từ dòng 108): `TRACEBACK: OSError: [Errno 36] File name too long: '.../FAKEpw123aaaa...'`
- `publish`: `/home/quybuno/banking/src/n06/cli/publish.py:183-184`: `rc 10 [publish] Lỗi: Không đọc được dữ liệu nguồn: [Errno 36] File name too long: 'FAKEpw123aaaa.../source/manifests'`
- Tình huống ít gặp.

**5. (Mức thấp, chỉ lộ một phần) `doctor` in phần trước dấu `:` của endpoint sai.**
- `/home/quybuno/banking/src/n06/cli/doctor.py:764-765`. `N06__OBJECT_STORE__ENDPOINT=FAKEpw123:9000`:
```
[FAIL] service.minio | Service 'minio' (minio) không phản hồi tại fakepw123://localhost:1/minio/health/live   (rc 14)
```
- Gợi ý: chỉ nhận scheme `http`/`https`, không thì mã 10 và không in.

## Nên sửa
- Thêm test cho các đường 1-4 ở trên.

## Ghi nhận
- B: đọc `doctor.py:270-279` và phần che trong `build_doctor_report`, không tìm thấy đường in giá trị secret từ 4 ký tự. Hai khe hở lý thuyết, chưa tái hiện: secret dưới 4 ký tự không bị che (README có nêu ngưỡng, danh sách giới hạn bạn gửi thì không); trường `profile` ở tiêu đề và JSON (`doctor.py:1098, 1199`) không qua `_sanitize`, chỉ lộ nếu secret trùng một tên profile được chấp nhận hoặc tham số `--profile`.
- C, mã thoát (đã khai trong README): endpoint không có cổng và không đặt `N06_MINIO_API_HOST_PORT` nay là mã 10 cho cả bốn service (đã chạy `_resolve_host_endpoints`). Theo đọc diff, trước đây cổng 0 đi tiếp tới probe (mã 14, riêng minio); hành vi cũ tôi không chạy lại.
- C, `_HOST_PATTERN` nay từ chối host Unicode (`postgres.mart.host=bücher.example` bị mã 10); IPv6 `::1` vẫn được nhận. Chưa kiểm code cũ có kết nối được host IDN thật không; với môi trường Docker cục bộ khó gặp.
- C, test: so với origin/main chỉ thêm 2 file test mới; không có thay đổi trong `contracts/` hay `tests/fixtures/`. Trong nội bộ nhánh (`dbc50d5..8168a92`), `tests/unit/cli/test_doctor_secrets.py` có sửa hai test đã commit: (a) test bucket trùng secret đổi sang secret đúng quy tắc tên bucket và assert `bucket.***REDACTED***`; (b) `test_every_printed_field_of_a_check_is_redacted` đổi đầu vào `service.{KNOWN_SECRET}` thành `bucket.{KNOWN_SECRET}` vì code nay chỉ che `check_id` tiền tố `bucket.`. (b) là thu hẹp bảo đảm đã test; tôi không thấy `check_id` nào khác lấy từ cấu hình, nhưng người duyệt nên biết.
- `generate` với đường dẫn đúng cú pháp nhưng không tạo được (`/proc/FAKEpw123`) in đường dẫn, mã 12. Tôi xếp vào giới hạn "đường dẫn đúng cú pháp".
- Đã thử, không lộ: `doctor --json --skip-services` với `disk_probe_path` không tồn tại hoặc quá dài, `generator_output_dir` quá dài dưới `/mnt/d`, `N06_PROFILE` quá dài; bucket sai đơn lẻ ra `bucket.invalid_name`.

## Chưa kiểm
- Broker Kafka thật cho mục 3 (topic, replication_factor dùng admin giả).
- `DOCKER_HOST` với docker thật (chỉ đọc code: hai chỗ in stderr đều qua `_docker_error_detail`; stderr của `compose ps` bị bỏ).
- Thông báo lỗi của `tomllib` (có thể trích tên khóa hoặc một ký tự); các lỗi khác của generator ngoài những trường đã thử, gồm traceback do `build_dataset`.
- Không chạy verify_contract, unit hay integration theo yêu cầu.
