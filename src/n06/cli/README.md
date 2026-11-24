# src/n06/cli/

**Mục đích:** Điểm vào dòng lệnh thống nhất `n06` của hệ thống (TASK T03: `init-local`, `doctor`, `--help`, bảng mã thoát; T03b: tạo và kiểm bucket object store).

**Người sở hữu:** C (Quý)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 10, mục 12 phần M0; `docs/adr/0001-bo-phien-ban.md`

**Quy tắc:**
- Mọi lệnh hỗ trợ `--help`, có structured output (`--json`) và trả về đúng mã thoát (`ExitCode`).
- Tuyệt đối không in giá trị secret/password ra output hoặc log.
- Không hardcode endpoint, hostname hay port trong mã nguồn; toàn bộ địa chỉ dịch vụ lấy từ hệ thống cấu hình T01 (`n06.config.load_config`) và biến môi trường `.env`.
- `object_store.py` chỉ chứa mã quản trị bucket của CLI (ký AWS SigV4 bằng thư viện chuẩn, `HEAD`/`PUT` bucket, `ensure_bucket`), không thêm dependency. Đây không phải thư viện S3 dùng chung: mã đọc/ghi dữ liệu (T22, T23, T25) không được import module này.
- Logic đánh giá tài nguyên máy chủ (`evaluate_system_resources`) được tách riêng thành hàm thuần túy nhận `SystemResourceSnapshot` để kiểm thử độc lập với máy chạy test.

## Bảng mã thoát (`ExitCode`)

| Mã thoát | Tên hằng số | Ý nghĩa |
|---|---|---|
| `0` | `OK` | Mọi kiểm tra hoặc thao tác thành công |
| `2` | `CLI_USAGE_ERROR` | Sai cú pháp dòng lệnh hoặc tham số CLI không hợp lệ |
| `10` | `CONFIG_ERROR` | Lỗi thiếu biến môi trường bắt buộc, thiếu file `.env`, hoặc cấu hình `config/*.toml` sai (`ConfigValidationError`) |
| `11` | `PYTHON_ENV_ERROR` | Phiên bản Python không thuộc nhánh `3.11.x` theo `ADR-0001` |
| `12` | `RESOURCE_ERROR` | Tài nguyên máy chủ (CPU cores, RAM, hoặc ổ đĩa trống) thấp hơn ngưỡng tối thiểu |
| `13` | `DOCKER_ERROR` | Thiếu Docker CLI, thiếu plugin `docker compose`, hoặc Docker daemon chưa chạy |
| `14` | `SERVICE_ERROR` | Một hoặc nhiều service hạ tầng (`kafka`, `minio`, `postgres`, `spark`) đang tắt, `unhealthy`, hoặc không mở kết nối port |

## Bucket object store (T03b, ADR-0002 mục 1)

- Chỉ tạo và kiểm bucket do cấu hình định nghĩa: đúng một bucket `object_store.bucket` của profile đang dùng (`default`/`demo`: `n06-lakehouse`, `benchmark`: `n06-benchmark-lakehouse`, `test`: `n06-test-lakehouse`; ghi đè được bằng `N06__OBJECT_STORE__BUCKET`). Tên bucket, region và credential lấy từ cấu hình T01, không hardcode.
- Địa chỉ truy cập từ host suy ra như `doctor` (host của profile `test`, cổng `N06_MINIO_API_HOST_PORT`), vì endpoint trong cấu hình thường là tên trong Docker network.
- `n06 init-local [--skip-buckets | --require-buckets]`: sau khi sinh `.env`, `HEAD` bucket rồi `PUT` nếu thiếu, nên chạy lại không đổi gì. Trạng thái từng bucket (`created`, `exists`, `unreachable`, `error`) có trong output text và JSON (`buckets`, `buckets_ready`).
- Mã thoát của `init-local`: `0` khi xong; `0` kèm cảnh báo khi MinIO chưa chạy (bucket `unreachable`), trừ khi có `--require-buckets` (khi đó `14`); `14` khi object store từ chối hoặc lỗi (ví dụ sai credential); `10` khi cấu hình hoặc tên bucket không hợp lệ.
- `n06 doctor` thêm hạng mục `bucket.<tên>` sau khi service MinIO ổn: thiếu bucket thì báo đúng tên và mã `14`, kèm gợi ý `n06 init-local --profile <profile>`. Không in secret: lỗi chỉ gồm mã HTTP và mã lỗi S3.

### Chuỗi lệnh trên máy mới

1. `n06 init-local --profile demo`: lần 1 sinh `.env`; MinIO chưa chạy nên bucket ở trạng thái `unreachable`, lệnh thoát `0` kèm cảnh báo.
2. `docker compose --profile core up -d --wait`: khởi động hạ tầng.
3. `n06 init-local --profile demo --require-buckets`: lần 2 giữ nguyên `.env`, tạo bucket; thoát `14` nếu bucket chưa sẵn sàng.
4. `n06 doctor --profile demo`: kiểm tra toàn bộ, gồm bucket.

`doctor` chạy khi MinIO đã chạy mà bucket chưa được tạo sẽ báo `bucket.<tên>` thiếu (mã `14`); chạy lại bước 3. Kịch bản nghiệm thu mẫu của đặc tả (Phần I mục 12) liệt kê `doctor` trước `init-local` và không có bước `compose up`, nên không dùng nguyên thứ tự đó trên máy mới. `tests/integration/test_doctor_compose.py` đã được sửa theo chuỗi này (gọi `init-local --require-buckets` sau `compose up`).

### Giới hạn chưa kiểm

- Chỉ hỗ trợ path-style (`object_store.path_style_access = true`); cấu hình `false` bị từ chối bằng lỗi `CONFIG_ERROR`.
- Chưa thử endpoint `https` (mã có nhánh `HTTPSConnection` nhưng chưa chạy với máy chủ thật).
- Mới thử region `us-east-1` và `eu-west-1` trên MinIO thật (`tests/integration/test_init_local_buckets.py`); region lấy từ cấu hình và dùng để ký. Chưa thử region nào khác và chưa thử trên AWS S3 thật.
- Phân loại lỗi truyền tải: chỉ từ chối kết nối hoặc không phân giải được tên mới là `unreachable` (thoát `0` kèm cảnh báo); timeout, phản hồi không phải HTTP, lỗi TLS hoặc ngắt kết nối là `error` (thoát `14`).
- Cấu hình không an toàn (`N06_MINIO_API_HOST_PORT` ngoài 1-65535, region hoặc credential chứa ký tự điều khiển/khoảng trắng) thoát `10`, thông báo chỉ nêu tên trường, không in giá trị.
