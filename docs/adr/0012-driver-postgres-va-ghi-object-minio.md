# ADR-0012: Driver PostgreSQL cho Spark (T22) và cách ghi object lên MinIO (T23)

- Trạng thái: Proposed
- Người viết: agent (soạn theo yêu cầu của chủ dự án để mở khóa T22 và T23), 2026-11-25
- Người duyệt: A (người sở hữu T22, T23, image Spark theo ADR-0001), C (Compose, cấu hình), B (control store)
- Ngày: 2026-11-25
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 6, 8.1; Phần II mục 4, 7

Chưa được sửa `pyproject.toml`, `uv.lock` hay `infra/spark/Dockerfile` trước khi ADR này chuyển Accepted.
Hai quyết định dưới đây độc lập, có thể duyệt riêng.

## Bối cảnh

Đặc tả mục 8.1 bước 3: *"Sau khi mọi output của batch đã ghi xong, upsert một dòng vào bảng control `ingest_batches` [...] Callback chỉ trả thành công sau bước này."* Callback `foreachBatch` chạy trong driver Python của Spark nên driver phải ghi được PostgreSQL.

Hiện trạng kiểm tra ngày 2026-11-25:

- Image `n06-spark:3.5.8-py311` không có `psycopg` lẫn `psycopg2`.
- **`psycopg` không có trong `pyproject.toml` hay `uv.lock`, và không cài trên máy host.** Vì vậy chưa có "phiên bản đã ghim ở host" để image dùng trùng; quyết định này phải thêm cùng một phiên bản vào cả hai nơi.
- **Control store chưa có upsert cho `ingest_batches`.** `src/n06/storage/control_store.py` chỉ có `list_committed_ingest_batches`; các câu `ON CONFLICT` hiện có là của `current_snapshot`.
- **Đường `psycopg` của control store (`PsycopgControlStore`) chưa từng chạy với PostgreSQL thật.** Test tích hợp chạy Postgres thật đi qua `PsqlCliControlStore` bằng `docker compose exec ... psql` (host không có `psql`). Bằng chứng "đã kiểm với Postgres thật" thuộc về đường `psql`, không thuộc đường `psycopg`.
- `connect_control_store` và `n06.control.migrations` đã ưu tiên `psycopg`, rồi `psycopg2`, rồi `psql`.

## Quyết định 1: thêm `psycopg` ghim phiên bản

Thêm vào `pyproject.toml` (`dependencies`), `uv.lock` và image Spark **cùng một bộ phiên bản** (kết quả `uv pip compile` cho Python 3.11, linux x86_64, ngày 2026-11-25):

| Gói | Phiên bản |
|---|---|
| `psycopg[binary]` (`psycopg`) | `3.3.9` |
| `psycopg-binary` | `3.3.9` |
| `typing-extensions` | `4.16.0` |

Trong image, cài bằng `pip install --require-hashes` từ một tệp yêu cầu sinh ra từ `uv.lock` (ví dụ `uv export`), để host và image không lệch nhau.

Lý do:

1. Truy vấn tham số hóa qua DB-API, thay vì ghép chuỗi SQL như đường `psql` (`_escape_literal`).
2. Một transaction ngay trong driver, không tạo tiến trình con cho mỗi batch và không đưa mật khẩu qua `PGPASSWORD`.
3. Dùng lại giao diện `ControlStore` và lớp `PsycopgControlStore` đã viết, vốn đã là lựa chọn đầu tiên của mã hiện có.

Lý do **không** được dùng để biện minh: "mã upsert đã kiểm với Postgres thật" (xem Bối cảnh: chưa đúng).

Điều kiện bắt buộc khi hiện thực (thuộc T22 hoặc một task mở đường):

- Chạy test PostgreSQL hiện có qua đường `psycopg` trên Postgres thật trước khi viết mã mới. `tests/integration/test_commit_postgres.py` hiện bắt `except Exception` rồi chuyển sang `psql`, và có `pytest.skip`: phải sửa để lỗi của đường `psycopg` không bị che.
- Viết upsert `ingest_batches` với `ON CONFLICT (query_name, batch_id) DO UPDATE` (ADR-0003) và test trên Postgres thật, gồm ca gọi hai lần cùng khóa.

### Phương án đã loại

- **(b) JDBC driver của PostgreSQL trong image.** Thêm một JAR phải ghim phiên bản và checksum. `DataFrameWriter` JDBC của Spark không có upsert `ON CONFLICT`, nên vẫn phải tự gọi JDBC qua py4j trong driver: một đường truy cập DB thứ hai, không dùng lại control store bằng Python.
- **(c) Chạy driver Spark trên máy host.** Cần thêm `pyspark` và Java đúng phiên bản trên host làm phụ thuộc; bộ JAR đã ghim (ADR-0001) chỉ có trong image nên môi trường chạy lệch khỏi ma trận tương thích; executor trong mạng Docker phải kết nối ngược về driver trên host.
- **(d) Cài `postgresql-client` vào image và dùng đường `psql`** (không nằm trong ba lựa chọn ban đầu, nêu để người duyệt biết). Đây là đường **duy nhất đã có bằng chứng** trên Postgres thật. Không chọn vì vẫn là phụ thuộc mới của image, SQL ghép chuỗi, một tiến trình con mỗi batch. Nếu người duyệt ưu tiên mã đã kiểm hơn các điểm này thì (d) là lựa chọn hợp lệ.

### Tác động

- `docs/compatibility-matrix.md`: thêm ba dòng gói Python ở trên kèm SHA-256 của wheel (lấy từ PyPI khi hiện thực) và cách kiểm lại; ghi rõ chúng nằm cả ở host lẫn image, khác dòng `confluent-kafka` (chỉ host).
- Build image: thêm một lớp `pip install` sau các JAR; build cần truy cập PyPI (ADR-0001 chỉ cấm tải lúc chạy job). Phải dựng lại image (`docker compose --profile core build spark`) trên mọi máy; kiểm bằng `docker run --rm n06-spark:3.5.8-py311 python3 -c "import psycopg"`.
- Giấy phép `psycopg` là LGPL-3.0.
- Khi `psycopg` được cài trên host, `connect_control_store` và migration **tự chuyển** từ `psql` trong container sang `psycopg`: hành vi của test hiện có thay đổi.
- Chưa quyết ở ADR này: cách đưa mã `n06` và `contracts/` vào container và cách khởi chạy `n06 stream` (thuộc kế hoạch T22).

## Quyết định 2: ghi object lên MinIO từ Python (T23)

T23 chạy trên host, đưa file sao kê lên MinIO theo thứ tự file dữ liệu, manifest, rồi biên nhận `_ingest/`; tải lại cùng khóa chỉ được khi `sha256` giống (ADR-0002 mục 4).

### Số đo file sao kê của dataset do generator sinh

Cấu hình `generator.demo-faults.json` (3 ngày, `statement_file_window_seconds = 3600`, hai kênh), đo ngày 2026-11-25:

| Số giao dịch | Số file | Tổng | B mỗi dòng | Trung vị | p95 | Lớn nhất (dòng) |
|---:|---:|---:|---:|---:|---:|---:|
| 20.000 | 152 | 7,3 MB | 364,8 | 37,6 KB | 78,4 KB | 91,2 KB (250) |
| 50.000 | 152 | 18,2 MB | 364,8 | 85,6 KB | 185,0 KB | 215,3 KB (590) |

Mỗi file có một manifest 285 byte trở xuống. **Ngoại suy tuyến tính, chưa đo:** ở 1.000.000 giao dịch file lớn nhất khoảng 4,3 MB, tổng khoảng 365 MB; ở 5.000.000 khoảng 21,5 MB, tổng khoảng 1,8 GB. Số file giữ nguyên 152 vì theo cửa sổ và kênh. Nếu đặt cửa sổ 86.400 giây, file lớn nhất tăng khoảng 24 lần (khoảng 0,5 GB ở 5.000.000).

Giới hạn của S3: một lần `PUT` tối đa 5 GiB; multipart yêu cầu mỗi phần (trừ phần cuối) tối thiểu 5 MiB. Mọi mức ở trên nằm xa dưới 5 GiB, nên **không cần multipart**.

### Phương án A: mở rộng bộ ký SigV4 bằng thư viện chuẩn

Thêm `PUT` object (và `HEAD` object) cạnh `src/n06/cli/object_store.py`.

- Phải viết: gửi thân yêu cầu theo luồng với `Content-Length`; tính SHA-256 của payload trước khi gửi (bộ ký đã nhận `payload_sha256`); lưu `sha256` vào metadata và đọc lại bằng `HEAD` để giữ tính bất biến; ánh xạ lỗi và thử lại. Nếu cần liệt kê (`_ingest/`, hoặc `require_unused_run_prefix` của T25) thì phải tự phân trang `ListObjectsV2` và phân tích XML.
- Docstring hiện tại cấm rõ: *"Đây không phải thư viện S3 dùng chung: mã đọc/ghi dữ liệu (T22, T23, T25) không được dựa vào module này."* Chọn A là **đổi quy tắc đó**; nên chuyển phần ký ra module trung lập thay vì để T23 nhập từ `cli`.
- Không thêm phụ thuộc. Không có multipart; đối tượng lớn hơn giới hạn một lần `PUT` sẽ không ghi được.
- Kiểm thử: bộ ví dụ ký của AWS hiện có đã gồm một `PUT` có thân; thêm test với máy chủ S3 giả (đã có) và test tích hợp MinIO thật (ghi, đọc lại, so `sha256`, ghi đè khác nội dung bị từ chối, file lớn).

### Phương án B: thêm client S3 (`boto3`)

- Ghim `boto3 1.44.11` cùng `botocore`, `s3transfer`, `jmespath`, `python-dateutil`, `six`, `urllib3` (bảy gói, giải ngày 2026-11-25) vào `pyproject.toml` và `uv.lock`; thêm vào ma trận tương thích. Chỉ host, trừ khi T25 cần trong image.
- Có sẵn multipart, thử lại, phân trang liệt kê, `HeadObject`. Ít mã tự bảo trì hơn.
- Rủi ro **chưa kiểm**: các bản `boto3` từ 1.36 đổi mặc định về checksum toàn vẹn, một số kho tương thích S3 không chấp nhận; phải kiểm với image MinIO đã ghim và có thể phải cấu hình lại.
- Kho mã sẽ có hai đường gọi S3 (quản trị bucket bằng bộ ký tự viết, ghi dữ liệu bằng `boto3`).
- Kiểm thử: `botocore.stub.Stubber` cho unit test, MinIO thật cho tích hợp.

(`minio` SDK cũng làm được nhưng kéo theo chín gói, có gói biên dịch mã hóa; không đề xuất.)

### Đề xuất của người viết

**A**, với điều kiện T23 chỉ cần `PUT` và `HEAD`. Lý do: kích thước file nhỏ so với giới hạn một lần `PUT`, bộ ký đã chạy với MinIO thật, không thêm phụ thuộc. Nếu T23 hoặc T25 cần liệt kê object và ghi nhiều đối tượng lớn từ host thì B đáng chi phí hơn; người duyệt quyết.

## Hệ quả chung

- Sau khi Accepted: một task cập nhật phụ thuộc, image và ma trận; T22 phụ thuộc quyết định 1, T23 phụ thuộc quyết định 2.
- Nếu chỉ một quyết định được duyệt, task tương ứng được mở khóa riêng.
