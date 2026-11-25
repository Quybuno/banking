# ADR-0012: Driver PostgreSQL cho Spark (T22) và cách ghi object lên MinIO (T23)

- Trạng thái: Proposed
- Người viết: agent (soạn theo yêu cầu của chủ dự án để mở khóa T22 và T23), 2026-11-25
- Người duyệt: A (T22, T23; image Spark theo `infra/README.md`), C (Compose, cấu hình, `src/n06/cli/`), B (`src/n06/storage/`: control store, object store)
- Ngày: 2026-11-25
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 2, 3, 6, 8.1, 12; Phần II mục 4, 7

Chưa được sửa `pyproject.toml`, `uv.lock` hay `infra/spark/Dockerfile` trước khi ADR này chuyển Accepted.
Hai quyết định dưới đây độc lập, có thể duyệt riêng.

## Bối cảnh

Đặc tả mục 8.1 bước 3: *"Sau khi mọi output của batch đã ghi xong, upsert một dòng vào bảng control `ingest_batches` [...] Callback chỉ trả thành công sau bước này."* Callback `foreachBatch` chạy trong driver Python của Spark nên driver phải ghi được PostgreSQL.

Hiện trạng kiểm tra ngày 2026-11-25:

- Image `n06-spark:3.5.8-py311` không có `psycopg` lẫn `psycopg2`.
- **`psycopg` không có trong `pyproject.toml` hay `uv.lock`, và không cài trên máy host.** Vì vậy chưa có "phiên bản đã ghim ở host" để image dùng trùng; quyết định này phải thêm cùng một phiên bản vào cả hai nơi.
- **Control store chưa có upsert cho `ingest_batches`.** `src/n06/storage/control_store.py` chỉ có `list_committed_ingest_batches`; các câu `ON CONFLICT` hiện có là của `current_snapshot`.
- **Không có bằng chứng đường `psycopg` của control store (`PsycopgControlStore`) đã chạy với PostgreSQL thật**, và không có unit test cho nó. Trên máy hiện tại host không có `psycopg` lẫn `psql`, nên test tích hợp Postgres đi qua `PsqlCliControlStore` bằng `docker compose exec ... psql` (đường dự phòng viết trong test).
- `connect_control_store` và `n06.control.migrations` thử `psycopg`, rồi `psycopg2`, rồi `psql` **trên PATH của host**, rồi ném lỗi; chúng không dùng `psql` trong container, và không có nơi gọi nào trong `src/`.
- Đặc tả mục 3: *"SQL có tham số cho giá trị; tên bảng/cột động chỉ nhận từ allowlist."* Đường `psql` hiện có ghép chuỗi SQL (`_escape_literal`), tức đã lệch yêu cầu này từ trước.
- Đặc tả Phần I mục 2 ("Cách Airflow gọi Spark", dòng 201-202) nói: *"Có một image job chứa Java, PySpark, các JAR đã ghim và gói `n06`."* và *"Spark chạy client mode ngay trong container của task: `local[N]` mặc định"*.

## Quyết định 1: thêm `psycopg` ghim phiên bản

Thêm vào `pyproject.toml` (`dependencies`, vì mã trong `src/` chạy cả trên host), `uv.lock` và image Spark **cùng một bộ phiên bản** (kết quả `uv pip compile` cho Python 3.11, linux x86_64, ngày 2026-11-25):

| Gói | Phiên bản |
|---|---|
| `psycopg[binary]` (`psycopg`) | `3.3.9` |
| `psycopg-binary` | `3.3.9` |
| `typing-extensions` | `4.16.0` |

Trong image, cài bằng `pip install --require-hashes` từ một tệp yêu cầu sinh từ `uv.lock`, không kéo theo `confluent-kafka` (chỉ host theo ADR-0010 và ma trận tương thích) hay chính dự án ở dạng editable: `uv export --frozen --no-dev --no-emit-project --no-emit-package confluent-kafka`. Đã chạy thử trên `uv.lock` hiện tại (chưa có `psycopg`) ngày 2026-11-25: lệnh chạy được; **chưa kiểm** đầu ra sau khi thêm `psycopg` và việc cài trong image.

Lý do:

1. Truy vấn tham số hóa qua DB-API, đúng yêu cầu *"SQL có tham số cho giá trị"* của đặc tả mục 3, thay vì ghép chuỗi như đường `psql`.
2. Một transaction ngay trong driver, không tạo tiến trình con cho mỗi batch và không đưa mật khẩu qua `PGPASSWORD`.
3. Dùng lại giao diện `ControlStore` và lớp `PsycopgControlStore` đã viết, vốn đã là lựa chọn đầu tiên của mã hiện có.

Lý do **không** được dùng để biện minh: "mã upsert đã kiểm với Postgres thật" (xem Bối cảnh: chưa đúng). Upsert mới nằm trong `src/n06/storage/` (B sở hữu) trong khi T22 thuộc A, nên cần B duyệt phần đó. Việc cập nhật phụ thuộc, image và ma trận chưa có thẻ nào trong `docs/TASKS.md`; người duyệt quyết gộp vào T22 hay tách task.

Điều kiện bắt buộc khi hiện thực (thuộc T22 hoặc một task mở đường):

- Chạy test PostgreSQL hiện có qua đường `psycopg` trên Postgres thật trước khi viết mã mới. `tests/integration/test_commit_postgres.py` hiện bắt `except Exception` rồi chuyển sang `psql`, và có `pytest.skip`: phải sửa để lỗi của đường `psycopg` không bị che.
- Viết upsert `ingest_batches` với `ON CONFLICT (query_name, batch_id) DO UPDATE` (ADR-0003) và test trên Postgres thật, gồm ca gọi hai lần cùng khóa.

### Phương án đã loại

- **(b) JDBC driver của PostgreSQL trong image.** Thêm một JAR phải ghim phiên bản và checksum. `DataFrameWriter` JDBC của Spark không có upsert `ON CONFLICT`, nên vẫn phải tự gọi JDBC qua py4j trong driver: một đường truy cập DB thứ hai, không dùng lại control store bằng Python.
- **(c) Chạy driver Spark trên máy host.** Theo cách hiểu của người viết, lệch Phần I mục 2 (có một image job chứa Java, PySpark, JAR và gói `n06`; đoạn nói về Airflow gọi Spark ghi Spark chạy client mode ngay trong container của task). Dòng 554 chỉ nói `stream` "chạy terminal/service riêng", không nói host hay container. Còn phải thêm `pyspark` và Java đúng phiên bản trên host làm phụ thuộc, trong khi bộ JAR đã ghim (ADR-0001) chỉ có trong image nên môi trường chạy lệch khỏi ma trận tương thích. Với `spark://` (profile cluster), executor trong mạng Docker còn phải kết nối ngược về driver trên host; với `local[N]` mặc định thì không.
- **(d) Cài `postgresql-client` vào image và dùng đường `psql`** (không nằm trong ba lựa chọn ban đầu, nêu để người duyệt biết). Đây là đường duy nhất đã có bằng chứng trên Postgres thật, nhưng ghép chuỗi SQL, **trái yêu cầu SQL tham số hóa của đặc tả**: chọn (d) phải viết lại để tham số hóa hoặc lập ADR ngoại lệ. Ngoài ra vẫn là phụ thuộc mới của image và một tiến trình con mỗi batch.

### Tác động

- `docs/compatibility-matrix.md`: thêm ba dòng gói Python ở trên kèm SHA-256 của wheel (lấy từ PyPI khi hiện thực) và cách kiểm lại; ghi rõ chúng nằm cả ở host lẫn image, khác dòng `confluent-kafka` (chỉ host).
- Build image: thêm một lớp `pip install` sau các JAR; build cần truy cập PyPI (ADR-0001 chỉ cấm tải lúc chạy job). Phải dựng lại image (`docker compose --profile core build spark`) trên mọi máy; kiểm bằng `docker run --rm n06-spark:3.5.8-py311 python3 -c "import psycopg"`.
- Giấy phép `psycopg`: LGPL-3.0 theo hiểu biết của người viết, **chưa kiểm** từ gói.
- Khi `psycopg` được cài trên host, `connect_control_store` và migration hết ném lỗi trên máy này và dùng `psycopg`; **test** hiện có chuyển từ `psql` trong container sang `psycopg`.
- Chưa quyết ở ADR này: cách đưa gói `n06` và `contracts/` vào image (đặc tả yêu cầu image job chứa gói `n06`; image hiện tại chưa có) và cách khởi chạy `n06 stream` (thuộc kế hoạch T22).

## Quyết định 2: ghi object lên MinIO từ Python (T23)

T23 đưa file sao kê lên MinIO theo thứ tự file dữ liệu, manifest, rồi biên nhận `_ingest/`; tải lại cùng khóa chỉ được khi `sha256` giống (ADR-0002 mục 4).

**Giả định:** T23 được chạy từ host như các lệnh `n06` hiện nay. Phần I mục 2 nói *"Toàn bộ job chạy qua CLI `n06`. Có một image job chứa Java, PySpark, các JAR đã ghim và gói `n06`."*; người viết hiểu rằng job có thể được chạy trong image đó. Nếu T23 chạy trong image thì phương án B phải cài thêm gói vào image; A không cần gói, nhưng cả hai cần cách suy địa chỉ MinIO trong mạng Docker (`resolve_host_endpoint` hiện chỉ suy địa chỉ cho host).

**Liệt kê object:** ADR-0002 mục 4 yêu cầu pipeline liệt kê `_ingest/` để phát hiện file mới, và T25 phải liệt kê S3 thật trước khi ghi một run. Chưa quyết thành phần nào liệt kê bằng Python, thành phần nào dùng S3A/Hadoop FS trong Spark; câu trả lời ảnh hưởng lựa chọn dưới đây.

### Số đo file sao kê của dataset do generator sinh

Cấu hình `generator.demo-faults.json` (3 ngày, `statement_file_window_seconds = 3600`, hai kênh, `account_count = 100`), đo ngày 2026-11-25 trên mã `main` `6c3ac71`; lệnh và script ở cuối tài liệu:

| Số giao dịch | Số file | Tổng | B mỗi dòng | Trung vị | p95 | Lớn nhất (dòng) |
|---:|---:|---:|---:|---:|---:|---:|
| 20.000 | 152 | 7,3 MB | 364,8 | 37,6 KB | 78,4 KB | 91,2 KB (250) |
| 50.000 | 152 | 18,2 MB | 364,8 | 85,6 KB | 185,0 KB | 215,3 KB (590) |

Mỗi file có một manifest 285 byte trở xuống. **Ngoại suy tuyến tính, chưa đo:** ở 1.000.000 giao dịch file lớn nhất khoảng 4,3 MB, tổng khoảng 365 MB; ở 5.000.000 khoảng 21,5 MB, tổng khoảng 1,8 GB. Số file giữ nguyên 152 vì theo cửa sổ và kênh. Nếu đặt cửa sổ 86.400 giây, file lớn nhất tăng khoảng 20-24 lần (cỡ 0,4-0,5 GB ở 5.000.000); **chưa đo**.

Giới hạn của S3: một lần `PUT` tối đa 5 GiB; multipart yêu cầu mỗi phần (trừ phần cuối) tối thiểu 5 MiB. Mọi mức ở trên nằm xa dưới 5 GiB, nên **không cần multipart**.

### Phương án A: mở rộng bộ ký SigV4 bằng thư viện chuẩn

Thêm `PUT` object (và `HEAD` object), đặt trong `src/n06/storage/` cùng phần ký chuyển từ `src/n06/cli/object_store.py`.

- Phải viết: gửi thân yêu cầu theo luồng với `Content-Length`; tính SHA-256 của payload trước khi gửi (bộ ký đã nhận `payload_sha256`); lưu `sha256` vào metadata và đọc lại bằng `HEAD` để giữ tính bất biến; ánh xạ lỗi và thử lại. Phần nào liệt kê bằng Python thì phải tự phân trang `ListObjectsV2` và phân tích XML.
- Quy tắc hiện tại cấm rõ: `src/n06/cli/README.md` (C sở hữu) ghi *"Đây không phải thư viện S3 dùng chung: mã đọc/ghi dữ liệu (T22, T23, T25) không được import module này."*; docstring `object_store.py` ghi *"[...] mã đọc/ghi dữ liệu (T22, T23, T25) không được dựa vào module này."* Chọn A là **đổi quy tắc đó**, cần C đồng ý; đề xuất chuyển phần ký và gọi HTTP sang `src/n06/storage/` (B sở hữu, đúng mục đích *"Object store"*), `cli` nhập từ đó.
- Không thêm phụ thuộc. Không có multipart; đối tượng lớn hơn giới hạn một lần `PUT` sẽ không ghi được.
- Kiểm thử: bộ ví dụ ký của AWS hiện có đã gồm một `PUT` có thân. Với MinIO thật, bộ ký **mới được kiểm cho các yêu cầu thân rỗng** (`HEAD`/`PUT` bucket; test tích hợp còn ký `GET /` và `DELETE` bucket); `PUT` object có thân và metadata **chưa kiểm**. Máy chủ S3 giả hiện có không đọc thân yêu cầu nên phải mở rộng; thêm test tích hợp MinIO thật (ghi, đọc lại, so `sha256`, ghi đè khác nội dung bị từ chối, file lớn).

### Phương án B: thêm client S3 (`boto3`)

- Ghim `boto3 1.44.11`, `botocore 1.44.11`, `s3transfer 0.19.3`, `jmespath 1.1.0`, `python-dateutil 2.9.0.post0`, `six 1.17.0`, `urllib3 2.8.0` (kết quả giải phụ thuộc ngày 2026-11-25, **chưa cài thử**) vào `pyproject.toml` và `uv.lock`; thêm vào ma trận tương thích. Chỉ host, trừ khi T23 hoặc T25 chạy trong image.
- Có sẵn multipart, thử lại, phân trang liệt kê, `HeadObject`. Ít mã tự bảo trì hơn.
- Rủi ro **chưa kiểm**: các bản `boto3` từ 1.36 đổi mặc định về checksum toàn vẹn, một số kho tương thích S3 không chấp nhận; phải kiểm với image MinIO đã ghim và có thể phải cấu hình lại.
- Kho mã sẽ có hai đường gọi S3 (quản trị bucket bằng bộ ký tự viết, ghi dữ liệu bằng `boto3`).
- Kiểm thử: `botocore.stub.Stubber` cho unit test, MinIO thật cho tích hợp.

(`minio` SDK cũng làm được; kết quả giải ngày 2026-11-25 là chín gói, có gói biên dịch mã hóa, **chưa cài thử**; không đề xuất.)

### Quyết định 2 (đề xuất)

Chọn **A**, đặt phần ký và gọi HTTP trong `src/n06/storage/`, phạm vi chỉ `PUT` và `HEAD` object cho T23. Lý do: kích thước file nhỏ so với giới hạn một lần `PUT`, không thêm phụ thuộc, chạy được ở host lẫn image. Điều kiện xem lại: nếu T23 hoặc T25 phải liệt kê object từ Python và ghi nhiều đối tượng lớn thì B đáng chi phí hơn. Cần B và C đồng ý trước khi Accepted.

## Hệ quả chung

- Sau khi Accepted: một task cập nhật phụ thuộc, image và ma trận; T22 phụ thuộc quyết định 1, T23 phụ thuộc quyết định 2.
- Nếu chỉ một quyết định được duyệt, task tương ứng được mở khóa riêng.

## Phụ lục: tái hiện số đo file sao kê

```bash
uv run n06 generate --profile demo \
  --generator-config contracts/N06_INPUT_CONTRACT_V1/config/generator.demo-faults.json \
  --transactions 20000 --out /đường/dẫn/ngoài-repo/20000     # lặp với 50000
uv run python /đường/dẫn/stmt_sizes.py /đường/dẫn/ngoài-repo/20000 20000
```

```python
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
files = sorted((root / "source" / "statements").rglob("*.jsonl"))
sizes = [f.stat().st_size for f in files]
rows = [sum(1 for _ in f.open("rb")) for f in files]
manifests = list((root / "source" / "manifests").glob("statements_*.json"))
print(
    f"files={len(files)} total={sum(sizes)} B/row={sum(sizes) / sum(rows):.1f} "
    f"median={int(statistics.median(sizes))} p95={sorted(sizes)[int(len(sizes) * 0.95)]} "
    f"max={max(sizes)} max_rows={max(rows)} max_manifest={max(m.stat().st_size for m in manifests)}"
)
```
