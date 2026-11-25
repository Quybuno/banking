# ADR-0012: Driver PostgreSQL trong image Spark (T22) và cách ghi object lên MinIO (T23)

- Trạng thái: Proposed
- Người viết: agent (soạn theo yêu cầu của chủ dự án để mở khóa T22 và T23), 2026-11-25
- Người duyệt: A (T22, T23; image Spark theo `infra/README.md`), C (Compose, cấu hình, `src/n06/cli/`), B (`src/n06/storage/`: control store, object store)
- Ngày: 2026-11-25
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 2, 3, 6, 8.1; Phần II mục 4, 7

Chưa được sửa `pyproject.toml`, `uv.lock` hay `infra/spark/Dockerfile` trước khi ADR này chuyển Accepted.
Hai quyết định dưới đây độc lập, có thể duyệt riêng.

## Bối cảnh

Đặc tả mục 8.1 bước 3: *"Sau khi mọi output của batch đã ghi xong, upsert một dòng vào bảng control `ingest_batches` [...] Callback chỉ trả thành công sau bước này."* Callback `foreachBatch` chạy trong driver Python của Spark nên driver phải ghi được PostgreSQL.

### Driver mà control store của T21/T24 đang dùng ở host

**`psql` bên trong container `postgres`, gọi bằng `docker compose exec`, qua lớp `PsqlCliControlStore`** (và bộ chạy `psql` riêng của test migration). Bằng chứng đo ngày 2026-11-25 trên máy phát triển, Postgres thật 16.6 (lệnh ở phụ lục A):

| Quan sát | Kết quả |
|---|---|
| `tests/integration/test_control_migrations.py` (T21) và `tests/integration/test_commit_postgres.py` (T24) | 2/2 PASS, không skip |
| `import psycopg`, `import psycopg2` trên host | không có |
| `psql` trên PATH của host | không có |
| Lệnh con chạm Postgres | 44 lần `docker compose -f docker-compose.yml exec -T ... postgres psql` |
| Phương thức `PsqlCliControlStore` được gọi | `_run_sql` 17, `commit_run_atomic` 5, `start_run` 4, `get_run` 4, `resolve_current_snapshot` 4, `fail_run` 1 |
| Phương thức `PsycopgControlStore` được gọi | **0** |

Hệ quả: không có bằng chứng đường `psycopg` đã chạy với PostgreSQL thật; `psycopg` cũng không có trong `pyproject.toml`, `uv.lock`. Control store chưa có upsert `ingest_batches` (chỉ có `list_committed_ingest_batches`).

Hiện trạng khác:

- `connect_control_store` thử `psycopg`, rồi `psycopg2`, rồi `psql` trên PATH, rồi ném lỗi. Image `n06-spark:3.5.8-py311` không có cái nào trong ba.
- Đặc tả mục 3: *"SQL có tham số cho giá trị; tên bảng/cột động chỉ nhận từ allowlist."* `PsqlCliControlStore` hiện **ghép chuỗi SQL** (`_escape_literal`), tức đã lệch yêu cầu này từ trước.

## Quyết định 1: image Spark dùng cùng `psql` 16.6 mà host đang dùng

Tiêu chí: **khớp với host nếu chạy được trong image**. Đường của host chạy được trong image (đo ngày 2026-11-25 trong container tạm, không sửa image hay Dockerfile):

| Thử nghiệm | Kết quả |
|---|---|
| Chép `psql` và `libpq.so.5` từ `postgres:16.6-bookworm` vào image Spark (build nhiều tầng tạm, đã xóa image) | `ldd` đủ thư viện; `psql (PostgreSQL) 16.6`; nối được server 16.6 |
| `connect_control_store` trong container Spark có `psql` trên PATH (bản `postgresql-client-16` 16.16 từ kho PGDG) | chọn `PsqlCliControlStore`; truy vấn đọc `ingest_batches` thật thành công |
| Truyền tham số: `psql -v val=...` và `SELECT $1::text \bind :val \g` (16.6) | giá trị chứa dấu nháy, `;`, `\`, khoảng trắng được giữ nguyên, không cần tự escape |

**Chốt đề xuất:**

1. `infra/spark/Dockerfile` thêm một tầng `FROM postgres:16.6-bookworm` và chép `psql` cùng `libpq.so.5`. Image nguồn ghim theo tag và digest `sha256:557fea37a744d5f4c8faab304b0a90858b53ab119735a88c131fd19dab802f36`; đây chính là image Compose đang dùng cho server, nên client và server cùng 16.6.
2. Không thêm gói Python và không đổi `pyproject.toml`, `uv.lock`.
3. **Điều kiện bắt buộc trước khi T22 dùng:** câu lệnh có giá trị (gồm upsert `ingest_batches` mới và các câu hiện có của `PsqlCliControlStore`) phải truyền giá trị bằng biến `-v` và `\bind`, bỏ ghép chuỗi, để đáp ứng yêu cầu SQL tham số hóa; test trên Postgres thật với giá trị có dấu nháy, `;`, `\`. Việc này sửa mã của B (T24).
4. Upsert `ingest_batches` dùng `ON CONFLICT (query_name, batch_id) DO UPDATE` (ADR-0003), test gọi hai lần cùng khóa.

Giới hạn đã biết:

- Mỗi lần ghi là một tiến trình con.
- Giá trị truyền qua `-v` nằm trong argv, xem được bằng `ps` trong container (dữ liệu control không phải secret; mật khẩu đi qua `PGPASSWORD`).
- Linux giới hạn một đối số khoảng 128 KiB; `topic_offsets` nhỏ hơn nhiều, **chưa kiểm** ở nhiều partition.
- Chưa kiểm một transaction nhiều câu lệnh dùng `\bind` qua mã Python, và hook chèn lỗi của `commit_run_atomic`.
- Host vẫn dùng `docker compose exec`; image dùng `psql` cục bộ: cùng client, khác cách gọi.

### Phương án đã loại

- **`psycopg` ghim phiên bản** (đề xuất trước của người viết). Không khớp host (host không dùng và chưa từng cài), thêm phụ thuộc Python mới vào `pyproject.toml`, `uv.lock` và image, và đưa vào sử dụng một đường chưa có bằng chứng chạy. Đổi lại sẽ có tham số hóa qua DB-API mà không phải viết lại lệnh `psql`.
- **`postgresql-client-16` từ kho apt PGDG.** Chạy được (16.16), nhưng thêm kho và khóa ngoài lúc build, phiên bản cũ có thể bị gỡ khỏi kho, và lệch minor với server.
- **`postgresql-client` của Debian bookworm (15).** Không có `\bind` (chỉ từ 16).
- **JDBC driver trong image.** Thêm JAR phải ghim; `DataFrameWriter` JDBC không có upsert `ON CONFLICT`, phải gọi qua py4j: đường truy cập thứ hai, không dùng lại control store Python.
- **Chạy driver Spark trên host.** Theo cách hiểu của người viết, lệch Phần I mục 2 (một image job chứa Java, PySpark, JAR và gói `n06`; đoạn nói về Airflow gọi Spark ghi Spark chạy client mode ngay trong container của task; dòng 554 chỉ nói `stream` "chạy terminal/service riêng"). Cần thêm `pyspark` và Java trên host, môi trường lệch khỏi ma trận tương thích.

### Tác động

- `docs/compatibility-matrix.md`: thêm dòng `psql`/`libpq` 16.6 lấy từ `postgres:16.6-bookworm` kèm digest và cách kiểm (`docker run --rm n06-spark:3.5.8-py311 psql --version`).
- Build image cần sẵn `postgres:16.6-bookworm` (Compose đã kéo về); thêm một tầng nhỏ; phải dựng lại image trên mọi máy.
- Chưa quyết ở ADR này: cách đưa gói `n06` và `contracts/` vào image (đặc tả yêu cầu image job chứa gói `n06`; image hiện tại chưa có) và cách khởi chạy `n06 stream` (kế hoạch T22).

## Quyết định 2: T23 ghi object bằng bộ ký SigV4 sẵn có, mở rộng sang `PUT` object

T23 đưa file sao kê lên MinIO theo thứ tự file dữ liệu, manifest, rồi biên nhận `_ingest/`; tải lại cùng khóa chỉ được khi `sha256` giống (ADR-0002 mục 4).

### Số đo file sao kê của dataset do generator sinh

Cấu hình `generator.demo-faults.json` (3 ngày, `statement_file_window_seconds = 3600`, hai kênh, `account_count = 100`), đo ngày 2026-11-25 trên mã `main` `6c3ac71`; lệnh và script ở cuối tài liệu:

| Số giao dịch | Số file | Tổng | B mỗi dòng | Trung vị | p95 | Lớn nhất (dòng) |
|---:|---:|---:|---:|---:|---:|---:|
| 20.000 | 152 | 7,3 MB | 364,8 | 37,6 KB | 78,4 KB | 91,2 KB (250) |
| 50.000 | 152 | 18,2 MB | 364,8 | 85,6 KB | 185,0 KB | 215,3 KB (590) |

Mỗi file có một manifest 285 byte trở xuống. **Ngoại suy tuyến tính, chưa đo:** ở 1.000.000 giao dịch file lớn nhất khoảng 4,3 MB, tổng khoảng 365 MB; ở 5.000.000 khoảng 21,5 MB, tổng khoảng 1,8 GB. Số file giữ nguyên 152 vì theo cửa sổ và kênh. Nếu đặt cửa sổ 86.400 giây, file lớn nhất tăng khoảng 20-24 lần (cỡ 0,4-0,5 GB ở 5.000.000); **chưa đo**.

Giới hạn của S3: một lần `PUT` tối đa 5 GiB; multipart yêu cầu mỗi phần (trừ phần cuối) tối thiểu 5 MiB. Mọi mức ở trên nằm xa dưới 5 GiB, nên **không cần multipart**.

### Nội dung

1. Thêm `PUT` object và `HEAD` object dùng bộ ký SigV4 bằng thư viện chuẩn. Chuyển phần ký và gọi HTTP từ `src/n06/cli/object_store.py` sang `src/n06/storage/` (B sở hữu, mục đích *"Object store"*); `cli` nhập từ đó.
2. Việc này **đổi một quy tắc hiện hành**: `src/n06/cli/README.md` ghi *"Đây không phải thư viện S3 dùng chung: mã đọc/ghi dữ liệu (T22, T23, T25) không được import module này."*, docstring `object_store.py` ghi *"[...] mã đọc/ghi dữ liệu (T22, T23, T25) không được dựa vào module này."* Cần C và B đồng ý; cập nhật cả hai chỗ.
3. Gửi thân yêu cầu theo luồng với `Content-Length`; tính SHA-256 của payload trước khi gửi; lưu `sha256` vào metadata và đọc lại bằng `HEAD` để giữ tính bất biến; ánh xạ lỗi và thử lại có giới hạn.
4. Không multipart (mọi kích thước đo và ngoại suy nằm xa dưới giới hạn một lần `PUT`).
5. Không thêm phụ thuộc.

### Kiểm thử

- Dùng lại known-answer test **PUT Object** của AWS đã có: `tests/unit/cli/test_object_store_sigv4.py` (vector `PUT Object (test$file.text)`, thân `Welcome to Amazon S3.`), chuyển theo mã đã dời.
- Bộ ký **mới được kiểm trên MinIO thật cho yêu cầu thân rỗng** (`HEAD`/`PUT` bucket, `GET /`, `DELETE` bucket); `PUT` object có thân và metadata **chưa kiểm**. Máy chủ S3 giả hiện có không đọc thân yêu cầu nên phải mở rộng.
- Test tích hợp MinIO thật: ghi, đọc lại, so `sha256`, tải lại cùng nội dung không đổi, khác nội dung bị từ chối, file lớn nhất theo ngoại suy.

### Phương án đã loại

- **`boto3`.** Thêm bảy gói (`boto3 1.44.11` cùng `botocore`, `s3transfer`, `jmespath`, `python-dateutil`, `six`, `urllib3`; kết quả giải phụ thuộc ngày 2026-11-25, **chưa cài thử**), có sẵn multipart, thử lại, phân trang liệt kê mà T23 chưa cần; các bản từ 1.36 đổi mặc định checksum, tương thích với MinIO đã ghim **chưa kiểm**.
- **`minio` SDK.** Chín gói theo kết quả giải ngày 2026-11-25, **chưa cài thử**.

### Còn mở

- **Liệt kê object:** ADR-0002 mục 4 yêu cầu pipeline liệt kê `_ingest/` và T25 phải liệt kê S3 thật trước khi ghi một run. Quyết định này chỉ phủ `PUT`/`HEAD`; nếu việc liệt kê làm bằng Python thì phải thêm `ListObjectsV2` (phân trang, XML), còn dùng S3A/Hadoop FS trong Spark thì không cần. Chủ: B, trước T25.
- **Giả định:** T23 chạy từ host như các lệnh `n06` hiện nay. Phần I mục 2 nói *"Toàn bộ job chạy qua CLI `n06`. Có một image job [...]"*; nếu T23 chạy trong image thì cần suy địa chỉ MinIO trong mạng Docker (`resolve_host_endpoint` hiện chỉ suy cho host).

## Hệ quả chung

- Sau khi Accepted: T22 phụ thuộc quyết định 1, T23 phụ thuộc quyết định 2; duyệt quyết định nào mở khóa task đó.
- Việc cập nhật image, ma trận tương thích, viết lại lệnh `psql` và dời bộ ký chưa có thẻ trong `docs/TASKS.md`; người duyệt quyết gộp vào T22/T23 hay tách task.

## Phụ lục A: bằng chứng đường kết nối (đo 2026-11-25)

- Host: chạy hai test tích hợp trên với một plugin pytest tạm (ngoài repo) bọc `subprocess.run` (che `PGPASSWORD`) và đếm phương thức của `PsqlCliControlStore`, `PsycopgControlStore`.
- Image, bản 16.6: Dockerfile tạm gồm `FROM postgres:16.6-bookworm AS pg`, `FROM n06-spark:3.5.8-py311`, `COPY --from=pg /usr/lib/postgresql/16/bin/psql /usr/local/bin/psql`, `COPY --from=pg /usr/lib/x86_64-linux-gnu/libpq.so.5 /usr/lib/x86_64-linux-gnu/`; chạy `ldd`, `psql --version`, `psql -h postgres ... -c 'select version()'` trên mạng `n06_network`.
- Image, control store: container tạm `--user root`, cài `postgresql-client-16` từ PGDG, gắn `src/` và `config/` chỉ đọc, gọi `connect_control_store(load_config(profile="default").postgres.control).list_committed_ingest_batches(...)`.

## Phụ lục B: tái hiện số đo file sao kê

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
