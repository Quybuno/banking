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

**`psql` bên trong container `postgres`, gọi bằng `docker compose exec`.** Với T24 là lớp `PsqlCliControlStore` nhận bộ chạy do test dựng (`make_compose_psql_runner`); với T21 là bộ chạy `psql` riêng của test migration. Đường này hiện **chỉ được nối trong test**: trên host không có `psycopg`, `psycopg2` hay `psql`, nên `connect_control_store` gọi từ mã chạy trên host sẽ ném lỗi.

Bằng chứng đo ngày 2026-11-25 trên máy phát triển, Postgres thật 16.6 (cách tái hiện ở phụ lục A):

| Quan sát | Kết quả |
|---|---|
| `tests/integration/test_control_migrations.py` (T21) và `tests/integration/test_commit_postgres.py` (T24) | 2/2 PASS, không skip |
| `import psycopg`, `import psycopg2` trên host; `psql` trên PATH | không có cả ba |
| Lệnh con chạm Postgres | 44 lần `docker compose -f docker-compose.yml exec -T ... postgres psql` |
| Phương thức `PsqlCliControlStore` được gọi | `_run_sql` 17, `commit_run_atomic` 5, `start_run` 4, `get_run` 4, `resolve_current_snapshot` 4, `fail_run` 1. `list_committed_ingest_batches` **không** được hai test này gọi |
| Phương thức `PsycopgControlStore` được gọi | **0** |

Hệ quả: không có bằng chứng đường `psycopg` đã chạy với PostgreSQL thật; `psycopg` không có trong `pyproject.toml`, `uv.lock`. Control store chưa có upsert `ingest_batches` (chỉ có `list_committed_ingest_batches`).

Hiện trạng khác:

- `connect_control_store` thử `psycopg`, rồi `psycopg2`, rồi `psql` trên PATH, rồi ném lỗi. Image `n06-spark:3.5.8-py311` không có cái nào trong ba.
- Đặc tả mục 3: *"SQL có tham số cho giá trị; tên bảng/cột động chỉ nhận từ allowlist."* `PsqlCliControlStore` **ghép chuỗi SQL** (`_escape_literal`), kể cả bên trong khối `DO $$ ... $$` của `start_run`, `commit_run_atomic`, `fail_run`: đã lệch yêu cầu này từ trước.
- Trong đường `psql`, hook chèn lỗi của `commit_run_atomic` chạy **trước** khi gửi SQL, nên ca "lỗi giữa transaction" của test T24 **không kiểm rollback** trên đường này.

## Quyết định 1: image Spark dùng cùng `psql` 16.6 mà host đang dùng

Tiêu chí của chủ dự án: **khớp với host nếu chạy được trong image**. Kết quả đo ngày 2026-11-25 trong container tạm (không sửa image hay Dockerfile):

| Thử nghiệm | Kết quả | Phạm vi của bằng chứng |
|---|---|---|
| Chép `psql` và `libpq.so.5` từ `postgres:16.6-bookworm` vào image Spark (build nhiều tầng tạm, đã xóa image) | `ldd` đủ thư viện; `psql (PostgreSQL) 16.6`; nối server 16.6 | chỉ `--version` và `select version()` |
| `connect_control_store` trong container Spark có `psql` trên PATH (`postgresql-client-16` **16.16** từ PGDG) | chọn `PsqlCliControlStore`; `list_committed_ingest_batches` đọc bảng thật | **chỉ đọc**, bản 16.16, không phải bản chép 16.6 |
| Trong container postgres (`psql` 16.6): giá trị truyền bằng một biến JSON `-v params=...`, nạp bằng `:'params'::jsonb` vào bảng tạm `ON COMMIT DROP`, đọc trong khối `DO` | chuỗi chứa nháy, `;`, `\`, `$$`, xuống dòng giữ nguyên (46 ký tự); `null` giữ là NULL; transaction `ROLLBACK` | cơ chế, chưa qua mã Python của control store |

**Chưa chạy:** tổ hợp bản chép 16.6 cùng `PsqlCliControlStore`; đường **ghi** trong image qua nhánh `subprocess` cục bộ của `_run_sql`.

**Chốt đề xuất:**

1. `infra/spark/Dockerfile` thêm tầng `FROM postgres:16.6-bookworm` và chép `psql` cùng `libpq.so.5`. Compose hiện dùng **tag** `postgres:16.6-bookworm` không kèm digest, nên đề xuất ghim cùng một digest ở cả Compose và Dockerfile (bản đang có trên máy: `sha256:557fea37a744d5f4c8faab304b0a90858b53ab119735a88c131fd19dab802f36`) để client và server chắc chắn cùng bản.
2. Không thêm gói Python, không đổi `pyproject.toml`, `uv.lock`.
3. **Điều kiện bắt buộc trước khi T22 dùng (sửa mã T24 của B):**
   - Viết lại mọi câu có giá trị trong `PsqlCliControlStore` và upsert `ingest_batches` mới theo cơ chế biến JSON ở trên: không ghép giá trị vào chuỗi SQL; NULL là `null` của JSON (cần cho `dataset_id` theo ADR-0004, `manifest_path`, `business_date_from/to`).
   - Giao diện bộ chạy `(sql, tuples_only)` phải nhận thêm biến.
   - Test trên Postgres thật, **trong image**: ghi và đọc lại, giá trị có nháy, `;`, `\`, `$$`, NULL; lỗi giữa transaction phải rollback thật (lỗi chèn trong SQL, không phải hook Python).
4. Upsert `ingest_batches` dùng `ON CONFLICT (query_name, batch_id) DO UPDATE` (ADR-0003), test gọi hai lần cùng khóa.

**Điều người duyệt phải quyết:** cơ chế trên để `psql` tự trích dẫn biến ở phía client; đây **không phải** tham số phía server. Nếu "SQL có tham số" của đặc tả đòi tham số phía server thì đường `psql` không đáp ứng gọn (`\bind` của psql 16 không dùng được trong khối `DO` và không truyền được NULL), và nên chọn `psycopg`.

Giới hạn đã biết:

- Mỗi lần ghi là một tiến trình con.
- Biến `-v` nằm trong argv, xem được bằng `ps` trong container; gồm cả `error_summary`, `input_versions`, `topic_offsets`. Linux giới hạn một đối số khoảng 128 KiB, **chưa kiểm** với giá trị dài. Trên host, `make_compose_psql_runner` đưa `PGPASSWORD` vào argv của `docker`.
- Đường dẫn thư viện `x86_64-linux-gnu` khóa image vào kiến trúc x86_64.
- Host vẫn dùng `docker compose exec`; image dùng `psql` cục bộ: cùng client, khác cách gọi.

### Phương án đã loại

- **`psycopg` ghim phiên bản** (đề xuất trước của người viết). Bị loại theo tiêu chí "khớp với host": host không dùng và chưa cài; thêm phụ thuộc Python vào `pyproject.toml`, `uv.lock`, image; đường `PsycopgControlStore` chưa có bằng chứng chạy. **Ưu điểm bị bỏ:** `PsycopgControlStore` đã dùng tham số phía server (`%s`) nên không phải viết lại SQL, truyền NULL tự nhiên, transaction thật trong một kết nối. Chi phí của đường `psql` (viết lại SQL và giao diện của T24) lớn hơn ước lượng ban đầu; người duyệt có thể đảo quyết định vì lý do này.
- **`postgresql-client-16` từ kho apt PGDG.** Chạy được (16.16), nhưng thêm kho và khóa ngoài lúc build, phiên bản cũ có thể bị gỡ khỏi kho, và lệch minor với server.
- **`postgresql-client` của Debian bookworm (15).** Lệch major với server 16.
- **JDBC driver trong image.** Thêm JAR phải ghim; `DataFrameWriter` JDBC không có upsert `ON CONFLICT`, phải gọi qua py4j: đường truy cập thứ hai, không dùng lại control store Python.
- **Chạy driver Spark trên host.** Theo cách hiểu của người viết, lệch Phần I mục 2 (một image job chứa Java, PySpark, JAR và gói `n06`; đoạn nói về Airflow gọi Spark ghi Spark chạy client mode ngay trong container của task; dòng 554 chỉ nói `stream` "chạy terminal/service riêng"). Cần thêm `pyspark` và Java trên host, môi trường lệch khỏi ma trận tương thích.

### Tác động

- `docs/compatibility-matrix.md`: thêm dòng `psql`/`libpq` 16.6 lấy từ `postgres:16.6-bookworm` kèm digest và cách kiểm (`docker run --rm n06-spark:3.5.8-py311 psql --version`); `docker-compose.yml` ghim cùng digest.
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

Mỗi file có một manifest 285 byte trở xuống. **Ngoại suy tuyến tính, chưa đo:** ở 1.000.000 giao dịch file lớn nhất khoảng 4,3 MB, tổng khoảng 365 MB; ở 5.000.000 khoảng 21,5 MB, tổng khoảng 1,8 GB. Số file giữ nguyên 152 vì theo cửa sổ và kênh. Cửa sổ lớn hơn làm file lớn hơn nhưng bị ràng buộc `late_within_delay + statement_file_window < allowed_lateness` (bộ kiểm cấu hình generator), nên với cấu hình này không thể đặt cửa sổ một ngày.

Giới hạn của S3: một lần `PUT` tối đa 5 GiB; multipart yêu cầu mỗi phần (trừ phần cuối) tối thiểu 5 MiB. Mọi mức ở trên nằm xa dưới 5 GiB, nên **không cần multipart**.

### Nội dung

1. Thêm `PUT` object và `HEAD` object dùng bộ ký SigV4 bằng thư viện chuẩn. Chuyển phần ký và gọi HTTP từ `src/n06/cli/object_store.py` sang `src/n06/storage/` (B sở hữu, mục đích *"Object store"*); `cli` nhập từ đó.
2. Việc này **đổi một quy tắc hiện hành**: `src/n06/cli/README.md` ghi *"Đây không phải thư viện S3 dùng chung: mã đọc/ghi dữ liệu (T22, T23, T25) không được import module này."*, docstring `object_store.py` ghi *"[...] mã đọc/ghi dữ liệu (T22, T23, T25) không được dựa vào module này."* Cần C và B đồng ý; cập nhật cả hai chỗ.
3. Gửi thân yêu cầu theo luồng với `Content-Length`; tính SHA-256 của payload trước khi gửi; lưu `sha256` vào metadata và đọc lại bằng `HEAD` để giữ tính bất biến; ánh xạ lỗi và thử lại có giới hạn.
4. Không multipart (mọi kích thước đo và ngoại suy nằm xa dưới giới hạn một lần `PUT`).
5. `HEAD` rồi `PUT` không nguyên tử: T23 chạy một tiến trình ghi cho mỗi dataset; object đã có nhưng thiếu metadata `sha256` coi là lỗi, không ghi đè. Lớp phản hồi hiện tại không giữ header nên phải mở rộng.
6. Không thêm phụ thuộc.

### Kiểm thử

- Vector này chỉ kiểm hàm ký; phải thêm test cho đường gửi `PUT` thật (thân, `Content-Length`, hash payload).
- Dùng lại known-answer test **PUT Object** của AWS đã có: `tests/unit/cli/test_object_store_sigv4.py` (vector `PUT Object (test$file.text)`, thân `Welcome to Amazon S3.`), chuyển theo mã đã dời.
- Bộ ký **mới được kiểm trên MinIO thật cho yêu cầu thân rỗng** (`HEAD`/`PUT` bucket, `GET /`, `DELETE` bucket); `PUT` object có thân và metadata **chưa kiểm**. Máy chủ S3 giả hiện có không đọc thân yêu cầu nên phải mở rộng.
- Test tích hợp MinIO thật: ghi, đọc lại, so `sha256`, tải lại cùng nội dung không đổi, khác nội dung bị từ chối, file lớn nhất theo ngoại suy.

### Phương án đã loại

- **`boto3`.** Thêm bảy gói (`boto3 1.44.11` cùng `botocore`, `s3transfer`, `jmespath`, `python-dateutil`, `six`, `urllib3`; kết quả giải phụ thuộc ngày 2026-11-25, **chưa cài thử**), có sẵn multipart, thử lại, phân trang liệt kê mà T23 chưa cần; các bản từ 1.36 đổi mặc định checksum, tương thích với MinIO đã ghim **chưa kiểm**.
- **`minio` SDK.** Chín gói theo kết quả giải ngày 2026-11-25, **chưa cài thử**.

### Còn mở

- **Liệt kê object:** ADR-0002 mục 4 yêu cầu pipeline liệt kê `_ingest/`; ADR-0002 mục 6 và thẻ T25 yêu cầu liệt kê S3 thật trước khi ghi một run. Quyết định này chỉ phủ `PUT`/`HEAD`; nếu việc liệt kê làm bằng Python thì phải thêm `ListObjectsV2` (phân trang, XML), còn dùng S3A/Hadoop FS trong Spark thì không cần. Chủ: B, trước T25.
- **Giả định:** T23 chạy từ host như các lệnh `n06` hiện nay. Phần I mục 2 nói *"Toàn bộ job chạy qua CLI `n06`. Có một image job [...]"*; nếu T23 chạy trong image thì cần suy địa chỉ MinIO trong mạng Docker (`resolve_host_endpoint` hiện chỉ suy cho host).

## Hệ quả chung

- Sau khi Accepted: T22 phụ thuộc quyết định 1, T23 phụ thuộc quyết định 2; duyệt quyết định nào mở khóa task đó.
- Việc cập nhật image, ma trận tương thích, viết lại lệnh `psql` và dời bộ ký chưa có thẻ trong `docs/TASKS.md`; người duyệt quyết gộp vào T22/T23 hay tách task.

## Phụ lục A: cách tái hiện bằng chứng đường kết nối (đo 2026-11-25)

**Host.** Một plugin pytest tạm (ngoài repo) bọc `subprocess.run` để đếm lệnh có `psql` (bỏ đối số `PGPASSWORD`) và đếm phương thức của `PsqlCliControlStore`, `PsycopgControlStore`:

```bash
PYTHONPATH=<thư mục chứa plugin> uv run pytest tests/integration/test_control_migrations.py \
  tests/integration/test_commit_postgres.py -p n06_trace_plugin -o addopts="" -rs -v
```

**Image, bản chép 16.6** (Dockerfile tạm ngoài repo, build thành tag tạm rồi `docker rmi`):

```dockerfile
FROM postgres:16.6-bookworm AS pg
FROM n06-spark:3.5.8-py311
USER root
COPY --from=pg /usr/lib/postgresql/16/bin/psql /usr/local/bin/psql
COPY --from=pg /usr/lib/x86_64-linux-gnu/libpq.so.5 /usr/lib/x86_64-linux-gnu/
USER spark
```

Chạy `ldd /usr/local/bin/psql`, `psql --version`, `psql -h postgres -U <user> -d <db> -At -c 'select version()'` trên mạng `n06_network`.

**Image, control store (16.16, chỉ đọc).** `docker run --rm --user root --network n06_network --env-file .env`, cài `postgresql-client-16` từ kho PGDG, gắn `src/` và `config/` chỉ đọc, chạy `connect_control_store(load_config(profile="default").postgres.control).list_committed_ingest_batches(dataset_id="n06-adr-probe")`.

**Biến JSON** (trong container postgres, `psql -X -v ON_ERROR_STOP=1 -v params=<json>`):

```sql
BEGIN;
CREATE TEMP TABLE n06_params ON COMMIT DROP AS SELECT :'params'::jsonb AS j;
DO $$
DECLARE v jsonb;
BEGIN
  SELECT j INTO v FROM n06_params;
  RAISE NOTICE 'do_dai=% dataset_null=%', length(v->>'a'), (v->>'dataset_id') IS NULL;
END $$;
ROLLBACK;
```

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
