# ADR-0013: Phiên bản và cách tích hợp Metabase (T43) và Airflow (T60)

- Trạng thái: Accepted
- Người viết: agent (soạn theo yêu cầu của chủ dự án để mở khóa T43 và T60), 2026-11-27
- Người duyệt: chủ repo (QUY), ủy quyền cho giám sát.
- Đã duyệt (Accepted): chủ repo (ủy quyền giám sát), 2026-11-27
- Ngày: 2026-11-27
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 2 ("Quy tắc chọn bộ phiên bản", "Cách Airflow gọi Spark"), mục 9, mục 10, mục 14.1

Từ 2026-11-27 ADR này là căn cứ code cho T43 và T60. Bản Proposed ban đầu chưa sửa Compose, Dockerfile, `pyproject.toml`, `uv.lock`, `.env.example`; các tệp đó chỉ được sửa trong T43, T60 và T41 đúng theo quyết định ở đây.

## Quyết định đã chốt

Chủ repo (ủy quyền giám sát) trả lời chín câu hỏi mở ngày 2026-11-27 như sau. Các câu trả lời này thắng mọi chỗ trong ADR còn ghi "đề xuất".

1. **Phiên bản.** Metabase `v0.64.9` (LTS); Airflow `3.4.0`. Đã có đủ nguồn kiểm chứng ở mục 1 của "Quyết định". Image Airflow tự dựng `FROM n06-spark:3.5.8-py311` nên vấn đề nền Debian 13 của image chính thức không áp dụng.
2. **RAM.** Không bật `core` + `bi` + `orchestration` + task Spark cùng lúc. Demo DAG: dừng `metabase`; `driver_memory` của profile `demo` đặt đủ nhỏ để vừa RAM (T60 đo và ghi số thật). Benchmark: chỉ `core`.
3. **Xác thực.** Airflow dùng SimpleAuthManager, người dùng lấy từ `.env`, ghi vào mục hạn chế của báo cáo. Metabase: tạo admin tay lần đầu, ghi trong hướng dẫn T43.
4. **Volume.** Không thêm volume giả cho Metabase; trạng thái nằm ở `n06_postgres_data`.
5. **Lockfile Airflow.** Commit `constraints-3.4.0-3.11.txt` vào repo, gỡ dòng `confluent-kafka` (dự án ghim `2.16.2`). Ghim base `python:3.11.11-slim-bookworm` bằng digest.
6. **Role chỉ đọc cho mart.** `n06_mart_ro` tạo bằng migration idempotent của database mart (thuộc T41). Không dùng script init vì script đó chỉ chạy trên volume mới.
7. **SQL dashboard.** P0 dùng SQL không tham số; T62 dùng biến của Metabase.
8. **Tên profile.** `bi` và `orchestration`.
9. **PostgreSQL.** Giữ `postgres:16.6-bookworm`.

Lịch sử: 2026-11-27 viết ở trạng thái Proposed (PR #27); cùng ngày chủ repo (ủy quyền giám sát) chốt chín câu hỏi mở và chuyển sang Accepted.

## Bối cảnh

`docker-compose.yml` trên `main` mới có `kafka`, `minio`, `postgres`, `spark` (profile `core`). `docs/compatibility-matrix.md` chưa có Metabase hay Airflow. PostgreSQL đã có sẵn database và role riêng `n06_airflow`/`n06_airflow_user` và `n06_metabase`/`n06_metabase_user` (`infra/postgres/init/01-init-databases.sh`, T02). `config/default.toml` đã có `services.airflow_enabled` và `services.metabase_enabled`; `config/benchmark.toml` đặt cả hai là `false`, `config/demo.toml` đặt `true`.

Các câu đặc tả ràng buộc quyết định này (trích nguyên văn):

- Phần I mục 2: *"Docker Compose cho local. Kafka, MinIO, PostgreSQL, Spark, Airflow, Metabase có healthcheck và named volumes."*
- Phần I mục 2: *"Airflow/Metabase bật theo profile để giảm RAM khi chạy benchmark."*
- Phần I mục 2: *"Dùng PostgreSQL riêng database/user cho mart, bảng control, Airflow metadata và Metabase application database; không để công cụ ghi vào schema nghiệp vụ của nhau."*
- Phần I mục 2: *"Không tải dependency động từ Internet mỗi lần chạy job. [...] Không dùng tag `latest`."*
- Quy tắc chọn bộ phiên bản, dòng 5: *"Java và Python lấy trong danh sách bản Spark đó hỗ trợ chính thức; Airflow và PySpark dùng chung một phiên bản Python."*
- Quy tắc chọn bộ phiên bản, dòng cuối: *"Ghi kết quả vào compatibility matrix kèm nguồn kiểm chứng, commit lockfile."*
- Cách Airflow gọi Spark (đã chốt): *"Image Airflow được build từ cùng danh sách dependency đó; task dùng `BashOperator` gọi `n06 ...`. Spark chạy client mode ngay trong container của task: `local[N]` mặc định, hoặc `spark://...` khi bật profile cluster."* và *"Không để DAG tham chiếu một `spark-submit` không tồn tại trong container Airflow; không mount Docker socket; không sao chép logic Spark vào DAG."*
- Mục 9: *"Metabase có bootstrap/seed hướng dẫn tái lập câu hỏi/dashboard với API tương thích phiên bản đã ghim hoặc export được hỗ trợ. Nếu cần thao tác thủ công, ghi từng bước chính xác và cung cấp SQL, không tuyên bố dashboard đã provision tự động."*
- Mục 14.1: *"Tắt Airflow/Metabase khi đo nếu thiếu RAM và ghi lại điều đó."*
- Mục 15 (checklist): *"Airflow gọi được Spark thật, không có đường dẫn/binary giả định."*
- Mục 1 (P2): *"Tự động provision dashboard Metabase qua API"* là P2.

Spec không nêu số phiên bản Metabase hay Airflow, nên người viết phải tra nguồn chính thức (mục 1 dưới đây). Khi tra, phát sinh ba phát hiện làm đổi cách chọn so với hướng "lấy image chính thức mới nhất":

1. Metabase bản 65 (mới nhất) là bản không LTS, hết hỗ trợ **2027-01-01**, một tháng sau hạn nộp 2026-12-01 của đồ án. Bản 64 là LTS, hết hỗ trợ 2027-12-01.
2. Image Airflow chính thức 3.4.0 đã chuyển sang Debian 13 (trixie) nền "hardened", không có `curl`. Debian trixie **không có** `openjdk-17-jre-headless`, trong khi ADR-0001 đã chốt Java 17 là Java Spark 3.5.8 hỗ trợ chính thức.
3. Ràng buộc phụ thuộc `constraints-3.4.0-3.11.txt` của Airflow ghim `confluent-kafka==2.16.1`, mâu thuẫn với `confluent-kafka==2.16.2` mà `pyproject.toml` và ADR-0010 đã ghim.

## Quyết định

### 1. Phiên bản được ghim

Ngày truy cập mọi nguồn dưới đây: **2026-11-27**.

| Thành phần | Giá trị ghim | Nguồn kiểm chứng (URL, truy cập 2026-11-27) |
|---|---|---|
| Metabase (open source) | `metabase/metabase:v0.64.9@sha256:b204168ce18470a9f2cba59470659b981dc39677ffa1dbfaf3551498034574a9` (digest của image index đa kiến trúc; manifest `linux/amd64`: `sha256:211ae465a2d260bb22e5d4627c367a0cb0520a5769fb6b895c0d552f333c2f4d`; build 2026-11-24T13:21:51Z) | Digest: `https://hub.docker.com/v2/repositories/metabase/metabase/tags?name=v0.64.9` và Registry API `https://registry-1.docker.io/v2/metabase/metabase/manifests/v0.64.9` (token ẩn danh từ `auth.docker.io`). Số phiên bản và vòng đời: `https://www.metabase.com/version-support` (bản 64: LTS, phát hành 2026-09-16, hết hỗ trợ 2027-12-01, bản mới nhất 64.9 ngày 2026-11-24; bản 65: hết hỗ trợ 2027-01-01). Quy ước tag cố định `v0.<phiên bản>` cho bản open source, cấm `latest`: `https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker` |
| Java trong image Metabase | `jdk-25.0.5.1+2` (biến `JAVA_VERSION` trong config blob của image) | Config blob của manifest amd64 trên; tài liệu: *"Metabase should be run on Java version 25"* (`https://www.metabase.com/docs/latest/troubleshooting-guide/running`). Không cài Java riêng cho Metabase. |
| Apache Airflow | `apache-airflow==3.4.0` cài từ PyPI; wheel `apache_airflow-3.4.0-py3-none-any.whl` sha256 `3d58ee1725712299658f3b453cab9a74391f4e80555fe3843b16b02a90b63c21` (`Requires-Python: !=3.15,>=3.11`, upload 2026-11-06T11:54:10Z) | `https://pypi.org/pypi/apache-airflow/3.4.0/json`; bản phát hành `https://github.com/apache/airflow/releases` (tag `3.4.0`, 2026-11-06T13:08:22Z); vòng đời: `https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html` (3.x là dòng hiện hành, bản 3.4.0; 2.11 đã EOL, kết thúc hỗ trợ 2026-04-22) |
| Ràng buộc cài Airflow | `constraints-3.4.0-3.11.txt`, sha256 `22ac327cdf6fe75b8d873ab363455bbe0645c03cee3e3896c6513d583c25e5f8` (751 dòng, sinh 2026-11-06T12:16:48) | `https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.11.txt`. T60 phải chép tệp này vào repo (đề xuất `infra/airflow/constraints-3.4.0-3.11.txt`, gỡ dòng `confluent-kafka==2.16.1`, xem phát hiện 3) để lúc build không phải tải gì khác ngoài PyPI. |
| Python của image Airflow | `3.11.11`, cùng base `python:3.11.11-slim-bookworm@sha256:081075da77b2b55c23c088251026fb69a7b2bf92471e491ff5fd75c192fd38e5` (digest image index) với image Spark | Registry API `https://registry-1.docker.io/v2/library/python/manifests/3.11.11-slim-bookworm`. Airflow hỗ trợ 3.11: `https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html` (*"Python: 3.11, 3.12, 3.13, 3.14"*). Spark 3.5.8 hỗ trợ Python 3.8+ (ADR-0001). |
| Java của image Airflow | `openjdk-17-jre-headless` (Debian bookworm, `17.0.21.1+2-1~deb12u1` tại ngày truy cập), cùng ADR-0001 | `https://packages.debian.org/search?keywords=openjdk-17-jre-headless&searchon=names&suite=all&section=all` (có ở bookworm; không có ở trixie, forky) |
| Spark trong image Airflow | `3.5.8-bin-hadoop3` kèm 6 JAR đã ghim, lấy bằng `FROM n06-spark:3.5.8-py311` | ADR-0001, `docs/compatibility-matrix.md` |
| PostgreSQL làm metadata DB và application DB | `postgres:16.6-bookworm` (đã có trong Compose, không đổi) | Airflow: *"PostgreSQL: 14, 15, 16, 17, 18"* (`.../installation/prerequisites.html`). Metabase: *"supports the oldest supported version of PostgreSQL through the latest stable version"* (`https://www.metabase.com/docs/latest/installation-and-operation/configuring-application-database`); tài liệu không nêu số tối thiểu. PostgreSQL 16 còn hỗ trợ tới 2028-11-09 (`https://www.postgresql.org/support/versioning/`). |

Ghi chú về cách chọn:

- **Áp quy tắc I.2.** Quy tắc 1 đến 4 là của bộ Spark nên không đổi. Quy tắc 5 áp cho Airflow: Python 3.11 nằm trong danh sách Spark 3.5.8 hỗ trợ và trong danh sách Airflow 3.4.0 hỗ trợ. Cấm `latest` và cấm tải động áp cho cả hai: Metabase ghim tag cộng digest; Airflow ghim phiên bản PyPI cộng sha256 wheel cộng constraints đã chép vào repo. Lockfile: `uv.lock` hiện chỉ cho môi trường host và image Spark không dùng nó; Airflow dùng constraints đã commit làm lockfile của image (xem câu hỏi mở 5).
- **Vì sao Metabase `v0.64.9` chứ không `v0.65.x`.** Xem phát hiện 1. Các tag `v0.64.9.1` và `v0.64.9.2` trên Docker Hub là bản dựng lại trong ngày kế tiếp, không dùng để giữ digest ổn định. GitHub Releases của dự án mới liệt kê tới `v0.64.8` ở thời điểm truy cập, còn trang version-support và Docker Hub đã có `64.9`; ADR theo trang version-support và digest trực tiếp từ registry.
- **Vì sao Airflow `3.4.0` chứ không `3.3.2` hoặc `2.11`.** Trang supported-versions chỉ coi dòng 3.x hiện hành (3.4.0) là còn được bảo trì và khuyến nghị cài bản mới nhất; 2.11 đã kết thúc hỗ trợ. `3.4.0` mới phát hành 21 ngày trước ngày truy cập, đây là rủi ro đã biết (bản `.0`); phương án lùi là `3.3.2` (PyPI/image 2026-09-17, image `apache/airflow:3.3.2-python3.11` index digest `sha256:a6685a7a708ed6fa910c2c05458d7f2b0db9cca5c3bf58a72ded389767ad3e65`, nền bookworm), chỉ đổi số phiên bản và tệp constraints. Xem câu hỏi mở 1.
- **Kiểm chứng phân giải phụ thuộc (không cài).** `uv pip compile` với Python 3.11 và constraints trên (chỉ phân giải, không tải wheel để cài):
  - `apache-airflow==3.4.0` + `pyspark==3.5.8` + `psycopg[binary]`: phân giải được (`py4j==0.10.9.9`, `psycopg==3.3.6`, `apache-airflow-providers-standard==1.21.0` kéo theo bởi `apache-airflow-core`).
  - Thêm `confluent-kafka==2.16.2`: **không phân giải được** khi giữ nguyên constraints (`confluent-kafka==2.16.1` đối lập `==2.16.2`); gỡ dòng đó khỏi constraints thì phân giải được và chọn `2.16.2`. Airflow core không phụ thuộc `confluent-kafka`; dòng ghim chỉ do môi trường "all extras" sinh ra.
  - Lệnh kiểm lại: `uv pip compile req.in -c constraints-3.4.0-3.11.txt --python-version 3.11 --no-header`.

### 2. Cách Airflow gọi Spark thật

**Chọn: phương án A, đúng đặc tả.** Image `n06-airflow` build `FROM n06-spark:3.5.8-py311`, rồi `pip install apache-airflow==3.4.0 --constraint <constraints đã chép vào repo>` và `pip install .` (gói `n06`). Image có Java 17, bản Spark 3.5.8 và 6 JAR ở `/opt/spark`, Python 3.11.11, Airflow và `n06`. DAG dùng `BashOperator` (nhập từ `airflow.providers.standard.operators.bash`, `apache-airflow-providers-standard==1.21.0` là phụ thuộc của core) gọi `n06 process-day ...`, `n06 load-mart ...`, `n06 validate ...`. Executor là `LocalExecutor`, nên tiến trình Spark driver (`local[N]`) chạy là tiến trình con của container `airflow-scheduler`, đúng câu *"Spark chạy client mode ngay trong container của task"*.

Phụ thuộc mới: `apache-airflow==3.4.0` (cộng cây phụ thuộc theo constraints) trong image `n06-airflow`; thư mục `infra/airflow/` (Dockerfile và constraints); 3 đến 4 service Compose (mục 3); các biến bí mật mới trong `.env.example` (mục 3). Không thêm thư viện vào image Spark.

Bằng chứng "Spark thật" T60 phải nộp (checklist I.15): `docker compose exec airflow-scheduler sh -c 'command -v spark-submit && java -version && spark-submit --version'` in ra Spark 3.5.8 và Java 17; một DAG run thật gọi `n06 process-day` trên fixture và ghi Gold; DAG không chứa `spark-submit`, đường dẫn Spark hay logic Spark.

Phương án đã loại:

| Phương án | Phụ thuộc mới | Lý do loại |
|---|---|---|
| B. `DockerOperator` (`apache-airflow-providers-docker==4.6.1`) chạy container Spark | Docker socket mount vào Airflow, provider docker | Đặc tả cấm: *"không mount Docker socket"*. Mount socket cho tiến trình Airflow quyền root trên Docker host. |
| C. `docker compose exec spark spark-submit` từ task | Docker CLI và socket trong Airflow | Cùng lý do với B; thêm việc DAG gọi `spark-submit` nằm ở container khác, đúng điều đặc tả cấm (*"Không để DAG tham chiếu một `spark-submit` không tồn tại trong container Airflow"*). |
| D. `SparkSubmitOperator` (`apache-airflow-providers-apache-spark==6.3.4`) | Provider (ràng buộc `pyspark-client>=4.0.0`, tức client Spark 4, theo metadata PyPI của 6.3.4) và binary `spark-submit` trong container Airflow | Đưa logic cấu hình Spark vào DAG, trái *"không sao chép logic Spark vào DAG"*; kéo `pyspark-client` 4.x cạnh `pyspark==3.5.8` (cả hai cung cấp gói `pyspark`; `uv pip compile` không báo lỗi nhưng đây là rủi ro runtime chưa kiểm chứng bằng cài đặt). Vẫn cần `spark-submit` trong container Airflow nên không tiết kiệm được gì so với A. |
| E. `SSHOperator` vào container `spark` | Provider ssh, sshd trong image Spark | Thêm cổng và khóa SSH, sửa image Spark; không có lợi so với A. |
| F. Gọi `spark://spark:7077` làm mặc định | Worker Spark | Service `spark` hiện chỉ chạy `Master`, chưa có worker; đặc tả ghi `spark://...` chỉ *"khi bật profile cluster"*. Cấu hình cluster vẫn chạy được từ A bằng cách đổi master qua cấu hình, nên không loại khả năng, chỉ không dùng làm mặc định. |
| G. Dùng image `apache/airflow:3.4.0-python3.11` rồi thêm Java và Spark | JDK từ apt, tải Spark | Image này (index digest `sha256:be92e15d57c5be726fc39a91cd09cce8f7488483b67979e0fb74c1a2c7cfb4e2`, Python 3.11.18, Debian 13) không cài được `openjdk-17-jre-headless`; dùng JDK 21 là ngoài danh sách Java mà ADR-0001 đã chốt cho Spark 3.5.8, còn tải Temurin 17 là thêm một nguồn tải JDK ngoài ADR-0001. Python 3.11.18 lệch 3.11.11 của image Spark, nên khó nói "dùng chung một phiên bản Python". Giữ làm phương án lùi nếu A thất bại ở bước build. |

### 3. Compose (mô tả, chưa sửa tệp)

Quy ước chung: tên service, mạng `n06-net`, `restart: unless-stopped` như các service hiện có; không tag `latest`; mọi cổng host, mật khẩu và khóa lấy từ `.env`/cấu hình, mật khẩu bắt buộc dùng cú pháp `${VAR:?...}` như `postgres`; không secret trong tệp.

**Profile.** Tách khỏi `core`:

- `bi`: service `metabase`.
- `orchestration`: các service `airflow-*`.

Cả hai cùng `depends_on` `postgres` (profile `core`), nên lệnh chạy phải kèm `core`. Chạy: `docker compose --profile core --profile bi up -d --wait`, `docker compose --profile core --profile orchestration up -d --wait`. Dừng khi đo benchmark: `docker compose --profile bi --profile orchestration stop` (giữ volume). `infra/README.md` và `docs/runbook.md` phải ghi các lệnh này (T43 và T60). Cờ `services.metabase_enabled` và `services.airflow_enabled` trong `config/*.toml` là cờ cho ứng dụng (ví dụ `n06 doctor` có kiểm tra hay không); Compose profile mới là cơ chế bật tắt thật.

**Metabase (`metabase`, profile `bi`)**

- `image: metabase/metabase:v0.64.9@sha256:b204168ce18470a9f2cba59470659b981dc39677ffa1dbfaf3551498034574a9`.
- Application database là PostgreSQL: `MB_DB_TYPE=postgres`, `MB_DB_HOST=postgres`, `MB_DB_PORT=5432`, `MB_DB_DBNAME=${N06_POSTGRES_METABASE_DB}`, `MB_DB_USER=${N06_POSTGRES_METABASE_USER}`, `MB_DB_PASS=${N06_POSTGRES_METABASE_PASSWORD:?...}`. Tên biến `MB_DB_*` lấy từ tài liệu Metabase (`.../configuring-application-database`).
- `JAVA_OPTS=-Xmx${N06_METABASE_HEAP:-1g}`; `mem_limit` đề xuất 1536m (xem mục 4). `MB_CHECK_FOR_UPDATES=false`, `MB_ANON_TRACKING_ENABLED=false` (cả hai có trong danh sách biến môi trường của tài liệu): bản đã ghim nên không cần gọi ra ngoài.
- Cổng: `"${N06_METABASE_HOST_PORT:-13000}:3000"` (theo mẫu `1xxxx` của các service khác; 3000 là cổng mặc định của Metabase).
- Healthcheck: `curl --fail -I http://localhost:3000/api/health` (đúng ví dụ compose trong tài liệu Docker của Metabase), `start_period` dài (Metabase chạy migration application DB lần đầu). **Chưa kiểm chứng** image có `curl`; T43 phải kiểm trước khi giữ cách này, nếu không có thì dùng tiến trình khác trong image.
- `depends_on: postgres: condition: service_healthy`.
- Volume: Metabase không giữ trạng thái trên đĩa; mọi câu hỏi và dashboard nằm trong database `n06_metabase` (volume `n06_postgres_data`). Vì vậy đề xuất **không** thêm named volume riêng, dù mục 2 của đặc tả viết "có healthcheck và named volumes" (câu hỏi mở 4).
- Kết nối tới mart làm tay trong giao diện, dùng role chỉ đọc (mục 5).

**Airflow (profile `orchestration`)**, image `n06-airflow:3.4.0-py311` (build từ `infra/airflow/Dockerfile`, tag cố định theo mẫu `n06-spark:3.5.8-py311`), executor `LocalExecutor`:

| Service | Lệnh | Healthcheck |
|---|---|---|
| `airflow-init` (một lần, `restart: "no"`) | `airflow db migrate` | không áp dụng; các service còn lại `depends_on` với `service_completed_successfully` |
| `airflow-api-server` | `airflow api-server` | `curl -fsS http://127.0.0.1:8080/api/v2/monitor/health` và đọc JSON (tài liệu nói mã HTTP chỉ phản ánh lời gọi REST, không phản ánh sức khỏe) |
| `airflow-scheduler` | `airflow scheduler` | `airflow jobs check --job-type SchedulerJob --local` (có trong tài liệu check-health) |
| `airflow-dag-processor` | `airflow dag-processor` | tài liệu không có lệnh CLI riêng; đề xuất gọi `/api/v2/monitor/health` của `airflow-api-server` và kiểm trường `dag_processor` (T60 xác minh) |

Không có `airflow-triggerer` vì DAG không dùng deferrable operator.

- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` ghép từ `N06_POSTGRES_AIRFLOW_USER`, `N06_POSTGRES_AIRFLOW_PASSWORD`, `N06_POSTGRES_AIRFLOW_DB`, host `postgres`. Tài liệu Airflow nêu driver mặc định là `psycopg` 3 (URL `postgresql://...` được viết lại thành `postgresql+psycopg://`), nên không cần `psycopg2`. Cần quyền `ALL` trên schema `public`: script init đã `ALTER SCHEMA public OWNER TO` đúng role.
- `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, `AIRFLOW__CORE__LOAD_EXAMPLES=false`, múi giờ mặc định lấy từ `business_timezone` (`Asia/Ho_Chi_Minh`, `config/default.toml`), `AIRFLOW__CORE__PARALLELISM` cấu hình qua `.env` với mặc định 1 (mỗi task là một driver Spark 2g).
- Bí mật mới cần thêm vào `.env.example`, đều bắt buộc bằng `${...:?}`: khóa ký JWT `[api_auth] jwt_secret` (tài liệu: *"Must be the same across all components"*, nếu bỏ trống thì sinh ngẫu nhiên khác nhau theo tiến trình), khóa Fernet, mật khẩu quản trị cho SimpleAuthManager (câu hỏi mở 3). Tên biến Airflow ánh xạ theo mẫu `AIRFLOW__SECTION__KEY`, **chưa kiểm chứng** từng tên trong cấu hình tham chiếu; T60 phải kiểm. URL Execution API cho task (Airflow 3 chạy task qua api-server) cũng cần kiểm chứng ở T60.
- `airflow-scheduler` (nơi tiến trình `n06` và Spark chạy) cần các biến `N06_*` của kho đối tượng và PostgreSQL (mart, control) giống container `spark`, để `n06` đọc đúng endpoint qua cấu hình; `airflow-api-server` và `airflow-dag-processor` không cần khóa kho đối tượng.
- Cổng: `"${N06_AIRFLOW_HOST_PORT:-18081}:8080"` (18080 đang dùng cho Spark UI).
- Volume: named volume `n06_airflow_logs` cho `/opt/airflow/logs`; thư mục `dags/` gắn chỉ đọc từ repo (`./dags:/opt/airflow/dags:ro`); thư mục `config/` gắn chỉ đọc nếu `n06` đọc cấu hình từ đường dẫn tệp, ngược lại đóng gói vào image (theo cách T03 đã chọn).
- Đề xuất `mem_limit` (mục 4): `airflow-api-server` 1g, `airflow-dag-processor` 768m, `airflow-scheduler` 4g (chứa Spark driver).

### 4. Ngân sách RAM

Đo ngày 2026-11-27, chỉ đọc, không dừng hay khởi động container:

```
$ free -m
               total        used        free      shared  buff/cache   available
Mem:            8935        5136         146          20        3652        3582
Swap:           4096          23        4072
$ nproc
16
$ docker stats --no-stream
n06-postgres-1   CPU 34.91%  MEM  36.64MiB / 8.726GiB
n06-spark-1      CPU  0.16%  MEM 309.1MiB  / 8.726GiB
n06-kafka-1      CPU  1.22%  MEM 474.5MiB  / 8.726GiB
n06-minio-1      CPU  0.12%  MEM 130.5MiB  / 8.726GiB
```

Bốn container lõi đang chạy không tải dùng tổng khoảng **951 MiB**. `used` 5136 MB của máy lớn hơn nhiều vì còn các tiến trình ngoài Docker (VS Code, các phiên làm việc khác); số cần dùng để lập ngân sách là `available` = **3582 MB** tại thời điểm đo (số này thay đổi theo phiên đang chạy, chỉ là một lần đo).

Ước tính phần thêm. Nguồn chính thức chỉ có các số sau; phần còn lại là ước tính của người viết, đánh dấu rõ:

| Mục | Số liệu chính thức | Ước tính |
|---|---|---|
| Metabase | Heap mặc định của JVM là một phần tư RAM máy; ví dụ *"set `-Xmx` to `1g` on a machine with 2 GB of RAM"*; *"Leaving 1 to 2 GB of RAM for other processes on the machine is generally enough"* (`https://www.metabase.com/docs/latest/troubleshooting-guide/running`). Không có số RAM tối thiểu cho container. | Heap 1g cộng phần ngoài heap, tổng khoảng 1,3 đến 1,6 GB; đặt `-Xmx1g` và `mem_limit` 1536m. Không để heap mặc định (một phần tư của 8,7 GiB là khoảng 2,2 GiB). |
| Airflow (3 tiến trình, LocalExecutor) | *"use at least 4.00 GB of memory for the Airflow containers"* và *"ideally 8GB"* cho Docker Engine (`https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html`), áp cho stack Celery đủ worker, triggerer, redis, postgres. Không có số theo từng tiến trình. | `api-server`, `scheduler`, `dag-processor` lúc rảnh cộng khoảng 1,0 đến 1,8 GB. |
| Một task chạy Spark trong `airflow-scheduler` | Spark `local[2]`, driver 2g (cấu hình hiện hành của dự án) | Driver 2g cộng phần ngoài heap JVM khoảng 2,5 đến 3 GB trong lúc chạy. |

Kết luận cho máy này (kết luận từ ước tính, cần đo lại bằng `docker stats` khi T43 và T60 chạy thật, số đo thật ghi vào `PROGRESS.md`):

| Chế độ | Phần thêm so với lõi | So với `available` 3582 MB |
|---|---|---|
| `core` | 0 (khoảng 0,95 GB đang chạy) | Dư |
| `core` + `bi` (xem dashboard) | khoảng 1,5 GB | Chạy được, còn khoảng 2 GB |
| `core` + `orchestration` (rảnh) | khoảng 1,0 đến 1,8 GB | Chạy được, nhưng chưa có chỗ cho Spark |
| `core` + `orchestration` + một task Spark đang chạy | khoảng 3,5 đến 4,8 GB | **Không đủ** trên máy như đo hôm nay; chỉ chạy được nếu giải phóng RAM ngoài Docker hoặc tăng RAM cấp cho WSL2 (câu hỏi mở 2) |
| `core` + `bi` + `orchestration` + task Spark | khoảng 5 đến 6,5 GB | **Không chạy được** cùng lúc trên máy này |

Quy tắc vận hành (đề xuất, thành điều khoản khi Accept):

1. Mặc định khi đo benchmark (E1 đến E8): chỉ `core` chạy; `metabase` và `airflow-*` đã `stop`. Mọi báo cáo benchmark ghi dòng `airflow_enabled=false, metabase_enabled=false` từ `config/benchmark.toml` và lệnh `docker compose ps` lúc đo. Không chờ "thiếu RAM" mới tắt: với 9 GB và Spark driver 2g, coi là thiếu.
2. Demo hoặc nghiệm thu dashboard: `core` + `bi`.
3. Demo hoặc nghiệm thu Airflow (T60, T62): `core` + `orchestration`, dùng profile `demo` (10.000 giao dịch), dừng `bi` trong lúc DAG chạy; `AIRFLOW__CORE__PARALLELISM=1`.
4. Nếu cần xem dashboard sau khi DAG chạy xong: dừng `orchestration`, bật `bi`.

### 5. Metabase: dựng dashboard tái lập được cho T43 (hướng dẫn tay)

Provision qua API là P2, nên T43 chỉ cung cấp SQL cộng hướng dẫn từng bước và không tuyên bố tự động hóa (đúng mục 9).

**Chuẩn bị ở PostgreSQL (T43 hoặc T41 thực hiện, ngoài phạm vi ADR này).** Metabase không được dùng `n06_mart_user` (chủ sở hữu, ghi được). Cần một role chỉ đọc cho database mart (đề xuất tên `n06_mart_ro`, `CONNECT` trên database vì script init đã `REVOKE CONNECT ... FROM PUBLIC`, `USAGE` trên schema mart, `SELECT` trên bảng và `ALTER DEFAULT PRIVILEGES` cho bảng mới). Mật khẩu từ `.env`. Script init hiện **không** tạo role này (câu hỏi mở 6). Tên schema mart do T41 chốt.

**Tệp SQL.** `sql/dashboard_queries/` mỗi tệp một truy vấn, tên tệp có số thứ tự và tên báo cáo; đầu tệp có chú thích: tiêu đề câu hỏi trong Metabase, loại biểu đồ, cột trục, collection. SQL thuần PostgreSQL để cũng chạy bằng `psql`, kèm một test tích hợp chạy từng tệp trên mart nạp từ fixture. Phủ các báo cáo của mục 9: số giao dịch theo ngày và kênh; tổng tiền theo currency và status; số dư cuối ngày; số lượng và tỷ lệ đối soát theo trạng thái (mẫu số là số match key cần đối soát, PENDING hiển thị riêng); tuổi sai lệch chưa xử lý nhóm theo trạng thái và khoảng tuổi (`discrepancy_age_seconds`); chất lượng và độ tươi pipeline (từ watermark nguồn và thời điểm mart published); và (P1, T62) truy vấn drill-down từ một sai lệch tới giao dịch, sự kiện, tệp nguồn hoặc tọa độ Kafka.

**Các bước tay (T43 viết thành văn bản trong `sql/dashboard_queries/README.md` hoặc `docs/runbook.md`, kèm ảnh chụp):**

1. `docker compose --profile core --profile bi up -d --wait`; mở `http://localhost:${N06_METABASE_HOST_PORT}`.
2. Màn hình thiết lập lần đầu: tạo tài khoản quản trị (lưu thông tin ở nơi ngoài repo), bỏ qua phần "thêm dữ liệu".
3. Admin, Databases, thêm PostgreSQL: Host `postgres`, Port `5432`, Database `n06_mart`, Username và Password của role chỉ đọc; lọc schema chỉ gồm schema mart; chạy "Sync database schema".
4. Với mỗi tệp SQL: New, SQL query, chọn database mart, dán nội dung, Visualize, chọn loại biểu đồ ghi ở đầu tệp, Save vào collection `N06`.
5. Tạo dashboard `N06`, thêm các câu hỏi theo thứ tự trong README, lưu.
6. Kiểm: số liệu trên dashboard khớp kết quả `psql` của cùng tệp SQL và khớp số kỳ vọng trên fixture (giá trị số dư 93.600 / 56.400 / 93.850 / 56.150 theo TASKS.md T40).

Tái lập sau `down -v`: dashboard nằm trong database `n06_metabase` nên mất theo volume Postgres; tái lập bằng các bước trên từ SQL trong repo. Export hay API để tự động là P2.

## Phương án đã cân nhắc

- **Metabase `v0.65.x` mới nhất:** loại vì hết hỗ trợ 2027-01-01 (không phải LTS). Có thể dùng nếu chủ repo chấp nhận rủi ro sau hạn nộp (câu hỏi mở 1).
- **Metabase bản Enterprise (`v1.x`):** loại, cần token cấp phép.
- **Metabase dùng H2 làm application database:** loại, tài liệu *"Avoid using this default database in production"*, mất dữ liệu khi xóa container; đặc tả đã chốt dùng PostgreSQL riêng.
- **Airflow 2.11.x:** loại, đã kết thúc hỗ trợ 2026-04-22.
- **Airflow `3.3.2`:** là phương án lùi hợp lệ (nền bookworm), loại vì không phải bản bảo trì hiện hành.
- **Image `apache/airflow` chính thức làm nền:** loại, xem phương án G trong mục 2.
- **CeleryExecutor (redis, worker):** loại, thêm hai service và RAM, không có lợi cho một máy; Spark local mode ngay trong container task vẫn chỉ cần LocalExecutor.
- **Cổng metabase và airflow cố định trong Compose:** loại, theo quy tắc không hardcode.

## Hệ quả

- **T43 (C):** dùng Metabase `v0.64.9`; viết SQL và hướng dẫn tay ở mục 5; cần role chỉ đọc cho mart và service `metabase` trong Compose; cập nhật `docs/compatibility-matrix.md`, `infra/README.md`, `.env.example`, `docs/runbook.md`.
- **T60 (C):** dùng Airflow `3.4.0`, `BashOperator` gọi `n06`; tạo `infra/airflow/Dockerfile` và constraints, thêm 4 service vào Compose, thêm bí mật vào `.env.example`; kiểm các điểm "chưa kiểm chứng" ở mục 3; đo RAM thật; cập nhật compatibility matrix.
- **`docs/compatibility-matrix.md`:** thêm các dòng ở mục 1 của ADR này, kèm lệnh kiểm lại (xem phụ lục).
- **`docs/benchmark-method.md` và báo cáo benchmark:** ghi trạng thái Airflow/Metabase lúc đo, theo quy tắc 1 mục 4.
- **ADR-0001:** phần "Hệ quả" về Airflow vẫn đúng; ADR này bổ sung phiên bản Airflow và Metabase, không sửa ADR-0001.
- **Không làm trong ADR này:** sửa Compose, Dockerfile, `pyproject.toml`, `uv.lock`, `.env.example`, init Postgres, cấu hình.

## Câu hỏi mở

Cả chín câu đã được trả lời, xem mục "Quyết định đã chốt" ở đầu ADR; nội dung dưới đây giữ làm lịch sử.

Spec im lặng hoặc ADR cần chủ repo quyết; người viết đề xuất, không tự chọn.

1. **Chọn bản có hỗ trợ dài hơn hay bản mới nhất?** Đề xuất: Metabase `v0.64.9` (LTS) và Airflow `3.4.0`. Nếu chủ repo muốn tránh bản `.0` của Airflow, đổi sang `3.3.2` (nền bookworm, không phải bản bảo trì hiện hành). Người viết không tìm được tiêu chí độ tuổi tối thiểu của bản phát hành trong đặc tả.
2. **Ngân sách RAM của WSL2.** Đo thật cho thấy `core` + Airflow + một task Spark không vừa khoảng 3,5 GB `available`. Đề xuất: chấp nhận quy tắc ở mục 4 (không bật cùng lúc). Hoặc chủ repo tăng giới hạn bộ nhớ WSL2 (`.wslconfig`) nếu máy vật lý còn RAM; người viết chưa biết RAM của máy chủ.
3. **Xác thực của Airflow và Metabase.** SimpleAuthManager của Airflow *"is intended for development and testing"* (tài liệu); mật khẩu mặc định là tự sinh và in ra log. Đề xuất: giữ SimpleAuthManager với danh sách người dùng cấu hình qua `.env`, ghi hạn chế vào báo cáo (đặc tả đặt ngoài phạm vi *"bảo mật/chứng nhận ngân hàng sản xuất"*); T60 tìm cách đặt mật khẩu từ `.env` và ghi bước xác minh. Metabase: tài khoản quản trị tạo tay lần đầu, khóa mã hóa `MB_ENCRYPTION_SECRET_KEY` đặt từ `.env` (tên biến này chưa được đọc từ tài liệu trong lần tra này).
4. **"Named volumes" cho Metabase.** Đặc tả viết *"có healthcheck và named volumes"* cho cả danh sách. Metabase không có dữ liệu trên đĩa (trạng thái ở Postgres). Đề xuất: không thêm volume giả; ghi rõ trong `infra/README.md` rằng trạng thái nằm ở `n06_postgres_data`. Chủ repo xác nhận cách hiểu này.
5. **Lockfile cho image Airflow và quy tắc Python "một phiên bản".** `uv.lock` phục vụ host, image Spark không dùng. Đề xuất: constraints đã commit (`infra/airflow/constraints-3.4.0-3.11.txt`, bỏ dòng `confluent-kafka`) làm lockfile của image Airflow, và hiểu "dùng chung một phiên bản Python" là cùng nhánh 3.11 (ADR-0001 đã ghi `3.11.x`); với phương án A hai image có cùng `3.11.11` nên không lệch. Ngoài ra base `python:3.11.11-slim-bookworm` mới ghim bằng tag, chưa bằng digest trong Dockerfile hiện có; đề xuất ghim digest như ở mục 1 khi T60 dựng image (chủ repo quyết có áp cho cả `infra/spark/Dockerfile` không, ngoài phạm vi ADR này).
6. **Role chỉ đọc cho mart.** Script init (T02) không tạo; Metabase cần. Đề xuất: bổ sung vào script init và `.env.example` (`N06_POSTGRES_MART_RO_*`) hoặc vào migration mart của T41; chủ repo chọn nơi và chủ sở hữu.
7. **Tham số trong SQL dashboard.** Bộ lọc theo ngày và drill-down (T62) cần tham số. Đề xuất: P0 dùng SQL không tham số để chạy được bằng `psql`; T62 dùng cú pháp biến của Metabase kèm quy ước thay thế trong test. Chủ repo xác nhận hoặc yêu cầu cách khác.
8. **Tên profile Compose.** Đề xuất `bi` và `orchestration`, đang không có trong đặc tả, ADR hiện hành hoặc `config/`. Chủ repo xác nhận tên.
9. **PostgreSQL 16.6.** ADR này chỉ xác nhận tương thích. Trang versioning của PostgreSQL liệt kê `16.16` là bản nhỏ mới nhất; việc nâng tag `postgres:16.6-bookworm` không thuộc ADR này.

## Phụ lục: lệnh kiểm lại các số trong mục 1

```bash
# Digest Metabase (index) và tag/ngày
curl -s "https://hub.docker.com/v2/repositories/metabase/metabase/tags?name=v0.64.9" | python3 -m json.tool | grep -E '"name"|"digest"'
# Hoặc, nếu có buildx: docker buildx imagetools inspect metabase/metabase:v0.64.9

# Wheel Airflow và Python yêu cầu
curl -s https://pypi.org/pypi/apache-airflow/3.4.0/json | python3 -m json.tool | grep -E 'requires_python|sha256|filename'

# Constraints và xung đột confluent-kafka
curl -sS https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.11.txt -o constraints.txt
sha256sum constraints.txt
grep -i '^confluent-kafka' constraints.txt
printf 'apache-airflow==3.4.0\npyspark==3.5.8\nconfluent-kafka==2.16.2\npsycopg[binary]\n' > req.in
uv pip compile req.in -c constraints.txt --python-version 3.11 --no-header          # lỗi: 2.16.1 và 2.16.2
grep -vi '^confluent-kafka==' constraints.txt > c2.txt
uv pip compile req.in -c c2.txt --python-version 3.11 --no-header                   # phân giải được

# Digest image python nền
curl -s "https://hub.docker.com/v2/repositories/library/python/tags?name=3.11.11-slim-bookworm&page_size=5" | python3 -m json.tool | grep -E '"name"|^ *"digest"'  # lấy dòng digest cấp tag (ngoài mảng images)
```
