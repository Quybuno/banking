# ADR-0001: Bộ phiên bản Spark, Hadoop, AWS SDK, Kafka Connector, Java và Python

- Trạng thái: Accepted
- Người viết: C (Quý — khởi tạo Python tại T00), A (Mạnh — hoàn thiện và kiểm chứng bộ phiên bản tại T04)
- Người duyệt: B (Quang), C (Quý)
- Ngày: 2026-11-22
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 2 ("Quy tắc chọn bộ phiên bản" và "Cách Airflow gọi Spark")

## Bối cảnh
Đặc tả Phần I mục 2 quy định quy tắc bắt buộc khi chọn bộ phiên bản công nghệ:
> "1. Chọn một bản Spark 3.5.x dựng sẵn kèm Hadoop 3 làm gốc; mọi thành phần khác suy ra từ bản này.
> 2. `hadoop-aws` phải **đúng bằng** phiên bản Hadoop mà bản Spark đó đóng gói (đọc từ thư mục `jars/` của bản phân phối, không đoán).
> 3. AWS SDK bundle phải **đúng phiên bản** mà POM của `hadoop-aws` đó khai báo phụ thuộc.
> 4. `spark-sql-kafka-0-10` cùng phiên bản Spark và cùng phiên bản Scala; liệt kê thêm các JAR phụ thuộc bắt buộc của connector.
> 5. Java và Python lấy trong danh sách bản Spark đó hỗ trợ chính thức; Airflow và PySpark dùng chung một phiên bản Python.
>
> Ghi kết quả vào compatibility matrix kèm nguồn kiểm chứng, commit lockfile. Một smoke test đọc/ghi Parquet qua S3A tới MinIO và đọc một topic Kafka phải chạy được trước M2."

Ngoài ra, dự án cấm dùng tag `latest` và cấm tải dependency động từ Internet mỗi lần chạy job (`--packages`). Toàn bộ connector JAR phải được tải, kiểm tra checksum và đóng gói sẵn vào image Spark trong bước build.

## Quyết định

Chốt bộ phiên bản dưới đây (chi tiết mã băm và lệnh kiểm lại nằm ở `docs/compatibility-matrix.md`):

| Thành phần | Phiên bản chốt | Nguồn kiểm chứng |
|---|---|---|
| Bản phân phối Spark gốc | `3.5.8` (`spark-3.5.8-bin-hadoop3.tgz`) | `https://archive.apache.org/dist/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz.sha512` |
| Hadoop (đóng gói sẵn trong Spark) | `3.3.4` | `/opt/spark/jars/hadoop-client-api-3.3.4.jar`, `/opt/spark/jars/hadoop-client-runtime-3.3.4.jar` và `<hadoop.version>3.3.4</hadoop.version>` trong `spark-parent_2.12-3.5.8.pom` |
| Scala (đóng gói sẵn trong Spark) | `2.12.18` (binary `2.12`) | `/opt/spark/jars/scala-library-2.12.18.jar` và `<scala.version>2.12.18</scala.version>` trong `spark-parent_2.12-3.5.8.pom` |
| `org.apache.hadoop:hadoop-aws` | `3.3.4` | Bằng đúng phiên bản `hadoop-client-api-3.3.4.jar` trong `/opt/spark/jars/` |
| `com.amazonaws:aws-java-sdk-bundle` | `1.12.262` | `<aws-java-sdk.version>1.12.262</aws-java-sdk.version>` trong `hadoop-project-3.3.4.pom` (cha của `hadoop-aws-3.3.4.pom`) |
| `org.apache.spark:spark-sql-kafka-0-10_2.12` | `3.5.8` | Cùng phiên bản Spark `3.5.8` và Scala binary `2.12` trên Maven Central |
| `org.apache.spark:spark-token-provider-kafka-0-10_2.12` | `3.5.8` | Compile dependency bắt buộc trong `spark-sql-kafka-0-10_2.12-3.5.8.pom` |
| `org.apache.kafka:kafka-clients` | `3.4.1` | Compile dependency bắt buộc trong `spark-sql-kafka-0-10_2.12-3.5.8.pom` (`<kafka.version>3.4.1</kafka.version>`) |
| `org.apache.commons:commons-pool2` | `2.11.1` | Compile dependency bắt buộc trong `spark-sql-kafka-0-10_2.12-3.5.8.pom` (`<commons-pool2.version>2.11.1</commons-pool2.version>`) |
| Java Runtime (JRE/JDK) | `OpenJDK 17` (`17.0.x`, `openjdk-17-jre-headless`) | Trang chủ tài liệu Spark 3.5.8 (`https://spark.apache.org/docs/3.5.8/`: *"Spark runs on Java 8/11/17, Scala 2.12/2.13, Python 3.8+, and R 3.5+"*) |
| Python | `3.11` (`>=3.11,<3.12`, base image `python:3.11.11-slim-bookworm`) | Hỗ trợ chính thức bởi Spark 3.5.8 (`Python 3.8+`), khớp T00 (`pyproject.toml`, `.github/workflows/ci.yml`) và tương thích Apache Airflow 2.x |

### Các JAR phụ thuộc bắt buộc đã có sẵn trong `/opt/spark/jars/` (không tải trùng)
Khi đối chiếu `spark-sql-kafka-0-10_2.12-3.5.8.pom` và `kafka-clients-3.4.1.pom` với `/opt/spark/jars/` của bản phân phối `spark-3.5.8-bin-hadoop3`:
- `com.google.code.findbugs:jsr305:3.0.0` → đã có `/opt/spark/jars/jsr305-3.0.0.jar`.
- `org.apache.spark:spark-tags_2.12:3.5.8` → đã có `/opt/spark/jars/spark-tags_2.12-3.5.8.jar`.
- Các thư viện nén và log của `kafka-clients` (`lz4-java`, `snappy-java`, `zstd-jni`, `slf4j-api`) → đều đã có sẵn trong `/opt/spark/jars/`.

## Phương án đã cân nhắc
1. **Phiên bản Spark 3.5.x:**
   - Chọn `3.5.8` vì là bản phát hành ổn định trên nhánh 3.5.x có sẵn gói `spark-3.5.8-bin-hadoop3.tgz` trên Apache CDN và Apache Archive, đóng gói Hadoop `3.3.4` ổn định.
2. **Phiên bản Java (Java 8 vs Java 11 vs Java 17):**
   - Không chọn Java 8 vì đã bị deprecated từ Spark 3.5.0 (trước 8u371).
   - Không chọn Java 11 vì tài liệu Spark 3.5.8 lưu ý Java 11 cần cấu hình thủ công `-Dio.netty.tryReflectionSetAccessible=true` cho Apache Arrow.
   - Chọn **OpenJDK 17** vì là bản LTS được Spark 3.5.8 hỗ trợ chính thức và có sẵn trong Debian Bookworm (`python:3.11.11-slim-bookworm`).
3. **Phiên bản Python (3.8 / 3.9 / 3.10 vs 3.11):**
   - Giữ nguyên **Python 3.11** đã chọn tại T00: tương thích đồng thời với `verify_contract.py` (3.10+), `tomllib` trong stdlib (3.11+), PySpark 3.5.8 và Apache Airflow 2.x.

## Hệ quả
- **Đối với TASK T02 (Docker Compose):**
  - Service Spark trong `docker-compose.yml` sẽ build hoặc sử dụng image từ `infra/spark/Dockerfile` (tag cố định `n06-spark:3.5.8-py311`), không dùng tag `latest`.
- **Đối với TASK T05 (Smoke test S3A và Kafka):**
  - Khi chạy `spark-submit` hoặc `SparkSession`, toàn bộ 6 JAR connector đã nằm sẵn trong `/opt/spark/jars/`, tuyệt đối không dùng `--packages` hay tải JAR lúc runtime.
  - Cấu hình S3A dùng `org.apache.hadoop.fs.s3a.S3AFileSystem` và `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` tương thích với `hadoop-aws:3.3.4` và `aws-java-sdk-bundle:1.12.262`.
- **Đối với TASK T60 (Airflow gọi Spark ở P1):**
  - Image Airflow sẽ được dựng trên cùng phiên bản **Python 3.11**, **OpenJDK 17**, bản phân phối Spark `3.5.8-bin-hadoop3` kèm 6 JAR trong `/opt/spark/jars/` và gói `n06`, để `BashOperator` gọi trực tiếp CLI `n06 ...` chạy Spark client mode ngay trong container theo đặc tả Phần I mục 2.
