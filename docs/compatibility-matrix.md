# Ma trận tương thích bộ phiên bản (Compatibility Matrix) — N06

Tài liệu này ghi nhận bộ phiên bản đã được kiểm chứng từ nguồn thật theo đúng quy tắc tại `docs/spec.md` Phần I mục 2 và `docs/adr/0001-bo-phien-ban.md`.

---

## 1. Bảng phiên bản tổng thể và nguồn kiểm chứng

| Thành phần | Artifact / Gói | Phiên bản ghim | Checksum đã kiểm chứng (SHA-512 / SHA-1 / SHA-256) | Nguồn kiểm chứng đã đọc | Cách kiểm lại |
|---|---|---|---|---|---|
| **Apache Spark** | `spark-3.5.8-bin-hadoop3.tgz` | `3.5.8` | SHA-512: `5d5d2e6e111182ee1210d4a46a17ae27107c7ccc70433da0ded3d8197e027f949b00445bb5678ed0c1d5c59f12993d9b9ed12e43686d5aad89a9ec570e93a083` | 1. `https://archive.apache.org/dist/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz.sha512`<br>2. `spark-submit --version` trong container | `docker run --rm n06-spark:3.5.8-py311 spark-submit --version` |
| **Apache Hadoop (bundled)** | `hadoop-client-api`, `hadoop-client-runtime` | `3.3.4` | Đóng gói trong `spark-3.5.8-bin-hadoop3.tgz` | 1. File `/opt/spark/jars/hadoop-client-api-3.3.4.jar` và `/opt/spark/jars/hadoop-client-runtime-3.3.4.jar`<br>2. Thẻ `<hadoop.version>3.3.4</hadoop.version>` tại `https://repo1.maven.org/maven2/org/apache/spark/spark-parent_2.12/3.5.8/spark-parent_2.12-3.5.8.pom` | `docker run --rm n06-spark:3.5.8-py311 sh -c "ls -1 /opt/spark/jars/hadoop-client-*.jar"` |
| **Scala (bundled)** | `scala-library` | `2.12.18` (binary `2.12`) | Đóng gói trong `spark-3.5.8-bin-hadoop3.tgz` | 1. File `/opt/spark/jars/scala-library-2.12.18.jar`<br>2. Thẻ `<scala.version>2.12.18</scala.version>` tại `https://repo1.maven.org/maven2/org/apache/spark/spark-parent_2.12/3.5.8/spark-parent_2.12-3.5.8.pom` | `docker run --rm n06-spark:3.5.8-py311 sh -c "ls -1 /opt/spark/jars/scala-library-*.jar"` |
| **Hadoop AWS (S3A)** | `org.apache.hadoop:hadoop-aws` | `3.3.4` | SHA-1: `a65839fbf1869f81a1632e09f415e586922e4f80`<br>SHA-256: `53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a` | 1. Quy tắc I.2.2: bằng đúng phiên bản `hadoop-client-api-3.3.4.jar`<br>2. `https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar.sha1` | `docker run --rm n06-spark:3.5.8-py311 sha1sum /opt/spark/jars/hadoop-aws-3.3.4.jar` |
| **AWS Java SDK Bundle** | `com.amazonaws:aws-java-sdk-bundle` | `1.12.262` | SHA-1: `02deec3a0ad83d13d032b1812421b23d7a961eea`<br>SHA-256: `873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5` | 1. Dependency `com.amazonaws:aws-java-sdk-bundle` trong `https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.pom`<br>2. Thẻ `<aws-java-sdk.version>1.12.262</aws-java-sdk.version>` trong `https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-project/3.3.4/hadoop-project-3.3.4.pom`<br>3. `https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar.sha1` | `docker run --rm n06-spark:3.5.8-py311 sha1sum /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar` |
| **Spark SQL Kafka Connector** | `org.apache.spark:spark-sql-kafka-0-10_2.12` | `3.5.8` | SHA-1: `cbeb2a0efa9b9921870f2bbd1860f9a26da932d7`<br>SHA-256: `d2b7068eabce2a1267103625e58efae32f7ee384db1cbc22424532ad2ed95fb4` | 1. `https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.pom`<br>2. `https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.jar.sha1` | `docker run --rm n06-spark:3.5.8-py311 sha1sum /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.8.jar` |
| **Spark Kafka Token Provider** | `org.apache.spark:spark-token-provider-kafka-0-10_2.12` | `3.5.8` | SHA-1: `e6c11ec7275c0b03929fed2ec52eaaf75c411ddb`<br>SHA-256: `85f44359b60943d262f37d1327ebb7931fe47e0392286fe3023299e25b84b0d8` | 1. Compile dependency trong `spark-sql-kafka-0-10_2.12-3.5.8.pom`<br>2. `https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.8/spark-token-provider-kafka-0-10_2.12-3.5.8.jar.sha1` | `docker run --rm n06-spark:3.5.8-py311 sha1sum /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.8.jar` |
| **Kafka Clients** | `org.apache.kafka:kafka-clients` | `3.4.1` | SHA-1: `a2f55251deb3d31a21903a0cfbd4d1fa6b7463ca`<br>SHA-256: `9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6` | 1. Compile dependency trong `spark-sql-kafka-0-10_2.12-3.5.8.pom` và `<kafka.version>3.4.1</kafka.version>` trong `spark-parent_2.12-3.5.8.pom`<br>2. `https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar.sha1` | `docker run --rm n06-spark:3.5.8-py311 sha1sum /opt/spark/jars/kafka-clients-3.4.1.jar` |
| **Apache Commons Pool 2** | `org.apache.commons:commons-pool2` | `2.11.1` | SHA-1: `8970fd110c965f285ed4c6e40be7630c62db6f68`<br>SHA-256: `ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83` | 1. Compile dependency trong `spark-sql-kafka-0-10_2.12-3.5.8.pom` và `<commons-pool2.version>2.11.1</commons-pool2.version>` trong `spark-parent_2.12-3.5.8.pom`<br>2. `https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar.sha1` | `docker run --rm n06-spark:3.5.8-py311 sha1sum /opt/spark/jars/commons-pool2-2.11.1.jar` |
| **Java Runtime** | `openjdk-17-jre-headless` (Debian Bookworm) | `17.0.21.1` (Java 17) | Gói Debian `bookworm` | Tài liệu chính thức Spark 3.5.8: `https://spark.apache.org/docs/3.5.8/` (*"Spark runs on Java 8/11/17, Scala 2.12/2.13, Python 3.8+, and R 3.5+"*) | `docker run --rm n06-spark:3.5.8-py311 java -version` |
| **Python** | Base image `python:3.11.11-slim-bookworm` | `3.11.11` (nhánh `3.11.x`) | Docker official image `python:3.11.11-slim-bookworm` | 1. Tài liệu chính thức Spark 3.5.8 (`Python 3.8+`)<br>2. Khớp `requires-python = ">=3.11,<3.12"` tại `pyproject.toml` (T00) | `docker run --rm n06-spark:3.5.8-py311 python3 --version` |
| **Kafka client (Python, publish)** | `confluent-kafka` (wheel `confluent_kafka-2.16.2-cp311-cp311-manylinux_2_28_x86_64.whl`, librdkafka 2.16.0 đóng gói) | `2.16.2` | SHA-256 wheel: `f4cae8bc8e5208def5cfe9ac07b85c934c07e5424011608b3bac713cbcc82c3e` | 1. `https://pypi.org/pypi/confluent-kafka/json`<br>2. `https://raw.githubusercontent.com/confluentinc/librdkafka/master/INTRODUCTION.md` (Idempotent Producer)<br>3. `docs/adr/0010-kafka-client-va-quy-tac-publish.md` | `uv run python -c "import confluent_kafka as c; print(c.version(), c.libversion())"`; chỉ thuộc môi trường host `uv sync`, không nằm trong image Spark; chạy với broker `apache/kafka:3.7.2` của Compose bằng `tests/integration/test_publish_kafka.py` |
| **PostgreSQL client trong image Spark** | `psql` và `libpq.so.5` chép từ `postgres:16.6-bookworm` | `16.6` | Digest image: `sha256:557fea37a744d5f4c8faab304b0a90858b53ab119735a88c131fd19dab802f36` (cùng digest ghim ở `docker-compose.yml`) | `docs/adr/0012-driver-postgres-va-ghi-object-minio.md` quyết định 1 | `docker run --rm n06-spark:3.5.8-py311 psql --version` |
| **Matplotlib (nhóm `benchmark`)** | `matplotlib` (wheel `matplotlib-3.11.2-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.whl`) | `3.11.2` | SHA-256 wheel: `07d9b9fa60cd4c393692f50d0bb03123242ddf61c99bb0e95e75feb354e7c1a8` | `https://pypi.org/pypi/matplotlib/json` (truy cập 2026-11-28, `requires_python: >=3.11`, upload `2026-09-11T19:03:04Z`); `docs/adr/0007-do-benchmark.md` Q10 | `uv run --extra benchmark python -c "import matplotlib; print(matplotlib.__version__)"` |

---

## 2. Cây phụ thuộc compile của `spark-sql-kafka-0-10_2.12:3.5.8`

Theo `https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.8/spark-sql-kafka-0-10_2.12-3.5.8.pom`, các dependency có `<scope>compile</scope>` gồm:

| GroupId : ArtifactId | Phiên bản | Trạng thái trong `/opt/spark/jars/` |
|---|---|---|
| `org.apache.spark:spark-sql-kafka-0-10_2.12` | `3.5.8` | Tải thêm và kiểm tra SHA-1 trong `infra/spark/Dockerfile` |
| `org.apache.spark:spark-token-provider-kafka-0-10_2.12` | `3.5.8` | Tải thêm và kiểm tra SHA-1 trong `infra/spark/Dockerfile` |
| `org.apache.kafka:kafka-clients` | `3.4.1` | Tải thêm và kiểm tra SHA-1 trong `infra/spark/Dockerfile` |
| `org.apache.commons:commons-pool2` | `2.11.1` | Tải thêm và kiểm tra SHA-1 trong `infra/spark/Dockerfile` |
| `com.google.code.findbugs:jsr305` | `3.0.0` | **Đã có sẵn** trong bản phân phối Spark (`/opt/spark/jars/jsr305-3.0.0.jar`) |
| `org.apache.spark:spark-tags_2.12` | `3.5.8` | **Đã có sẵn** trong bản phân phối Spark (`/opt/spark/jars/spark-tags_2.12-3.5.8.jar`) |

---

## 3. Lệnh kiểm chứng độc lập toàn bộ Image Spark

```bash
# 1. Build image từ gốc repository
docker build -t n06-spark:3.5.8-py311 -f infra/spark/Dockerfile .

# 2. Kiểm tra phiên bản Spark, Hadoop, Scala, Java, Python
docker run --rm n06-spark:3.5.8-py311 spark-submit --version
docker run --rm n06-spark:3.5.8-py311 java -version
docker run --rm n06-spark:3.5.8-py311 python3 --version

# 3. Kiểm tra danh sách JAR đã ghim và đối chiếu SHA-1 trong container
docker run --rm n06-spark:3.5.8-py311 sh -c "ls -la \
  /opt/spark/jars/hadoop-client-api-3.3.4.jar \
  /opt/spark/jars/hadoop-client-runtime-3.3.4.jar \
  /opt/spark/jars/scala-library-2.12.18.jar \
  /opt/spark/jars/hadoop-aws-3.3.4.jar \
  /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.8.jar \
  /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.8.jar \
  /opt/spark/jars/kafka-clients-3.4.1.jar \
  /opt/spark/jars/commons-pool2-2.11.1.jar"
```
