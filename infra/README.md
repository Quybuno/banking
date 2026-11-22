# infra/

**Mục đích:** Docker Compose, Dockerfile (Spark kèm JAR đã ghim), script khởi tạo database.

**Người sở hữu:** C (Quý); image Spark: A (Mạnh)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 2

**Quy tắc:** Không dùng tag `latest`. Không tải dependency từ Internet lúc chạy job.

## Cấu trúc hiện có (sau T04)

- `infra/spark/Dockerfile`: Dựng image Spark `3.5.8` (kèm Hadoop `3.3.4`, Scala `2.12.18`) trên nền `python:3.11.11-slim-bookworm` và `openjdk-17-jre-headless`. Tải sẵn và kiểm tra checksum cho 6 JAR connector S3A và Kafka 0-10.
- `infra/spark/jars.sha1`: Danh sách mã băm SHA-1 chính thức từ Maven Central của 6 JAR connector được kiểm tra bằng `sha1sum -c` trong quá trình `docker build`.

## Cách build và kiểm tra image Spark

```bash
# Build image từ thư mục gốc repository
docker build -t n06-spark:3.5.8-py311 -f infra/spark/Dockerfile .

# Kiểm tra phiên bản runtime
docker run --rm n06-spark:3.5.8-py311 spark-submit --version
docker run --rm n06-spark:3.5.8-py311 java -version
docker run --rm n06-spark:3.5.8-py311 python3 --version

# Liệt kê các JAR S3A và Kafka đã ghim trong /opt/spark/jars/
docker run --rm n06-spark:3.5.8-py311 sh -c "sha1sum \
  /opt/spark/jars/hadoop-aws-3.3.4.jar \
  /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.8.jar \
  /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.8.jar \
  /opt/spark/jars/kafka-clients-3.4.1.jar \
  /opt/spark/jars/commons-pool2-2.11.1.jar"
```

Chi tiết nguồn kiểm chứng của từng số phiên bản xem tại `docs/compatibility-matrix.md` và `docs/adr/0001-bo-phien-ban.md`.
