# infra/

**Mục đích:** Docker Compose, Dockerfile (Spark kèm JAR đã ghim), script khởi tạo database.

**Người sở hữu:** C (Quý); image Spark: A (Mạnh)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 2

**Quy tắc:** Không dùng tag `latest`. Không tải dependency từ Internet lúc chạy job.

## Cấu trúc hiện có (sau T02 & T04)

- `docker-compose.yml` (ở thư mục gốc repo): Cấu hình Docker Compose profile `core` cho 4 dịch vụ hạ tầng lõi (`kafka`, `minio`, `postgres`, `spark`) với healthcheck, named volume (`n06_kafka_data`, `n06_minio_data`, `n06_postgres_data`, `n06_spark_data`) và biến môi trường từ `.env`.
- `infra/postgres/init/01-init-databases.sh`: Script khởi tạo idempotent tạo 4 role và 4 database riêng biệt (`n06_mart`, `n06_control`, `n06_airflow`, `n06_metabase`), cô lập quyền truy cập và schema `public` giữa các database.
- `infra/spark/Dockerfile`: Dựng image Spark `3.5.8` (kèm Hadoop `3.3.4`, Scala `2.12.18`) trên nền `python:3.11.11-slim-bookworm` và `openjdk-17-jre-headless`. Tải sẵn và kiểm tra checksum cho 6 JAR connector S3A và Kafka 0-10.
- `infra/spark/jars.sha1`: Danh sách mã băm SHA-1 chính thức từ Maven Central của 6 JAR connector được kiểm tra bằng `sha1sum -c` trong quá trình `docker build`.

## Vận hành Docker Compose (Profile `core`)

```bash
# 1. Tạo file .env từ mẫu (hoặc dùng `n06 init-local` sau T03)
cp .env.example .env

# 2. Kiểm tra cấu hình Compose hợp lệ
docker compose --profile core config

# 3. Khởi động 4 dịch vụ lõi và chờ trạng thái healthy
docker compose --profile core up -d --wait
docker compose --profile core ps

# 4. Chạy lại script khởi tạo PostgreSQL (idempotent — chạy nhiều lần không lỗi)
docker compose --profile core exec -T postgres bash /docker-entrypoint-initdb.d/01-init-databases.sh

# 5. Dừng container nhưng giữ nguyên dữ liệu trong named volume
docker compose --profile core down

# 6. Dừng và xóa toàn bộ named volume
docker compose --profile core down -v
```

## Cách build và kiểm tra riêng image Spark

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
