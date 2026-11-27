# syntax=docker/dockerfile:1
# Image Spark 3.5.8 (Hadoop 3.3.4, Scala 2.12.18, OpenJDK 17, Python 3.11) kèm JAR S3A & Kafka đã ghim.
# Tuân thủ docs/spec.md Phần I mục 2, docs/adr/0001-bo-phien-ban.md và docs/compatibility-matrix.md.
# Không dùng tag `latest`; không tải dependency từ Internet lúc chạy job.

# ADR-0012: client PostgreSQL cùng bản với server của Compose (ghim cùng digest).
FROM postgres:16.6-bookworm@sha256:557fea37a744d5f4c8faab304b0a90858b53ab119735a88c131fd19dab802f36 AS pg

FROM python:3.11.11-slim-bookworm

ARG SPARK_VERSION=3.5.8
ARG HADOOP_VERSION=3.3.4
ARG SCALA_BINARY_VERSION=2.12
ARG SCALA_VERSION=2.12.18
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG SPARK_TGZ_SHA512=5d5d2e6e111182ee1210d4a46a17ae27107c7ccc70433da0ded3d8197e027f949b00445bb5678ed0c1d5c59f12993d9b9ed12e43686d5aad89a9ec570e93a083

ENV DEBIAN_FRONTEND=noninteractive \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home

ENV JAVA_HOME=/opt/java-home \
    PATH="/opt/spark/bin:/opt/spark/sbin:/opt/java-home/bin:${PATH}"

# 1. Tải bản phân phối Spark 3.5.8 kèm Hadoop 3 và kiểm tra SHA-512 chính thức từ Apache Archive
RUN set -eux; \
    SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop3.tgz"; \
    (curl -fSL --retry 3 "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}" -o "/tmp/${SPARK_TGZ}" \
     || curl -fSL --retry 3 "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}" -o "/tmp/${SPARK_TGZ}"); \
    echo "${SPARK_TGZ_SHA512}  /tmp/${SPARK_TGZ}" | sha512sum -c -; \
    mkdir -p /opt/spark; \
    tar -xzf "/tmp/${SPARK_TGZ}" -C /opt/spark --strip-components=1; \
    rm -f "/tmp/${SPARK_TGZ}"; \
    # 2. Xác minh trực tiếp trong /opt/spark/jars/ các phiên bản Hadoop, Scala và thư viện phụ thuộc đi kèm
    test -f "/opt/spark/jars/hadoop-client-api-${HADOOP_VERSION}.jar"; \
    test -f "/opt/spark/jars/hadoop-client-runtime-${HADOOP_VERSION}.jar"; \
    test -f "/opt/spark/jars/scala-library-${SCALA_VERSION}.jar"; \
    test -f "/opt/spark/jars/spark-core_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    test -f "/opt/spark/jars/spark-tags_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    test -f "/opt/spark/jars/jsr305-3.0.0.jar"

# 3. Tải các JAR connector (S3A + Kafka 0-10) đã ghim phiên bản từ Maven Central và kiểm tra checksum
COPY infra/spark/jars.sha1 /tmp/jars.sha1

RUN set -eux; \
    MAVEN_URL="https://repo1.maven.org/maven2"; \
    curl -fSL --retry 3 "${MAVEN_URL}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
        -o "/opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fSL --retry 3 "${MAVEN_URL}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        -o "/opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    curl -fSL --retry 3 "${MAVEN_URL}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" \
        -o "/opt/spark/jars/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fSL --retry 3 "${MAVEN_URL}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" \
        -o "/opt/spark/jars/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fSL --retry 3 "${MAVEN_URL}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
        -o "/opt/spark/jars/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fSL --retry 3 "${MAVEN_URL}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
        -o "/opt/spark/jars/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    sha1sum -c /tmp/jars.sha1; \
    rm -f /tmp/jars.sha1

# 4. `psql` 16.6 và `libpq` để driver Spark ghi bảng control (ADR-0012 quyết định 1)
COPY --from=pg /usr/lib/postgresql/16/bin/psql /usr/local/bin/psql
COPY --from=pg /usr/lib/x86_64-linux-gnu/libpq.so.5 /usr/lib/x86_64-linux-gnu/libpq.so.5
RUN psql --version

RUN groupadd --gid 1000 spark && useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash spark \
    && chown -R spark:spark /opt/spark

USER spark
WORKDIR /workspace

CMD ["spark-submit", "--version"]

