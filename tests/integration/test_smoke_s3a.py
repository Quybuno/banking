"""Smoke test Spark <-> MinIO (S3A) trên đúng bộ phiên bản đã ghim tại T04 (TASK T05).

- Không hardcode endpoint, bucket, credential hay partition; mọi giá trị lấy từ cấu hình T01.
- Không tải dependency lúc runtime (không dùng --packages / spark.jars.packages);
  dùng trực tiếp các JAR đã ghim trong /opt/spark/jars/ của image n06-spark:3.5.8-py311.
- Dữ liệu kỳ vọng được khai báo độc lập (tiền là số nguyên amount_minor, không dùng float).
- Dọn sạch dữ liệu test trên MinIO sau khi chạy để có thể chạy lại nhiều lần liên tiếp.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

from n06.config import AppConfig, load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENV_FILE = REPO_ROOT / ".env"
ENV_EXAMPLE_FILE = REPO_ROOT / ".env.example"

# Dữ liệu kỳ vọng độc lập (sắp xếp theo transaction_id, amount_minor là số nguyên)
EXPECTED_S3A_ROWS: list[dict[str, object]] = [
    {
        "transaction_id": "11111111-1111-5111-8111-000000000001",
        "account_from": "acc-vnd-001",
        "account_to": "acc-vnd-002",
        "amount_minor": 150000,
        "currency": "VND",
        "business_date": "2026-01-01",
    },
    {
        "transaction_id": "22222222-2222-5222-8222-000000000002",
        "account_from": "acc-vnd-002",
        "account_to": "acc-vnd-003",
        "amount_minor": 2750000,
        "currency": "VND",
        "business_date": "2026-01-01",
    },
    {
        "transaction_id": "33333333-3333-5333-8333-000000000003",
        "account_from": "acc-usd-001",
        "account_to": "acc-usd-002",
        "amount_minor": 4200,
        "currency": "USD",
        "business_date": "2026-01-02",
    },
    {
        "transaction_id": "44444444-4444-5444-8444-000000000004",
        "account_from": "acc-vnd-003",
        "account_to": "acc-vnd-001",
        "amount_minor": 999999999,
        "currency": "VND",
        "business_date": "2026-01-02",
    },
]

# Kịch bản PySpark chạy bên trong container `spark` (n06-spark:3.5.8-py311)
_SPARK_S3A_WORKER_SCRIPT = r"""
import json
import os
import sys
from glob import glob

spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
sys.path.insert(0, os.path.join(spark_home, "python"))
for py4j_zip in glob(os.path.join(spark_home, "python", "lib", "py4j-*.zip")):
    sys.path.insert(0, py4j_zip)

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

payload = json.loads(os.environ["N06_SMOKE_S3A_PAYLOAD"])
endpoint = payload["endpoint"]
region = payload["region"]
bucket = payload["bucket"]
path_style = payload["path_style_access"]
access_key = payload["access_key"]
secret_key = payload["secret_key"]
run_prefix_uri = payload["run_prefix_uri"]
parquet_uri = payload["parquet_uri"]
input_rows = payload["rows"]
ssl_enabled = endpoint.lower().startswith("https://")

# 1. Xác minh các JAR S3A đã ghim có sẵn trong /opt/spark/jars/ (không tải từ Internet)
for required_jar in ("hadoop-aws-3.3.4.jar", "aws-java-sdk-bundle-1.12.262.jar"):
    jar_path = os.path.join(spark_home, "jars", required_jar)
    if not os.path.isfile(jar_path):
        raise RuntimeError(f"Thiếu JAR đã ghim trong image Spark: {jar_path}")

# 2. Khởi tạo SparkSession với cấu hình S3A từ T01
spark = (
    SparkSession.builder.appName(payload["app_name"])
    .master(payload["master"])
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.endpoint.region", region)
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true" if path_style else "false")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if ssl_enabled else "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)

cleanup_confirmed = False
try:
    # 3. Đảm bảo bucket tồn tại trên MinIO qua AWS S3 client trong aws-java-sdk-bundle
    jvm = spark._jvm
    aws_creds = jvm.com.amazonaws.auth.BasicAWSCredentials(access_key, secret_key)
    creds_provider = jvm.com.amazonaws.auth.AWSStaticCredentialsProvider(aws_creds)
    endpoint_cfg = jvm.com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(
        endpoint, region
    )
    s3_client = (
        jvm.com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(endpoint_cfg)
        .withCredentials(creds_provider)
        .withPathStyleAccessEnabled(bool(path_style))
        .build()
    )
    if not s3_client.doesBucketExistV2(bucket):
        s3_client.createBucket(bucket)

    schema = StructType(
        [
            StructField("transaction_id", StringType(), nullable=False),
            StructField("account_from", StringType(), nullable=False),
            StructField("account_to", StringType(), nullable=False),
            StructField("amount_minor", LongType(), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("business_date", StringType(), nullable=False),
        ]
    )

    hadoop_conf = spark._jsc.hadoopConfiguration()
    uri_obj = jvm.java.net.URI(run_prefix_uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri_obj, hadoop_conf)
    run_path = jvm.org.apache.hadoop.fs.Path(run_prefix_uri)

    try:
        # 4. Ghi DataFrame dạng Parquet qua giao thức s3a:// lên MinIO
        df_write = spark.createDataFrame(input_rows, schema=schema)
        df_write.write.mode("overwrite").parquet(parquet_uri)

        # 5. Đọc lại từ đường dẫn Parquet trên MinIO
        df_read = spark.read.parquet(parquet_uri).orderBy("transaction_id")
        read_dtypes = dict(df_read.dtypes)
        collected = [row.asDict() for row in df_read.collect()]
    finally:
        # 6. Luôn dọn sạch dữ liệu test của lần chạy này trên MinIO
        if fs.exists(run_path):
            fs.delete(run_path, True)
        cleanup_confirmed = not bool(fs.exists(run_path))

    result = {
        "rows": collected,
        "dtypes": read_dtypes,
        "cleanup_confirmed": cleanup_confirmed,
    }
    print("N06_SMOKE_S3A_RESULT=" + json.dumps(result, sort_keys=True))
finally:
    spark.stop()
"""


def _load_runtime_env() -> dict[str, str]:
    """Nạp biến môi trường từ .env (hoặc từ .env.example nếu chạy độc lập)."""
    env = dict(os.environ)
    source_file = ENV_FILE if ENV_FILE.is_file() else ENV_EXAMPLE_FILE
    if source_file.is_file():
        for line in source_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            key, _, val = stripped.partition("=")
            key = key.strip()
            val = val.strip()
            if key not in env or env[key].startswith("CHANGE_ME_"):
                if val.startswith("CHANGE_ME_"):
                    val = val.replace("CHANGE_ME_", "n06_local_")
                env[key] = val
    return env


def _load_container_config() -> tuple[AppConfig, dict[str, str]]:
    """Nạp cấu hình T01 cho mạng nội bộ Docker (profile default)."""
    env = _load_runtime_env()
    cfg = load_config(profile="default", env=env)
    return cfg, env


def test_spark_s3a_parquet_roundtrip_and_cleanup() -> None:
    """Spark ghi DataFrame dạng Parquet qua S3A lên MinIO, đọc lại khớp từng dòng và dọn sạch."""
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        pytest.skip("Máy hiện tại không có binary `docker` trong PATH")

    cfg, env = _load_container_config()
    run_id = uuid.uuid4().hex[:12]

    run_prefix_uri = f"s3a://{cfg.object_store.bucket}/{cfg.paths.raw_prefix}/smoke-s3a-{run_id}"
    parquet_uri = f"{run_prefix_uri}/transactions.parquet"

    worker_payload = {
        "app_name": f"{cfg.spark.app_name}-smoke-s3a-{run_id}",
        "master": cfg.spark.master,
        "endpoint": cfg.object_store.endpoint,
        "region": cfg.object_store.region,
        "bucket": cfg.object_store.bucket,
        "path_style_access": cfg.object_store.path_style_access,
        "access_key": cfg.object_store.access_key,
        "secret_key": cfg.object_store.secret_key,
        "run_prefix_uri": run_prefix_uri,
        "parquet_uri": parquet_uri,
        "rows": EXPECTED_S3A_ROWS,
    }

    proc = subprocess.run(
        [
            docker_bin,
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "--profile",
            "core",
            "exec",
            "-T",
            "-e",
            f"N06_SMOKE_S3A_PAYLOAD={json.dumps(worker_payload)}",
            "spark",
            "python3",
            "-c",
            _SPARK_S3A_WORKER_SCRIPT,
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, (
        f"Smoke test Spark <-> MinIO (S3A) thất bại (exit={proc.returncode}):\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )

    marker_line = next(
        (line for line in proc.stdout.splitlines() if line.startswith("N06_SMOKE_S3A_RESULT=")),
        None,
    )
    assert marker_line is not None, f"Không tìm thấy kết quả trả về trong stdout:\n{proc.stdout}"

    result_data = json.loads(marker_line.split("=", 1)[1])
    actual_rows = result_data["rows"]
    actual_dtypes = result_data["dtypes"]

    # 1. Kiểm tra kiểu dữ liệu tiền tệ là số nguyên 64-bit (bigint), không phải float/double
    assert actual_dtypes.get("amount_minor") == "bigint"

    # 2. Kiểm tra số lượng dòng và khớp chính xác từng dòng với dữ liệu gốc độc lập
    assert len(actual_rows) == len(EXPECTED_S3A_ROWS)
    assert actual_rows == EXPECTED_S3A_ROWS

    # 3. Xác nhận dữ liệu test trên MinIO đã được xóa sạch sau khi chạy
    assert result_data["cleanup_confirmed"] is True
