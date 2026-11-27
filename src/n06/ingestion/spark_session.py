"""SparkSession cho job ingestion: S3A và múi giờ phiên UTC, mọi giá trị lấy từ cấu hình T01."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from n06.config import REPO_ROOT, AppConfig


def build_spark(cfg: AppConfig, job_name: str) -> SparkSession:
    """`master` và bộ nhớ driver do `spark-submit` đặt (xem `n06.ingestion.launcher`)."""
    store = cfg.object_store
    builder = (
        SparkSession.builder.appName(f"{cfg.spark.app_name}-{job_name}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(cfg.spark.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(cfg.spark.aqe_enabled).lower())
        .config("spark.sql.warehouse.dir", "/tmp/n06-spark-warehouse")
        .config("spark.executor.memory", cfg.spark.executor_memory)
        .config("spark.hadoop.fs.s3a.endpoint", store.endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", store.region)
        .config("spark.hadoop.fs.s3a.access.key", store.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", store.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", str(store.path_style_access).lower())
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(store.endpoint.lower().startswith("https://")).lower(),
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )
    if cfg.spark.event_log_enabled:
        log_dir = Path(cfg.spark.event_log_dir)
        resolved = log_dir if log_dir.is_absolute() else REPO_ROOT / log_dir
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", resolved.as_uri()
        )
    return builder.getOrCreate()


def hadoop_path(spark: SparkSession, uri: str):  # type: ignore[no-untyped-def]
    """`(FileSystem, Path)` của một URI, dùng đúng cấu hình Hadoop của phiên Spark."""
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path
