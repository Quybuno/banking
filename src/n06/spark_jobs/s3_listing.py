"""Liệt kê object S3 thật bằng Hadoop FileSystem của S3A trong job Spark (ADR-0002 mục 6).

Không thêm phụ thuộc: dùng chính cấu hình S3A của phiên Spark (`n06.ingestion.spark_session`).
Kết quả đưa cho `require_unused_run_prefix` trước khi ghi object đầu tiên của một run.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

from pyspark.sql import SparkSession

from n06.ingestion.spark_session import hadoop_path


def make_list_objects(spark: SparkSession) -> Callable[[str], Iterator[str]]:
    """`list_objects(prefix)` trả URI mọi object (đệ quy) dưới `prefix`; rỗng nếu chưa có gì."""

    def list_objects(prefix: str) -> Iterator[str]:
        filesystem, path = hadoop_path(spark, prefix)
        if not filesystem.exists(path):
            return
        files = filesystem.listFiles(path, True)
        while files.hasNext():
            yield files.next().getPath().toString()

    return list_objects


def list_directory(spark: SparkSession, uri: str) -> list[str]:
    """Tên các tệp (không đệ quy) trực tiếp trong một thư mục; rỗng nếu thư mục chưa có."""
    filesystem, path = hadoop_path(spark, uri)
    if not filesystem.exists(path):
        return []
    return sorted(
        status.getPath().getName() for status in filesystem.listStatus(path) if status.isFile()
    )


def exists(spark: SparkSession, uri: str) -> bool:
    filesystem, path = hadoop_path(spark, uri)
    return bool(filesystem.exists(path))
