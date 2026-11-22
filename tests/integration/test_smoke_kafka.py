"""Smoke test Spark <-> Kafka trên đúng bộ phiên bản đã ghim tại T04 (TASK T05).

- Không hardcode bootstrap_servers, topic prefix hay partition; mọi giá trị lấy từ cấu hình T01.
- Tạo topic test riêng theo run_id, gửi message bằng producer độc lập với Spark
  (kafka-console-producer.sh trên container kafka), Spark đọc batch bằng connector
  spark-sql-kafka-0-10_2.12:3.5.8 đã ghim trong /opt/spark/jars/, đối chiếu số message
  và nội dung, sau đó xóa topic trong khối finally.
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

# Dữ liệu kỳ vọng độc lập (5 bản ghi, amount_minor là số nguyên int, không dùng float)
EXPECTED_KAFKA_MESSAGES: list[dict[str, object]] = [
    {
        "transaction_id": "aaaa1111-0000-5000-8000-000000000001",
        "external_reference": "REF-SMOKE-0001",
        "account_from": "acc-vnd-101",
        "account_to": "acc-vnd-102",
        "amount_minor": 100000,
        "currency": "VND",
        "status": "SUCCESS",
        "business_date": "2026-01-01",
    },
    {
        "transaction_id": "aaaa1111-0000-5000-8000-000000000002",
        "external_reference": "REF-SMOKE-0002",
        "account_from": "acc-vnd-102",
        "account_to": "acc-vnd-103",
        "amount_minor": 250000,
        "currency": "VND",
        "status": "SUCCESS",
        "business_date": "2026-01-01",
    },
    {
        "transaction_id": "aaaa1111-0000-5000-8000-000000000003",
        "external_reference": "REF-SMOKE-0003",
        "account_from": "acc-usd-201",
        "account_to": "acc-usd-202",
        "amount_minor": 7500,
        "currency": "USD",
        "status": "SUCCESS",
        "business_date": "2026-01-01",
    },
    {
        "transaction_id": "aaaa1111-0000-5000-8000-000000000004",
        "external_reference": "REF-SMOKE-0004",
        "account_from": "acc-vnd-103",
        "account_to": "acc-vnd-101",
        "amount_minor": 50000,
        "currency": "VND",
        "status": "FAILED",
        "business_date": "2026-01-02",
    },
    {
        "transaction_id": "aaaa1111-0000-5000-8000-000000000005",
        "external_reference": "REF-SMOKE-0005",
        "account_from": "acc-vnd-101",
        "account_to": "acc-vnd-103",
        "amount_minor": 8800000,
        "currency": "VND",
        "status": "SUCCESS",
        "business_date": "2026-01-02",
    },
]

# Kịch bản PySpark chạy bên trong container `spark` để đọc batch từ topic Kafka
_SPARK_KAFKA_READER_SCRIPT = r"""
import json
import os
import sys
from glob import glob

spark_home = os.environ.get("SPARK_HOME", "/opt/spark")
sys.path.insert(0, os.path.join(spark_home, "python"))
for py4j_zip in glob(os.path.join(spark_home, "python", "lib", "py4j-*.zip")):
    sys.path.insert(0, py4j_zip)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

payload = json.loads(os.environ["N06_SMOKE_KAFKA_PAYLOAD"])
bootstrap_servers = payload["bootstrap_servers"]
topic = payload["topic"]

# 1. Xác minh các JAR Kafka connector đã ghim trong /opt/spark/jars/
for required_jar in (
    "spark-sql-kafka-0-10_2.12-3.5.8.jar",
    "spark-token-provider-kafka-0-10_2.12-3.5.8.jar",
    "kafka-clients-3.4.1.jar",
    "commons-pool2-2.11.1.jar",
):
    jar_path = os.path.join(spark_home, "jars", required_jar)
    if not os.path.isfile(jar_path):
        raise RuntimeError(f"Thiếu JAR Kafka đã ghim trong image Spark: {jar_path}")

spark = (
    SparkSession.builder.appName(payload["app_name"])
    .master(payload["master"])
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

try:
    df_kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    df_decoded = (
        df_kafka.select(
            F.col("key").cast("string").alias("key_str"),
            F.col("value").cast("string").alias("value_str"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").isNotNull().alias("has_kafka_timestamp"),
        )
        .orderBy("key_str")
    )

    records = []
    for row in df_decoded.collect():
        records.append(
            {
                "key": row["key_str"],
                "value": json.loads(row["value_str"]),
                "topic": row["topic"],
                "partition": int(row["partition"]),
                "offset": int(row["offset"]),
                "has_kafka_timestamp": bool(row["has_kafka_timestamp"]),
            }
        )

    print("N06_SMOKE_KAFKA_RESULT=" + json.dumps({"records": records}, sort_keys=True))
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


def _run_compose_exec(
    docker_bin: str,
    env: dict[str, str],
    service: str,
    args: list[str],
    *,
    stdin_text: str | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        docker_bin,
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "--profile",
        "core",
        "exec",
        "-T",
    ]
    if extra_env:
        for k, v in extra_env.items():
            cmd.extend(["-e", f"{k}={v}"])
    cmd.append(service)
    cmd.extend(args)
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        input=stdin_text,
        capture_output=True,
        text=True,
        check=False,
    )


def test_spark_kafka_batch_read_and_topic_cleanup() -> None:
    """Tạo topic test, gửi message bằng producer độc lập, Spark đọc batch khớp 100%, xóa topic."""
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        pytest.skip("Máy hiện tại không có binary `docker` trong PATH")

    cfg, env = _load_container_config()
    run_id = uuid.uuid4().hex[:12]
    test_topic = f"{cfg.kafka.events_topic}.smoke.{run_id}"
    bootstrap_servers = cfg.kafka.bootstrap_servers

    # 1. Tạo topic test trên Kafka với số partition và replication_factor từ cấu hình T01
    create_res = _run_compose_exec(
        docker_bin,
        env,
        "kafka",
        [
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            bootstrap_servers,
            "--create",
            "--if-not-exists",
            "--topic",
            test_topic,
            "--partitions",
            str(cfg.kafka.num_partitions),
            "--replication-factor",
            str(cfg.kafka.replication_factor),
        ],
    )
    assert create_res.returncode == 0, (
        f"Không tạo được topic test {test_topic}:\n"
        f"STDOUT:\n{create_res.stdout}\nSTDERR:\n{create_res.stderr}"
    )

    topic_deleted = False
    try:
        # 2. Gửi các message mẫu bằng producer độc lập với Spark (kafka-console-producer.sh)
        producer_lines = (
            "\n".join(
                f"{msg['transaction_id']}\t{json.dumps(msg, separators=(',', ':'), sort_keys=True)}"
                for msg in EXPECTED_KAFKA_MESSAGES
            )
            + "\n"
        )

        produce_res = _run_compose_exec(
            docker_bin,
            env,
            "kafka",
            [
                "/opt/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server",
                bootstrap_servers,
                "--topic",
                test_topic,
                "--property",
                "parse.key=true",
                "--property",
                "key.separator=\t",
                "--request-required-acks",
                "all",
            ],
            stdin_text=producer_lines,
        )
        assert produce_res.returncode == 0, (
            f"Gửi message vào topic {test_topic} thất bại:\n"
            f"STDOUT:\n{produce_res.stdout}\nSTDERR:\n{produce_res.stderr}"
        )

        # 3. Dùng Spark đọc batch từ topic vừa gửi
        worker_payload = {
            "app_name": f"{cfg.spark.app_name}-smoke-kafka-{run_id}",
            "master": cfg.spark.master,
            "bootstrap_servers": bootstrap_servers,
            "topic": test_topic,
        }
        spark_res = _run_compose_exec(
            docker_bin,
            env,
            "spark",
            ["python3", "-c", _SPARK_KAFKA_READER_SCRIPT],
            extra_env={"N06_SMOKE_KAFKA_PAYLOAD": json.dumps(worker_payload)},
        )
        assert spark_res.returncode == 0, (
            f"Spark đọc topic Kafka thất bại (exit={spark_res.returncode}):\n"
            f"STDOUT:\n{spark_res.stdout}\nSTDERR:\n{spark_res.stderr}"
        )

        marker_line = next(
            (
                line
                for line in spark_res.stdout.splitlines()
                if line.startswith("N06_SMOKE_KAFKA_RESULT=")
            ),
            None,
        )
        assert marker_line is not None, (
            f"Không tìm thấy kết quả trả về trong stdout của Spark:\n{spark_res.stdout}"
        )

        parsed = json.loads(marker_line.split("=", 1)[1])
        records = parsed["records"]

        # 4. Đối chiếu số lượng message, key, metadata và từng trường payload với dữ liệu gốc
        assert len(records) == len(EXPECTED_KAFKA_MESSAGES)
        for rec, expected_msg in zip(records, EXPECTED_KAFKA_MESSAGES, strict=True):
            assert rec["key"] == expected_msg["transaction_id"]
            assert rec["topic"] == test_topic
            assert 0 <= rec["partition"] < cfg.kafka.num_partitions
            assert rec["offset"] >= 0
            assert rec["has_kafka_timestamp"] is True
            assert isinstance(rec["value"]["amount_minor"], int)
            assert rec["value"] == expected_msg
    finally:
        # 5. Luôn xóa topic test sau khi chạy và xác nhận topic không còn trong danh sách
        del_res = _run_compose_exec(
            docker_bin,
            env,
            "kafka",
            [
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                bootstrap_servers,
                "--delete",
                "--if-exists",
                "--topic",
                test_topic,
            ],
        )
        list_res = _run_compose_exec(
            docker_bin,
            env,
            "kafka",
            [
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                bootstrap_servers,
                "--list",
            ],
        )
        remaining_topics = {line.strip() for line in list_res.stdout.splitlines() if line.strip()}
        topic_deleted = (del_res.returncode == 0) and (test_topic not in remaining_topics)

    assert topic_deleted is True, f"Topic test {test_topic} chưa được xóa sạch sau khi chạy"
