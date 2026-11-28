"""Unit test kiểm tra cấu trúc DAG Airflow T60 (không cần cài Airflow trên host)."""

from __future__ import annotations

import ast

from n06.config import REPO_ROOT

DAGS_DIR = REPO_ROOT / "dags"


def test_airflow_dags_follow_spec_and_adr0013_rules() -> None:
    dag_files = [
        DAGS_DIR / "n06_daily_pipeline.py",
        DAGS_DIR / "n06_backfill_pipeline.py",
    ]
    for path in dag_files:
        assert path.is_file(), f"Thiếu file DAG: {path}"
        source = path.read_text(encoding="utf-8")
        ast.parse(source)
        assert "BashOperator" in source
        assert "n06 wait-ingestion" in source
        assert "retries" in source
        # Cấm gọi spark-submit trực tiếp, cấm chép logic Spark, cấm sleep readiness
        assert "spark-submit" not in source
        assert "pyspark" not in source
        assert "time.sleep" not in source
        assert "docker.sock" not in source


def test_airflow_dockerfile_and_compose_have_no_docker_socket() -> None:
    dockerfile = (REPO_ROOT / "infra" / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    assert "FROM n06-spark:3.5.8-py311" in dockerfile
    assert "apache-airflow==3.4.0" in dockerfile
    assert "constraints-3.4.0-3.11.txt" in dockerfile

    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "orchestration" in compose
    assert "airflow-scheduler" in compose
    assert "docker.sock" not in compose
