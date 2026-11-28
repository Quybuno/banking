"""DAG backfill theo khoảng ngày cho N06 (TASK T60, ADR-0013, cần T42).

Gọi `n06 backfill` bằng BashOperator; không chép logic Spark vào DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.models.param import Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

BUSINESS_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

DEFAULT_ARGS = {
    "owner": "n06",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="n06_backfill_pipeline",
    description="N06 backfill pipeline: wait-ingestion -> ingest-statements -> n06 backfill",
    start_date=datetime(2026, 1, 1, tzinfo=BUSINESS_TZ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "dataset_id": Param("n06-demo-clean", type="string"),
        "from_date": Param("2026-01-01", type="string"),
        "to_date": Param("2026-01-03", type="string"),
        "as_of_time": Param("2026-01-05T00:00:00Z", type="string"),
        "profile": Param("demo", type="string"),
        "source_dir": Param("/workspace/data/source", type="string"),
    },
    tags=["n06", "backfill"],
) as dag:
    wait_ingestion = BashOperator(
        task_id="wait_ingestion",
        bash_command=(
            "n06 wait-ingestion "
            "--profile '{{ params.profile }}' "
            "--dataset-id '{{ params.dataset_id }}' "
            "--source-dir '{{ params.source_dir }}' "
            "--timeout-seconds 300"
        ),
    )

    ingest_statements = BashOperator(
        task_id="ingest_statements",
        bash_command=(
            "n06 ingest-statements "
            "--profile '{{ params.profile }}' "
            "--dataset-id '{{ params.dataset_id }}' "
            "--source-dir '{{ params.source_dir }}' "
            "--as-of-time '{{ params.as_of_time }}'"
        ),
    )

    run_backfill = BashOperator(
        task_id="backfill_range",
        bash_command=(
            "n06 backfill "
            "--profile '{{ params.profile }}' "
            "--dataset-id '{{ params.dataset_id }}' "
            "--from-date '{{ params.from_date }}' "
            "--to-date '{{ params.to_date }}' "
            "--as-of-time '{{ params.as_of_time }}'"
        ),
    )

    wait_ingestion >> ingest_statements >> run_backfill
