"""DAG chạy hàng ngày theo business_date cho N06 (TASK T60, ADR-0013).

Quy tắc:
- Chỉ dùng BashOperator gọi CLI `n06` (không chép logic Spark vào DAG).
- Readiness dựa trên `n06 wait-ingestion` (đọc `ingest_batches` + manifest), không dùng `sleep`.
- Dùng logical date (`{{ ds }}`) theo múi giờ `Asia/Ho_Chi_Minh`, không lấy ngày lúc parse.
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

AS_OF_EXPR = (
    "{{ params.as_of_time or "
    '(data_interval_end.in_timezone("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")) }}'
)

with DAG(
    dag_id="n06_daily_pipeline",
    description="N06 daily: wait-ingestion -> ingest-statements -> process-day -> load-mart",
    start_date=datetime(2026, 1, 1, tzinfo=BUSINESS_TZ),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "dataset_id": Param("n06-demo-clean", type="string"),
        "profile": Param("demo", type="string"),
        "source_dir": Param("/workspace/data/source", type="string"),
        "as_of_time": Param("", type="string"),
    },
    tags=["n06", "daily"],
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
            f"AS_OF='{AS_OF_EXPR}' && "
            "n06 ingest-statements "
            "--profile '{{ params.profile }}' "
            "--dataset-id '{{ params.dataset_id }}' "
            "--source-dir '{{ params.source_dir }}' "
            '--as-of-time "$AS_OF"'
        ),
    )

    process_day = BashOperator(
        task_id="process_day",
        bash_command=(
            f"AS_OF='{AS_OF_EXPR}' && "
            "n06 process-day "
            "--profile '{{ params.profile }}' "
            "--dataset-id '{{ params.dataset_id }}' "
            "--business-date '{{ ds }}' "
            '--as-of-time "$AS_OF"'
        ),
    )

    load_mart = BashOperator(
        task_id="load_mart",
        bash_command=(
            "n06 load-mart "
            "--profile '{{ params.profile }}' "
            "--dataset-id '{{ params.dataset_id }}' "
            "--from-date '{{ ds }}' "
            "--to-date '{{ ds }}'"
        ),
    )

    wait_ingestion >> ingest_statements >> process_day >> load_mart
