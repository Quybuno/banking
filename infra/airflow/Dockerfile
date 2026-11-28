# Image Airflow cho N06 (TASK T60, ADR-0013 phương án A).
# Dựng từ n06-spark:3.5.8-py311 để dùng chung Python 3.11, Java 17, Spark 3.5.8 và 6 JAR đã ghim.
# Không mount Docker socket; task Airflow gọi `n06 ...` trực tiếp bằng BashOperator.

FROM n06-spark:3.5.8-py311

USER root
ENV AIRFLOW_HOME=/opt/airflow

COPY infra/airflow/constraints-3.4.0-3.11.txt /tmp/constraints-3.4.0-3.11.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.4.0" \
    --constraint /tmp/constraints-3.4.0-3.11.txt \
    && rm -f /tmp/constraints-3.4.0-3.11.txt

WORKDIR /workspace
COPY pyproject.toml README.md /workspace/
COPY src /workspace/src
COPY config /workspace/config
RUN pip install --no-cache-dir --no-deps .

RUN mkdir -p /opt/airflow/dags /opt/airflow/logs
WORKDIR /opt/airflow
