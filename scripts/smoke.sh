#!/usr/bin/env bash
# Chạy smoke test Spark <-> MinIO (S3A) và Spark <-> Kafka (TASK T05).
# - Tự động nạp biến môi trường từ .env (hoặc khởi tạo .env cục bộ từ .env.example nếu chưa có).
# - Đảm bảo các dịch vụ hạ tầng lõi (kafka, minio, postgres, spark) đều ở trạng thái healthy.
# - Chạy hai bài smoke test bằng pytest và in rõ kết quả PASS.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# 1. Nếu chưa có file .env cục bộ (file này nằm trong .gitignore), tạo bản .env local từ .env.example
if [[ ! -f "${REPO_ROOT}/.env" ]]; then
    sed \
        -e 's/CHANGE_ME_OBJECT_STORE_ACCESS_KEY/n06_local_minio_admin/' \
        -e 's/CHANGE_ME_OBJECT_STORE_SECRET_KEY/n06_local_minio_secret_key/' \
        -e 's/CHANGE_ME_POSTGRES_SUPERUSER_PASSWORD/n06_local_pg_super_pw/' \
        -e 's/CHANGE_ME_MART_DB_PASSWORD/n06_local_mart_pw/' \
        -e 's/CHANGE_ME_CONTROL_DB_PASSWORD/n06_local_control_pw/' \
        -e 's/CHANGE_ME_AIRFLOW_DB_PASSWORD/n06_local_airflow_pw/' \
        -e 's/CHANGE_ME_METABASE_DB_PASSWORD/n06_local_metabase_pw/' \
        "${REPO_ROOT}/.env.example" > "${REPO_ROOT}/.env"
fi

set -a
# shellcheck disable=SC1091
source "${REPO_ROOT}/.env"
set +a

echo "[smoke] 1/2 Khởi động và chờ trạng thái healthy của Docker Compose profile 'core'..."
if ! docker image inspect minio/minio:RELEASE.2024-11-07T00-52-20Z > /dev/null 2>&1; then
    docker build -t minio/minio:RELEASE.2024-11-07T00-52-20Z - <<'EOF'
FROM alpine/minio:RELEASE.2025-10-15T17-29-55Z
USER root
RUN apk add --no-cache curl
EOF
fi
docker compose --profile core up -d --wait

echo "[smoke] 2/2 Thực thi smoke test Spark <-> MinIO (S3A) và Spark <-> Kafka..."
if [[ -x "${REPO_ROOT}/.venv/bin/pytest" ]]; then
    PYTEST_BIN="${REPO_ROOT}/.venv/bin/pytest"
else
    PYTEST_BIN="pytest"
fi

"${PYTEST_BIN}" \
    tests/integration/test_smoke_s3a.py \
    tests/integration/test_smoke_kafka.py \
    -v -rA
