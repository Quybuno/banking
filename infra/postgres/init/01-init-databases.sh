#!/usr/bin/env bash
# Script khởi tạo idempotent cho PostgreSQL: tạo 4 role và 4 database riêng biệt
# cho mart, control, airflow, metabase (đặc tả Phần I mục 2).
# Chạy lại nhiều lần không báo lỗi (idempotent) và tách quyền giữa các database.

set -euo pipefail

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"

: "${N06_POSTGRES_MART_DB:?N06_POSTGRES_MART_DB is required}"
: "${N06_POSTGRES_MART_USER:?N06_POSTGRES_MART_USER is required}"
: "${N06_POSTGRES_MART_PASSWORD:?N06_POSTGRES_MART_PASSWORD is required}"

: "${N06_POSTGRES_CONTROL_DB:?N06_POSTGRES_CONTROL_DB is required}"
: "${N06_POSTGRES_CONTROL_USER:?N06_POSTGRES_CONTROL_USER is required}"
: "${N06_POSTGRES_CONTROL_PASSWORD:?N06_POSTGRES_CONTROL_PASSWORD is required}"

: "${N06_POSTGRES_AIRFLOW_DB:?N06_POSTGRES_AIRFLOW_DB is required}"
: "${N06_POSTGRES_AIRFLOW_USER:?N06_POSTGRES_AIRFLOW_USER is required}"
: "${N06_POSTGRES_AIRFLOW_PASSWORD:?N06_POSTGRES_AIRFLOW_PASSWORD is required}"

: "${N06_POSTGRES_METABASE_DB:?N06_POSTGRES_METABASE_DB is required}"
: "${N06_POSTGRES_METABASE_USER:?N06_POSTGRES_METABASE_USER is required}"
: "${N06_POSTGRES_METABASE_PASSWORD:?N06_POSTGRES_METABASE_PASSWORD is required}"

provision_role_and_db() {
    local db_name="$1"
    local db_user="$2"
    local db_password="$3"

    # 1. Tạo hoặc cập nhật mật khẩu role một cách idempotent
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
        -v role_name="${db_user}" \
        -v role_password="${db_password}" <<'EOSQL'
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :'role_name')
    THEN format('CREATE ROLE %I WITH LOGIN PASSWORD %L', :'role_name', :'role_password')
    ELSE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'role_name', :'role_password')
END
\gexec
EOSQL

    # 2. Tạo database nếu chưa tồn tại và gán owner cho đúng role
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
        -v target_db="${db_name}" \
        -v owner_role="${db_user}" <<'EOSQL'
SELECT format('CREATE DATABASE %I OWNER %I ENCODING ''UTF8''', :'target_db', :'owner_role')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'target_db')
\gexec

SELECT format('ALTER DATABASE %I OWNER TO %I', :'target_db', :'owner_role')
\gexec

SELECT format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', :'target_db')
\gexec

SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', :'target_db', :'owner_role')
\gexec
EOSQL

    # 3. Cô lập schema public của từng database, chỉ cho phép đúng owner_role ghi
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${db_name}" \
        -v owner_role="${db_user}" <<'EOSQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SELECT format('ALTER SCHEMA public OWNER TO %I', :'owner_role')
\gexec
SELECT format('GRANT ALL ON SCHEMA public TO %I', :'owner_role')
\gexec
EOSQL
}

provision_role_and_db "${N06_POSTGRES_MART_DB}" "${N06_POSTGRES_MART_USER}" "${N06_POSTGRES_MART_PASSWORD}"
provision_role_and_db "${N06_POSTGRES_CONTROL_DB}" "${N06_POSTGRES_CONTROL_USER}" "${N06_POSTGRES_CONTROL_PASSWORD}"
provision_role_and_db "${N06_POSTGRES_AIRFLOW_DB}" "${N06_POSTGRES_AIRFLOW_USER}" "${N06_POSTGRES_AIRFLOW_PASSWORD}"
provision_role_and_db "${N06_POSTGRES_METABASE_DB}" "${N06_POSTGRES_METABASE_USER}" "${N06_POSTGRES_METABASE_PASSWORD}"
