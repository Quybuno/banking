# Tiến độ N06

Trạng thái: `TODO`, `IN_PROGRESS`, `IN_REVIEW`, `DONE`, `BLOCKED`. Milestone chỉ xong khi mọi task DONE và cổng milestone đã chạy trên `main`, có bằng chứng.

## Milestone

| Milestone | Mức | Trạng thái | Bằng chứng cổng |
|---|---|---|---|
| M0 Môi trường và hợp đồng | P0 | DONE | `verify_contract.py` (14/14 PASS), `n06 doctor` (10/10 PASS), `bash scripts/smoke.sh` (2/2 PASS), `pytest tests/unit` (18/18 PASS) |
| M1 Domain và generator | P0 | TODO | |
| M2 Lát cắt end-to-end | P0 | TODO | |
| M3 Chất lượng và đối soát | P0 | TODO | |
| M4 Số dư, mart, dashboard | P0 | TODO | |
| M5 Phục hồi và benchmark lõi | P0 | TODO | |
| M6 Điều phối, late correction, E6–E8 | P1 | TODO | |

## Task

| ID | Người | Trạng thái | PR | Bằng chứng | Ghi chú |
|---|---|---|---|---|---|
| T00 | C | DONE | `8f6dd2b` | `verify_contract.py` (14/14 PASS), `ruff check`, `pytest tests/unit/test_smoke.py` (2/2 PASS) | Khung `pyproject.toml`, `uv.lock`, cấu hình ruff/pytest, smoke test |
| T01 | C | DONE | `8f6dd2b` | `pytest tests/unit/config/test_config.py` (11/11 PASS), `verify_contract.py` (14/14 PASS) | Hệ thống cấu hình 4 tầng (`default`/`demo`/`benchmark`/`test`), validate kiểu, không secret trong git |
| T02 | C | DONE | `task/T02-docker-compose` | `pytest tests/integration/test_compose_core.py` (4/4 PASS), `verify_contract.py` (14/14 PASS) | Docker Compose profile `core` (Kafka, MinIO, Postgres 4 DB, Spark) |
| T03 | C | DONE | `task/T03-cli-doctor` | `pytest tests/unit/cli/test_cli_doctor.py tests/integration/test_doctor_compose.py` (6/6 PASS) | CLI `n06` (`--help`, `init-local`, `doctor`, bảng `ExitCode`, ẩn secret) |
| T04 | A | DONE | `8fbc36a` | `ADR-0001` (Accepted), `docs/compatibility-matrix.md`, image `n06-spark:3.5.8-py311` | Chốt bộ phiên bản Spark 3.5.8, Hadoop 3.3.4, AWS SDK 1.12.262, Kafka 0-10 3.5.8, JDK 17, Python 3.11 |
| T05 | A | DONE | `task/T05-smoke-s3a-kafka` | `bash scripts/smoke.sh` (2/2 PASS x 2 lần liên tiếp), `verify_contract.py` (14/14 PASS) | Smoke test Spark <-> MinIO (S3A Parquet) và Spark <-> Kafka batch read |
| T10 | A | DONE | `task/T10-domain-models-validator` | `pytest tests/unit/domain/test_domain_validation.py` (6/6 PASS), `verify_contract.py` (14/14 PASS) | Domain models, validator Draft 2020-12 có format checks, validator nghiệp vụ, 17 mã lỗi theo thứ tự ưu tiên |
| T11 | A | TODO | | | |
| T12 | A | TODO | | | |
| T13 | A | TODO | | | |
| T14 | B | DONE | `task/T14-oracle-validate` | `pytest tests/unit -q` (69/69 PASS), `check_rules.py` OK, `verify_contract.py` (14/14 PASS) | Test harness oracle trong `tests/harness/oracle.py` và khung CLI `n06 validate --against-ground-truth` |
| T20 | A | TODO | | | |
| T21 | B | IN_REVIEW | task/T21-control-migrations | verify_contract.py (14/14 PASS), check_rules.py OK, ruff (T21 files PASS), pytest tests/unit (64/64 PASS); integration test chờ Postgres thật | ADR-0003, sql/migrations/control/0001_init_control_schema.{up,down}.sql, src/n06/control/ |
| T22 | A | TODO | | | |
| T23 | A | TODO | | | |
| T24 | B | TODO | | | |
| T25 | B | TODO | | | |
| T26 | C | TODO | | | |
| T30 | B | TODO | | | |
| T31 | B | TODO | | | |
| T32 | B | TODO | | | |
| T33 | B | TODO | | | |
| T34 | B | TODO | | | |
| T40 | B | TODO | | | |
| T41 | C | TODO | | | |
| T42 | C | TODO | | | |
| T43 | C | TODO | | | |
| T50 | B | TODO | | | |
| T51 | C | TODO | | | |
| T52 | C + A | TODO | | | |
| T53 | cả nhóm | TODO | | | |

## Nhật ký phiên

Mỗi phiên agent thêm một dòng: ngày, task, việc đã làm, quyết định, vướng mắc.

- 2026-11-22 | T02 | Thêm `docker-compose.yml` (profile `core` với Kafka 3.7.2, MinIO, Postgres 16.6, Spark 3.5.8), script idempotent `infra/postgres/init/01-init-databases.sh` tạo 4 DB/user riêng, cập nhật `.env.example`, `infra/README.md` và `tests/integration/test_compose_core.py` | Dùng KRaft cho Kafka với 2 listener INTERNAL (9092) / EXTERNAL (19092) khớp `default.toml` và `test.toml` | WSL hiện chưa bật Docker CLI nên chưa chạy `docker compose up -d` trực tiếp.
- 2026-11-22 | T05 | Thêm `tests/integration/test_smoke_s3a.py`, `tests/integration/test_smoke_kafka.py`, `scripts/smoke.sh` và cập nhật lệnh Integration trong `CLAUDE.md` | Chạy PySpark trên container `n06-spark:3.5.8-py311` với cấu hình lấy từ `n06.config.load_config` và 6 JAR đã ghim, chạy 2 lần liên tiếp PASS 100% | Không.
- 2026-11-22 | T03 | Thêm gói `src/n06/cli/` (`init-local`, `doctor`, bảng `ExitCode`), unit test `tests/unit/cli/test_cli_doctor.py`, integration test `tests/integration/test_doctor_compose.py` và cập nhật `README.md` | Tách hàm thuần túy `evaluate_system_resources` để test bằng giá trị giả; tắt thử `minio` trong Compose xác nhận `doctor` báo đúng `service.minio` với mã thoát 14 | Không. Hoàn tất toàn bộ Milestone M0.
- 2026-11-22 | T10 | Thêm `src/n06/domain/` (`errors.py`, `models.py`, `schema_validator.py`, `business_validator.py`) và `tests/unit/domain/test_domain_validation.py` | Đọc trực tiếp JSON Schema Draft 2020-12 từ `contracts/`, bật format checks (uuid, date, date-time UTC), kiểm chứng 100% acceptance suite và negative suite theo thứ tự ưu tiên 17 mã lỗi | Không.
- 2026-11-23 | T14 | Thêm oracle độc lập tại `tests/harness/oracle.py`, khung CLI `n06 validate --against-ground-truth` trong `src/n06/cli/validate.py` và unit test `tests/unit/oracle/test_oracle.py` | Tách `src/n06/cli/validate.py` gọi harness qua subprocess/callback để không import `tests/` và đạt `check_rules.py` | Chưa có output pipeline thật (thuộc T33/T34).
- 2026-11-23 | T21 | Thêm `docs/adr/0003-control-db-schema.md`, `sql/migrations/control/0001_init_control_schema.{up,down}.sql`, `src/n06/control/migrations.py`, `tests/unit/control/test_control_migrations_unit.py` và `tests/integration/test_control_migrations.py` | Chốt schema 4 bảng control (`ingest_batches`, `runs`, `current_snapshot`, `quality_metrics`) + `schema_migrations` và vị trí `sql/migrations/control/` qua ADR-0003; runner hỗ trợ `psycopg`/`psycopg2` hoặc `psql` CLI với cấu hình từ `load_config().postgres.control` | Môi trường sandbox chưa có `docker`/`psql`/`psycopg` và Postgres chưa chạy tại `localhost:15432` nên integration test bị skip.
