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
| T11 | A | DONE | `task/T11-determinism-primitives` | `pytest tests/unit/generator/test_determinism.py` (34/34 PASS), `verify_contract.py` (14/14 PASS) | `canonical_json`, `draw()`, UUIDv5, `external_reference` khớp 29/29 golden vectors |
| T12 | A | DONE | `task/T12-scenario-generator` | `pytest tests/unit` (73/73 PASS), `pytest tests/integration/test_generator_scenario.py` (1/1 PASS), `verify_contract.py` (14/14 PASS), `check_rules.py` OK, `ruff` (T12 files PASS) | Generator kịch bản gốc: `UNIFORM_DISTINCT_PAIR`, `HOT_ACCOUNT_V1`, external sort `(event_time, ordinal)`, bất biến chunk size |
| T13 | A | IN_REVIEW | `task/T13-fault-injection`, `task/T13-followup` | `pytest tests/unit` (235/235 PASS), `pytest tests/integration/test_generator_faults.py` (1/1 PASS), `verify_contract.py` (14/14 PASS), `check_rules.py` OK, `ruff` PASS | Tiêm lỗi floor chính xác, sao kê chia file theo cửa sổ, manifest, control_report, ground_truth tách riêng; ADR-0009 (Accepted), ADR-0011 (Proposed: normal delay cộng cửa sổ file phải trước cutoff). Tồn đọng: bộ nhớ bộ chia file sao kê tỷ lệ với số dòng một cửa sổ, chưa bị chặn bởi `generation_chunk_rows` |
| T14 | B | DONE | `task/T14-oracle-validate` | `pytest tests/unit/oracle/test_oracle.py` (5/5 PASS: 3 snapshot, T007/T008, `discrepancy_age_seconds`, `cutoff_cases`), `pytest tests/unit -q` (69/69 PASS), `check_rules.py` OK, `verify_contract.py` (14/14 PASS) | Test harness oracle trong `tests/harness/oracle.py` và khung CLI `n06 validate --against-ground-truth` (không tham chiếu `tests/` trong `src/`) |
| T20 | A | IN_REVIEW | `task/T20-publish` | `pytest tests/unit` (346/346 PASS), `pytest tests/integration/test_publish_kafka.py` (7/7 PASS trên Kafka thật, 2 lần liên tiếp), `scripts/smoke.sh` (2/2 PASS), `verify_contract.py` (14/14 PASS), `check_rules.py` OK, `ruff` PASS | `n06 publish`: producer idempotent (`confluent-kafka==2.16.2`, ADR-0010), key `transaction_id`/`account_from`, tốc độ cấu hình được, từ chối topic không rỗng, mã thoát 15. Chưa kiểm chứng retry bằng chèn lỗi mạng; chưa đo thông lượng E2 |
| T21 | B | DONE | `ce08109` | `pytest tests/integration/test_control_migrations.py` (1/1 PASS trên Postgres thật, 4.74s), `pytest tests/unit` (PASS), `verify_contract.py` (14/14 PASS), `check_rules.py` OK, `ruff` (T21 files PASS) | ADR-0003, `sql/migrations/control/0001_init_control_schema.{up,down}.sql`, `src/n06/control/` |
| T22 | A | TODO | | | |
| T23 | A | TODO | | | |
| T24 | B | DONE | `task/T24-batch-commit` | `pytest tests/integration/test_commit_postgres.py` (1/1 PASS trên Postgres thật, 4.02s), `pytest tests/unit` (72/72 PASS), `verify_contract.py` (14/14 PASS), `check_rules.py` OK, `ruff` (T24 files PASS) | `src/n06/storage/{commit.py,control_store.py}`, `tests/{unit/storage,integration}/` |
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
- 2026-11-23 | T21 | Thêm `docs/adr/0003-control-db-schema.md`, `sql/migrations/control/0001_init_control_schema.{up,down}.sql`, `src/n06/control/migrations.py`, `tests/unit/control/test_control_migrations_unit.py` và `tests/integration/test_control_migrations.py` | Chốt schema 4 bảng control (`ingest_batches`, `runs`, `current_snapshot`, `quality_metrics`) + `schema_migrations` và vị trí `sql/migrations/control/` qua ADR-0003; runner hỗ trợ `psycopg`/`psycopg2` hoặc `psql` CLI với cấu hình từ `load_config().postgres.control` | Đã chạy integration test trên Postgres thật (`docker compose --profile core up -d`), `pytest tests/integration/test_control_migrations.py -v` 1 passed in 4.74s.
- 2026-11-23 | T11 | Thêm `src/n06/generator/determinism/` và `tests/unit/generator/test_determinism.py` | Cài đặt `canonical_json`, `draw()` SHA-256 rejection sampling, UUIDv5 và `external_reference` theo mục 5.2; kiểm chứng 29 golden vector riêng lẻ + test `retry_counter` và subprocess | Chưa test trên phiên bản Python thứ hai (chỉ có 3.11.16 cục bộ).
- 2026-11-23 | T24 | Thêm `src/n06/storage/{commit.py,control_store.py,__init__.py}`, `tests/unit/storage/test_commit_protocol.py` và `tests/integration/test_commit_postgres.py` | Dùng `pg_advisory_xact_lock` theo phạm vi `(dataset_id, layer, entity, business_date)` với quy tắc last-committed-wins trong một transaction duy nhất; `semantic_hash` loại trừ `run_id` và timestamp | Đã chạy integration test trên Postgres thật (`docker compose --profile core up -d`), `pytest tests/integration/test_commit_postgres.py -v` 1 passed in 4.02s.
- 2026-11-23 | T12 | Thêm `src/n06/generator/{external_sort.py,scenario.py}`, cập nhật `src/n06/generator/__init__.py`, thêm `tests/unit/generator/test_scenario_generator.py` và `tests/integration/test_generator_scenario.py` | Dùng multi-pass k-way external merge sort trên đĩa theo `(event_time, ordinal)`, tính `business_checksum` độc lập với `generation_chunk_rows`, kiểm tra đủ tiền và validate mọi bản ghi qua T10 | Không.
- 2026-11-23 | T13 | Thêm `src/n06/generator/` (fault, statements, events, manifest, control_report, ground_truth, dataset), ADR-0009 và test unit/integration | Ground truth ghi cạnh `source/`, canonical của T12 ghi thẳng vào `ground_truth/canonical` không sao chép; manifest ordinal 0 gồm accounts, control_report, events shard 0 | Chưa chạy 5 triệu giao dịch; ADR-0009 chờ chuyển Accepted.
- 2026-11-23 | T13 | Sửa theo review: kiểm normal delay < cutoff lúc startup, gom bố cục đầu ra vào `OutputLayout`, truyền `allowed_source_batch_ids`, thêm test chunk nhiều shard, bổ sung quyết định 4 vào ADR-0009, cập nhật README generator | Không sửa bộ nhớ `_pending` (ghi tồn đọng) | ADR-0009 vẫn Proposed.
