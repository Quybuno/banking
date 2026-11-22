# Tiến độ N06

Trạng thái: `TODO`, `IN_PROGRESS`, `IN_REVIEW`, `DONE`, `BLOCKED`. Milestone chỉ xong khi mọi task DONE và cổng milestone đã chạy trên `main`, có bằng chứng.

## Milestone

| Milestone | Mức | Trạng thái | Bằng chứng cổng |
|---|---|---|---|
| M0 Môi trường và hợp đồng | P0 | TODO | |
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
| T01 | C | TODO | | | |
| T02 | C | DONE | `task/T02-docker-compose` | `pytest tests/integration/test_compose_core.py` (4/4 PASS), `verify_contract.py` (14/14 PASS) | Docker Compose profile `core` (Kafka, MinIO, Postgres 4 DB, Spark) |
| T03 | C | TODO | | | |
| T04 | A | TODO | | | |
| T05 | A | DONE | `task/T05-smoke-s3a-kafka` | `bash scripts/smoke.sh` (2/2 PASS x 2 lần liên tiếp), `verify_contract.py` (14/14 PASS) | Smoke test Spark <-> MinIO (S3A Parquet) và Spark <-> Kafka batch read |
| T10 | A | TODO | | | |
| T11 | A | TODO | | | |
| T12 | A | TODO | | | |
| T13 | A | TODO | | | |
| T14 | B | TODO | | | |
| T20 | A | TODO | | | |
| T21 | B | TODO | | | |
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
