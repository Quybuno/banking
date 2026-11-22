# N06 — Pipeline dữ liệu lớn cho giao dịch ngân hàng mô phỏng

Tiểu luận môn Dữ liệu lớn. Kafka → Spark Structured Streaming → MinIO (Raw/Bronze/Silver/Gold) → PostgreSQL mart → Metabase; Airflow điều phối theo ngày nghiệp vụ.

Trạng thái: khung repository. Cách cài đặt và chạy sẽ được bổ sung theo từng milestone (xem `PROGRESS.md`).

## Đọc theo thứ tự

1. `docs/spec.md` — đặc tả triển khai bản 1.2 (bắt buộc).
2. `docs/TASKS.md` — phân rã task, người sở hữu, mẫu giao việc cho agent.
3. `CLAUDE.md` — quy tắc cho coding agent (người cũng nên đọc).
4. `PROGRESS.md` — tiến độ hiện tại.
5. `docs/adr/` — các quyết định đã chốt.

## Cấu trúc

```text
.
├── CLAUDE.md                 quy tắc cho agent
├── PROGRESS.md               tiến độ task và milestone
├── contracts/                hợp đồng dữ liệu 1.0.0 — không sửa
│   └── N06_INPUT_CONTRACT_V1/{schemas,config,fixtures,tools}
├── config/                   cấu hình và profile, không có secret
├── infra/                    Docker Compose, Dockerfile, init script
├── src/n06/
│   ├── domain/  generator/  producer/  ingestion/          (A — Mạnh)
│   ├── spark_jobs/  quality/  reconciliation/  storage/    (B — Quang)
│   └── warehouse/  observability/                          (C — Quý)
├── sql/{migrations,dashboard_queries}/
├── dags/                     Airflow (P1)
├── tests/
│   ├── unit/  integration/  e2e/
│   ├── harness/              oracle đọc ground truth — src/ không được import
│   └── fixtures/             fixture bổ sung cho test
├── scripts/                  check_rules.py, stop_hook.sh
├── reports/                  kết quả benchmark do `n06 benchmark` sinh
├── docs/{spec.md,TASKS.md,adr/,architecture.md,runbook.md,benchmark-method.md}
├── .github/                  CI, CODEOWNERS, mẫu PR
└── .claude/                  Stop hook, subagent spec-reviewer (Claude Code)
```

Mỗi thư mục có `README.md` riêng ghi mục đích, người sở hữu, mục đặc tả liên quan và quy tắc.

## Cài đặt

Yêu cầu Python 3.11 (theo `docs/adr/0001-bo-phien-ban.md`). Từ một clone sạch, cài đặt toàn bộ môi trường và công cụ dev bằng một lệnh:

```bash
uv sync --extra dev
```

*(Hoặc nếu đã kích hoạt sẵn môi trường ảo Python 3.11: `pip install -e ".[dev]"`).*

## Kiểm tra hiện có

```bash
python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py   # 14/14 PASS
python scripts/check_rules.py                                      # quy tắc cứng
ruff check .                                                       # lint
ruff format --check .                                              # kiểm tra định dạng
pytest tests/unit -q                                               # unit test
```

## Thiết lập GitHub (làm một lần)

1. Sửa `.github/CODEOWNERS`, thay `<github-A>`, `<github-B>`, `<github-C>` bằng tài khoản thật.
2. Settings → Branches → bảo vệ `main`: bắt buộc PR, bắt buộc check `ci / checks` xanh, bắt buộc review từ code owner.
3. Tạo nhãn `contract-change` cho PR được phép sửa `contracts/` hoặc `tests/fixtures/`.
4. Commit đầu tiên chứa `contracts/` nên chạy với `N06_ALLOW_CONTRACT_CHANGE=1` nếu chạy `check_rules.py` cục bộ.
