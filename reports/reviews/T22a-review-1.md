**Review T22a (PR #20, `git diff origin/main...HEAD`, commit 952c99b).** Kết luận: không có phát hiện chặn merge. `docs/TASKS.md` chỉ có thẻ T22, không có T22a, nên tôi đối chiếu theo mô tả được giao. Đường dẫn dưới đây tính từ `/home/quybuno/banking/`.

## Phải sửa
Không có.

## Nên sửa

1. **Thứ tự và bộ lọc status của danh sách batch không có test.**
   - `tests/integration/test_control_store_in_image.py:47-60` chỉ tạo một dòng (`batch_id=7`, `COMMITTED`).
   - Theo đọc code (chưa chạy mutation), bỏ `AND status = 'COMMITTED'` hoặc đổi `ORDER BY batch_id` trong `_LIST_INGEST_SQL` (`src/n06/storage/control_store.py:718-728`) hay bản psycopg (`:534-535`) vẫn xanh.
   - Nên thêm `batch_id` 10 và 2, cùng một dòng status khác.

2. **Test chưa đưa giá trị khó vào khóa.**
   - `test_control_store_in_image.py:62-93` dùng chuỗi an toàn cho `run_id`, `dataset_id`, `layer`, `entity` (tức các giá trị trong `WHERE` và `scope`). `manifest_path` chỉ là `None`.
   - `tests/unit/storage/test_psql_params.py:30-52` chỉ kiểm `upsert_ingest_batch` và `list_committed_ingest_batches`. `start_run`, `commit_run_atomic`, `fail_run` không có kiểm tra "SQL không chứa giá trị". `get_run` chỉ được kiểm là marker có trong script, `resolve_current_snapshot` không được kiểm.
   - Code hiện tại đúng (probe của tôi dùng giá trị khó cho `dataset_id`, `entity`, `manifest_path`, `run_id`), nhưng test không giữ được điều đó.

3. **Lệch câu chữ ADR-0012, nên ghi lại cho người duyệt.**
   - ADR, quyết định 1 điểm 3: *"Giao diện bộ chạy `(sql, tuples_only)` phải nhận thêm biến."*
   - Code (`control_store.py:730-731`) giữ `PsqlRunner = Callable[[str, bool], str]` và nhúng biến vào script bằng `\set`: `# Runner: (kịch bản, tuples_only) -> stdout; kịch bản đã gồm biến tham số.`
   - ADR cũng mô tả cơ chế `-v params=...`. Phần "Giới hạn đã biết" mới chỉ gợi ý stdin (chưa thử). Mục tiêu vẫn đạt, nhưng nên có một dòng trong lịch sử ADR hoặc mô tả PR.

4. **Nên ghim `encoding="utf-8"` ở `subprocess.run`** (`control_store.py:800` và `:1028`).
   - Code ép `PGCLIENTENCODING=UTF8`, còn Python mã hóa stdin theo locale (`text=True`).
   - Trong container spark hiện đúng (`UTF-8`; locale POSIX cho `utf-8`).
   - Theo hiểu biết của tôi (chưa tra mạng), psql 16.6 chưa có bản vá CVE-2025-1094 (sửa ở 16.7): lỗi trích dẫn khi đầu vào sai mã hóa. Với `str` mã hóa UTF-8 strict thì đường này không tới được.
   - Thử byte sai trực tiếp: server từ chối với `invalid byte sequence for encoding "UTF8": 0xc2 0x27`. Tôi không thử khai thác sâu hơn.

## Ghi nhận

**Điều kiện của thẻ**

| Điều kiện | Kết quả | Bằng chứng |
|---|---|---|
| Migration 0002 up/down, down lỗi rõ khi còn NULL | Đạt | output bên dưới |
| `dataset_id: str \| None` | Đạt | `control_store.py:74` |
| Đổi giao diện, hai hiện thực, store giả, `commit.py` | Đạt (bản psycopg chưa chạy) | `control_store.py:122-126, 484-545`; `test_commit_protocol.py:159-174` |
| SQL psql không ghép giá trị, tham số qua stdin | Đạt | f-string chỉ nhúng hằng `_PARAMS_TABLE`, `_PARAMS`, `_COMMITTED_SQL`; khối `DO` đọc bảng tạm |
| Test trong image trên Postgres thật | Đạt, trừ độ phủ ở mục 1 và 2 | `1 passed in 3.57s` |
| compatibility-matrix | Đạt | dòng 23; trong container `psql (PostgreSQL) 16.6`, `ldd` thấy `libpq.so.5` |
| Digest | Khớp | Compose, Dockerfile và `RepoDigests` cục bộ cùng `sha256:557fea37…2f36`, amd64 |
| Không thêm phụ thuộc | Đạt | `pyproject.toml`, `uv.lock` không có trong diff |

- Không có thay đổi trong `contracts/`, `tests/fixtures/`. Không test nào bị xóa, thêm skip hay nới. Expected `["0001"]` thành `["0001","0002"]` là hệ quả ADR-0004 mục 8. `test_control_migrations.py` bỏ skip theo socket và đường psycopg, nay lỗi thay vì skip khi Postgres tắt.
- Không thấy hardcode mới, float cho tiền, đọc ground truth, hay việc ngoài phạm vi.
- Expected của `test_psql_params.py` viết tay, tôi tính lại thấy đúng. Test trong image so với hằng ở phía host. Ca rollback sẽ bắt được việc bỏ `BEGIN/COMMIT` (theo đọc code).

**Hành vi T24, so main với nhánh** (cùng 26 bước, hai database tạm, psql 16.6 trong container postgres; đã chuẩn hóa số dòng trong thông báo)
- `steps: 26 26 diffs: 4 dbdump equal: True`.
- Hai khác biệt (status sai, `row_count` âm): cùng lỗi ràng buộc, khác đoạn CONTEXT và timestamp.
- Tràn BIGINT: `bigint out of range` thành `value "9223372036854775808" is out of range for type bigint`.
- `output_path=None`: main ném `AttributeError`, nhánh ném `CommitError`.
- Thông báo lỗi trạng thái run giống main. Thứ tự khóa giữ nguyên, chỉ thêm tạo bảng tạm trước advisory lock.
- `commit waited on external advisory lock (s): 4.59` (lần chạy trước 3.22).
- 8 commit đồng thời cùng scope và 8 upsert cùng khóa: `errors: [] | ingest rows: 1 | snaps: 1`.

**Main có lỗi mà nhánh đã sửa.** Chạy mã main với giá trị chứa `$$`: khối `DO` bị đóng sớm và psql đọc phần còn lại như meta-command (`\z: extra argument ";" ignored ... invalid command \"`). Nhánh chạy đúng.

**Trích dẫn**
- `values: 4025 returned: 4025 mismatch: 0 largest bytes: 3000000`; `individual fixed mismatches: 0 of 24`. Bộ thử gồm `\.`, xuống dòng kèm `\!`, backtick, `:'params'`, `$$`, ký tự điều khiển, U+2028.
- NUL: cả main và nhánh đều ném `CommitError` (nhánh: `unsupported Unicode escape sequence`).
- Surrogate lẻ: cả hai ném `UnicodeEncodeError`, không bọc thành `CommitError` (có sẵn từ main).

**Migration** (database tạm)
```
up 0002 on data: ['0002']
full down w/ NULL -> ... ERROR:  column "dataset_id" of relation "ingest_batches" contains null values
applied: ['0001', '0002'] rows: ['2']
full down: ['0002', '0001'] up: ['0001', '0002']
```

**Khác**
- `_UPSERT_INGEST_SQL` (`control_store.py:694-700`) tạo bảng tạm dù không có khối `DO`, làm phình catalog theo mỗi microbatch. Có thể thay bằng subquery.
- Lỗi upsert ở đường psql không nêu `query_name`/`batch_id` như bản psycopg (`:517-521`).
- `CommitError` chứa nguyên stderr, gồm `DETAIL: Failing row contains (...)` với giá trị. Main cũng vậy.
- Không test nào gọi `BatchCommitCoordinator.list_committed_ingest_batches`; hai hàm ingest của store giả cũng không được gọi.
- `idx_ingest_batches_dataset_date` không còn hữu ích (ADR-0004 mục 8), thẻ không yêu cầu xóa.
- Mọi máy phải build lại image spark, nếu không test mới sẽ đỏ. Trong `src`, `scripts`, `infra` tôi không thấy chỗ nào tự gọi `migrate_up`. DB control dùng chung trên máy này đã ở `0001`, `0002` (truy vấn chỉ đọc).
- Diff đụng `storage/`, `sql/control` (B) và Compose (C), cần người sở hữu duyệt. `PROGRESS.md:36` còn ghi T22 `BLOCKED` chờ ADR, nên cập nhật.

**Output lệnh**
```
check_rules: OK
PASS: Manifest SHA-256, physical rows, UTF-8/LF and safe paths
PASS: All positive fixture records satisfy package schema subset with format checks
PASS: Generator config schemas and cross-field constraints
PASS: Business keys, account references, currencies, dates and chronology
PASS: Envelope consistency; replay dedup yields exact reference business records
PASS: Ledger completeness, balanced sides and transaction linkage
PASS: Hand-authored expected counts and per-currency control totals
PASS: Hand-authored control report: schema, row rules and per-group totals
PASS: Two-day opening/movement/closing arithmetic and no overdraft
PASS: Actual statement batches and delivery schedule equal reference without early release
PASS: All three reconciliation snapshots match every key and expected histograms
PASS: Before, exactly at and after cutoff use an unambiguous inclusive deadline
PASS: Negative records, ID conflicts, incomplete/unbalanced ledger and malformed JSON
PASS: Golden vectors for draw(), UUIDv5 identities and external_reference
14 check groups passed. Report: .../contracts/N06_INPUT_CONTRACT_V1/VERIFICATION_REPORT.json
ruff: All checks passed! 117 files already formatted
tests/unit: 705 passed in 141.07s
```

## Chưa kiểm
- Các test integration khác, gồm `test_control_migrations.py` và `test_commit_postgres.py` (ngoài phạm vi được phép chạy). Tôi chỉ probe hành vi tương ứng trên database tạm.
- `PsycopgControlStore` trên Postgres thật.
- Build image: tôi dùng image có sẵn. Digest không đối chiếu với registry.
- `scripts/smoke.sh`.

## Về ràng buộc
- `git status` cuối chỉ còn hai file untracked `SUPERVISOR_BRIEF.md*` có từ trước. `verify_contract.py` tự ghi file báo cáo của nó, không làm đổi file nào git theo dõi.
- Database tạm đã xóa hết (đếm còn 0). Không in giá trị `.env`. Không dừng, khởi động hay build container/image; chỉ `exec` lệnh đọc, và test trong image tự `run --rm`.
- Một lệnh `grep` toàn repo của tôi vô tình in ra một dòng của `SUPERVISOR_BRIEF.md`. Tôi không mở file và không dùng nội dung đó cho báo cáo.
- Script probe: `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review20/probe2.py` (phần probe migration nằm ở `/tmp/claude-1000/-home-quybuno-banking/0cdf45d8-973b-4848-b3a8-b08defd766e4/scratchpad/review20/probe.py`).