# N06 — Phân rã task và giao việc cho coding agent

Tài liệu đi kèm `N06_DAC_TA_TRIEN_KHAI_HOAN_CHINH_v1_2.md` (gọi tắt là **đặc tả**). Đặc tả trả lời *làm gì*; tài liệu này trả lời *ai làm, theo thứ tự nào, giao cho agent ra sao và kiểm soát thế nào để agent làm đúng*.

Đặt file này trong repository tại `docs/TASKS.md`. Đặc tả đặt tại `docs/spec.md`.

---

## 1. Nguyên tắc

1. **Một task = một phiên agent = một nhánh = một PR.** Không giao cả milestone trong một phiên. Phiên dài làm agent quên các ràng buộc đã đọc ở đầu và bắt đầu tự đoán.
2. **Agent chỉ đọc đúng các mục đặc tả ghi trong thẻ task**, cộng với file quy tắc chung. Không bảo agent "đọc toàn bộ đặc tả" ở mỗi task; việc đó chỉ làm một lần ở T00.
3. **Mọi task kết thúc bằng lệnh kiểm tra chạy được**, không bằng lời agent tự xác nhận. Người giao việc tự chạy lại ít nhất một lệnh trước khi merge.
4. **Tầng hợp đồng là bất biến.** `contracts/` và `tests/fixtures/` chỉ đổi qua PR riêng có lý do, được cả nhóm duyệt. Agent không được sửa đáp án để code pass.
5. **Code lõi phải có người đọc** trước khi merge (danh sách ở mục 6.4). Đây cũng là các phần hội đồng hay hỏi khi vấn đáp.

---

## 2. Vai trò và sở hữu

Theo gợi ý ở đặc tả, Phần I, mục 0.3. Nhóm có thể đổi tên người, nhưng giữ nguyên ranh giới.

| Người | Mã | Sở hữu mã nguồn | Câu hỏi nghiên cứu | Báo cáo |
|---|---|---|---|---|
| Mạnh | **A** | `src/n06/domain`, `generator`, `producer`, `ingestion`; `contracts/` | hỗ trợ (i) ở E2, E6 | 2.4, 3.2 |
| Quang | **B** | `quality`, `reconciliation`, `storage` (commit protocol), `spark_jobs` Silver/Gold, `tests/` đúng đắn và phục hồi | (ii), (iii) | 2.6, 3.3 |
| Quý | **C** | `infra/`, `cli.py`, `config.py`, `warehouse/`, `sql/`, `dags/`, `observability/`, lệnh `benchmark` | (i) | 2.7, 3.1, 3.4, 3.5, chương 4 |

Người sở hữu một thư mục là người **duyệt bắt buộc** mọi PR chạm vào thư mục đó (cấu hình bằng `CODEOWNERS` trên GitHub).

---

## 3. Hợp đồng nội bộ phải chốt trước khi làm song song

Ba người làm song song chỉ an toàn khi các điểm nối đã chốt bằng ADR. Thiếu bước này, lúc ghép sẽ phải sửa lại nhiều phần.

| ADR | Nội dung chốt | Người viết | Người duyệt | Chốt trước task |
|---|---|---|---|---|
| ADR-001 | Bộ phiên bản Spark/Hadoop/AWS SDK/Kafka connector/Java/Python (đặc tả I.2) | A | C | T04 |
| ADR-002 | Cấu trúc đường dẫn MinIO: Raw, Bronze, Silver, Gold, statement, tiền tố benchmark | A | B | T22 |
| ADR-003 | Schema bảng control: `ingest_batches`, `current_snapshot`, `runs`, `quality_metrics` | B | A, C | T21 |
| ADR-004 | Schema Bronze/Silver theo entity (tên cột, kiểu, cột lineage) | B | A | T25 |
| ADR-005 | Schema Gold và mart: grain, khóa, cột của 3 fact + 4 dim (đặc tả I.9) | C | B | T41 |
| ADR-006 | Quy tắc đối soát, cutoff, `discrepancy_age_seconds` (đặc tả I.7) | B | A, C | T33 |
| ADR-007 | Cách đo và định dạng file kết quả benchmark (đặc tả I.14.2–14.3) | C | A, B | T51 |
| ADR-008 | Cách làm song song (mục 5 tài liệu này) | cả nhóm | cả nhóm | sau T05 |

---

## 4. Danh sách task

Ký hiệu: **Đọc** = mục đặc tả agent phải đọc; **Phụ thuộc** = task phải merge trước; **Kiểm tra** = điều kiện để PR được merge. Mức P0/P1 theo đặc tả.

### M0 — Môi trường và chốt hợp đồng (P0)

| ID | Người | Việc | Đọc | Phụ thuộc | Kiểm tra |
|---|---|---|---|---|---|
| T00 | C | Repo khởi đầu đã có sẵn khung thư mục, `contracts/`, CI, quy tắc agent. Task này thêm `pyproject.toml`, lockfile, cấu hình ruff/pytest, một unit test mẫu; xác nhận CI chạy cả bước lint + unit | toàn bộ đặc tả (chỉ lần này), `README.md` gốc | — | CI xanh cả 4 bước; `verify_contract.py` 14/14 PASS |
| T01 | C | Hệ thống cấu hình: file mặc định + profile demo/benchmark/test, validate khi khởi động, `.env.example`, không secret trong git | I.3, I.10 | T00 | config sai bị báo lỗi có tên trường; test cho 3 ca sai |
| T02 | C | Docker Compose: Kafka, MinIO, PostgreSQL (database riêng cho mart, control, Airflow, Metabase), Spark; healthcheck, named volume, profile | I.2 | T00 | `docker compose --profile core up` healthy toàn bộ |
| T03 | C | CLI `n06` khung: `doctor`, `init-local`, `--help`, exit code; `doctor` kiểm tra Docker, CPU/RAM/disk, port, Python | I.10, I.12 M0 | T01, T02 | `n06 doctor` báo đúng thiếu gì khi tắt một service |
| T04 | A | Chọn bộ phiên bản theo quy tắc I.2, dựng image Spark kèm JAR đã ghim; compatibility matrix | I.2 | T02 | ADR-001 có nguồn kiểm chứng cho từng số phiên bản |
| T05 | A | Smoke test: Spark đọc/ghi Parquet qua S3A tới MinIO; Spark đọc một topic Kafka | I.2 | T04 | test integration chạy được bằng một lệnh |

**Cổng M0** (đặc tả I.12): config sai bị phát hiện; `doctor` báo cụ thể; smoke test S3A và Kafka chạy; không có secret; README ghi giới hạn máy thực tế.

### M1 — Domain và generator (P0)

| ID | Người | Việc | Đọc | Phụ thuộc | Kiểm tra |
|---|---|---|---|---|---|
| T10 | A | Domain models; validator Draft 2020-12 có format checks; validator nghiệp vụ; mã lỗi theo thứ tự ưu tiên | II.1, II.3, II.8 | T00 | acceptance suite hợp lệ; negative suite ra đúng mã lỗi |
| T11 | A | `canonical_json`, `draw()`, UUIDv5, external_reference | II.5.2 | T10 | khớp toàn bộ `fixtures/golden/deterministic_vectors.json` |
| T12 | A | Generator kịch bản gốc: accounts + opening, chọn tài khoản (`UNIFORM_DISTINCT_PAIR`, `HOT_ACCOUNT_V1`), kiểm tra đủ tiền theo `(event_time, ordinal)` bằng external sort, ghi theo chunk | II.5.1, II.5.2 | T11 | cùng seed → cùng checksum nghiệp vụ; đổi chunk size không đổi kết quả |
| T13 | A | Fault injection đếm chính xác; statement simulator chia file theo cửa sổ; lịch giao file; manifest; `control_report.json`; ground truth tách riêng | II.4, II.5.3 | T12 | số lỗi đúng công thức floor; control report đúng quy tắc dòng; mốc D theo giờ Việt Nam |
| T14 | B | Test harness oracle: từ ground truth + `as_of_time` tính trạng thái và tuổi sai lệch kỳ vọng; khung `n06 validate --against-ground-truth` | II.5.3, II.6, I.7 | T10 | ra đúng ba snapshot của fixture; pipeline không import module này |

**Cổng M1**: golden vector khớp; cùng seed cùng dữ liệu; SUCCESS có 2 vế cân, FAILED không có vế; pipeline không đọc đáp án.

### M2 — Lát cắt end-to-end sạch (P0)

| ID | Người | Việc | Đọc | Phụ thuộc | Kiểm tra |
|---|---|---|---|---|---|
| T20 | A | `n06 publish`: producer idempotent, tốc độ phát, chiến lược key (`transaction_id` mặc định, `account_from`) | II.4, I.14.1 E2/E6 | T13, T05 | đếm message trên topic khớp manifest; đổi key không đổi value |
| T21 | B | Migration control DB theo ADR-003 | I.8.1 | T02 | migration lên/xuống chạy được; unique constraint đúng |
| T22 | A | `n06 stream`: một query `foreachBatch` ghi Raw + Bronze theo `batch_id`, upsert `ingest_batches`; `n06 wait-ingestion` | I.6, I.8.1 (streaming) | T20, T21 | kill tiến trình giữa batch rồi chạy lại: không có dòng `ingest_batches` trùng, đường dẫn bị ghi đè đúng |
| T23 | A | `n06 ingest-statements`: đưa file sao kê lên MinIO theo lịch giao, manifest từng batch | II.4, II.7 | T13, T21 | không file nào xuất hiện trước `file_available_at` theo đồng hồ giả lập |
| T24 | B | Thư viện commit batch: ghi theo `run_id`, cập nhật `current_snapshot` trong một transaction, reader chỉ đọc snapshot hiện hành | I.8.1 (batch) | T21 | test: run thất bại không bị đọc; hai run cùng input cho cùng semantic hash |
| T25 | B | `n06 process-day` bản sạch: Bronze → Silver (dedup cơ bản) → đối soát MATCHED → Gold. **Bắt buộc** gọi `require_unused_run_prefix` (`src/n06/storage/commit.py`) với bản liệt kê object S3 thật trước khi ghi bất kỳ object nào của một run, và dựng đường dẫn bằng `build_run_output_path` (ADR-0002 mục 3 và 6) | I.6, I.7.1 | T22, T23, T24 | dataset clean nhỏ: mọi khóa MATCHED; tổng khớp control report; ghi vào `run_id` đã có object thì báo lỗi |
| T26 | C | `n06 load-mart` tối thiểu: một bảng mart qua staging + một transaction | I.8.2 | T25 | chạy hai lần không tăng số dòng |

**Cổng M2**: tập nhỏ toàn bộ MATCHED; count/total theo ngày/currency khớp control report; chạy lại không tăng count; có lineage nguồn.

### M3 — Chất lượng và đối soát (P0) — chủ yếu B

| ID | Người | Việc | Đọc | Phụ thuộc | Kiểm tra |
|---|---|---|---|---|---|
| T30 | B | Quarantine: error_code, detail, source pointer, raw reference; bản ghi JSON hỏng vẫn có đường tới quarantine | I.6 (Bronze, Silver), II.8 | T25 | negative suite đi qua pipeline ra đúng mã |
| T31 | B | Phân biệt replay với duplicate nghiệp vụ; EVENT_ID_CONFLICT, BUSINESS_KEY_CONFLICT; xuất `key_conflicts` | I.6 (Silver), I.7.1 | T30 | T009, T010, T011 và negative conflict đúng |
| T32 | B | Quality metrics và control totals so với `control_report.json`; ngưỡng chặn publish | I.6 (control totals) | T30 | fixture khớp từng dòng control report |
| T33 | B | Đối soát đủ 8 trạng thái theo thứ tự ưu tiên, `issue_codes`, cutoff theo `as_of_time`, `anchor_event_time`, `discrepancy_age_seconds` | I.7, ADR-006 | T31, T14 | từng khóa khớp oracle của T14 |
| T34 | B | Kiểm thử nghiệm thu: ba snapshot tính lại độc lập từ đầu với đồng hồ giả lập; mỗi snapshot chạy hai lần | II.6, II.7 | T33 | `n06 validate --against-ground-truth` PASS cả ba snapshot |

**Cổng M3**: ba snapshot khớp từng khóa và tổng hợp; negative suite đúng mã lỗi.

### M4 — Số dư, mart, dashboard (P0)

| ID | Người | Việc | Đọc | Phụ thuộc | Kiểm tra |
|---|---|---|---|---|---|
| T40 | B | `fact_daily_balances` nhiều ngày, gồm tài khoản không phát sinh | I.9, II.6 (số dư) | T34 | 93.600 / 56.400 / 93.850 / 56.150 trên fixture; opening + movement = closing |
| T41 | C | DDL mart đủ 3 fact + 4 dim theo ADR-005; publish thay thế cả phạm vi ngày trong một transaction; xóa dòng cũ không còn trong output | I.8.2, I.9 | T26, T40 | test "output mới bỏ một record cũ" PASS |
| T42 | C | `n06 backfill` điều phối process-day + load-mart theo khoảng ngày và `as_of_time` | I.8.3, I.12 (kịch bản mẫu) | T41 | kịch bản nghiệm thu mẫu ở I.12 chạy hết |
| T43 | C | Truy vấn dashboard trong `sql/dashboard_queries/`; dựng dashboard Metabase; hướng dẫn tái lập | I.9 | T41 | đủ các báo cáo của đề cương 2.7, gồm tuổi sai lệch |

**Cổng M4**: số dư đúng trên fixture; chạy lại không trùng và xóa được dòng cũ; dashboard đủ báo cáo.

### M5 — Phục hồi và benchmark lõi (P0)

| ID | Người | Việc | Đọc | Phụ thuộc | Kiểm tra |
|---|---|---|---|---|---|
| T50 | B | Kiểm thử phục hồi: tắt streaming query giữa chừng, kill job batch, chạy lại | I.8.1, I.13 (E2E, Idempotency) | T42 | không mất record đã được xác nhận; Gold không nhân đôi |
| T51 | C | Lệnh `n06 benchmark`: lấy mẫu CPU/RAM theo thời gian, Spark event log, ghi CSV/JSON kết quả kèm commit SHA, seed, config hash | I.14.2, I.14.3, ADR-007 | T42 | chạy thử E1 ở mức smoke ra file kết quả đúng định dạng |
| T52 | C + A | Chạy E1, E3, E4, E5 (C) và E2 (A); mỗi cấu hình lặp theo I.14.1; mỗi lần đo phải đúng kết quả | I.14 | T51 | file kết quả thật; lần đo sai không được tính |
| T53 | cả nhóm | README chạy từ clean clone; test report; báo cáo benchmark | I.12 (sau M5), I.15, I.16 | T50, T52 | một thành viên khác clone sạch và chạy được theo README |

**Cổng M5**: đã có bản nộp bảo vệ được.

### M6 — P1, làm sau khi cổng M5 đạt

| ID | Người | Việc | Đọc |
|---|---|---|---|
| T60 | C | Airflow DAG daily và backfill; readiness theo `ingest_batches` và manifest; retry | I.2 (cách Airflow gọi Spark), I.10 |
| T61 | B | Sửa kết quả đã publish khi sao kê đến muộn; `first_seen_at`, `resolved_at`; backfill ledger ngày cũ lan sang số dư ngày sau | I.7.3, I.8.3 |
| T62 | C | Drill-down một sai lệch tới nguồn; runbook restart/backfill | I.9, I.10 |
| T63 | A | E6 (lệch tải, `HOT_ACCOUNT_V1`) | I.14 |
| T64 | B | E7 (file nhỏ) | I.14 |
| T65 | C | E8 (baseline DuckDB) | I.14 |

**Cổng M6**: ba snapshot chạy **nối tiếp** cho đúng chuỗi T007 PENDING → MATCHED và T008 PENDING → MISSING_EXTERNAL → MATCHED, mart được sửa đúng; DAG chạy lại một ngày cho cùng kết quả; có số đo E6–E8.

M7 (P2) chỉ làm khi M6 xong; chọn hạng mục ở đặc tả I.1, mỗi hạng mục một task riêng.

---

## 5. Làm song song

Đặc tả I.12 quy định không bắt đầu milestone sau khi cổng trước chưa đạt. Áp dụng nguyên văn thì ba người phải chờ nhau. Đề xuất ghi vào ADR-008:

- **Cổng áp dụng cho việc merge vào `main`**, không cấm chuẩn bị trên nhánh.
- Được chuẩn bị sớm khi chỉ dùng **fixture cố định** thay cho đầu ra của task chưa xong. Ví dụ: B viết T33 trên `tests/fixtures/acceptance/` trong lúc A làm T12–T13; C viết DDL mart T41 theo ADR-005 trong lúc B làm M3.
- PR của milestone sau chỉ được merge khi cổng milestone trước đã đạt trên `main`.

Lịch song song gợi ý:

| Giai đoạn | A — Mạnh | B — Quang | C — Quý |
|---|---|---|---|
| 1 | T04, T05 | duyệt ADR-001, viết ADR-003, ADR-006 | T00 → T03 |
| 2 | T10 → T13 | T14, T21, T24 | chuẩn bị T41 trên ADR-005, T51 khung |
| 3 | T20, T22, T23 | T25, rồi T30 → T34 | T26, rồi T41 → T43 |
| 4 | T52 (E2) | T40, T50 | T42, T51, T52 |
| 5 | T53 và báo cáo | T53 và báo cáo | T53 và báo cáo |

---

## 6. Cách giao việc cho agent

### 6.1. File quy tắc chung

Claude Code đọc `CLAUDE.md` ở gốc repo. Antigravity có cơ chế rules riêng; đặt cùng nội dung vào đó và kiểm tra tài liệu của phiên bản đang dùng để biết vị trí chính xác. Giữ file ngắn; chi tiết nằm ở `docs/spec.md`.

```markdown
# N06 — Quy tắc cho coding agent

## Bối cảnh
Pipeline dữ liệu lớn cho giao dịch ngân hàng mô phỏng (tiểu luận môn Dữ liệu lớn).
Đặc tả bắt buộc: docs/spec.md. Danh sách task: docs/TASKS.md. Tiến độ: PROGRESS.md.
Quyết định đã chốt: docs/adr/. Đọc ADR liên quan trước khi code.

## Lệnh
- Lint: ruff check . && ruff format --check .
- Unit test: pytest tests/unit -q
- Kiểm tra hợp đồng: python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py
- Integration: [ĐIỀN lệnh sau T05]

## Quy tắc cứng
- Chỉ làm task được giao. Không làm việc của task khác, không thêm tính năng ngoài thẻ task.
- Không sửa contracts/ hoặc tests/fixtures/. Không sửa expected/đáp án để test pass.
- Tiền là số nguyên (amount_minor). Không float. Kiểm tra miền trước khi ghi BIGINT.
- business_date theo Asia/Ho_Chi_Minh; timestamp hợp đồng là UTC dạng ...Z.
- Không hardcode endpoint, credential, bucket, topic, ngày, seed, quy mô, partition, executor.
- Pipeline không đọc ground truth hay expected_results. Chỉ test harness được đọc.
- Không dùng TODO/pass/pseudocode thay nghiệp vụ lõi. Không giả số đo benchmark.
- Không tự chọn cách hiểu khi đặc tả mơ hồ: dừng, nêu mâu thuẫn, đề xuất ADR.

## Kết thúc mỗi task
Chạy lint, unit test, verify_contract và lệnh kiểm tra của task. Dán output thật.
Báo cáo: file đã đổi; lệnh đã chạy và kết quả; giả định đã đặt; phần chưa chạy được và lý do;
đề xuất cập nhật PROGRESS.md. Không commit khi chưa được xác nhận.
```

### 6.2. Thẻ task — mẫu prompt mở phiên

Mỗi task mở một phiên mới (Claude Code: `/clear` hoặc phiên mới; bật plan mode). Dán thẻ sau, điền từ bảng ở mục 4.

```text
TASK [ID] — [tên việc]

Đọc trước khi lập kế hoạch:
- docs/spec.md: [các mục ở cột "Đọc"]
- docs/adr/[các ADR liên quan]
- PROGRESS.md (trạng thái hiện tại)

Mục tiêu: [một câu]
Phụ thuộc đã merge: [ID]. Đầu vào dùng: [module/bảng/đường dẫn có sẵn].
Được tạo/sửa: [thư mục/file]. Không được sửa: contracts/, tests/fixtures/, [module của người khác].
Ngoài phạm vi: [việc của task khác cần tránh].

Điều kiện hoàn thành:
1. [kiểm tra ở cột "Kiểm tra"]
2. Lint, unit test, verify_contract đều PASS.
3. Có test mới cho hành vi của task, expected tính độc lập (không dùng chính hàm đang test).

Quy trình:
1. Lập kế hoạch: file sẽ tạo, hàm chính, quyết định thiết kế, cách kiểm tra. DỪNG chờ tôi duyệt.
2. Hiện thực sau khi được duyệt.
3. Chạy toàn bộ kiểm tra, dán output thật.
4. Báo cáo theo mục "Kết thúc mỗi task" trong CLAUDE.md.
```

Ví dụ đã điền cho T33:

```text
TASK T33 — Đối soát đủ 8 trạng thái và tuổi sai lệch

Đọc trước khi lập kế hoạch:
- docs/spec.md: Phần I mục 7 (toàn bộ), mục 9 (dòng fact_reconciliation); Phần II mục 1.9–1.11, mục 6
- docs/adr/ADR-004, ADR-006
- PROGRESS.md

Mục tiêu: từ Silver hợp lệ và key_conflicts, tạo kết quả đối soát hiện tại cho mỗi match key tại một as_of_time.
Phụ thuộc đã merge: T31, T14. Đầu vào: Silver transactions/statements, key_conflicts, quality metadata.
Được tạo/sửa: src/n06/reconciliation/, tests/unit/reconciliation/, tests/integration/test_recon_*.py.
Không được sửa: contracts/, tests/fixtures/, src/n06/quality/, oracle của T14.
Ngoài phạm vi: first_seen_at/resolved_at (T61), publish mart (T41), lịch sử đối soát (P2).

Điều kiện hoàn thành:
1. Từng khóa của ba snapshot khớp oracle T14 khi mỗi snapshot tính lại từ đầu.
2. Thứ tự ưu tiên trạng thái đúng bảng 7.2; một khóa nhiều lỗi có đủ issue_codes.
3. as_of đúng tại cutoff được coi là quá hạn.
4. discrepancy_age_seconds đúng định nghĩa 7.3; MATCHED để rỗng; chạy lại không đổi giá trị.
5. Group theo match key và kiểm tra cardinality trước khi join; không many-to-many.
6. Lint, unit test, verify_contract PASS.

Quy trình: như mẫu.
```

### 6.3. Prompt review — chạy trong phiên sạch

Dùng subagent reviewer (Claude Code) hoặc mở một phiên mới hoàn toàn. Reviewer **không sửa code**.

```text
REVIEW PR cho TASK [ID]. Không sửa code, chỉ báo cáo.

Đọc: thẻ task [ID] trong docs/TASKS.md; các mục đặc tả trong thẻ; diff của nhánh so với main.

Kiểm tra:
1. Mỗi điều kiện hoàn thành: đạt / chưa đạt / không kiểm chứng được, kèm dòng code hoặc output.
2. Có thay đổi trong contracts/ hoặc tests/fixtures/ không. Có test nào bị xóa, skip hoặc nới expected không.
3. Có hardcode endpoint/ngày/seed/quy mô/partition không.
4. Có dùng float cho tiền, có đọc ground truth trong pipeline không.
5. Có làm việc ngoài phạm vi task không.
6. Chỗ đặc tả bị hiểu khác với văn bản (trích nguyên văn đặc tả và đoạn code).
7. Test có expected tính độc lập không, hay gọi chính hàm đang test.

Xếp phát hiện theo mức: chặn merge / nên sửa / ghi chú.
```

### 6.4. Code người phải tự đọc

Không merge các phần sau chỉ dựa vào CI và reviewer agent:

| Phần | Task | Người đọc |
|---|---|---|
| `foreachBatch`, ghi theo `batch_id`, upsert `ingest_batches` | T22 | B và A |
| Cập nhật `current_snapshot` và reader | T24 | B và C |
| Dedup, conflict, `key_conflicts` | T31 | B và A |
| Bảng ưu tiên trạng thái, cutoff, tuổi sai lệch | T33 | B và C |
| Publish mart trong một transaction | T41 | C và B |
| Cách đo trong `benchmark` | T51 | C và A |

### 6.5. Khi agent làm sai

```text
TASK [ID] chưa đạt.

Hiện tượng: [lệnh đã chạy + output lỗi nguyên văn]

Yêu cầu:
1. Tìm nguyên nhân gốc trước khi sửa; nêu 2–4 câu, chỉ ra dòng code.
2. Sửa tối thiểu trong phạm vi task. Không sửa/xóa test, không sửa fixture.
3. Chạy lại toàn bộ kiểm tra của task, dán output.
Nếu nguyên nhân nằm ở task khác hoặc ở đặc tả: DỪNG và báo, không sửa lan.
```

Quy tắc xử lý:

- Agent sửa fixture/expected → hoàn tác (`git checkout -- contracts tests/fixtures`), nhắc lại quy tắc.
- Agent sửa sai hai lần liên tiếp cùng một lỗi → dừng phiên, mở phiên mới chỉ với thẻ task, `PROGRESS.md` và output lỗi.
- Agent phát hiện đặc tả mâu thuẫn → không cho agent tự quyết; người sở hữu viết ADR, cả nhóm duyệt, rồi mới tiếp tục.

---

## 7. Lớp cưỡng chế tự động

Lời dặn trong prompt không đủ; các lớp sau chặn được lỗi mà không phụ thuộc agent có tuân thủ hay không.

| Lớp | Cách làm | Chặn được |
|---|---|---|
| CI trên mỗi PR | lint, format check, unit test, `verify_contract.py`, migration check | code không chạy, gói hợp đồng bị hỏng |
| Bảo vệ hợp đồng | job CI fail nếu diff chạm `contracts/` hoặc `tests/fixtures/` mà PR không có nhãn `contract-change` | agent sửa đáp án |
| `CODEOWNERS` + branch protection | PR vào `main` cần CI xanh và người sở hữu thư mục duyệt | merge code chưa ai đọc |
| Stop hook (Claude Code) | chạy lint + unit test + `verify_contract.py` khi agent định kết thúc lượt; lỗi thì agent phải tiếp tục | agent báo "đã xong" khi còn đỏ |
| Kiểm tra cấm hardcode | job grep các mẫu như `localhost`, `9092`, `minio:9000`, `2026-01-0` trong `src/` | hardcode cấu hình |

---

## 8. Theo dõi tiến độ

`PROGRESS.md` ở gốc repo, mỗi task một dòng; agent đề xuất cập nhật, người merge chốt.

```markdown
| ID | Trạng thái | PR | Bằng chứng | Ghi chú |
|---|---|---|---|---|
| T00 | DONE | #1 | CI run #12; verify 14/14 | |
| T10 | IN_REVIEW | #5 | negative suite PASS (CI run #20) | chờ A đọc mã lỗi |
| T12 | BLOCKED | — | — | chờ ADR-001 |
```

Trạng thái: `TODO`, `IN_PROGRESS`, `IN_REVIEW`, `DONE`, `BLOCKED`. Một milestone chỉ được đánh dấu xong khi mọi task của nó DONE và cổng milestone đã chạy trên `main`, có bằng chứng ghi kèm.

Mỗi tuần nhóm họp ngắn: xem các dòng BLOCKED, ADR đang chờ duyệt, và quyết định có cắt hạng mục P1 nào không. Hạng mục bị cắt ghi vào mục hạn chế của báo cáo theo đặc tả I.1.
