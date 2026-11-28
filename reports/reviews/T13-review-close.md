# Review đóng T13 (fault injection, sao kê, manifest, control_report, ground truth)

Nguồn: báo cáo nguyên văn của subagent `spec-reviewer` (một vòng), nhánh `task/T13-close` trùng `origin/main` tại `603787f`.

Worktree: `/home/quybuno/banking/.claude/worktrees/agent-a35edba8c2252234f`, nhánh `task/T13-close`, trùng `origin/main`. Tôi đọc trực tiếp toàn bộ `src/n06/generator/`, `tests/unit/generator/` và `tests/integration/test_generator_*.py`. Không sửa file nào, không chạy Docker, integration hay spark.

## Kết luận

Không có phát hiện chặn merge. Mã khớp thẻ T13, spec mục 4, 5.2, 5.3 và ADR-0009 (Accepted). Mã cũng khớp ADR-0011 (Proposed) từng chữ.

Có 4 mục "nên sửa" và 6 ghi chú. Điều kiện cần xử lý trước khi đóng T13: Accept ADR-0011, cập nhật `PROGRESS.md`, và quyết định cách xử lý bố cục tên đầu ra (mục N1).

## 7. Kết quả hai lệnh bắt buộc (chạy bằng `.venv` của worktree)

```
$ python scripts/check_rules.py
check_rules: OK
EXIT=0

$ python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py
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
EXIT=0
```

Tôi chạy thêm các lệnh sau, và `git status` sạch sau khi chạy:
- `pytest tests/unit -q`: toàn bộ PASS. Riêng `tests/unit/generator` là 197 test PASS.
- `ruff check` và `ruff format --check` trên `src/n06/generator`, `tests/unit/generator` và các test integration generator: PASS.

## 1. Điều kiện hoàn thành của thẻ

| Điều kiện | Kết quả | Bằng chứng |
|---|---|---|
| Số lỗi đúng công thức floor | Đạt | `src/n06/generator/fault.py:55-57`: `(eligible_count * basis_points[name]) // BASIS_POINT_DENOMINATOR`. Thứ tự cắt đoạn ở `fault.py:14-25` là `FAULT_ORDER`, đúng thứ tự mục 5.3. Khóa xếp hạng ở `fault.py:63-66` là SHA-256 của canonical JSON `[contract_version, seed, 'fault-order', transaction_id]`, tie-break theo transaction_id (`fault.py:100-106`). Tập eligible là SUCCESS canonical, đếm ở `dataset.py:266-268`. Test độc lập: `tests/unit/generator/test_fault_counts.py:132-139` (lưới eligible x bp), `:162-202` (tự xếp hạng bằng hashlib/json rồi so với `fault_assignment.jsonl` và `fault_counts.json`). |
| Control report đúng quy tắc dòng | Đạt | `control_report.py:42-53`: sắp theo `(entity, business_date, currency, status_or_side)`, lọc `count >= 1`, mỗi tổ hợp một lần (khóa dict). `write_control_report` ghi một object trên một dòng, kết thúc LF. `dataset.py:327-330` bỏ giao dịch `missing_internal`; replay không được cộng vì totals thêm một lần theo bản ghi canonical. Test: `test_control_report.py:23-28` khớp từng byte với `fixtures/acceptance/control_report.json` (viết tay), `:70-89` kiểm quy tắc dòng, `:92-132` tự tính tổng độc lập từ canonical trừ `missing_internal`. |
| Mốc D theo giờ Việt Nam | Đạt | `statements.py:24-30`: 00:00 theo `business_timezone` của ngày sau, đổi sang UTC. Cấu hình lấy từ config, không hardcode `Asia/Ho_Chi_Minh` trong `src`. `statements.py:33-43`: late fault = D + delay, normal = event_time + normal delay. Test: `test_statements_window_and_d.py:248-262` (literal, gồm ví dụ spec `2026-01-01 -> 2026-01-01T17:00:00Z`, năm nhuận, qua năm), `:265-271`, `:274-288`, `:415-424` (D tự tính bằng UTC-7 rồi đối chiếu `available_at` và cutoff). |
| Chia file sao kê theo cửa sổ | Đạt | `statements.py:83-86`: `w = epoch // W`, `file_available_at = (w+1)*W`. `dataset.py:109-215`: mỗi `(w, channel)` một file, dòng sắp theo `(available_at, statement_record_id)` (`dataset.py:346`), mỗi file một manifest với `source_batch_id = uuid("source_batch", w, "statement:<channel>")` (`params.py:78-79`). Test: `test_statements_window_and_d.py:291-307`, `:329-351`. |
| Lịch giao file | Đạt | `dataset.py:181-191`: bản ghi đúng `statement_delivery` schema, kiểm lúc ghi. `delivery_order` tăng theo `(file_available_at, channel)`, mỗi file một số (`dataset.py:208`), theo ADR-0009 quyết định 3. Test: `:354-366`. |
| Manifest | Đạt | `manifest.py:24-40`: sha256 và `physical_rows` tính từ byte thật, path sắp tăng dần. Manifest shard 0 gồm `accounts.jsonl`, `control_report.json` và events shard 0, ghi sau cùng (`dataset.py:359-374`), đúng ADR-0009 quyết định 1. Test: `test_manifest_schema.py:264-302` tự tính sha256 và đếm LF, kiểm mọi file nguồn thuộc đúng một manifest. |
| Ground truth tách riêng khỏi prefix operational | Đạt (ở mức thư mục cục bộ) | `layout.py:30` và `:107`: `source/` và `ground_truth/` là hai thư mục cùng cấp, `layout.py:60-63` bắt buộc tên khác nhau. `canonical/` do T12 ghi thẳng vào `ground_truth/`, không sao chép. `test_ground_truth_isolation.py:161-169` quét toàn bộ `source/` không có nhãn lỗi, từ `ground_truth`, `kind`, `expected_`. Ánh xạ sang bucket/prefix object store nằm ngoài T13. |

## 2. Thay đổi hợp đồng, fixture, test

- `contracts/` và `tests/fixtures/`: không có thay đổi. `git log -- contracts tests/fixtures` chỉ có commit T00 `8f6dd2b`.
- Test bị xóa: không có (`git log --diff-filter=D -- tests` rỗng).
- Test bị skip hoặc xfail trong `tests/unit/generator` và `tests/integration/test_generator_*.py`: không có (grep `skip|xfail|importorskip` rỗng).
- Đổi expected: commit `7bdf3c2` (ADR-0011) đổi expected của 3 test trong `tests/unit/generator/test_config_normal_delay.py` (`test_check_matches_independent_cutoff_arithmetic`, `test_boundary_cases_are_exactly_at_and_just_below_cutoff`, `test_build_accepts_delay_just_below_cutoff`, dòng 20-80). Thay đổi này làm chặt (cộng thêm W), không phải nới. ADR-0011 đã ghi rõ ba test này. Xem mục "Ghi chú" G1.

## 3. Hardcode, float, ground truth

- Float cho tiền: không có. Grep `float|round(|Decimal` trong `src/n06/generator` không ra kết quả. `amount_minor` luôn là `int`. `statements.py:54` dùng `MAX_AMOUNT_MINOR` từ `n06.domain` (10^12, `models.py:22`).
- Endpoint, ngày, seed, quy mô, partition: không có literal trong `src/n06/generator`. Mọi giá trị lấy từ `DatasetParams.from_config`. `LOGICAL_SHARD_ROWS = 10_000` (`params.py:11`) là quy ước identity v1, đúng spec 5.2 điểm 6. `_CURRENCY_SWAP` (`statements.py:14`) là enum hợp đồng. Các tên thư mục và file mặc định: xem N1.
- `src` đọc ground truth hoặc import `tests`: không có. `src/n06/generator` chỉ ghi `ground_truth/` và không đọc ngược. `build_control_report_from_envelopes` chỉ đọc envelope trong `source`. Không có import `tests.` hay `expected_results` trong `src`. Ngoài generator, `src/n06/cli/generate.py:128` gọi `build_dataset`. `src/n06/ingestion/statements.py:25` có import, xem G3.

## 4. Việc ngoài phạm vi thẻ

Không phát hiện. `layout.py` (`OutputLayout`) và `ground_truth.py` nằm trong ý "ground truth tách riêng". CLI `generate` thuộc task khác và chỉ gọi `build_dataset`. Commit `4d62ca3` chỉ sửa dòng nào trong `dataset.py` để tách `public_message` (che giá trị bị từ chối), không đổi nghiệp vụ.

## 5. Chỗ code hiểu khác đặc tả

- **Đã được duyệt** (ADR-0009, quyết định 4). Spec dòng 787: "Xuất ground truth gồm fault assignment của từng giao dịch, ..." Mã (`dataset.py:296-315`) chỉ ghi giao dịch bị tiêm lỗi vào `fault_assignment.jsonl`, giao dịch không lỗi vắng mặt. Đã ghi rõ trong ADR và README. Không phải phát hiện mới.
- **Chặt hơn chữ spec, theo ADR-0011 (Proposed).** Spec dòng 761: "Normal delay phải giữ statement trước cutoff." Mã `params.py:90-116` yêu cầu `normal_delay + W <= allowed_lateness`, tức xét cả `file_available_at`. Tôi kiểm tra số học: `worst = normal - 1 + W`, từ chối khi `worst >= allowed`. Điều này tương đương `normal + W > allowed`, đúng với ADR và spec 1.10 ("tại đúng cutoff đã quá hạn"). Ca biên `normal + W == allowed` được chấp nhận. Ba profile `generator.demo-clean`, `demo-faults` và `bench-skew` (60 / 3600 / 86400) đều qua. Dùng đúng ADR-0011 làm căn cứ, nên chờ ADR được Accept (xem N2).
- Các điểm còn lại khớp nguyên văn:
  - D: spec dòng 802 là "00:00:00 theo `Asia/Ho_Chi_Minh` của ngày sau business_date", mã ở `statements.py:24-30`.
  - Cửa sổ file: spec 5.2 là "`file_available_at` = (w + 1) × W", mã ở `statements.py:83-86`.
  - Duplicate: bản trùng dùng discriminator `duplicate` (`statements.py:66-68`, ADR-0009 quyết định 2).
  - Mọi `emitted_at` bằng `event_time` (`events.py:30`).
  - Late delay ràng buộc (late_within + W < allowed <= late_after) do validator T10 kiểm (`business_validator.py:923`), `build_dataset` gọi ở `dataset.py:227`.

## 6. Test có expected độc lập không

Phần lớn độc lập:
- `test_fault_counts.py:118-129`: tự viết công thức floor và hash bằng `hashlib`/`json`, không gọi `fault_rank_key`.
- `test_statements_window_and_d.py:242-262`: `_independent_d` dùng số học UTC-7 và literal.
- `test_control_report.py:23-28`: expected là file fixture viết tay. `:92-132`: tự cộng từ canonical, trừ `missing_internal`.
- `test_config_normal_delay_file_window.py`: tính `worst_file_before_cutoff` bằng `timedelta` riêng.
- `test_fault_effects.py:70-86`: literal 10^12 cho biên MAX_AMOUNT.

Các điểm yếu nhỏ, đều là ghi chú:
- `test_statements_window_and_d.py:402-405`: assert `amount_minor + 1` không điều kiện cho `amount_mismatch`. Chỉ đúng khi dữ liệu mẫu không chạm MAX. Biên MAX được test riêng ở `test_fault_effects.py:70-86`, nên rủi ro thấp.
- `test_control_report.py:128-132` và `test_generator_faults.py` so báo cáo với `build_control_report_from_envelopes` (hàm của mã). Nhưng cùng test đã có phép so độc lập ở `:92-126`, nên không phụ thuộc hoàn toàn vào hàm đang test.
- `test_d_around_midnight_event_times` (`:274-288`) dùng `derive_business_date` từ `n06.domain`. Đây là hàm của T10, không phải hàm đang test.

## Phát hiện

### Chặn merge
Không có.

### Nên sửa

**N1. Tên thư mục và file đầu ra không thể cấu hình từ `config/` hay CLI.**
- Spec mục 4: "Tên path/object key là cấu hình." CLAUDE.md: "Không hardcode endpoint, credential, bucket, topic, ..."
- Mã: `src/n06/generator/layout.py:21-42` định nghĩa `DEFAULT_LAYOUT` (`"source"`, `"ground_truth"`, `"events"`, `"control_report.json"`, `statements_w{window:09d}.jsonl`, ...). `build_dataset(..., layout=DEFAULT_LAYOUT)` nhận `layout` (`dataset.py:223`). Nhưng `src/n06/cli/generate.py` gọi `build(generator_cfg, target_dir)` (khoảng dòng 137) không truyền `layout`, và `config/*.toml` không có khóa nào cho bố cục.
- Hệ quả: tên bố cục chỉ đổi được bằng cách sửa mã hoặc gọi trực tiếp `build_dataset`. Cần quyết định: nối `OutputLayout` vào config và CLI, hoặc ghi ADR chấp nhận mặc định cố định cho v1. Tên `control_report.json` và `manifests/` xuất hiện trong spec nên có thể coi là hợp đồng. Các tên còn lại thì chưa rõ.

**N2. ADR-0011 vẫn Proposed trong khi mã và ba test đã phụ thuộc vào nó.**
- `docs/adr/0011-normal-delay-va-cua-so-file.md:3` ghi "Trạng thái: Proposed". Mã `params.py:90-116` và các test ở mục 2 đã chuyển sang hành vi này. CLAUDE.md chỉ cho dựa vào ADR Accepted. Cần Accept ADR-0011 để T13 đóng, hoặc đảo lại.

**N3. `PROGRESS.md` lạc hậu so với trạng thái ADR.**
- `PROGRESS.md:69` ghi "ADR-0009 vẫn Proposed". Hiện ADR-0009 ở `docs/adr/0009-t13-manifest-delivery-duplicate.md:3` là Accepted.
- `PROGRESS.md:32` ghi `pytest tests/unit (235/235 PASS)`. Số này cần cập nhật theo lần chạy hiện tại.
- Đề xuất dòng cập nhật: T13 chuyển sang DONE sau khi ADR-0011 được Accept; ghi lại các lệnh đã chạy ở trên.

**N4. Chưa kiểm chứng ở quy mô lớn, README đã thừa nhận nhưng thiếu một điểm.**
- Spec mục 5.2: "Dùng external sort/disk-backed storage khi dữ liệu lớn, không `list()` toàn bộ 5 triệu record." Lần chạy lớn nhất là 12.000 giao dịch (README, mục "Tồn đọng").
- `dataset.py:283`: `accounts = {a["account_id"]: a for a in iter_jsonl_files(...)}` nạp toàn bộ accounts vào RAM, tỷ lệ với `account_count`. README chỉ liệt kê `_pending` (`dataset.py:133`) là tồn đọng bộ nhớ, chưa nhắc `accounts`. Điều kiện "không kiểm chứng được ở 5 triệu" chưa có số đo. Nên bổ sung vào README tồn đọng, và đo thật trước cổng M1 nếu cần.

### Ghi chú

- **G1.** Ba test của `test_config_normal_delay.py` bị đổi expected theo ADR-0011 (làm chặt). Số tham số parametrize `offset` có thêm `-3600`. Không nới. Nếu ADR-0011 bị từ chối thì phải hoàn tác cả mã và test.
- **G2.** `dataset.py:349-351`: `first = next(sorted_rows, None)` dùng như tác dụng phụ để buộc `main_pass` chạy hết trước khi `writer.flush()`, ghi `control_report.json` và manifest. Đúng vì `external_sort_jsonl` (`external_sort.py:62-79`) tiêu thụ hết đầu vào trước khi yield, nhưng ràng buộc này không có test riêng. Các test `test_control_report.py:92-132` và `test_manifest_schema.py:264-283` sẽ bắt lỗi nếu hành vi đổi.
- **G3.** `src/n06/ingestion/statements.py:25` có `from n06.generator.determinism import canonical_json`. Đây là hàm primitive, không đọc ground truth. Tuy vậy nó kéo cả `n06/generator/__init__.py` (dataset, scenario) vào pipeline. Thuộc phạm vi T23, không phải T13.
- **G4.** Khi `build_dataset` lỗi giữa chừng, thư mục đầu ra để lại file dở và lần chạy sau bị từ chối vì thư mục không trống (`dataset.py:241-246`). Chưa có dọn dẹp tự động.
- **G5.** `FORBIDDEN` trong `test_ground_truth_isolation.py:142-158` gồm token `"kind"` và `"fault"`. Hiện qua vì dữ liệu `source/` không chứa các chuỗi này. Test sẽ vỡ nếu một trường hợp lệ sau này có tên chứa các chuỗi đó.
- **G6.** `ledger_entries.jsonl` có trong fixture và manifest mẫu của hợp đồng (`fixtures/acceptance/source_manifest.json:22`) nhưng generator không sinh file này vào `source/`. Test khẳng định không có `ledger_entries*` dưới `source/` (`test_ground_truth_isolation.py:208-209`) và ADR-0009 quyết định 1 coi nó là tập tham chiếu kiểm toán, không thuộc manifest. Khớp ADR, nên chỉ ghi nhận.
