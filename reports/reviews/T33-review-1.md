# Review T33 vòng 1

- Agent: spec-reviewer (agentId a3d149c4befedb640), phiên điều phối của Claude Code
- Thời điểm: 2026-11-27 (báo cáo nhận sau commit 197f932)
- SHA-256 của phần nguyên văn bên dưới (từ dòng `BÁO CÁO REVIEW T33` đến hết): 3c9c2664b6d039f44828d8eaf0400164d23474b6c4a90d958e46451cccef6d53

---

BÁO CÁO REVIEW T33 (nhánh task/T33-reconciliation, commit 197f932, so với 33a5ed4)

Phạm vi review: chỉ đọc mã. Tôi không chạy docker, Spark hay pytest tích hợp. Máy này không có pyspark nên không thể tự kiểm các điểm Spark ở mục 8.

## Kết quả lệnh đã chạy (trong worktree, .venv/bin)

- `ruff check .` cho "All checks passed!". `ruff format --check .` cho "143 files already formatted".
- `python scripts/check_rules.py` cho "check_rules: OK".
- `pytest tests/unit/reconciliation -q -o addopts= -p no:cacheprovider` cho "20 passed in 0.12s". Tôi chạy thêm `tests/unit/config` cho "30 passed" (không hỏng loader).
- `verify_contract.py` cho "14 check groups passed" (14/14 PASS). `git status` sau đó sạch.

## 1. Điều kiện hoàn thành

1. Từng khóa của ba snapshot khớp expected_results.json và oracle T14 khi tính lại từ đầu: **không kiểm chứng được** vì phụ thuộc kết quả chạy Spark thật. Về thiết kế thì test đủ để kiểm.
   - `tests/integration/test_recon_spark.py:339-395` so `expected_status_by_reference`, `expected_counts` và 13 khóa cho cả ba snapshot.
   - Cùng test tính anchor và tuổi độc lập từ `transactions.jsonl`, `statements.jsonl`, `statement_deliveries.jsonl` (dòng 368-381).
   - Test gọi `compare_against_oracle` (dòng 383-387).
   - Hai chỗ yếu, xem phát hiện N3 và N4.
2. Thứ tự ưu tiên đúng bảng 7.2 và đủ issue_codes: **đạt về thiết kế**.
   - Thứ tự `when()` ở `reconcile.py:281-290` là DUPLICATE, CURRENCY, AMOUNT, STATUS, comparable thì MATCHED, `~overdue` thì PENDING, `n_int==1` thì MISSING_EXTERNAL, còn lại MISSING_INTERNAL. Khớp D5.
   - `issue_codes` đánh giá độc lập hàng 2-4 (dòng 273-275, 195-198).
   - Test `multi-issue` (dòng 625-640 trong file test, hàm `test_priority_and_issue_codes_for_multi_issue_keys`) kiểm `["CURRENCY_MISMATCH","AMOUNT_MISMATCH","STATUS_MISMATCH"]`.
   - Chưa chạy Spark nên chưa chứng thực.
3. Đúng cutoff là quá hạn: **đạt về thiết kế**.
   - `reconcile.py:280` dùng `overdue = as_of >= cutoff_time`.
   - Công thức cutoff ở `reconcile.py:98-104`: `to_utc_timestamp(đầu ngày kế tiếp, business_timezone)` cộng `allowed_lateness_seconds`. Tôi suy luận lại với session UTC thì đúng (00:00 ngày 02/01 giờ VN trừ 7 giờ ra 17:00Z ngày 01/01).
   - Test `test_exact_cutoff_is_overdue` đọc `cutoff_cases.json`. Test `test_cutoff_follows_configured_lateness_and_timezone` kiểm biên 16:59:59/17:00:00 và biên New_York.
   - Kỳ vọng độc lập (từ file tham chiếu và số học trong test).
4. `discrepancy_age_seconds` đúng 7.3, MATCHED để rỗng, chạy lại không đổi: **đạt về thiết kế, một ngoại lệ ở N5**.
   - `anchor_event_time` là `coalesce(t_event, s_event)` ở `reconcile.py:262`. `t_event` là event_time nhỏ nhất của giao dịch, `s_event` là nhỏ nhất của sao kê đã có mặt và đã dedup.
   - Tuổi bị kẹp bằng `greatest(raw_age, 0)` (dòng 302) và có đếm `age_clamped`.
   - MATCHED cho NULL ở dòng 298 và 302.
   - Test kiểm REF-T006 early bằng 97194 (dòng 398-404 file test), tuổi âm bị kẹp ở N1, và `first == second`.
   - "Chạy lại" chỉ so hai lần gọi trong cùng một phiên Spark. Đủ cho P0.
5. Group theo match key và kiểm cardinality trước join, không many-to-many: **đạt về thiết kế**.
   - `_internal_side` (dòng 107-121) và `_statement_side` (dòng 124-167) mỗi bên group về một dòng cho mỗi khóa, kèm `n_int`/`n_st`. Hai phép full outer join (dòng 233-237) vì vậy là 1:1.
   - Khóa có `n>1` thì `is_duplicate` và không đi qua so sánh (`comparable` ở dòng 247-248).
   - Có test: kịch bản `cardinality` (2 giao dịch × 3 sao kê cùng khóa) kiểm `len(rows)==2` và đếm 2/3.
   - Không có assert cardinality tường minh trong mã. Việc đảm bảo hoàn toàn nhờ cấu trúc nên chấp nhận được.
6. ruff, check_rules, verify_contract, unit PASS: **đạt** (output ở trên). Test Spark không skip: `test_recon_spark.py:306-307` dùng `assert docker is not None`, không có skip hay xfail (grep chỉ ra `# fmt: skip`). Việc chạy thật trong container Spark thì tôi **không kiểm chứng được**.

## 2. Thay đổi bị cấm / test bị nới

- Không có thay đổi nào trong `contracts/`, `tests/fixtures/`, `tests/harness/`, `src/n06/quality/` (`git diff --stat` các đường dẫn đó rỗng).
- Không có dòng nào bị xóa trong diff. Toàn bộ diff là 1623 dòng thêm.
- Không có test bị xóa, skip hay nới expected.

## 3-4. Hardcode, float, ground truth

- Không thấy hardcode endpoint, ngày, seed, quy mô hay partition trong `src/n06/reconciliation/`. Grep không có `Ho_Chi_Minh`, `86400`, `SIM_CHANNEL` hay `datetime.now` ngoài một dòng docstring `settings.py:25`.
- Múi giờ, lateness, statuses, channels đều qua `ReconciliationSettings.from_config` (`settings.py:149-156`) từ `AppConfig`. Đúng yêu cầu.
- Không có float cho tiền. `_require_columns` (`reconcile.py:75-80`) từ chối `amount_minor` không phải bigint.
- `src/` không import `tests/` và không đọc `expected_results.json`, ground truth hay đồng hồ hệ thống. `as_of_time` là tham số.

## 5. Ngoài phạm vi

- `first_seen_at` và `resolved_at` để NULL (`reconcile.py:339-340`). Đúng.
- `quality_counters` và `reconcile_with_diagnostics` là thống kê theo Q3 và Q6, không vượt phạm vi.
- Có sửa `src/n06/config.py` (thuộc C) và `config/default.toml`, `config/README.md`. Xem N6.

## 7. Độc lập của expected trong test

Tốt về nguyên tắc: không test nào gọi `n06.reconciliation` để tạo kỳ vọng. Kỳ vọng đến từ `expected_results.json`, `cutoff_cases.json`, `tests.harness.oracle` và số học trong test. Điểm yếu nằm ở độ phủ, xem N3 và N4.

## PHÁT HIỆN

### A. Phải sửa (chặn merge)

**P1 (có điều kiện). `reconcile.py:68`, `_EMPTY_STRINGS = F.array().cast("array<string>")` chạy ở cấp module khi import.**
- Với PySpark 3.5.8 (`infra/spark/Dockerfile:11`), `F.array()` gọi `get_active_spark_context()` hoặc assert `SparkContext._active_spark_context is not None`. Khi import chưa có SparkContext thì lỗi (RuntimeError hoặc AssertionError).
- `recon_spark_worker.py:34-40` import `n06.reconciliation.reconcile` ở đầu file, còn SparkSession chỉ tạo trong `main()` (dòng 192). Nếu phân tích đúng thì worker sập ngay khi import.
- Đây là suy luận từ mã nguồn PySpark, tôi không chạy được. Người điều phối cần đối chiếu với lần chạy Spark thật. Nếu lần chạy thật PASS thì phát hiện này không còn nghĩa.
- Dù lần chạy PASS, nên chuyển sang tạo trong hàm (ví dụ `F.array().cast(...)` ngay trong `reconcile_with_diagnostics`). Cách này còn giúp import được module khi chưa có phiên Spark (ví dụ `n06 --help`).

### B. Nên sửa

**N1. Null-safety của so sánh thuộc tính, `reconcile.py:273-275`.**
- `t.currency != s.currency`, `t.amount_minor != s.amount_minor` và `t.status != s.status` trả NULL khi một bên NULL.
- Khi đó `is_*_diff` là NULL. `F.when(NULL)` bị coi là sai, nên khóa rơi xuống nhánh `is_comparable` (dòng 286) và bị gán **MATCHED** dù thuộc tính thiếu.
- ADR-0004b khai báo các cột này không null, nên rủi ro thấp. Nhưng "đáp án sai vẫn PASS" là loại lỗi tai hại nhất của đối soát.
- Nên dùng `~F.col("t.currency").eqNullSafe(F.col("s.currency"))` hoặc coalesce thành cờ boolean. Thêm một test ca null.

**N2. Khóa chỉ có bản ghi quarantine bị loại, `reconcile.py:242-243` (`kept = ... (n_int>0)|(n_st>0)|has_conflict`) và test C5 ở `test_recon_spark.py:~735` (`set(rows) == {"C1","C2","C3","C4"}`).**
- ADR-0006 D2 bước 4: *"Kết quả có đúng một dòng cho mỗi khóa thuộc hợp tập khóa hai phía và `key_conflicts`."*
- D2 bước 5: *"Phía bị quarantine nhưng khôi phục được key vẫn tham gia qua `key_conflicts`"*.
- Spec 7.1: *"không để record bị loại khỏi Silver làm biến mất bằng chứng conflict"*.
- Mã loại các khóa có `key_conflicts` nhưng `conflict_code` không phải `EVENT_ID_CONFLICT`/`BUSINESS_KEY_CONFLICT` và không còn bản ghi hợp lệ nào (C5).
- README mục "Chưa làm" chỉ ghi trường hợp khóa-chỉ-trong-key_conflicts của nhóm *source conflict*, không nói rõ quyết định này cho quarantine-only.
- Đây là chỗ ADR mơ hồ (không định nghĩa trạng thái cho khóa không có phía nào). Theo CLAUDE.md, nên ghi tường minh vào README và đề xuất bổ sung ADR/Q8 thay vì tự chọn.

**N3. Điều kiện 1 chưa kiểm số tiền và currency từng khóa.**
- `compare_against_oracle` (`tests/harness/oracle.py:420-498`) chỉ so `recon_status`, `discrepancy_age_seconds` và `anchor_event_time`. Nó không so số tiền, currency hay `issue_codes`.
- `expected_results.json` chỉ có status và đếm theo trạng thái.
- Trong ba snapshot acceptance, các cột `internal_amount_minor`, `statement_amount_minor`, `*_currency`, `*_status` chỉ được assert với REF-T001 (dòng 453-456) và `issue_codes` ở một số khóa. Trạng thái đúng nhưng số tiền hai phía sai sẽ vẫn PASS.
- Spec II.6 nói *"phải so từng match key/status/amount/currency"*.
- Nên thêm assert per-key: tiền và currency hai phía lấy từ `transactions.jsonl` và `statements.jsonl` (ví dụ T003 300 so với 301, T004 VND so với USD).

**N4. Các ca chưa có test (chỗ mã dễ sai mà vẫn xanh):**
- Cột null (N1).
- Khóa MATCHED có dòng quarantine. Mã đang bỏ `COUNTERPART_QUARANTINED` khi MATCHED (`reconcile.py:203-205`). ADR-0005 nói issue_codes rỗng nếu MATCHED nên có lý, nhưng Q8 không nói rõ và chưa có test chốt.
- `EVENT_ID_CONFLICT` trên khóa có bản ghi hợp lệ. Khóa vừa có conflict nguồn vừa có quarantine.
- Cùng `statement_record_id` nhưng khác nội dung. Dedup `F.min(struct)` (dòng 134-155) âm thầm lấy bản `file_available_at` sớm nhất và che khác biệt. Spec chỉ yêu cầu loại bản sao, nên đây thuộc ghi chú, nhưng nên có test hoặc ghi chú trong README.
- `unit` trên host hiện chỉ có test hằng số (`test_recon_rules.py`) và settings. Mọi logic nghiệp vụ chỉ được test qua container Spark.

**N5. Khóa chỉ có trong key_conflicts (C4): anchor và tuổi NULL dù trạng thái khác MATCHED.**
- Spec 7.3: *"Với mọi khóa có `recon_status` khác MATCHED, lưu `anchor_event_time` và `discrepancy_age_seconds`"*.
- ADR-0005 mục 2.3: `discrepancy_age_seconds` là *"NULL khi MATCHED; >= 0 khi khác MATCHED"*.
- Mã (`reconcile.py:262`, `anchor_candidate` NULL) và test (`rows["C4"]["anchor_event_time"] is None`) cho NULL. README có ghi ở mục "Chưa làm" nhưng đây là lệch đặc tả có chủ ý, chưa có ADR. `key_conflicts` không có `event_time` nên không tính được; cần ADR hoặc bổ sung `event_time` vào `key_conflicts` (T31, ADR-0004b).

**N6. Sửa `src/n06/config.py` (+34 dòng).**
- File này thuộc C (`docs/TASKS.md:27`) và không nằm trong danh sách "Được tạo/sửa" của thẻ (thẻ chỉ nêu `config/` mục `[reconciliation]` và README). Theo quy tắc CODEOWNERS (`TASKS.md` mục 2), PR phải có C duyệt.
- **Cần thiết**: `validate_config_dict` từ chối khóa không có trong `FIELD_TYPES` (`config.py:547-554`), và `FIELD_TYPES`/`_require_typed`/`_coerce_env_value` chỉ hỗ trợ int, bool, str, date (`config.py:371-480`), không có list.
- **Tối thiểu hợp lý**: thêm 2 khóa str, `ReconciliationConfig`, `_require_csv_set` (từ chối rỗng và trùng), và một lần gọi. Thêm kiểu list vào loader sẽ lớn hơn.
- Hai lệch nhỏ. Thứ nhất, Q10 ghi mặc định dạng danh sách `["SUCCESS","FAILED"]`, còn mã dùng chuỗi `"SUCCESS,FAILED"` (có ghi trong `config/README.md` và `default.toml`). Thứ hai, test loader nằm ở `tests/unit/reconciliation/test_recon_settings.py` thay vì `tests/unit/config/`.
- Không có chỗ nào khác dựng `AppConfig(` ngoài `config.py:844`, nên không ảnh hưởng nơi khác.

### C. Ghi chú

**G1. Cột đầu ra.**
- `RESULT_COLUMNS` (`reconcile.py:38-65`) khớp thứ tự và kiểu của ADR-0005 mục 2.3 cộng `input_snapshot_id`. Cột này cần cho spec S361 nhưng **ADR-0005 (Accepted) chưa có**, nên DDL mart (T41) phải thêm.
- `internal_record_count` và `statement_record_count` là int (dòng 333-334). `discrepancy_age_seconds` là long. `date_key` là int. Test kiểm dtype.

**G2. Phần nhận xét về cách hiểu đặc tả, đều khớp văn bản.**
- Visible statements: `file_available_at <= as_of` (`reconcile.py:130`), đúng D3 và S787. Cột `file_available_at` do T25 cung cấp.
- Dedup theo `statement_record_id` trước khi group (`reconcile.py:132-155`), đúng S336 và D2 bước 1. Phép dedup làm sau bộ lọc visibility là đúng.
- Q3: `t_bd` (nhỏ nhất của giao dịch), rồi `s_bd`, rồi `kc_bd`. Mở rộng `kc_bd` không có trong Q3 nhưng vô hại.
- Q7: `F.min("event_time")` cho giao dịch.
- `F.min(F.struct(...))` (dòng 113, 161) xác định vì có `transaction_id` hoặc `statement_record_id` làm khóa phá hòa, và các cột còn lại chỉ dùng khi `n==1`.
- Xử lý key_conflicts: `EVENT_ID_CONFLICT` hoặc `BUSINESS_KEY_CONFLICT` cho `DUPLICATE_OR_CONFLICT` (dòng 178-181), mã khác chỉ thêm `COUNTERPART_QUARANTINED`. Khớp lời dặn của task và Q8.
- Mã nguồn conflict (`BUSINESS_KEY_CONFLICT`, `EVENT_ID_CONFLICT`) cũng được đưa vào `issue_codes` (`rules.py:51-55`). Q1 nói *"giữ đúng tập này ... thêm mã dẫn xuất từ quality khi có (Q8)"*, chỉ nêu `COUNTERPART_QUARANTINED`. Đây là mở rộng nhỏ, có test, không mâu thuẫn rõ. Nên ghi vào ADR khi có dịp.

**G3. Giả định session timezone.**
- Cutoff phụ thuộc `spark.sql.session.timeZone` (`to_timestamp` đọc chuỗi theo múi giờ phiên). `_require_utc_session` (dòng 89-95) chặn trường hợp khác và có test (`non_utc_session`). Chấp nhận được.
- `to_utc_timestamp` xử lý đúng DST theo IANA.

**G4. Generator chưa đọc mục `[reconciliation]`.** Spec 7.2 yêu cầu *"áp dụng nhất quán cho generator và pipeline"*. README config có ghi "Generator chưa đọc mục này". Cần task riêng, không thuộc T33.

**G5. Hạn chế có thể chấp nhận.**
- Test mô tả `first == second` chỉ so hai lần gọi trong cùng một phiên Spark, không phải hai tiến trình độc lập.
- Số lần chạy Spark gói một container cho mọi kịch bản. Một kịch bản lỗi (ví dụ P1) làm hỏng toàn bộ test.

## Tệp liên quan (đường dẫn tuyệt đối)

- `/home/quybuno/banking/.claude/worktrees/agent-a1ed227bade4c4be0/src/n06/reconciliation/reconcile.py`
- `/home/quybuno/banking/.claude/worktrees/agent-a1ed227bade4c4be0/src/n06/reconciliation/rules.py`
- `/home/quybuno/banking/.claude/worktrees/agent-a1ed227bade4c4be0/src/n06/reconciliation/settings.py`
- `/home/quybuno/banking/.claude/worktrees/agent-a1ed227bade4c4be0/src/n06/config.py`
- `/home/quybuno/banking/.claude/worktrees/agent-a1ed227bade4c4be0/tests/integration/test_recon_spark.py`
- `/home/quybuno/banking/.claude/worktrees/agent-a1ed227bade4c4be0/tests/integration/recon_spark_worker.py`
- `/home/quybuno/banking/.claude/worktrees/agent-a1ed227bade4c4be0/tests/unit/reconciliation/`
