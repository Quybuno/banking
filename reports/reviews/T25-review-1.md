# Review T25 vòng 1 (spec-reviewer, nguyên văn)

Nhánh `task/T25-process-day`, commit `edcaac9`, so với `f2d33b8`. Prompt tự chứa gửi cho reviewer nêu thẻ T25, điều kiện đạt, phạm vi được sửa, tài liệu cần đọc và tám điểm kiểm (a đến h). Phần dưới đây là báo cáo của reviewer, giữ nguyên văn.

---

REVIEW T25 (nhánh task/T25-process-day, commit edcaac9, so với f2d33b8). Chỉ đọc, không chạy test tích hợp hay spark-submit.

Lệnh đã chạy (output thật)
- `.venv/bin/pytest tests/unit/spark_jobs -o addopts="" -q` -> `38 passed in 1.23s`
- `.venv/bin/ruff check .` -> `All checks passed!`; `ruff format --check .` -> `147 files already formatted`
- `.venv/bin/python scripts/check_rules.py` -> `check_rules: OK`
- `.venv/bin/python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py` -> 14 dòng PASS, kết thúc `14 check groups passed.`
- `git diff f2d33b8..HEAD --stat -- config docs contracts tests/fixtures` -> rỗng. Không đổi contracts/, tests/fixtures/, config/ hay docs/.

## Phải sửa

1. Lời hứa "không dữ liệu nào bị bỏ im lặng" (spark_jobs/README.md, mục "Chưa làm") không đúng. Job chỉ dừng với một tập con lỗi.
   - Bronze `parse_error` không được đọc, đếm hay làm job dừng. `grep -n parse_error src/n06/spark_jobs/*.py` không có kết quả. `process_day_job.py:282-303` chỉ đọc entity `transaction` và `ledger_entry`. ADR-0004 mục 1 và G1 nói bản ghi hỏng schema chỉ nằm ở `parse_error`. Một dataset có bản hỏng vẫn ra run SUCCEEDED với số bản ghi thiếu.
   - Không có kiểm tra lệch envelope và payload. Bronze có `envelope_dataset_id` và `envelope_event_time` (ADR-0004 mục 3). Silver không so chúng với `dataset_id` và `event_time` của payload (`ENVELOPE_PAYLOAD_MISMATCH`, spec S891). Ngoài ra `transforms.dedup_event_bronze` lọc theo `dataset_id` của payload (`transforms.py:138`).
   - Cách sửa: đếm các thư mục `parse_error` của các batch và dừng với mã có tên (vd `PARSE_ERROR_PRESENT`), thêm kiểm lệch envelope và payload. Nếu không làm trong T25 thì README phải ghi đúng phạm vi đã kiểm, không được khẳng định "không bỏ im lặng".

2. `EVENT_ID_CONFLICT` bị lọt khi cùng `event_id` mang khóa nghiệp vụ khác.
   - `transforms.py:139` tính `payload_hash = sha2(_text(payload))`. `TRANSACTION_PAYLOAD` (`transforms.py:32-43`) không chứa `transaction_id`. `LEDGER_PAYLOAD` (`transforms.py:44-52`) không chứa `entry_id`.
   - `event_id_conflicts` (`transforms.py:155-161`) đếm `countDistinct(payload_hash)` theo `event_id`. Hai bản ghi cùng `event_id`, khác `transaction_id`, các trường còn lại giống nhau cho `variants = 1`. Không lỗi nào được báo. Silver có hai bản ghi nghiệp vụ dùng chung một `event_id`.
   - Đặc tả (ADR-0004b, trích spec I.6): *"Cùng event_id + cùng payload: giữ một business event nhưng lưu lineage các lần nhận. Khác payload: quarantine conflict, không chọn ngẫu nhiên."* Khóa nghiệp vụ là một phần của payload.
   - Cách sửa: băm cả khóa nghiệp vụ khi tìm xung đột `event_id`.

3. Lõi "dedup cơ bản" và lọc theo `as_of_time` không có test nào.
   - Dataset test dùng `generator.demo-clean.json` có `event_replay_basis_points: 0`, `business_replay_basis_points: 0`, `statement_duplicate_basis_points: 0` (dòng 34-38 của file cấu hình). `replay_count` luôn bằng 1, `event_ids` luôn có một phần tử. Nhánh gộp bản trùng chưa chạy lần nào trong test.
   - Không có `test_transforms.py`. Không có test cho: dedup sao kê theo `statement_record_id`, lọc `file_available_at <= as_of_time` (dòng `transforms.py:450`), khóa chưa phân loại (`unclassified_keys`), cờ `--business-date`, `_statement_integrity`, và bất kỳ mã `DataError` nào (BUSINESS_KEY_CONFLICT, STATEMENT_FILE_MISMATCH...).
   - Tối thiểu cần một test Spark (container hoặc local) với dữ liệu replay và xung đột nhỏ, hoặc README ghi rõ các nhánh này chưa test.

## Nên sửa

4. `process_day_job.py:370-373` có comment "Đối soát cần toàn bộ giao dịch và sao kê của dataset, không chỉ ngày đang lọc", nhưng truyền `silver["transactions"]` và `silver["statements"]` đã qua `date_filter` (`process_day_job.py:321, 347`). Với `--business-date`, đối soát chạy trên tập đã lọc. Khóa có sao kê khác ngày thành "chưa phân loại" sai. Comment và mã mâu thuẫn. Nên đối soát trên bản chưa lọc rồi mới lọc đầu ra, hoặc sửa comment và README.

5. Silver sao kê chứa cả file chưa nhìn thấy tại `as_of_time`.
   - `silver_statements` và `finish_silver_statements` xử lý mọi biên nhận (`process_day_job.py:325-347`). Việc lọc chỉ nằm ở `reconcile_matched` (`transforms.py:450`). ADR-0006 D3 chỉ cho file có biên nhận và `file_available_at <= as_of_time` làm đầu vào.
   - Dedup lấy `min(delivery_order)` (`transforms.py:307-316`) rồi mới lọc. Nếu `file_available_at` không tăng theo `delivery_order` thì bản sao đến sớm hơn bị bỏ.
   - File chưa tới hạn cũng bị kiểm sha256 và tính toàn vẹn, nên có thể làm job dừng.

6. Test độc lập nhưng bao phủ thiếu (`test_process_day_clean.py`):
   - Gold `fact_transactions` chỉ so số dòng (dòng 419). Tổng `amount_minor` theo ngày và currency của Gold không được so với control report. Cột kiểu "columns" (dòng 90-91) chỉ để lấy số dòng, không kiểm tên cột hay kiểu so với ADR-0005 2.1 và 2.3.
   - Silver `statements` và `accounts` không được so với nguồn.
   - Test "run_id đã có object" (dòng 469-491) chỉ đặt object vào gốc `silver/transactions`. 5 gốc còn lại chỉ được kiểm là rỗng sau lỗi. Test unit `test_plan` bù một phần bằng liệt kê giả.

7. `row_count` và `semantic_hash` của snapshot.
   - `process_day_job.py:415` truyền `row_count=total_rows` cho mọi snapshot. `total_rows` gồm cả `accounts` nhân bản theo từng ngày (`process_day_job.py:391-394`), nên số này phồng lên.
   - Cả 6 entity có cùng `semantic_hash` (hash cả run). T26 hoặc T41 không thể dùng hash để biết riêng một entity có đổi hay không. Đây là hệ quả của API T24, đã được tác giả ghi. Cần ghi vào ADR hoặc README T26/T41.

8. `_statement_integrity` báo sai với file sao kê rỗng.
   - `process_day_job.py:174-176`: file không có dòng nào không xuất hiện trong `seen`, nên `sha = None`, khác `r.sha256`, và job báo `STATEMENT_FILE_MISMATCH`. Biên nhận `physical_rows = 0` sẽ làm job dừng oan.
   - Job chỉ so với biên nhận, manifest chỉ được kiểm tồn tại (`process_day_job.py:157-161`). ADR-0002 mục 4 (dòng 116) ghi "kiểm sha256/physical_rows theo manifest".

9. `accounts.jsonl`.
   - `ingestion/statements.py` (phần diff thêm) tải file mà không kiểm `sha256` theo `source_manifest.json`, trong khi `source_manifest.json` có liệt kê `accounts.jsonl`. File sao kê thì có `expected_sha256`. Job process-day cũng không có biên nhận hay hash của accounts để kiểm.
   - Lệch ADR-0004b 3.4 so với `account.schema.json`: ADR tự mâu thuẫn. Nó nói "theo account.schema.json" nhưng liệt kê `account_code`, `opening_date`, `status`, là các cột hợp đồng không có (đã xác nhận từ `account.schema.json`). CLAUDE.md yêu cầu dừng và đề xuất ADR. Tác giả chọn theo hợp đồng và ghi trong README nhưng không đề xuất ADR. ADR-0005 mục 1 yêu cầu `dim_account.account_code`, `opening_date`, `status` NOT NULL. Các cột đó không thể sinh từ Silver này. Cần một ADR chỉnh 0004b/0005 trước T26 và T41.

10. Quy mô lớn.
   - `binaryFile` đọc nguyên file thành một dòng rồi `split` (`transforms.py:219-232`, và `process_day_job.py:350` cho accounts). Một file sao kê hay accounts lớn dồn vào một task và bị chặn bởi giới hạn 2 GB của `binaryFile`.
   - `frame.coalesce(cores)` (`process_day_job.py:100`) giảm song song hóa của cả giai đoạn trước nó ở dữ liệu lớn.
   - Không có `unpersist` nào (grep rỗng).
   - `source` (Bronze) không persist nên bị quét lại nhiều lần: `event_id_conflicts`, `source.count()`, `grouped`.
   - Collect lên driver chỉ gồm: nội dung biên nhận (nhỏ), `groupBy(path)`, `groupBy(business_date).count`, một dòng digest. Không dùng Python UDF.

11. Tác giả sửa `src/n06/cli/statements.py` (+1 dòng `accounts_status`), ngoài danh sách tệp được sửa, nhưng nhỏ và cần cho `accounts_status`. `--run-id` và `--fail-at` là cổng kiểm thử nằm trong đường chạy sản xuất (`process_day_job.py:488-491`, `cli/__init__.py` dùng `SUPPRESS`). Chấp nhận được nếu ghi rõ.

12. `plan.CODE_VERSION = "process-day-1"` (`plan.py:33`) và `RECON_RULE_VERSION` là hằng đặt tay. Đổi logic mà quên tăng thì hash không đổi. Chỉ nên ghi chú.

## Đã kiểm và đạt

- Điều kiện (2), thứ tự gọi: `start_run` (`process_day_job.py:212`) -> `claim_run_prefixes` với `s3_listing.make_list_objects(spark)` (`process_day_job.py:234-237`) -> mới ghi. Trước đó chỉ đọc. `plan.py:76-83` kiểm cả 6 gốc `run_id=` qua `require_unused_run_prefix`. `s3_listing.py:138-149` dùng `FileSystem.listFiles(path, True)` của S3A thật, trả generator lười nên `next(iter())` ở `commit.py` hợp lệ. Lỗi thì `fail_run` rồi mã `COMMIT_ERROR` (`process_day_job.py:514-520`).
- Điều kiện (3), đường dẫn và hash: mọi đường dẫn đi qua `plan.output_path` -> `build_run_output_path` (`plan.py:55-61`). Ghi `errorifexists` rồi đọc lại đối chiếu số dòng (`process_day_job.py:100-106`). Commit chỉ sau khi mọi output đã ghi (`process_day_job.py:406-418`). Hash tính từ digest nội dung đã loại `run_id`, không thứ tự dòng (`transforms.py:507-520`). `input_versions` khi tính hash bỏ đường dẫn (`process_day_job.py:459-461`). Thân test kiểm hash không đổi giữa hai run (`test_process_day_clean.py:436-441`). Chưa chạy được.
- Run lỗi không bị đọc: `after_write` -> `fail_run`, `current_snapshot` chưa bị chạm (`process_day_job.py:396-397`). `RunStateError` và `CommitError` cùng cây (`control_store.py:21-29`), nên `except CommitError: pass` (`process_day_job.py:255`) không che lỗi gốc.
- Cấm: không hardcode bucket, endpoint, topic, ngày, seed hay đường dẫn tuyệt đối trong `spark_jobs/` và `cli/process_day.py`. Grep chỉ thấy `LAYER_SILVER = "silver"`. Không float cho tiền. Hằng miền `MAX_AMOUNT_MINOR = 1_000_000_000_000` và `MAX_OPENING_BALANCE_MINOR = 9_000_000_000_000_000` khớp `transaction.schema.json` và `account.schema.json`.
- Đồng hồ: mã Spark không đọc đồng hồ (grep `now|utcnow|current_timestamp|time.time` rỗng). `as_of_time` là tham số bắt buộc của job (`process_day_job.py:485`). Đồng hồ chỉ được đọc một lần ở host (`cli/process_day.py:35-40`) rồi truyền tường minh. `build_spark` đặt `session.timeZone=UTC`, nên `to_date(from_utc_timestamp(...))` ở `business_date_mismatch` đúng.
- Ground truth và glob: không import `tests`, không đọc `expected_results`, `statement_deliveries`, `control_report`. Bronze chỉ từ `ingest_batches` COMMITTED (`process_day_job.py:197`). Sao kê chỉ từ liệt kê `_ingest/` (`s3_listing.list_directory`). Test đọc `control_report.json` và `events` từ thư mục source của test, phù hợp với `tests/`.
- Cột Silver `transactions`, `ledger_entries`, `statements` khớp ADR-0004b 3.1-3.3. Gold `fact_transactions` khớp ADR-0005 2.1. `fact_reconciliation` khớp 2.3: `issue_codes` rỗng, `anchor_event_time` và `discrepancy_age_seconds` NULL, `first_seen_at` và `resolved_at` NULL.
- Đối soát theo ADR-0006: khóa là `(dataset_id, channel, external_reference)`. Mỗi phía phải có đúng 1 bản ghi độc lập (D2). Lọc sao kê theo `file_available_at <= as_of` (D3). Giao dịch không bị lọc theo thời gian (Q4). `business_date` lấy `min` ngày giao dịch (Q3).
- Test: expected độc lập với mã đang kiểm. Tổng và đếm theo `(business_date, currency, status_or_side)` lấy từ `control_report.json` (`test_process_day_clean.py:384-397`). Khóa kỳ vọng từ byte `events` nguồn (dòng 403-412). Liệt kê S3 bằng SigV4 viết riêng (dòng 170-203). Đường dẫn dựng bằng chuỗi (dòng 349-354). Không gọi `plan.*` hay hàm đang test.
- `tests/integration/test_ingest_statements_minio.py` sửa `3 * n` thành `3 * n + 1` đúng ADR-0004b Q2. Đây là chỉnh expected có lý do nhưng thuộc loại cần người duyệt xác nhận.

## Chưa kiểm được

- Toàn bộ test tích hợp (Kafka/MinIO/Postgres/Spark): điều kiện (1), phần kiểm run_id đã có object, rerun cùng hash, `current_snapshot` trỏ đúng run. Mã và test có, nhưng chưa có output chạy.
- `test_ingest_statements_minio.py` có thật sự đạt `3n+1`, tức nguồn của test đó có `accounts.jsonl` hay không.
- Hành vi `from_json` của Spark đã chọn: số trong ngoặc kép ("100") có bị ép thành số nguyên hay không, trường lạ (hợp đồng `statement.schema.json` đặt `additionalProperties:false`, dòng 5) có bị bỏ qua. Nếu có, bản ghi sao kê hoặc accounts sai hợp đồng sẽ lọt qua mà không báo lỗi (`transforms.py:274-301`, `355-371`).
- `binaryFile` trả `path` có đúng bằng chuỗi `uris[...]` dùng để join và đối chiếu sha hay không (`process_day_job.py:171-174`, `transforms.py:259-272`). Chưa kiểm vì chưa chạy.
- `listFiles` của S3A bỏ qua dấu thư mục 0 byte. Một gốc `run_id` chỉ có marker sẽ không bị `require_unused_run_prefix` phát hiện.
- Semantic hash giữa các phiên bản Spark khác nhau (định dạng `to_json` của timestamp và mảng). Test chỉ so hai run cùng image.
- Khoảng thời gian bị chết giữa các lần `commit_run` (phần snapshot trỏ run mới, phần trỏ run cũ). Tác giả đã ghi trong README. Không có test tiêm lỗi (thuộc P2).

---

# Xử lý của tác giả (không thuộc báo cáo của reviewer)

Lưu ý: kết quả tích hợp ở các mục "Chưa kiểm được" đã có sau đó (xem báo cáo T25 và PR): bốn test của `test_process_day_clean.py` đạt trên hạ tầng thật ở commit `edcaac9`, trước các sửa dưới đây.

| Mục | Xử lý |
|---|---|
| Phải sửa 1 | Đã sửa: job đọc `entity=parse_error` và dừng với `PARSE_ERROR_PRESENT` khi có bản ghi của dataset hoặc không rõ dataset; thêm `ENVELOPE_PAYLOAD_MISMATCH` (`transforms.envelope_mismatch`); dedup lấy cả bản ghi mà envelope hoặc payload nêu dataset để bản lệch lộ ra thay vì bị lọc mất. |
| Phải sửa 2 | Đã sửa: `event_hash` băm cả khóa nghiệp vụ; `event_id_conflicts` dùng cột này. |
| Phải sửa 3 | Đã sửa: `tests/integration/test_process_day_transforms.py` chạy trong container Spark với dữ liệu dựng tay (replay, xung đột khóa và `event_id`, lệch envelope, lệch ngày, loại trùng sao kê, sao kê chưa nhìn thấy, khóa chưa phân loại, file rỗng, dòng sao kê hỏng, hash nội dung). `--business-date` có test trong `test_process_day_clean.py`. Các mã `DataError` ở tầng job (`STATEMENT_FILE_MISMATCH`, `RECEIPT_*`) chưa có test tích hợp riêng: ghi vào tồn đọng. |
| Nên sửa 4 | Đã sửa: đối soát chạy trên bản chưa lọc ngày, `--business-date` chỉ lọc đầu ra; có test. |
| Nên sửa 5 | Không đổi, ghi tồn đọng: Silver sao kê chứa mọi file có biên nhận (cách hiểu ADR-0006 D3: đối soát nhận cột `file_available_at` và lọc ở đầu hàm). `delivery_order` tăng theo `(file_available_at, channel)` (ADR-0009) nên dedup chọn `min(delivery_order)` cũng là bản đến sớm nhất. Cần người duyệt xác nhận. |
| Nên sửa 6 | Đã sửa: Gold `fact_transactions` so tổng với control_report, cột Gold so với ADR-0005; Silver statements và accounts so với byte nguồn; kiểm run_id đã dùng với hai gốc (`silver/transactions`, `gold/fact_reconciliation`). |
| Nên sửa 7 | Ghi vào `src/n06/spark_jobs/README.md` (một hash cho cả run; `row_count` của snapshot là tổng cả run). |
| Nên sửa 8 | Đã sửa phần file rỗng (`file_stats` tính sha và số dòng từ file, có test). So với manifest: chưa làm, so với biên nhận (biên nhận do `n06 ingest-statements` ghi từ manifest đã kiểm sha); ghi tồn đọng. |
| Nên sửa 9 | Ghi tồn đọng và đề xuất ADR chỉnh ADR-0004b 3.4 và ADR-0005 mục 1 (cột `account_code`, `status`, `opening_date`). Không tự chọn thêm. Kiểm sha256 `accounts.jsonl` theo `source_manifest.json`: chưa làm, ghi tồn đọng. |
| Nên sửa 10 | Một phần: có `unpersist` ở cuối run và persist `source` Bronze. Giới hạn `binaryFile` 2 GB và `coalesce` ghi vào README là tồn đọng cho quy mô lớn. |
| Nên sửa 11, 12 | Ghi README (cờ kiểm thử `--run-id`, `--fail-at` ẩn; hằng phiên bản đặt tay). |
