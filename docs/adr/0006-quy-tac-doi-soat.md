# ADR-0006: Quy tắc đối soát, cutoff, `anchor_event_time` và `discrepancy_age_seconds`

- Trạng thái: Accepted
- Người viết: B (soạn bởi agent theo `docs/TASKS.md` mục 3, ADR-006), chờ người duyệt
- Người duyệt: A, C
- Ngày: 2026-11-27
- Đã duyệt (Accepted): chủ repo (ủy quyền giám sát), 2026-11-27
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 7.1–7.3 (S329–S364); Phần II mục 1 (S675–S692, riêng 1.4, 1.5, 1.9, 1.10, 1.11), mục 6 (S810–S857), mục 7 (S859–S870), mục 8 (S898); kiểm thử S567–S571, S577.

Ký hiệu `S<dòng>` là số dòng trong `docs/spec.md` bản 1.2 (đếm từ dòng 1 của tệp). Dùng ADR này cho T33 (đối soát), T34 (nghiệm thu ba snapshot) và T61 (`first_seen_at`, `resolved_at`). T14 (oracle, `tests/harness/oracle.py`) đã DONE và độc lập với pipeline; ADR này ghi chỗ pipeline phải khớp oracle.

## Bối cảnh

Đặc tả chốt phần lớn quy tắc đối soát nhưng rải ở ba nơi: Phần I mục 7, Phần II mục 1.9–1.11 và mục 6. T33 cần một văn bản duy nhất đủ cụ thể để hiện thực mà không đoán. ADR này (1) gom các quy tắc đặc tả đã chốt thành quyết định, mỗi quy tắc kèm số dòng; (2) kiểm chúng trên fixture `contracts/N06_INPUT_CONTRACT_V1/fixtures/acceptance/` (chỉ đọc); (3) ghi các chỗ đặc tả im lặng hoặc mơ hồ vào mục "Câu hỏi mở" kèm đề xuất, **không tự chọn**. Phạm vi dự án là P0 + P1 đầy đủ nên phần P1 (`first_seen_at`, `resolved_at`, late correction) nằm trong ADR; lịch sử đối soát (P2, S363) nằm ngoài.

Kiểm bằng chạy: một script dùng một lần (không đưa vào repo; chỉ đọc `transactions.jsonl`, `statements.jsonl`, `statement_deliveries.jsonl`, `cutoff_cases.json`, `expected_results.json`) áp dụng đúng các quy tắc ở mục "Quyết định" và cho kết quả **trùng 39/39** trạng thái (13 khóa × 3 snapshot), trùng ba dòng của `cutoff_cases.json`, và cho `discrepancy_age_seconds` của `REF-T006` ở snapshot early bằng **97.194** như S857. Bảng kết quả ở mục "Kiểm chứng trên fixture".

## Quyết định

### D1. Đầu vào và khóa (S333, S685)

- Đầu vào là Silver hợp lệ của giao dịch (phía nội bộ) và của sao kê (phía ngoài), cộng chỉ mục `key_conflicts` từ bước quality (S333, S334, S898).
- Match key là `(dataset_id, channel, external_reference)`, so sánh bằng chuỗi đúng nguyên văn. Không thêm `currency` hay ngày vào khóa (S685); thêm currency sẽ che mất `CURRENCY_MISMATCH`.
- Mỗi match key có đúng **một** kết quả hiện tại (S337, S363). Lưu `transaction_id` nếu có và danh sách `statement_record_id` bằng child table, không ép chọn một ID (S337).
- Silver đã loại replay (cùng `event_id`, hoặc cùng payload nghiệp vụ khác `event_id`); phần đó thuộc T31, không thuộc ADR này. Fixture: `REF-T010` và `REF-T011` tới đối soát chỉ còn một bản (S827–S828).

### D2. Loại bản sao và kiểm cardinality trước khi join (S336)

Thứ tự cố định, mỗi bước chạy trước bước sau:

1. Loại bản sao sao kê cùng `statement_record_id` (S336, S568).
2. Group từng phía theo match key; đếm số bản ghi **độc lập** của phía đó (giao dịch: `transaction_id` khác nhau; sao kê: `statement_record_id` khác nhau).
3. Một khóa có hơn một bản ghi độc lập ở **bất kỳ** phía nào, hoặc có mặt trong `key_conflicts`, được gắn `DUPLICATE_OR_CONFLICT` (hàng 1 của bảng, S343) và **không** đi qua phép so thuộc tính. Đây là cách duy nhất để không có many-to-many nhân dòng (S336).
4. Sau bước 3, mỗi phía còn tối đa một dòng cho mỗi khóa thì mới full outer join theo match key (S333). Kết quả có đúng một dòng cho mỗi khóa thuộc hợp tập khóa hai phía và `key_conflicts`.
5. Phía bị quarantine nhưng khôi phục được key vẫn tham gia qua `key_conflicts`; nếu không khôi phục được key thì chỉ thống kê quarantine, **không bịa key** (S334, S898).

Fixture: `REF-T009` có hai `statement_record_id` khác nhau cho cùng khóa nên là `DUPLICATE_OR_CONFLICT` ở cả ba snapshot (S826, S853).

### D3. Bản ghi sao kê nhìn thấy được tại `as_of_time` (S787, S861–S868)

Một file sao kê (và mọi dòng trong file) có mặt trong đầu vào của một lần đối soát khi và chỉ khi biên nhận `_ingest/<delivery_order:09d>.json` do `n06 ingest-statements` (T23, `src/n06/ingestion/statements.py`) ghi trên MinIO tồn tại và có `file_available_at <= as_of_time`. Dùng `file_available_at` **trong biên nhận đó** (S868: *"Production-like job chỉ đọc file đã ingest theo source manifest"*), không dùng `available_at` của từng dòng thay cho `file_available_at`. Pipeline **không** đọc `statement_deliveries.jsonl`, `statements.jsonl` tham chiếu hay ground truth (S868, S870). Hàm đối soát nhận DataFrame sao kê đã gắn sẵn cột `file_available_at` lấy từ biên nhận và lọc `file_available_at <= as_of_time` ở đầu hàm (tham số `as_of_time`, không đọc đồng hồ). Phía giao dịch: xem Q4.

### D4. Cutoff và quá hạn (S358, S686)

- `cutoff = đầu ngày kế tiếp của business_date theo Asia/Ho_Chi_Minh (UTC+7, không DST) + allowed_lateness_seconds` (mặc định 86.400; S686).
- Quá hạn khi `as_of_time >= cutoff`; chưa quá hạn khi `as_of_time < cutoff`. **Tại đúng cutoff là quá hạn** (S358, S686).
- `as_of_time` là tham số truyền vào, UTC dạng `...Z`; mã đối soát không gọi đồng hồ hệ thống (S358; README của `src/n06/reconciliation/`).
- Ví dụ khớp `cutoff_cases.json`: ngày 2026-01-01 có cutoff `2026-01-02T17:00:00Z` (S845). `REF-T006` ở `16:59:59Z` là `PENDING`, ở `17:00:00Z` và `17:00:01Z` là `MISSING_EXTERNAL`.
- Hạn mỗi khóa tính từ `business_date` của khóa, không từ `available_at` (S335); chọn `business_date` khi hai phía khác nhau: xem Q3.
- Ràng buộc cấu hình sinh dữ liệu liên quan nằm ở ADR-0011 (normal delay + cửa sổ file không vượt cutoff), không lặp ở đây.

### D5. Tám trạng thái và thứ tự ưu tiên (S341–S350)

Xét lần lượt từ ưu tiên 1; điều kiện đầu tiên đúng quyết định `recon_status`. Hai phía "đủ" nghĩa là sau D2 mỗi phía có đúng một dòng.

| Ưu tiên | Điều kiện | `recon_status` | Dòng |
|---:|---|---|---|
| 1 | Có `key_conflicts`, hoặc >1 bản ghi độc lập ở một phía (D2) | `DUPLICATE_OR_CONFLICT` | S343 |
| 2 | Đủ hai phía và `currency` khác nhau | `CURRENCY_MISMATCH` | S344 |
| 3 | Đủ hai phía và `amount_minor` khác nhau | `AMOUNT_MISMATCH` | S345 |
| 4 | Đủ hai phía và `status` khác nhau | `STATUS_MISMATCH` | S346 |
| 5 | Đủ hai phía và cả ba thuộc tính khớp | `MATCHED` | S347 |
| 6 | Thiếu một phía và `as_of_time < cutoff` | `PENDING` | S348 |
| 7 | Có giao dịch, thiếu sao kê, `as_of_time >= cutoff` | `MISSING_EXTERNAL` | S349 |
| 8 | Có sao kê, thiếu giao dịch, `as_of_time >= cutoff` | `MISSING_INTERNAL` | S350 |

- Thuộc tính đối chiếu là `currency`, `amount_minor`, `status`; `amount_minor` là số nguyên, so bằng `==`, **tolerance bằng 0**, không làm tròn, không epsilon, không quy đổi ngoại tệ (S353, S681, S687). Đổi tolerance cần `recon rule version` mới (S353, S687).
- Khóa đủ hai phía được phân loại ngay, không chờ cutoff; ví dụ `REF-T003` là `AMOUNT_MISMATCH` ở snapshot early (S849).
- `FAILED` được đối soát như `SUCCESS`: giao dịch `FAILED` không có ledger nhưng có bản ghi sao kê (S354, S680; `REF-T002` khớp, `REF-T005` là `STATUS_MISMATCH`). Cách cấu hình tập status/channel: xem Q10.

### D6. `issue_codes` và `recon_status` (S352, S567)

- `recon_status` là trạng thái chính theo D5; `issue_codes` là **tập mọi vấn đề phát hiện được** cho khóa (S352). Một khóa lệch cả currency và amount có `recon_status = CURRENCY_MISMATCH` và `issue_codes` chứa cả hai mã (S567: "nhiều lỗi một key").
- Hàng 2–4 được đánh giá **độc lập** để lập `issue_codes`, kể cả khi hàng nhỏ hơn đã quyết định trạng thái chính; `recon_status` chỉ lấy hàng nhỏ nhất.
- `issue_codes` lưu theo thứ tự ưu tiên của bảng D5 để kết quả xác định. Từ vựng mã và mã cho phía bị quarantine: xem Q1, Q8.
- Schema error thuần túy thuộc quarantine, không phải kết quả đối soát (S352).

### D7. Tuổi sai lệch (P0, S362, S857)

- Với mọi khóa có `recon_status != MATCHED` (kể cả `PENDING`): lưu `anchor_event_time` và `discrepancy_age_seconds = as_of_time − anchor_event_time`, số nguyên giây, không âm.
- `anchor_event_time` = `event_time` của giao dịch nội bộ nếu có; nếu không, `event_time` **nhỏ nhất** trong các bản ghi sao kê của khóa (S362). Khi có nhiều giao dịch hoặc dùng bản ghi nào: xem Q7.
- Khóa `MATCHED`: cả hai trường rỗng (NULL).
- Chỉ phụ thuộc đầu vào và `as_of_time`, không phụ thuộc trạng thái lần chạy trước; mỗi snapshot P0 tính lại từ đầu và chạy lại không đổi giá trị (S362, S855, S570).
- Giá trị tham chiếu trong đặc tả: `REF-T006`, `event_time` `2026-01-01T01:00:06Z`, snapshot early `2026-01-02T04:00:00Z` cho **97.194** giây (S857); script kiểm cho đúng số này.
- Test harness tính tuổi từ `event_time` và `as_of_time` của chính nó, không gọi hàm của pipeline (S857).

### D8. Siêu dữ liệu mỗi lần đánh giá (P0, S361)

Mỗi kết quả lưu `last_evaluated_at`, mã snapshot đầu vào (input snapshot id) và `recon rule version`. Thời điểm dùng làm `last_evaluated_at`: xem Q9.

### D9. Đến muộn sau publish và lịch sử (P1, S137, S358–S361, S527, S855)

- Sau hạn, thiếu phía còn lại là `MISSING_*`; khi sao kê tới muộn thì kết quả hiện tại **được sửa** thành `MATCHED` hoặc mismatch bằng cách chạy lại ngày bị ảnh hưởng (S359). "Trạng thái cuối sau hạn" là trạng thái hiện tại, không cấm sửa (S360).
- Chuỗi bắt buộc khi ba snapshot chạy **nối tiếp** trên cùng trạng thái (S855, S529): `REF-T007`: `PENDING → MATCHED`; `REF-T008`: `PENDING → MISSING_EXTERNAL → MATCHED`.
- P1 thêm `first_seen_at` và `resolved_at`; **không reset** `first_seen_at` mỗi lần chạy lại; chạy lại một snapshot không đổi thì không đổi `first_seen_at` (S361, S855, S571). Khi có `first_seen_at`, dashboard có thể hiển thị thêm "thời gian từ lúc pipeline phát hiện" nhưng **không thay** định nghĩa tuổi sai lệch của P0 (S362). Ngữ nghĩa chi tiết của hai trường: xem Q9.
- Current result có khóa duy nhất theo match key (S363). Bảng lịch sử chỉ thêm khi nội dung đổi là P2, ngoài phạm vi (S363).
- Watermark streaming (nếu dùng) chỉ giới hạn state của query, không thay chính sách đối soát hay backfill (S364).

## Kiểm chứng trên fixture

Nguồn: `expected_results.json` (đáp án) đối với kết quả script áp dụng D1–D7. Cột cuối là hàng của D5 quyết định trạng thái ở snapshot đầu tiên đổi hoặc ở mọi snapshot.

| Khóa | early (02/01 04:00Z) | after_cutoff (02/01 18:00Z) | late_corrected (03/01 01:00Z) | Quy tắc |
|---|---|---|---|---|
| REF-T001 | MATCHED | MATCHED | MATCHED | 5 |
| REF-T002 | MATCHED | MATCHED | MATCHED | 5 (FAILED/FAILED) |
| REF-T003 | AMOUNT_MISMATCH | AMOUNT_MISMATCH | AMOUNT_MISMATCH | 3 (301 so với 300) |
| REF-T004 | CURRENCY_MISMATCH | CURRENCY_MISMATCH | CURRENCY_MISMATCH | 2 (USD so với VND) |
| REF-T005 | STATUS_MISMATCH | STATUS_MISMATCH | STATUS_MISMATCH | 4 |
| REF-T006 | PENDING | MISSING_EXTERNAL | MISSING_EXTERNAL | 6 rồi 7 (không có sao kê) |
| REF-T007 | PENDING | MATCHED | MATCHED | 6 rồi 5 (file 02/01 12:00Z) |
| REF-T008 | PENDING | MISSING_EXTERNAL | MATCHED | 6, 7, rồi 5 (file 03/01 00:00Z) |
| REF-T009 | DUPLICATE_OR_CONFLICT | DUPLICATE_OR_CONFLICT | DUPLICATE_OR_CONFLICT | 1 (hai `statement_record_id`) |
| REF-T010 | MATCHED | MATCHED | MATCHED | 5 (replay đã loại ở Silver) |
| REF-T011 | MATCHED | MATCHED | MATCHED | 5 (replay đã loại ở Silver) |
| REF-T013 | MATCHED | MATCHED | MATCHED | 5 (business_date 02/01, cutoff 03/01 17:00Z) |
| REF-X012 | PENDING | MISSING_INTERNAL | MISSING_INTERNAL | 6 rồi 8 (chỉ có sao kê) |

- Histogram khớp S849–S851: early MATCHED 5, PENDING 4, bốn sai lệch khác; after_cutoff MATCHED 6, MISSING_EXTERNAL 2, MISSING_INTERNAL 1; late_corrected MATCHED 7, MISSING_EXTERNAL 1, MISSING_INTERNAL 1; mỗi snapshot 13 match keys (S853).
- Tuổi tính bằng script (giây), ví dụ early: `REF-T003` 97.197, `REF-T006` 97.194, `REF-X012` 97.188 (anchor là `event_time` sao kê vì không có giao dịch); after_cutoff: `REF-T006` 147.594; late_corrected: `REF-T006` 172.794. Khóa MATCHED không có tuổi.
- Fixture **không phân biệt được** một số lựa chọn ở mục "Quyết định đã chốt cho các câu hỏi Q1–Q12" (ghi rõ ở từng câu).

## Quyết định đã chốt cho các câu hỏi Q1–Q12

Chủ repo (ủy quyền giám sát) duyệt ngày 2026-11-27: Q1–Q4, Q6, Q7, Q9, Q11, Q12 **theo đúng đề xuất** ghi ở từng câu; Q5, Q8, Q10 chốt như ghi riêng ở câu đó. Các lựa chọn khớp oracle T14 nên oracle không phải sửa. Trích dẫn đặc tả giữ nguyên như lúc đặt câu hỏi.

**Q1. Từ vựng `issue_codes` của đối soát.** S352: *"Lưu `issue_codes` cho mọi vấn đề phát hiện được"*; S567: *"kết quả chính và issue_codes đúng"*; TASKS T33 điều kiện 2: *"một khóa nhiều lỗi có đủ issue_codes"*. Đặc tả không liệt kê mã. Mười bảy mã ở Phần II mục 8 (S878–S896) là mã quarantine/quality, không phải mã đối soát. Oracle T14 dùng chính tên trạng thái làm mã (`evaluate_match_key`: `CURRENCY_MISMATCH`, `AMOUNT_MISMATCH`, `STATUS_MISMATCH`, `DUPLICATE_OR_CONFLICT`, `PENDING`, `MISSING_EXTERNAL`, `MISSING_INTERNAL`; `MATCHED` có tập rỗng). **Quyết định** (theo đề xuất): giữ đúng tập này cho phần thuộc bảng D5, thêm mã dẫn xuất từ quality khi có (Q8), sắp theo thứ tự bảng D5.

**Q2. So `amount_minor` khi `currency` đã khác.** S345 chỉ nói "lệch amount_minor" với "đủ hai phía". Không quy đổi ngoại tệ (S681), nhưng so số nguyên trực tiếp vẫn xác định. **Quyết định** (theo đề xuất, khớp oracle): vẫn so cả ba thuộc tính, `issue_codes` ghi mọi lệch, trạng thái chính theo ưu tiên thấp nhất. Fixture `REF-T004` (400 VND và 400 USD) chỉ lệch currency nên không phân biệt.

**Q3. `business_date` dùng tính cutoff khi hai phía khác ngày hoặc khi nhiều bản ghi.** S335: *"`available_at` không thay business_date gốc"*; S685: *"Ngày dùng chọn phạm vi, không nối thêm ngày vào key"*. Không nói ngày nào quyết định cutoff nếu giao dịch và sao kê cùng khóa mang `business_date` khác nhau. Oracle lấy ngày của giao dịch đầu tiên, nếu không có thì của sao kê đầu tiên. **Quyết định** (theo đề xuất): ngày của giao dịch nội bộ nếu có; nếu nhiều giao dịch, ngày nhỏ nhất; nếu không có giao dịch, ngày nhỏ nhất của sao kê; khóa có hai ngày khác nhau ghi thêm một dòng thống kê chất lượng. Fixture không có ca này.

**Q4. Phía giao dịch có bị lọc theo `as_of_time` không.** S787 chỉ định nghĩa nhìn thấy cho sao kê (*"coi một dòng sao kê là đã có khi `file_available_at <= as_of_time`"*). S861 cho publish `events.jsonl` một lần ngay từ đầu. Oracle không lọc giao dịch. **Quyết định** (theo đề xuất): không lọc giao dịch theo `event_time` hay `as_of_time` (mọi giao dịch Silver đã commit đều có mặt). Hệ quả: tuổi có thể âm nếu `as_of_time` sớm hơn `event_time` của giao dịch (Q6). Fixture không phân biệt vì mọi `event_time` giao dịch nhỏ hơn `as_of_time` của snapshot sớm nhất.

**Q5. Nguồn của `file_available_at` trong pipeline.** S787 gọi `file_available_at` nằm trong ground truth, còn S868 cấm pipeline đọc file tham chiếu: *"Production-like job chỉ đọc file đã ingest theo source manifest, không đọc file tham chiếu statements.jsonl rồi lọc available_at"*. **Quyết định** (thay đề xuất ban đầu dùng `statement_deliveries`): pipeline **không** đọc `statement_deliveries.jsonl` hay ground truth. Một file sao kê có mặt tại `as_of_time` khi biên nhận `_ingest/<delivery_order:09d>.json` do `n06 ingest-statements` (T23) ghi trên MinIO có `file_available_at <= as_of_time`; `file_available_at` lấy từ biên nhận. Nội dung biên nhận có `delivery_order`, `file_available_at`, `file_path`, `manifest_path`, `sha256`, `physical_rows` (`build_receipt_payload`). Việc nạp biên nhận vào cột `file_available_at` của Silver sao kê thuộc T25/ADR-0004b; T33 chỉ nhận cột này.

**Q6. Tuổi âm.** S362: *"(giây, số nguyên, không âm)"*. Với Q4 đề xuất, tuổi âm xảy ra khi `as_of_time < anchor_event_time`. Oracle dùng `max(0, ·)`. **Quyết định** (theo đề xuất): kẹp về 0 như oracle và đếm số khóa bị kẹp như một chỉ số chất lượng; không báo lỗi. Với sao kê do generator sinh, tuổi không âm vì `file_available_at` không nhỏ hơn `available_at` (ADR-0011) và `available_at` không nhỏ hơn `event_time`; điều này chưa được kiểm cho dữ liệu ngoài generator.

**Q7. `anchor_event_time` khi nhiều bản ghi nội bộ.** S362: *"`event_time` của giao dịch nội bộ nếu có"*; khóa `DUPLICATE_OR_CONFLICT` có thể có hơn một giao dịch độc lập. Oracle lấy giá trị nhỏ nhất trong giao dịch. **Quyết định** (theo đề xuất): nhỏ nhất của giao dịch; bản ghi bị quarantine không tham gia. Fixture `REF-T009` chỉ có một giao dịch.

**Q8. Phân biệt "thiếu thật" và "thiếu do bị quarantine".** S352: *"kết quả đối soát phải phân biệt thiếu thật với thiếu do record bị quarantine qua quality metadata"*. Không nói cơ chế. **Quyết định**: khi quality metadata cho biết có record quarantine khôi phục được key trùng match key, `recon_status` vẫn theo bảng D5 (không đổi thứ tự ưu tiên) và `issue_codes` có thêm mã `COUNTERPART_QUARANTINED`, kèm con trỏ nguồn trong child table. `COUNTERPART_QUARANTINED` là **mã đối soát bổ sung của dự án, không thuộc danh sách 17 mã lỗi hợp đồng ở Phần II mục 8 (S878–S896)**. Khóa không khôi phục được thì không tạo khóa (S334). Trường metadata cụ thể do T30–T32 cung cấp; T33 nhận nó như một đầu vào tùy chọn (`quarantined_keys`), thiếu thì không thêm mã.

**Q9. Định nghĩa `last_evaluated_at`, `first_seen_at`, `resolved_at`.** S361 chỉ nêu tên và *"không reset first_seen_at mỗi lần chạy lại"*; S362 nói "thời gian từ lúc pipeline phát hiện". Không nói đồng hồ nào, `PENDING` có tính "phát hiện" không, và khóa mở lại thì sao. **Quyết định** (theo đề xuất): (a) cả ba dùng `as_of_time` của lần đánh giá, để giữ tái lập và đúng S358 (không dùng đồng hồ hệ thống); thời điểm chạy thật nếu cần ghi ở cột riêng ngoài đối soát; (b) `first_seen_at` = `as_of_time` của lần đánh giá đầu tiên mà khóa có `recon_status != MATCHED` (kể cả `PENDING`), không đổi sau đó; khóa `MATCHED` ngay lần đầu có `first_seen_at` NULL; (c) `resolved_at` = `as_of_time` của lần đầu khóa chuyển sang `MATCHED` sau khi đã có `first_seen_at`; nếu về sau khóa lại lệch thì `resolved_at` về NULL và `first_seen_at` giữ nguyên; (d) chạy lại cùng `as_of_time` không đổi cả hai. Chuỗi nghiệm thu: `REF-T007` có `first_seen_at` = early, `resolved_at` = after_cutoff; `REF-T008` có `first_seen_at` = early, `resolved_at` = late_corrected.

**Q10. Cấu hình tập status/channel cần đối soát.** S354: *"Tập status/channel cần đối chiếu phải cấu hình và tài liệu hóa"*. **Quyết định**: thêm mục `[reconciliation]` vào `config/default.toml` với `statuses` (mặc định `["SUCCESS","FAILED"]`) và `channels` (mặc định `["SIM_CHANNEL_A","SIM_CHANNEL_B"]`, S680); mục này và tài liệu trong `config/README.md` được hiện thực ở **T33** (không phải việc riêng của C).

**Q11. Phạm vi ngày được đánh giá mỗi lần chạy.** S335: *"Cửa sổ đủ rộng và cơ chế chọn ngày phải cho phép statement đến sau vẫn đối chiếu giao dịch ngày cũ"*. Không nêu cơ chế chọn ngày. **Quyết định** (theo đề xuất): P0 đánh giá mọi `business_date` có bản ghi trong đầu vào của dataset tại `as_of_time`; P1 chạy lại các `business_date` của sao kê mới ingest (suy từ `ingest_batches`) cộng mọi khóa còn `PENDING`.

**Q12. Chạy một snapshot cũ sau snapshot mới (P1).** S855 chỉ nói chạy lại **cùng** snapshot không đổi `first_seen_at`. Chạy `as_of_time` sớm hơn khi kết quả hiện tại đã ở `as_of_time` muộn hơn chưa được nói. **Quyết định** (theo đề xuất): từ chối ghi đè kết quả hiện tại bằng `as_of_time` cũ hơn (mã thoát riêng) trừ khi có cờ tường minh; khi có cờ, `first_seen_at = min(hiện có, as_of_time mới)`. Fixture chỉ chạy tăng dần.

## Phương án đã cân nhắc

- **Phân loại bằng join rồi mới đếm trùng.** Loại: join trước khi kiểm cardinality tạo many-to-many và nhân dòng, trái S336.
- **Thêm `currency` vào match key.** Loại: S685 cấm vì che mất `CURRENCY_MISMATCH`.
- **Dùng đồng hồ hệ thống cho `first_seen_at`.** Chưa chọn; xem Q9, vì đồng hồ hệ thống phá tái lập và vi phạm S358 nếu dùng cho cutoff.
- **Phân loại `DUPLICATE_OR_CONFLICT` chỉ khi đủ hai phía.** Loại: S343 ghi "nhiều record độc lập cho match key" không điều kiện phía, và `REF-T009` cần trạng thái này trước cutoff.
- **Coi "tại đúng cutoff" là chưa quá hạn.** Loại: S358 và S686 ghi ngược lại; `cutoff_cases.json` xác nhận.

## Hệ quả

- **T33** hiện thực D1–D8 và các quyết định Q1–Q8, Q10, Q11; kiểm bằng từng khóa của ba snapshot (D5 và bảng kiểm chứng) và `cutoff_cases.json`.
- **T34** nghiệm thu ba snapshot tính lại độc lập từ đầu, mỗi snapshot chạy hai lần.
- **T61** hiện thực D9 và Q9, Q12; thêm cột `first_seen_at`, `resolved_at` vào kết quả hiện tại (migration thuộc người sở hữu schema).
- **T14 (oracle)**: Q1–Q7 chốt theo đề xuất, khớp oracle hiện có; không phải sửa.
- **T30–T32 (quality)**: cần xuất `key_conflicts` đủ match key (S898) và metadata quarantine theo Q8.
- **Config**: mục `[reconciliation]` theo Q10, hiện thực ở T33.
- **T23/T25**: biên nhận `_ingest/` là nguồn `file_available_at` (Q5); T25 phải đưa cột này vào Silver sao kê.
- ADR này không đổi tệp mã hay fixture nào.

## Lịch sử

- 2026-11-27: soạn, trạng thái Proposed (PR #26).
- 2026-11-27: Accepted bởi chủ repo (ủy quyền giám sát); Q1–Q4, Q6, Q7, Q9, Q11, Q12 theo đề xuất; D3 và Q5 sửa theo biên nhận `_ingest/`; Q8 thêm `COUNTERPART_QUARANTINED`; Q10 giao T33.
