# ADR-0002: Cấu trúc bucket và đường dẫn MinIO (Raw, Bronze, Silver, Gold, sao kê, benchmark)

- Trạng thái: Proposed
- Người viết: agent (task ADR-002; người sở hữu trong docs/TASKS.md: A), các điểm 1–9 mục "Điểm đặc tả mơ hồ" đã được người giao task duyệt trong phiên lập kế hoạch 2026-11-24
- Người duyệt: B (chờ duyệt, theo docs/TASKS.md)
- Ngày: 2026-11-24
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 6 (Raw, Bronze, Silver), mục 8.1 và 8.2, mục 14.1; Phần II mục 4, mục 5.2, mục 7. Ngoài đặc tả: ADR-0003, ADR-0009, ADR-0010.

## Bối cảnh
Đặc tả chốt giao thức commit nhưng chỉ nêu một phần đường dẫn. Task T22 (Raw/Bronze), T23 (sao kê) và T24 (commit batch) cần cùng một quy ước, nếu không sẽ mỗi nơi tự chọn một kiểu.

**Căn cứ đặc tả (trích nguyên văn).**

Cấu hình, không hardcode (Phần I mục 3):
> *"Không nhúng endpoint, hostname, port, credential, đường dẫn tuyệt đối, tên bucket/topic, ngày chạy, seed, quy mô dữ liệu, partition count hoặc cấu hình executor vào logic nghiệp vụ."*

> *"Tên path/object key là cấu hình. Trong fixture path tương đối gốc gói; source pointer gồm file/checksum/line number hoặc topic/partition/offset."* (Phần II mục 4)

Raw và Bronze (Phần I mục 6):
> *"Khóa kỹ thuật Kafka: topic + partition + offset. Không dedup Raw bằng transaction_id; Raw cần giữ dấu vết phát lại và lỗi."*

> *"Lưu statement nguyên bản cùng manifest/checksum; file đến sau phải được phát hiện qua ingest manifest, không chỉ scan thư mục của ngày hiện tại."*

> *"Partition theo ingestion_date để bắt được sự kiện đến muộn."*

> *"Record parse lỗi vẫn có đường đến quarantine, không bị filter bỏ im lặng."*

> *"Checkpoint nằm trên volume/bucket bền vững, riêng theo query và môi trường. Không xóa checkpoint trong thao tác restart thông thường."*

Giao thức streaming (Phần I mục 8.1):
> *"Ghi Raw rồi Bronze (kèm bản ghi parse lỗi) vào đường dẫn **xác định theo `batch_id`**, chế độ overwrite: `.../ingestion_date=<ngày>/batch_id=<id>/`."*

> *"`ingestion_date` lấy từ thời điểm nhỏ nhất của Kafka timestamp trong batch để lần phát lại cho cùng đường dẫn."*

> *"upsert một dòng vào bảng control `ingest_batches` với khóa (query_name, batch_id), kèm khoảng offset theo topic-partition và số bản ghi."*

> *"Chết trước bước 3: Spark phát lại đúng `batch_id` với cùng khoảng offset từ checkpoint, ghi đè đúng đường dẫn cũ."*

> *"runbook yêu cầu dùng `query_name` và tiền tố mới rồi nạp lại từ offset đầu"*

Silver và Gold (Phần I mục 6 và mục 8.1):
> *"Silver và Gold lưu Parquet, phân vùng theo `business_date` bên dưới tiền tố dataset/entity/run_id."*

> *"Mỗi lần chạy ghi một phiên bản bất biến vào đường dẫn chứa dataset/entity/`run_id`, phân vùng theo `business_date`."*

> *"Reader chỉ đọc đường dẫn do `current_snapshot` trỏ tới; không glob toàn bucket, không đọc run thất bại hay phiên bản cũ."*

> *"Không ghi thẳng đè partition đang được đọc; không dùng append vô điều kiện cho Silver/Gold."*

Sao kê (Phần II):
> *"File đã công bố bất biến. Sửa dữ liệu phải tạo batch/file mới theo contract version/rule đã định; MVP fixture chỉ bổ sung late statements, không sửa transaction cùng ID."* (mục 4)

> *"Production-like job chỉ đọc file đã ingest theo source manifest, không đọc file tham chiếu statements.jsonl rồi lọc available_at như cách thay thế cho kiểm thử đường ingest thực."* (mục 7)

> *"Accounts/reference files/expected_results không được đặt chung prefix operational mà Spark tự glob. Test harness và pipeline phải có quyền/đường dẫn tách biệt đối với oracle."* (mục 7)

Benchmark (Phần I mục 14.1):
> *"E4 dùng một đường ghi riêng của lệnh `benchmark` vào tiền tố benchmark; không thêm JSON vào pipeline chính."*

**Hiện trạng cấu hình và mã.**
- `config/default.toml` đã có `object_store.bucket = "n06-lakehouse"` và `paths.{raw,bronze,silver,gold,quarantine,statements,checkpoints,benchmark}_prefix`; `src/n06/config.py` kiểm các prefix không trùng nhau. `config/test.toml` dùng bucket và prefix `test-*` riêng.
- ADR-0003: `ingest_batches` có `raw_path TEXT NOT NULL`, `bronze_path TEXT NOT NULL`; `current_snapshot` có `business_date DATE NOT NULL`, `output_path TEXT NOT NULL` và `manifest_path TEXT`, khóa duy nhất `(dataset_id, layer, entity, business_date)`.
- T24 (DONE, `src/n06/storage/commit.py`): `build_run_output_path` hiện trả `<base>/<dataset_id>/<layer>/<entity>/run_id=<run_id>/business_date=<d>`, và `commit_run` từ chối `output_path` không chứa `run_id=<run_id>`. Cách dựng này không dùng `paths.silver_prefix`/`paths.gold_prefix`.
- `config/benchmark.toml` chỉ ghi đè `dataset_id` (và Spark/Kafka), không ghi đè `object_store` hay `paths`.

### Điểm đặc tả mơ hồ hoặc im lặng và cách chốt
Mỗi điểm dưới đây được người giao task duyệt trong phiên lập kế hoạch 2026-11-24. Điểm 1 được điều chỉnh sau khi phát hiện hiện trạng T24 ở trên.

1. **Vị trí tiền tố tầng và "dataset".** Đặc tả nói "bên dưới tiền tố dataset/entity/run_id" nhưng không nói tiền tố tầng đứng ở đâu, và "dataset" có phải `dataset_id` không. Chốt: tiền tố tầng lấy từ `paths.<tầng>_prefix` và đứng đầu; "dataset" là `dataset_id`; `run_id` dùng dạng `run_id=<run_id>` để khớp kiểm tra của T24 và vẫn đúng nguyên văn đặc tả.
2. **Phần đứng trước `ingestion_date`.** Đặc tả viết `.../ingestion_date=<ngày>/batch_id=<id>/` và không định nghĩa phần `...`. Chốt: phần đó là `<tiền tố tầng>/<query_name>/`. Lý do: `batch_id` của một query mới bắt đầu lại từ đầu, nên thiếu `query_name` thì sau sự cố mất checkpoint (đặc tả yêu cầu đổi `query_name`) batch 0 mới ghi đè batch 0 cũ.
3. **Múi giờ của `ingestion_date`.** Đặc tả chỉ nói lấy từ Kafka timestamp nhỏ nhất của batch. Chốt: ngày tính theo `Asia/Ho_Chi_Minh`, cùng múi với `business_date` (CLAUDE.md); timestamp lưu trong dữ liệu vẫn là UTC.
4. **Bản ghi parse lỗi và quarantine.** Đặc tả nói bản ghi parse lỗi nằm trong output của batch và "có đường đến quarantine" nhưng không nêu vị trí. Chốt: parse lỗi nằm trong thư mục batch Bronze với `entity=parse_error`; quarantine nghiệp vụ của Silver dùng `paths.quarantine_prefix` với đúng giao thức run_id của Silver, `layer = 'quarantine'` trong `current_snapshot`.
5. **Dimension không có `business_date`.** Đặc tả yêu cầu Silver/Gold phân vùng theo `business_date`, ADR-0003 đặt `business_date DATE NOT NULL`, trong khi `dim_account`, `dim_channel`... không có cột này. Chốt: mọi entity Gold, kể cả dimension, ghi dưới `business_date=<ngày đang xử lý>` (bản chụp của ngày đó).
6. **`output_path` của `current_snapshot`.** ADR-0003 không nói trỏ tới gốc run hay tới partition. Chốt: trỏ tới thư mục partition `.../run_id=<run_id>/business_date=<d>`; một run nhiều ngày có nhiều dòng con trỏ cùng `run_id`. `manifest_path` dành cho tên `_manifest.json` ở gốc run (`.../run_id=<run_id>/_manifest.json`); manifest là P2, không bắt buộc ở P0.
7. **Sao kê và `delivery_order`.** Đặc tả yêu cầu phát hiện file đến muộn "qua ingest manifest", nhưng ADR-0003 không có bảng ingest manifest cho sao kê, và ADR-0009 ghi `statement_deliveries.jsonl` (lịch điều phối) không nằm trong manifest. Chốt: biên nhận `_ingest/` (xem mục 4 của Quyết định); không thêm bảng control.
8. **Profile benchmark dùng chung bucket và tiền tố.** `config/benchmark.toml` không ghi đè `object_store`/`paths`, nên pipeline đầy đủ khi đo (E1–E3, E5) sẽ ghi vào cùng `raw`, `bronze` với dữ liệu nghiệp vụ; Raw/Bronze không có `dataset_id` trong đường dẫn để phân biệt. Chốt: xem mục 5 của Quyết định.
9. **Accounts và oracle.** Đặc tả cấm đặt oracle chung prefix mà Spark tự glob nhưng không nói accounts nằm đâu trên MinIO. Chốt: ground truth và `expected_results` không bao giờ nằm trong bucket nghiệp vụ; vị trí `accounts.jsonl` để task nạp accounts quyết (ngoài phạm vi ADR này).

## Quyết định

### Quy ước chung
- Mọi đường dẫn ghi vào `ingest_batches.raw_path`, `ingest_batches.bronze_path`, `current_snapshot.output_path` và `manifest_path` là URI đầy đủ `s3a://<object_store.bucket>/<key>`, dựng lúc chạy từ cấu hình. Mã không chứa tên bucket hay tên tiền tố bằng chữ.
- `<key>` không có dấu `/` đầu và cuối ở các thành phần ghép; thư mục kết thúc bằng `/` khi dùng làm đích ghi.
- Ngày luôn là `YYYY-MM-DD`.
- Ký tự cho phép: `dataset_id` theo schema hợp đồng (`^[a-z][a-z0-9-]{2,63}$`); `entity` theo `^[a-z][a-z0-9_]{0,63}$`; `query_name` theo `^[a-z][a-z0-9_-]{2,63}$`; `run_id` theo `^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$` (`generate_run_id()` của T24 trả `run-<hex>` đã thỏa). Giá trị ngoài mẫu phải bị từ chối trước khi ghi.
- `query_name` là giá trị cấu hình hoặc tham số của lệnh `n06 stream`; nếu `config/` chưa có khóa thì T22 thêm khóa và ghi vào README của `config/`.

### 1. Bucket và tiền tố gốc
- Một bucket cho mỗi môi trường, tên lấy từ `object_store.bucket` (mặc định `n06-lakehouse`; profile `test` dùng `n06-test-lakehouse`).
- Tiền tố theo tầng lấy từ `paths.raw_prefix`, `bronze_prefix`, `silver_prefix`, `gold_prefix`, `quarantine_prefix`, `statements_prefix`, `checkpoints_prefix`, `benchmark_prefix`. Không thêm khóa cấu hình mới.
- Checkpoint của một query: `<paths.checkpoints_prefix>/<query_name>/`. "Riêng theo môi trường" được bảo đảm bằng bucket và prefix theo profile (như `test.toml`), không bằng thêm một đoạn trong đường dẫn.

### 2. Raw và Bronze
- Raw: `<paths.raw_prefix>/<query_name>/ingestion_date=<YYYY-MM-DD>/batch_id=<batch_id>/`
- Bronze: `<paths.bronze_prefix>/<query_name>/ingestion_date=<YYYY-MM-DD>/batch_id=<batch_id>/entity=<entity>/`, với `entity` gồm các entity Bronze và `parse_error` (điểm 4). Tên entity cụ thể và schema thuộc ADR-0004.
- `ingest_batches.raw_path` là URI thư mục Raw của batch; `ingest_batches.bronze_path` là URI thư mục Bronze của batch, tức đến `batch_id=<id>/` và chưa gồm `entity=`. Một batch có đúng một đường dẫn Raw và một đường dẫn Bronze, nên "ghi đè đúng đường dẫn cũ" là ghi đè một thư mục.
- `batch_id` là số nguyên của `foreachBatch`; `ingestion_date` tính theo điểm 3. Đường dẫn được dựng tường minh từ hai giá trị này, không dùng `partitionBy` động của Spark.
- Chế độ overwrite chỉ áp dụng cho đúng thư mục `batch_id=<id>/` (Raw và Bronze), không bao giờ cho thư mục cha.
- Raw giữ nguyên replay do T13 tiêm và do phát lại; không dedup (ADR-0010 mục 6: *"Replay do T13 tiêm là message riêng, không bị loại ở bước publish."*). Định dạng file bên trong Raw thuộc T22/ADR-0004.
- Job batch chỉ đọc các thư mục có `batch_id` trong `ingest_batches` (đặc tả I.8.1 bước 4), không glob theo `ingestion_date`.
- Giới hạn cần biết: S3A không có rename thư mục nguyên tử. Ghi đè một thư mục batch đã COMMITTED (ca "chết sau bước 3") làm thư mục đó tạm thời ở trạng thái dở dang với reader chạy đồng thời. Đây là hệ quả của thiết kế đặc tả (ghi đè cùng nội dung), không được giải quyết ở ADR này.

### 3. Silver, Gold và quarantine
- Silver: `<paths.silver_prefix>/<dataset_id>/<entity>/run_id=<run_id>/business_date=<YYYY-MM-DD>/`
- Gold: `<paths.gold_prefix>/<dataset_id>/<entity>/run_id=<run_id>/business_date=<YYYY-MM-DD>/` (entity Gold gồm cả fact và dimension, xem điểm 5 ở mục "Điểm đặc tả mơ hồ").
- Quarantine: `<paths.quarantine_prefix>/<dataset_id>/<entity>/run_id=<run_id>/business_date=<YYYY-MM-DD>/`.
- Giá trị `layer` trong `current_snapshot` và `runs` khớp tên tầng: `silver`, `gold`, `quarantine`.
- Một run ghi tất cả `business_date` của nó dưới cùng `run_id=<run_id>/`. Mỗi `(dataset_id, layer, entity, business_date)` có một dòng `current_snapshot` với `output_path` là URI partition tương ứng (điểm 6). Reader chỉ đọc `output_path` đó, không đọc theo glob.
- Thứ tự commit theo ADR-0003 và T24: ghi toàn bộ output và validate, rồi mới cập nhật `current_snapshot` trong một transaction. Đường dẫn của run không thay đổi sau bước đó.
- Số file mỗi partition và kích thước file là cấu hình (đặc tả I.6), không phải thành phần của đường dẫn.

### 4. Sao kê trên MinIO
- Khóa đối tượng của file dữ liệu và manifest: `<paths.statements_prefix>/<dataset_id>/<đường dẫn tương đối của file trong gói nguồn>`. Đường dẫn tương đối lấy nguyên từ `path` trong manifest hoặc từ vị trí file trong nguồn do T13 sinh, ví dụ `statements/CH/statements_w000000001.jsonl` và `manifests/statements_CH_w000000001.manifest.json`. Không đổi tên, không dịch đường dẫn, để `sha256`, `physical_rows` và source pointer đối chiếu trực tiếp được. Đoạn `statements/` lặp lại sau `statements_prefix` là chủ ý.
- Cách phân biệt: **channel** là thư mục `<channel>/` và là một phần của tên manifest (`statements_<channel>_w...`); **cửa sổ** là `w<9 chữ số>` trong tên file và manifest (đặc tả II.5.2: *"Mỗi cặp (w, channel) có đúng một file"*); bản `statement_duplicate` nằm cùng file với bản gốc (ADR-0009 quyết định 2) nên không có đường dẫn riêng.
- **`delivery_order`** không nằm trong khóa dữ liệu. ADR-0009 quyết định 3: *"`delivery_order` xếp theo `(file_available_at, channel)` tăng dần, đánh số từ 0, mỗi file một số; các dòng cùng file cùng `delivery_order`."* Mỗi file giao xong có một biên nhận `<paths.statements_prefix>/<dataset_id>/_ingest/<delivery_order:09d>.json` (số có đủ 9 chữ số, để thứ tự từ vựng khớp thứ tự giao). Nội dung biên nhận tối thiểu: `file_path`, đường dẫn manifest, `sha256`, `physical_rows`, `file_available_at`, `delivery_order`; schema chi tiết thuộc T23.
- Thứ tự tải lên của T23: file dữ liệu, rồi manifest, rồi biên nhận. Biên nhận là dấu hiệu "file đã đến". Pipeline phát hiện file mới bằng cách liệt kê `_ingest/`, không quét thư mục theo ngày (đặc tả I.6 Raw), và kiểm `sha256`/`physical_rows` theo manifest trước khi đọc. Biên nhận mà thiếu file hoặc manifest tương ứng là lỗi toàn vẹn, không được bỏ qua.
- File sao kê bất biến (đặc tả II.4). Tải lại cùng khóa chỉ được phép khi `sha256` giống file đã có; khác `sha256` là lỗi. File đến muộn là file mới (cửa sổ khác hoặc batch mới), có khóa mới.
- `statement_deliveries.jsonl` và `statements.jsonl` tham chiếu không được tải lên MinIO. Pipeline không đọc chúng (đặc tả II.7).
- Ground truth, `expected_results.json`, `fault_assignment.jsonl` và `canonical/` không bao giờ nằm trong bucket nghiệp vụ; test harness đọc từ thư mục máy cục bộ (đặc tả II.7).

### 5. Tiền tố benchmark
- `<paths.benchmark_prefix>/<experiment>/<benchmark_run_id>/<variant>/...`, với `experiment` là `e1`..`e8` (chữ thường), `benchmark_run_id` theo mẫu của `run_id`, và `variant` là tên biến thể đo, ví dụ E4 ghi `.../e4/<benchmark_run_id>/json/` và `.../e4/<benchmark_run_id>/parquet/`.
- Chỉ đường ghi riêng của lệnh `benchmark` (E4, E7 và bản sao dữ liệu cho E8 nếu cần) ghi vào `benchmark_prefix`. Chúng không đi qua `current_snapshot` và không được pipeline chính đọc. E8 đọc Silver đã commit theo `current_snapshot`, chỉ đọc, không ghi lại vào tầng nghiệp vụ.
- Pipeline đầy đủ chạy lúc đo (E1–E3, E5) dùng một bucket riêng, đặt bằng ghi đè `object_store.bucket` trong `config/benchmark.toml`, cùng cách `config/test.toml` đang làm. Cấu trúc đường dẫn trong bucket đó giống hệt các mục 2–4. Việc sửa `benchmark.toml` nằm ngoài ADR này (xem Hệ quả).
- Kết quả đo (CSV/JSON, biểu đồ) lưu ở thư mục `reports/` trên máy, không lưu lên MinIO. Chỉ dữ liệu mà các thí nghiệm ghi và đọc mới nằm trong `benchmark_prefix`.

### 6. Điều được đổi và điều không được ghi đè khi chạy lại
| Khu vực | Được phép khi chạy lại | Tuyệt đối không |
|---|---|---|
| Raw, Bronze | Ghi đè nguyên thư mục `batch_id=<id>/` với cùng nội dung; upsert `ingest_batches` theo `(query_name, batch_id)` | Ghi vào thư mục của `query_name` khác; ghi đè thư mục cha hoặc nhiều batch trong một lệnh; xóa checkpoint khi restart thông thường |
| Silver, Gold, quarantine | Chạy lại luôn sinh `run_id` mới và một thư mục mới; chuyển con trỏ `current_snapshot` sang run mới | Ghi, xóa hoặc ghi đè bất kỳ object nào dưới `run_id=<run_id>/` của run đã commit hoặc từng được `current_snapshot` trỏ tới; ghi vào `run_id` đã có object (phải báo lỗi, kiểm tra trước khi ghi); đọc run thất bại hoặc phiên bản cũ |
| Sao kê | Tải lại cùng khóa với `sha256` giống nhau; ghi lại biên nhận cùng nội dung | Ghi đè khóa đã có với `sha256` khác |
| Benchmark | Mỗi lần đo dùng `benchmark_run_id` mới | Ghi vào `raw`, `bronze`, `silver`, `gold` của bucket nghiệp vụ |

Object của run thất bại được giữ nguyên, không ai đọc; việc dọn là P2 (đặc tả I.8.1: *"dọn object mồ côi của run thất bại"*).

## Phương án đã cân nhắc
- **Đường dẫn T24 hiện tại `<base>/<dataset_id>/<layer>/<entity>/run_id=.../`**: không cần sửa mã, nhưng làm `paths.silver_prefix`/`gold_prefix` vô dụng cho Silver/Gold và đưa tên tầng vào chuỗi cứng của mã. Không chọn; T24 phải sửa nhỏ (xem Hệ quả).
- **`run_id` không có `run_id=`** (sát nghĩa đen "dataset/entity/run_id"): phải sửa cả đường dẫn lẫn kiểm tra `run_id=` trong `commit_run`, không có lợi ích đo được. Không chọn.
- **Raw/Bronze không có `query_name`**: ghi đè nhầm khi đổi query sau khi mất checkpoint. Không chọn (điểm 2).
- **`entity` đứng trước `ingestion_date` ở Bronze**: partition discovery của Spark dễ hơn, nhưng một batch rải ra nhiều thư mục, trái với `bronze_path` một cột của ADR-0003 và với nguyên văn `.../ingestion_date=<ngày>/batch_id=<id>/`. Không chọn.
- **Dimension ghi vào thư mục "static" không có `business_date`**: không khớp `current_snapshot.business_date NOT NULL` và khóa duy nhất của ADR-0003, phải sửa ADR-0003. Không chọn.
- **Không có biên nhận sao kê, chỉ dựa vào manifest** (phương án B của phiên lập kế hoạch): đơn giản hơn nhưng mất thứ tự đến của file trên bucket. Không chọn.
- **Bảng control mới cho ingest manifest sao kê**: phải sửa ADR-0003 và T21, ngoài phạm vi. Không chọn.
- **Pipeline đo hiệu năng ghi dưới `benchmark_prefix` của cùng bucket** (cần thêm một đoạn gốc cho mọi đường dẫn): cần khóa cấu hình mới ngoài danh sách. Dùng bucket riêng theo profile đơn giản hơn và đã có tiền lệ ở `test.toml`.
- **Một bucket cho mỗi tầng**: nhiều bucket phải cấu hình, tạo và cấp quyền; đặc tả dùng "tiền tố" và `object_store.bucket` chỉ có một giá trị. Không chọn.

## Hệ quả
- **T22 (`n06 stream`)**: dựng đường dẫn Raw/Bronze theo mục 2; thêm khóa cấu hình `query_name` nếu chưa có; ghi `raw_path`/`bronze_path` là URI thư mục batch; ghi đè đúng thư mục batch.
- **T23 (`n06 ingest-statements`)**: khóa đối tượng theo mục 4; tải file, manifest rồi biên nhận `_ingest/`; từ chối tải lại khác `sha256`; định nghĩa schema biên nhận.
- **T24 (đã DONE, cần sửa nhỏ)**: `build_run_output_path` phải nhận tiền tố tầng từ `paths.<tầng>_prefix` thay cho `base_prefix` chung, bỏ đoạn `<layer>` khỏi đường dẫn, và kiểm tra thư mục run chưa có object trước khi ghi. Test `test_build_run_output_path_and_independent_semantic_hash` kỳ vọng đường dẫn cũ nên phải cập nhật theo. Kiểm tra `run_id=` trong `commit_run` giữ nguyên. Việc này do người sở hữu T24 (B) quyết định thời điểm; ADR này chưa thay đổi mã.
- **ADR-0003**: không đổi cột; cần ghi thêm giá trị `layer` cho phép gồm `silver`, `gold`, `quarantine` khi ADR-0003 được cập nhật tiếp theo (hiện chỉ là `TEXT`).
- **ADR-0004, ADR-0005**: chốt tên entity Bronze/Silver/Gold dùng trong đường dẫn (mẫu `^[a-z][a-z0-9_]{0,63}$`) và định dạng file Raw.
- **`config/benchmark.toml`**: cần một task nhỏ thêm ghi đè `object_store.bucket` (và README của `config/`) trước khi chạy E1–E3, E5.
- **Vị trí `accounts.jsonl` trên MinIO**: còn mở, thuộc task nạp accounts.
- **Runbook (`docs/runbook.md`)**: khi viết hướng dẫn mất checkpoint, dùng `query_name` mới thì Raw/Bronze mới tự nằm ở cây thư mục mới, không cần đổi `paths.*_prefix`.
- Chỉ ADR ở trạng thái Accepted mới được dùng làm căn cứ code; T22, T23 chưa được bắt đầu trước khi ADR này được B duyệt.
