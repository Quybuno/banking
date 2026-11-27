# ADR-0007: Cách đo benchmark và định dạng file kết quả (E1–E8)

- Trạng thái: Proposed
- Người viết: C (soạn bởi agent theo `docs/TASKS.md` mục 3, ADR-007), chờ người duyệt
- Người duyệt: A, B
- Ngày: 2026-11-27
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 14.1–14.3 (S589–S632), mục 2 (S183–S187), M5 và M6 (S516–S530), kiểm thử S574; Phần II mục 5.1 (HOT_ACCOUNT_V1), 1.16 (S692).

Ký hiệu `S<dòng>` là số dòng trong `docs/spec.md` bản 1.2. ADR này dùng cho T51 (lệnh `n06 benchmark`), T52 (chạy E1–E5), T63 (E6), T64 (E7), T65 (E8) và T53 (báo cáo). Phạm vi dự án là P0 + P1 đầy đủ: E1–E5 (P0) và E6–E8 (P1) đều phải có số đo, không rút gọn.

## Bối cảnh

Đặc tả mục 14 cho ma trận E1–E8, định nghĩa metrics và danh sách cột kết quả, nhưng để mở cách thực hiện: mỗi cấu hình chạy mấy lần và đếm lần nào, lần "sai kết quả" xử lý ra sao, mức nào không chạy được thì báo gì, file kết quả cụ thể có cột nào. Đồng thời E8 cần một phụ thuộc mới (DuckDB) chưa có trong `pyproject.toml` và `uv.lock`. ADR này chốt cách đo để T51 và các task chạy thí nghiệm làm theo, và ghi những chỗ đặc tả chưa nói vào "Câu hỏi mở".

Thiết lập máy, kiểm bằng lệnh ngày 2026-11-27 (không dùng con số trong thẻ task):

| Mục | Giá trị đo | Lệnh |
|---|---|---|
| RAM tổng của WSL2 | 8.935 MB (`MemTotal`), tức khoảng 8,7 GiB | `free -m` |
| RAM đang dùng / khả dụng lúc kiểm | 5.117 MB / 3.601 MB (có phiên khác đang chạy) | `free -m` |
| Swap | 4.096 MB | `free -m` |
| `.wslconfig` | `memory=9GB`, `swap=4GB` | đọc `.wslconfig` của người dùng Windows |
| CPU | AMD Ryzen 7 5800HS, 16 luồng logic (2 luồng mỗi nhân) | `lscpu`, `nproc` |
| Docker | 28.5.1; Docker thấy 16 CPU, 8,726 GiB RAM | `docker info` |
| Ổ dữ liệu D | tổng 237 GB, đã dùng 202 GB, còn 36 GB (86%) | `df -h /mnt/d` |
| `/` của WSL | báo 1007 GB, còn 926 GB (đĩa ảo; số này **không** phải chỗ trống thật, thật là ổ D, xem `PROGRESS.md` dòng CFG-scale-guard) | `df -h /` |
| Spark, `config/default.toml` | `local[2]`, `driver_memory = "2g"`, `executor_memory = "2g"`, `shuffle_partitions = 50`, `aqe_enabled = true`, `cores = 2` | đọc tệp |
| Spark, `config/benchmark.toml` | `local[4]`, `driver_memory = "4g"`, `executor_memory = "4g"`, `cores = 4`, `kafka.num_partitions = 6`, `max_offsets_per_trigger = 100000` | đọc tệp |
| Quy mô benchmark trong `config/benchmark.toml` | `transaction_count = 1000000`, `account_count = 10000` | đọc tệp |

Hai điều rút ra: (1) cấu hình Spark khác nhau giữa profile `default` (`local[2]`, 2g) và `benchmark` (`local[4]`, 4g); con số `local[2]` trong thẻ task đúng với `default.toml` nhưng profile `benchmark` ghi đè. Cấu hình gốc chưa được chốt (xem Q1). (2) Máy nhỏ: RAM khả dụng đã thấp khi có phiên khác chạy, nên phép đo chỉ có nghĩa khi máy rảnh (mục B2).

## Quyết định

### B1. Ma trận đủ, không rút gọn (S593–S606)

Mọi mức dưới đây là **tham số** của `n06 benchmark`, không sửa mã để đổi (S613). "Cấu hình" là một tổ hợp giá trị biến; mỗi cấu hình chạy theo B2.

| Mã | Biến (mọi thứ khác giữ nguyên) | Mức | Quy mô | Số cấu hình | Mức ưu tiên | Task chạy |
|---|---|---|---|---:|---|---|
| E1 | số giao dịch | 100.000 / 1.000.000 / 5.000.000 | chính nó | 3 | P0 | T52 (C) |
| E2 | số partition Kafka | 1 / 3 / 6, tốc độ phát không giới hạn (`producer.rate = 0`) | 1M hoặc mức cao nhất ổn định | 3 | P0 | T52 (A) |
| E3 | `spark.sql.shuffle.partitions` × AQE (hai yếu tố, ghi từng tổ hợp) | {8, 50, 200} × {bật, tắt} | 1M hoặc mức cao nhất ổn định | 6 | P0 | T52 (C) |
| E4 | định dạng Silver giao dịch, cùng nội dung | JSON / Parquet | 1M hoặc mức cao nhất ổn định | 2 | P0 | T52 (C) |
| E5 | số core (`local[N]` hoặc executor) | 1 / 2 / 4 | 1M hoặc mức cao nhất ổn định | 3 | P0 | T52 (C) |
| E6 | phân bố tài khoản × Kafka key (hai yếu tố) | {đều, `HOT_ACCOUNT_V1`} × {`transaction_id`, `account_from`} | 1M hoặc mức cao nhất ổn định | 4 | P1 | T63 (A) |
| E7 | cách ghi Silver theo `business_date` | không repartition / repartition theo khóa phân vùng | 1M hoặc mức cao nhất ổn định | 2 | P1 | T64 (B) |
| E8 | bộ máy: Spark (cấu hình gốc) / DuckDB đơn tiến trình | trên cùng bản Silver Parquet đã commit | 100.000 và 1.000.000; thêm mức cao nhất của E1 nếu chạy được | 4 (6 nếu E1 5M chạy được) | P1 | T65 (C) |

Tổng: **27 cấu hình** (29 nếu E8 thêm mức E1 cao nhất). Smoke dataset nhỏ chạy trước mỗi nhóm (S608). Không gọi số transaction là số event: mỗi giao dịch SUCCESS còn tạo hai ledger event và có thể có replay (S608); mọi báo cáo ghi rõ "giao dịch" hay "event".

**Quy tắc "mức cao nhất chạy ổn định"** (S595, S630) cho E2–E7: thử theo thang 1.000.000, 500.000, 250.000, 100.000; mức được chọn là mức lớn nhất mà **cả** warm-up và ba lần đo hoàn tất không lỗi, không bị kill vì hết bộ nhớ, và đúng kết quả (B6). Mức bị bỏ phải có dòng trong `not_run.csv` (B7) với nguyên nhân bằng số đo. Mức 100.000 là thấp nhất; dưới đó ghi "không chạy được" thay vì đổi thang. Mọi cấu hình trong **một thí nghiệm** dùng cùng một mức.

**Dùng chung lần chạy cấu hình gốc.** Cấu hình gốc ở 1M xuất hiện trong E1, E2, E3, E5 và E6 (nếu giá trị mức đó đúng bằng cấu hình gốc). Một lần chạy được phép dùng cho nhiều thí nghiệm **chỉ khi** `config_hash` và input manifest giống hệt, khối lượng đo là cùng một lần chạy pipeline và hàng kết quả ghi `shared_with`. Cách này giảm tối đa 4 cấu hình (tổng còn 23 cấu hình, tức 92 lần chạy, xem mục Ước lượng). Mặc định không dùng chung; T51 chỉ bật khi người chạy yêu cầu, để mỗi thí nghiệm tự đủ khi đọc riêng.

### B2. Quy trình một cấu hình (S614)

1. **Điều kiện máy.** Chỉ chạy khi máy rảnh; Airflow và Metabase tắt (profile `benchmark`: `airflow_enabled = false`, `metabase_enabled = false`) và hàng kết quả ghi `services_stopped` (S614). Ghi `free -m`, `df` ổ dữ liệu và số container đang chạy trước mỗi lần. Phiên Claude/Spark/Docker khác chạy cùng lúc làm lần đo bị coi là nhiễu: người chạy ghi `noise_note`; ngưỡng tự động loại: Q9.
2. **Trình tự.** Với mỗi cấu hình: 1 lần **warm-up** (`phase = warmup`, `repetition = 0`), sau đó **≥ 3 lần đo** (`phase = measured`, `repetition = 1..n`). Các cấu hình của một thí nghiệm chạy **xen kẽ theo vòng lặp** (vòng 1 của mọi cấu hình, rồi vòng 2, ...) để trôi nhiệt hoặc trôi cache không dồn vào một cấu hình; thứ tự trong vòng là hoán vị xác định bằng `seed`, ghi `run_order`.
3. **Cold/warm cache.** Warm-up là lần đầu sau khi khởi động lại JVM/container nên ghi `cache_state = cold`; ba lần đo ghi `cache_state = warm`. Không xóa page cache của hệ điều hành (cần quyền root trong WSL); ghi `os_cache_dropped = false`. Số cold không vào trung vị.
4. **Cô lập trạng thái.** Mỗi lần chạy có `run_id` và tiền tố object riêng dưới bucket benchmark (ADR-0002 mục 5, `paths.benchmark_prefix`), topic Kafka được tạo mới cho từng giá trị partition của E2, dọn sau lần chạy. Không dùng lại đầu ra của lần trước làm đầu vào.
5. **Số lần.** Mặc định `repetitions = 3`, `warmup = 1`; cả hai là tham số. Nếu một lần đo bị loại (B6) thì chạy bù tối đa 2 lần cho mỗi cấu hình; nếu vẫn thiếu thì báo `n_counted < 3` và giữ nguyên số liệu hiện có (không điền).
6. **Tổng hợp.** Báo **trung vị và khoảng min–max** trên các lần đếm được (B6) của mỗi cấu hình, kèm `n_counted`. Trung vị của số chẵn lần là trung bình hai giá trị giữa. Không báo trung bình thay trung vị, không báo độ lệch chuẩn khi `n_counted < 5`.

### B3. Chỉ số và cách đo (S616–S626)

- **Thời gian sinh dữ liệu** tách khỏi ingest và xử lý Spark (S618): ghi ở bản ghi dataset (`generation_seconds`, `generator_peak_rss_mb`), không cộng vào `wall_seconds` của lần đo.
- **Ingest throughput** = số unique source event đã commit / thời gian ingest đo được; báo thêm `physical_replay_count` và tốc độ phát mục tiêu của producer (`producer.rate`, 0 là không giới hạn) (S619).
- **Batch throughput** = số input business record xử lý / `wall_seconds`; hàng ghi `throughput_entity` (giao dịch, ledger entry, sao kê, match key) và `throughput_scope` (khoảng ngày, giai đoạn) (S620).
- **Độ trễ truyền dữ liệu** = `ingested_at` − `kafka_timestamp` (record timestamp do producer gán khi gửi thật). Tuyệt đối không dùng `emitted_at` hay `event_time` (S621, S689). Độ trễ tới mart = thời điểm mart published − `kafka_timestamp` lớn nhất của ngày đó, báo riêng (S621). Báo p50/p95/p99 chỉ khi đủ mẫu; mặc định đủ mẫu là p50, p95 khi ≥ 100 mẫu và p99 khi ≥ 1.000 mẫu (tham số; số khác thì ghi rỗng và `latency_samples`). Ghi rõ các container dùng chung đồng hồ máy chủ (`clock_source = host_shared`) (S622).
- **Spark** từ event log (`event_log_enabled = true`, `event_log_dir`): input/output rows, shuffle read/write, spill (bộ nhớ và đĩa), thời lượng stage/task, tỷ lệ task max/median, số task, và đường dẫn tham chiếu event log (`spark_event_log_ref`) (S623).
- **Lưu trữ**: số object, byte theo tầng (Raw, Bronze, Silver, Gold, quarantine, benchmark), phân bố kích thước file (min, p50, p95, max) và số file "tiny" (nhỏ hơn `benchmark.tiny_file_bytes`, mặc định 1.048.576 byte; C thêm khóa này vào `config/`) (S624).
- **CPU/RAM** theo **chuỗi thời gian**, không chụp một ảnh cuối job (S625): lấy mẫu mỗi `benchmark.sample_interval_seconds` (mặc định 1) cho (a) cả máy (CPU%, RAM đã dùng, swap), (b) từng container (cgroup/`docker stats`: Kafka, MinIO, Postgres, Spark), (c) tiến trình `n06 benchmark` và con của nó. Ghi chuỗi vào `timeseries/<run_id>.csv`; hàng kết quả chỉ giữ `peak` và `avg`. Peak RAM của một lần = max theo thời gian của tổng RSS các tiến trình thuộc phạm vi đo; phạm vi ghi ở `ram_scope` (host tổng, hoặc tiến trình đo) để E8 so RAM của Spark và DuckDB cùng một phạm vi.
- **Tính đúng** trong mọi lần đo (S626): unique key count, tổng tiền theo từng currency (số nguyên `amount_minor`), số lượng theo trạng thái đối soát so với ground truth, và `semantic_hash` của đầu ra (loại `run_id` và timestamp, như `src/n06/storage/commit.py`). So với ground truth bằng **tiến trình con gọi test harness** như `n06 validate` đang làm (`src/n06/cli/validate.py`), không import `tests/` và không đọc ground truth trong `src/` (CLAUDE.md, mục Quy tắc cứng). E4 và E8 là truy vấn trên bảng đã chuẩn hóa, nên tính đúng của chúng là: cùng số dòng và cùng tổng tiền với bản tham chiếu (B5).

### B4. Từng thí nghiệm: khối lượng đo, hằng số, ghi chú

| Mã | Khối lượng đo | Giữ nguyên | Chỉ số chính (S597–S606) | Ghi chú |
|---|---|---|---|---|
| E1 | `ingest` rồi `process-day` toàn bộ khoảng ngày; hai giai đoạn đo riêng và tổng | seed, nội dung, cấu hình gốc | thời gian, throughput, peak CPU/RAM, dung lượng theo tầng | Một dataset sinh một lần cho mỗi quy mô (hash trong input manifest) |
| E2 | `ingest` Raw/Bronze, topic mới, producer `rate = 0` | dataset 1M, key `transaction_id` | throughput ingest, độ trễ p50/p95/p99 | Producer, Kafka, Spark chung một máy nên chạm trần CPU/IO sớm; ghi trong lời lý giải |
| E3 | Silver + đối soát + số dư, lưới 3 × 2 | dataset đã ingest, cấu hình gốc | thời gian stage, shuffle read/write, spill, số task | Hai yếu tố: ghi đủ 6 tổ hợp, không gộp |
| E4 | ba truy vấn cố định (Q-A, Q-B, Q-C bên dưới), ghi và đọc | cùng nội dung Silver giao dịch | dung lượng, thời gian ghi, thời gian từng truy vấn | Đường ghi riêng của `benchmark` vào `paths.benchmark_prefix`; không thêm JSON vào pipeline chính; không so JSON nhiều metadata với Gold ít cột (S611) |
| E5 | `process-day` | dataset đã ingest, `driver_memory` | speedup = T(1)/T(N), hiệu suất = speedup/N | `local[N]`; Kafka, MinIO, Postgres dùng chung CPU nên speedup dưới tuyến tính là kỳ vọng |
| E6 | ingest, và tính số dư lũy kế nhỏ nhất theo tài khoản/ngày bằng window | seed, quy mô | bản ghi mỗi partition Kafka, tỷ lệ task max/median, thời gian stage | Phép số dư lũy kế là khối lượng đo, **không** là cổng chất lượng (S612). Dataset lệch dùng profile `bench-skew` (`HOT_ACCOUNT_V1`, 10 tài khoản nóng, 5000 basis point) |
| E7 | ghi Silver theo `business_date` rồi đọc lại toàn bộ | nội dung Silver | số file, phân bố kích thước file, thời gian ghi, thời gian đọc, thời gian liệt kê | Không sửa mã pipeline chính; hai chế độ ghi là tham số của đường ghi của `benchmark` |
| E8 | ba truy vấn của E4 và phép full outer join đối soát theo match key | cùng bản Silver Parquet đã commit, cùng đầu ra kỳ vọng | thời gian từng truy vấn, peak RAM, kết quả có khớp | Xem B5 |

**Ba truy vấn cố định của E4/E8** (đặc tả chỉ ghi "đếm toàn bộ, tổng hợp trên hai cột, lọc một `business_date`", S602; cột cụ thể xem Q4). Đề xuất trên Silver giao dịch:

- Q-A: `SELECT COUNT(*) FROM silver_transactions`
- Q-B: `SELECT channel, SUM(amount_minor) FROM silver_transactions GROUP BY channel` (hai cột: `channel`, `amount_minor`)
- Q-C: `SELECT COUNT(*), SUM(amount_minor) FROM silver_transactions WHERE business_date = <ngày giữa khoảng>` (ngày là tham số)

Tiền luôn là số nguyên; tổng dùng kiểu thập phân đủ rộng hoặc BIGINT sau kiểm miền (CLAUDE.md), không float.

### B5. E8: Spark và DuckDB công bằng và có thể kiểm

- Chỉ so **bộ máy truy vấn trên cùng dữ liệu đã chuẩn hóa**; không thay pipeline chính bằng DuckDB; không so DuckDB đọc Silver với Spark chạy cả ingest (S610).
- Hai bộ máy đọc **cùng tệp Parquet Silver đã commit** (cùng sha256 từng tệp, ghi trong hàng kết quả). Điều kiện: DuckDB đọc bản sao cục bộ vì extension `httpfs` phải tải về lúc chạy, trái S187 (*"Không tải dependency động từ Internet mỗi lần chạy job"*). Spark đọc từ cùng bản sao cục bộ (`file://`) cho phép so thuần bộ máy; có thể thêm hàng phụ Spark đọc qua S3A để thấy chi phí I/O qua MinIO (Q5).
- Ghi riêng ba thời gian cho mỗi bộ máy: `startup_seconds` (tạo `SparkSession`/mở kết nối DuckDB), `query_seconds` của từng truy vấn (sau khởi tạo, ở trạng thái warm), và `end_to_end_seconds` (gồm khởi tạo). Giả thuyết E8 nói rõ DuckDB nhanh nhờ "không có chi phí khởi tạo và lập lịch phân tán" (S606), nên khởi tạo là một phần của câu trả lời và phải đo riêng.
- Số luồng: `duckdb_threads` = số core của cấu hình gốc Spark; ghi `duckdb_memory_limit` (mặc định của DuckDB nếu không đặt, ghi giá trị thực). Peak RAM theo cùng `ram_scope`.
- **Điều kiện tính số đo**: kết quả hai bộ máy phải giống nhau (cùng số dòng, cùng tổng tiền) cho cả ba truy vấn và phép join; số dòng của join, số khóa mỗi trạng thái đối soát cũng phải khớp. Khác thì lần đó là `invalid` (B6) và không vào bảng hiệu năng (S610, S626).
- Báo đúng kết quả kể cả khi trái giả thuyết; kết luận chỉ áp dụng cho môi trường một máy đã ghi (S610, S632).
- **Phụ thuộc mới: DuckDB.** Chỉ là dependency của lệnh `benchmark` (S610). Đề xuất khai báo trong nhóm tùy chọn `benchmark` của `pyproject.toml` (`duckdb==<phiên bản>`), ghim trong `uv.lock`, ghi vào `docs/compatibility-matrix.md`. **Chưa sửa** `pyproject.toml`, `uv.lock` hay compatibility matrix trong PR này. Bảng ứng viên (nguồn `https://pypi.org/pypi/duckdb/json`, truy vấn ngày 2026-11-27; `requires_python` của gói: 1.5.6 là `>=3.10.0`, 2.0.x là `>=3.11.0`; không bản nào bị yanked; dự án dùng Python `>=3.11,<3.12`):

| Phiên bản | Ngày phát hành lần đầu (PyPI) | Wheel cp311 manylinux x86_64 | sha256 của wheel |
|---|---|---|---|
| **1.5.6 (đề xuất)** | 2026-09-28 | `duckdb-1.5.6-cp311-cp311-manylinux_2_26_x86_64.manylinux_2_28_x86_64.whl` (21.537.771 byte) | `73b108c04c932b36c2fa4e41110cc1c3c8cd510eb49f065f92d050be8e6929fd` |
| 2.0.0 | 2026-10-21 | `duckdb-2.0.0-cp311-cp311-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl` (24.351.483 byte) | `5a3601121c7cc50d0b70412995634682fafcb58a0156d5d663dfd0b3bbe30a33` |
| 2.0.1 | 2026-11-20 | `duckdb-2.0.1-cp311-cp311-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl` (24.925.733 byte) | `eb32b389d99d14cbbec4c110f58237c77eacefc65ef6f737619f413c97a91a77` |

  Lý do đề xuất 1.5.6: bản mới nhất của dòng 1.x, đã phát hành gần hai tháng; 2.0.0 mới ra hơn một tháng và 2.0.1 mới ra một tuần nên ít thời gian thử. Chọn bản nào cũng được nếu người duyệt muốn; hai dòng khác nhau có thể cho kế hoạch truy vấn khác nhau nên số đo chỉ so sánh trong cùng một phiên bản đã ghim. **Chưa cài thử** các wheel này, chưa kiểm đọc Parquet do Spark 3.5.8 ghi (nén snappy mặc định), chưa kiểm RAM tối đa trên máy này; phải kiểm trước khi T65 bắt đầu.

### B6. Lần chạy lỗi, lần sai kết quả, warm-up: quy tắc đếm (S626, S630)

Mỗi lần chạy có đúng một `status` và cờ `counted`:

| `phase` | `status` | Ý nghĩa | `counted` | Vào trung vị/min–max |
|---|---|---|---|---|
| warmup | bất kỳ | lần khởi động, cold | false | không |
| measured | `ok`, `correctness_ok = true` | lần đo hợp lệ | **true** | có |
| measured | `error` | lỗi chạy (ngoại lệ, kill, OOM, hết đĩa, timeout) | false | không |
| measured | `invalid` | chạy xong nhưng sai kết quả (B3, B5) | false | không |

- Lần sai kết quả **không bao giờ** là số đo hiệu năng (S626). Hàng của nó vẫn nằm trong `runs.csv` với `counted = false`, `status`, `exclusion_reason` (`error_type` hoặc mô tả chỗ sai) và các giá trị đúng/sai đã đo, để lỗi có dấu vết.
- Bảng tổng hợp và biểu đồ chỉ dùng hàng `counted = true`. Mỗi dòng tổng hợp ghi `n_total`, `n_counted`, `n_error`, `n_invalid`, `n_warmup`.
- Tính đúng được kiểm **trên chính lần chạy được đo**, trước khi ghi `counted`; không kiểm lại sau trên bản sao.
- Chọn lọc số liệu bị cấm: người chạy không được bỏ một lần đo hợp lệ vì "số xấu". Lý do loại duy nhất là `error`, `invalid` hoặc `noise` (B2 mục 1, Q9), đều có ghi trong hàng.

### B7. Mức không chạy được (S630)

- Nếu E1 5.000.000 hoặc mức khác không chạy được: báo mức cao nhất đã chạy và **nguyên nhân bằng số đo**, không bằng ước lượng (S630). Mỗi mức không chạy ghi một hàng trong `not_run.csv` (và mảng `not_run` trong JSON): `experiment`, `level`, `reason_code` (`DISK_FULL`, `OOM_KILLED`, `JVM_OOM`, `TIMEOUT`, `GUARD_REJECTED`, `OTHER`), `measured_evidence_json`, `highest_level_run`.
- Bằng chứng chấp nhận được là số đo ở thời điểm xảy ra: `df` của ổ dữ liệu trước/sau và byte thực tế đã ghi trước khi hết chỗ; peak RAM và swap từ chuỗi thời gian; mã thoát 137 hoặc `OOMKilled` từ `docker inspect`; dòng `java.lang.OutOfMemoryError` trong log Spark (không cần quyền root để đọc `dmesg`); thời gian chạy khi chạm timeout.
- **Cấm** ghi số ước lượng hay ngoại suy vào cột kết quả hoặc biểu đồ (`estimated` không tồn tại như một cột). Nếu `n06 generate` hoặc `n06 doctor --transactions` từ chối vì guard dung lượng (mã thoát 16), đó là `GUARD_REJECTED`; hàng ghi số đo `free_bytes_measured` và `generator_bytes_per_tx` đã đo, còn phép nhân của guard được nêu trong ghi chú và gắn nhãn "tính từ B/giao dịch đã đo ở quy mô nhỏ hơn, không phải số đo ở mức này".
- Mức 5.000.000 chỉ được thử khi đã có số đo thật `pipeline_bytes_per_tx` (hiện **chưa đo**, `config/default.toml` mục `[limits]`) từ các mức nhỏ hơn, hoặc khi người chạy chấp nhận thử và để lỗi hết đĩa tự làm bằng chứng.

### B8. Định dạng file kết quả (S628–S630)

Lệnh `n06 benchmark` là nơi **duy nhất** ghi vào `reports/` (CLAUDE.md; `reports/README.md`). Thư mục mỗi phiên: `reports/benchmark/<batch_id>/` (`paths.reports_dir`; `batch_id` = UTC `YYYYMMDDTHHMMSSZ` của lúc bắt đầu):

| Tệp | Nội dung |
|---|---|
| `runs.csv`, `runs.json` | một hàng mỗi lần chạy (kể cả warm-up, lỗi, sai); cùng tên trường |
| `metrics.csv` | dạng dài: `run_id, metric, value, unit, scope` cho mọi metric theo B3 không nằm trong `runs.csv` |
| `summary.csv`, `summary.json` | một hàng mỗi cấu hình: `n_*`, trung vị, min, max của từng metric chính, chỉ trên `counted = true` |
| `not_run.csv` | mức không chạy được (B7) |
| `timeseries/<run_id>.csv` | chuỗi CPU/RAM/swap/container theo thời gian (B3) |
| `environment.json` | thông số máy và container đo lúc bắt đầu phiên |
| `charts/*.png` | biểu đồ vẽ từ `summary.csv` (S628 "cùng biểu đồ"); mỗi biểu đồ ghi `batch_id` và `commit_sha` |

Quy tắc mã hóa: UTF-8, dấu phẩy, hàng tiêu đề, timestamp UTC dạng `...Z`, số dùng dấu chấm thập phân, giá trị rỗng là null, cột dạng JSON ghi JSON gọn có khóa sắp xếp, tiền là số nguyên `amount_minor`, dung lượng là byte (không KB/GB trong cột).

**Cột của `runs.csv`** (S630 *"mã thí nghiệm, giá trị biến, lần lặp, CPU/RAM/OS/container limits, phiên bản, profile, commit SHA, input manifest, seed, as_of_time, config hash và lỗi nếu có"*):

| Nhóm | Cột |
|---|---|
| Định danh | `batch_id`, `run_id`, `experiment`, `config_id`, `variable_name`, `variable_value`, `variable2_name`, `variable2_value` (E3, E6), `repetition`, `run_order`, `phase`, `cache_state`, `shared_with` |
| Kết quả đếm | `status`, `counted`, `exclusion_reason`, `error_type`, `error_message`, `exit_code` |
| Thời gian | `started_at`, `ended_at`, `wall_seconds`, `phase_seconds_json`, `startup_seconds`, `end_to_end_seconds` (E8) |
| Máy | `host_os`, `kernel`, `cpu_model`, `cpu_logical`, `ram_total_mb`, `swap_total_mb`, `disk_free_bytes_start`, `container_limits_json` (cpus/memory từng container, "none" nếu không giới hạn), `services_stopped`, `os_cache_dropped`, `clock_source`, `noise_note` |
| Phiên bản | `n06_version`, `python`, `spark`, `hadoop`, `scala`, `java`, `kafka`, `minio_image`, `postgres_image`, `duckdb` (rỗng nếu không dùng) |
| Nguồn gốc | `profile`, `commit_sha`, `git_dirty`, `dataset_id`, `transaction_count`, `seed`, `input_manifest_path`, `input_manifest_sha256`, `source_batch_id`, `as_of_time`, `config_hash`, `config_json` (cấu hình hiệu lực, **không** có secret) |
| Tính đúng | `correctness_ok`, `unique_key_count`, `totals_by_currency_json`, `status_counts_json`, `semantic_hash`, `ground_truth_match` |
| Kết quả chính | `throughput_value`, `throughput_unit`, `throughput_entity`, `throughput_scope`, `cpu_peak_pct`, `cpu_avg_pct`, `ram_peak_mb`, `ram_avg_mb`, `ram_scope`, `swap_peak_mb` |

- `config_hash` = SHA-256 của JSON chuẩn tắc (khóa sắp xếp) của cấu hình hiệu lực sau khi gộp bốn tầng (`config/README.md`) và đã loại secret. `commit_sha` là mã commit đầy đủ; cây làm việc bẩn (`git_dirty = true`) bị từ chối trừ khi có `--allow-dirty` và hàng ghi cờ này.
- `phase_seconds_json` giữ thời gian từng giai đoạn (ingest, silver, recon, balances, publish) để E1 tách ingest và `process-day` (S599).
- `metrics.csv` dùng cho metric riêng thí nghiệm (độ trễ p50/p95/p99, spill, tỷ lệ task max/median, file size distribution, số tiny file, byte theo tầng, thời gian từng truy vấn).
- JSON giữ cùng tên trường như CSV, `*_json` được mở thành đối tượng lồng nhau.

### B9. Giao diện `n06 benchmark` (T51)

ADR không thiết kế toàn bộ lệnh; T51 phải có tối thiểu các tham số sau (S613: đổi mức tải bằng tham số, không sửa mã): `--experiment`, `--levels`, `--repetitions` (mặc định 3), `--warmup` (mặc định 1), `--seed`, `--as-of-time`, `--profile` (mặc định `benchmark`), `--output-dir`, `--allow-dirty`, `--share-baseline`. Mọi endpoint, bucket, topic, ngày, seed, quy mô, partition, executor lấy từ `config/` hoặc tham số (CLAUDE.md: không hardcode). Chạy thử E1 ở mức smoke phải ra file đúng định dạng này (TASKS T51).

## Ước lượng thời gian chạy toàn ma trận (để lập lịch)

**Kết luận trước: thời gian chạy ở mọi tầng pipeline chưa đo.** Repo chỉ có số đo thật cho **bộ sinh dữ liệu** và **dung lượng**. Phần còn lại ghi "chưa đo" và chỉ có thể lập lịch theo công thức; T51 chạy smoke ở 100.000 giao dịch (một lần đo thật) để hiệu chỉnh trước khi chốt lịch.

**Nguồn số đo có sẵn:**

- G: `docs/measurements/generator-bytes-per-tx.md` (commit `93e4d66`, `generator.demo-faults.json`): 20.000 giao dịch 23,9 s, RSS 61 MB, 3.980,0 B/giao dịch (79,6 MB); 100.000 giao dịch 118,6 s, RSS 55 MB, 3.992,8 B/giao dịch (399,3 MB). Không đo 1.000.000 và 5.000.000 (tài liệu ghi "mức 1.000.000 là ngoại suy tuyến tính").
- F: `docs/adr/0012-driver-postgres-va-ghi-object-minio.md`, mục "Số đo file sao kê": 20.000 giao dịch 152 file 7,3 MB (364,8 B/dòng); 50.000 giao dịch 152 file 18,2 MB. Chỉ là dung lượng, không có thời gian ingest.
- L: `config/default.toml` mục `[limits]`: `min_free_disk_gb_reserve = 8`; `pipeline_bytes_per_tx` **chưa đo**.
- D: lệnh `df -h /mnt/d` ngày 2026-11-27: còn 36 GB.

**Số lượng lần chạy** (mặc định 1 warm-up + 3 lần đo = 4 lần/cấu hình; chưa tính chạy bù tối đa 2 lần mỗi cấu hình):

| Quy mô | Cấu hình | Lần chạy (×4) | Gồm |
|---|---:|---:|---|
| 100.000 | 3 | 12 | E1 (1), E8 (2) |
| 1.000.000 | 23 | 92 | E1 (1), E2 (3), E3 (6), E4 (2), E5 (3), E6 (4), E7 (2), E8 (2) |
| 5.000.000 | 1 | 4 | E1 (1); thêm E8 (2 cấu hình, 8 lần) nếu E1 5M chạy được |
| **Tổng** | **27** | **108** | 81 lần đo đếm được tối thiểu; tối đa 162 nếu mọi cấu hình dùng hết 2 lần chạy bù |

(E2–E7 ở 1.000.000 là mức mặc định; nếu hạ mức theo thang B1 thì chuyển số cấu hình sang dòng quy mô thấp hơn. Nếu dùng chung lần chạy cấu hình gốc (B1) thì tổng giảm tối đa 4 cấu hình: 23 cấu hình và 92 lần chạy trên toàn ma trận, không phải riêng mức 1.000.000.)

**Thời gian sinh dữ liệu** (sinh một lần, dùng lại cho mọi lặp):

| Dataset | Dùng cho | Thời gian | Trạng thái |
|---|---|---:|---|
| 100.000 giao dịch (`demo-faults`) | E1, E8 | 118,6 s | **đo** (G) |
| 1.000.000 giao dịch đều | E1–E8 | khoảng 1.186 s (19,8 phút) | **chưa đo**; ngoại suy tuyến tính từ 118,6 s ở 100.000 (hai điểm đo 20.000, 100.000 cho 837 và 843 giao dịch/giây) |
| 1.000.000 giao dịch lệch (`bench-skew`) | E6 | khoảng 1.186 s | **chưa đo** (cấu hình khác; cùng ngoại suy) |
| 5.000.000 giao dịch | E1 | khoảng 5.930 s (98,8 phút) | **chưa đo**; ngoại suy; bộ sắp xếp ngoài có thể không tuyến tính |
| Tổng | | khoảng 8.421 s (2,3 giờ) | trong đó **118,6 s là số đo**, còn lại ngoại suy |

**Thời gian thực thi pipeline và truy vấn:** tất cả **chưa đo** (publish Kafka, streaming ingest, Silver, đối soát, số dư, ghi mart, E4 ghi và truy vấn, E7, E8, độ trễ lấy mẫu CPU/RAM). Công thức lập lịch:

`T_tổng = T_sinh + Σ(cấu hình) 4 × (t_chuẩn_bị + t_chạy) + T_dọn + T_chạy_bù`

Phép tính số học trên số lần chạy ở 1.000.000 giao dịch (92 lần), **chỉ là bảng nhạy cảm giả định để lập lịch, không phải số đo**:

| Nếu mỗi lần chạy 1M tốn (giả định) | Tổng 92 lần chạy ở mức 1.000.000 (không dùng chung cấu hình gốc) |
|---:|---:|
| 10 phút | 15,3 giờ |
| 20 phút | 30,7 giờ |
| 30 phút | 46,0 giờ |
| 60 phút | 92,0 giờ |

Mức 100.000 (12 lần) và 5.000.000 (4 lần) tính riêng sau khi có `t_chạy` của 100.000 và 1.000.000. Lịch nên chia theo thí nghiệm để mỗi phiên nằm gọn trong một buổi máy rảnh (B2 mục 1). Thứ tự gợi ý: smoke nhỏ (hiệu chỉnh), E1 100k và 1M, E3, E4, E5, E2 (T52), rồi E8 (cần phụ thuộc DuckDB và Silver Parquet), E6, E7, cuối cùng thử E1 5M vì chiếm nhiều đĩa nhất.

**Dung lượng:** sinh dữ liệu chiếm 399,3 MB ở 100.000 (**đo**), khoảng 4,0 GB ở 1.000.000 và khoảng 20,0 GB ở 5.000.000 (**ngoại suy** từ 3.992,8 B/giao dịch, chưa đo ở hai mức này), chưa tính phần pipeline (Kafka, MinIO các tầng, Postgres, Spark spill; **chưa đo**). Chỗ trống đo được: 36 GB ở ổ D, trừ dự trữ 8 GB còn khoảng 28 GB. Hệ quả cho lịch: không giữ đồng thời nhiều dataset lớn; sinh, đo, xóa lần lượt; 5.000.000 chỉ tối đa 8 GB dư cho phần pipeline sau khi sinh dữ liệu, mà phần pipeline chưa có số đo, nên E1 5.000.000 là mức rủi ro cao nhất và phải theo quy tắc B7.

## Câu hỏi mở (cần người duyệt chốt; đề xuất không phải quyết định)

**Q1. Cấu hình gốc (baseline) của Spark.** S595: *"Cấu hình gốc (baseline) được chốt một lần sau M4 và dùng chung cho mọi thí nghiệm."* Hai tệp trong repo khác nhau: `config/default.toml` (`local[2]`, driver 2g, `kafka.num_partitions = 3`) và `config/benchmark.toml` (`local[4]`, driver 4g, `kafka.num_partitions = 6`). Đề xuất: cấu hình gốc là `config/benchmark.toml` đang có, nhưng chốt sau một lần chạy thử 1.000.000 giao dịch đo peak RAM; nếu tổng RAM đã dùng của máy vượt 85% `ram_total_mb` hoặc có swap, hạ `driver_memory` (và ghi) trước khi chốt. Ghi `config_hash` của cấu hình gốc vào `environment.json` và không đổi sau đó. Ghi chú: ở chế độ local, `executor_memory` không có tác dụng riêng (executor nằm trong JVM của driver) theo tài liệu Spark; chưa kiểm trong repo.

**Q2. Ý nghĩa "lặp ít nhất 3 lần" và warm-up.** S614: *"Mỗi cấu hình lặp ít nhất 3 lần nếu tài nguyên cho phép [...] Tách warm-up, cold/warm cache và lần lỗi khỏi lần đo ổn định."* Không nói warm-up có tính vào 3 lần không. Đề xuất: không (1 warm-up cộng ≥ 3 lần đo), nên 4 lần/cấu hình tối thiểu. Nếu tài nguyên không cho phép, người chạy ghi `n_counted < 3` và lý do.

**Q3. Số lần chạy bù khi lần đo bị loại.** Đặc tả không nói. Đề xuất tối đa 2 lần mỗi cấu hình (B2 mục 5); quá đó báo `n_counted < 3`.

**Q4. Cột và truy vấn cụ thể của "ba truy vấn cố định".** S602 chỉ ghi: *"đếm toàn bộ, tổng hợp trên hai cột, lọc một `business_date`"*. Chưa có lược đồ Silver giao dịch (ADR-0004 chỉ phủ Raw và Bronze). Đề xuất Q-A, Q-B, Q-C ở B4; xác nhận khi lược đồ Silver được chốt.

**Q5. E8: Spark đọc từ đâu.** S610: *"cùng bản Silver Parquet đã commit"*; S606 cấu hình gốc Spark đọc qua S3A/MinIO. DuckDB không thể dùng S3 mà không tải extension lúc chạy (trái S187). Đề xuất: cả hai đọc cùng bản sao cục bộ làm hàng chính, thêm một hàng phụ "Spark qua S3A" (báo riêng, không thay hàng chính). Người duyệt cho biết có chấp nhận cách này hay muốn đóng gói extension `httpfs` vào image (cần kiểm tải offline, chưa kiểm).

**Q6. Nén của E4.** S602 yêu cầu "cùng nội dung"; không nói codec. Đề xuất: JSON không nén và Parquet với codec mặc định của Spark 3.5.8 (snappy), ghi `codec` vào hàng; chạy thêm hàng phụ JSON nén gzip nếu muốn tách ảnh hưởng nén (không bắt buộc).

**Q7. Mức "đủ mẫu" của p50/p95/p99.** S622: *"Báo p50/p95/p99 khi có đủ mẫu"*. Đề xuất ngưỡng 100 và 1.000 mẫu (B3), đặt thành tham số.

**Q8. Vùng đo RAM của DuckDB và Spark.** S606 ghi "peak RAM". Spark local chạy trong JVM, DuckDB trong tiến trình Python; so tổng RSS tiến trình đo (không cả máy) để công bằng. Đề xuất `ram_scope = process_tree` cho E8 và `host` cho E1–E7.

**Q9. Ngưỡng nhiễu tự động.** Máy dùng chung với các phiên khác (RAM khả dụng đã chỉ 3.601 MB lúc kiểm). Đề xuất: ghi swap và RAM khả dụng trước mỗi lần; lần có `swap_peak_mb > 0` hoặc RAM khả dụng đầu lần dưới `driver_memory + 2 GB` được đánh `noise` và chạy lại (không vào `counted`). Ngưỡng cần người duyệt chốt vì phụ thuộc cấu hình gốc (Q1).

**Q10. Biểu đồ.** S628 yêu cầu "cùng biểu đồ" nhưng không nói công cụ. Đề xuất sinh PNG từ `summary.csv` bằng matplotlib (phụ thuộc mới, cần chốt ở T51/T53) hoặc dùng công cụ ngoài không là phụ thuộc runtime; chưa chọn.

## Phương án đã cân nhắc

- **Một file CSV rộng chứa mọi metric.** Loại: metric khác nhau theo thí nghiệm nên bảng rỗng nhiều và thay đổi khi thêm metric; chọn `runs.csv` cho cột cố định của S630 và `metrics.csv` dạng dài cho phần còn lại.
- **Tính warm-up vào ba lần đo.** Loại: S614 yêu cầu tách warm-up khỏi lần đo ổn định.
- **Ước lượng thay số đo khi 5.000.000 không chạy.** Loại: S630 cấm; chỉ báo mức cao nhất và nguyên nhân đo được.
- **Bỏ hàng lỗi/sai khỏi file kết quả.** Loại: mất dấu vết; giữ hàng nhưng `counted = false` (B6).
- **Chạy E8 với Spark tính cả ingest.** Loại: S610 cấm.
- **Để DuckDB đọc trực tiếp trên MinIO qua `httpfs`.** Chưa chọn (Q5): cần tải extension lúc chạy, trái S187.
- **Chạy tất cả cấu hình ở 5.000.000.** Loại: S595 chỉ cho E1 ở 5M; E2–E7 ở 1M hoặc mức cao nhất ổn định.

## Hệ quả

- **T51** hiện thực B2, B3, B6, B7, B8, B9: lấy mẫu CPU/RAM theo chuỗi thời gian, đọc Spark event log, ghi `runs.*`, `metrics.csv`, `summary.*`, `not_run.csv`, `timeseries/`, `environment.json`; gọi harness ground truth qua tiến trình con. Thêm khóa `benchmark.tiny_file_bytes`, `benchmark.sample_interval_seconds` vào `config/` (C).
- **T52, T63, T64, T65** chạy đúng ma trận B1; mỗi cấu hình theo B2; T65 cần phụ thuộc DuckDB (B5) và Silver Parquet đã commit.
- **Phụ thuộc DuckDB:** PR riêng sửa `pyproject.toml`, `uv.lock`, `docs/compatibility-matrix.md` sau khi người duyệt chốt phiên bản và chạy kiểm B5 (cài thử, đọc Parquet Spark, RAM). Không thuộc PR này.
- **`config/benchmark.toml`** có thể phải đổi sau khi Q1 được chốt (thuộc C).
- **Báo cáo (T53):** bảng kết quả chỉ từ `summary.csv`; mục "giới hạn" nêu E8 và mọi kết quả chỉ áp dụng cho máy một node đã ghi, dữ liệu mô phỏng (S632).
- **`PROGRESS.md`:** sau khi Accepted, thêm dòng ghi quyết định cấu hình gốc và `config_hash` của nó.
