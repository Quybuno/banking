# src/n06/ingestion/

**Mục đích:** Lệnh `stream` (một query foreachBatch ghi Raw + Bronze), `wait-ingestion`, `ingest-statements`.

**Người sở hữu:** A (Mạnh); giao thức commit do B duyệt

**Đặc tả liên quan (docs/spec.md):** Phần I mục 6, 8.1 (streaming)

**Quy tắc:** Ghi theo đường dẫn xác định bởi batch_id; upsert `ingest_batches` sau khi mọi output đã ghi.

## `n06 stream`

```bash
docker compose --profile core build spark && docker compose --profile core up -d --wait
n06 stream --profile demo                     # tiến trình dài hạn
n06 stream --profile demo --available-now     # xử lý hết dữ liệu hiện có rồi dừng
```

**Chạy ở đâu.** Lệnh chạy trên host chỉ kiểm cấu hình rồi mở một container dùng một lần của service `spark` (`docker compose run --rm`), `spark-submit` chế độ local (`spark.master`, `spark.driver_memory`) trong mạng `n06_network`. Profile phải dùng địa chỉ trong mạng Docker (`default`, `demo`, `benchmark`); `test` trỏ `localhost` nên bị từ chối với mã `10`.

**Đóng gói.** `src/`, `config/`, `contracts/`, `sql/` được **gắn chỉ đọc** vào `/opt/n06` lúc chạy; image không chứa gói `n06` (ADR-0012 không quyết việc này). Yêu cầu của đặc tả "một image job chứa ... gói `n06`" (Phần I mục 2) còn mở, cần quyết trước khi Airflow gọi job. Biến `N06*` được chuyển vào container bằng tên, giá trị không nằm trong argv.

**Checkpoint.** `s3a://<object_store.bucket>/<paths.checkpoints_prefix>/<query_name>/` trên MinIO (volume `n06_minio_data`). Không xóa khi khởi động lại. Mất checkpoint: đổi `kafka.query_name` rồi chạy lại để nạp từ offset đầu (đặc tả I.8.1 bước 6).

**Ghi gì.** Raw: `<raw_prefix>/<query_name>/ingestion_date=<d>/batch_id=<id>/`; Bronze: cùng cấu trúc dưới `<bronze_prefix>`, thêm `entity=transaction|ledger_entry|parse_error` (schema: ADR-0004). Thứ tự trong callback: Raw, Bronze, rồi mới upsert `ingest_batches`. Job tự áp migration còn thiếu của control DB khi khởi động.

**Mã thoát.** `0` xong (`--available-now`); `10` cấu hình sai; `13` không có Docker; `14` job thất bại.

**Chưa có.** Nạp accounts và sao kê; đo hiệu năng.

## `n06 wait-ingestion`

```bash
n06 wait-ingestion --profile demo --dataset-id <id> [--source-dir <dir>/source] --timeout-seconds 300
```

**Cách container thấy nguồn của host.** Thư mục `source/` (mặc định `<paths.generator_output_dir>/source`, ghi đè bằng `--source-dir`) được gắn **chỉ đọc** vào `/opt/n06/source-input` của container dùng một lần. Chỉ gắn `source/`, không gắn thư mục cha chứa `ground_truth/`. Thư mục phải đọc được bởi user `spark` (uid 1000) của image; volume `n06_spark_data` của service không được dùng.

**Thuật toán** (ADR-0004 mục 9): kiểm file events của dataset theo manifest (sha256, số dòng, không có file ngoài manifest); so đa tập `sha256` byte từng dòng với đa tập `sha256(value)` của Raw thuộc các batch có trong `ingest_batches` của query; đủ khi mọi dòng, kể cả bản lặp, có ít nhất bấy nhiêu bản giống từng byte.

**Mã thoát.** `0` đủ; `10` cấu hình hoặc nguồn sai (không chờ); `14` hết thời gian chờ hoặc job thất bại, in `missing_lines` và số dòng `parse_error`. Cách đọc fixture nghiệm thu (bố cục khác) **chưa làm** (việc mở của ADR-0004).
