# config/

**Mục đích:** Cấu hình mặc định và profile `demo`/`benchmark`/`test`. Không chứa secret.

**Người sở hữu:** C (Quý)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 2, 3, 10

**Quy tắc:** Mọi endpoint, bucket, topic, ngày, seed, quy mô, partition, executor đều nằm ở đây hoặc biến môi trường, không nằm trong `src/`.

## Các tệp cấu hình

- `default.toml`: Cấu hình nền cho toàn bộ hệ thống.
- `demo.toml`: Profile cho kịch bản chạy demo end-to-end (bật Airflow/Metabase, 10.000 giao dịch).
- `benchmark.toml`: Profile cho đo hiệu năng (tắt Airflow/Metabase để giảm RAM, producer `rate = 0` không giới hạn, 1.000.000 giao dịch); dùng bucket riêng `n06-benchmark-lakehouse` (ADR-0002 mục 5) để dữ liệu đo không lẫn dữ liệu nghiệp vụ, endpoint giữ nguyên. Bucket này chưa có cơ chế tự tạo, xem tồn đọng ở `PROGRESS.md`.
- `test.toml`: Profile cô lập endpoint/bucket/topic/prefix cho kiểm thử tự động.
- `kafka.query_name` (mặc định `raw-bronze`): tên query của `n06 stream`, là một đoạn đường dẫn Raw, Bronze, checkpoint và khóa `ingest_batches`; mẫu `^[a-z][a-z0-9_-]{2,63}$`.
- Mục `[reconciliation]` (trong `default.toml`, mọi profile kế thừa; ADR-0006 Q10, hiện thực ở T33): `statuses` (mặc định `"SUCCESS,FAILED"`) và `channels` (mặc định `"SIM_CHANNEL_A,SIM_CHANNEL_B"`) là tập status/channel được đối soát, viết dạng chuỗi phân tách bằng dấu phẩy (loader không có kiểu danh sách); đặt lại qua `N06__RECONCILIATION__STATUSES` và `N06__RECONCILIATION__CHANNELS`. Bản ghi giao dịch hoặc sao kê ngoài tập này không vào đối soát. Khóa trống, phần tử trống hoặc trùng bị `load_config()` từ chối. Generator chưa đọc mục này.
- Mục `[limits]` (trong `default.toml`, mọi profile kế thừa): giới hạn theo dung lượng máy, xem `README.md` mục "Giới hạn quy mô theo dung lượng máy". Khóa không đặt (`max_transactions`, `pipeline_bytes_per_tx`, `disk_probe_path`) nhận giá trị `None`.
- Mục `[quality]` (`default.toml` chặt: mọi lỗi chặn publish; `demo.toml` là policy công khai cho demo có lỗi chủ đích): ngưỡng cổng chất lượng của `process-day`, xem `docs/adr/0014-cong-chat-luong-va-control-report.md`.

## Thứ tự ưu tiên 4 tầng (thấp → cao)

1. `config/default.toml`
2. `config/<profile>.toml` (chọn qua `N06_PROFILE`, tham số `profile` hoặc `cli_overrides["profile"]`)
3. Biến môi trường (`N06_*` trong `.env.example` và cú pháp phân cấp `N06__<SECTION>__<KEY>`)
4. Tham số ghi đè từ CLI (`cli_overrides` truyền vào `n06.config.load_config`)

## Quản lý Credential

- Các trường nhạy cảm (`object_store.access_key`, `object_store.secret_key`, `postgres.mart.password`, `postgres.control.password`, `postgres.airflow.password`, `postgres.metabase.password`) **chỉ được nạp từ biến môi trường** (xem `.env.example`).
- Nếu bất kỳ file `.toml` nào trong `config/` khai báo password hoặc secret_key, `load_config()` sẽ ném `ConfigValidationError` và dừng ngay khi khởi động.

## Cách dùng trong Python

```python
from n06.config import load_config

cfg = load_config(
    profile="demo",
    cli_overrides={"producer.key_strategy": "transaction_id"},
)
```
