# Đo dung lượng generator ghi cho mỗi giao dịch

Nguồn của `limits.generator_bytes_per_tx` trong `config/default.toml` (CFG-scale-guard).
Đơn vị: KB = 1.000 byte, GB = 10^9 byte. Đây là số đo thật; phần pipeline (Kafka, MinIO, Postgres)
**chưa đo**, nên `limits.pipeline_bytes_per_tx` không có giá trị.

## Kết quả

Mã generator: commit `93e4d66` (nhánh `main` tại thời điểm đo). Nhánh `task/CFG-scale-guard` không
sửa gì trong `src/n06/generator/`, nên số đo áp dụng cho cả hai. Máy: WSL2, Python 3.11, đĩa ảo
ext4 trên ổ Windows.

| Số giao dịch | `source/` (B/giao dịch) | `ground_truth/` (B/giao dịch) | Tổng (B/giao dịch) | Tổng dung lượng | Thời gian | RSS đỉnh |
|---:|---:|---:|---:|---:|---:|---:|
| 20.000 | 2.529,8 | 1.450,2 | **3.980,0** | 79,6 MB | 23,9 s | 61 MB |
| 100.000 | 2.538,0 | 1.454,8 | **3.992,8** | 399,3 MB | 118,6 s | 55 MB |

Hai mức cho kết quả sát nhau (chênh dưới 0,4%), nên số byte mỗi giao dịch gần như không đổi theo
quy mô. Cấu hình lấy giá trị làm tròn lên **4.000 B**.

## Phạm vi và giới hạn của số đo

- Cấu hình generator: `contracts/N06_INPUT_CONTRACT_V1/config/generator.demo-faults.json`
  (có tiêm lỗi, 95% giao dịch thành công), đổi `transaction_count` và
  `account_count = max(50, N // 200)`. Cấu hình khác (ví dụ `bench-skew`, tỷ lệ lỗi khác) có thể
  cho số khác; chưa đo.
- Chỉ đo kích thước cuối của hai thư mục đầu ra. **Chưa đo** dung lượng tạm đạt đỉnh trong lúc
  sinh (sắp xếp ngoài `external_sort`).
- Chưa đo ở 1.000.000 và 5.000.000 giao dịch; mức 1.000.000 là ngoại suy tuyến tính từ hai mức đo.

## Cách tái hiện

Từ thư mục gốc repo, lưu đoạn mã dưới đây thành `measure.py` ở **ngoài** repo (ví dụ thư mục tạm),
rồi chạy cho từng mức. Thư mục đầu ra phải chưa tồn tại hoặc rỗng:

```bash
git rev-parse --short HEAD            # ghi lại commit đang đo
uv run python /đường/dẫn/measure.py /đường/dẫn/out-20000 20000
uv run python /đường/dẫn/measure.py /đường/dẫn/out-100000 100000
```

```python
import json
import sys
import time
from pathlib import Path

from n06.generator import build_dataset

out = Path(sys.argv[1])
n = int(sys.argv[2])
config_path = Path("contracts/N06_INPUT_CONTRACT_V1/config/generator.demo-faults.json")
cfg = json.loads(config_path.read_text("utf-8"))
cfg.update(dataset_id=f"measure-{n}", transaction_count=n, account_count=max(50, n // 200))

started = time.time()
build_dataset(cfg, out)
print("sec", round(time.time() - started, 1))

total = 0
for top in sorted(out.iterdir()):
    if top.is_dir():
        size = sum(f.stat().st_size for f in top.rglob("*") if f.is_file())
    else:
        size = top.stat().st_size
    total += size
    print(f"{top.name:25s} {size / n:9.1f} B/giao dịch")
print("TOTAL", round(total / n, 1), "B/giao dịch")
```

Đầu ra mong đợi (khác nhau chỉ ở thời gian): ở 20.000 giao dịch `ground_truth` 1450,2,
`source` 2529,8, `TOTAL` 3980,0; ở 100.000 giao dịch `ground_truth` 1454,8, `source` 2538,0,
`TOTAL` 3992,8. RSS đỉnh đo bằng `/usr/bin/time -f "%M"` (KB).
