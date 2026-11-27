# Đo build_dataset ở 100.000 giao dịch (đóng T13)

Mã đo: commit `603787f71650a8f5b4e8acf3d47e009fc68a9c6e` (`origin/main` lúc đo, nhánh `task/T13-close`).
Máy: WSL2, Python 3.11, 8,9 GB RAM, đĩa ảo ext4; máy dùng chung với hai phiên khác nên thời gian
tường dài hơn lần đo trước (118,6 s ở `generator-bytes-per-tx.md`) và chỉ mang tính tham khảo.
Đơn vị: byte (B), KB = 1.000 B.

## Lệnh nguyên văn

Cấu hình đo được suy ra từ `contracts/N06_INPUT_CONTRACT_V1/config/generator.demo-faults.json`
(không sửa tệp gốc) với `dataset_id=measure-100000`, `account_count=500` (= `max(50, N // 200)`);
bản `cfg-b.json` thêm `generation_chunk_rows=7000` (bản gốc 10000). Quy mô đưa qua `--transactions`.
Lệnh đi qua `n06 generate`, nên bước kiểm `[limits]` chạy trước khi sinh (không bị từ chối).

```
/usr/bin/time -v .venv/bin/n06 generate --generator-config <scratchpad>/m/cfg-a.json --out <scratchpad>/m/out-a1 --transactions 100000 --json
/usr/bin/time -v .venv/bin/n06 generate --generator-config <scratchpad>/m/cfg-a.json --out <scratchpad>/m/out-a2 --transactions 100000 --json
/usr/bin/time -v .venv/bin/n06 generate --generator-config <scratchpad>/m/cfg-b.json --out <scratchpad>/m/out-b1 --transactions 100000 --json
```

Kiểm dung lượng (`[limits]`): `status=WARN reason=pipeline_unmeasured`, cần 8,40 GB (0,40 GB dữ liệu +
reserve 8,00 GB), còn 38,12 GB; không bị từ chối, không nới giới hạn.

## Kết quả

| Lượt | Cấu hình | Wall time | RSS đỉnh | `business_checksum` |
|---|---|---:|---:|---|
| a1 | `generation_chunk_rows=10000` | 5:04,03 | 66.204 KB | `64a280e124ee16b5272156698b907943f726c138b97438bc7da83b6e375e6f2c` |
| a2 | cùng seed, cùng cấu hình | 4:43,86 | 65.896 KB | `64a280e124ee16b5272156698b907943f726c138b97438bc7da83b6e375e6f2c` |
| b1 | `generation_chunk_rows=7000` | 4:44,40 | 52.808 KB | `64a280e124ee16b5272156698b907943f726c138b97438bc7da83b6e375e6f2c` |

`event_record_count` = 288.987 ở cả ba lượt. CPU 99% (một lõi).

- Cùng seed chạy lại: checksum giống nhau; `diff -rq out-a1 out-a2` không báo khác biệt (cả `source/` lẫn
  `ground_truth/`).
- Đổi `generation_chunk_rows`: checksum giống nhau; `diff -rq out-a1/source out-b1/source` không báo
  khác biệt. Trong `ground_truth/canonical/` tên và ranh giới part file đổi theo chunk (đã ghi ở README
  của generator); `diff -rq` báo các part ledger khác nhau, đúng như dự kiến.

## Dung lượng theo thư mục (lượt a1, kích thước thật của nội dung)

| Thư mục | Byte | B/giao dịch |
|---|---:|---:|
| `source/` | 253.839.713 | 2.538,4 |
| `source/events` | 192.430.621 | 1.924,3 |
| `source/statements` | 36.395.535 | 364,0 |
| `source/statement_deliveries.jsonl` | 24.825.044 | 248,3 |
| `source/manifests` | 66.552 | 0,7 |
| `source/accounts.jsonl` | 116.000 | 1,2 |
| `source/control_report.json` | 1.865 | 0,0 |
| `ground_truth/` | 145.491.687 | 1.454,9 |
| `ground_truth/canonical` | 111.357.116 | 1.113,6 |
| `ground_truth/statement_rows.jsonl` | 32.029.356 | 320,3 |
| `ground_truth/fault_assignment.jsonl` | 2.100.048 | 21,0 |
| `ground_truth/fault_counts.json` | 666 | 0,0 |
| `ground_truth/canonical_index.json` | 405 | 0,0 |
| **Tổng** | **399.331.400** | **3.993,3** |

Khớp số đo cũ (3.992,8 B/giao dịch ở 100.000, `generator-bytes-per-tx.md`) trong 0,02%, nên
`limits.generator_bytes_per_tx = 4000` vẫn đúng.

## Giới hạn

- Chỉ đo `demo-faults`, một lần cho mỗi cấu hình; chưa đo ở 1.000.000 hay 5.000.000 giao dịch.
- Chưa đo dung lượng tạm đạt đỉnh của external sort.
- Dữ liệu sinh ra đã xóa sau khi đo (chỉ các thư mục `out-*` do phiên này tạo).
