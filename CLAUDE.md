# N06 — Quy tắc cho coding agent

## Bối cảnh
Pipeline dữ liệu lớn cho giao dịch ngân hàng mô phỏng (tiểu luận môn Dữ liệu lớn).
- Đặc tả bắt buộc: `docs/spec.md` (bản 1.2). Chỉ đọc các mục ghi trong thẻ task.
- Danh sách task, người sở hữu, mẫu prompt: `docs/TASKS.md`.
- Tiến độ: `PROGRESS.md`. Quyết định đã chốt: `docs/adr/` (chỉ ADR trạng thái Accepted).
- Mỗi thư mục có `README.md` ghi mục đích, người sở hữu và quy tắc riêng. Đọc README của thư mục trước khi sửa file trong đó.

## Bản đồ thư mục
- `contracts/N06_INPUT_CONTRACT_V1/` — hợp đồng dữ liệu, fixture, đáp án. **Không sửa.**
- `src/n06/<module>/` — mã pipeline. Một module một người sở hữu.
- `tests/harness/` — oracle đọc ground truth. `src/` không được import `tests/`.
- `config/` — mọi giá trị triển khai. `infra/` — Docker. `sql/` — migration và truy vấn dashboard.
- `reports/` — chỉ chứa kết quả do `n06 benchmark` sinh.

## Lệnh
- Cài đặt: `uv sync --extra dev` (hoặc `pip install -e ".[dev]"` trong môi trường Python 3.11)
- Kiểm tra hợp đồng: `python contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py` (14/14 PASS)
- Kiểm tra quy tắc: `python scripts/check_rules.py`
- Lint: `ruff check . && ruff format --check .`
- Unit test: `pytest tests/unit -q`
- Integration: [cập nhật sau T05]

## Quy tắc cứng
- Chỉ làm task được giao. Không làm việc của task khác, không thêm tính năng ngoài thẻ task.
- Không sửa `contracts/` hoặc `tests/fixtures/`. Không sửa expected/đáp án để test pass.
- Tiền là số nguyên (`amount_minor`). Không float. Kiểm tra miền trước khi ghi BIGINT.
- `business_date` theo Asia/Ho_Chi_Minh; timestamp hợp đồng là UTC dạng `...Z`.
- Không hardcode endpoint, credential, bucket, topic, ngày, seed, quy mô, partition, executor.
- Pipeline không đọc ground truth hay `expected_results.json`. Chỉ `tests/harness/` được đọc.
- Không dùng TODO/pass/pseudocode thay nghiệp vụ lõi. Không giả số đo benchmark.
- Đặc tả mơ hồ hoặc mâu thuẫn: dừng, trích nguyên văn chỗ mâu thuẫn, đề xuất ADR. Không tự chọn cách hiểu.

## Quy trình mỗi task
1. Đọc thẻ task, các mục đặc tả trong thẻ, ADR liên quan, README của thư mục sẽ sửa.
2. Lập kế hoạch (file, hàm, quyết định, cách kiểm tra). Dừng chờ duyệt.
3. Hiện thực trên nhánh `task/<ID>-<ten-ngan>`.
4. Chạy toàn bộ lệnh ở mục "Lệnh" và lệnh kiểm tra của task. Dán output thật.
5. Báo cáo: file đã đổi; lệnh đã chạy và kết quả; giả định; phần chưa chạy được và lý do; đề xuất dòng cập nhật `PROGRESS.md`. Không commit khi chưa được xác nhận.
