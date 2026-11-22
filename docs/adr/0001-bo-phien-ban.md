# ADR-0001: Bộ phiên bản công nghệ (khởi tạo phiên bản Python tại T00)

- Trạng thái: Proposed
- Người viết: C (Quý — khởi tạo phần Python tại T00), A (Mạnh — hoàn thiện toàn bộ bộ phiên bản tại T04)
- Người duyệt: C (Quý), A (Mạnh)
- Ngày: 2026-11-22
- Mục đặc tả liên quan (docs/spec.md): Phần I mục 2 ("Quy tắc chọn bộ phiên bản")

## Bối cảnh
Đặc tả Phần I mục 2 quy định quy tắc chọn bộ phiên bản:
> "1. Chọn một bản Spark 3.5.x dựng sẵn kèm Hadoop 3 làm gốc; mọi thành phần khác suy ra từ bản này.
> 2. `hadoop-aws` phải **đúng bằng** phiên bản Hadoop mà bản Spark đó đóng gói (đọc từ thư mục `jars/` của bản phân phối, không đoán).
> 3. AWS SDK bundle phải **đúng phiên bản** mà POM của `hadoop-aws` đó khai báo phụ thuộc.
> 4. `spark-sql-kafka-0-10` cùng phiên bản Spark và cùng phiên bản Scala.
> 5. Java và Python lấy trong danh sách bản Spark đó hỗ trợ chính thức; Airflow và PySpark dùng chung một phiên bản Python."

Tại TASK T00, dự án cần chốt tạm phiên bản Python để cấu hình `pyproject.toml`, tạo lockfile và chạy lint/unit test đồng bộ với CI (`.github/workflows/ci.yml`), trước khi TASK T04 xác minh bản phân phối Spark 3.5.x kèm JAR và chuyển ADR này sang trạng thái `Accepted`.

## Quyết định
1. **Phiên bản Python tạm chọn tại T00:** **Python 3.11 (`>=3.11,<3.12`)**.
   - **Lý do:**
     - Apache Spark 3.5.x hỗ trợ chính thức Python 3.8 đến 3.11 (theo tài liệu phát hành Spark 3.5.x).
     - Công cụ kiểm tra hợp đồng `contracts/N06_INPUT_CONTRACT_V1/tools/verify_contract.py` yêu cầu Python 3.10+.
     - Workflow CI hiện có (`.github/workflows/ci.yml`) đang cấu hình `python-version: "3.11"`.
     - Apache Airflow 2.x (dùng ở P1) và PySpark 3.5.x đều tương thích tốt trên cùng Python 3.11.
2. **Các thành phần chưa xác minh tại T00 (sẽ do T04 xác minh và chốt):**
   - Phiên bản cụ thể của bản phân phối Spark `3.5.x` kèm Hadoop 3: *chưa xác minh từ bản phân phối tại T00*.
   - Phiên bản `hadoop-aws` (đọc từ `jars/` của Spark) và `aws-java-sdk-bundle` (đọc từ POM của `hadoop-aws`): *chưa xác minh tại T00*.
   - Phiên bản `spark-sql-kafka-0-10` và Scala: *chưa xác minh tại T00*.
   - Phiên bản Java (JDK 11/17) và Airflow: *chưa xác minh tại T00*.

## Phương án đã cân nhắc
- **Python 3.8 / 3.9:** Dù Spark 3.5.x còn hỗ trợ, nhưng `verify_contract.py` yêu cầu Python 3.10+ và thiếu `tomllib` trong thư viện chuẩn.
- **Python 3.10:** Thỏa `verify_contract.py` và Spark 3.5.x, nhưng thiếu `tomllib` trong thư viện chuẩn (có từ Python 3.11) và lệch với cấu hình mặc định `3.11` trong `.github/workflows/ci.yml`.
- **Python 3.12+:** Không nằm trong danh sách hỗ trợ chính thức từ bản đầu của nhánh Spark 3.5.x trên mọi tổ hợp Airflow/PySpark, dễ phát sinh lỗi tương thích C-extension/pickle.

## Hệ quả
- `pyproject.toml` tại T00 đặt `requires-python = ">=3.11,<3.12"` và `[tool.ruff]` đặt `target-version = "py311"`.
- TASK T04 (do A thực hiện) có trách nhiệm kiểm tra thực tế thư mục `jars/` của bản Spark 3.5.x được chọn, tra cứu POM của `hadoop-aws`, điền bảng compatibility matrix kèm nguồn kiểm chứng vào ADR này và chuyển trạng thái sang `Accepted`.

