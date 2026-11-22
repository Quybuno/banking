# src/n06/cli/

**Mục đích:** Điểm vào dòng lệnh thống nhất `n06` của hệ thống (TASK T03: `init-local`, `doctor`, `--help`, bảng mã thoát).

**Người sở hữu:** C (Quý)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 10, mục 12 phần M0; `docs/adr/0001-bo-phien-ban.md`

**Quy tắc:**
- Mọi lệnh hỗ trợ `--help`, có structured output (`--json`) và trả về đúng mã thoát (`ExitCode`).
- Tuyệt đối không in giá trị secret/password ra output hoặc log.
- Không hardcode endpoint, hostname hay port trong mã nguồn; toàn bộ địa chỉ dịch vụ lấy từ hệ thống cấu hình T01 (`n06.config.load_config`) và biến môi trường `.env`.
- Logic đánh giá tài nguyên máy chủ (`evaluate_system_resources`) được tách riêng thành hàm thuần túy nhận `SystemResourceSnapshot` để kiểm thử độc lập với máy chạy test.

## Bảng mã thoát (`ExitCode`)

| Mã thoát | Tên hằng số | Ý nghĩa |
|---|---|---|
| `0` | `OK` | Mọi kiểm tra hoặc thao tác thành công |
| `2` | `CLI_USAGE_ERROR` | Sai cú pháp dòng lệnh hoặc tham số CLI không hợp lệ |
| `10` | `CONFIG_ERROR` | Lỗi thiếu biến môi trường bắt buộc, thiếu file `.env`, hoặc cấu hình `config/*.toml` sai (`ConfigValidationError`) |
| `11` | `PYTHON_ENV_ERROR` | Phiên bản Python không thuộc nhánh `3.11.x` theo `ADR-0001` |
| `12` | `RESOURCE_ERROR` | Tài nguyên máy chủ (CPU cores, RAM, hoặc ổ đĩa trống) thấp hơn ngưỡng tối thiểu |
| `13` | `DOCKER_ERROR` | Thiếu Docker CLI, thiếu plugin `docker compose`, hoặc Docker daemon chưa chạy |
| `14` | `SERVICE_ERROR` | Một hoặc nhiều service hạ tầng (`kafka`, `minio`, `postgres`, `spark`) đang tắt, `unhealthy`, hoặc không mở kết nối port |
