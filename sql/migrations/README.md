# sql/migrations/

**Mục đích:** Migration cho control DB và mart.

**Người sở hữu:** B (Quang) cho control; C (Quý) cho mart

**Đặc tả liên quan (docs/spec.md):** Phần I mục 8, 9

**Quy tắc:** Migration phải chạy được lên và xuống.

- `control/`: trình chạy `n06.control` (`migrate_up`, `migrate_down`).
- `mart/`: trình chạy `n06.warehouse` (`migrate_mart_up`, `migrate_mart_down`), cùng cơ chế và bảng `schema_migrations` riêng trong database mart. Tên file `NNNN_ten_ngan.up.sql` / `.down.sql`; mỗi file chạy trong một transaction và phải idempotent. Chi tiết ở `src/n06/warehouse/README.md`.
