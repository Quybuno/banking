# src/n06/warehouse/

**Mục đích:** Lệnh `load-mart`, `backfill`: staging, publish mart trong một transaction.

**Người sở hữu:** C (Quý)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 8.2, 8.3, 9

**Quy tắc:** Không Spark JDBC append thẳng vào bảng cuối. Publish phải xóa được dòng cũ không còn trong output.
