# dags/

**Mục đích:** Airflow DAG theo business_date (P1).

**Người sở hữu:** C (Quý)

**Đặc tả liên quan (docs/spec.md):** Phần I mục 2 (cách Airflow gọi Spark), mục 10

**Quy tắc:** DAG chỉ gọi CLI `n06`; không chép logic Spark vào DAG; không gọi mạng khi parse.
