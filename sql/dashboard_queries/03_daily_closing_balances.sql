-- Tiêu đề Metabase: Số dư cuối ngày theo ngày và loại tiền
-- Loại biểu đồ: Line Chart / Table (Trục X: business_date, Series: currency, Trục Y: total_closing_balance_minor)
-- Collection: N06
-- Database: n06_mart (role chỉ đọc n06_mart_ro)
-- Mô tả: Tổng hợp số dư mở đầu, ghi nợ, ghi có và số dư cuối ngày từ fact_daily_balances (có dữ liệu sau T40).

SELECT
    dataset_id,
    business_date,
    currency,
    COUNT(*) AS account_count,
    SUM(opening_balance_minor)::NUMERIC(30, 0) AS total_opening_balance_minor,
    SUM(debit_minor)::NUMERIC(30, 0) AS total_debit_minor,
    SUM(credit_minor)::NUMERIC(30, 0) AS total_credit_minor,
    SUM(closing_balance_minor)::NUMERIC(30, 0) AS total_closing_balance_minor
FROM public.fact_daily_balances
GROUP BY
    dataset_id,
    business_date,
    currency
ORDER BY
    dataset_id,
    business_date,
    currency;
