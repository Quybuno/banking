-- Tiêu đề Metabase: Tổng tiền giao dịch theo loại tiền và trạng thái
-- Loại biểu đồ: Table / Bar Chart (Trục X: currency, Series: status, Trục Y: total_amount_minor)
-- Collection: N06
-- Database: n06_mart (role chỉ đọc n06_mart_ro)
-- Mô tả: Tổng tiền dạng số nguyên minor unit (không float) theo currency và status (đặc tả Phần I mục 9).

SELECT
    dataset_id,
    currency,
    status,
    COUNT(*) AS transaction_count,
    SUM(amount_minor)::NUMERIC(30, 0) AS total_amount_minor
FROM public.fact_transactions
GROUP BY
    dataset_id,
    currency,
    status
ORDER BY
    dataset_id,
    currency,
    status;
