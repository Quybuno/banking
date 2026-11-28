-- Tiêu đề Metabase: Số giao dịch theo ngày và kênh
-- Loại biểu đồ: Stacked Bar Chart (Trục X: business_date, Series: channel, Trục Y: transaction_count)
-- Collection: N06
-- Database: n06_mart (role chỉ đọc n06_mart_ro)
-- Mô tả: Đếm số giao dịch theo ngày nghiệp vụ và kênh (đặc tả Phần I mục 9).

SELECT
    ft.dataset_id,
    ft.business_date,
    ft.channel,
    dc.channel_label,
    COUNT(*) AS transaction_count
FROM public.fact_transactions AS ft
INNER JOIN public.dim_channel AS dc
    ON dc.channel = ft.channel
GROUP BY
    ft.dataset_id,
    ft.business_date,
    ft.channel,
    dc.channel_label
ORDER BY
    ft.dataset_id,
    ft.business_date,
    ft.channel;
