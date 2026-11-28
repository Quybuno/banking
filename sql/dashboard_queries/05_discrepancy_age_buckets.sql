-- Tiêu đề Metabase: Tuổi sai lệch chưa xử lý theo trạng thái và khoảng tuổi
-- Loại biểu đồ: Pivot Table / Stacked Bar Chart (Trục X: age_bucket, Series: recon_status, Trục Y: discrepancy_count)
-- Collection: N06
-- Database: n06_mart (role chỉ đọc n06_mart_ro)
-- Mô tả: Nhóm sai lệch chưa xử lý (recon_status <> 'MATCHED') theo trạng thái và khoảng discrepancy_age_seconds (đặc tả Phần I mục 7.3 & 9).

SELECT
    dataset_id,
    recon_status,
    CASE
        WHEN discrepancy_age_seconds IS NULL THEN 'UNKNOWN'
        WHEN discrepancy_age_seconds < 3600 THEN '<1h'
        WHEN discrepancy_age_seconds < 21600 THEN '1h-6h'
        WHEN discrepancy_age_seconds < 86400 THEN '6h-24h'
        WHEN discrepancy_age_seconds < 259200 THEN '24h-72h'
        ELSE '>=72h'
    END AS age_bucket,
    COUNT(*) AS discrepancy_count,
    MIN(discrepancy_age_seconds) AS min_age_seconds,
    MAX(discrepancy_age_seconds) AS max_age_seconds
FROM public.fact_reconciliation
WHERE recon_status <> 'MATCHED'
GROUP BY
    dataset_id,
    recon_status,
    CASE
        WHEN discrepancy_age_seconds IS NULL THEN 'UNKNOWN'
        WHEN discrepancy_age_seconds < 3600 THEN '<1h'
        WHEN discrepancy_age_seconds < 21600 THEN '1h-6h'
        WHEN discrepancy_age_seconds < 86400 THEN '6h-24h'
        WHEN discrepancy_age_seconds < 259200 THEN '24h-72h'
        ELSE '>=72h'
    END
ORDER BY
    dataset_id,
    recon_status,
    age_bucket;
