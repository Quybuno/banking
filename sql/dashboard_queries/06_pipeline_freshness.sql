-- Tiêu đề Metabase: Độ tươi pipeline (Freshness)
-- Loại biểu đồ: Table (dataset_id, business_date_from..to, source_watermark_at, mart_published_at, freshness_lag_seconds)
-- Collection: N06
-- Database: n06_mart (role chỉ đọc n06_mart_ro)
-- Mô tả: Độ tươi xác định bằng source_watermark_at và mart_published_at trong mart_publish_metadata (đặc tả Phần I mục 9).

SELECT
    dataset_id,
    business_date_from,
    business_date_to,
    gold_run_id,
    source_watermark_at,
    mart_published_at,
    EXTRACT(EPOCH FROM (mart_published_at - source_watermark_at))::BIGINT AS publish_minus_watermark_seconds
FROM public.mart_publish_metadata
ORDER BY
    mart_published_at DESC,
    dataset_id;
