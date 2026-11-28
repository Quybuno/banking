-- Tiêu đề Metabase: Số lượng và tỷ lệ 8 trạng thái đối soát
-- Loại biểu đồ: Bar Chart / Table (Trục X: recon_status, Trục Y: match_key_count / pct_of_match_keys)
-- Collection: N06
-- Database: n06_mart (role chỉ đọc n06_mart_ro)
-- Mô tả: Đủ 8 trạng thái đối soát; mẫu số là tổng số match key cần đối soát; PENDING hiển thị riêng, không gộp vào MATCHED (đặc tả Phần I mục 9).

WITH all_statuses(recon_status, sort_order) AS (
    VALUES
        ('MATCHED', 1),
        ('PENDING', 2),
        ('MISSING_EXTERNAL', 3),
        ('MISSING_INTERNAL', 4),
        ('AMOUNT_MISMATCH', 5),
        ('CURRENCY_MISMATCH', 6),
        ('STATUS_MISMATCH', 7),
        ('DUPLICATE_OR_CONFLICT', 8)
),
datasets AS (
    SELECT DISTINCT dataset_id
    FROM public.fact_reconciliation
),
totals AS (
    SELECT
        dataset_id,
        COUNT(*) AS total_match_keys
    FROM public.fact_reconciliation
    GROUP BY dataset_id
),
counts AS (
    SELECT
        dataset_id,
        recon_status,
        COUNT(*) AS match_key_count
    FROM public.fact_reconciliation
    GROUP BY dataset_id, recon_status
)
SELECT
    d.dataset_id,
    s.recon_status,
    COALESCE(c.match_key_count, 0) AS match_key_count,
    t.total_match_keys,
    ROUND(
        100.0 * COALESCE(c.match_key_count, 0) / NULLIF(t.total_match_keys, 0),
        4
    ) AS pct_of_match_keys
FROM datasets AS d
CROSS JOIN all_statuses AS s
INNER JOIN totals AS t
    ON t.dataset_id = d.dataset_id
LEFT JOIN counts AS c
    ON c.dataset_id = d.dataset_id
    AND c.recon_status = s.recon_status
ORDER BY
    d.dataset_id,
    s.sort_order;
