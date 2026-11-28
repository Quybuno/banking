-- Tiêu đề Metabase: Drill-down chi tiết một sai lệch đối soát (T62)
-- Loại biểu đồ: Table (có biến lọc Metabase: dataset_id, channel, external_reference, recon_status)
-- Collection: N06
-- Database: n06_mart (role chỉ đọc n06_mart_ro)
-- Mô tả: Truy vết từ một sai lệch đối soát tới giao dịch nội bộ, tài khoản gửi/nhận (2 role dim_account),
--         và source_pointers (file sao kê / đường dẫn Bronze / tọa độ Kafka) theo đặc tả Phần I mục 9.

SELECT
    fr.dataset_id,
    fr.business_date,
    fr.channel,
    fr.external_reference,
    fr.recon_status,
    fr.issue_codes,
    fr.transaction_id,
    fr.internal_amount_minor,
    fr.statement_amount_minor,
    fr.internal_currency,
    fr.statement_currency,
    fr.internal_status,
    fr.statement_status,
    fr.internal_record_count,
    fr.statement_record_count,
    fr.anchor_event_time,
    fr.discrepancy_age_seconds,
    fr.as_of_time,
    fr.run_id,
    fr.input_snapshot_id,
    fr.source_pointers AS recon_source_pointers,
    ft.account_from,
    acc_from.account_type AS account_from_type,
    ft.account_to,
    acc_to.account_type AS account_to_type,
    ft.event_time AS internal_event_time,
    ft.replay_count,
    ft.source_pointer AS transaction_source_pointer
FROM public.fact_reconciliation AS fr
LEFT JOIN public.fact_transactions AS ft
    ON ft.dataset_id = fr.dataset_id
    AND ft.transaction_id = fr.transaction_id
LEFT JOIN public.dim_account AS acc_from
    ON acc_from.dataset_id = ft.dataset_id
    AND acc_from.account_id = ft.account_from
LEFT JOIN public.dim_account AS acc_to
    ON acc_to.dataset_id = ft.dataset_id
    AND acc_to.account_id = ft.account_to
WHERE fr.recon_status <> 'MATCHED'
    [[AND fr.dataset_id = {{dataset_id}}]]
    [[AND fr.channel = {{channel}}]]
    [[AND fr.external_reference = {{external_reference}}]]
    [[AND fr.recon_status = {{recon_status}}]]
ORDER BY
    fr.discrepancy_age_seconds DESC NULLS LAST,
    fr.business_date DESC,
    fr.external_reference
LIMIT 200;
