-- Migration mart 0001 (UP): 4 dimension, 3 fact và metadata publish theo ADR-0005
-- (docs/spec.md Phần I mục 8.2 và 9). Idempotent: chạy lại không lỗi và không đổi dữ liệu.
--
-- Quy ước: tiền là BIGINT (`*_minor`), không float. Timestamp là TIMESTAMPTZ (UTC).
-- Không đặt khóa ngoại cứng từ fact_reconciliation sang dim_account (ADR-0005 Q2) để không chặn
-- MISSING_INTERNAL; fact_transactions.account_from/account_to cũng chỉ "nối" dim_account.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------- Dimension

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT dim_date_key_matches_date CHECK (
        date_key = year * 10000 + month * 100 + day
        AND year = EXTRACT(YEAR FROM calendar_date)
        AND month = EXTRACT(MONTH FROM calendar_date)
        AND day = EXTRACT(DAY FROM calendar_date)
    )
);

CREATE TABLE IF NOT EXISTS dim_account (
    dataset_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    account_type TEXT NOT NULL,
    currency TEXT NOT NULL,
    opening_balance_minor BIGINT NOT NULL,
    opening_business_date DATE NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (dataset_id, account_id)
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel TEXT PRIMARY KEY,
    channel_label TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_transaction_type (
    transaction_type TEXT PRIMARY KEY,
    description TEXT NOT NULL
);

-- --------------------------------------------------------------------- Fact

CREATE TABLE IF NOT EXISTS fact_transactions (
    dataset_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    business_date DATE NOT NULL,
    date_key INT NOT NULL REFERENCES dim_date (date_key),
    external_reference TEXT NOT NULL,
    account_from TEXT NOT NULL,
    account_to TEXT NOT NULL,
    channel TEXT NOT NULL REFERENCES dim_channel (channel),
    transaction_type TEXT NOT NULL REFERENCES dim_transaction_type (transaction_type),
    currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    event_time TIMESTAMPTZ NOT NULL,
    replay_count INT NOT NULL CHECK (replay_count >= 1),
    source_pointer TEXT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (dataset_id, transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_transactions_scope
    ON fact_transactions (dataset_id, business_date);

CREATE TABLE IF NOT EXISTS fact_daily_balances (
    dataset_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    business_date DATE NOT NULL,
    currency TEXT NOT NULL,
    date_key INT NOT NULL REFERENCES dim_date (date_key),
    opening_balance_minor BIGINT NOT NULL,
    debit_minor BIGINT NOT NULL CHECK (debit_minor >= 0),
    credit_minor BIGINT NOT NULL CHECK (credit_minor >= 0),
    closing_balance_minor BIGINT NOT NULL,
    ledger_entry_count INT NOT NULL CHECK (ledger_entry_count >= 0),
    run_id TEXT NOT NULL,
    PRIMARY KEY (dataset_id, account_id, business_date, currency),
    -- ADR-0005 Q1 / spec dòng 264: closing = opening + credit - debit (tính bằng numeric, không tràn).
    CONSTRAINT fact_daily_balances_invariant CHECK (
        opening_balance_minor::numeric + credit_minor::numeric - debit_minor::numeric
        = closing_balance_minor::numeric
    )
);

CREATE INDEX IF NOT EXISTS idx_fact_daily_balances_scope
    ON fact_daily_balances (dataset_id, business_date);

CREATE TABLE IF NOT EXISTS fact_reconciliation (
    dataset_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    external_reference TEXT NOT NULL,
    business_date DATE NOT NULL,
    date_key INT NOT NULL REFERENCES dim_date (date_key),
    recon_status TEXT NOT NULL CHECK (recon_status IN (
        'DUPLICATE_OR_CONFLICT', 'CURRENCY_MISMATCH', 'AMOUNT_MISMATCH', 'STATUS_MISMATCH',
        'MATCHED', 'PENDING', 'MISSING_EXTERNAL', 'MISSING_INTERNAL'
    )),
    issue_codes TEXT[] NOT NULL,
    transaction_id TEXT,
    internal_amount_minor BIGINT,
    internal_currency TEXT,
    internal_status TEXT,
    statement_amount_minor BIGINT,
    statement_currency TEXT,
    statement_status TEXT,
    internal_record_count INT NOT NULL CHECK (internal_record_count >= 0),
    statement_record_count INT NOT NULL CHECK (statement_record_count >= 0),
    anchor_event_time TIMESTAMPTZ,
    discrepancy_age_seconds BIGINT CHECK (discrepancy_age_seconds >= 0),
    as_of_time TIMESTAMPTZ NOT NULL,
    last_evaluated_at TIMESTAMPTZ NOT NULL,
    first_seen_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    recon_rule_version TEXT NOT NULL,
    source_pointers TEXT[] NOT NULL,
    run_id TEXT NOT NULL,
    -- Cột bổ sung theo đặc tả mục 7.3 (S361: "input snapshot id"); ADR-0005 chưa liệt kê.
    input_snapshot_id TEXT,
    PRIMARY KEY (dataset_id, channel, external_reference),
    -- Khóa MATCHED không có issue code, anchor_event_time và discrepancy_age_seconds (mục 7.3).
    CONSTRAINT fact_reconciliation_matched_clean CHECK (
        recon_status <> 'MATCHED'
        OR (
            cardinality(issue_codes) = 0
            AND anchor_event_time IS NULL
            AND discrepancy_age_seconds IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_fact_reconciliation_scope
    ON fact_reconciliation (dataset_id, business_date);

-- ----------------------------------------------------------- Metadata publish

-- Độ tươi mart (spec mục 8.2 và 9; ADR-0005 mục 3). Một dòng cho mỗi lần publish (khóa theo
-- Gold run và phạm vi); publish lại cùng run và phạm vi cập nhật dòng, không thêm dòng.
CREATE TABLE IF NOT EXISTS mart_publish_metadata (
    dataset_id TEXT NOT NULL,
    business_date_from DATE NOT NULL,
    business_date_to DATE NOT NULL,
    gold_run_id TEXT NOT NULL,
    mart_published_at TIMESTAMPTZ NOT NULL,
    source_watermark_at TIMESTAMPTZ,
    PRIMARY KEY (dataset_id, gold_run_id, business_date_from, business_date_to),
    CHECK (business_date_from <= business_date_to)
);

-- ------------------------------------------------------------------ Quyền sở hữu

-- Migration có thể do superuser chạy (cần cho role chỉ đọc ở 0002). Chuyển chủ sở hữu bảng về chủ
-- sở hữu database để user ứng dụng (n06_mart_user) publish được bằng quyền của chính nó.
DO $$
DECLARE
    db_owner TEXT;
    tbl TEXT;
BEGIN
    SELECT pg_get_userbyid(datdba) INTO db_owner
    FROM pg_database WHERE datname = current_database();
    FOREACH tbl IN ARRAY ARRAY[
        'schema_migrations', 'dim_date', 'dim_account', 'dim_channel', 'dim_transaction_type',
        'fact_transactions', 'fact_daily_balances', 'fact_reconciliation', 'mart_publish_metadata'
    ] LOOP
        EXECUTE format('ALTER TABLE public.%I OWNER TO %I', tbl, db_owner);
    END LOOP;
END
$$;
