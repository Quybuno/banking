-- Migration mart 0002 (DOWN): thu hồi mọi quyền của `n06_mart_ro` trên database này.
-- Không DROP ROLE vì role thuộc cả cluster và có thể đang được database khác (hoặc Metabase) dùng.

DO $$
DECLARE
    db_owner TEXT;
BEGIN
    SELECT pg_get_userbyid(datdba) INTO db_owner
    FROM pg_database WHERE datname = current_database();

    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'n06_mart_ro') THEN
        EXECUTE format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public REVOKE SELECT ON TABLES FROM n06_mart_ro',
            db_owner
        );
        REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM n06_mart_ro;
        REVOKE USAGE ON SCHEMA public FROM n06_mart_ro;
        EXECUTE format('REVOKE CONNECT ON DATABASE %I FROM n06_mart_ro', current_database());
    END IF;
END
$$;
