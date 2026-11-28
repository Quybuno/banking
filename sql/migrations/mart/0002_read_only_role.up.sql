-- Migration mart 0002 (UP): role chỉ đọc `n06_mart_ro` cho Metabase (ADR-0013 Q6, ADR-0005).
-- Idempotent. Role là đối tượng của cả cluster nên cần superuser hoặc CREATEROLE lần đầu; role đã
-- tồn tại thì chỉ cấp lại quyền. Mật khẩu KHÔNG nằm trong migration: đặt riêng bằng
-- `ALTER ROLE n06_mart_ro PASSWORD ...` từ biến môi trường (T43). Chưa có mật khẩu thì không đăng
-- nhập được bằng mật khẩu.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'n06_mart_ro') THEN
        CREATE ROLE n06_mart_ro LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
EXCEPTION
    WHEN duplicate_object THEN
        NULL;  -- hai migration chạy đồng thời trên hai database cùng cluster
END
$$;

DO $$
DECLARE
    db_owner TEXT;
BEGIN
    SELECT pg_get_userbyid(datdba) INTO db_owner
    FROM pg_database WHERE datname = current_database();

    EXECUTE format('GRANT CONNECT ON DATABASE %I TO n06_mart_ro', current_database());
    GRANT USAGE ON SCHEMA public TO n06_mart_ro;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO n06_mart_ro;
    -- Bảng tạo sau bởi chủ sở hữu database cũng chỉ cho SELECT.
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT ON TABLES TO n06_mart_ro',
        db_owner
    );
END
$$;
