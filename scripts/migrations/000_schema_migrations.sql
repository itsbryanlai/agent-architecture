-- Records which migrations a database has had. scripts/migrate.py adds one row per
-- migration it applies, and init_db.sql adds one per migration it contains, so a
-- process can see whether the database is behind the code. The app role can read it.
-- Run as the owner role. Safe to run more than once.

CREATE TABLE IF NOT EXISTS schema_migrations (
  version     text        PRIMARY KEY,   -- the number in the migration's file name
  applied_at  timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON schema_migrations FROM agentos_app;
GRANT SELECT ON schema_migrations TO agentos_app;
