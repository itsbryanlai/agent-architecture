-- Tenants in the database. One row per tenant: its Slack workspace, whether it is
-- enabled, the agent that answers by default, the channel routing, the Slack user IDs
-- of its admins and the NAMES of the environment variables that hold its secrets
-- (never a secret value). Agents stay in the agents table. After a one-time import
-- (`uv run python -m agentos.tenant_store import`) this table is the source of truth
-- and every process picks up a change within seconds, without a restart.
--
-- The app role sees only its own tenant's row, like every tenant table. Two things
-- must read across tenants, so both go through narrow SECURITY DEFINER functions that
-- return IDs only: the gateway maps a Slack workspace to a tenant before it knows the
-- tenant, and a process lists the tenants it serves at start-up. The functions are
-- owned by a role with no login (agentos_directory) that one extra policy lets read
-- the table; the app role cannot select another tenant's row through any other path.
-- The app role can SELECT, INSERT and UPDATE tenants, and has no DELETE.
-- Run as the owner role, after 013 (and 014). Safe to run more than once.

CREATE TABLE IF NOT EXISTS tenants (
  tenant_id           text        PRIMARY KEY,
  slack_team_id       text        NOT NULL UNIQUE,
  enabled             boolean     NOT NULL DEFAULT true,
  default_agent       text        NOT NULL,
  notion_workspace_id text        UNIQUE,
  channels            jsonb       NOT NULL DEFAULT '{}'::jsonb,   -- channel ID -> agent ID
  admins              jsonb       NOT NULL DEFAULT '[]'::jsonb,   -- Slack user IDs
  secret_env          jsonb       NOT NULL DEFAULT '{}'::jsonb,   -- field -> env var NAME
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT tenants_id_format CHECK (tenant_id ~ '^[a-z0-9][a-z0-9-]{0,62}$'),
  CONSTRAINT tenants_channels_object CHECK (jsonb_typeof(channels) = 'object'),
  CONSTRAINT tenants_admins_array CHECK (jsonb_typeof(admins) = 'array'),
  CONSTRAINT tenants_secret_env_object CHECK (jsonb_typeof(secret_env) = 'object')
);

-- Every process notices a change by updated_at, so an UPDATE always moves it, including
-- one an operator types by hand.
CREATE OR REPLACE FUNCTION tenants_touch() RETURNS trigger
LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS tenants_touch ON tenants;
CREATE TRIGGER tenants_touch BEFORE UPDATE ON tenants
  FOR EACH ROW EXECUTE FUNCTION tenants_touch();

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_app') THEN
    CREATE ROLE agentos_app LOGIN PASSWORD 'agentos_app' NOSUPERUSER NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentos_directory') THEN
    CREATE ROLE agentos_directory NOLOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;

-- Lets this script hand function ownership to agentos_directory.
GRANT agentos_directory TO CURRENT_USER;

GRANT USAGE ON SCHEMA public TO agentos_app, agentos_directory;
REVOKE ALL ON tenants FROM agentos_app, agentos_directory;
GRANT SELECT, INSERT, UPDATE ON tenants TO agentos_app;
GRANT SELECT ON tenants TO agentos_directory;

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON tenants;
CREATE POLICY tenant_isolation ON tenants
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
DROP POLICY IF EXISTS tenant_directory ON tenants;
CREATE POLICY tenant_directory ON tenants FOR SELECT TO agentos_directory USING (true);

-- The tenant of a Slack workspace: its ID and whether it is enabled, nothing else.
CREATE OR REPLACE FUNCTION tenant_for_slack_team(team text)
RETURNS TABLE (tenant_id text, enabled boolean, updated_at timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT t.tenant_id, t.enabled, t.updated_at FROM tenants t WHERE t.slack_team_id = team
$$;

-- Every tenant: its ID, whether it is enabled and when it last changed, nothing else.
CREATE OR REPLACE FUNCTION list_tenants()
RETURNS TABLE (tenant_id text, enabled boolean, updated_at timestamptz)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT t.tenant_id, t.enabled, t.updated_at FROM tenants t ORDER BY t.tenant_id
$$;

ALTER FUNCTION tenant_for_slack_team(text) OWNER TO agentos_directory;
ALTER FUNCTION list_tenants() OWNER TO agentos_directory;
REVOKE ALL ON FUNCTION tenant_for_slack_team(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION list_tenants() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION tenant_for_slack_team(text) TO agentos_app;
GRANT EXECUTE ON FUNCTION list_tenants() TO agentos_app;
