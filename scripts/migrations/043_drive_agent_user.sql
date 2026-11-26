-- The agent's own Google user as the Drive account (D-53).
-- 1. drive_connections gains kind: 'service_account' (every existing row, the default) or
--    'user' (an admin signed in once as the agent's Google user in the customer's
--    Workspace). The service_account_email column holds the account's address for either
--    kind. A user address belongs to one tenant: the partial unique index refuses a second
--    tenant connecting the same address.
-- 2. tenant_credentials accepts the kind 'google_user', the agent user's OAuth refresh
--    token, stored encrypted like every credential here (the pattern is migration 038's,
--    with one more fixed kind).
-- 3. A Drive call knows only the account's email, so a directory function maps a user's
--    address to its tenant, as tenant_for_slack_team does for a Slack workspace: it returns
--    the tenant ID of a 'user' row and nothing else.
-- Run as the owner role, after 042. Safe to run more than once.

ALTER TABLE drive_connections
  ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'service_account';
ALTER TABLE drive_connections DROP CONSTRAINT IF EXISTS drive_connections_kind_check;
ALTER TABLE drive_connections ADD CONSTRAINT drive_connections_kind_check
  CHECK (kind IN ('service_account', 'user'));

CREATE UNIQUE INDEX IF NOT EXISTS drive_connections_user_email
  ON drive_connections (lower(service_account_email)) WHERE kind = 'user';

ALTER TABLE tenant_credentials DROP CONSTRAINT IF EXISTS tenant_credentials_kind_check;
ALTER TABLE tenant_credentials ADD CONSTRAINT tenant_credentials_kind_check
  CHECK (kind IN ('slack_bot', 'notion', 'google_user')
         OR kind ~ '^notion_creator:[A-Z0-9_]{1,64}$');

-- Lets this script hand function ownership to agentos_directory (as migration 015 does).
GRANT agentos_directory TO CURRENT_USER;
GRANT USAGE ON SCHEMA public TO agentos_directory;
REVOKE ALL ON drive_connections FROM agentos_directory;
GRANT SELECT ON drive_connections TO agentos_directory;
DROP POLICY IF EXISTS tenant_directory ON drive_connections;
CREATE POLICY tenant_directory ON drive_connections FOR SELECT TO agentos_directory
  USING (kind = 'user');

CREATE OR REPLACE FUNCTION drive_user_tenant(account text)
RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public, pg_temp AS $$
  SELECT d.tenant_id FROM drive_connections d
  WHERE d.kind = 'user' AND lower(d.service_account_email) = lower(account)
$$;

ALTER FUNCTION drive_user_tenant(text) OWNER TO agentos_directory;
REVOKE ALL ON FUNCTION drive_user_tenant(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION drive_user_tenant(text) TO agentos_app;
