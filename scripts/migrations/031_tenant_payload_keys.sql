-- Tenant payload keys, wrapped by master keys from AGENTOS_MASTER_KEY_V<n> host variables.
-- One row per tenant and version: the app creates version 1 when a tenant is installed
-- (INSERT ... ON CONFLICT DO NOTHING, so two concurrent installs create one row) and
-- rotation adds the next version. wrapped_key is nonce || AES-256-GCM under master version
-- master_version, with the key ID ("<tenant>/v<n>") as associated data, so a wrapped key
-- copied to another tenant's row, or stored as another version, does not unwrap. The master
-- key itself never enters the database.
-- The codec decrypts for whichever tenant a payload belongs to, so the app role reads every
-- row of this one table (key_read); inserts stay bound to the transaction's tenant
-- (key_insert). No UPDATE or DELETE: a stored key version is never changed or removed while
-- a history written under it is kept.
-- payload_key is the audit kind for the host command (create, rotate in db mode); it belongs
-- to the tenant, so it joins the kinds that may leave agent_id and agent_version null.
-- Run as the owner role, after 030. Safe to run more than once.

CREATE TABLE IF NOT EXISTS tenant_payload_keys (
  tenant_id      text        NOT NULL,
  version        integer     NOT NULL CHECK (version >= 1),
  wrapped_key    bytea       NOT NULL,
  master_version integer     NOT NULL CHECK (master_version >= 1),
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, version)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON tenant_payload_keys FROM agentos_app;
GRANT SELECT, INSERT ON tenant_payload_keys TO agentos_app;

ALTER TABLE tenant_payload_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_payload_keys FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS key_read ON tenant_payload_keys;
CREATE POLICY key_read ON tenant_payload_keys FOR SELECT USING (true);
DROP POLICY IF EXISTS key_insert ON tenant_payload_keys;
CREATE POLICY key_insert ON tenant_payload_keys FOR INSERT
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('payload_key') ON CONFLICT DO NOTHING;

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_required;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_required CHECK (
  kind IN ('member_add', 'member_remove', 'member_role', 'budget_set',
           'switch_off', 'switch_on', 'usage_repriced', 'retention_set', 'webhook_token',
           'payload_key')
  OR (kind = 'audience_resume' AND agent_id IS NOT NULL)
  OR (agent_id IS NOT NULL AND agent_version IS NOT NULL));
