-- Grants picked from Notion by the person who owns the draft, and two audit kinds.
--
-- grant_picks records that a Notion database or page was chosen from what the creator's own
-- Notion account can see (the console's "Choose from Notion"): who chose it and when, per
-- agent. It holds Notion IDs and a kind, never a title, page text or the creator's Notion
-- token (that token is used once and not stored anywhere). The app role can SELECT, INSERT and
-- UPDATE (choosing the same object again refreshes who and when), never DELETE.
-- New audit kinds: grant_pick (objects chosen) and widening_confirm (an approver ticked the
-- "this widens access" box when publishing).
-- Run as the owner role. Safe to run more than once.

CREATE TABLE IF NOT EXISTS grant_picks (
  tenant_id  text        NOT NULL,
  agent_id   text        NOT NULL,
  notion_id  text        NOT NULL,   -- normalised: lower case, no hyphens
  kind       text        NOT NULL CHECK (kind IN ('database', 'page')),
  picked_by  text        NOT NULL,
  picked_at  timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, agent_id, notion_id)
);

GRANT USAGE ON SCHEMA public TO agentos_app;
REVOKE ALL ON grant_picks FROM agentos_app;
GRANT SELECT, INSERT, UPDATE ON grant_picks TO agentos_app;

ALTER TABLE grant_picks ENABLE ROW LEVEL SECURITY;
ALTER TABLE grant_picks FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON grant_picks;
CREATE POLICY tenant_isolation ON grant_picks
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

INSERT INTO audit_kinds (kind) VALUES ('grant_pick'), ('widening_confirm') ON CONFLICT DO NOTHING;
