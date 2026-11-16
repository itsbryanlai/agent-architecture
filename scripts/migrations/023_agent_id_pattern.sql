-- Agent IDs follow one pattern: 2 to 40 lower-case ASCII letters, digits and hyphens, starting
-- with a letter (agentos.config.AGENT_ID_PATTERN). An ID with a slash cannot be routed to by the
-- console, a look-alike letter can pass for another agent in an approval, and a colon makes an
-- audit key ambiguous. Every table column that holds an agent ID gets a CHECK, and so does the
-- target of an agent kill switch (the target holds a connector name for the other scope).
-- A row that already breaks the pattern stops the migration with its table, tenant and ID in the
-- message; nothing is changed until each is fixed or removed by hand. The scan reads every
-- tenant's rows, so run it as the owner role that bypasses row-level security.
-- Run as the owner role, after 021. Safe to run more than once.

DO $$
DECLARE
  bad text;
BEGIN
  SELECT string_agg(format('%s %s/%s', src, tenant_id, agent_id), '; ' ORDER BY src, tenant_id, agent_id)
  INTO bad
  FROM (
    SELECT DISTINCT 'agents' AS src, tenant_id, agent_id FROM agents
    UNION ALL SELECT DISTINCT 'agent_versions', tenant_id, agent_id FROM agent_versions
    UNION ALL SELECT DISTINCT 'audit_events', tenant_id, agent_id FROM audit_events
    UNION ALL SELECT DISTINCT 'usage_events', tenant_id, agent_id FROM usage_events
    UNION ALL SELECT DISTINCT 'budgets', tenant_id, agent_id FROM budgets
    UNION ALL SELECT DISTINCT 'audience_pauses', tenant_id, agent_id FROM audience_pauses
    UNION ALL SELECT DISTINCT 'grant_picks', tenant_id, agent_id FROM grant_picks
    UNION ALL SELECT DISTINCT 'kill_switches', tenant_id, target FROM kill_switches
      WHERE scope = 'agent'
  ) AS ids
  WHERE agent_id IS NOT NULL AND agent_id !~ '^[a-z][a-z0-9-]{1,39}$';
  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'agent IDs that break the pattern (tenant/agent): %', bad;
  END IF;
END
$$;

ALTER TABLE agents DROP CONSTRAINT IF EXISTS agents_agent_id_pattern;
ALTER TABLE agents ADD CONSTRAINT agents_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE agent_versions DROP CONSTRAINT IF EXISTS agent_versions_agent_id_pattern;
ALTER TABLE agent_versions ADD CONSTRAINT agent_versions_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE audit_events DROP CONSTRAINT IF EXISTS audit_events_agent_id_pattern;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_agent_id_pattern
  CHECK (agent_id IS NULL OR agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE usage_events DROP CONSTRAINT IF EXISTS usage_events_agent_id_pattern;
ALTER TABLE usage_events ADD CONSTRAINT usage_events_agent_id_pattern
  CHECK (agent_id IS NULL OR agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE budgets DROP CONSTRAINT IF EXISTS budgets_agent_id_pattern;
ALTER TABLE budgets ADD CONSTRAINT budgets_agent_id_pattern
  CHECK (agent_id IS NULL OR agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE audience_pauses DROP CONSTRAINT IF EXISTS audience_pauses_agent_id_pattern;
ALTER TABLE audience_pauses ADD CONSTRAINT audience_pauses_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE grant_picks DROP CONSTRAINT IF EXISTS grant_picks_agent_id_pattern;
ALTER TABLE grant_picks ADD CONSTRAINT grant_picks_agent_id_pattern
  CHECK (agent_id ~ '^[a-z][a-z0-9-]{1,39}$');

ALTER TABLE kill_switches DROP CONSTRAINT IF EXISTS kill_switches_agent_target_pattern;
ALTER TABLE kill_switches ADD CONSTRAINT kill_switches_agent_target_pattern
  CHECK (scope <> 'agent' OR target ~ '^[a-z][a-z0-9-]{1,39}$');
