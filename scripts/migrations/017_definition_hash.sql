-- The hash of the definition an admin is asked to approve. Submit stores it on the version
-- (SHA-256 of the canonical JSON of the definition), the version page shows the admin that
-- version, and a publish names the hash it was shown. The publish is refused when the version
-- now carries another hash, which is what a withdraw, an edit and a resubmit of the same
-- version number produces. A draft has no hash; it is set by the move to pending and cleared
-- by the move back to draft; once a version is published nothing can change it.
-- Existing pending, published and superseded rows are backfilled with a digest of their jsonb
-- text. That is not the canonical-JSON digest new submits store, so a backfilled hash differs
-- from the digest in old audit rows; a pending version shown before this migration must be
-- reopened before it is published. The backfill reads every tenant's rows, so run this as the
-- owner role that bypasses row-level security, as the earlier migrations assume.
-- Run as the owner role, after 015. Safe to run more than once.


ALTER TABLE agent_versions ADD COLUMN IF NOT EXISTS definition_sha256 text;

-- The guard forbids changes to published rows; the backfill is the one change it must allow.
ALTER TABLE agent_versions DISABLE TRIGGER agent_versions_guard;
UPDATE agent_versions
SET definition_sha256 = encode(sha256(convert_to(definition::text, 'UTF8')), 'hex')
WHERE status IN ('pending', 'published', 'superseded') AND definition_sha256 IS NULL;
ALTER TABLE agent_versions ENABLE TRIGGER agent_versions_guard;

ALTER TABLE agent_versions DROP CONSTRAINT IF EXISTS agent_versions_hash_matches_status;
ALTER TABLE agent_versions ADD CONSTRAINT agent_versions_hash_matches_status CHECK (
  (status = 'draft') = (definition_sha256 IS NULL));

CREATE OR REPLACE FUNCTION agent_versions_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    IF OLD.status <> 'draft' THEN
      RAISE EXCEPTION 'agent_versions: only a draft can be deleted (status is %)', OLD.status;
    END IF;
    RETURN OLD;
  END IF;

  IF (NEW.tenant_id, NEW.agent_id, NEW.version, NEW.created_by, NEW.created_at)
     IS DISTINCT FROM (OLD.tenant_id, OLD.agent_id, OLD.version, OLD.created_by, OLD.created_at)
  THEN
    RAISE EXCEPTION 'agent_versions: the identity of a version cannot change';
  END IF;

  IF OLD.status IN ('published', 'superseded') THEN
    -- Frozen. The one move is published -> superseded, and only the status changes.
    IF NOT (OLD.status = 'published' AND NEW.status = 'superseded'
            AND (NEW.definition, NEW.definition_sha256, NEW.submitted_by, NEW.submitted_at,
                 NEW.approved_by, NEW.published_at)
                IS NOT DISTINCT FROM
                (OLD.definition, OLD.definition_sha256, OLD.submitted_by, OLD.submitted_at,
                 OLD.approved_by, OLD.published_at))
    THEN
      RAISE EXCEPTION 'agent_versions: a % version is immutable', OLD.status;
    END IF;
    RETURN NEW;
  END IF;

  IF NEW.definition IS DISTINCT FROM OLD.definition AND OLD.status <> 'draft' THEN
    RAISE EXCEPTION 'agent_versions: the definition changes only while a version is a draft';
  END IF;

  IF NEW.definition_sha256 IS DISTINCT FROM OLD.definition_sha256 AND NOT (
       (OLD.status = 'draft'   AND NEW.status = 'pending')
    OR (OLD.status = 'pending' AND NEW.status = 'draft')
  ) THEN
    RAISE EXCEPTION 'agent_versions: the definition hash changes only when a version is submitted or withdrawn';
  END IF;

  IF NEW.status <> OLD.status AND NOT (
       (OLD.status = 'draft'   AND NEW.status = 'pending')
    OR (OLD.status = 'pending' AND NEW.status IN ('draft', 'published'))
  ) THEN
    RAISE EXCEPTION 'agent_versions: % -> % is not allowed', OLD.status, NEW.status;
  END IF;
  RETURN NEW;
END
$$;

