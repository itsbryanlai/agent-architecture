-- The notice audit kind: a final text a turn shows that is not a model answer (the hand-off
-- announcement, the step-limit notice, the failure notice, the empty-message prompt) records
-- one notice event, the reason in tool and the hash of the text shown in output_sha256.
-- Run as the owner role, after 026. Safe to run more than once.

INSERT INTO audit_kinds (kind) VALUES ('notice') ON CONFLICT DO NOTHING;
