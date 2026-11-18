-- The approval_closed audit kind: an approval that closed without a write (the requester
-- declined it, or it expired) records one approval_closed event, declined or expired in tool
-- and the hash of the proposed payload in input_sha256.
-- Run as the owner role, after 027. Safe to run more than once.

INSERT INTO audit_kinds (kind) VALUES ('approval_closed') ON CONFLICT DO NOTHING;
