-- Adds the per-step key that makes history appends safe to retry.
-- Rows written before this migration keep a NULL step_key.
ALTER TABLE conversation_messages ADD COLUMN IF NOT EXISTS step_key text;
CREATE UNIQUE INDEX IF NOT EXISTS conversation_messages_step_key
  ON conversation_messages (tenant_id, conversation_id, step_key);
