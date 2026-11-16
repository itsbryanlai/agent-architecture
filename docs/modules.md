# Module map

One line per module under `src/agentos`: its purpose, the invariant it owns, and the tests that cover it. The test column lists the current test file names under `tests/`; `tests/pg/` runs the SQL against a real Postgres. Status of each capability is in [status.md](status.md); the design is in [architecture.md](architecture.md).

## Entry points and wiring

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `worker.py` | Temporal worker: runs every workflow and activity, builds one Slack and one Notion client per tenant. Refuses to start on a missing secret, key, price, grant check or an old schema | `test_config.py`, `test_pool_settings.py`, `test_grant_check.py` |
| `gateway/app.py` | HTTP ingress for Slack events, button clicks and Notion webhooks. Verifies signatures, takes the tenant from the request (never from text), stores each event before answering, handles a click within 2.0 s | `test_gateway.py`, `test_notion_webhook.py`, `test_content_refs.py`, `test_front_door.py` |
| `gateway/relay.py` | Signals conversation workflows with the events the gateway stored. At least once; one thread's messages are never delivered out of order | `test_content_refs.py` |
| `gateway/notion_signature.py` | Checks Notion's webhook signature in constant time | `test_notion_webhook.py` |
| `control_api.py` | The control-plane JSON API, a separate process from the gateway. Loopback only without sign-in; every request acts as one verified user | `test_control_api.py` |
| `web_ui.py`, `templates/` | Server-rendered console over the same `ControlPlane` methods. Decides nothing about roles; sets CSRF and CSP | `test_web_ui.py` |
| `index_knowledge.py` | Command that starts indexing runs, schedules them nightly, or confirms a gone root | `test_indexing.py`, `test_revoked_grants.py` |
| `retention.py` | Nightly purge of expired rows per tenant and the command that schedules it. Limits never fall below the longest a task can live | `test_retention_schedule.py`, `test_retention_workflow.py`, `tests/pg/test_retention.py` |
| `switches.py` | Kill switches: the table, the gate's state, and the operator command. Read with no cache; an unreadable switch fails the step | `test_kill_switches.py`, `test_agent_gate.py` |
| `check_grants.py` | Command that checks Notion grants and prints property IDs | `test_grant_check.py` |
| `tenant_keys.py` | Command that creates or rotates a tenant's payload key; never overwrites | `test_payload_codec.py` |
| `usage_reprice.py` | Command that prices a past month's unpriced usage rows by appending adjustments; never changes a stored row | `test_usage.py`, `tests/pg/test_reprice.py` |

## Workflows and activities

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `workflows/conversation.py` | The turn loop for one thread. Deterministic; carries references, never content; changes to commands sit behind `workflow.patched` | `test_conversation_workflow.py`, `test_conversation_replay.py`, `test_turn_loop.py`, `test_turn_limits.py`, `test_turn_failure.py`, `test_front_door.py` |
| `workflows/task.py` | One unit of work: approval, write, watch. Only the requester approves; what was approved is what runs | `test_task_workflow.py`, `test_task_actions.py`, `test_task_failures.py`, `test_task_limits.py`, `test_restart_write.py` |
| `workflows/indexing.py` | Indexes one granted root page. History carries page IDs and counts; one bad page never fails the run | `test_indexing.py`, `test_knowledge_sync.py` |
| `workflows/page_refresh.py` | Refreshes one page after webhook events, coalescing a burst | `test_page_refresh.py` |
| `workflows/audience_watch.py` | Acts on one `channel_shared` event; one never-reused ID per event | `test_audience_watch.py` |
| `workflows/retention.py` | Purges one tenant's expired rows, a batch per activity | `test_retention_workflow.py` |
| `activities.py` | All I/O the workflows do. One tenant per activity; the gate runs first in every activity that acts for an agent; writes happen once behind a lease | `test_agent_gate.py`, `test_exactly_once.py`, `test_restart_write.py`, `test_retry_safety.py`, `test_tool_outputs.py` |
| `task_notices.py` | The lines a task posts in the thread, written in one place from stored content | `test_task_workflow.py` |
| `models.py` | Plain data types passed through Temporal (IDs and small values), audit kinds, editable-field limits | `test_task_limits.py`, `test_payload_codec.py` |
| `ids.py` | The only place workflow, schedule and event-key IDs are built; every ID starts with the tenant | `test_ids.py` |
| `history_view.py` | Repairs and windows stored history before the model sees it | `test_turn_history.py`, `tests/pg/test_history_window.py` |

## Content, stores and the database

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `content_store.py` | Where message text, model replies, tool inputs and task drafts live while Temporal carries references | `test_content_refs.py`, `test_stores.py` |
| `stores.py` | Chat history, idempotency records, task links and external writes. Every call takes `tenant_id` and runs under row-level security | `test_stores.py`, `tests/pg/test_stores.py` |
| `unit_of_work.py` | One transaction and the tenant lock for a change and its audit event | `test_control_plane.py`, `tests/pg/test_publish_hash.py` |
| `threads.py` | The agent a thread was handed to, and the day an agent's owner was last mentioned in a refusal | `test_stores.py` |
| `audit.py` | Append-only audit events: IDs and hashes only, one row per event key | `test_audit_log.py`, `tests/pg/test_policies.py` |
| `schema_version.py` | Start-up check that the database has every migration the code expects | `tests/pg/test_schema.py` |
| `sessions.py` | Console sessions kept server-side; only the SHA-256 of the session ID is stored | `test_web_ui.py`, `test_stores.py` |
| `tenant_store.py` | Tenants in the database: one-time import, narrow cross-tenant reads, and the cached live view every process serves from | `test_tenant_store.py` |
| `tenants.py` | The tenants one process serves and each tenant's own Slack and Notion clients; an unknown tenant never falls back to another | `test_tenants.py` |
| `config.py` | Settings from the environment and agent definitions from YAML; the only module that reads environment variables. Validates every definition field and bound | `test_config.py`, `test_settings_env.py` |

## Agents, control and access

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `agent_store.py` | Agent versions and their publish lifecycle; a published version never changes; the one-time YAML import | `test_agent_store.py`, `tests/pg/test_publish_hash.py`, `tests/pg/test_agent_id_pattern.py` |
| `control.py` | The control plane: the only place roles are checked and the only writer (besides the operator command and imports) of agents, members, budgets and switches; one audit event per change in the same transaction | `test_control_plane.py`, `test_publish_review.py` |
| `control_errors.py` | Maps control-plane refusals to HTTP statuses, once | `test_web_ui.py` |
| `members.py` | Stored admins and creators; config-file admins are not rows | `test_control_plane.py` |
| `grant_checker.py` | Checks a version's Notion grants against the live objects before publish | `test_grant_check.py` |
| `grant_picks.py` | Which granted objects were picked from the creator's own Notion account | `test_grant_review.py` |
| `notion_picker.py` | The creator's Notion sign-in, used once to list what they can see; no token stored | `test_grant_review.py` |
| `access_diff.py` | What a version adds compared with the published one, and whether it widens access | `test_grant_review.py` |
| `slack_signin.py` | Sign in with Slack (OpenID Connect) for the console and the session identity | `test_slack_sign_in.py` |

## Audience, budgets and cost

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `audience.py` | Who may use an agent where: identity, place and agent come from the runtime, never the model; a lookup that fails refuses | `test_audience.py` |
| `audience_pauses.py` | Paused channels: one row per agent and channel, resumed rows stay as history | `test_audience_watch.py` |
| `audience_watch.py` | Decides from a Slack event which agents to pause and tells the people accountable | `test_audience_watch.py` |
| `budgets.py` | Monthly dollar budgets and the stop before a model call; unknown spend is not zero; a read failure fails the call | `test_budgets.py` |
| `usage.py` | One usage row per model call or embedding, priced when recorded, never holding text or the requester | `test_usage.py` |

## Connectors, knowledge and models

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `connectors/notion.py` | The Notion connector. The agent's grants are checked before a call that names a database; an ungranted page is indistinguishable from a missing one | `test_notion_connector.py`, `test_notion_reads.py`, `test_notion_transport.py` |
| `connectors/manifest.py`, `connectors/manifests/` | Safety metadata per tool (risk, idempotency, requester scoping, input schema) that the runtime reads | `test_notion_connector.py`, `test_task_actions.py` |
| `connectors/notion_errors.py` | The one table that says whether a Notion error is worth retrying | `test_notion_transport.py` |
| `connectors/notion_limiter.py` | Per-tenant request-rate bucket for Notion, interactive ahead of bulk | `test_notion_reads.py`, `test_notion_transport.py` |
| `knowledge/store.py` | Indexed pages and chunks; the tenant and scope filter is part of the SQL, never applied to results | `test_knowledge_store.py`, `tests/pg/test_stores.py` |
| `knowledge/indexing.py` | Activities that index a page tree; one tenant per activity; page text never enters workflow history | `test_indexing.py`, `test_knowledge_sync.py`, `test_revoked_grants.py` |
| `knowledge/search.py` | The re-check against Notion, the model-facing output and citations; retrieved text is untrusted data | `test_knowledge_search.py` |
| `knowledge/notion_text.py` | Turns a Notion page into text and chunks | `test_indexing.py` |
| `knowledge/embedder.py` | Embeddings behind one interface for any OpenAI-compatible endpoint, and a deterministic fake | `test_embed_endpoint.py`, `test_knowledge_sync.py` |
| `llm.py` | The LLM client (Claude or Qwen); history is stored in one format whichever provider runs | `test_llm.py`, `test_llm_errors.py` |
| `slack_client.py` | Posting to Slack, and the lookups the audience checks use | `test_slack_rate_limits.py`, `test_client_timeouts.py` |
| `slack_format.py` | Markdown to Slack mrkdwn out, Slack's encoding decoded in; text the runtime did not write is escaped | `test_slack_format.py`, `test_slack_escaping.py` |

## Encryption

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `payload_codec.py` | Encrypts every Temporal payload with its tenant's key, bound to the workflow ID; the tenant comes from the workflow ID only | `test_payload_codec.py`, `test_encrypted_flow.py` |
| `crypto_keys.py` | Per-tenant key files, owner-only; the one seam where a cloud key service would replace local files | `test_payload_codec.py` |
| `temporal_client.py` | The one way to connect to Temporal, so every client encrypts its payloads | `test_encrypted_flow.py` |

## Outside `src/agentos`

| Path | What it is |
|---|---|
| `scripts/init_db.sql` | Database schema, applied by Compose on first start; equals the migrations (checked by `tests/pg/test_schema.py`) |
| `scripts/migrate.py`, `scripts/migrations/` | Applies the migrations a database lacks and records them in `schema_migrations` |
| `scripts/setup-wizard.sh` | Walks a human through the tunnel, Slack app and Notion setup |
| `config/` | The example tenant file, the active tenant file and the price list |
| `slack/app-manifest.yaml` | The Slack app's events and scopes |
