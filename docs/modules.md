# Module map

One line per module under `src/agentos`: its purpose, the invariant it owns, and the tests that cover it. The test column lists test files by path under `tests/`, grouped in `core/`, `console/`, `knowledge/` and `connectors/`; `pg/` runs the SQL against a real Postgres. Status of each capability is in [status.md](status.md); the design is in [architecture.md](architecture.md).

## Entry points and wiring

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `worker.py` | Temporal worker: runs every workflow and activity, builds one Slack and one Notion client per tenant. Refuses to start on a missing secret, key, price or an old schema; a broken grant disables its agent (`agent_health.py`), never the worker | `core/test_config.py`, `core/test_pool_settings.py`, `core/test_agent_health.py` |
| `gateway/app.py` | HTTP ingress for Slack events, button clicks and Notion webhooks. Verifies signatures, takes the tenant from the request (never from text), stores each event before answering, handles a click within 2.0 s; a tenant with no default agent gets one fixed not-set-up reply per event; a feedback click records the signed clicker's vote; an uninstall event disables the tenant and deletes its Slack credential in one unit of work, audited `uninstall` | `core/test_gateway.py`, `connectors/test_notion_webhook.py`, `core/test_content_refs.py`, `core/test_front_door.py`, `core/test_answer_feedback.py`, `pg/test_slack_install.py` |
| `gateway/relay.py` | Signals conversation workflows with the events the gateway stored. At least once; one thread's messages are never delivered out of order | `core/test_content_refs.py` |
| `gateway/notion_signature.py` | Checks Notion's webhook signature in constant time | `connectors/test_notion_webhook.py` |
| `control_api.py` | The control-plane JSON API, a separate process from the gateway. Loopback only without sign-in; every request acts as one verified user | `console/test_control_api.py` |
| `web_ui.py`, `templates/` | Server-rendered console over the same `ControlPlane` methods. Decides nothing about roles; sets CSRF and CSP | `console/test_web_ui.py` |
| `index_knowledge.py` | Command that starts indexing runs, schedules them nightly, or confirms a gone root | `knowledge/test_indexing.py`, `knowledge/test_revoked_grants.py` |
| `retention.py` | Nightly purge of expired rows per tenant and the command that schedules it. Limits never fall below the longest a task can live | `core/test_retention_schedule.py`, `core/test_retention_workflow.py`, `pg/test_retention.py` |
| `switches.py` | Kill switches: the table, the gate's state, and the operator command. Read with no cache; an unreadable switch fails the step | `console/test_kill_switches.py`, `core/test_agent_gate.py` |
| `check_grants.py` | Command that checks Notion grants and prints property IDs | `connectors/test_grant_check.py` |
| `tenant_keys.py` | Command that creates or rotates a tenant's payload key (a file, or a wrapped database row in `db` mode), or prints a new key or master key value for a host variable; never overwrites or prints an existing key | `core/test_payload_codec.py`, `pg/test_tenant_payload_keys.py` |
| `notion_webhook.py` | The Notion webhook verification token, encrypted per tenant (pending, then confirmed); the operator command and the console's admin steps (through `ControlPlane`) show, confirm or clear it | `connectors/test_notion_webhook.py`, `pg/test_store_contracts.py` |
| `usage_reprice.py` | Command that prices a past month's unpriced usage rows by appending adjustments; never changes a stored row | `console/test_usage.py`, `pg/test_reprice.py` |

## Workflows and activities

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `workflows/conversation.py` | The turn loop for one thread. Deterministic; carries references, never content; changes to commands sit behind `workflow.patched` | `core/test_conversation_workflow.py`, `core/test_conversation_replay.py`, `core/test_turn_loop.py`, `core/test_turn_limits.py`, `core/test_turn_failure.py`, `core/test_front_door.py` |
| `workflows/task.py` | One unit of work: approval, write, watch. Only the requester approves; what was approved is what runs | `core/test_task_workflow.py`, `core/test_task_actions.py`, `core/test_task_failures.py`, `core/test_task_limits.py`, `connectors/test_restart_write.py` |
| `workflows/indexing.py` | Indexes one granted root page. History carries page IDs and counts; one bad page never fails the run | `knowledge/test_indexing.py`, `knowledge/test_knowledge_sync.py` |
| `workflows/page_refresh.py` | Refreshes one page after webhook events, coalescing a burst | `knowledge/test_page_refresh.py` |
| `workflows/audience_watch.py` | Acts on one `channel_shared` event; one never-reused ID per event | `console/test_audience_watch.py` |
| `workflows/retention.py` | Purges one tenant's expired rows, a batch per activity | `core/test_retention_workflow.py` |
| `activities.py` | All I/O the workflows do. One tenant per activity; the gate runs first in every activity that acts for an agent; writes happen once behind a lease; a final answer carries the feedback buttons, a fixed stop text none | `core/test_agent_gate.py`, `connectors/test_exactly_once.py`, `connectors/test_restart_write.py`, `core/test_retry_safety.py`, `core/test_tool_outputs.py`, `core/test_answer_feedback.py` |
| `task_notices.py` | The lines a task posts in the thread, written in one place from stored content | `core/test_task_workflow.py` |
| `models.py` | Plain data types passed through Temporal (IDs and small values), audit kinds, editable-field limits | `core/test_task_limits.py`, `core/test_payload_codec.py` |
| `ids.py` | The only place workflow, schedule and event-key IDs are built; every ID starts with the tenant | `core/test_ids.py` |
| `history_view.py` | Repairs and windows stored history before the model sees it | `core/test_turn_history.py`, `pg/test_history_window.py` |

## Content, stores and the database

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `content_store.py` | Where message text, model replies, tool inputs and task drafts live while Temporal carries references | `core/test_content_refs.py`, `pg/test_stores.py` |
| `feedback.py` | Answer feedback: the 👍/👎 buttons under an answer and one changeable vote per person per answer, with an optional reason on a 👎. The voter is the signed clicker, never the payload; the reason is the only free text and is purged with the row | `core/test_answer_feedback.py`, `pg/test_store_contracts.py` |
| `stores.py` | Chat history, idempotency records, task links and external writes. Every call takes `tenant_id` and runs under row-level security | `pg/test_stores.py` |
| `tasks.py` | The platform's record of each task: the state row the task workflow writes and `my_tasks` reads for the asker, open first, newest first, at most 10; the in-thread poke's lookup and the listed-state mapping | `core/test_my_tasks.py`, `pg/test_store_contracts.py` |
| `unit_of_work.py` | One transaction and the tenant lock for a change and its audit event | `console/test_control_plane.py`, `pg/test_publish_hash.py` |
| `threads.py` | The agent a thread was handed to, and the day an agent's owner was last mentioned in a refusal | `pg/test_stores.py` |
| `audit.py` | Append-only audit events: IDs and hashes only, one row per event key | `console/test_audit_log.py`, `pg/test_policies.py` |
| `schema_version.py` | Start-up check that the database has every migration the code expects | `pg/test_schema.py` |
| `sessions.py` | Console sessions kept server-side; only the SHA-256 of the session ID is stored | `console/test_web_ui.py`, `pg/test_stores.py` |
| `tenant_store.py` | Tenants in the database: one-time import, narrow cross-tenant reads, settings writes (routing, the Notion workspace, the enable flag in a unit of work), and the cached live view every process serves from; an empty table starts serving none; `disabled_tenants` names the stored-but-disabled ones | `console/test_tenant_store.py`, `pg/test_tenant_store_contract.py` |
| `tenants.py` | The tenants one process serves and each tenant's own Slack and Notion clients, built from the resolved credentials (row first, env fallback); a sync rebuilds a client whose credential changed and marks disabled tenants so their work ends cleanly; a Slack-only tenant has no Notion client; an unknown tenant never falls back to another | `console/test_tenants.py`, `console/test_tenant_store.py` |
| `credentials.py` | Per-tenant connector credentials, encrypted with the tenant's payload key; the resolvers for a tenant's Slack bot and Notion tokens (row first, env fallback, one key reload on a miss); Disconnect deletes the Notion row, uninstall the Slack bot row | `connectors/test_credentials.py`, `pg/test_credential_store.py` |
| `config.py` | Settings from the environment and agent definitions from YAML; the only module that reads environment variables. Validates every definition field and bound | `core/test_config.py`, `core/test_settings_env.py` |

## Agents, control and access

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `agent_store.py` | Agent versions and their publish lifecycle; a published version never changes; the one-time YAML import | `console/test_agent_store.py`, `pg/test_publish_hash.py`, `pg/test_agent_id_pattern.py` |
| `control.py` | The control plane: the only place roles are checked and the only writer (besides the operator command, the install and imports) of agents, members, budgets, switches and tenant settings; one audit event per change in the same transaction | `console/test_control_plane.py`, `console/test_publish_review.py`, `console/test_tenant_settings.py` |
| `control_errors.py` | Maps control-plane refusals to HTTP statuses, once | `console/test_web_ui.py` |
| `members.py` | Stored admins and creators; config-file admins are not rows | `console/test_control_plane.py` |
| `grant_checker.py` | Checks a version's Notion grants against the live objects before publish | `connectors/test_grant_check.py` |
| `agent_health.py` | A broken grant disables one agent, never the worker: the health row, the start-up and refresh checks, the fixed reply's refusal, and the once-per-transition audit and DMs. A transient Notion failure changes no health | `core/test_agent_health.py`, `pg/test_store_contracts.py` |
| `grant_picks.py` | Which granted objects were picked from the creator's own Notion account | `console/test_grant_review.py` |
| `notion_picker.py` | The creator's Notion sign-in, used once to list what they can see (no token stored), and Connect Notion's workspace authorization, whose token the control plane stores encrypted | `console/test_grant_review.py`, `console/test_tenant_settings.py` |
| `access_diff.py` | What a version adds compared with the published one, and whether it widens access | `console/test_grant_review.py` |
| `slack_signin.py` | Sign in with Slack (OpenID Connect) for the console and the session identity | `console/test_slack_sign_in.py` |
| `slack_install.py` | The Slack OAuth install: the manifest's bot scopes, the code exchange, the pilot allowlist, and the unit of work that creates the tenant, key, encrypted token and first admin (a reinstall replaces the token and re-enables a disabled tenant) | `console/test_slack_install.py`, `pg/test_slack_install.py` |

## Audience, budgets and cost

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `audience.py` | Who may use an agent where: identity, place and agent come from the runtime, never the model; a lookup that fails refuses | `console/test_audience.py` |
| `audience_pauses.py` | Paused channels: one row per agent and channel, resumed rows stay as history | `console/test_audience_watch.py` |
| `audience_watch.py` | Decides from a Slack event which agents to pause and tells the people accountable | `console/test_audience_watch.py` |
| `budgets.py` | Monthly dollar budgets and the stop before a model call; unknown spend is not zero; a read failure fails the call | `console/test_budgets.py` |
| `usage.py` | One usage row per model call or embedding, priced when recorded, never holding text or the requester | `console/test_usage.py` |

## Connectors, knowledge and models

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `connectors/notion.py` | The Notion connector. The agent's grants are checked before a call that names a database; an ungranted page is indistinguishable from a missing one | `connectors/test_notion_connector.py`, `knowledge/test_notion_reads.py`, `connectors/test_notion_transport.py` |
| `connectors/manifest.py`, `connectors/manifests/` | Safety metadata per tool (risk, idempotency, requester scoping, input schema) that the runtime reads | `connectors/test_notion_connector.py`, `core/test_task_actions.py` |
| `connectors/notion_errors.py` | The one table that says whether a Notion error is worth retrying | `connectors/test_notion_transport.py` |
| `connectors/notion_limiter.py` | Per-tenant request-rate bucket for Notion, interactive ahead of bulk | `knowledge/test_notion_reads.py`, `connectors/test_notion_transport.py` |
| `knowledge/store.py` | Indexed pages and chunks; the tenant and scope filter is part of the SQL, never applied to results | `knowledge/test_knowledge_store.py`, `pg/test_stores.py` |
| `knowledge/indexing.py` | Activities that index a page tree; one tenant per activity; page text never enters workflow history | `knowledge/test_indexing.py`, `knowledge/test_knowledge_sync.py`, `knowledge/test_revoked_grants.py` |
| `knowledge/search.py` | The re-check against Notion, the model-facing output and citations; retrieved text is untrusted data | `knowledge/test_knowledge_search.py` |
| `knowledge/notion_text.py` | Turns a Notion page into text and chunks | `knowledge/test_indexing.py` |
| `knowledge/embedder.py` | Embeddings behind one interface for any OpenAI-compatible endpoint, and a deterministic fake | `knowledge/test_embed_endpoint.py`, `knowledge/test_knowledge_sync.py` |
| `llm.py` | The LLM client (Claude or Qwen); history is stored in one format whichever provider runs | `core/test_llm.py`, `core/test_llm_errors.py` |
| `slack_client.py` | Posting to Slack, and the lookups the audience checks and the channel picker use | `core/test_slack_rate_limits.py`, `core/test_client_timeouts.py` |
| `slack_format.py` | Markdown to Slack mrkdwn out, Slack's encoding decoded in; text the runtime did not write is escaped | `core/test_slack_format.py`, `core/test_slack_escaping.py` |

## Encryption

| Module | Purpose and the invariant it owns | Tests |
|---|---|---|
| `payload_codec.py` | Encrypts every Temporal payload with its tenant's key, bound to the workflow ID; the tenant comes from the workflow ID only | `core/test_payload_codec.py`, `core/test_encrypted_flow.py` |
| `crypto_keys.py` | Per-tenant payload keys from owner-only files, host variables, or the database wrapped by master keys, chosen in one factory every process uses; the seam where a cloud key service would replace them | `core/test_payload_codec.py`, `core/test_key_store_factory.py`, `pg/test_tenant_payload_keys.py` |
| `temporal_client.py` | The one way to connect to Temporal, so every client encrypts its payloads | `core/test_encrypted_flow.py` |

## Outside `src/agentos`

| Path | What it is |
|---|---|
| `scripts/init_db.sql` | Database schema, applied by Compose on first start; equals the migrations (checked by `pg/test_schema.py`) |
| `scripts/migrate.py`, `scripts/migrations/` | Applies the migrations a database lacks and records them in `schema_migrations` |
| `scripts/setup-wizard.sh` | Walks a human through the tunnel, Slack app and Notion setup |
| `config/` | The example tenant file, the active tenant file and the price list |
| `slack/app-manifest.yaml` | The Slack app's events and scopes |
