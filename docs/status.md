# Status: what is built

Last updated 2026-11-20. One row per capability. `built` means the code does what [architecture.md](architecture.md) says; `partial` means part of it, with the gap named; `target` means designed and not in the code. A difference between this table and the code is a defect in this table.

The design is in [architecture.md](architecture.md), the reasons in [decisions.md](decisions.md), the product scope in [product.md](product.md), and a map of the code in [modules.md](modules.md). Nothing here changes the v1 scope or the roadmap: product scope items that are not in the code are listed as `target`.

| Capability | Status | Where in code | Gap or note |
|---|---|---|---|
| Slack DM and mention reach a conversation workflow | built | `gateway/app.py`, `gateway/relay.py`, `workflows/conversation.py` | The gateway stores the event and answers at once; a relay signals Temporal |
| Content kept out of Temporal history | built | `content_store.py`, `workflows/` | Workflows running at the deploy keep their earlier shape until they end (D-26) |
| Front door and hand-off to specialists | built | `workflows/conversation.py`, `threads.py` | A specialist cannot hand off |
| Notion knowledge: scoped search, live re-check, citations | built | `knowledge/`, `activities.py` | Child database rows indexed; text deeper than 5 levels is not. An answer that cites none of its turn's passages shows a Searched line naming those pages (behind the `searched-footer` patch marker); every search logs its hits, cut-off drops and kept pages' distances |
| Webhook page refresh | built | `workflows/page_refresh.py`, `gateway/app.py` | |
| Nightly indexing, on-demand indexing | built | `workflows/indexing.py`, `index_knowledge.py` | A gone granted root waits for three runs or an admin confirmation |
| Approval card: requester only, nonce, edit, expiry, reminder | built | `workflows/task.py`, `activities.py` | |
| Exactly-once Notion create and comment | built | `activities.py` (`_write_once`), `external_writes` table | A target whose search lags longer than the look-before-write window can get a second item from a retry (accepted) |
| Slack posts land once | built | `activities.py` (`_post_once`), `slack_client.py` (`find_post`) | Each post carries a marker in its message metadata and goes through the `external_writes` lease; a retry after a send looks the post up first. Needs the history scopes; without them a retry can repeat a post and logs it. Not yet run against a real workspace |
| Pending cards stop for agent off, pause or lost audience | built | `activities.py` (`check_before_write`), `workflows/task.py` | |
| Follow-up on status to "marked Done" | built | `workflows/task.py` | |
| Long task watch: continue-as-new | built | `workflows/task.py`, `models.py` (`WatchState`) | `WatchState` carries references and small values only; a hop at 1000 events or when Temporal suggests it; a burst of pokes answered once per 60 s |
| Replay fixtures for workflow changes | built | `scripts/record_replay_fixtures.py`, `tests/fixtures/replay/` | Recorded in a legacy shape and a reference shape under the encrypting converter; old fixtures are kept; `--check` replays them |
| "No movement" follow-up | partial | `workflows/task.py`, `task_notices.py` | One notice; no nudge offer and no action |
| Notion connector (read, create item, comment) | built | `connectors/notion.py` | |
| Connector manifest: risk, idempotency, requester scoping | built | `connectors/manifest.py`, `connectors/manifests/notion.yaml` | Risk class is read; no admin adjustment |
| Google Drive connector (read and write) | target | none | In the v1 scope in product.md; Confluence and Jira are out of scope |
| MCP client, OpenAPI adapter, certified and custom tiers | target | none | Deferred, not decided against (D-28) |
| Admin-adjustable approval policy, risk-class ceiling, second approver | target | none | Requester is the only approver (Q-5 closed) |
| Per-action approver chosen by the creator | target | none | |
| Requester-scoped reads, identity by email | built | `connectors/notion.py` | Result is `UnknownRequester` with no match |
| User delegation (requester's own token) | target | none | |
| Agent versions, roles, publish, hash-bound approval | built | `agent_store.py`, `control.py` | |
| Change and audit event in one transaction | built | `unit_of_work.py`, `control.py` | Switches command and one-time imports run outside `ControlPlane` on purpose |
| Grants checked before every connector call | built | `connectors/notion.py` | |
| Grant check at publish, access diff, widening acknowledgement | built | `grant_checker.py`, `access_diff.py`, `control.py` | |
| Agent health: a broken grant disables one agent, never the worker | built | `agent_health.py`, `worker.py`, `activities.py` | Fixed reply and refused Submit while unavailable; creator and admins told once per transition; re-checked on the directory refresh (D-35) |
| Notion picker (creator may grant only what they can see) | partial | `notion_picker.py`, `grant_picks.py` | Only when configured; without it the admin's approval is the only check |
| Nightly re-check of creators' access, suspension, notification | target | none | |
| Creators read only their own agents | built | `control.py` (`Principal`) | |
| Console, Sign in with Slack, sessions, CSRF | built | `web_ui.py`, `slack_signin.py`, `sessions.py`, `control_api.py` | |
| Console connectors page, tenant-settings page | partial | `web_ui.py`, `control.py`, `notion_picker.py`, `credentials.py`, migration 034 | The tenant-settings page is built (default agent, channel routing, Connect Notion by OAuth, webhook confirm); connector health status (last success, last error) is the follow-up. The connectors page is in the v1 scope in product.md |
| Audit log, append-only | built | `audit.py`, migration 005 | |
| Audit export | target | none | |
| Usage and cost, CSV, repricing | built | `usage.py`, `usage_reprice.py` | |
| Monthly budgets, including the unknown-spend stop | built | `budgets.py` | Overspend is bounded, not zero (D-21) |
| Kill switches, agent-off cancels approved writes | built | `switches.py`, `activities.py` | |
| Audience (channels, DMs, guests), publish confirmation | built | `audience.py`, `control.py` | |
| Audience watch: pause on a shared channel | partial | `audience_watch.py`, `audience_pauses.py`, `workflows/audience_watch.py` | Built and tested with fakes; never run against a real Slack Connect channel |
| Row-level security | built | `scripts/init_db.sql`, migrations, `tests/pg/` | Run against a real Postgres by the `pg` test tier |
| Tenants in Postgres with live reload | built | `tenant_store.py` | |
| Retention per tenant, nightly | built | `retention.py`, `workflows/retention.py` | Audit and usage rows exempt |
| Temporal payload encryption | partial | `payload_codec.py`, `crypto_keys.py`, `tenant_keys.py` | Local versioned key files, host variables (`AGENTOS_KEY_SOURCE=env`), or database-stored keys wrapped by a master key (`db`) with rotation; no KMS. Patch marker records are outside the codec |
| Credential vault, per-tenant key from a KMS, crypto-shredding | target | none | Secrets are environment variables named in the tenant's settings, except the Slack bot token, the Notion token and the Notion webhook token, stored encrypted per tenant in the database |
| `tenant_id` search attribute | target | none | |
| Per-tenant rate limits | partial | `connectors/notion_limiter.py` | Notion calls only, per tenant per worker process; none for Slack or the model |
| Model allowlist, bring-your-own LLM key | target | none | |
| Qwen as a chat provider, local embeddings | built | `llm.py`, `knowledge/embedder.py` | |
| Slack OAuth install, Marketplace listing | partial | `slack_install.py`, `credentials.py`, `web_ui.py`, migration 032 | "Add to Slack" creates the tenant, key, encrypted bot token and first admin (pilot allowlist); a not-set-up tenant gets a fixed reply; no Marketplace listing. Not yet run against a real workspace |
| Progress updates on the placeholder | built | `workflows/conversation.py`, `activities.py` | Fixed runtime labels per tool, throttled in workflow time; the final reply replaces them, or is posted as a new message when a progress edit was dropped (so a late edit cannot hide it). Behind the `progress` and `progress-dropped` patch markers |
| Token budget per turn, tenant or agent; cap on writes per task | target | none | Built: step limit, reply token limit, history window, `max_writes_per_turn` |
| Nightly retention of the audit log | target | none | Audit rows are kept |
| Worker Versioning | target | none | `workflow.patched` markers are used |
| OpenTelemetry traces | target | none | |
| Evals and agent test sets | target | none | |
| Creator feedback page (unanswered questions) | target | none | Named in product.md |
| Connector self-repair | target | none | Design in [design/self-repair.md](design/self-repair.md) |
| Cells (a stack per group of tenants) | target | none | One deployment today |
