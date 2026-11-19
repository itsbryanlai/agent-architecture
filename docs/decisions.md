# Decision log

Status values: **Decided** (agreed by the product owner), **Proposed** (recommended, awaiting confirmation), **Open** (needs input).

Last reviewed 2026-11-16. What each decision means for the code today is in [status.md](status.md).

| ID | Decision | Status |
|---|---|---|
| D-01 | v1 is an agent builder for approved creators plus a front-door assistant. Company assistant and broader scope go on the roadmap | Decided |
| D-02 | The architecture targets agents that do work (actions, approvals, multi-day follow-up) from day one | Decided |
| D-03 | Multi-tenant SaaS with a cell architecture. A customer-hosted deployment later is a single-tenant cell. Today there is one cell | Decided |
| D-04 | Thin custom agent loop on Temporal. No LangChain, LangGraph, or CrewAI | Decided |
| D-05 | Slack is the only chat platform in v1 | Decided |
| D-06 | Only creators approved by an admin can create agents | Decided |
| D-07 | Each agent is its own principal with access grants set by its creator. The creator is accountable for whether the agent's audience may see or change what the agent can reach | Decided |
| D-08 | MCP as the connector contract, wrapped by our runtime for credentials, grants, risk class, idempotency, and audit. **Deferred, not decided against** (D-28): the runtime reads a connector manifest and calls Notion directly | Decided |
| D-09 | Retrieval is filtered by the agent's granted scopes inside the vector search, then re-checked against the source | Decided |
| D-10 | Conversations and tasks are separate workflows. Tasks are child workflows with `ABANDON` as parent-close policy | Decided |
| D-11 | All write actions need approval by an authorized person by default. Only a tenant admin can relax this, per action (relaxing is not built: every write needs the requester's approval) | Decided |
| D-12 | Chat history and tool outputs live outside Temporal. Temporal payloads are encrypted with a per-tenant key. **Amended by D-27:** the key is a local versioned file, not a KMS key | Decided |
| D-13 | v1 write action: add items to a Notion database (such as a request tracker) and comment on them, with durable follow-up until the item's status reaches done. Jira moves to v2 | Decided |
| D-14 | Python on the Temporal Python SDK; Postgres with row-level security; pgvector for v1 | Decided |
| D-15 | Connectors are defined by a manifest (risk class, idempotency, attribution, watch, indexing) on top of MCP or OpenAPI. Two tiers: certified (by us, exactly-once guaranteed) and custom (by tenant admins, conservative defaults). Today the manifest carries risk, idempotency, requester scoping and input schema; the MCP and OpenAPI layer and the tiers are deferred (D-28) | Proposed |
| D-16 | Connector self-repair (roadmap, design in [design/self-repair.md](design/self-repair.md)): connector failures go into a Temporal remediation workflow (one per failure signature) with a circuit breaker; a sandboxed coding agent proposes fixes; humans approve deploys; failed writes are never replayed automatically | Proposed |
| D-17 | Requester-scoped tools: the runtime fills in the requester's identity (the LLM can't set it) and answers where the question was asked, since the audience rule (D-07) covers visibility. The requester's own account is used only for systems whose API can't filter by person, and those answers go only to a DM (the user-delegation exception is not built) | Decided |
| D-18 | Agents, versions and members live in Postgres behind one service layer that holds every role check and audits every change, in the same transaction as the change. A tenant's first admins come from its config file; an admin may approve their own submission, and the audit records it | Decided |
| D-19 | The control plane is a separate process from the Slack gateway. It listens on loopback with a fixed operator until Sign in with Slack is configured; then it may be public, with server-side sessions, CSRF tokens and a browser guard that lets only page navigations in from other sites | Decided |
| D-20 | Tenants live in Postgres, not in the YAML file. The file is imported once; every process serves from the table through a short cache, so a tenant can be added, changed or disabled without a restart | Decided |
| D-21 | Monthly dollar budgets per tenant and per agent stop spend, and spend that cannot be measured stops it too. A reached tenant budget also stops indexing | Decided |
| D-22 | Kill switches (an agent, all agents, a connector, all writes) apply on the next step, and turning an agent off also cancels a write the requester already approved that has not run | Decided |
| D-23 | A channel pause, or the requester leaving the agent's audience, also stops pending approval cards: the task re-checks both just before the write | Decided |
| D-24 | Audience changes are watched only for a channel shared with another organisation. People joining a channel are not watched | Decided |
| D-25 | Qwen is a second chat provider, and embeddings may come from any OpenAI-compatible endpoint, including a local Ollama model. The embedding vector size is fixed at 1024 | Decided |
| D-26 | Chat and document content stays out of Temporal history: it lives in Postgres under IDs. Workflows that were running at the deploy keep their earlier shape until they end | Decided |
| D-27 | Payload keys are local versioned files per tenant (`<tenant>/vN`), bound to the workflow ID, rotated by a command. Amends D-12: no KMS yet. **Amended by D-32:** keys may also come from host variables; the Notion webhook token is stored encrypted per tenant | Decided |
| D-28 | MCP, OpenAPI adapters and the certified/custom connector tiers are **deferred, not decided against**. Notion is called directly behind a manifest. Amends D-08 and D-15 | Decided |
| D-29 | A publish approves a content: the version carries the hash of the definition it was submitted with, and publishing a different hash is refused | Decided |
| D-30 | A creator lists and reads only the agents they created; admins read every agent | Decided |
| D-31 | Bounds on how long a task can live: an approval window of at most 168 hours, a follow-up watch of at most 60 days, and a retention floor of 68 days for the data a live task still needs | Decided |
| D-32 | Payload keys may come from host variables (`AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>`) instead of local files, chosen once per process; the Notion webhook verification token is stored encrypted per tenant in the database (pending → confirmed by a host command). Amends D-27. **Amended by D-33:** keys may also live in the database, wrapped by a master key | Decided |
| D-33 | Payload keys may live in the database (`AGENTOS_KEY_SOURCE=db`): one master key in host variables (`AGENTOS_MASTER_KEY_V<n>`), each tenant's key generated by the app and stored wrapped, bound to its tenant and version. Amends D-32 | Decided |

## Context for the main decisions

### D-03 Cells
- **Why:** one namespace and database per tenant is too much to operate with many small tenants. A single shared pool can't offer dedicated isolation to the regulated customers we want to win. Cells give both, and the same unit later becomes the customer-hosted deployment.
- **Consequence:** tenant ID goes in every workflow ID, row, index partition, and log line from day one. Routing from a tenant to its cell happens at the ingress gateway.

### D-07 Agent-scoped permissions
- **Decision:** the agent has its own access. Whether the members of a channel the agent is deployed to may see or change what the agent can reach is the creator's responsibility.
- **Why:** this matches how teams think about a shared bot ("the IT helpdesk can see the IT space"), behaves the same for everyone who asks, and makes retrieval simpler and faster.
- **Alternatives considered:** acting on behalf of each user (agents behave differently for each person, and every user must connect every account); "channel-safe" knowledge only (too restrictive for team agents).
- **Risks and the guardrails that come with this decision:**
  - *A creator over-shares:* the admin approves publishing and any wider grants, and sees each grant's live title and parent, a warning when the creator's label and the live title don't contain one another as whole words once normalised, and a diff against the published version; the creator confirms an explicit audience summary when deploying. Where the Notion picker is configured, a new grant must also be picked from the creator's own Notion account, so it cannot exceed what they can see. Without the picker the admin's approval is the only check.
  - *The audience changes after deploy:* a Slack Connect share of the channel is watched and pauses the agent in that channel until the creator confirms again. Who is added to a channel is the admins' responsibility and pauses nothing.
  - *The creator leaves:* target, not built: grants re-checked every night against the creator's access and suspended if it no longer holds, with the owner and the admin notified.
  - *Questions about one person's own records:* see D-17.

### D-13 Notion as the first write action
- **Why Notion works:** many teams already run request trackers as Notion databases. Notion's integration access model (only pages explicitly shared with the integration) maps directly onto agent grants, and Notion is also a knowledge source, so one connector covers both.
- **What it costs us:** Notion has no native idempotency key, so we add a hidden request-ID property that makes a write findable, and Postgres decides which attempt writes (a lease per idempotency key); the lookup before creating is a check, not the guard. "Done" depends on each database's own status property, so creators map it when granting a database. Notion's low API rate limits mean we need per-tenant queuing. Details in [architecture.md §7](architecture.md).
- **Positioning effect:** we no longer start head to head with IT-ticketing products (Moveworks, ServiceNow). The first demo becomes "requests to any team tracked in Notion", which is broader but less of a ready-made helpdesk story. Jira and ServiceNow follow in v2.

### D-17 Requester-scoped data
- **Why:** an agent-level credential can read everyone's records, and the LLM can't be trusted to filter by person. The runtime pins the person field instead and removes it from what the LLM sees.
- **Alternative kept as an exception:** the requester's own account (OAuth), for systems whose API can't filter by person. That account can reach beyond the agent's grants, so those answers go only to a DM.
- **Where answers go:** in the conversation where the question was asked. Everyone the agent serves may already see what it can reach (D-07), so a private reply would add nothing. Rejected: always answering requester-scoped questions privately.

### D-10 Separate task workflows
- **Why:** work outlives the conversation. Keeping tasks separate gives each one its own lifecycle, its own audit trail, and an ID that approvals and webhooks can target. It also keeps conversation workflows short.

### D-11 Approval by default
- **Why:** it's the main defense against prompt injection and model mistakes. It doesn't depend on the model behaving correctly.

### D-15 Connector manifests
- **Why:** with MCP and OpenAPI, *calling* a tool needs no connector-specific code. What needs per-system work is what makes a write safe and trackable: how to avoid duplicates, who it's attributed to, how to know when it's done. Putting that in a manifest makes most new connectors a configuration and testing job instead of an integration build.
- **Consequence:** certified write actions are still added one at a time, because each needs testing against a real instance. Custom connectors never retry writes automatically unless they declare how to avoid duplicates.

### D-16 Connector self-repair
- **Why:** custom connectors are untested when they're added. Catching and fixing their failures quickly is what makes "bring any MCP server" credible.
- **Why Temporal:** the fix process includes waits that can last days (human approval, watching a canary). Using the failure signature as the workflow ID groups many identical failures into one fix.
- **Limits:** only code we own gets fixed; most failures (logins, permissions, outages) are routed elsewhere, not to the coding agent. The agent can't change safety fields, never deploys, and runs in a separate cell with no production credentials. Details in [design/self-repair.md](design/self-repair.md).
- **Rollout:** detection and the circuit breaker first (v2), human-approved fixes next (v3), automatic deploy only for low-risk read-tool fixes later.

### D-20 Tenants in Postgres
- **Decision:** after a one-time import of the YAML file, the `tenants` table is the source of truth for the Slack workspace, the enabled flag, the default agent, channel routing, admins, the Notion workspace and the names of the secret variables. A secret value is never stored. The gateway and the console re-read the table every 15 seconds, the worker every 5, so adding, changing or disabling a tenant needs no restart. An empty table stops a process with a message naming the import.
- **Why:** several tenants share one deployment, and a settings change should not need a deploy. Row-level security hides other tenants' rows; the two reads that cross tenants go through narrow `SECURITY DEFINER` functions.
- **Consequence:** editing the YAML afterwards changes nothing. There is no console page for tenant settings yet.

### D-21 Budgets and their stop
- **Decision:** an admin sets a monthly dollar budget for the tenant and for each agent; at or over it the agent answers with a fixed text and calls no model. The tenant budget also stops indexing's embedding calls. Spend that cannot be measured is not zero: a budgeted scope with an unpriced row this month, or a call whose usage row could not be saved, is stopped until the rows are priced or the cache window passes.
- **Why:** a limit that lets unmetered spend through is not a limit. Overspend is bounded, not zero: the other processes' spend in one cache window, plus the calls in flight, plus one call's cost.
- **Consequence:** the worker refuses to start with no price for its chat or embedding model, and prices are matched by name, alias or dated prefix. Deviation: a dropped usage row marks the scope unmetered only in that worker process for one window.

### D-22 Kill switches, and what "agent off" means
- **Decision:** an admin turns an agent, every agent, a connector or all writes off, and on again; each applies on the next step, with no cache. Turning an agent off stops everything it does: new model calls, read tools already dispatched, follow-up polling (the watch ends), its offer to a front door as a specialist, and **a write the requester already approved that has not run**. The card says the agent was turned off by an admin and that nothing was changed, and the task ends without a follow-up watch. A connector that is off makes polling fail retryably, so the task keeps watching.
- **Why:** an admin who turns an agent off during an incident expects nothing more to happen.
- **Consequence:** one gate in front of every activity that acts for an agent applies the agent, writes and connector switches by effect (model, read, write, poll). Only an admin changes a switch. The host operator can use the same switches from the command line, audited as `operator:<OS login>`.

### D-23 A pause or a lost audience stops pending approval cards
- **Decision:** a channel pause, or the requester leaving the agent's audience, stops cards that are already pending. The task re-checks both immediately before the write, refuses with a fixed line on the card ("<agent name> can no longer act for you here; nothing was changed.") and ends; the requester can ask again once the agent is available to them. A check that cannot be completed stops the write too.
- **Why:** a card shown before a pause or a membership change should not outlive it.
- **Consequence:** the task workflow has a pre-write check, behind a workflow version marker so histories recorded before it replay unchanged.

### D-24 Audience watch is limited to shared channels
- **Decision:** an agent is paused in a channel when the channel is shared with another organisation (Slack Connect). A person joining a channel, guest or not, pauses nothing: who is added to a channel is the admins' responsibility, and a guest or outside person who asks the agent is still refused by the audience check.
- **Why:** a share is a fact the event itself carries, so it needs no Slack lookup and has no "could not check" case. Joins are frequent and ambiguous.
- **Status:** built and verified with fakes only; not yet run against a real Slack Connect channel.
- **Resuming:** the agent's creator or an admin confirms a fresh count of the people in the channel, as at publish.

### D-25 Qwen and local embeddings
- **Decision:** the worker can use Qwen (an OpenAI-compatible endpoint) instead of Claude, chosen once at start, and embeddings come from any OpenAI-compatible endpoint, such as a local Ollama model (`bge-m3`). The vector size is fixed by the index column at 1024, so every embedding model must produce it.
- **Why:** testing and development without paying a hosted provider; chat history is stored in one format whichever provider is used.
- **Consequence:** the default Claude model is still the production path (Q-1). Each page records its embedding model, so changing the model re-embeds everything and searches compare only chunks of the model in use.

### D-26 Content stays out of Temporal history
- **Decision:** what people write, what the model replies, each tool call's input and a task's draft live in Postgres under IDs, and Temporal carries the IDs. Deleting a thread's rows deletes what was said in it. The gateway stores a Slack event and answers at once; a relay loop signals the workflow.
- **Deviation 1:** a conversation or task that was running when this change was deployed keeps carrying content in its history until it ends, because histories recorded before the change must still replay. A message signalled by an older gateway has its text inline and is run the way it was; every message after the deploy is content-free, also in a thread that began before it. A task started before keeps its payload in its history until it finishes.
- **Deviation 2:** two small strings can still reach history: the field names a model chose in an invalid tool input, which are told back to it as the reason the call did not run, and a failure reason from an activity (fixed texts from the connector). Neither is what a person wrote or a document held.

### D-27 Payload keys are local versioned files (amends D-12)
- **Decision:** each tenant's Temporal payloads are encrypted with AES-256-GCM under a key kept as a local file (`<tenant>.key`, then `<tenant>.vN.key` after a rotation), owner-only and outside the repo. The key ID `<tenant>/vN` travels with the payload and the workflow ID is the associated data. `python -m agentos.tenant_keys rotate` adds a version; old versions stay so older histories read. A payload with no encrypted tag is refused unless `AGENTOS_ALLOW_PLAINTEXT_PAYLOADS` is set.
- **Why:** it keeps tenants' payloads apart and makes a key swap possible before a cloud key service is chosen. The key store is the one seam where a KMS replaces local files.
- **Known limit:** the `workflow.patched` marker records are written by the Temporal SDK core outside the codec and carry only patch IDs. Not built: KMS and crypto-shredding.

### D-28 MCP and the connector tiers are deferred, not decided against
- **Decision:** the runtime calls Notion through its own connector, behind a manifest that holds each tool's risk class, idempotency method, requester scoping and input schema. There is no MCP client, no OpenAPI adapter, and no certified/custom tiers. This amends D-08 and D-15: the contract stays the goal and is reopened when a second connector is built, since a connector's write safety depends on the system, not on the transport.
- **Why:** one connector does not need a protocol layer, and certified write actions are tested one at a time anyway.
- **Consequence:** architecture §7 keeps the design under a "Target" label.

### D-29 Approving is approving a content
- **Decision:** submit stores the SHA-256 of the definition the version was submitted with. A publish names the hash the approver was shown and is refused if the version carries another, which a withdraw, an edit and a resubmit under the same number would produce. The `agent_publish` event records the reviewed hash. The API refuses a publish with no hash.
- **Why:** an admin's approval is the control that compensates for what the platform cannot yet check about a grant, so it must bind to what was read.
- **Consequence:** a pending version shown before the hash existed has to be reopened before it is published.

### D-30 A creator reads only the agents they created
- **Decision:** a creator lists and reads only the agents they created: definitions, versions, usage, budgets and switches follow one rule, enforced in `ControlPlane`, which the console and the API both call. An agent that belongs to someone else answers "not found", as an agent that does not exist does. Admins read every agent.
- **Why:** a definition holds the system prompt and the access an agent was granted. Before this, usage, budgets and switches were limited to a creator's own agents but definitions were not, so the rule depended on which page asked.
- **Consequence:** the caller's role and owned agents are resolved once per request into a `Principal` that plane calls accept, so a page makes one role lookup instead of one per call.

### D-31 How long a task can live
- **Decision:** an agent definition may ask for an approval window of at most 168 hours (default 72) and a follow-up watch of at most 60 days (default 30). Retention for conversation messages, tool outputs and idempotency records is never shorter than the longest a task can live: 168 hours rounded up to 7 days, plus 60, plus 1, is 68 days. Setting a lower value is refused; the nightly job raises a lower stored one.
- **Why:** nothing a live approval or follow-up still needs may be purged under it.
- **Consequence:** audit and usage rows, external writes, task links and agent definitions are exempt from retention.

### D-32 Keys from host variables, the webhook token in the database (amends D-27)
- **Decision:** a process reads its payload keys either from the local key files (the default, `AGENTOS_KEY_SOURCE=files`) or from host variables named `AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>` (`AGENTOS_KEY_SOURCE=env`): base64 of 32 bytes, `<TENANT>` the tenant ID upper-cased with `-` turned to `_`. Every process chooses through one factory (`agentos.crypto_keys.build_key_store`), so the worker, the gateway and the Temporal client agree. `python -m agentos.tenant_keys value` prints a new key value once, to paste into the host's variables; it never reads or prints an existing key. The Notion webhook verification token is stored encrypted per tenant in the database (the codec's AES-256-GCM cipher, the tenant ID as associated data): the handshake stores it pending and cannot replace a pending or active token; a host command (`python -m agentos.notion_webhook show|confirm|clear`) prints it once, confirms it, or removes it, each audited as the operator. Only a confirmed token accepts events. The tenant's environment variable stays as a local override: while it is set, events use it and the handshake is refused.
- **Why:** a hosted deployment (Railway) has no operator laptop to hold owner-only files; service variables are the hosted equivalent of "exported in the terminal", and the database is already the tenant-scoped, backed-up store. The token no longer needs a gateway volume, nor a restart to export it.
- **Consequence:** rotation on a variable host adds `_V2`, redeploys, and keeps `_V1`; moving a tenant from a key file to a variable is the operator copying the file's content themselves. Two variables claiming one tenant's key version are refused at start-up. Decided by the maintainer 2026-11-19.

### D-33 Tenant payload keys in the database, wrapped by a master key (amends D-32)
- **Decision:** a process may read payload keys from the database (`AGENTOS_KEY_SOURCE=db`): each tenant's key is generated by the app and stored in `tenant_payload_keys`, wrapped with AES-256-GCM under a master key from `AGENTOS_MASTER_KEY_V<n>` host variables (base64 of 32 bytes), with the key ID (`<tenant>/v<n>`) as associated data. New keys use the highest master version; older masters stay while keys wrapped under them are kept. One factory (`agentos.crypto_keys.build_key_store`) still chooses for every process; the database store serves unwrapped keys from memory, loaded at start-up and every few seconds, and a key miss triggers one re-read before refusing, so a tenant installed seconds ago is served without a restart and a missing key stays a retryable workflow-task failure, never a terminal one. `tenant_keys create` and `rotate` write to the database in this mode, audited as `payload_key`; `master-value` prints one new master value once. The app role reads every row of this one table (the codec decrypts for whichever tenant a payload belongs to) and inserts only within its own tenant; stored key versions are never updated or deleted.
- **Why:** Add to Slack must create a working tenant with no manual step: with `env`, every tenant key is a variable a person adds and then redeploys. One master variable set once plus app-generated tenant keys is seamless (nothing per customer) and scalable (any number of tenants). A cloud key service may hold the master key later.
- **Consequence:** the master key never enters the database; a database dump holds only wrapped keys, and a wrapped key copied to another tenant's row does not unwrap. Losing every master version makes every tenant key unreadable, so old master versions are kept like old key versions. The cross-tenant read is confined to this one table; every other table keeps tenant isolation. Two known limits: while a tenant's key is missing, the Temporal SDK cannot encrypt the workflow-task failure either and writes its message in plaintext (it names only the tenant and key version); and each process re-reads every key row every few seconds, a cost that grows with the number of tenants and moves to a watermark or a change notification when it matters. Decided by the maintainer 2026-11-19.

### D-34 Slack bot tokens per tenant in the database, encrypted with the tenant key
- **Decision:** a tenant's Slack bot token is stored in `tenant_credentials` (one row per tenant and kind, `slack_bot` first), encrypted with the tenant's payload key — the codec's AES-256-GCM cipher, the tenant ID as associated data, the same shape as the Notion webhook token (D-32). Every process resolves the token through one resolver: the credential row first, the environment variable the tenant's settings name when no row exists, so a local single-tenant run is unchanged. A key miss reloads the keys once; a row that cannot be decrypted is a setup error naming the tenant and key version, never a value, and never a silent fallback to another token. The Slack OAuth install ("Add to Slack" on the console, OAuth v2 with the manifest's bot scopes, a single-use state cookie, a pilot allowlist `AGENTOS_INSTALL_ALLOWED_TEAMS` that refuses every install when unset) creates the tenant row, its key, its token and its first admin in one unit of work, audited as `install`; a reinstall replaces the token (audited) and creates nothing else. An installed tenant has `default_agent` NULL and no Notion: the gateway answers its messages with one fixed not-set-up reply (no model, no Notion call), and a tenant with no Notion token is served Slack-only. Processes start on an empty tenants table and serve none until the next refresh; an activity for a not-yet-loaded tenant is retryable within the shared retry policy (5 attempts, about 15 s), which outlasts the worker's 5 s refresh.
- **Why:** Add to Slack must create a working tenant with no manual step (D-33's reason): a token a person exports per tenant is a per-customer manual step and a redeploy. One click in Slack plus one allowlist entry for the pilot is seamless; the database row plus the directory refresh scales to any number of tenants. The allowlist keeps the pilot bounded without a per-tenant setup step.
- **Consequence:** the app role can SELECT, INSERT and UPDATE `tenant_credentials` (no DELETE until an uninstall flow exists); row-level security hides every tenant's row from the others. A reinstall stores the new token; processes rebuild the tenant's client from it on their next read (whether Slack retires the old token is Slack's side of the reinstall). The master key (D-33) now protects bot tokens as well as histories: losing every master version loses the stored tokens too, and a reinstall is the recovery. The fixed not-set-up reply names `AGENTOS_CONSOLE_URL`, which the gateway must therefore also be given. Decided by the maintainer 2026-11-19.

## Open questions

| # | Question | Suggested default |
|---|---|---|
| Q-1 | Default LLM provider, and do tenants need to bring their own key in v1? | Claude via the LLM gateway; bring-your-own key in v2 |
| Q-2 | Hosting cloud and region strategy (EU data residency at launch?) | Pick one cloud; plan an EU cell early |
| Q-3 | Default retention for conversation history and audit data | 90 days for history, 1 year for audit; configurable per tenant. Built so far: per-tenant limits for conversation data with a 68-day floor (D-31), defaults 365 days for messages and 90 for tool outputs; audit rows are not purged |
| Q-4 | Pricing unit (per seat, per active user, per action) | Affects metering; per active user plus action volume tiers |
| ~~Q-5~~ | Are approvals by other people (a manager approving) needed in v1, or only self-approval? | **Closed: self-approval only.** The code accepts a decision only from the requester; approval by other people is on the v2 roadmap |
| Q-6 | Does v1 need Slack Enterprise Grid (one tenant across many workspaces)? | Design the data model for it; test against a single workspace first |
