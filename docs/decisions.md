# Decision log

Status values: **Decided** (agreed by the product owner), **Proposed** (recommended, awaiting confirmation), **Open** (needs input).

Last reviewed 2026-11-14.

| ID | Decision | Status |
|---|---|---|
| D-01 | v1 is an agent builder for approved creators plus a front-door assistant. Company assistant and broader scope go on the roadmap | Decided |
| D-02 | The architecture targets agents that do work (actions, approvals, multi-day follow-up) from day one | Decided |
| D-03 | Multi-tenant SaaS with a cell architecture. A customer-hosted deployment later is a single-tenant cell | Decided |
| D-04 | Thin custom agent loop on Temporal. No LangChain, LangGraph, or CrewAI | Decided |
| D-05 | Slack is the only chat platform in v1 | Decided |
| D-06 | Only creators approved by an admin can create agents | Decided |
| D-07 | Each agent is its own principal with access grants set by its creator. The creator is accountable for whether the agent's audience may see or change what the agent can reach | Decided |
| D-08 | MCP as the connector contract, wrapped by our runtime for credentials, grants, risk class, idempotency, and audit | Decided |
| D-09 | Retrieval is filtered by the agent's granted scopes inside the vector search, then re-checked against the source | Decided |
| D-10 | Conversations and tasks are separate workflows. Tasks are child workflows with `ABANDON` as parent-close policy | Decided |
| D-11 | All write actions need approval by an authorized person by default. Only a tenant admin can relax this, per action | Decided |
| D-12 | Chat history and tool outputs live outside Temporal. Temporal payloads are encrypted with a per-tenant KMS key | Decided |
| D-13 | v1 write action: add items to a Notion database (such as a request tracker) and comment on them, with durable follow-up until the item's status reaches done. Jira moves to v2 | Decided |
| D-14 | Python on the Temporal Python SDK; Postgres with row-level security; pgvector for v1 | Decided |
| D-15 | Connectors are defined by a manifest (risk class, idempotency, attribution, watch, indexing) on top of MCP or OpenAPI. Two tiers: certified (by us, exactly-once guaranteed) and custom (by tenant admins, conservative defaults) | Proposed |
| D-16 | Connector self-repair (roadmap): connector failures go into a Temporal remediation workflow (one per failure signature) with a circuit breaker; a sandboxed coding agent proposes fixes; humans approve deploys; failed writes are never replayed automatically | Proposed |
| D-17 | Requester-scoped tools: the runtime fills in the requester's identity (the LLM can't set it) and answers where the question was asked, since the audience rule (D-07) covers visibility. The requester's own account is used only for systems whose API can't filter by person, and those answers go only to a DM | Decided |
| D-18 | Agents, versions and members live in Postgres behind one service layer that holds every role check and audits every change. A tenant's first admins come from its config file; an admin may approve their own submission, and the audit records it | Decided |
| D-19 | The control plane is a separate process from the Slack gateway. It listens on loopback with a fixed operator until Sign in with Slack is configured; then it may be public, with server-side sessions, CSRF tokens and a browser guard that lets only page navigations in from other sites | Decided |

## Context for the main decisions

### D-03 Cells
- **Why:** one namespace and database per tenant is too much to operate with many small tenants. A single shared pool can't offer dedicated isolation to the regulated customers we want to win. Cells give both, and the same unit later becomes the customer-hosted deployment.
- **Consequence:** tenant ID goes in every workflow ID, row, index partition, and log line from day one. Routing from a tenant to its cell happens at the ingress gateway.

### D-07 Agent-scoped permissions
- **Decision:** the agent has its own access. Whether the members of a channel the agent is deployed to may see or change what the agent can reach is the creator's responsibility.
- **Why:** this matches how teams think about a shared bot ("the IT helpdesk can see the IT space"), behaves the same for everyone who asks, and makes retrieval simpler and faster.
- **Alternatives considered:** acting on behalf of each user (agents behave differently for each person, and every user must connect every account); "channel-safe" knowledge only (too restrictive for team agents).
- **Risks and the guardrails that come with this decision:**
  - *A creator over-shares:* grants can't exceed the creator's own access; the admin approves publishing and any wider grants; the creator confirms an explicit audience summary when deploying.
  - *The audience changes after deploy:* membership changes are watched; a new external guest or Slack Connect share pauses the agent in that channel until the creator confirms again.
  - *The creator leaves:* grants are re-checked every night against the creator's access and suspended if it no longer holds; the agent's owner and the admin are notified.
  - *Questions about one person's own records:* see D-17.

### D-13 Notion as the first write action
- **Why Notion works:** many teams already run request trackers as Notion databases. Notion's integration access model (only pages explicitly shared with the integration) maps directly onto agent grants, and Notion is also a knowledge source, so one connector covers both.
- **What it costs us:** Notion has no native idempotency key, so we add a hidden request-ID property and query before creating. "Done" depends on each database's own status property, so creators map it when granting a database. Notion's low API rate limits mean we need per-tenant queuing. Details in [architecture.md §7](architecture.md).
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
- **Limits:** only code we own gets fixed; most failures (logins, permissions, outages) are routed elsewhere, not to the coding agent. The agent can't change safety fields, never deploys, and runs in a separate cell with no production credentials. Details in [architecture.md §12](architecture.md).
- **Rollout:** detection and the circuit breaker first (v2), human-approved fixes next (v3), automatic deploy only for low-risk read-tool fixes later.

## Open questions

| # | Question | Suggested default |
|---|---|---|
| Q-1 | Default LLM provider, and do tenants need to bring their own key in v1? | Claude via the LLM gateway; bring-your-own key in v2 |
| Q-2 | Hosting cloud and region strategy (EU data residency at launch?) | Pick one cloud; plan an EU cell early |
| Q-3 | Default retention for conversation history and audit data | 90 days for history, 1 year for audit; configurable per tenant |
| Q-4 | Pricing unit (per seat, per active user, per action) | Affects metering; per active user plus action volume tiers |
| Q-5 | Are approvals by other people (a manager approving) needed in v1, or only self-approval? | Self-approval only in v1 |
| Q-6 | Does v1 need Slack Enterprise Grid (one tenant across many workspaces)? | Design the data model for it; test against a single workspace first |
