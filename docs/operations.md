# Operations

How to set up, configure and run the system. What the system does is in [architecture.md](architecture.md); the first-run summary is in the [README](../README.md); what is built is in [status.md](status.md).

Contents: [Setup](#setup) · [Notion webhook](#notion-webhook-optional) · [Front door and channels](#front-door-and-channels) · [Several tenants](#several-tenants) · [Tenants in the database](#tenants-in-the-database) · [Encrypted workflow data](#encrypted-workflow-data) · [Payload keys on a host without files](#payload-keys-on-a-host-without-files) · [Model provider](#model-provider) · [Usage and cost](#usage-and-cost) · [Monthly budgets](#monthly-budgets) · [Kill switches](#kill-switches) · [Database and migrations](#database-and-migrations) · [Connections and concurrency](#connections-and-concurrency) · [Retention](#retention) · [Run](#run) · [Tests](#tests) · [Knowledge](#knowledge-indexing-and-search) · [Agents in the database](#agents-in-the-database) · [Members and roles](#members-and-roles) · [Audit log](#audit-log) · [Control-plane API](#control-plane-api) · [Agent audiences](#agent-audiences) · [Web console](#web-console) · [Slack tunnel](#slack-tunnel-and-live-checks)

Prerequisites: [uv](https://docs.astral.sh/uv/) (it installs Python 3.12 for you), Docker with Docker Compose, a tunnel tool (`cloudflared` or `ngrok`), a Slack workspace where you can create apps, a Notion workspace where you can create integrations, and an Anthropic API key (or Qwen, see Model provider).

## Setup

```bash
uv sync
scripts/setup-wizard.sh
```

The wizard starts Postgres and Temporal, walks you through the tunnel, the
Slack app (from `slack/app-manifest.yaml`) and the Notion integration and
database, and writes the non-secret values (Slack team ID, Notion database ID)
into `config/agents.yaml`. It then imports the tenant into the database (see
below).

It never asks for, stores or prints a secret. At the end it shows the
`export` lines you fill in and run yourself:

```bash
export SLACK_BOT_TOKEN='<paste here>'
export SLACK_SIGNING_SECRET='<paste here>'
export NOTION_TOKEN='<paste here>'
export ANTHROPIC_API_KEY='<paste here>'
```

Secrets are read only from the environment. Don't put them in a file.

In the Notion integration's settings, enable the **Read comments** and
**Insert comments** capabilities (both are off by default). The agent needs
them to comment on items.

After the wizard and the import, with `NOTION_TOKEN` exported, check the Notion grant
of the agent's draft and get the stable property IDs (paste the `property_ids` block into
the definition before publishing; the worker will not start on a published agent without it):

```bash
uv run python -m agentos.check_grants --tenant <tenant> --agent <agent>   # newest draft
uv run python -m agentos.check_grants                                     # every published agent
```

The property names in `config/agents.yaml` are labels; the worker addresses
Notion properties only by ID, so a rename in Notion doesn't break it. The
command reads the database's schema (it never changes it), reports any
misnamed, mistyped or missing property, and prints a `property_ids` block. Paste
that block into the grant in `config/agents.yaml`. The worker checks the same
things at startup and refuses to start if anything is wrong or an ID is
missing. The command exits 1 on any problem.

Import the tenant and its agents from the YAML file into the database, once.
It needs the `006` and `015` migrations (a fresh `docker compose up` has both):

```bash
uv run python -m agentos.tenant_store import
uv run python -m agentos.agent_store import
```

The worker, the gateway and the console refuse to start while the `tenants`
table is empty, or while a tenant's `default_agent` or a channel's agent has no
published version, so run these before them. See
[Tenants in the database](#tenants-in-the-database) and
[Agents in the database](#agents-in-the-database).

Optional settings: `AGENTOS_MODEL` (default depends on the provider; see
below),
`AGENTOS_TEMPORAL_ADDRESS` (default `localhost:7233`) and `AGENTOS_CONFIG`
(default `config/agents.yaml`; a file for one tenant, or a directory with one
`*.yaml` per tenant). Only the two import commands read it.
`AGENTOS_DATABASE_URL` is required and has no default; see
[Database and migrations](#database-and-migrations) (Compose publishes Postgres on host
port 55432, bound to 127.0.0.1 along with Temporal's 7233 and 8233; Temporal keeps
its data in the `temporal-data` volume).

If you exported `DATABASE_URL` or `TEMPORAL_ADDRESS` for this project before,
export the `AGENTOS_` names instead: the old ones are now ignored.

Knowledge settings, used only when an agent has a Notion page grant:

| Variable | Default | Meaning |
|---|---|---|
| `AGENTOS_EMBED_MODEL` | `text-embedding-v4` | Embedding model served by whichever embedding endpoint is configured. Output is 1024 dimensions, fixed to match the database column |
| `AGENTOS_EMBED_BASE_URL` | none | An OpenAI-compatible embeddings endpoint of its own, such as a local Ollama. When unset, the Qwen settings are used |
| `AGENTOS_EMBED_MAX_DISTANCE` | per model | The farthest a chunk may be from the question (cosine distance, 0 to 2) and still reach the model. Defaults: `bge-m3` 0.6 (a starting point from published scores, not measured on your pages), none for other models. Set it to tune the cut-off; an off-topic question gets "No documents this agent can read match that." |
| `EMBED_API_KEY` | none | Key for that endpoint (secret; export it, never save it). A local Ollama needs none |
| `QWEN_API_KEY`, `QWEN_BASE_URL` | none | Needed for embeddings, when any agent has a page grant, only if `AGENTOS_EMBED_BASE_URL` is unset |

## Notion webhook (optional)

The webhook makes a task notice a finished Notion item sooner than the 15-minute
poll. Notion signs every event, and the gateway refuses any event whose
signature it cannot check, so set it up in this order:

1. In the Notion integration, add a webhook subscription that points at
   `POST /notion/webhook/<tenant_id>` on the gateway. (`POST /notion/webhook`
   without a tenant works only while exactly one tenant is configured, so a
   subscription made before there were several tenants keeps working.)
2. Notion sends a one-time handshake. The gateway encrypts the verification
   token with the tenant's payload key and stores it in the database as
   **pending**, and logs that a token is pending (never the token). While a
   token is pending or active a second handshake gets 409 and changes nothing.
3. Print the pending token once, paste it into the Notion integration to finish
   subscribing, then confirm it:

   ```bash
   uv run python -m agentos.notion_webhook show --tenant <tenant_id>
   uv run python -m agentos.notion_webhook confirm --tenant <tenant_id>
   ```

   Only a **confirmed** (active) token accepts events; a pending one never does.
   Each command is audited as the operator (`operator:<OS login>`), and the
   audit row never holds the token. `show` refuses once the token is confirmed.
   The command reads the tenant's payload key from the same key source as the
   gateway (`AGENTOS_KEY_SOURCE`), so run it where those keys are available.

To re-subscribe (a new subscription sends a new token), clear the stored token
first, then let the new handshake arrive:

```bash
uv run python -m agentos.notion_webhook clear --tenant <tenant_id>
```

The handshake is also refused while the tenant's webhook token variable is set
(409): the variable is a local override, and while it is set every event is
verified against it instead of the database. Unset it and restart the gateway
to use the database flow. A gateway built without a database keeps the older
file flow: the handshake writes an owner-only token file
(`notion-webhook-verification-token-<tenant_id>` in `~/.agentos`, unless
`AGENTOS_NOTION_TOKEN_DIR` is set), the operator pastes it into Notion and
exports it, and deleting the file reopens the handshake.

Until a token is active (or exported), events get a 503. Events with a missing
or wrong signature (a signature made with another tenant's token is wrong) get
a 401. A path that names no tenant gets a 404. None of these reaches a
workflow. If the tenant's config sets `notion_workspace_id`, a correctly signed
event from any other Notion workspace is ignored (200, no action).

| Variable | Default | Meaning |
|---|---|---|
| `NOTION_WEBHOOK_VERIFICATION_TOKEN` | none | Local override: Notion's verification token for a tenant that has no `secrets` block (secret; export it, never save it in a dotenv file). While it is set, events are verified against it and the handshake is refused. Other tenants name their own variable |
| `AGENTOS_NOTION_TOKEN_DIR` | `~/.agentos` | Directory where a gateway without a database writes each tenant's token file, `notion-webhook-verification-token-<tenant_id>`, at subscription time (mode `0600`, directory `0700`) |

## Front door and channels

An agent whose config lists `specialists` is a **front door**. It answers what
it can and hands a question that belongs to one of those agents over with the
`hand_off` tool, which names only its own specialists. Each specialist needs a
`description`, which the front door reads to choose.

```yaml
default_agent: concierge          # DMs and mentions in unmapped channels land here
channels:                         # optional: Slack channel ID -> agent
  C0123456789: it-helper
agents:
  concierge:
    version: 1
    name: Concierge
    description: General questions about the company.
    system_prompt: |
      You answer general questions. Hand specialist questions to the right agent.
    max_steps: 4
    specialists: [it-helper]      # makes this agent a front door
  it-helper:
    version: 1
    name: IT helper
    description: Laptops, accounts, VPN, access and other IT requests.
    # ... prompt, grants and the rest as before
```

How the hand-off behaves is in [architecture.md §3](architecture.md#3-workflow-model); where a message lands is in §5.

- Config errors fail at load: an undefined specialist or channel agent, an
  agent listing itself, a specialist listed twice, a specialist without a `description`, or a specialist
  that is itself a front door.

## Several tenants

`AGENTOS_CONFIG` may name a **directory**. Every `*.yaml` directly inside it is
one tenant, in the same format as `config/agents.yaml`. Import them once
(`uv run python -m agentos.tenant_store import`); the gateway, the worker and
the console then serve all of them side by side from the database:

```
config/tenants/
  acme.yaml
  globex.yaml
```

```yaml
# config/tenants/globex.yaml (the agents block is as in agents.yaml)
tenant_id: globex
slack_team_id: T0GLOBEX
notion_workspace_id: "<globex's Notion workspace ID>"   # optional: webhook cross-check
secrets:                                                # names of variables, not values
  slack_bot_token_env: SLACK_BOT_TOKEN_GLOBEX
  notion_token_env: NOTION_TOKEN_GLOBEX
  notion_webhook_token_env: NOTION_WEBHOOK_VERIFICATION_TOKEN_GLOBEX
default_agent: it-helper
agents: { ... }
```

```bash
export SLACK_BOT_TOKEN_GLOBEX='<paste here>'
export NOTION_TOKEN_GLOBEX='<paste here>'
export AGENTOS_CONFIG=config/tenants
```

- Each tenant's Slack workspace and Notion webhook path (`/notion/webhook/<tenant_id>`)
  reach only that tenant, and each tenant's tokens are used only for its own
  work. The gateway drops a Slack event from a workspace it has no tenant for.
- Secrets still come only from the environment. The `secrets` block names the
  variables; a tenant without one uses `SLACK_BOT_TOKEN`, `NOTION_TOKEN` and
  `NOTION_WEBHOOK_VERIFICATION_TOKEN`, and only one tenant may. The worker
  refuses to start, naming the tenant and the variable, if one is unset.
- A `tenant_id` is 1 to 63 lowercase letters, digits and hyphens, starting with a letter or digit; anything else fails at load.
- Two tenants may not share a `tenant_id`, a `slack_team_id`, a
  `notion_workspace_id` or a secret variable; loading fails naming both.
- `SLACK_SIGNING_SECRET` stays one value, because there is one Slack app. The
  model and the embedder are the platform's own and are shared.
- `check_grants` checks every tenant. `index_knowledge` indexes every tenant,
  or one with `--tenant <tenant_id>`; schedules are kept per tenant.
- A single file keeps working exactly as before.

## Tenants in the database

After the one-time import, the `tenants` table is the source of truth for
tenant settings: the Slack workspace, whether the tenant is enabled, the
default agent, the channel routing, the admins, the Notion workspace and the
**names** of the secret variables. A secret value is never stored; secrets
still come only from the environment. Agents are in their own table.

```bash
uv run python -m agentos.tenant_store import
```

- The import adds each YAML tenant that is not in the table yet and **skips**
  one that is, so running it again never overwrites a setting changed since.
  A tenant that would share a Slack workspace, a Notion workspace or a secret
  variable with a stored one is refused, with both tenants named.
- Editing the YAML file after the import changes nothing. Change a tenant in
  the database. There is no console page for it yet, so for now that is SQL
  run as the owner role, for example `UPDATE tenants SET enabled = false WHERE tenant_id = 'acme'`. A trigger moves
  `updated_at` on every update, which is how the processes notice the change.
- Every process starts only if the table has an enabled tenant. An empty table
  stops it with a message that names the import; there is no silent fallback to
  the YAML file.
- **No restart.** The gateway and the console re-read the table every 15
  seconds, the worker every 5. The gateway serves a new tenant at once, because
  an event from a Slack workspace it does not know is looked up in the
  database. The worker builds the new tenant's Slack and Notion clients from its
  variables (a tenant whose variable is unset or whose payload key is unusable is
  skipped and logged once, and tried again on the next pass). A **disabled**
  tenant is dropped within that time: the gateway answers nothing for it, the
  worker no longer runs its activities, and the console refuses its sign-ins
  and sessions. Re-enabling it brings it back the same way.
- A tenant's index schedules are still created by `index_knowledge --schedule`,
  which you run after adding a tenant. The worker decides at start-up whether
  it runs the indexing workflows (some published agent has a page grant), so
  restart it once when a deployment's first page grant is published.
- Row-level security hides every tenant's row from the others. The two reads
  that cross tenants (a Slack workspace to its tenant, and the list of tenants)
  go through two `SECURITY DEFINER` functions that return the tenant ID, the
  enabled flag and a change time only. The app role cannot delete a tenant.

## Encrypted workflow data

Every payload the platform writes to Temporal (workflow inputs and results,
signals, queries, activity inputs and results) is encrypted with the key of the
tenant it belongs to (AES-256-GCM). The tenant comes from the workflow ID, which
always starts with the tenant ID; a payload with no tenant is never sent. Each
payload also carries its key ID (`<tenant>/v<N>`) and is bound to its workflow ID,
so a payload from another tenant, or moved to another workflow, is refused.

**Create a key for each tenant before starting the worker or the gateway:**

```bash
uv run python -m agentos.tenant_keys create <tenant_id>
```

This writes `<tenant_id>.key` (256 random bits, base64) with mode `0600` in a
`0700` directory and prints the path. It never overwrites an existing key. The
directory is `AGENTOS_KEY_DIR` (default `~/.agentos/keys`). A key file that is
missing, or readable by anyone but its owner, is refused. Keep the keys outside
the repo and back them up: **if a key is lost, that tenant's in-flight workflow
data cannot be read again.**

The worker and the gateway check every configured tenant's key before they
connect, and refuse to start with a one-line summary naming each missing or
unsafe key file or key variable and the command that creates it. Errors and failure messages in
the history are encrypted too.

**Rotate a tenant's key** with:

```bash
uv run python -m agentos.tenant_keys rotate <tenant_id>
```

This adds `<tenant_id>.v2.key` (then `.v3.key`, and so on) beside the first
one. New payloads use the newest version from the next one written; every older
version stays in the directory so histories written under it still read. Copy
the new file to every host that runs the worker, the gateway or
`index_knowledge` before rotating on any of them, and never delete an old
version while a history written under it is kept.

Payloads written before keys were versioned (key ID without `/v<N>`) still read,
as version 1 of their own tenant. A payload with no encrypted tag is refused. To
read a history written before encryption existed, start that process with
`AGENTOS_ALLOW_PLAINTEXT_PAYLOADS=1`, then unset it.

The Temporal web UI shows these payloads as encrypted until a codec server is
added (a later change). Every process that connects to Temporal (worker,
gateway, `index_knowledge`) goes through `agentos.temporal_client.build_temporal_client`,
so each needs the keys it will use. On a host without a key directory, the keys
come from variables instead: see
[Payload keys on a host without files](#payload-keys-on-a-host-without-files).

| Variable | Default | Meaning |
|---|---|---|
| `AGENTOS_KEY_DIR` | `~/.agentos/keys` | Directory of per-tenant key files, `<tenant_id>.key` and `<tenant_id>.v<N>.key` |
| `AGENTOS_KEY_SOURCE` | `files` | Where payload keys come from: `files` (`AGENTOS_KEY_DIR`) or `env` (`AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>` variables) |
| `AGENTOS_ALLOW_PLAINTEXT_PAYLOADS` | off | Accept payloads that were never encrypted (old histories only) |

## Payload keys on a host without files

A hosted deployment (for example Railway) keeps payload keys in the service's
variables instead of a key directory. Set `AGENTOS_KEY_SOURCE=env` on every
service that reads or writes Temporal payloads (worker, gateway, and any
`index_knowledge` or `retention` run); the default `files` keeps local runs unchanged. Each
key version is one variable:

```
AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>=<base64 of 32 bytes>
```

`<TENANT>` is the tenant ID upper-cased with `-` turned to `_` (`acme` →
`ACME`, `my-tenant` → `MY_TENANT`). A malformed or wrong-length value is
refused at start-up with an error naming the variable and the tenant, never
the value; two variables claiming one tenant's key version are refused too.
Every variable starting `AGENTOS_PAYLOAD_KEY_` must follow that name: one that
does not (for example an old key renamed to `..._V1_OLD` to "disable" it) stops
every service from starting, for all tenants.

**Create a key** for a new tenant:

```bash
uv run python -m agentos.tenant_keys value <tenant_id>
```

It prints one new value as `AGENTOS_PAYLOAD_KEY_<TENANT>_V1=<base64>` and a
warning. Paste it into the host's variables (on Railway: the service's
Variables tab) and redeploy. The value is shown once and the command stores it
nowhere; if you lose it, that tenant's in-flight workflow data cannot be read
again. The command never reads or prints an existing key, and writes no file.

**Rotate a tenant's key**:

```bash
uv run python -m agentos.tenant_keys value <tenant_id> --next
```

It names the version after the tenant's newest, read from the key source the
command's own environment selects. Add the printed `_V2` variable beside
`_V1`, redeploy every service, and **keep `_V1`**: new payloads use the newest
version, and every older version must stay while a history written under it is
kept. Never delete an old version.

**Move an existing tenant from a key file to a variable**: copy the key file's
content yourself into `AGENTOS_PAYLOAD_KEY_<TENANT>_V1` (the command never
reads a key file), set `AGENTOS_KEY_SOURCE=env`, and redeploy. The file already
holds the base64 of the 32 bytes; paste it as-is. Keep the file as a backup
until the hosted deployment has decoded existing histories.

## Model provider

The worker talks to Claude by default. For testing, it can use Qwen instead,
through Qwen's OpenAI-compatible Chat Completions endpoint. The provider is
chosen once, when the worker starts. Chat history is stored in the same format
whichever provider is in use.

| Variable | Default | Meaning |
|---|---|---|
| `AGENTOS_LLM_PROVIDER` | `anthropic` | `anthropic` or `qwen` |
| `QWEN_API_KEY` | none | Qwen API key (secret; export it, never save it). Needed only for `qwen` |
| `QWEN_BASE_URL` | none | Qwen's OpenAI-compatible base URL. Needed only for `qwen` |
| `AGENTOS_QWEN_THINKING` | `false` | `true`, `1`, `yes` or `on` turns Qwen thinking on |

`AGENTOS_MODEL` defaults to `claude-sonnet-5-5` for `anthropic` and
`qwen3.7-plus` for `qwen`. With `qwen`, `ANTHROPIC_API_KEY` is not needed.

Qwen thinking is off unless `AGENTOS_QWEN_THINKING=true`: thinking output
counts toward the per-reply token limit (3072 tokens for both providers) and
makes replies slower.

```bash
export AGENTOS_LLM_PROVIDER=qwen
export QWEN_BASE_URL='<your Qwen OpenAI-compatible base URL>'
export QWEN_API_KEY='<paste here>'
```

## Usage and cost

Every model call and every embedded page is recorded as a usage row (what a row holds, and how it is priced, is in [architecture.md §10](architecture.md#10-data-and-storage)).

Prices live in `config/prices.yaml` (override the path with `AGENTOS_PRICES`):
US dollars per million input, output, cache-read and cache-write tokens for a
model, with an `effective_from` date; a model can have several entries. The file
ships with entries for `qwen3.7-plus` and `bge-m3` and a commented example; prices
come from your provider's pricing page, so add one entry per model you use and per price change. A model with no entry is recorded with its tokens and no cost, and
the page shows "no price" (a budgeted scope is then stopped, see [Monthly budgets](#monthly-budgets)). The worker reads the file when it starts, so restart it
after an edit, and refuses to start if the file is malformed or has no price for the
configured chat model (or the embedding model, when one is in use). An entry may list
`aliases:` (other exact names) and a dated name such as `qwen3.7-plus-2026-11-01` matches
the entry `qwen3.7-plus`.

To price a month whose rows were recorded without a price (a model added late, a dated
name, a price file fixed after the fact), fix the file, then run as the database owner:

    export AGENTOS_OWNER_DATABASE_URL=postgresql://<owner>:<password>@localhost:55432/agentos
    uv run python -m agentos.usage reprice --tenant <id> --month 2026-11        # dry run
    uv run python -m agentos.usage reprice --tenant <id> --month 2026-11 --yes  # write

It appends one `usage_adjustments` row per row it can now price (the stored rows never
change), audits the run as `usage_repriced`, and needs migration `019`. `--url-var NAME`
names another variable for the URL. A row whose provider never reported its token counts
can only be priced when the price of that kind is 0.

An agent may carry an optional `team: IT` label in its definition (up to 64
characters). The usage page sums cost per team across its agents; agents without
one show as "No team". The team of a call is the team of the agent version that
made it.

The usage rows need migration `011` (a new install gets the table from
`scripts/init_db.sql`):

```bash
uv run python scripts/migrate.py
```

The [web console](#web-console) has a **Usage** page: pick a UTC month and see the
total, then tables by agent, by team and by model, and embedding usage as its own
line (admins only). An admin sees every agent; a creator sees the agents they
created. **Download CSV** exports the same aggregates. The JSON API serves them as
`GET /tenants/<t>/usage?month=YYYY-MM`. There is no per-person view: nothing is
grouped, filtered or exported by who asked.

### Monthly budgets

An admin sets a monthly budget in US dollars for the whole organisation and for each agent,
for the UTC calendar month, on the console's **Usage** page or through the API
(`PUT`, `DELETE /tenants/{t}/budget` and `.../agents/{a}/budget`, see the API table). No
budget means no limit. What a budget stops, the warning levels, the overspend bound and the
stop for spend that cannot be measured are in [architecture.md §8](architecture.md#8-safety).

- Each change is an audit event (`budget_set`). Creators see the budgets of the agents they
  created, read-only.
- A stopped scope with unpriced rows needs the price added to `config/prices.yaml`, the worker
  restarted, and the month repriced (see Usage and cost above). Raising the budget does not
  lift it.
- Indexing resumes by itself within a minute of an admin raising or removing the tenant
  budget, or when the UTC month changes; the next nightly run, or a page event, catches up.

Budgets need migration `012` (a new install gets the table from `scripts/init_db.sql`).
Run it before the worker or the console runs the new code, or the budget reads fail and the
audit insert for `budget_set` is refused:

```bash
uv run python scripts/migrate.py
```

Budget checks read a monthly rollup, `usage_month_totals` (migration `026`), which each
recorded call and each reprice updates in the same transaction. The migration fills it from
the usage rows already stored, so run it before the worker starts on the new code.

## Kill switches

An admin can turn things off at once, from the console's **Switches** page, through the API,
or from the host with a command. Each takes effect on the next step, with no restart: every
check reads the `kill_switches` table again, with no cache, in an activity or in the
gateway. Nothing turned off is held for later, and work already finished is not undone.

What each switch stops is in [architecture.md §8](architecture.md#8-safety).

Only admins turn switches off and on, never a creator, not even on their own agent. Turning
off every agent or all writes asks for a tick in the console form. Each change is an audit
event, `switch_off` or `switch_on`.

| Request | Does |
|---|---|
| `GET /tenants/{t}/switches` | every switch and its state (a creator sees their own agents) |
| `PUT`, `DELETE /tenants/{t}/switches/agents/{a}` | turn an agent off, on |
| `PUT`, `DELETE /tenants/{t}/switches/all-agents` | turn every agent off, on |
| `PUT`, `DELETE /tenants/{t}/switches/connectors/notion` | turn the Notion connector off, on |
| `PUT`, `DELETE /tenants/{t}/switches/writes` | turn all writes off, on |

The same switches work with the console down:

```bash
uv run python -m agentos.switches --tenant acme list
uv run python -m agentos.switches --tenant acme off --agent it-helper
uv run python -m agentos.switches --tenant acme off --everything   # all agents, writes, Notion
uv run python -m agentos.switches --tenant acme on --writes
```

The command reads no Slack identity. It is audited as `operator:<login of the OS account
running it>`, which the runtime reads, so it cannot be typed or chosen by the model.

Switches need migration `014` (a new install gets the table from `scripts/init_db.sql`). Run
it before the worker, the gateway and the console run the new code, or the switch reads
fail and the audit insert for `switch_off` is refused. It keeps every earlier audit kind,
so run it after `013`:

```bash
uv run python scripts/migrate.py
```

## Database and migrations

Two passwords come from your shell, never from a file in the repository. Export both
before `docker compose up` (Compose refuses to start without them) and before the
migration script:

```bash
export AGENTOS_OWNER_DB_PASSWORD='<owner password>'   # the role "agentos" that owns the tables
export AGENTOS_APP_DB_PASSWORD='<app password>'       # the role "agentos_app" the app connects as
export AGENTOS_DATABASE_URL="postgresql://agentos_app:${AGENTOS_APP_DB_PASSWORD}@localhost:55432/agentos"
```

`AGENTOS_DATABASE_URL` has no default. The worker, the gateway, the console and the
command-line tools exit with a message naming it when it is unset. It must name
`agentos_app`: the table owner and any superuser bypass row-level security, so the app
never connects as `agentos`.

A new install needs nothing more. On the first start of an empty data directory,
Compose creates `agentos_app` with `AGENTOS_APP_DB_PASSWORD` and builds the whole
schema from `scripts/init_db.sql`, which also records every migration as applied.

An existing database is brought up to date with one command, run as the owner role:

```bash
export AGENTOS_OWNER_DATABASE_URL="postgresql://agentos:${AGENTOS_OWNER_DB_PASSWORD}@localhost:55432/agentos"
uv run python scripts/migrate.py
```

It applies the migrations in `scripts/migrations/` that `schema_migrations` does not list,
in order, each in its own transaction. An empty database (no tables at all) is instead
built from `scripts/init_db.sql` in one transaction, with every migration recorded as
applied. It creates `agentos_app` with
`AGENTOS_APP_DB_PASSWORD` when the role is missing and leaves an existing role's password
alone; `--set-app-password` resets it to `AGENTOS_APP_DB_PASSWORD`.

Exactly-once writes need `scripts/migrations/018_external_writes.sql` (the `external_writes` table);
the script applies it with the rest.

The worker, the gateway and the console check `schema_migrations` at start-up and
exit, naming the missing migrations, when the database is behind the code. Run the script
before restarting them after you pull a change that adds a migration.

A database built before `scripts/migrate.py` existed has the migrations but no record of
them. Record the ones it has, then run the script for the rest. The earlier migrations
were written for an empty table and must not be run again over data (`007` narrows a
constraint that later migrations widened), so this skips them:

```bash
uv run python scripts/migrate.py --baseline 015    # the last migration the database already has
uv run python scripts/migrate.py
```

A migration that adds an audit event kind inserts one row into `audit_kinds` and adds the
kind to `AUDIT_KINDS` in `src/agentos/models.py`; add its number to `EXPECTED_VERSIONS` in
`src/agentos/schema_version.py` too. After the first import, the tenants and agents
come from the database; import them once with
`uv run python -m agentos.tenant_store import` and `uv run python -m agentos.agent_store import`
(see [Tenants in the database](#tenants-in-the-database) and
[Agents in the database](#agents-in-the-database)).
Migration `017` stores the hash of each submitted definition; a pending version shown
before it ran must be reopened before it is published.

The new image's system library (glibc) is older than the one the previous image
used, so Postgres may warn about a collation version mismatch, and text indexes
built under the old library must be rebuilt. After recreating the container,
rebuild the indexes and refresh the collation version for the `agentos`,
`postgres` and `template1` databases:

```bash
for db in agentos postgres template1; do
  docker compose exec -T postgres psql -U agentos -d "$db" \
    -c "REINDEX DATABASE $db" \
    -c "ALTER DATABASE $db REFRESH COLLATION VERSION"
done
```

`REINDEX DATABASE` and `ALTER DATABASE ... REFRESH COLLATION VERSION` act on the
database you are connected to, so run them once per database.

## Connections and concurrency

The worker runs up to `AGENTOS_MAX_CONCURRENT_ACTIVITIES` activities at once (default 32), and a
model call makes about six queries, so each process keeps a pool of database connections between
`AGENTOS_DB_POOL_MIN` (default 2) and `AGENTOS_DB_POOL_MAX` (default 32). The worker refuses to
start with a pool maximum below its activity limit, because activities would wait for a
connection. The worker, the gateway and the console each hold their own pool, so the three
defaults together stay under Postgres's default of 100 connections. Raise both numbers together,
and the server's `max_connections` with them.

Model and embedding calls give up on their own (75 seconds for a model call, 60 for one
embeddings request) before their activity's timeout, and the provider SDKs never retry: Temporal
retries the activity.

## Retention

A nightly Temporal schedule (04:00 UTC) purges each tenant's expired rows in batches and logs
counts only. Create or update the schedules once, after the worker is running:

```bash
uv run python -m agentos.retention --schedule            # every tenant
uv run python -m agentos.retention --tenant acme          # purge one tenant now, without Temporal
```

A tenant's limits, in whole days, live in `tenants.retention` (an empty object means the
defaults). Set them as the owner role; the change is recorded in the audit log as `retention_set`:

```bash
export AGENTOS_OWNER_DATABASE_URL=postgresql://<owner>:<password>@localhost:55432/agentos
uv run python -m agentos.retention set --tenant acme --messages-days 180 --tool-outputs-days 60
```

| Setting | Default | What goes |
|---|---|---|
| `messages_days` | 365 | A conversation's messages, once its newest message is older than this (a thread is never cut in the middle), and a thread's stored specialist idle for longer than this |
| `tool_outputs_days` | 90 | Tool output text, tool inputs and stored model replies older than this, in a conversation with no newer message; and the drafts of a task whose newest revision is older than this |
| `idempotency_days` | 90 | Idempotency records older than this, and inbound Slack events (and saved Edit forms) delivered longer ago than this |
| `sessions_days` | 1 | Console sessions, this many days after they expired (they are also purged at sign-in) |

The first three are never shorter than the longest a task can live: an agent definition may
ask for at most 168 hours of approval and a 60-day watch, so the floor is 68 days. `set` refuses
a lower value, and the nightly job raises a lower stored one to it, so nothing a live approval or
follow-up still needs is deleted. The record of when an agent's owner was last mentioned in a
refusal (`owner_pings`) is purged after two days. Audit and usage rows are kept (append-only by
design), as are `external_writes`, `task_links` and agent definitions. An inbound event the relay
has not yet delivered is never purged.

## Run

`scripts/dev-up.sh` starts all of this, the tunnels and the console in one command: see [local-run.md](local-run.md). By hand:

```bash
docker compose up -d                                   # Postgres + Temporal
uv run python -m agentos.worker                        # terminal 1
uv run uvicorn agentos.gateway.app:app --port 8000     # terminal 2
```

Keep your tunnel pointed at port 8000. The Temporal web UI is at
<http://localhost:8233>. DM the bot, or @mention it in a channel it has been
invited to.

## Railway (hosted)

The whole project runs on Railway from code. One app image (the root
`Dockerfile`, uv-built, non-root, `.venv` baked in) starts as the worker, the
gateway or the console; three infrastructure images live under `deploy/`.
`.railway/railway.ts` declares all six services, their volumes and their
non-secret variables. Apply it with the Railway CLI (`railway config plan`,
then `railway config apply`); a deploy does not read the file. Secrets are
never in the file: each is a project shared variable, set once, and a service
gets only the secrets it references as `${{shared.NAME}}`. The two optional
ones (`ANTHROPIC_API_KEY`, `NOTION_WEBHOOK_VERIFICATION_TOKEN`) are `preserve()`
entries in that one service's variables, unset unless needed.

**The hosted runbook** — the maintainer follows these steps once, from a checkout
of this repository. Every `railway` command is the Railway CLI's
([docs.railway.com/cli](https://docs.railway.com/cli)); each step names the page
that documents it.

1. **Install and sign in.** `brew install railway` (other installers:
   [CLI](https://docs.railway.com/cli)), then `railway login`
   ([login](https://docs.railway.com/cli/login)). The first `railway ssh` asks to
   register an SSH key ([ssh](https://docs.railway.com/cli/ssh)); the schema check
   in step 9 needs it.
2. **Create the project and apply the config.** `railway init --name
   agent-architecture` creates the project and links this directory
   ([init](https://docs.railway.com/cli/init)). Install the authoring SDK the CLI
   evaluates `.railway/railway.ts` with: `npm install railway`
   ([IaC](https://docs.railway.com/infrastructure-as-code)). Then `railway config
   plan`, read the changes (variable values are redacted), and `railway config
   apply` ([config](https://docs.railway.com/cli/config)). This creates the six
   services and the three volumes, and each service starts its first deploy at
   once from the GitHub source, so set the shared variables (step 3) first:
   they can exist before any service. Volumes are at most 5000 MB on the
   current plan; a larger size fails the whole apply with no message naming it.
3. **Set the secret variables** the table below lists, once each, as project
   shared variables (dashboard: Project Settings → Shared Variables); the
   services already reference them. For `acme`'s existing
   payload key, the value to paste is the key file's content: the file already
   holds the base64 of the 32 bytes. The maintainer prints it themselves —
   `cat ~/.agentos/keys/acme.key` (the file under their `AGENTOS_KEY_DIR`) — and
   pastes it as the shared variable `AGENTOS_PAYLOAD_KEY_ACME_V1`; no agent runs
   that command. A new tenant's key comes from `python -m
   agentos.tenant_keys value <tenant_id>` (see [Payload keys on a host without
   files](#payload-keys-on-a-host-without-files)).
4. **Generate the public domains.** `railway domain --service gateway` and
   `railway domain --service console` each create a fixed `<name>.up.railway.app`
   domain ([domain](https://docs.railway.com/cli/domain)). The console's
   `AGENTOS_CONSOLE_URL` is declared as `https://${{RAILWAY_PUBLIC_DOMAIN}}` and
   resolves to its domain on its own; check it matches the console URL Slack will
   redirect to.
5. **Deploy in order:** postgres, temporal, ollama (its first start pulls
   `bge-m3`, which takes a few minutes), worker (its pre-deploy command migrates;
   on the empty database it builds the whole schema), gateway, console. Trigger
   each service's deploy from the dashboard, or with `railway up --service
   <name>` from the directory it builds from: the repository root for worker,
   gateway and console, `deploy/<name>` for the three infrastructure services
   ([up](https://docs.railway.com/cli/up)).
6. **Import the tenants and agents once**, inside the worker service
   ([ssh](https://docs.railway.com/cli/ssh)):

   ```bash
   railway ssh --service worker -- python -m agentos.tenant_store import
   railway ssh --service worker -- python -m agentos.agent_store import
   ```

   `config/agents.yaml` ships in the image for these imports; afterwards the
   database is the source of truth.
7. **Point the Slack app at the fixed URLs**, once: Event Subscriptions
   `https://<gateway domain>/slack/events`, Interactivity & Shortcuts
   `https://<gateway domain>/slack/interactivity`, OAuth redirect
   `https://<console domain>/auth/slack/callback`. Reinstall the app if its
   scopes changed. Step 9 prints all three.
8. **Notion webhook (optional).** Follow [Notion webhook](#notion-webhook-optional)
   against the fixed gateway URL, once. The `show` and `confirm` commands run
   where the tenant's payload key variables are set — inside the worker:
   `railway ssh --service worker -- python -m agentos.notion_webhook show --tenant
   <tenant_id>` (and `confirm`).
9. **Check.** `scripts/railway-status.sh` prints one line per check — the six
   deployments, both `/healthz` endpoints, the schema version inside the worker —
   and the Slack URLs to compare. Exit 0 means every check passed; 2 means the
   CLI is missing or not logged in.

| Service | Built from | Start command | Port | Volume mount | Public domain |
|---|---|---|---|---|---|
| postgres | `deploy/postgres` (pgvector/pgvector:0.8.1-pg16) | image default | 5432 | `/var/lib/postgresql/data` | no |
| temporal | `deploy/temporal` (temporalio/temporal:1.9.1) | `temporal server start-dev --ip 0.0.0.0 --db-filename /data/temporal.db` | 7233 | `/data` | no |
| ollama | `deploy/ollama` (ollama/ollama:0.40.16) | entrypoint: serve, then pull `bge-m3` on first start only | 11434 (`PORT`) | `/data` (models in `/data/models`) | no |
| worker | root `Dockerfile` | `python -m agentos.worker` | none | none | no |
| gateway | root `Dockerfile` | `uvicorn agentos.gateway.app:app --host 0.0.0.0 --port $PORT` | `$PORT` | none | yes |
| console | root `Dockerfile` | `python -m agentos.control_api --host 0.0.0.0 --port $PORT` | `$PORT` | none | yes |

Dockerfile start commands run in exec form, so the gateway's and console's
commands are wrapped in `/bin/sh -c "exec …"` for `$PORT` to expand. Services
reach each other at `<service>.railway.internal` over `http://` (the private
network is encrypted); postgres, temporal, ollama and the worker have no public
domain. Generate the gateway's and console's domains in the dashboard.

Variables, by service (names only; a secret is a project shared variable that
the service references as `${{shared.NAME}}`). `${{VAR}}` is Railway's
reference syntax; `POSTGRES_PASSWORD` references the shared
`AGENTOS_OWNER_DB_PASSWORD`.

| Variable | Services | Value shape |
|---|---|---|
| `AGENTOS_DATABASE_URL` | worker, gateway, console | `postgresql://agentos_app:${{AGENTOS_APP_DB_PASSWORD}}@postgres.railway.internal:5432/agentos` |
| `AGENTOS_APP_DB_PASSWORD` | worker, gateway, console | secret: the app role's password, the same in all three |
| `AGENTOS_OWNER_DATABASE_URL` | worker (pre-deploy only) | `postgresql://agentos:${{AGENTOS_OWNER_DB_PASSWORD}}@postgres.railway.internal:5432/agentos` |
| `AGENTOS_OWNER_DB_PASSWORD` | worker (pre-deploy only) | secret: the owner role's password |
| `POSTGRES_PASSWORD` | postgres | secret: the owner role's password, same as above |
| `POSTGRES_USER`, `POSTGRES_DB` | postgres | `agentos`, `agentos` |
| `AGENTOS_TEMPORAL_ADDRESS` | worker, gateway | `temporal.railway.internal:7233` |
| `AGENTOS_EMBED_BASE_URL` | worker | `http://ollama.railway.internal:11434/v1` |
| `AGENTOS_EMBED_MODEL` | worker | `bge-m3` |
| `AGENTOS_KEY_SOURCE` | worker, gateway | `env`: payload keys come from the service's variables |
| `AGENTOS_PAYLOAD_KEY_ACME_V1` | worker, gateway | secret: tenant `acme`'s payload key; one `AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>` per tenant and version, the same in both services |
| `SLACK_SIGNING_SECRET` | gateway | secret |
| `SLACK_BOT_TOKEN` | worker, gateway, console | secret (or the variable the tenant's `secrets` block names) |
| `NOTION_TOKEN` | worker, gateway, console | secret (or the tenant's own variable) |
| `NOTION_WEBHOOK_VERIFICATION_TOKEN` | gateway | secret, optional local override; leave unset so the token lives in the database |
| `ANTHROPIC_API_KEY` | worker | secret, for the Anthropic provider |
| `QWEN_API_KEY`, `QWEN_BASE_URL` | worker | secret and endpoint, for the Qwen provider |
| `AGENTOS_LLM_PROVIDER`, `AGENTOS_MODEL` | worker | `qwen`, `qwen3.7-plus`: the hosted chat model. The worker refuses to start when the model has no entry in `config/prices.yaml` (shipped prices: `qwen3.7-plus`, `bge-m3`) |
| `SLACK_CLIENT_ID`, `SLACK_CLIENT_SECRET` | console | secrets; sign-in needs both, plus `AGENTOS_CONSOLE_URL` |
| `AGENTOS_CONSOLE_URL` | console | `https://${{RAILWAY_PUBLIC_DOMAIN}}`: the exact public base URL; the Host and Origin allowlists are built from it |
| `NOTION_OAUTH_CLIENT_ID`, `NOTION_OAUTH_CLIENT_SECRET` | console | secrets, optional (the grant picker) |
| `OLLAMA_MODELS` | ollama | `/data/models` |
| `RAILWAY_RUN_UID` | temporal | `0`: the only service that runs as root (see below) |

**Payload keys** follow [Payload keys on a host without files](#payload-keys-on-a-host-without-files):
`AGENTOS_KEY_SOURCE=env` on the worker and the gateway, one
`AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>` variable per tenant key version, the same
in both services. Create a tenant's key with `python -m agentos.tenant_keys
value <tenant_id>` and paste the printed variable into both services; keep
every older version when rotating. There is no key directory and no volume.

**The Notion webhook token** lives in the database, encrypted with the
tenant's payload key, so the gateway needs no token directory. Follow
[Notion webhook](#notion-webhook-optional): after the handshake, print and
confirm the pending token with `python -m agentos.notion_webhook show --tenant
<tenant_id>` and `python -m agentos.notion_webhook confirm --tenant
<tenant_id>`, run where the tenant's payload key variables are set. Leave
`NOTION_WEBHOOK_VERIFICATION_TOKEN` unset: while it is set the handshake is
refused and events are verified against the variable instead of the database.

Migrations run as the worker's pre-deploy command,
`python scripts/migrate.py --set-app-password`, before the worker starts; it
creates the app role with `AGENTOS_APP_DB_PASSWORD` and applies what the
database lacks. The first deploy needs nothing else: on an empty database the
script builds the whole schema from `scripts/init_db.sql` and records every
migration as applied, so one command brings a fresh Railway Postgres current.
The gateway and console refuse a behind schema at start-up, so deploy the
worker first when a change adds a migration.

Volumes are declared in `.railway/railway.ts`: `postgres-data`,
`temporal-data` and `ollama-models`. Railway mounts a volume root-owned. The
temporal image runs as the non-root `temporal` user, so that service alone
sets `RAILWAY_RUN_UID=0` to run as root and write its `/data` volume. The app
image stays non-root and mounts no volume: payload keys are variables and the
webhook token is in the database.

Health checks: the gateway and the console answer `/healthz`; the console's
`/healthz` bypasses its Host guard, so Railway's `healthcheck.railway.app`
probe passes. The console binds `0.0.0.0` only with sign-in configured, and
refuses a development-default database password off loopback. At the first
deploy, check the console's Host rule behind Railway's proxy: with sign-in
configured the Host and Origin allowlists come from `AGENTOS_CONSOLE_URL`, so
a request whose `Host` header is the public domain must be accepted and a
foreign one refused. No code change is planned for it.

The hosted services do not read the agent YAML at run time: tenants and agents
come from the database. `config/agents.yaml` ships in the image (the default of
`AGENTOS_CONFIG`) for the one-time imports, `python -m agentos.tenant_store
import` and `python -m agentos.agent_store import`.

### Releases

Every service deploys from the `deployment/prod` branch, never from `main`, so
merging work into `main` deploys nothing. A release moves `deployment/prod`
forward to a commit already on `main` (a fast-forward), and each service then
builds and deploys that commit. The maintainer does this, in their own terminal
or on GitHub:

```bash
git push origin main:deployment/prod
```

Deploy the worker first when the release adds a migration (see step 5).

### Logs

`railway logs --service <name> --lines 200` prints one service's recent runtime
logs without streaming; `--build` shows the build logs and `--latest` the latest
deployment even when it failed ([logs](https://docs.railway.com/cli/logs)).

### Backups

**Postgres, logical.** `pg_dump` runs inside the postgres container and streams
to a local file; no password is printed
([ssh](https://docs.railway.com/cli/ssh)):

```bash
railway ssh --service postgres -- pg_dump -U agentos -d agentos --no-owner --no-privileges > "agentos-$(date +%F).sql"
```

Restore into an empty `agentos` database (a fresh postgres service or volume):

```bash
railway ssh --service postgres -- psql -U agentos -d agentos -v ON_ERROR_STOP=1 < agentos-YYYY-MM-DD.sql
```

The dump holds tenant data: keep it access-controlled, and never commit it.

**Volumes.** Railway backs up mounted volumes: the service settings' Backups tab
schedules daily (kept 6 days), weekly (27) or monthly (89) backups and lists
manual ones ([backups](https://docs.railway.com/volumes/backups)). Enable daily
backups on `postgres-data` and `temporal-data`; `ollama-models` holds only
re-pullable models. Restore from the same tab: Restore stages a new volume
mounted at the same path, and Deploy completes it. A restore stays within the
same project and environment, and wiping a volume deletes its backups.

**Temporal, portable copy.** The dev server's state is the SQLite file
`/data/temporal.db`. The volume backups above cover it (the feature explicitly
includes SQLite files). For a local copy, stop the service first so the file is
not being written:

```bash
railway down --service temporal        # removes the latest deployment; the service stays
railway volume files --volume temporal-data download /temporal.db "./temporal-$(date +%F).db"
railway redeploy --service temporal
```

([down](https://docs.railway.com/cli/down),
[volume](https://docs.railway.com/cli/volume),
[redeploy](https://docs.railway.com/cli/redeploy)).

### Cost

`railway usage` shows the workspace's current and estimated bill; `railway usage
projects --project agent-architecture` breaks it down per service (CPU, memory,
egress, volume, backup) ([usage](https://docs.railway.com/cli/usage); the
dashboard view: [project usage](https://docs.railway.com/projects/project-usage)).
What dominates: Ollama's memory (the `bge-m3` model stays loaded), the Postgres
and Temporal volumes, and six always-on services. A spend alert and a hard
limit: `railway usage limit set --target workspace --soft 75 --hard 125`.

### Local runs

The quick-tunnel steps stay only for local runs: see [Slack tunnel and live
checks](#slack-tunnel-and-live-checks) and [local-run.md](local-run.md). Hosted,
the gateway and console URLs are fixed, so the Slack settings are entered once.

## Tests

The tests use fakes only (a scripted LLM, a fake Slack, a fake Notion and
in-memory stores) plus Temporal's time-skipping test server, which downloads a
test-server binary on first use. They never call Claude, Slack or Notion.

```bash
uv run pytest tests/core/test_ids.py tests/core/test_config.py \
  tests/connectors/test_notion_connector.py tests/core/test_conversation_workflow.py \
  tests/core/test_task_workflow.py tests/core/test_gateway.py -v
uv run ruff check . && uv run ruff format --check .
```

The `pg` tier in `tests/pg/` runs the SQL (policies, grants, triggers, every store,
`init_db.sql` against the migrations, `scripts/migrate.py`) against the Postgres that
Compose publishes on 127.0.0.1:55432. It builds throwaway databases named
`agentos_test_<pid>_<n>`, drops them at the end, touches no other database and no
password, and skips itself when the server is down. The owner connection is
`AGENTOS_PG_TEST_OWNER_URL` (default `postgresql://agentos:agentos@127.0.0.1:55432/postgres`).

```bash
uv run pytest tests/pg -v -m pg
```

## Knowledge: indexing and search

An agent with a Notion page grant (`grants.notion.pages`) can answer from every
page under that page. Index the pages first, with the worker running (the
worker does the reading and embedding; the command only asks Temporal to start
it):

```bash
uv run python -m agentos.index_knowledge             # one run per granted page, now
uv run python -m agentos.index_knowledge --wait      # ... and print a summary per run
uv run python -m agentos.index_knowledge --schedule  # nightly runs at 03:00 UTC
uv run python -m agentos.index_knowledge --confirm-gone  # see "A page that cannot be read"
```

Indexing calls the embeddings endpoint, so it **spends embedding credit**;
pages whose content has not changed since the last run are not embedded again.
Two agents granted the same page share one run. `--schedule` creates or updates
one schedule per granted page and deletes the schedules of pages that are no
longer granted. Pages removed from the tree or trashed are removed from the
index on the next run.

What is indexed, how pages that cannot be read are treated, how a webhook refreshes a single page
and how a search is checked and cited are in [architecture.md §6](architecture.md#6-knowledge-retrieval).
A scope whose granted page is gone is reported to the agent's creator; to delete its index sooner
than three nightly runs, run `uv run python -m agentos.index_knowledge --confirm-gone` (add
`--tenant` for one tenant).

### Upgrading an existing index

Migration `021_knowledge_health.sql` adds what each page row records (embedding model,
last-edited time, parent, ancestors, sub-pages) and the scope-health table. Apply it as the
owner role like the others, then run `uv run python -m agentos.index_knowledge --wait` once:
until a page row is read again it has no recorded embedding model, so searches skip it. That
first run reads every page but embeds none, because the content has not changed.

```bash
docker compose exec -T postgres psql -U agentos -d agentos < scripts/migrations/021_knowledge_health.sql
```

### Keeping the index fresh

Set up the Notion webhook (above) to refresh a page within about 30 seconds of an edit. The
nightly run (`--schedule`) remains the backstop for anything the webhook misses.

### Local embeddings with Ollama

To embed on your own machine instead of a hosted endpoint:

1. Install the Ollama app. The Homebrew build may lack the runner `bge-m3` needs.
2. Run `ollama pull bge-m3`.
3. Export `AGENTOS_EMBED_BASE_URL=http://localhost:11434/v1` and
   `AGENTOS_EMBED_MODEL=bge-m3`, then restart the worker. No key is needed, and
   the Qwen variables are not used for embeddings.
4. Changing the embedding model re-embeds every page on the next run, because
   the content hash includes the model name. Until a page is re-embedded its
   old chunks are never searched.

## Agents in the database

The lifecycle of agent versions (draft, pending, published, superseded), how a published version is read and how grants apply are in [architecture.md §5](architecture.md#agent-versions-roles-and-publishing).

A definition has the shape of one agent block in the YAML file and is parsed by the same code, so it is validated the same way. The front-door rules (a specialist exists, is not a front door itself, is listed once) hold for a tenant's published set: a publish or import that would break them is refused.

`max_writes_per_turn` in a definition (a whole number from 1 to 10, default 2) is how many writes one model reply may start; what happens to the rest is in [architecture.md §3](architecture.md#turn-loop-conversationworkflow).

```yaml
max_steps: 6
max_writes_per_turn: 2   # optional
```

A thread handed to a specialist is remembered in the `threads` table (migration `025`), which also adds `owner_pings`.

Tenant settings stay in the YAML file: `tenant_id`, `slack_team_id`,
`default_agent`, `channels`, `notion_workspace_id` and `secrets`. The worker and
the gateway refuse to start if `default_agent` or a channel names an agent with
no published version.

The import copies each YAML agent that has no row yet in as a published version
1 (`import:yaml` is the creator and approver). Running it again skips each
agent that exists and names it. **After the import, editing the `agents`
section of the YAML file changes nothing**: the database is the source.
`check_grants` reads the database, not the YAML file: run it with `--tenant` and
`--agent` on a draft to get a grant's property IDs before publishing (`--version`
picks another version).

Known gap: a version published after the worker started that adds a Notion page
grant gets no index schedule and no startup grant check until the worker
restarts, and a worker started with no page grants has no embedder until it
restarts. Run `uv run python -m agentos.index_knowledge --schedule` after such a
publish.

## Members and roles

The roles, and what each may do, are in [architecture.md §5](architecture.md#agent-versions-roles-and-publishing).

The first admins come from the tenant YAML file:

```yaml
admins: [U0123ABCD]   # Slack user IDs
```

An admin listed there is always an admin, and nobody can remove or demote them
through the app. Other admins and the creators are added by an admin and stored
in `members`; the app role can delete rows there, and the audit log keeps the
record. **A tenant with no `admins` can publish nothing**, because only an admin
can publish.

A refused control-plane action is not audited; the refusals that are (`audience_refused`, and a refused write recorded as `approval` then `write` with an error) are listed in [audit-events.md](audit-events.md). Run `uv run python scripts/migrate.py` on an existing database.

## Audit log

What the audit log records, and how it is kept append-only, is in [architecture.md §10](architecture.md#10-data-and-storage). Audit rows are exempt from retention: they are kept until someone removes them outside the app.

Every event kind, what it records and how to read it: [audit-events.md](audit-events.md).

Print a tenant's events, newest first, as JSON lines:

```bash
uv run python -m agentos.audit --tenant acme --limit 50
```

## Control-plane API

A JSON API for agents, versions, members and the audit log, in a process of its
own. It is not part of the gateway, and it listens on this machine only:

```bash
AGENTOS_CONTROL_ACTOR=U0123ABCD uv run python -m agentos.control_api   # 127.0.0.1:8100
```

The API refuses browser cross-site requests (a foreign `Origin` or
`Sec-Fetch-Site`), any `Host` header but its own loopback address and port, and
any write that is not `application/json`.

**Without sign-in configured, never point a tunnel at this port**, and do not add
it to `docker-compose.yml` (a container binds `0.0.0.0`). The process then exits
unless the host is `127.0.0.1`, `::1` or `localhost`; no flag overrides that. It
also exits without `AGENTOS_CONTROL_ACTOR`, your Slack user ID: every request acts
as that user, and nothing in a request (header, query or body) can change it. It also
needs each tenant's Notion key exported (`NOTION_TOKEN`, or the variable the tenant's
`secrets.notion_token_env` names), because the grant check and publish read Notion
with it; the process exits naming an unset variable. With
[sign-in configured](#web-console) the process may bind more widely and every
request acts as the signed-in user instead. What the user may do comes from their role in the
tenant named in the path (`/tenants/<tenant id>/...`), decided by
`ControlPlane`: 403 when the role does not allow it, 404 for an unknown tenant,
agent, version or member, 409 for a wrong state (publishing a draft or a stale
version, adding an existing member), 422 for an invalid definition or request.

| Request | Does |
|---|---|
| `GET /healthz` | liveness, no identity |
| `GET /tenants/{t}/me` | your user ID and role |
| `GET`, `POST /tenants/{t}/agents` | list agents; create one as a draft (`{agent_id, definition}`) |
| `GET /tenants/{t}/agents/{a}/versions[/{v}]` | versions, newest first; one version |
| `POST /tenants/{t}/agents/{a}/drafts` | new draft (`{from_version?}`) |
| `PUT /tenants/{t}/agents/{a}/versions/{v}` | replace a draft (`{definition}`) |
| `POST .../versions/{v}/submit`, `withdraw`, `reject`, `publish` | lifecycle |
| `POST .../versions/{v}/check-grants` | check the version's Notion database grants against Notion |
| `GET`, `POST /tenants/{t}/members`; `PATCH`, `DELETE .../members/{user}` | members |
| `GET /tenants/{t}/budgets` | budgets against this month's spend: the organisation line (admins) and the agents (a creator sees their own) |
| `GET /tenants/{t}/switches`, `PUT`, `DELETE /tenants/{t}/switches/{agents/a, all-agents, connectors/notion, writes}` | kill switches; see [Kill switches](#kill-switches); changes are admins only |
| `PUT`, `DELETE /tenants/{t}/budget`; `PUT`, `DELETE /tenants/{t}/agents/{a}/budget` | set or raise (`{amount}` in dollars, at most two decimals) or remove the organisation or an agent budget; admins only |
| `GET /tenants/{t}/usage?month=YYYY-MM` | usage and cost by agent, team and model for a UTC month (default the current one); a creator sees only their agents |
| `GET /tenants/{t}/audit?limit=&before=` | audit log, newest first |

`publish` takes the hash of the version you read (`{"definition_sha256": "..."}`, the
`definition_sha256` of the version record) and is refused with 409 when the version carries
another, which a withdraw, an edit and a resubmit under the same number produces; a
publish without it is refused with 422. It runs the grant check next (with the tenant's
Notion token) and is refused with 409 and the problems while any database or page grant has one (a
page must be readable and not in the trash). If Notion cannot be read right now
(a network error, a rate limit, a server error) the answer is 502 and nothing is
published. A new page grant still gets no index schedule until
the worker restarts. The audit log is paged by `seq`: pass the previous page's
`next_before` as `before`; `limit` is capped at 200. A write may carry an
`Idempotency-Key` header: a retry with the same key, path and body records one
audit event (the change itself is refused the second time, with 409).

## Agent audiences

An agent version can say who may use it and where:

```yaml
audience:
  channels: [C0123ABCD]          # channels the agent may answer in
  dm: everyone                   # or: {user_groups: [S0123ABCD]}
  guests: false                  # true lets single and multi-channel guests in
```

Edit it in the console with the rest of the definition; a change is a new version that an admin publishes. A definition without `audience` keeps working. What counts as a person, what a refused user sees, how the publish confirmation works and the API form of it are in [architecture.md §5](architecture.md#deployment-and-audience).

A channel that the tenant file's `channels:` maps to an agent must be in that agent's audience. Publishing, and starting the worker, refuse it otherwise.

**In the Slack app** (api.slack.com/apps), before the first run with audiences:

1. OAuth & Permissions → Scopes → **Bot Token Scopes**: add `channels:read`, `groups:read`
   and `usergroups:read`. `users:read` is already there.
2. **Reinstall the app** to the workspace (adding a scope does not take effect until you do),
   then export the new bot token to the worker and to the console process.

The console process now also reads each tenant's Slack bot token (the variable
`secrets.slack_bot_token_env` names, `SLACK_BOT_TOKEN` by default) to count the people; a
tenant without it can't publish until it is set.

### Audience changes are watched

An agent is paused in a channel when the channel is shared with another organisation
(Slack Connect). What a paused channel shows, how pending approval cards are stopped and how an
owner resumes are in [architecture.md §5](architecture.md#audience-changes-are-watched).

> **Untested live.** This pause is covered by the automated tests only. It has not been
> run against a real Slack Connect channel, which needs a paid Slack plan on both sides.

The pauses need migration `013` (a new install gets the table from `scripts/init_db.sql`;
run it before the worker and the gateway restart, or the audit insert for a pause is
refused). It also keeps the audit kind `budget_set`, so run it after the budgets migration
when both are in use:

```bash
uv run python scripts/migrate.py
```

**In the Slack app** (api.slack.com/apps), Event Subscriptions → Subscribe to bot events,
add `channel_shared` (a channel was shared with another organisation), then save and
**reinstall the app**. (`app_mention` and `message.im` stay.) It needs the bot scopes
`channels:read` and `groups:read`, already added for audiences. The bot only hears about
channels it is a member of. `slack/app-manifest.yaml` lists the same events and
scopes.

## Slack posts land once

Slack has no idempotency key, so a retried post could appear twice. Each post carries a marker in its message metadata, and a retry that may have posted already looks the message up by that marker before posting again. The lookup reads the thread's replies, or the channel's recent history.

**In the Slack app** (api.slack.com/apps), before relying on it:

1. OAuth & Permissions → Scopes → **Bot Token Scopes**: add `channels:history`,
   `groups:history` and `mpim:history`. `im:history` is already there.
2. **Reinstall the app** to the workspace, then export the new bot token to the worker.

Without a scope the worker still posts: a retry that cannot look the message up posts it again
and logs `possible duplicate Slack post` with the tenant ID and key, and the log names the
missing scope once per tenant. A public channel the bot has not joined refuses the lookup the
same way. `slack/app-manifest.yaml` lists the same scopes.

## Web console

The same process serves web pages for agents, versions, members, usage and the audit log,
behind Sign in with Slack. The pages call the same `ControlPlane` methods as the
JSON API, so a user's role in the tenant decides what they can do in both. The
console is off unless sign-in is configured.

The pages call the same `ControlPlane` methods as the JSON API. Who sees what, the browser guard, the session cookie and the error pages are in [architecture.md §5](architecture.md#control-plane-and-console).

**In the Slack app** (api.slack.com/apps, the app the gateway uses):

1. OAuth & Permissions → Redirect URLs → add `<AGENTOS_CONSOLE_URL>/auth/slack/callback`
   and save.
2. Sign in with Slack uses the OpenID scopes `openid` and `profile`. Add them under
   OAuth & Permissions → Scopes → **User Token Scopes** (not Bot Token Scopes). The
   bot scopes for sign-in are unchanged; agent audiences need three more, see
   [Agent audiences](#agent-audiences).
3. Basic Information → App Credentials: the Client ID and Client Secret.

**In the shell that runs the process** (nothing is stored in a file):

```bash
export SLACK_CLIENT_ID=...                  # the Slack app's client ID
export SLACK_CLIENT_SECRET=...              # its client secret
export AGENTOS_CONSOLE_URL=https://console.example.com   # the public base URL
export NOTION_TOKEN='<paste here>'          # the tenant's Notion key (its variable, see below)
uv run python scripts/migrate.py
uv run python -m agentos.control_api --host 0.0.0.0 --port 8100
```

The grant check ("Check Notion grants", and inside every publish) reads Notion with
the tenant's own Notion key, so the process needs it too. The variable is the one the
tenant's `secrets.notion_token_env` names (`NOTION_TOKEN` by default,
`NOTION_TOKEN_GLOBEX` in the example above); export one per tenant. The process
refuses to start, naming the unset variable, when any tenant's is missing.

**Choose from Notion (optional).** A creator may grant only what their own Notion account
can see. With the picker on, "Choose from Notion" on a draft signs the creator in to
Notion, lists what their token can search, and records their picks (migration `020`; a new
install gets the table from `scripts/init_db.sql`). Submit then refuses a grant that is new
since the published version and was not picked. The creator's Notion token is used once to
list and is never stored.

1. In Notion (notion.so/profile/integrations), create a **public** integration. Under
   OAuth Domain & URIs add the redirect URI `<AGENTOS_CONSOLE_URL>/auth/notion/callback`.
   Copy its OAuth client ID and secret.
2. Export, in the console's shell:

```bash
export NOTION_OAUTH_CLIENT_ID=...           # the public integration's OAuth client ID
export NOTION_OAUTH_CLIENT_SECRET=...       # its OAuth client secret
export AGENTOS_REQUIRE_GRANT_PICKER=1       # optional: refuse unpicked new grants at submit
```

Setting only one of the two OAuth variables, or `AGENTOS_REQUIRE_GRANT_PICKER` without
both, stops the process at start. Without them, behaviour is as before: the admin still
sees each grant's live title and parent, a warning when the creator's label and the live
title don't contain one another as whole words once normalised, and a diff against the published version,
and must tick a box to publish a version that widens
access. Run `uv run python scripts/migrate.py` for migration `020` on an existing database.

Sign-in counts as configured only when all three variables are set and the URL is
`https://`. The one exception is a local trial at `http://127.0.0.1:<port>` or
`http://localhost:<port>`, where cookies are not `Secure` (a browser would not send
them over plain http). A partial configuration, or any other URL, stops the process
at start. A new install gets the `sessions` table from `scripts/init_db.sql`.

To run it locally, set `AGENTOS_CONSOLE_URL=http://127.0.0.1:8100`, add the same
address plus `/auth/slack/callback` as the Slack app's redirect URL (Slack may insist
on https there; then use a tunnel), start the process with the default host, and open
that URL.

A **public URL (a tunnel, a reverse proxy) is allowed only once sign-in is
configured.** Then the process may listen beyond loopback, the fixed operator
(`AGENTOS_CONTROL_ACTOR`) is not used and nothing acts without a session, and the
`Host` allowlist is the host of `AGENTOS_CONSOLE_URL` plus loopback. Point a tunnel
at this port only in that mode. It also refuses to listen beyond loopback while the
database password in `AGENTOS_DATABASE_URL` is empty or a development default.

## Slack tunnel and live checks

The gateway is reached through a tunnel (`cloudflared tunnel --url http://127.0.0.1:8000` or `ngrok`). A quick tunnel's URL changes on every restart and it can drop after a laptop sleep or a network change, after which Slack messages get no reply. Check it before a live run:

```bash
curl -s <tunnel URL>/healthz      # expect {"ok":true}
```

If it fails, restart the tunnel and update both Slack app settings with the new URL: Event Subscriptions to `<URL>/slack/events` and Interactivity & Shortcuts to `<URL>/slack/interactivity`. Messages sent while the tunnel was down are lost and must be sent again. A changed tunnel URL also means the Notion webhook subscription must be recreated (a verified subscription's URL can't be edited): follow [Notion webhook](#notion-webhook-optional).
