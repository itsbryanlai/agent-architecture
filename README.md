# agentos: walking skeleton

This repo is a platform for Slack agents that *do work*: they answer from the
sources their creator granted, take actions only after the requester approves,
and follow up for days until the work is done. The design is in
[`docs/product.md`](docs/product.md), [`docs/architecture.md`](docs/architecture.md)
and [`docs/decisions.md`](docs/decisions.md).

The code here is a walking skeleton of that design, built on Temporal
(Python SDK), Postgres, Slack and Notion. It covers one path end to end:

1. A Slack DM (or an @mention in a channel) reaches the gateway, which stores
   the event in Postgres and answers Slack at once. A relay loop in the gateway
   then signal-with-starts a `ConversationWorkflow` for that thread, and retries
   until Temporal takes it, so a Temporal outage never loses or repeats a message.
   Temporal history holds IDs and small values only: the text of the message, the
   model's reply, each tool call's input and a task's draft live in Postgres.
2. Claude answers using Notion read tools (`search_items`, `my_items`), or
   asks to create an item.
3. Creating an item starts a `TaskWorkflow`, which DMs the requester an
   approval card. Nothing is filed until they click **Submit**.
4. Submit creates the Notion item exactly once, even if the worker crashes
   and retries: a lease in Postgres lets one attempt write at a time, and an
   existing database needs `scripts/migrations/018_external_writes.sql`.
5. The task watches the item's status and posts in the original thread when it
   reaches a "done" value.

Claude can also draft a comment on an existing item (`add_comment`). The same
approval card is sent, and the comment is posted only after **Submit**, exactly
once, and only on items in a database the agent was granted. Every approval
card has an **Edit** button: it opens a form prefilled with the draft, and
saving replaces the draft and asks for a fresh approval (the old buttons stop
working). A request can be edited up to 5 times. Each edit is stored as a new
revision of the draft, and an approval names the revision it approved, so what
runs is exactly what the requester saw.

One deployment can serve several tenants (see [Several tenants](#several-tenants)).
Tenants and agents live in the database (see
[Tenants in the database](#tenants-in-the-database) and
[Agents in the database](#agents-in-the-database)). A tenant starts as a YAML
file, such as `config/agents.yaml`, that you import once. Every workflow ID,
database row and log line carries the tenant ID.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (it installs Python 3.12 for you)
- Docker with Docker Compose
- A tunnel tool: `cloudflared` or `ngrok`
- A Slack workspace where you can create apps, a Notion workspace where you
  can create integrations, and an Anthropic API key

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
2. Notion sends a one-time handshake. The gateway writes the verification token
   to a file readable only by you, and logs the path (never the token). The file
   is `notion-webhook-verification-token-<tenant_id>` in `~/.agentos`, unless
   you set `AGENTOS_NOTION_TOKEN_DIR`. While that file exists a second handshake
   gets 409 and changes nothing: delete the file to subscribe again.
3. Paste the token from that file into the Notion integration to finish
   subscribing, then export it in the variable the tenant's config names
   (`NOTION_WEBHOOK_VERIFICATION_TOKEN` by default), for example
   `export NOTION_WEBHOOK_VERIFICATION_TOKEN='<token>'`, and restart the
   gateway.

The setup message is accepted only while no token is configured.

Once the token is exported, the gateway refuses further handshakes (409) so the
file cannot be overwritten; to re-subscribe, unset that tenant's webhook token
variable and restart the gateway first.

Until the token is exported, events get a 503. Events with a missing or wrong
signature (a signature made with another tenant's token is wrong) get a 401. A
path that names no tenant gets a 404. None of these reaches a workflow. If the
tenant's config sets `notion_workspace_id`, a correctly signed event from any
other Notion workspace is ignored (200, no action).

| Variable | Default | Meaning |
|---|---|---|
| `NOTION_WEBHOOK_VERIFICATION_TOKEN` | none | Notion's verification token for a tenant that has no `secrets` block (secret; export it, never save it in a dotenv file). Without it every webhook event for that tenant is refused. Other tenants name their own variable |
| `AGENTOS_NOTION_TOKEN_DIR` | `~/.agentos` | Directory where the gateway writes each tenant's token file, `notion-webhook-verification-token-<tenant_id>`, at subscription time (mode `0600`, directory `0700`) |

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

- Where a message lands: a DM or a mention in an unmapped channel goes to
  `default_agent`; a mention in a channel listed under `channels` goes straight
  to that agent.
- The front door's "thinking" message becomes "<name> here, picking this up.",
  and the specialist answers the same message below it and keeps the thread.
  Anything else the front door asked for in that reply is not run. A new
  thread starts back at the front door.
- A specialist cannot hand off, and a thread is handed off at most once. The
  specialist acts with its own grants, and any request it starts carries its
  own agent.
- A thread idle for seven days closes. A later message in it starts again at
  `default_agent`, which can hand off again.
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
  variables (a tenant whose variable is unset or whose key file is unusable is
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
unsafe key file and the command that creates it. Errors and failure messages in
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
so each needs the keys it will use.

| Variable | Default | Meaning |
|---|---|---|
| `AGENTOS_KEY_DIR` | `~/.agentos/keys` | Directory of per-tenant key files, `<tenant_id>.key` and `<tenant_id>.v<N>.key` |
| `AGENTOS_ALLOW_PLAINTEXT_PAYLOADS` | off | Accept payloads that were never encrypted (old histories only) |

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

Every model call and every embedded page is recorded as a usage row: the tenant,
the agent and agent version, the model the provider reported, the four token
counts (input, output, cache read, cache write), the cost, the conversation ID
for tracing and the time. A row never names the person who asked and never holds
text. Each billed attempt is counted once: if Temporal retries a call whose first
answer was lost, the provider bills both attempts and both are recorded. A call is
priced when it is recorded, with the entry in effect that day,
and the cost is stored on the row, so editing a price later never changes past
months.

Prices live in `config/prices.yaml` (override the path with `AGENTOS_PRICES`):
US dollars per million input, output, cache-read and cache-write tokens for a
model, with an `effective_from` date; a model can have several entries. The file
ships empty, with a commented example, because prices come from your provider's
pricing page. A model with no entry is recorded with its tokens and no cost, and
the page shows "no price". The worker reads the file when it starts, so restart it
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

An admin can set a monthly budget in US dollars for the whole organisation and for each
agent, for the UTC calendar month. No budget means no limit. The organisation budget counts
every model call and every embedding; an agent's budget counts that agent's model calls
across all its versions.

- From **80%** the Usage page, and the agent's page, show a warning with the spend, the
  budget and the percent. Spend is shown per cost type, side by side: model and embedding,
  and the total (an agent's line shows model calls only). The Usage page and
  `GET /tenants/{t}/budgets` carry the same split.
- At **100%** the agent answers every message with "<agent name> has reached its monthly
  budget. An admin can raise it in the console." and does not call the model, so nothing
  more is billed and no usage row is written. This applies when either the agent's or the
  organisation's budget is reached. Each stopped turn logs one line with the tenant, the
  agent and which budget, and nothing else.
- At **100% of the organisation budget** indexing stops as well: no page is embedded, and
  each stopped page logs one line with the organisation, the scope and the budget. Nothing
  already indexed is removed because of the stop, and the run's summary counts the pages it
  held back as `deferred`. Indexing resumes by itself when an admin raises or removes the
  budget (within a minute) or the UTC month changes; the next nightly run, or a page event,
  catches up. An agent's budget never stops indexing.
- Admins set, raise and remove budgets on the **Usage** page and through the API; each
  change is an audit event (`budget_set`, with the new amount, or `0` for a removal).
  Creators see the budgets of the agents they created, read-only.
- The worker reads the month's spend at most once a minute per organisation, in one
  query however many turns ask at once, and adds each call it records to that figure at
  once. The most a budget can be overspent is what the other worker processes spend in one
  minute, plus the calls already running, plus the cost of one call (a call's cost is
  unknown until it returns); a raised budget lifts a stop within a minute.
- Spend that cannot be measured is not zero. When a budget is set and the month has a call
  with no price, that agent (or, for the organisation budget, every agent and indexing) is
  paused with its own reply ("... spending can't be measured this month ..."), and the
  pages and API say why, per cost type. Raising the budget does not lift it. Add the price
  to `config/prices.yaml`, restart the worker, and reprice the month (see Usage above).
  The same pause covers a call whose usage row could not be saved after three tries (also
  logged at error level with the token counts), for one minute, in that worker process.
- If the worker cannot read the budgets or the spend, the model call fails and Temporal
  retries it; a call is never let through unchecked.

Budgets need migration `012` (a new install gets the table from `scripts/init_db.sql`).
Run it before the worker or the console runs the new code, or the budget reads fail and the
audit insert for `budget_set` is refused:

```bash
uv run python scripts/migrate.py
```

## Kill switches

An admin can turn things off at once, from the console's **Switches** page, through the API,
or from the host with a command. Each takes effect on the next step, with no restart: every
check reads the `kill_switches` table again, with no cache, in an activity or in the
gateway. Nothing turned off is held for later, and work already finished is not undone.

| Switch | What it stops |
|---|---|
| An agent | Every turn of that agent gets "<agent name> is turned off by an admin." The model is not called, nothing is billed and no usage row is written. This is checked before the budget. It also stops everything else the agent does: a read tool already dispatched returns an error without calling Notion, follow-up polling ends with no post, and the agent is not offered to a front door as a specialist. Pending approval cards stop too: Submit on one, or a write approved but not yet run, writes nothing, and the card says the agent is turned off by an admin and that nothing was changed. |
| All agents | The same reply for every agent of the organisation. |
| A connector (`notion`) | Its tool calls return an error the model passes on, knowledge search refuses, and the approval step of a comment is refused. Page events are dropped, and a full indexing run or its prune step stops with a failure instead of reading or deleting anything. Follow-up polling makes no Notion call and the task keeps watching, so the next poll after the switch is back on carries on. A write approved before the switch went off is refused. |
| All writes | Read-only mode. Agents can still draft and the approval card is posted. Clicking Submit gets "Writes are turned off by your admin; nothing was changed." in the card's conversation, and the card stays, so Submit works again once writes are back on. A write that was approved before the switch went off, but had not run yet, is refused before the call to Notion: the task ends, tells the requester that nothing was changed and that they can ask again later, and is not held. |

Only admins turn switches off and on, never a creator, not even on their own agent. A
creator sees the state on the agent's page, read-only. Turning off every agent or all writes
asks for a tick in the console form. Each change is an audit event, `switch_off` or
`switch_on`, naming the agent for an agent switch and the scope in `tool`; turning off what
is already off, or on what is already on, changes and records nothing.

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

If a switch cannot be read, the step fails and Temporal retries it; a step is never let
through unchecked.

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
in order, each in its own transaction. It creates `agentos_app` with
`AGENTOS_APP_DB_PASSWORD` when the role is missing and leaves an existing role's password
alone; `--set-app-password` resets it to `AGENTOS_APP_DB_PASSWORD`.

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

```bash
docker compose up -d                                   # Postgres + Temporal
uv run python -m agentos.worker                        # terminal 1
uv run uvicorn agentos.gateway.app:app --port 8000     # terminal 2
```

Keep your tunnel pointed at port 8000. The Temporal web UI is at
<http://localhost:8233>. DM the bot, or @mention it in a channel it has been
invited to.

## Tests

The tests use fakes only (a scripted LLM, a fake Slack, a fake Notion and
in-memory stores) plus Temporal's time-skipping test server, which downloads a
test-server binary on first use. They never call Claude, Slack or Notion.

```bash
uv run pytest tests/test_ids.py tests/test_config.py tests/test_notion_connector.py \
  tests/test_conversation_workflow.py tests/test_task_workflow.py tests/test_gateway.py -v
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

The pages indexed are the granted page, every sub-page beneath it at any depth,
and the rows of any database placed on those pages (a row is indexed as a page of
the same grant: its title, its properties as text and its body). Text nested more
than five levels inside one page (a toggle in a toggle in ...) is not indexed:
each level costs a Notion call per block that has children, and real pages rarely
nest that deep. A page unchanged since the last run (same last-edited time, same
embedding model) is not read again; the run still follows the sub-pages and
database rows stored for it.

### A page that cannot be read

A page ends a run in one of three states. A page that is trashed or answers 404
is gone, and it and what hung beneath it leave the index. A page that fails for
any other lasting reason (403 included, or a request the embedder or Notion
rejects) keeps what is stored for it and for everything beneath it, and the run
goes on: one bad page never stops the run. Only a rate limit, a timeout or a
server error is retried.

If the **granted page itself** is gone, nothing is deleted on that evidence alone.
The scope is recorded as unhealthy, the agent's creator is told in a direct
message once (the tenant's admins when the creator cannot be messaged), and its
index is deleted when three nightly runs in a row find the page gone. A run that
finds the page again clears the record. To delete it sooner, run
`uv run python -m agentos.index_knowledge --confirm-gone` (add `--tenant` for one
tenant); it changes nothing for a scope whose page is back. A page stored while a
run is going (by a page event) is never deleted by that run.

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

With the Notion webhook set up (see above), a signed page event (content or
properties updated, created, moved, deleted or restored) refreshes that single
page within about 30 seconds of the last edit: bursts of edits are coalesced
into one refresh. An edited page is re-embedded, a trashed page or one moved out
of every granted page is removed, and a new page under a granted page is
indexed. Sub-pages added under a new page wait for the nightly run, which
remains the backstop for anything the webhook misses.

### Local embeddings with Ollama

To embed on your own machine instead of a hosted endpoint:

1. Install the Ollama app. The Homebrew build may lack the runner `bge-m3` needs.
2. Run `ollama pull bge-m3`.
3. Export `AGENTOS_EMBED_BASE_URL=http://localhost:11434/v1` and
   `AGENTOS_EMBED_MODEL=bge-m3`, then restart the worker. No key is needed, and
   the Qwen variables are not used for embeddings.
4. Changing the embedding model re-embeds every page on the next run, because
   the content hash includes the model name. Until a page is re-embedded its
   old chunks are never searched: each page records the model that embedded it,
   and a search compares the question only with chunks of the model in use.

The agent's `search_knowledge` tool searches only its own granted pages. Chunks
farther from the question than the model's cut-off are dropped, and one page
gives at most two passages. Every page found is re-checked with Notion before
the model sees it, at three calls per page however deep it sits: the page itself
(not trashed, still under the parent it was indexed under), that parent, and the
granted page. A page moved or deleted since the last run is left out. A page that
moved together with its whole parent is caught by the nightly run. Passages reach the model wrapped in `<source>` tags as data, numbered
like `[1]`. When the answer cites a number, the runtime adds a Sources line
with the page's title and link; the model never writes a link.

## Agents in the database

Each agent is stored in Postgres as numbered versions (`agents` and
`agent_versions`). A version is a `draft` while it is edited, `pending` once it
is submitted, `published` when an approver publishes it, and `superseded` when
a later version is published. At most one version of an agent is published, and
the worker loads that one on every turn, so a draft or a pending version changes
nothing. A published version never changes; an edit starts a new draft. A
database trigger enforces this for every role: the definition changes only
while a version is a draft, and the only moves are draft to pending, pending to
draft (withdrawn or rejected), pending to published and published to
superseded. Every publish records the approver. Publishing a version lower
than the published one is refused; to roll back, make a new draft from the old
version, then submit and publish it.

A definition has the shape of one agent block in the YAML file and is parsed by
the same code, so it is validated the same way. The front-door rules (a
specialist exists, is not a front door itself, is listed once) hold for a
tenant's published set: a publish or import that would break them is refused.

Grants are read from the published version when a tool runs, not when the
worker starts. A grant removed by a new published version applies to the next
call, including a write that was approved before. A turn's model calls and its
audience check, on the other hand, use the version the turn started with, so a
publish in the middle of a turn does not change its prompt.

`max_writes_per_turn` in a definition (a whole number from 1 to 10, default 2) is how
many writes one model reply may start. Each write is an approval card in the
requester's DMs. The agent's other write calls in that reply, and a write repeated
in it, get an error result that tells the model so. A definition that leaves it out
keeps the default.

```yaml
max_steps: 6
max_writes_per_turn: 2   # optional
```

A thread handed to a specialist is remembered in the `threads` table (migration
`025`), so it keeps its specialist when the conversation workflow closes after a week
of silence and starts again. The same migration adds `owner_pings`: a refusal tells the
asker to ask the agent's owner and mentions them, which pages them, so it does so once
a day for each person in each place and names the owner without a mention after that.

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

A tenant has two roles. An **admin** approves every publish (so a wider grant
reaches users only when an admin publishes it), rejects pending versions,
manages members and reads the audit log. A **creator** creates agents and
drafts, edits, submits and withdraws the versions of agents they created; an
admin can do the same for any agent.

The first admins come from the tenant YAML file:

```yaml
admins: [U0123ABCD]   # Slack user IDs
```

An admin listed there is always an admin, and nobody can remove or demote them
through the app. Other admins and the creators are added by an admin and stored
in `members`; the app role can delete rows there, and the audit log keeps the
record. **A tenant with no `admins` can publish nothing**, because only an admin
can publish. An admin may approve their own submission, so a tenant with one
admin works; the audit event records both IDs. The last admin cannot remove or
demote themself.

`agentos.control.ControlPlane` checks the role for every change and writes one
audit event for it (`agent_create`, `agent_draft` for a new draft, `agent_edit` for
replacing a draft's definition, `agent_submit`, `agent_withdraw`, `agent_reject`,
`agent_publish`, `member_add`, `member_remove`, `member_role`). The one-time YAML
import writes an `agent_import` event for each agent it inserts, in the same
transaction (a rerun that skips an agent writes none). Events hold IDs and a hash of the definition, never the
definition. A refused action is not audited yet. Run `uv run python scripts/migrate.py` on an existing database.

## Audit log

Every tenant has an append-only record of what its agents did: who asked, which
agent and version, whose access was used (`agent:<agent id>`), which tool, what
was read or changed, and who approved. One row is written for each of: a read
tool call, a model answer, an approval, a write to Notion (also a failed write),
and a hand-off to a specialist. A row holds IDs and SHA-256 hashes of the
canonical JSON of inputs and outputs, never chat text, document text or tool
input or output. Each row has an event key, so a retried activity records once.

The app role can only INSERT and SELECT on `audit_events`, and a trigger
rejects UPDATE, DELETE and TRUNCATE for every role. If the database cannot be
written, the activity fails and is retried, so nothing is done without a
record. Retention (one year) is not enforced yet: rows are kept until someone
removes them outside the app.

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

Edit it in the console with the rest of the definition; a change is a new version that an
admin publishes. A definition without `audience` keeps working: every full member by direct
message, the channels the tenant file maps to the agent, no guests. People from other Slack
workspaces (Slack Connect) and deactivated users are never let in.

A channel that the tenant file's `channels:` maps to an agent must be in that agent's
audience. Publishing, and starting the worker, refuse it otherwise.

**What a refused user sees.** One reply in the thread: "<agent name> isn't available to you
here. Ask <creator> for access." Nothing is read or run for them, and an `audience_refused`
event is audited. If Slack can't be asked, the user sees "I couldn't check whether <agent
name> is available to you just now. Try again in a few minutes." and the turn is refused.

**Publishing.** The version page must be current: Publish approves exactly the definition
the page showed (the form carries its hash), and if the creator has withdrawn, edited and
resubmitted the version since you opened it, the publish is refused and you reload and
review it again. The version page shows the audience and, live from Slack, how many people
can reach the agent (total, guests, from other workspaces). Publishing needs the box "I
confirm these people may see what this agent can access"; the API's publish takes
`{"confirm_audience": {"total": 12, "guests": 1, "external": 0}}` and answers 409 when it is
missing or the numbers have changed.

**In the Slack app** (api.slack.com/apps), before the first run with audiences:

1. OAuth & Permissions → Scopes → **Bot Token Scopes**: add `channels:read`, `groups:read`
   and `usergroups:read`. `users:read` is already there.
2. **Reinstall the app** to the workspace (adding a scope does not take effect until you do),
   then export the new bot token to the worker and to the console process.

The console process now also reads each tenant's Slack bot token (the variable
`secrets.slack_bot_token_env` names, `SLACK_BOT_TOKEN` by default) to count the people; a
tenant without it can't publish until it is set.

### Audience changes are watched

An answer posted in a channel is read by everyone in it, including guests and people from
other Slack workspaces who cannot ask the agent themselves. So when one of these happens in
a channel that a published agent answers in, the agent is **paused in that channel**:

- the channel is shared with another organisation (Slack Connect).

> **Untested live.** This pause is covered by the automated tests only. It has not been
> run against a real Slack Connect channel, which needs a paid Slack plan on both sides.

Anyone joining a channel, guests and people from other workspaces included, changes nothing:
who is added to a channel is the admins' responsibility. A guest or outside person who
asks the agent is still refused as described under audiences above.

**What a paused channel shows.** Every message there gets one fixed reply, "<agent name> is
paused in this channel until its owner reviews who can see it.", and nothing else runs: no
model call, no tool. Direct messages and other channels are unaffected. A thread that was
already running in the channel is paused too, from its next message. The agent's creator and
the tenant's admins each get a direct message once per pause, naming the agent, the channel
and the reason, never any content.

**Pending approval cards.** A pause does not leave the agent's cards live. Just before an approved write runs, the task checks again that the channel is not paused and that the requester is still in the audience of the agent's current published version. If either fails, nothing is written and the card says "<agent name> can no longer act for you here; nothing was changed." The task ends; the requester can ask again once the agent is available to them.

**Resuming.** The agent page and the version page list the paused channels with the reason,
the time and how many people are in each channel now (total, guests, from other workspaces).
The agent's creator or an admin ticks "I confirm these people may see what this agent says
here" and presses **Resume**; the counts must still match a fresh read, as at publish.
Pausing (`audience_pause`) and resuming (`audience_resume`) are audited, and a resumed pause
stays in the `audience_pauses` table as history. The same trigger while a channel is already
paused changes nothing.

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

## Web console

The same process serves web pages for agents, versions, members, usage and the audit log,
behind Sign in with Slack. The pages call the same `ControlPlane` methods as the
JSON API, so a user's role in the tenant decides what they can do in both. The
console is off unless sign-in is configured.

A creator sees and opens only the agents they created: the agent list, an agent's
versions and definitions, and its usage, budget and switch state. An agent that belongs to
someone else is not listed, and opening it answers "not found", the same as for an agent that
does not exist. An admin sees every agent. The JSON API follows the same rule. Before sign-in,
an unknown tenant and a real one get the same answer.

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
sees each grant's live title and parent, a warning when the creator's label differs, and a
diff against the published version, and must tick a box to publish a version that widens
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

How it behaves:

- The Slack user's workspace must be a tenant's `slack_team_id`, and the user must
  have a role there (an admin in the tenant file, or a `members` row). A user with
  no role sees a "no access" page and gets no session.
- A session is a row in Postgres under the tenant's row-level security. The browser
  holds a random 256-bit ID in an `HttpOnly`, `SameSite=Lax` cookie (`Secure` on
  https) that expires after 8 hours; only the ID's SHA-256 is stored. **Sign out**
  deletes the row.
- Every form carries a per-session CSRF token and must come from the console's own
  origin. Pages are served with a `Content-Security-Policy` that allows no script and
  no outside asset.
- A page never answers 502 or 504, because a proxy such as Cloudflare replaces those
  with its own error page. When Notion cannot be reached, "Check Notion grants" and a
  refused publish show the version page with a message and status 503. The JSON API
  keeps 502.
- The JSON API accepts the same session cookie. A session is valid only for its own
  tenant's paths.

## Restart demo: exactly once, and follow-up that survives a crash

This is the end-to-end proof that the work outlives the process.

1. With the worker and gateway running, DM the bot: "My laptop battery is
   swelling, please file a request."
2. An approval card arrives in your DM. Click **Submit**.
3. Straight away, kill the worker (Ctrl-C in terminal 1). To see the intake survive
   a Temporal outage too, DM the bot while the worker is down: Slack gets its answer
   from the gateway, and the reply arrives once the worker is back.
4. Restart it: `uv run python -m agentos.worker`.
5. Check Notion: there is **exactly one** new item in the database, with a
   hidden "Agent request ID" ending in `:create`. The thread shows one "Filed"
   message.
6. Set the item's Status to **Done** in Notion. Within 15 minutes (sooner if
   the Notion webhook is set up) the bot posts in the original thread that the
   request was marked Done.

The Temporal UI shows the `…:task:…` workflow resuming where it stopped.

## Layout

| Path | What it is |
|---|---|
| `src/agentos/gateway/app.py` | HTTP ingress: Slack events, Slack button clicks, Notion webhook |
| `src/agentos/gateway/relay.py` | The loop that signals conversation workflows with the events the gateway stored |
| `src/agentos/content_store.py` | Where message text, model replies, tool inputs and task drafts live while Temporal carries references |
| `src/agentos/worker.py` | Temporal worker running both workflows and all activities |
| `src/agentos/workflows/` | `ConversationWorkflow` (turn loop) and `TaskWorkflow` (approval, create, follow-up) |
| `src/agentos/activities.py` | All I/O the workflows do |
| `src/agentos/connectors/` | Connector manifest and the Notion connector, with grant checks |
| `src/agentos/knowledge/` | Embedder, knowledge store, Notion text extraction and chunking, indexing and search helpers |
| `src/agentos/control_api.py` | The control-plane API, a local-only process |
| `src/agentos/grant_checker.py` | Checks a version's Notion database grants |
| `src/agentos/index_knowledge.py` | Command that starts indexing, or schedules it nightly |
| `src/agentos/retention.py` | Nightly purge of expired rows per tenant, and the command that schedules it |
| `src/agentos/ids.py` | The only place workflow IDs are built |
| `src/agentos/agent_store.py` | Agents in the database: versions, publish lifecycle, one-time YAML import |
| `src/agentos/tenant_store.py` | Tenants in the database: one-time YAML import, the narrow cross-tenant reads, and the cached live view every process serves from |
| `src/agentos/usage.py` | Usage rows, model prices, pricing at record time and the usage report |
| `src/agentos/budgets.py` | Monthly budgets, the stop before a model call, and the warning levels |
| `src/agentos/switches.py` | Kill switches: the table, the checks, and the operator command |
| `src/agentos/unit_of_work.py` | One transaction and the tenant lock for a change and its audit event |
| `src/agentos/stores.py` | Chat history, idempotency records and task links (Postgres, in-memory) |
| `src/agentos/threads.py` | The agent a thread was handed to, and the day an agent's owner was last mentioned in a refusal |
| `scripts/init_db.sql` | Database schema, applied by Compose on first start |
| `scripts/migrate.py` | Applies the migrations a database lacks and records them in `schema_migrations` |
| `src/agentos/schema_version.py` | The start-up check that the database has every migration the code expects |
