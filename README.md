# agentos: walking skeleton

This repo is a platform for Slack agents that *do work*: they answer from the
sources their creator granted, take actions only after the requester approves,
and follow up for days until the work is done. The design is in
[`docs/product.md`](docs/product.md), [`docs/architecture.md`](docs/architecture.md)
and [`docs/decisions.md`](docs/decisions.md).

The code here is a walking skeleton of that design, built on Temporal
(Python SDK), Postgres, Slack and Notion. It covers one path end to end:

1. A Slack DM (or an @mention in a channel) reaches the gateway, which
   signal-with-starts a `ConversationWorkflow` for that thread.
2. Claude answers using Notion read tools (`search_items`, `my_items`), or
   asks to create an item.
3. Creating an item starts a `TaskWorkflow`, which DMs the requester an
   approval card. Nothing is filed until they click **Submit**.
4. Submit creates the Notion item exactly once, even if the worker crashes
   and retries.
5. The task watches the item's status and posts in the original thread when it
   reaches a "done" value.

Claude can also draft a comment on an existing item (`add_comment`). The same
approval card is sent, and the comment is posted only after **Submit**, exactly
once, and only on items in a database the agent was granted. Every approval
card has an **Edit** button: it opens a form prefilled with the draft, and
saving replaces the draft and asks for a fresh approval (the old buttons stop
working). A request can be edited up to 5 times.

One deployment can serve several tenants (see [Several tenants](#several-tenants)).
A single tenant is set in `config/agents.yaml`. Every workflow ID, database
row and log line carries the tenant ID.

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
into `config/agents.yaml`.

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

After the wizard, with `NOTION_TOKEN` exported, check the Notion grant and get
the stable property IDs:

```bash
uv run python -m agentos.check_grants
```

The property names in `config/agents.yaml` are labels; the worker addresses
Notion properties only by ID, so a rename in Notion doesn't break it. The
command reads the database's schema (it never changes it), reports any
misnamed, mistyped or missing property, and prints a `property_ids` block. Paste
that block into the grant in `config/agents.yaml`. The worker checks the same
things at startup and refuses to start if anything is wrong or an ID is
missing. The command exits 1 on any problem.

Optional settings: `AGENTOS_MODEL` (default depends on the provider; see
below),
`AGENTOS_TEMPORAL_ADDRESS` (default `localhost:7233`), `AGENTOS_DATABASE_URL`
(default `postgresql://agentos_app:agentos_app@localhost:55432/agentos`, the
local development app role, on the host port 55432 that Compose publishes) and `AGENTOS_CONFIG`
(default `config/agents.yaml`; a file for one tenant, or a directory with one
`*.yaml` per tenant).

If you exported `DATABASE_URL` or `TEMPORAL_ADDRESS` for this project before,
export the `AGENTOS_` names instead: the old ones are now ignored.

Knowledge settings, used only when an agent has a Notion page grant:

| Variable | Default | Meaning |
|---|---|---|
| `AGENTOS_EMBED_MODEL` | `text-embedding-v4` | Embedding model served by whichever embedding endpoint is configured. Output is 1024 dimensions, fixed to match the database column |
| `AGENTOS_EMBED_BASE_URL` | none | An OpenAI-compatible embeddings endpoint of its own, such as a local Ollama. When unset, the Qwen settings are used |
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
   you set `AGENTOS_NOTION_TOKEN_DIR`.
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
one tenant, in the same format as `config/agents.yaml`. The gateway and the
worker serve all of them side by side:

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

## Encrypted workflow data

Every payload the platform writes to Temporal (workflow inputs and results,
signals, queries, activity inputs and results) is encrypted with the key of the
tenant it belongs to (AES-256-GCM). The tenant comes from the workflow ID, which
always starts with the tenant ID; a payload with no tenant is never sent.

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

Workflows that started before encryption was turned on still read: payloads
without the encrypted tag pass through unchanged. New payloads are always
encrypted.

The Temporal web UI shows these payloads as encrypted until a codec server is
added (a later change). Every process that connects to Temporal (worker,
gateway, `index_knowledge`) goes through `agentos.temporal_client.build_temporal_client`,
so each needs the keys it will use.

| Variable | Default | Meaning |
|---|---|---|
| `AGENTOS_KEY_DIR` | `~/.agentos/keys` | Directory of per-tenant key files, `<tenant_id>.key` |

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

## Upgrading an existing local database

Chat history appends are now idempotent by a per-step key. A database created
before this change needs one migration, `scripts/migrations/001_history_step_key.sql`:

```bash
docker compose exec -T postgres psql -U agentos -d agentos < scripts/migrations/001_history_step_key.sql
```

This is needed only for a database created before this change; a fresh
`docker compose up` gets the column from `scripts/init_db.sql`.

Tool outputs and knowledge need two more migrations, and Compose now uses a
Postgres 16 image with pgvector. Recreate the container on the new image (the
data volume carries over), then apply `002` and `003` the same way as `001`:

```bash
docker compose up -d postgres
docker compose exec -T postgres psql -U agentos -d agentos < scripts/migrations/002_tool_outputs.sql
docker compose exec -T postgres psql -U agentos -d agentos < scripts/migrations/003_knowledge.sql
```

Tenant separation is now also enforced by the database with row-level security.
Apply `004` as the owner role, `agentos`, the same way:

```bash
docker compose exec -T postgres psql -U agentos -d agentos < scripts/migrations/004_row_level_security.sql
```

It creates the `agentos_app` role, and the app now connects as `agentos_app`
by default. The table owner and any superuser bypass row-level security, so the
app must not connect as `agentos`; use `agentos` only to run migrations. If you
set `AGENTOS_DATABASE_URL` yourself, point it at `agentos_app`.

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

## Knowledge: indexing and search

An agent with a Notion page grant (`grants.notion.pages`) can answer from every
page under that page. Index the pages first, with the worker running (the
worker does the reading and embedding; the command only asks Temporal to start
it):

```bash
uv run python -m agentos.index_knowledge             # one run per granted page, now
uv run python -m agentos.index_knowledge --wait      # ... and print a summary per run
uv run python -m agentos.index_knowledge --schedule  # nightly runs at 03:00 UTC
```

Indexing calls the embeddings endpoint, so it **spends embedding credit**;
pages whose content has not changed since the last run are not embedded again.
Two agents granted the same page share one run. `--schedule` creates or updates
one schedule per granted page and deletes the schedules of pages that are no
longer granted. Pages removed from the tree, trashed or unreachable are removed
from the index on the next run.

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
   the content hash includes the model name.

The agent's `search_knowledge` tool searches only its own granted pages. Every
page found is re-checked with Notion before the model sees it (not trashed, still
under the granted page), so a page moved or deleted since the last run is
left out. Passages reach the model wrapped in `<source>` tags as data, numbered
like `[1]`. When the answer cites a number, the runtime adds a Sources line
with the page's title and link; the model never writes a link.

## Restart demo: exactly once, and follow-up that survives a crash

This is the end-to-end proof that the work outlives the process.

1. With the worker and gateway running, DM the bot: "My laptop battery is
   swelling, please file a request."
2. An approval card arrives in your DM. Click **Submit**.
3. Straight away, kill the worker (Ctrl-C in terminal 1).
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
| `src/agentos/worker.py` | Temporal worker running both workflows and all activities |
| `src/agentos/workflows/` | `ConversationWorkflow` (turn loop) and `TaskWorkflow` (approval, create, follow-up) |
| `src/agentos/activities.py` | All I/O the workflows do |
| `src/agentos/connectors/` | Connector manifest and the Notion connector, with grant checks |
| `src/agentos/knowledge/` | Embedder, knowledge store, Notion text extraction and chunking, indexing and search helpers |
| `src/agentos/index_knowledge.py` | Command that starts indexing, or schedules it nightly |
| `src/agentos/ids.py` | The only place workflow IDs are built |
| `src/agentos/stores.py` | Chat history, idempotency records and task links (Postgres, in-memory) |
| `scripts/init_db.sql` | Database schema, applied by Compose on first start |
