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

The skeleton runs one tenant, set in `config/agents.yaml`, but every workflow
ID, database row and log line already carries the tenant ID.

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
(default `postgresql://agentos:agentos@localhost:55432/agentos`, matching
Compose, which publishes Postgres on host port 55432) and `AGENTOS_CONFIG`
(default `config/agents.yaml`).

If you exported `DATABASE_URL` or `TEMPORAL_ADDRESS` for this project before,
export the `AGENTOS_` names instead: the old ones are now ignored.

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
| `src/agentos/ids.py` | The only place workflow IDs are built |
| `src/agentos/stores.py` | Chat history, idempotency records and task links (Postgres, in-memory) |
| `scripts/init_db.sql` | Database schema, applied by Compose on first start |
