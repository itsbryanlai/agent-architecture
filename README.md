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

Optional settings: `AGENTOS_MODEL` (default `claude-sonnet-5-5`),
`TEMPORAL_ADDRESS` (default `localhost:7233`), `DATABASE_URL` (default
`postgresql://agentos:agentos@localhost:5432/agentos`, matching Compose) and
`AGENTOS_CONFIG` (default `config/agents.yaml`).

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
