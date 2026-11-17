# agentos

A platform for Slack agents that *do work*: they answer from the sources their creator granted, take actions only after the requester approves, and follow up for days until the work is done. It is built on Temporal (Python SDK), Postgres, Slack and Notion.

The product is in [`docs/product.md`](docs/product.md), the design in [`docs/architecture.md`](docs/architecture.md), the reasons in [`docs/decisions.md`](docs/decisions.md). What is built and what is not is in [`docs/status.md`](docs/status.md). Setup, configuration and every command are in [`docs/operations.md`](docs/operations.md), and a map of the code is in [`docs/modules.md`](docs/modules.md).

## What it does

1. A Slack DM, or an @mention in a channel, reaches the gateway, which stores the event and answers Slack at once. A relay then starts or signals a `ConversationWorkflow` for that thread.
2. The agent answers with its Notion read tools and indexed knowledge (with citations), or asks to create an item or comment on one.
3. A write starts a `TaskWorkflow`, which DMs the requester an approval card. Nothing is written until they click **Submit**; **Edit** asks again.
4. Submit writes to Notion exactly once, even if the worker crashes and retries.
5. The task watches the item and posts in the original thread when its status reaches "done".

One deployment serves several tenants. Every workflow ID, database row and log line carries the tenant ID. See [`docs/architecture.md`](docs/architecture.md) for how each step works.

## Quickstart

You need [uv](https://docs.astral.sh/uv/), Docker with Docker Compose, a tunnel tool (`cloudflared` or `ngrok`), a Slack workspace and a Notion workspace where you can create apps and integrations, and a model key.

```bash
uv sync
scripts/setup-wizard.sh      # Postgres, Temporal, tunnel, Slack app, Notion integration, tenant import
```

The wizard never asks for or stores a secret. At the end it prints the `export` lines to run yourself (`SLACK_BOT_TOKEN`, `SLACK_SIGNING_SECRET`, `NOTION_TOKEN`, `ANTHROPIC_API_KEY`); secrets live only in your shell, never in a file. The database passwords, migrations, the tenant and agent import, per-tenant keys and every other setting are in [`docs/operations.md`](docs/operations.md).

Then run the worker and the gateway:

```bash
docker compose up -d                                   # Postgres + Temporal
uv run python -m agentos.worker                        # terminal 1
uv run uvicorn agentos.gateway.app:app --port 8000     # terminal 2
```

To run everything (Docker, migrations, tunnels, worker, gateway, console) with one command, see [`docs/local-run.md`](docs/local-run.md).

Keep your tunnel pointed at port 8000. The Temporal web UI is at <http://localhost:8233>. DM the bot, or @mention it in a channel it has been invited to.

## Restart demo: exactly once, and follow-up that survives a crash

This is the end-to-end proof that the work outlives the process.

1. With the worker and gateway running, DM the bot: "My laptop battery is swelling, please file a request."
2. An approval card arrives in your DM. Click **Submit**.
3. Straight away, kill the worker (Ctrl-C in terminal 1). To see the intake survive a Temporal outage too, DM the bot while the worker is down: Slack gets its answer from the gateway, and the reply arrives once the worker is back.
4. Restart it: `uv run python -m agentos.worker`.
5. Check Notion: there is **exactly one** new item in the database, with a hidden "Agent request ID" ending in `:create`. The thread shows one "Filed" message.
6. Set the item's Status to **Done** in Notion. Within 15 minutes (sooner if the Notion webhook is set up) the bot posts in the original thread that the request was marked Done.

The Temporal UI shows the `…:task:…` workflow resuming where it stopped. Temporal keeps its data in the `temporal-data` volume, so pending approvals also survive restarting its container.

## Tests

The tests use fakes only (a scripted LLM, a fake Slack, a fake Notion and in-memory stores) plus Temporal's time-skipping test server, which downloads a test-server binary on first use. They never call Claude, Slack or Notion. The `pg` tier runs the SQL against a throwaway database on the Compose Postgres. Commands are in [`docs/operations.md`](docs/operations.md#tests).
