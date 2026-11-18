# Run it locally

How to run the whole stack on one machine with one command, and what to do when it breaks. Every other setting is in [operations.md](operations.md); what the system does is in [architecture.md](architecture.md).

Contents: [What runs](#what-runs) · [One-time setup](#one-time-setup) · [The variables](#the-variables-a-prompt-or-paste-once) · [Which process needs what](#which-process-needs-what) · [Start, check, stop](#start-check-stop) · [The manual way](#the-manual-way-several-terminals) · [Troubleshooting](#troubleshooting)

## What runs

| Part | How | Port |
|---|---|---|
| Postgres (pgvector) and Temporal | `docker compose up -d` | 55432, 7233, 8233 |
| The worker | `python -m agentos.worker` | none |
| The gateway (Slack and Notion events) | `uvicorn agentos.gateway.app:app` | 8000 |
| The console (web pages, Sign in with Slack) | `python -m agentos.control_api` | 8100 |
| Two Cloudflare quick tunnels | `cloudflared tunnel --url http://127.0.0.1:<port>` | to 8000 and 8100 |

Slack reaches the gateway through the first tunnel. Your browser and Slack sign-in reach the console through the second. A quick tunnel's address changes every time it restarts.

## One-time setup

1. **Tools.** [uv](https://docs.astral.sh/uv/), Docker with Compose, `cloudflared`, and `curl` and `lsof` (both ship with macOS). Run `uv sync` once in the checkout.
2. **Ollama, for embeddings.** Install the Ollama **app** (the Homebrew build may lack the runner `bge-m3` needs), open it, and run `ollama pull bge-m3`. Skip this only if you point `AGENTOS_EMBED_BASE_URL` at another endpoint.
3. **The Slack app.** Create it from `slack/app-manifest.yaml` (api.slack.com/apps, From a manifest). Then:
   - Install it to the workspace and copy the **Bot User OAuth Token** (`xoxb-...`).
   - Basic Information: copy the **Signing Secret**, **Client ID** and **Client Secret**.
   - OAuth & Permissions, User Token Scopes: add `openid` and `profile` for Sign in with Slack. The scopes for agent audiences and for the history lookups that keep posts from landing twice are in [operations.md, Agent audiences](operations.md#agent-audiences) and [Slack tunnel and live checks](operations.md#slack-tunnel-and-live-checks). Reinstall the app after adding a scope.
   - Three addresses change on every start and are pasted from the `dev-up` output: Event Subscriptions Request URL, Interactivity Request URL, and OAuth Redirect URL.
4. **The Notion integration.** Create an internal integration, enable **Read content**, **Read comments** and **Insert comments**, copy its token, and share the pages and database it should use with it (the integration's Connections menu on each page). The wizard (`scripts/setup-wizard.sh`) walks through this and writes the non-secret IDs into `config/agents.yaml`.
5. **The tenant, agents and key**, once per machine, with the exports below set and Docker up:

   ```bash
   docker compose up -d
   uv run python scripts/migrate.py
   uv run python -m agentos.tenant_store import
   uv run python -m agentos.agent_store import
   uv run python -m agentos.tenant_keys create <tenant_id>
   ```

   The worker, the gateway and the console refuse to start without a tenant, a published default agent and a key. See [operations.md](operations.md#tenants-in-the-database).
6. **Invite the bot to each channel it should answer in**, private ones included (`/invite @<bot>`). A private channel the bot is not in fails with `channel_not_found`.

## The variables: a prompt, or paste once

The easiest way is to let a script ask for each value in turn:

```bash
scripts/dev-env.sh            # asks for each value, then runs scripts/dev-up.sh (it accepts dev-up's options, e.g. --watch)
scripts/dev-env.sh --shell    # asks, then opens a shell with the same values (for dev-status, dev-down, CLI commands)
```

Run it, do not `source` it. Each prompt says where to find the value; secrets are not echoed and the script prints only `set (<n> characters)`. A value is checked as you enter it (no URL-special character in a database password, `xoxb-` once in the bot token, no text pasted twice, no whitespace) and asked again on a mistake, three tries at most. A variable already set in your shell is kept when you press Enter; `--reset` asks for every one. Nothing is written to a file or put on a command line: the values are exported and the next program inherits them.

The alternative is to export them yourself, once, in the one shell you will run the scripts from. Fill the placeholders by hand; never write them to a file, and paste each line once.

```bash
# Database. Letters and digits only: @ : / # % ? & or a space would break the database URL.
export AGENTOS_OWNER_DB_PASSWORD='<owner password>'
export AGENTOS_APP_DB_PASSWORD='<app password>'

# Slack and Notion
export SLACK_BOT_TOKEN='<xoxb-... once>'
export SLACK_SIGNING_SECRET='<signing secret>'
export SLACK_CLIENT_ID='<client id>'
export SLACK_CLIENT_SECRET='<client secret>'
export NOTION_TOKEN='<notion integration token>'

# The chat model: Qwen (shown) or leave AGENTOS_LLM_PROVIDER unset and export ANTHROPIC_API_KEY
export AGENTOS_LLM_PROVIDER=qwen
export QWEN_API_KEY='<key>'
export QWEN_BASE_URL='<OpenAI-compatible base URL>'
# export AGENTOS_MODEL=<model name, if not the default>

# Optional. dev-up supplies these when they are unset:
# export AGENTOS_EMBED_BASE_URL=http://localhost:11434/v1   # local Ollama
# export AGENTOS_EMBED_MODEL=bge-m3
# export AGENTOS_CONSOLE_URL=<the console tunnel's https URL>
# export AGENTOS_DATABASE_URL=...        # leave unset: dev-up builds it from the app password
# export AGENTOS_OWNER_DATABASE_URL=...  # leave unset: dev-up builds it from the owner password
```

`dev-up` builds `AGENTOS_DATABASE_URL` (as `agentos_app`) and `AGENTOS_OWNER_DATABASE_URL` (as `agentos`) from the two passwords. If you export either yourself and it disagrees with its password, it refuses and tells you to `unset` it.

## Which process needs what

Read from `src/agentos/config.py`, `worker.py`, `gateway/app.py`, `control_api.py` and `tenants.py`. A tenant can rename its secret variables in its config (`secrets:`); the table uses the defaults.

| Variable | Worker | Gateway | Console | Migrations and Compose |
|---|---|---|---|---|
| `AGENTOS_DATABASE_URL` (as `agentos_app`) | yes | yes | yes | no |
| `AGENTOS_OWNER_DB_PASSWORD` | no | no | no | Compose |
| `AGENTOS_APP_DB_PASSWORD` | no | no | no | Compose and `migrate.py` |
| `AGENTOS_OWNER_DATABASE_URL` | no | no | no | `migrate.py` |
| `SLACK_BOT_TOKEN` | yes | yes | yes | no |
| `SLACK_SIGNING_SECRET` | no | yes | no | no |
| `NOTION_TOKEN` | yes | no | yes | no |
| `SLACK_CLIENT_ID`, `SLACK_CLIENT_SECRET` | no | no | yes | no |
| `AGENTOS_CONSOLE_URL` (https, the console tunnel) | no | no | yes | no |
| `ANTHROPIC_API_KEY`, or `AGENTOS_LLM_PROVIDER=qwen` with `QWEN_API_KEY` and `QWEN_BASE_URL` | yes | no | no | no |
| `AGENTOS_MODEL` | yes | no | no | no |
| `AGENTOS_EMBED_BASE_URL`, `AGENTOS_EMBED_MODEL`, `EMBED_API_KEY` | yes | no | no | no |
| `AGENTOS_KEY_DIR` (default `~/.agentos/keys`) | yes | yes | no | no |
| `NOTION_OAUTH_CLIENT_ID`, `NOTION_OAUTH_CLIENT_SECRET`, `AGENTOS_REQUIRE_GRANT_PICKER` (optional) | no | no | yes | no |
| `NOTION_WEBHOOK_VERIFICATION_TOKEN` (optional; see [Notion webhook](operations.md#notion-webhook-optional)) | no | yes | no | no |

The scripts do not check the optional rows. `docker compose` needs both passwords for every command, even `exec`.

## Start, check, stop

```bash
scripts/dev-env.sh           # ask for each variable, then run dev-up.sh (takes dev-up's options)
scripts/dev-env.sh --shell   # ask for each variable, then open a shell that has them
scripts/dev-up.sh            # preflight, Docker, migrations, tunnels, worker, gateway, console
scripts/dev-up.sh --watch    # the same, then stay and restart a tunnel that dies
scripts/dev-status.sh        # one line per component, and the Slack addresses
scripts/dev-status.sh --watch  # check both tunnels every 30 s and shout when one dies
scripts/dev-down.sh          # stop what dev-up started; Docker keeps running
scripts/dev-down.sh --all    # and stop Docker too
```

Run them from any directory. Each one prints the repository it uses and works in that checkout, not in your current directory.

`dev-up` checks, before starting anything, without printing a value: every required variable is set; the app and owner passwords have no URL-special character; the Slack bot token starts with `xoxb-` and holds it once; no secret is the same text twice in a row; a database URL you exported matches its password; the embedding settings are both set (or defaults to Ollama `bge-m3`, and says so); Docker, `uv`, `cloudflared` and, for local embeddings, Ollama with `bge-m3` answer; no earlier `dev-up` is still running; ports 8000 and 8100 are free. One failure stops it with nothing started.

Then it starts the Docker services, applies the migrations still missing, opens both tunnels on `127.0.0.1`, sets `AGENTOS_CONSOLE_URL` from the console tunnel (unless you set one), and starts the worker, gateway and console with this shell's environment. It ends by printing the two URLs, the `/healthz` result through each tunnel (retried for up to 60 seconds while the name propagates), the three Slack settings to paste, and a `tail -f` command for each log.

State lives in `.dev/` at the repository root: `logs/<name>.log`, `pids/<name>.pid`, `urls/`. The folder holds a `.gitignore` of `*`, so git ignores it without any tracked file changing. Secrets are never written there or put on a command line: the processes inherit your shell's environment.

After a migration that needs a knowledge index (021; see [operations.md](operations.md#upgrading-an-existing-index)), `dev-up` says so; run `scripts/dev-up.sh --index` once on the next start, or `.venv/bin/python -m agentos.index_knowledge --wait` with the stack up. Indexing calls the embeddings endpoint.

When a quick tunnel dies and restarts, its address changes. `--watch` shows the new addresses in a loud banner; paste them into Slack again. A new gateway address also means recreating the Notion webhook subscription.

## The manual way: several terminals

What `dev-up` does, one piece at a time, with the exports above in every terminal. Use it to debug one part. Start with `cd` into the checkout you mean.

```bash
cd /path/to/agent-architecture
export AGENTOS_DATABASE_URL="postgresql://agentos_app:${AGENTOS_APP_DB_PASSWORD}@127.0.0.1:55432/agentos"
export AGENTOS_OWNER_DATABASE_URL="postgresql://agentos:${AGENTOS_OWNER_DB_PASSWORD}@127.0.0.1:55432/agentos"
export AGENTOS_EMBED_BASE_URL=http://localhost:11434/v1 AGENTOS_EMBED_MODEL=bge-m3

docker compose up -d
uv run python scripts/migrate.py

# Terminal A and B: tunnels, in their own terminals (never in a server's terminal)
cloudflared tunnel --url http://127.0.0.1:8000     # gateway; note the https address
cloudflared tunnel --url http://127.0.0.1:8100     # console; note the https address
export AGENTOS_CONSOLE_URL=https://<console tunnel address>

uv run python -m agentos.worker                                  # terminal C
uv run uvicorn agentos.gateway.app:app --port 8000               # terminal D
uv run python -m agentos.control_api --port 8100                 # terminal E
```

Then paste the gateway and console addresses into Slack as `dev-up` prints them.

## Troubleshooting

Each entry is something that went wrong on a first live run: what you see, why, what to do. `dev-up` prevents the first six; the rest it reports or this section explains.

| # | You see | Cause | Fix |
|---|---|---|---|
| 1 | Behaviour from old code; a fixed bug is still there | The shell is inside an old worktree (for example `.claude/worktrees/<name>`) and ran that checkout's code | Use the scripts: they print the repository path they use and run only that one. By hand, `cd` into the checkout you mean and check `pwd` and `git branch --show-current` |
| 2 | `docker compose` stops with `export AGENTOS_OWNER_DB_PASSWORD` or `export AGENTOS_APP_DB_PASSWORD` | Compose needs both for every command, even `exec` and `ps` | Export both in that shell. `dev-up` checks them first |
| 3 | The app cannot connect; the error names a strange host | The app password has `@ : / # % ? &`, so the database URL splits in the wrong place | Choose a password of letters and digits. `dev-up` refuses one with those characters. An existing role keeps its password: `uv run python scripts/migrate.py --set-app-password` resets it to `AGENTOS_APP_DB_PASSWORD` |
| 4 | `password authentication failed`, in one terminal only | That terminal still has an old `AGENTOS_DATABASE_URL` from before the password changed | `unset AGENTOS_DATABASE_URL` and run `dev-up`, which builds it from the current password and refuses a URL that disagrees |
| 5 | The worker exits at start naming a model with no price | `AGENTOS_EMBED_BASE_URL` and `AGENTOS_EMBED_MODEL` were not exported in this terminal, so it fell back to `text-embedding-v4` | Export both (or neither, and `dev-up` defaults to Ollama `bge-m3`). `dev-up` refuses exactly one of the two |
| 6 | `invalid_auth` from Slack in the worker, gateway and console | `SLACK_BOT_TOKEN` was pasted twice into one `export`, so the value is doubled | Export it again, once. `dev-up` checks that it starts with `xoxb-` and contains it once, and that no secret repeats itself. A token from a different app or a revoked one also gives `invalid_auth`: reinstall the app and copy the new token |
| 7 | Slack messages get no reply; the tunnel log repeats `Unauthorized: Tunnel not found` | A quick tunnel has no uptime guarantee and drops after a laptop sleep or a network change | `dev-status.sh` shows it; `dev-up.sh --watch` restarts it. A restart changes the address: paste the three new Slack settings again. Messages sent while it was down are lost; send them again. If the console tunnel changed, `AGENTOS_CONSOLE_URL` must match it (`--watch` restarts the console with the new one; `dev-status` warns when a shell's value differs) |
| 7b | The tunnel is up but nothing arrives | `cloudflared --url http://localhost:8000` dialled IPv6 `::1`, and the server listens on `127.0.0.1` | Point the tunnel at `127.0.0.1`. The scripts do |
| 8 | Stopping a tunnel stops the gateway or the console | The tunnel was started in the server's own terminal | The scripts run each part in the background with its own log. By hand, give each tunnel its own terminal |
| 9 | `channel_not_found` in the logs when the bot posts in a private channel | A private channel is invisible to a bot that was not invited | `/invite @<bot>` in the channel. The `groups:read` and `groups:history` scopes must be on the app |
| 10 | Knowledge searches find nothing after an upgrade that applied migration 021 | Page rows have no recorded embedding model until they are read again, so searches skip them | Run the index once: `scripts/dev-up.sh --index`, or `.venv/bin/python -m agentos.index_knowledge --wait`. It reads every page and embeds none that did not change |

Other things `dev-up` stops on:

- **`the database is behind the code`** in a process log: run `uv run python scripts/migrate.py` (dev-up does this on start).
- **`--baseline` in the migration message**: the database was built before the migration script. Record what it has: `.venv/bin/python scripts/migrate.py --baseline <last migration number it has>`, then start again.
- **A process exits at start**: `dev-up` shows the last lines of its log. The usual causes are no tenant imported, no published default agent, no tenant key, and a tenant's secret variable that is unset (the message names the variable).
- **Port 8000 or 8100 in use**: another process holds it (often a server started by hand). `dev-up` never stops a process it did not start; stop that one yourself.
- **Ollama has no `bge-m3`**: open the Ollama app, run `ollama pull bge-m3`.
