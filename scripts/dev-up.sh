#!/usr/bin/env bash
#
# Start the whole local stack from one shell: Docker services, migrations, two quick
# tunnels, the worker, the gateway and the console. Run it from any directory; it always
# works in the repository it lives in.
#
#   scripts/dev-up.sh            start, print the Slack settings, return
#   scripts/dev-up.sh --watch    start, then stay and restart a tunnel that dies
#   scripts/dev-up.sh --index    also run one knowledge index (needed once after migration 021)
#
# Export the variables ONCE in this shell first (docs/local-run.md). This script reads no
# secret value into a file, prints none, and puts none on a command line: the processes
# inherit this shell's environment.

set -euo pipefail

# shellcheck source=scripts/dev-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/dev-lib.sh"
cd "$DEV_ROOT"

WATCH=0
INDEX=0
for arg in "$@"; do
  case "$arg" in
    --watch) WATCH=1 ;;
    --index) INDEX=1 ;;
    -h | --help) sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option: $arg" ;;
  esac
done

say "${BOLD}Repository in use:${RESET} $DEV_ROOT"
say "Branch: $(git -C "$DEV_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
say "This script runs the code in the path above, not in the directory you started it from."
say

# --- preflight: names and shapes only; no value is printed -------------------

FAILED=0
fail() { bad "$*"; FAILED=1; }

need() {
  if [[ -z "${!1:-}" ]]; then fail "$1 is not set. Export it once in this shell (see docs/local-run.md)."; fi
}

# is_doubled VAR: the value is the same text twice in a row (a paste made twice).
is_doubled() {
  local v="${!1}" n half
  n=${#v}
  half=$((n / 2))
  ((n >= 8 && n % 2 == 0)) && [[ "${v:0:half}" == "${v:half}" ]]
}

need AGENTOS_OWNER_DB_PASSWORD
need AGENTOS_APP_DB_PASSWORD
need SLACK_BOT_TOKEN
need SLACK_SIGNING_SECRET
need NOTION_TOKEN
need SLACK_CLIENT_ID
need SLACK_CLIENT_SECRET

PROVIDER="$(printf '%s' "${AGENTOS_LLM_PROVIDER:-anthropic}" | tr '[:upper:]' '[:lower:]')"
case "$PROVIDER" in
  qwen) need QWEN_API_KEY; need QWEN_BASE_URL ;;
  anthropic) need ANTHROPIC_API_KEY ;;
  *) fail "AGENTOS_LLM_PROVIDER must be anthropic or qwen." ;;
esac

# URL-special characters would break the database URL (the driver reads the rest as the host).
SPECIAL='[@:/#%?& ]'
for v in AGENTOS_APP_DB_PASSWORD AGENTOS_OWNER_DB_PASSWORD; do
  if [[ -n "${!v:-}" && "${!v}" == *$SPECIAL* ]]; then
    fail "$v contains a character that breaks a database URL (one of @ : / # % ? & or a space). Choose a password without them."
  fi
done

# A pasted-twice secret: the Slack bot token must start with xoxb- and contain it once.
if [[ -n "${SLACK_BOT_TOKEN:-}" ]]; then
  if [[ "$SLACK_BOT_TOKEN" != xoxb-* ]]; then
    fail "SLACK_BOT_TOKEN must start with xoxb- (the Bot User OAuth Token, not the user token)."
  else
    stripped="${SLACK_BOT_TOKEN//xoxb-/}"
    if (((${#SLACK_BOT_TOKEN} - ${#stripped}) != 5)); then
      fail "SLACK_BOT_TOKEN contains xoxb- more than once: it was pasted twice. Export it again, once."
    fi
  fi
fi
for v in SLACK_BOT_TOKEN SLACK_SIGNING_SECRET NOTION_TOKEN SLACK_CLIENT_SECRET QWEN_API_KEY ANTHROPIC_API_KEY \
  AGENTOS_APP_DB_PASSWORD AGENTOS_OWNER_DB_PASSWORD; do
  [[ -n "${!v:-}" ]] || continue
  if is_doubled "$v"; then fail "$v is the same text twice in a row: it was probably pasted twice. Export it again, once."; fi
  if [[ "${!v}" =~ [[:space:]] ]]; then fail "$v contains a space or a newline. Export it again without one."; fi
done

# The database URLs: build them from the passwords, and refuse one that disagrees.
if [[ -n "${AGENTOS_APP_DB_PASSWORD:-}" && -n "${AGENTOS_OWNER_DB_PASSWORD:-}" ]]; then
  want_app="postgresql://agentos_app:${AGENTOS_APP_DB_PASSWORD}@${DEV_DB_HOST}/agentos"
  want_owner="postgresql://agentos:${AGENTOS_OWNER_DB_PASSWORD}@${DEV_DB_HOST}/agentos"
  if [[ -z "${AGENTOS_DATABASE_URL:-}" ]]; then
    AGENTOS_DATABASE_URL="$want_app"
    say "AGENTOS_DATABASE_URL was unset: built it for agentos_app at ${DEV_DB_HOST}."
  elif [[ "$AGENTOS_DATABASE_URL" != "$want_app" ]]; then
    fail "AGENTOS_DATABASE_URL is set and does not match AGENTOS_APP_DB_PASSWORD (an old one from an earlier terminal?). Run 'unset AGENTOS_DATABASE_URL' and start again."
  fi
  if [[ -z "${AGENTOS_OWNER_DATABASE_URL:-}" ]]; then
    AGENTOS_OWNER_DATABASE_URL="$want_owner"
    say "AGENTOS_OWNER_DATABASE_URL was unset: built it for the owner role at ${DEV_DB_HOST}."
  elif [[ "$AGENTOS_OWNER_DATABASE_URL" != "$want_owner" ]]; then
    fail "AGENTOS_OWNER_DATABASE_URL is set and does not match AGENTOS_OWNER_DB_PASSWORD. Run 'unset AGENTOS_OWNER_DATABASE_URL' and start again."
  fi
  export AGENTOS_DATABASE_URL AGENTOS_OWNER_DATABASE_URL
fi

# Embeddings: default to the local Ollama bge-m3 when nothing is set; an explicit setting stays.
USES_OLLAMA=0
if [[ -z "${AGENTOS_EMBED_BASE_URL:-}" && -z "${AGENTOS_EMBED_MODEL:-}" ]]; then
  export AGENTOS_EMBED_BASE_URL="http://localhost:11434/v1"
  export AGENTOS_EMBED_MODEL="bge-m3"
  say "Embedding settings were unset: using the local Ollama bge-m3 (AGENTOS_EMBED_BASE_URL=http://localhost:11434/v1)."
elif [[ -z "${AGENTOS_EMBED_BASE_URL:-}" || -z "${AGENTOS_EMBED_MODEL:-}" ]]; then
  fail "Set both AGENTOS_EMBED_BASE_URL and AGENTOS_EMBED_MODEL, or neither (the worker would fall back to a model with no price)."
fi
if [[ "${AGENTOS_EMBED_BASE_URL:-}" == *localhost:11434* || "${AGENTOS_EMBED_BASE_URL:-}" == *127.0.0.1:11434* ]]; then USES_OLLAMA=1; fi

# Tools and the repository.
for tool in docker uv cloudflared curl lsof; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool is not installed or not on PATH."
done
if command -v docker >/dev/null 2>&1 && ! docker info >/dev/null 2>&1; then fail "Docker is not running."; fi
if [[ ! -x "$DEV_PY" ]]; then fail "$DEV_PY is missing. Run 'uv sync' in $DEV_ROOT first."; fi
if [[ "$USES_OLLAMA" == 1 ]]; then
  if ! curl -fsS -m 3 "http://127.0.0.1:11434/api/tags" 2>/dev/null | grep -q 'bge-m3'; then
    fail "Ollama is not answering on 127.0.0.1:11434 or has no bge-m3 model. Open the Ollama app and run 'ollama pull bge-m3'."
  fi
fi

# The stack must not already be running from here, and the ports must be free.
for n in worker gateway console tunnel-gateway tunnel-console; do
  if alive "$n"; then fail "$n is already running (pid in $DEV_PIDS). Run scripts/dev-down.sh first."; fi
done
for p in "$DEV_GATEWAY_PORT" "$DEV_CONSOLE_PORT"; do
  if port_busy "$p"; then fail "port $p is already in use by another process. dev-up only manages what it started; stop that process yourself."; fi
done

if ((FAILED)); then
  say
  die "Preflight failed. Nothing was started."
fi
ok "preflight passed"

# --- start -------------------------------------------------------------------

dev_prepare_dirs

say; say "${BOLD}Docker services${RESET}"
docker compose up -d

say; say "${BOLD}Migrations${RESET}"
MIGRATE_LOG="$DEV_LOGS/migrate.log"
if ! "$DEV_PY" scripts/migrate.py >"$MIGRATE_LOG" 2>&1; then
  cat "$MIGRATE_LOG"
  say "If the message above says to baseline: this database was built before the migration script."
  say "Record what it has, then start again:  .venv/bin/python scripts/migrate.py --baseline <last migration number it has>"
  die "migrations failed. Nothing else was started."
fi
cat "$MIGRATE_LOG"
NEED_INDEX=0
if grep -q 'applying 021_' "$MIGRATE_LOG"; then NEED_INDEX=1; fi

say; say "${BOLD}Tunnels${RESET}"
GW_URL="$(start_tunnel gateway "$DEV_GATEWAY_PORT")" || die "the gateway tunnel gave no URL; see $DEV_LOGS/tunnel-gateway.log"
CON_URL="$(start_tunnel console "$DEV_CONSOLE_PORT")" || die "the console tunnel gave no URL; see $DEV_LOGS/tunnel-console.log"
ok "gateway tunnel $GW_URL"
ok "console tunnel $CON_URL"

if [[ -z "${AGENTOS_CONSOLE_URL:-}" ]]; then
  export AGENTOS_CONSOLE_URL="$CON_URL"
  : >"$DEV_DIR/console_url_auto"
  say "AGENTOS_CONSOLE_URL was unset: set it to the console tunnel for the console process."
else
  rm -f "$DEV_DIR/console_url_auto"
  if [[ "${AGENTOS_CONSOLE_URL%/}" != "$CON_URL" ]]; then
    warn "AGENTOS_CONSOLE_URL is set and is not this console tunnel's URL. Slack sign-in will fail unless it points at the tunnel; unset it to let dev-up use $CON_URL."
  fi
fi

say; say "${BOLD}Processes${RESET}"
start_worker
start_gateway
start_console
PROBLEM=0
for n in worker gateway console; do settle "$n" || PROBLEM=1; done

if ((INDEX)); then
  say
  say "Running one knowledge index (--index)."
  "$DEV_PY" -m agentos.index_knowledge --wait || warn "the index run failed; see its output above."
elif ((NEED_INDEX)); then
  say
  warn "Migration 021 was just applied. Run the knowledge index once, or searches skip every page:"
  say "  scripts/dev-up.sh --index   (after dev-down), or: .venv/bin/python -m agentos.index_knowledge --wait"
fi

# --- report ------------------------------------------------------------------

say; say "${BOLD}Addresses${RESET}"
say "  gateway  $GW_URL"
say "  console  $CON_URL"
say "  Temporal http://localhost:8233"
say
say "Checking /healthz through each tunnel (up to ${DEV_HEALTH_TIMEOUT}s each, while the name propagates)..."
if wait_healthy "$GW_URL"; then ok "gateway $GW_URL/healthz"; else bad "gateway /healthz through the tunnel did not answer; see $DEV_LOGS/gateway.log and $DEV_LOGS/tunnel-gateway.log"; PROBLEM=1; fi
if wait_healthy "$CON_URL"; then ok "console $CON_URL/healthz"; else bad "console /healthz through the tunnel did not answer; see $DEV_LOGS/console.log and $DEV_LOGS/tunnel-console.log"; PROBLEM=1; fi
say
print_slack_settings "$GW_URL" "$CON_URL"
say
print_tail_commands
say
say "Check status: scripts/dev-status.sh    Stop: scripts/dev-down.sh"
say "A private Slack channel needs the bot invited (/invite @<bot>), or it answers channel_not_found."

if ((PROBLEM)); then warn "something above failed; the rest of the stack is still running."; fi
if ((WATCH)); then
  say
  watch_tunnels 1
fi
exit "$PROBLEM"
