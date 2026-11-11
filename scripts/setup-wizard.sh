#!/usr/bin/env bash
#
# Setup wizard for the agentos walking skeleton.
#
# Walks you through the steps only a human can do: starting local services, a
# public tunnel, creating the Slack app and the Notion integration, and filling
# the non-secret values into config/agents.yaml.
#
# Secrets (Slack bot token, Slack signing secret, Notion token, Anthropic API
# key) are NEVER asked for, read, printed, stored or tested by this script, and
# it never writes a dotenv file. At the end it prints the `export` lines for you
# to fill in and run in your own shell.

set -euo pipefail

cd "$(dirname "$0")/.."

# ──────────────────────────────────────────────────────────────────────────
# Display helpers.
# ──────────────────────────────────────────────────────────────────────────

if [[ -t 1 ]] && command -v tput >/dev/null 2>&1 && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
  BOLD=$(tput bold); DIM=$(tput dim); RESET=$(tput sgr0)
  BLUE=$(tput setaf 4); GREEN=$(tput setaf 2); YELLOW=$(tput setaf 3)
else
  BOLD=""; DIM=""; RESET=""; BLUE=""; GREEN=""; YELLOW=""
fi

TOTAL_STAGES=7
_STAGE_INDEX=0
TODO=()

_clear() {
  [[ -t 1 ]] || return 0
  if command -v tput >/dev/null 2>&1; then tput clear; else printf '\033[2J\033[3J\033[H'; fi
}

banner() {
  _clear
  printf '\n%s%s  %s%s\n' "$BOLD" "$BLUE" "$1" "$RESET"
  printf '%s  %s stages%s\n\n' "$DIM" "$TOTAL_STAGES" "$RESET"
  printf '%s  You drive the browser; this wizard tells you exactly what to do.\n' "$DIM"
  printf '  It never asks for a secret. Stop any time with Ctrl-C and re-run later.%s\n' "$RESET"
  pause "Ready to start? Press Enter."
}

stage() {
  _clear
  _STAGE_INDEX=$((_STAGE_INDEX + 1))
  printf '\n%s%s▸ Stage %s/%s · %s%s\n' \
    "$BOLD" "$BLUE" "$_STAGE_INDEX" "$TOTAL_STAGES" "$1" "$RESET"
}

say()  { printf '  %s\n' "$1"; }
step() { printf '  %s•%s %s\n' "$BLUE" "$RESET" "$1"; }
note() { printf '  %s%s%s\n' "$DIM" "$1" "$RESET"; }
warn() { printf '  %s⚠ %s%s\n' "$YELLOW" "$1" "$RESET"; }
ok()   { printf '  %s✓%s %s\n' "$GREEN" "$RESET" "$1"; }

open_url() {
  local url="$1"
  printf '  %s↗ opening%s %s\n' "$GREEN" "$RESET" "$url"
  { if   command -v wslview      >/dev/null 2>&1; then wslview "$url"
    elif command -v explorer.exe >/dev/null 2>&1; then explorer.exe "$url"
    elif command -v xdg-open     >/dev/null 2>&1; then xdg-open "$url"
    elif command -v open         >/dev/null 2>&1; then open "$url"
    else warn "couldn't open a browser; visit it manually: $url"; fi
  } >/dev/null 2>&1 || warn "couldn't open a browser, so visit it manually: $url"
}

pause() {
  printf '  %s%s%s ' "$DIM" "${1:-Press Enter to continue}" "$RESET"
  read -r _ || true
}

confirm() {
  local reply=""
  printf '  %s? %s [y/N]%s ' "$YELLOW" "$1" "$RESET"
  read -r reply || true
  [[ "$reply" =~ ^[Yy] ]]
}

# ask VAR "Prompt" reads a visible, NON-SECRET value into VAR.
ask() {
  local var="$1" prompt="$2" input=""
  printf '  %s%s%s ' "$BOLD" "$prompt" "$RESET"
  read -r input || true
  printf -v "$var" '%s' "$input"
}

# ──────────────────────────────────────────────────────────────────────────
# Stages
# ──────────────────────────────────────────────────────────────────────────

banner "agentos walking skeleton: setup"

# ── 1 ─────────────────────────────────────────────────────────────────────
stage "Check the tools"
missing=0
for tool in docker uv; do
  if command -v "$tool" >/dev/null 2>&1; then ok "$tool found"; else warn "$tool not found"; missing=1; fi
done
TUNNEL=""
if command -v cloudflared >/dev/null 2>&1; then
  TUNNEL=cloudflared; ok "cloudflared found"
elif command -v ngrok >/dev/null 2>&1; then
  TUNNEL=ngrok; ok "ngrok found"
else
  warn "no tunnel tool found: install cloudflared or ngrok"; missing=1
fi
if (( missing )); then
  say "Install what's missing, then run this wizard again."
  exit 1
fi
pause

# ── 2 ─────────────────────────────────────────────────────────────────────
stage "Start Postgres and Temporal"
say "This runs: docker compose up -d"
note "Postgres on localhost:5432, Temporal on localhost:7233 (web UI on http://localhost:8233)."
if confirm "Start them now?"; then
  docker compose up -d
  ok "services started"
else
  TODO+=("Start local services: docker compose up -d")
fi
pause

# ── 3 ─────────────────────────────────────────────────────────────────────
stage "Public tunnel to the gateway (port 8000)"
say "Slack and Notion need a public HTTPS URL that reaches the gateway on port 8000."
say "Open a second terminal and leave this running there:"
if [[ "$TUNNEL" == cloudflared ]]; then
  step "cloudflared tunnel --url http://localhost:8000"
  note "It prints a URL like https://something.trycloudflare.com"
else
  step "ngrok http 8000"
  note "It prints a Forwarding URL like https://something.ngrok-free.app"
fi
PUBLIC_URL=""
while [[ ! "$PUBLIC_URL" =~ ^https://[^/[:space:]]+$ ]]; do
  ask PUBLIC_URL "Paste the public https URL (no trailing slash):"
  PUBLIC_URL="${PUBLIC_URL%/}"
done
ok "using $PUBLIC_URL"
pause

# ── 4 ─────────────────────────────────────────────────────────────────────
stage "Create and install the Slack app"
MANIFEST_OUT="$(mktemp -t agentos-slack-manifest.XXXXXX)"
sed "s#https://REPLACE_ME#${PUBLIC_URL}#g" slack/app-manifest.yaml > "$MANIFEST_OUT"
say "Your app manifest, with the tunnel URL filled in (also saved to $MANIFEST_OUT):"
printf '\n'
sed 's/^/    /' "$MANIFEST_OUT"
printf '\n'
open_url "https://api.slack.com/apps?new_app=1"
step "Choose 'From a manifest', pick your workspace, paste the manifest above, and create the app."
step "Install the app to the workspace (Install App in the left menu)."
step "Bot token: 'OAuth & Permissions' → 'Bot User OAuth Token' (starts xoxb-)."
step "Signing secret: 'Basic Information' → 'App Credentials' → 'Signing Secret'."
warn "Do not paste either value here. You will export them yourself at the end."
note "The Events request URL is verified only once the gateway is running. If Slack"
note "shows it unverified, start the gateway and click Retry under 'Event Subscriptions'."
say ""
say "Your workspace's team ID starts with T. Open Slack in a browser: the address is"
say "app.slack.com/client/T…/…, and the part starting with T is the team ID."
SLACK_TEAM_ID=""
while [[ ! "$SLACK_TEAM_ID" =~ ^T[A-Z0-9]+$ ]]; do
  ask SLACK_TEAM_ID "Slack team ID (T…):"
done
ok "team ID $SLACK_TEAM_ID"
pause

# ── 5 ─────────────────────────────────────────────────────────────────────
stage "Create the Notion integration and database"
open_url "https://www.notion.so/profile/integrations"
step "Create a new internal integration for your workspace."
step "Capabilities: read content, insert content, and read user information"
step "  including email addresses (needed to match Slack users to Notion users)."
step "The 'Internal Integration Secret' is your Notion token. Do not paste it here."
say ""
say "In Notion, create a database (e.g. 'IT requests') with exactly these properties:"
step "Name: Title"
step "Status: Status (keep a 'Done' option)"
step "Requested by: Person"
step "Agent request ID: Text (you can hide this column)"
step "Share it with the integration: database '•••' menu → Connections → add your integration."
say ""
say "The database ID is the 32-character code in the database page's URL,"
say "notion.so/…/<database id>?v=…"
NOTION_DATABASE_ID=""
while [[ ! "$NOTION_DATABASE_ID" =~ ^[0-9a-fA-F]{32}$ ]]; do
  ask NOTION_DATABASE_ID "Notion database ID:"
  NOTION_DATABASE_ID="${NOTION_DATABASE_ID//-/}"
done
ok "database ID $NOTION_DATABASE_ID"
note "Optional: under the integration's Webhooks tab, subscribe ${PUBLIC_URL}/notion/webhook"
note "to page property updates. The gateway logs the verification token to paste back."
note "Follow-up still works without it: the agent polls Notion every 15 minutes."
pause

# ── 6 ─────────────────────────────────────────────────────────────────────
stage "Write config/agents.yaml"
if [[ ! -f config/agents.yaml ]]; then
  cp config/agents.example.yaml config/agents.yaml
fi
say "This sets slack_team_id and the IT requests database ID in config/agents.yaml."
note "Neither value is a secret."
if confirm "Write them now?"; then
  tmp="$(mktemp)"
  sed -e "s#^slack_team_id: .*#slack_team_id: ${SLACK_TEAM_ID}#" \
      -e "s#^\( *- id: \)\"[0-9a-fA-F-]*\"#\1\"${NOTION_DATABASE_ID}\"#" \
      config/agents.yaml > "$tmp"
  mv "$tmp" config/agents.yaml
  ok "config/agents.yaml updated"
else
  TODO+=("Set slack_team_id and the database id in config/agents.yaml")
fi
pause

# ── 7 ─────────────────────────────────────────────────────────────────────
stage "Export your secrets (you do this, in your own shell)"
say "Copy these lines, replace each <paste here>, and run them in every terminal"
say "that runs the worker or the gateway. Do not save them to a file."
printf '\n'
cat <<'EXPORTS'
    export SLACK_BOT_TOKEN='<paste here>'
    export SLACK_SIGNING_SECRET='<paste here>'
    export NOTION_TOKEN='<paste here>'
    export ANTHROPIC_API_KEY='<paste here>'
EXPORTS
printf '\n'
note "To test with Qwen instead of Claude, also export these (ANTHROPIC_API_KEY"
note "is then not needed):"
cat <<'QWEN'
    export AGENTOS_LLM_PROVIDER=qwen
    export QWEN_BASE_URL='<your Qwen OpenAI-compatible base URL>'
    export QWEN_API_KEY='<paste here>'
QWEN
printf '\n'
say "Then, with NOTION_TOKEN exported, check the Notion grant and print its"
say "property IDs; paste the property_ids block it prints into config/agents.yaml:"
step "uv run python -m agentos.check_grants"
printf '\n'
say "Then run, each in its own terminal:"
step "uv run python -m agentos.worker"
step "uv run uvicorn agentos.gateway.app:app --port 8000"
pause "Press Enter to finish."

_clear
printf '\n%s%s  ✓ Setup steps complete%s\n' "$BOLD" "$GREEN" "$RESET"
if (( ${#TODO[@]} )); then
  printf '\n'; warn "still to do by hand:"
  for t in "${TODO[@]}"; do note "  - $t"; done
fi
printf '\n'
