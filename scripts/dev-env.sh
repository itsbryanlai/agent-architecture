#!/usr/bin/env bash
#
# Ask for each live-run variable in turn, then start the stack (or open a shell) with them.
# Run it with bash or by path; do not source it.
#
#   scripts/dev-env.sh [dev-up options]   ask, then run scripts/dev-up.sh with the values
#   scripts/dev-env.sh --shell            ask, then open an interactive shell with the values
#   scripts/dev-env.sh --reset            ask for every variable, even one already set
#
# A variable already set in this shell is kept when you press Enter. Secrets are read with
# no echo. Nothing is written to a file, printed, or put on a command line: the values are
# exported and the next program inherits them.

set -euo pipefail

# shellcheck source=scripts/dev-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/dev-lib.sh"
DEV_REENTER="Paste it again"

SHELL_MODE=0
FORCE=0
ARGS=()
for arg in "$@"; do
  case "$arg" in
    --shell) SHELL_MODE=1 ;;
    --reset) FORCE=1 ;;
    -h | --help) sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) ARGS+=("$arg") ;;
  esac
done
if ((SHELL_MODE)) && ((${#ARGS[@]} > 0)); then die "--shell takes no dev-up options."; fi

# where NAME: kind (secret|plain) and where to find the value.
where() {
  case "$1" in
    AGENTOS_OWNER_DB_PASSWORD) echo "secret|choose one: letters and digits only (Docker creates the owner role with it)" ;;
    AGENTOS_APP_DB_PASSWORD) echo "secret|choose one: letters and digits only (the app role the services use)" ;;
    SLACK_BOT_TOKEN) echo "secret|Slack app > OAuth & Permissions > Bot User OAuth Token (xoxb-...)" ;;
    SLACK_SIGNING_SECRET) echo "secret|Slack app > Basic Information > Signing Secret" ;;
    SLACK_CLIENT_ID) echo "plain|Slack app > Basic Information > Client ID" ;;
    SLACK_CLIENT_SECRET) echo "secret|Slack app > Basic Information > Client Secret" ;;
    NOTION_TOKEN) echo "secret|Notion > Settings > Connections > your integration > Internal Integration Secret" ;;
    QWEN_API_KEY) echo "secret|your Qwen provider's API key page" ;;
    QWEN_BASE_URL) echo "plain|the provider's OpenAI-compatible base URL" ;;
    ANTHROPIC_API_KEY) echo "secret|console.anthropic.com > API keys" ;;
    AGENTOS_MODEL) echo "plain|a model name; Enter skips and uses the default" ;;
  esac
}

# read_line KIND: read one line into LINE; hidden on a terminal for a secret. Stdin that is
# not a terminal (the tests) is read as plain lines.
read_line() {
  LINE=""
  if [[ -t 0 && "$1" == secret ]]; then
    IFS= read -rs LINE || true
    printf '\n'
  else
    IFS= read -r LINE || true
  fi
}

# clean: trim surrounding whitespace and strip one pair of surrounding quotes, into LINE.
clean() {
  local v="$LINE"
  v="${v#"${v%%[![:space:]]*}"}"
  v="${v%"${v##*[![:space:]]}"}"
  if ((${#v} >= 2)); then
    case "$v" in
      \"*\") v="${v:1:${#v}-2}" ;;
      \'*\') v="${v:1:${#v}-2}" ;;
    esac
  fi
  LINE="$v"
}

# prompt_var NAME REQUIRED: ask for NAME until it passes the rules (3 tries), then export it.
prompt_var() {
  local name="$1" required="$2" meta kind hint tries=0 had=0 old="" problems
  meta="$(where "$name")"
  kind="${meta%%|*}"
  hint="${meta#*|}"
  if [[ -n "${!name:-}" ]]; then had=1; old="${!name}"; fi
  say
  if ((had && !FORCE)); then
    say "$name already set, keeping it (Enter keeps, or paste a new value). $hint"
  else
    say "$name: $hint"
  fi
  while ((tries < 3)); do
    printf '%s> ' "$name"
    read_line "$kind"
    clean
    if [[ -z "$LINE" ]]; then
      if ((had && !FORCE)); then
        printf -v "$name" '%s' "$old"
      elif ((!required)); then
        say "skipped"
        return 0
      else
        bad "$name needs a value."
        tries=$((tries + 1))
        continue
      fi
    else
      printf -v "$name" '%s' "$LINE"
    fi
    problems="$(value_problems "$name" || true)"
    if [[ "${!name}" =~ [[:space:]] && "$kind" == plain ]]; then problems="${problems}${problems:+$'\n'}$name contains a space or a newline. $DEV_REENTER without one."; fi
    if [[ -z "$problems" ]]; then
      export "${name?}"
      if [[ "$kind" == secret ]]; then local n="${!name}"; say "set (${#n} characters)"; fi
      return 0
    fi
    printf '%s\n' "$problems" | while IFS= read -r l; do bad "$l"; done
    if ((had)); then printf -v "$name" '%s' "$old"; else unset "$name"; fi
    tries=$((tries + 1))
  done
  die "$name: three tries failed. Nothing was started."
}

prompt_provider() {
  local def tries=0
  def="$(printf '%s' "${AGENTOS_LLM_PROVIDER:-qwen}" | tr '[:upper:]' '[:lower:]')"
  say
  say "AGENTOS_LLM_PROVIDER: the chat model provider, qwen or anthropic"
  while ((tries < 3)); do
    printf 'AGENTOS_LLM_PROVIDER [%s]> ' "$def"
    read_line plain
    clean
    [[ -n "$LINE" ]] || LINE="$def"
    LINE="$(printf '%s' "$LINE" | tr '[:upper:]' '[:lower:]')"
    if provider_vars "$LINE" >/dev/null; then
      AGENTOS_LLM_PROVIDER="$LINE"
      export AGENTOS_LLM_PROVIDER
      return 0
    fi
    bad "AGENTOS_LLM_PROVIDER must be anthropic or qwen."
    tries=$((tries + 1))
  done
  die "AGENTOS_LLM_PROVIDER: three tries failed. Nothing was started."
}

say "${BOLD}Live-run variables${RESET}: you are asked for each one. Secrets are not echoed."
for v in $(required_vars); do prompt_var "$v" 1; done
prompt_provider
for v in $(provider_vars "$AGENTOS_LLM_PROVIDER"); do prompt_var "$v" 1; done
prompt_var AGENTOS_MODEL 0
say

if ((SHELL_MODE)); then
  say "a shell with the variables; type exit to leave it"
  exec "${SHELL:-/bin/bash}" -i
fi
exec "$DEV_ROOT/scripts/dev-up.sh" ${ARGS[@]+"${ARGS[@]}"}
