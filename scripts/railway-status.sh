#!/usr/bin/env bash
#
# One line per check of the hosted stack on Railway: each of the six services'
# latest deployment, the gateway's and console's /healthz, the schema version
# inside the worker, then the Slack app settings to compare. It reads no secret
# and starts and changes nothing.
#
#   scripts/railway-status.sh                          URLs from the Railway domains
#   scripts/railway-status.sh GATEWAY_URL CONSOLE_URL  URLs given (custom domains)
#
# Needs the Railway CLI, logged in, run from a directory linked to the project.
# RAILWAY_SSH_KEY (optional): the path to the SSH key file `railway ssh` should use
# (-i), when it is not the default key. A path, not a secret value.
# Exit: 0 every check passed, 1 a check failed, 2 the CLI is missing or not logged in.
#
# Every Railway command and flag below is verified against https://docs.railway.com/cli:
#   whoami           https://docs.railway.com/cli/whoami
#   status           https://docs.railway.com/cli/status
#   deployment list  https://docs.railway.com/cli/deployment (--service, --limit;
#                    plain output is "ID | STATUS | timestamp", newest first)
#   domain list      https://docs.railway.com/cli/domain (--service; a generated
#                    domain is <name>.up.railway.app)
#   ssh              https://docs.railway.com/cli/ssh (--service; "-- COMMAND" runs
#                    the command inside the service container, no PTY when piped)
# `railway run` is deliberately not used: it runs the command on this laptop with
# the service's variables injected (https://docs.railway.com/cli/run), and the
# worker's AGENTOS_DATABASE_URL names postgres.railway.internal, which resolves
# only inside Railway's private network. `railway variable list --kv` is never
# used either: it prints secret values (https://docs.railway.com/cli/variable).

set -uo pipefail

# shellcheck source=scripts/dev-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/dev-lib.sh"

RC=0

case "${1:-}" in
  -h | --help) sed -n '2,14p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
esac
GW_URL=""
CON_URL=""
if (($# == 2)); then
  GW_URL="${1%/}"
  CON_URL="${2%/}"
elif (($# != 0)); then
  die "usage: scripts/railway-status.sh [GATEWAY_URL CONSOLE_URL]"
fi

# The CLI must be installed, logged in, and in a linked directory. Each is one line.
if ! command -v railway >/dev/null 2>&1; then
  bad "railway: CLI not installed (https://docs.railway.com/cli)"
  exit 2
fi
if ! railway whoami >/dev/null 2>&1 </dev/null; then
  bad "railway: not logged in; run 'railway login'"
  exit 2
fi
if ! railway status >/dev/null 2>&1 </dev/null; then
  bad "railway: this directory is not linked to a project; run 'railway link'"
  exit 2
fi

# Each service's latest deployment.
for s in postgres temporal ollama worker gateway console; do
  if out="$(railway deployment list --service "$s" --limit 1 2>&1 </dev/null)"; then
    state="$(printf '%s\n' "$out" | grep ' | ' | head -n 1 | awk -F'|' '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')"
    if [[ "$state" == SUCCESS ]]; then
      ok "deploy $s: SUCCESS"
    elif [[ -n "$state" ]]; then
      bad "deploy $s: $state"; RC=1
    else
      bad "deploy $s: no deployments"; RC=1
    fi
  else
    bad "deploy $s: $(printf '%s\n' "$out" | head -n 1 | cut -c1-120)"; RC=1
  fi
done

# The public URLs: arguments first, else the service's generated Railway domain.
domain_url() { # domain_url SERVICE: the generated domain, or nothing
  railway domain list --service "$1" 2>/dev/null </dev/null \
    | grep -Eo '[a-z0-9-]+\.up\.railway\.app' | head -n 1
}
if [[ -z "$GW_URL" ]]; then
  d="$(domain_url gateway)"
  [[ -n "$d" ]] && GW_URL="https://$d"
fi
if [[ -z "$CON_URL" ]]; then
  d="$(domain_url console)"
  [[ -n "$d" ]] && CON_URL="https://$d"
fi

for name in gateway console; do
  if [[ "$name" == gateway ]]; then url="$GW_URL"; else url="$CON_URL"; fi
  if [[ -z "$url" ]]; then
    bad "$name: no URL (no Railway domain found; pass it as an argument)"; RC=1
  elif healthy "$url"; then
    ok "$name: $url/healthz ok"
  else
    bad "$name: $url does not answer /healthz"; RC=1
  fi
done

# The schema check runs inside the worker, where the database URL resolves.
SSH_KEY_ARGS=()
if [[ -n "${RAILWAY_SSH_KEY:-}" ]]; then
  SSH_KEY_ARGS=(-i "$RAILWAY_SSH_KEY")
fi
if out="$(railway ssh ${SSH_KEY_ARGS[@]+"${SSH_KEY_ARGS[@]}"} --service worker -- python -m agentos.schema_version 2>&1 </dev/null)"; then
  ok "schema: $(printf '%s\n' "$out" | tail -n 1)"
else
  bad "schema: $(printf '%s\n' "$out" | tail -n 1 | cut -c1-120)"; RC=1
fi

say
if [[ -n "$GW_URL" && -n "$CON_URL" ]]; then
  print_slack_settings "$GW_URL" "$CON_URL"
fi
exit "$RC"
