#!/usr/bin/env bash
#
# One line per component of the local stack: Docker services, migrations, the three
# servers, and each tunnel with its URL and /healthz. Then the Slack settings to compare.
# It starts and stops nothing.
#
#   scripts/dev-status.sh           print once
#   scripts/dev-status.sh --watch   check both tunnels every 30 s and shout when one dies

set -uo pipefail

# shellcheck source=scripts/dev-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/dev-lib.sh"
cd "$DEV_ROOT"

WATCH=0
for arg in "$@"; do
  case "$arg" in
    --watch) WATCH=1 ;;
    -h | --help) sed -n '2,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option: $arg" ;;
  esac
done

say "${BOLD}Repository in use:${RESET} $DEV_ROOT"
RC=0

# Docker services.
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  running="$(docker compose ps --status running --services 2>/dev/null | tr '\n' ' ')"
  for s in postgres temporal; do
    if [[ " $running" == *" $s "* ]]; then ok "docker: $s running"; else bad "docker: $s not running"; RC=1; fi
  done
else
  bad "docker: not running or not installed"; RC=1
fi

# Migrations: read-only, with the database URL of this shell.
if [[ -x "$DEV_PY" && -n "${AGENTOS_DATABASE_URL:-}" ]]; then
  mkdir -p "$DEV_DIR"
  if "$DEV_PY" -c '
import os, sys
import psycopg
from agentos.schema_version import EXPECTED_VERSIONS
with psycopg.connect(os.environ["AGENTOS_DATABASE_URL"], connect_timeout=3) as c:
    have = {r[0] for r in c.execute("select version from schema_migrations")}
missing = sorted(set(EXPECTED_VERSIONS) - have)
if missing:
    print("missing: " + ", ".join(missing)); sys.exit(1)
' >"$DEV_DIR/.status-migrations" 2>&1; then
    ok "migrations: current"
  else
    bad "migrations: $(tail -n 1 "$DEV_DIR/.status-migrations" 2>/dev/null | cut -c1-120)"; RC=1
  fi
  rm -f "$DEV_DIR/.status-migrations"
else
  warn "migrations: not checked (AGENTOS_DATABASE_URL is unset in this shell)"
fi

# Processes.
for n in worker gateway console; do
  if alive "$n"; then ok "$n: running (pid $(cat "$(pid_file "$n")"))"; else bad "$n: not running (see $DEV_LOGS/$n.log)"; RC=1; fi
done

# Tunnels.
GW_URL="$(saved_url gateway)"
CON_URL="$(saved_url console)"
for n in gateway console; do
  url="$(saved_url "$n")"
  if [[ -z "$url" ]]; then bad "tunnel $n: no URL recorded"; RC=1; continue; fi
  if ! alive "tunnel-$n"; then bad "tunnel $n: process dead ($url is stale)"; RC=1; continue; fi
  if healthy "$url"; then ok "tunnel $n: $url /healthz ok"; else bad "tunnel $n: $url does not answer /healthz (a dead quick tunnel logs 'Tunnel not found')"; RC=1; fi
done

say
if [[ -n "$GW_URL" && -n "$CON_URL" ]]; then
  print_slack_settings "$GW_URL" "$CON_URL"
  if [[ -z "${AGENTOS_CONSOLE_URL:-}" ]]; then
    say "AGENTOS_CONSOLE_URL in this shell: unset (dev-up sets it for the console process)"
  elif [[ "${AGENTOS_CONSOLE_URL%/}" == "$CON_URL" ]]; then
    say "AGENTOS_CONSOLE_URL in this shell: matches the console tunnel"
  else
    warn "AGENTOS_CONSOLE_URL in this shell does not match the console tunnel"
  fi
fi

if ((WATCH)); then say; watch_tunnels 0; fi
exit "$RC"
