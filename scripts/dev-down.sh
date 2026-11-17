#!/usr/bin/env bash
#
# Stop what scripts/dev-up.sh started: the worker, the gateway, the console and the two
# tunnels. Only processes whose PID dev-up recorded in .dev/pids are touched.
# Docker keeps running unless you pass --all.

set -euo pipefail

# shellcheck source=scripts/dev-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/dev-lib.sh"
cd "$DEV_ROOT"

ALL=0
for arg in "$@"; do
  case "$arg" in
    --all) ALL=1 ;;
    -h | --help) sed -n '2,6p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option: $arg" ;;
  esac
done

say "${BOLD}Repository in use:${RESET} $DEV_ROOT"
# Servers first, then tunnels.
for n in console gateway worker tunnel-console tunnel-gateway; do stop_proc "$n"; done
rm -f "$DEV_DIR/console_url_auto" "$(url_file gateway)" "$(url_file console)"

if ((ALL)); then
  say "Stopping Docker services (data volumes stay)."
  docker compose down
else
  say "Docker services left running (use --all to stop them)."
fi
