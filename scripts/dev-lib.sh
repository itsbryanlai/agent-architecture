#!/usr/bin/env bash
# Shared helpers for dev-up.sh, dev-down.sh and dev-status.sh. Source it; do not run it.
#
# Nothing here reads a secret into a file, prints one, or puts one on a command line:
# the processes get the environment of the shell that ran dev-up, by inheritance only.

DEV_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
DEV_DIR="$DEV_ROOT/.dev"
DEV_LOGS="$DEV_DIR/logs"
DEV_PIDS="$DEV_DIR/pids"
DEV_URLS="$DEV_DIR/urls"
DEV_PY="$DEV_ROOT/.venv/bin/python"
DEV_GATEWAY_PORT="${DEV_GATEWAY_PORT:-8000}"
DEV_CONSOLE_PORT="${DEV_CONSOLE_PORT:-8100}"
DEV_HEALTH_TIMEOUT="${DEV_HEALTH_TIMEOUT:-60}"
DEV_WATCH_INTERVAL="${DEV_WATCH_INTERVAL:-30}"
DEV_SETTLE_SECONDS="${DEV_SETTLE_SECONDS:-6}"
DEV_DB_HOST="127.0.0.1:55432"

if [[ -t 1 ]] && command -v tput >/dev/null 2>&1 && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
  BOLD=$(tput bold); RED=$(tput setaf 1); GREEN=$(tput setaf 2); YELLOW=$(tput setaf 3); RESET=$(tput sgr0)
else
  BOLD=""; RED=""; GREEN=""; YELLOW=""; RESET=""
fi

say()  { printf '%s\n' "$*"; }
ok()   { printf '%s[ok]%s   %s\n' "$GREEN" "$RESET" "$*"; }
warn() { printf '%s[warn]%s %s\n' "$YELLOW" "$RESET" "$*"; }
bad()  { printf '%s[FAIL]%s %s\n' "$RED" "$RESET" "$*"; }
die()  { bad "$*"; exit 1; }

dev_prepare_dirs() {
  mkdir -p "$DEV_LOGS" "$DEV_PIDS" "$DEV_URLS"
  # The folder ignores itself, so no tracked ignore file has to know about it.
  [[ -f "$DEV_DIR/.gitignore" ]] || printf '*\n' >"$DEV_DIR/.gitignore"
}

# --- processes ---------------------------------------------------------------

pid_file() { printf '%s/%s.pid' "$DEV_PIDS" "$1"; }

# alive NAME: the process recorded for NAME is running.
alive() {
  local f pid
  f="$(pid_file "$1")"
  [[ -s "$f" ]] || return 1
  pid="$(cat "$f")"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

# start_bg NAME COMMAND...: run detached with the inherited environment; log and PID kept.
start_bg() {
  local name="$1"; shift
  : >"$DEV_LOGS/$name.log"
  nohup "$@" >>"$DEV_LOGS/$name.log" 2>&1 </dev/null &
  printf '%s' "$!" >"$(pid_file "$name")"
}

# stop_proc NAME: stop the process whose PID dev-up recorded, and its children. Only that PID.
stop_proc() {
  local name="$1" f pid i
  f="$(pid_file "$name")"
  [[ -s "$f" ]] || return 0
  pid="$(cat "$f")"
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
    for i in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
      pkill -KILL -P "$pid" 2>/dev/null || true
      kill -KILL "$pid" 2>/dev/null || true
    fi
    ok "stopped $name"
  else
    say "$name was not running"
  fi
  rm -f "$f"
}

port_busy() { [[ -n "$(lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null)" ]]; }

# --- tunnels -----------------------------------------------------------------

url_file() { printf '%s/%s' "$DEV_URLS" "$1"; }
saved_url() { [[ -s "$(url_file "$1")" ]] && cat "$(url_file "$1")" || true; }

# tunnel_url_from_log FILE: the https://*.trycloudflare.com address cloudflared printed.
# api.trycloudflare.com is cloudflared's own control endpoint, not a tunnel.
tunnel_url_from_log() {
  grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' "$1" 2>/dev/null | grep -v '^https://api\.' | head -n 1 || true
}

# start_tunnel NAME PORT: a quick tunnel to 127.0.0.1:PORT (not localhost: that can dial ::1).
# Prints the URL, or fails after the timeout.
start_tunnel() {
  local name="$1" port="$2" waited=0 url=""
  rm -f "$(url_file "$name")"
  start_bg "tunnel-$name" cloudflared tunnel --no-autoupdate --url "http://127.0.0.1:$port"
  while ((waited < DEV_HEALTH_TIMEOUT)); do
    url="$(tunnel_url_from_log "$DEV_LOGS/tunnel-$name.log")"
    [[ -n "$url" ]] && break
    alive "tunnel-$name" || return 1
    sleep 1; waited=$((waited + 1))
  done
  [[ -n "$url" ]] || return 1
  printf '%s' "$url" >"$(url_file "$name")"
  printf '%s' "$url"
}

# healthy URL: GET URL/healthz answers {"ok":true}.
healthy() {
  local body
  body="$(curl -fsS -m 5 "$1/healthz" 2>/dev/null)" || return 1
  [[ "$body" == *'"ok":true'* || "$body" == *'"ok": true'* ]]
}

# wait_healthy URL: retry while the tunnel name propagates.
wait_healthy() {
  local waited=0
  while ((waited < DEV_HEALTH_TIMEOUT)); do
    healthy "$1" && return 0
    sleep 2; waited=$((waited + 2))
  done
  return 1
}

print_slack_settings() {
  local gw="$1" console="$2"
  say "${BOLD}Slack app settings to paste (api.slack.com/apps):${RESET}"
  say "  Event Subscriptions > Request URL        $gw/slack/events"
  say "  Interactivity & Shortcuts > Request URL  $gw/slack/interactivity"
  say "  OAuth & Permissions > Redirect URLs      $console/auth/slack/callback"
  say "  (a changed gateway URL also means recreating the Notion webhook subscription)"
}

print_tail_commands() {
  local n
  say "${BOLD}Logs:${RESET}"
  for n in worker gateway console tunnel-gateway tunnel-console; do
    say "  tail -f $DEV_LOGS/$n.log"
  done
}

# --- the three servers -------------------------------------------------------

start_worker()  { start_bg worker "$DEV_PY" -m agentos.worker; }
start_gateway() {
  start_bg gateway "$DEV_PY" -m uvicorn agentos.gateway.app:app --host 127.0.0.1 --port "$DEV_GATEWAY_PORT"
}
start_console() {
  start_bg console "$DEV_PY" -m agentos.control_api --host 127.0.0.1 --port "$DEV_CONSOLE_PORT"
}

# settle NAME: give a fresh process a few seconds to refuse to start, and show why if it did.
settle() {
  local name="$1" i
  for ((i = 0; i < DEV_SETTLE_SECONDS; i++)); do
    alive "$name" || break
    sleep 1
  done
  if alive "$name"; then
    ok "$name is running (pid $(cat "$(pid_file "$name")"))"
    return 0
  fi
  bad "$name exited at start. Last lines of $DEV_LOGS/$name.log:"
  tail -n 15 "$DEV_LOGS/$name.log" 2>/dev/null | sed 's/^/        /'
  return 1
}

# --- watching ----------------------------------------------------------------

# check_tunnel NAME: 0 when the tunnel process runs and its URL answers /healthz.
check_tunnel() {
  local url
  alive "tunnel-$1" || return 1
  url="$(saved_url "$1")"
  [[ -n "$url" ]] && healthy "$url"
}

loud() {
  local line
  line="================================================================"
  printf '\n%s%s\n  %s\n%s%s\n\n' "$RED$BOLD" "$line" "$*" "$line" "$RESET"
}

# watch_tunnels RESTART: check both tunnels every DEV_WATCH_INTERVAL seconds, forever.
# RESTART=1 (dev-up --watch) restarts a dead tunnel and prints the new Slack settings;
# RESTART=0 (dev-status --watch) only reports. Set DEV_WATCH_ROUNDS to stop after N rounds.
watch_tunnels() {
  local restart="$1" rounds=0 name port new gw console
  say "Watching both tunnels every ${DEV_WATCH_INTERVAL}s. Ctrl-C stops the watch only; the stack keeps running."
  while true; do
    sleep "$DEV_WATCH_INTERVAL"
    for name in gateway console; do
      check_tunnel "$name" && continue
      sleep "${DEV_WATCH_RECHECK:-5}"
      check_tunnel "$name" && continue
      loud "THE $(printf '%s' "$name" | tr '[:lower:]' '[:upper:]') TUNNEL IS DOWN (its URL no longer answers). Its address will change on restart."
      [[ "$restart" == 1 ]] || { say "Run dev-up.sh --watch, or dev-down.sh then dev-up.sh, to get a new one."; continue; }
      if [[ "$name" == gateway ]]; then port="$DEV_GATEWAY_PORT"; else port="$DEV_CONSOLE_PORT"; fi
      stop_proc "tunnel-$name"
      if new="$(start_tunnel "$name" "$port")"; then
        if [[ "$name" == console && -f "$DEV_DIR/console_url_auto" ]]; then
          export AGENTOS_CONSOLE_URL="$new"
          stop_proc console
          start_console
          settle console || true
        fi
        gw="$(saved_url gateway)"; console="$(saved_url console)"
        loud "NEW ${name} URL: $new. Update Slack now."
        print_slack_settings "$gw" "$console"
      else
        bad "could not restart the $name tunnel; see $DEV_LOGS/tunnel-$name.log"
      fi
    done
    rounds=$((rounds + 1))
    if [[ -n "${DEV_WATCH_ROUNDS:-}" && "$rounds" -ge "$DEV_WATCH_ROUNDS" ]]; then return 0; fi
  done
}
