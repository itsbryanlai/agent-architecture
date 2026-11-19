#!/usr/bin/env bash
#
# Runs dev-up.sh, dev-status.sh, dev-down.sh and railway-status.sh against stub commands
# in a temp directory. No real docker, cloudflared, uv, Ollama, Railway or Python process
# is used, and nothing is started on this machine except `sleep`. Sentinel values stand
# in for secrets.
#
#   bash scripts/test-dev-scripts.sh

set -uo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
T="$(mktemp -d "${TMPDIR:-/tmp}/dev-scripts-test.XXXXXX")"
PASS=0
FAIL=0
cleanup() {
  # Stop any stub left running, by the PID files of this test's own temp repo only.
  local f
  for f in "$T"/repo/.dev/pids/*.pid; do
    [[ -s "$f" ]] && kill "$(cat "$f")" 2>/dev/null
  done
  true
}
trap cleanup EXIT

check() { # check "description" command...
  local desc="$1"; shift
  if "$@"; then PASS=$((PASS + 1)); printf 'ok    %s\n' "$desc"; else FAIL=$((FAIL + 1)); printf 'FAIL  %s\n' "$desc"; fi
}
contains() { grep -qF -- "$2" "$1"; }
lacks() { ! grep -qF -- "$2" "$1"; }

# --- the stub world ----------------------------------------------------------

mkdir -p "$T/repo/scripts" "$T/repo/.venv/bin" "$T/bin"
cp "$SRC"/dev-lib.sh "$SRC"/dev-up.sh "$SRC"/dev-down.sh "$SRC"/dev-status.sh "$SRC"/railway-status.sh "$T/repo/scripts/"
: >"$T/calls.log"

cat >"$T/bin/docker" <<STUB
#!/usr/bin/env bash
echo "docker \$*" >>"$T/calls.log"
[[ "\$1" == info ]] && exit 0
[[ "\$1 \$2" == "compose ps" ]] && { echo postgres; echo temporal; }
exit 0
STUB
cat >"$T/bin/uv" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
cat >"$T/bin/lsof" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
cat >"$T/bin/cloudflared" <<STUB
#!/usr/bin/env bash
n=\$(( \$(cat "$T/tunnel-count" 2>/dev/null || echo 0) + 1 ))
echo \$n >"$T/tunnel-count"
echo "INF Requesting new quick Tunnel on https://api.trycloudflare.com ..." >&2
echo "INF |  https://stub-\$n.trycloudflare.com  |" >&2
exec sleep 300
STUB
# curl: Ollama model list, and /healthz unless the URL is listed in dead-urls.
cat >"$T/bin/curl" <<STUB
#!/usr/bin/env bash
url="\${@: -1}"
[[ "\$url" == *11434/api/tags* ]] && { echo '{"models":[{"name":"bge-m3"}]}'; exit 0; }
base="\${url%/healthz}"
grep -qxF "\$base" "$T/dead-urls" 2>/dev/null && exit 22
echo '{"ok":true}'
STUB
# railway: a logged-in CLI in a linked project. Every service's latest deployment is
# SUCCESS unless STUB_RAILWAY_BAD_SERVICE names it; domains are <service>-stub.up.railway.app.
cat >"$T/bin/railway" <<STUB
#!/usr/bin/env bash
echo "railway \$*" >>"$T/calls.log"
svc=""
prev=""
for a in "\$@"; do
  [[ "\$prev" == --service ]] && svc="\$a"
  prev="\$a"
done
case "\$1" in
  whoami | status) exit 0 ;;
  deployment)
    if [[ "\$svc" == "\${STUB_RAILWAY_BAD_SERVICE:-}" ]]; then
      echo "  stub-deployment-id | FAILED | 2026-01-01 00:00:00 UTC"
    else
      echo "  stub-deployment-id | SUCCESS | 2026-01-01 00:00:00 UTC"
    fi ;;
  domain) echo "\$svc-stub.up.railway.app" ;;
  ssh) echo "schema current (latest 030)" ;;
  *) exit 0 ;;
esac
STUB
cat >"$T/repo/.venv/bin/python" <<STUB
#!/usr/bin/env bash
case "\$*" in
  *migrate.py*) echo "\${STUB_MIGRATE_OUT:-the database is up to date}"; exit "\${STUB_MIGRATE_RC:-0}" ;;
  *"-m agentos.worker"*) [[ -n "\${STUB_WORKER_FAILS:-}" ]] && { echo "worker: price missing for model" >&2; exit 1; }
                        echo "\$AGENTOS_EMBED_MODEL" >"$T/worker-embed"; exec sleep 300 ;;
  *"-m agentos.control_api"*) echo "\$AGENTOS_CONSOLE_URL" >>"$T/console-urls"; exec sleep 300 ;;
  *uvicorn*) exec sleep 300 ;;
  *) exit 0 ;;
esac
STUB
chmod +x "$T"/bin/* "$T/repo/.venv/bin/python"

export PATH="$T/bin:$PATH"
export DEV_HEALTH_TIMEOUT=6 DEV_SETTLE_SECONDS=1 DEV_WATCH_INTERVAL=0 DEV_WATCH_RECHECK=0
UP="$T/repo/scripts/dev-up.sh"
STATUS="$T/repo/scripts/dev-status.sh"
DOWN="$T/repo/scripts/dev-down.sh"
OUT="$T/out.txt"

# Sentinel secrets: if one shows up in output, a file or the process list, the test fails.
good_env() {
  export AGENTOS_OWNER_DB_PASSWORD=OwnerSentinel1 AGENTOS_APP_DB_PASSWORD=AppSentinel2
  export SLACK_BOT_TOKEN=xoxb-SentinelBotToken3 SLACK_SIGNING_SECRET=SigSentinel4 NOTION_TOKEN=NotionSentinel5
  export SLACK_CLIENT_ID=ClientId6 SLACK_CLIENT_SECRET=ClientSecretSentinel7 ANTHROPIC_API_KEY=AnthropicSentinel8
  unset AGENTOS_DATABASE_URL AGENTOS_OWNER_DATABASE_URL AGENTOS_CONSOLE_URL AGENTOS_EMBED_BASE_URL AGENTOS_EMBED_MODEL \
    AGENTOS_LLM_PROVIDER STUB_WORKER_FAILS STUB_MIGRATE_RC STUB_MIGRATE_OUT
}
no_secret_in() { # no sentinel value appears in the given file
  local s
  for s in OwnerSentinel1 AppSentinel2 SentinelBotToken3 SigSentinel4 NotionSentinel5 ClientSecretSentinel7 AnthropicSentinel8; do
    grep -qF "$s" "$1" && return 1
  done
  return 0
}
ran_compose_up() { grep -q '^docker compose up' "$T/calls.log"; }

# --- preflight refusals: nothing may start -----------------------------------

good_env; unset SLACK_SIGNING_SECRET
(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "a missing variable is named and refused" bash -c "[[ $rc -ne 0 ]] && grep -q 'SLACK_SIGNING_SECRET is not set' '$OUT'"
check "a refused preflight starts no container" bash -c "! grep -q '^docker compose up' '$T/calls.log'"

good_env; export SLACK_BOT_TOKEN=xoxb-Abcdefg1xoxb-Abcdefg1
(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "a bot token pasted twice is caught" bash -c "[[ $rc -ne 0 ]] && grep -q 'more than once' '$OUT'"
check "the doubled token is not printed" lacks "$OUT" "Abcdefg1"

good_env; export SLACK_BOT_TOKEN=NotABotToken
(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "a bot token without the xoxb- prefix is caught" bash -c "[[ $rc -ne 0 ]] && grep -q 'must start with xoxb-' '$OUT'"

for bad_pw in 'p@ss' 'p:ss' 'p/ss' 'p#ss' 'p%ss' 'p?ss' 'p&ss'; do
  good_env; export AGENTOS_APP_DB_PASSWORD="$bad_pw"
  (cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
  check "app password '$bad_pw' is refused" bash -c "[[ $rc -ne 0 ]] && grep -q 'breaks a database URL' '$OUT'"
done

good_env; export AGENTOS_DATABASE_URL='postgresql://agentos_app:OldPassword@127.0.0.1:55432/agentos'
(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "a stale database URL is refused" bash -c "[[ $rc -ne 0 ]] && grep -q 'does not match AGENTOS_APP_DB_PASSWORD' '$OUT'"
check "the stale URL's password is not printed" lacks "$OUT" "OldPassword"

good_env; export AGENTOS_EMBED_MODEL=bge-m3
(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "half-set embedding settings are refused" bash -c "[[ $rc -ne 0 ]] && grep -q 'both AGENTOS_EMBED_BASE_URL' '$OUT'"

check "none of the refusals created state" bash -c "[[ ! -d '$T/repo/.dev/pids' ]]"

# --- a normal start, from another directory ----------------------------------

good_env
(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "dev-up succeeds from another directory" bash -c "[[ $rc -eq 0 ]]"
check "it prints the repository path it uses" contains "$OUT" "Repository in use: $(cd "$T/repo" && pwd -P)"
check "it starts Docker with compose up" ran_compose_up
check "it prints the gateway URL" contains "$OUT" "gateway  https://stub-1.trycloudflare.com"
check "the quick-tunnel API address is not mistaken for the tunnel" lacks "$OUT" "https://api.trycloudflare.com/slack"
check "it prints the Event Subscriptions URL" contains "$OUT" "https://stub-1.trycloudflare.com/slack/events"
check "it prints the Interactivity URL" contains "$OUT" "https://stub-1.trycloudflare.com/slack/interactivity"
check "it prints the Redirect URL" contains "$OUT" "https://stub-2.trycloudflare.com/auth/slack/callback"
check "it prints a tail command for each log" contains "$OUT" "tail -f $(cd "$T/repo" && pwd -P)/.dev/logs/worker.log"
check "the console process gets the console tunnel URL" contains "$T/console-urls" "https://stub-2.trycloudflare.com"
check "the worker gets the default local embedding model" contains "$T/worker-embed" "bge-m3"
check "all five processes have PID files" bash -c "[[ \$(ls '$T/repo/.dev/pids' | wc -l) -eq 5 ]]"
check "no secret value appears in the output" no_secret_in "$OUT"
check "no secret value is in any file under .dev" bash -c "! grep -rqE 'Sentinel' '$T/repo/.dev'"
no_secret_in_ps() { # command lines of the processes dev-up started
  local p
  for p in $(cat "$T"/repo/.dev/pids/*.pid); do
    ps -ww -o args= -p "$p" | grep -q 'Sentinel' && return 1
  done
  return 0
}
check "no secret value is on any process command line" no_secret_in_ps
check ".dev ignores itself" contains "$T/repo/.dev/.gitignore" "*"

(cd / && bash "$STATUS") >"$OUT" 2>&1; rc=$?
check "dev-status reports a healthy stack with exit 0" bash -c "[[ $rc -eq 0 ]]"
check "dev-status shows the worker running" contains "$OUT" "worker: running"
check "dev-status shows a tunnel's URL and health" contains "$OUT" "tunnel gateway: https://stub-1.trycloudflare.com /healthz ok"

(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "a second dev-up refuses while the first runs" bash -c "[[ $rc -ne 0 ]] && grep -q 'already running' '$OUT'"

# --- a dead tunnel is reported loudly ----------------------------------------

echo "https://stub-1.trycloudflare.com" >"$T/dead-urls"
(cd / && DEV_WATCH_ROUNDS=1 bash "$STATUS" --watch) >"$OUT" 2>&1
check "dev-status --watch says loudly that the gateway tunnel is down" contains "$OUT" "THE GATEWAY TUNNEL IS DOWN"
: >"$T/dead-urls"

# --- stop --------------------------------------------------------------------

pids="$(cat "$T"/repo/.dev/pids/*.pid)"
(cd / && bash "$DOWN") >"$OUT" 2>&1; rc=$?
check "dev-down exits 0" bash -c "[[ $rc -eq 0 ]]"
check "dev-down removes every PID file" bash -c "[[ -z \$(ls '$T/repo/.dev/pids') ]]"
all_dead() { local p; for p in $pids; do kill -0 "$p" 2>/dev/null && return 1; done; return 0; }
check "dev-down stopped every process it started" all_dead
check "dev-down leaves Docker running by default" bash -c "! grep -q '^docker compose down' '$T/calls.log'"
(cd / && bash "$DOWN" --all) >"$OUT" 2>&1
check "dev-down --all stops Docker" bash -c "grep -q '^docker compose down' '$T/calls.log'"

# --- a worker that refuses to start ------------------------------------------

good_env; export STUB_WORKER_FAILS=1
(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "a process that exits at start fails dev-up with its log lines" bash -c "[[ $rc -ne 0 ]] && grep -q 'price missing for model' '$OUT'"
(cd / && bash "$DOWN") >/dev/null 2>&1

# --- a pending migration 021 asks for one index run --------------------------

good_env; export STUB_MIGRATE_OUT="applying 021_knowledge_health.sql"
(cd / && bash "$UP") >"$OUT" 2>&1
check "migration 021 prints the index reminder" contains "$OUT" "Migration 021 was just applied"
(cd / && bash "$DOWN") >/dev/null 2>&1

# --- a failed migration starts nothing else ----------------------------------

good_env; export STUB_MIGRATE_RC=1 STUB_MIGRATE_OUT="migration stopped: baseline needed"
before="$(cat "$T/tunnel-count")"
(cd / && bash "$UP") >"$OUT" 2>&1; rc=$?
check "a failed migration stops dev-up before the tunnels" bash -c "[[ $rc -ne 0 && \$(cat '$T/tunnel-count') -eq $before ]]"
check "it points at --baseline" contains "$OUT" "--baseline"

# --- --watch restarts a dead tunnel and shows the new URLs -------------------

good_env
(cd / && DEV_WATCH_INTERVAL=1 DEV_WATCH_ROUNDS=2 bash "$UP" --watch) >"$OUT" 2>&1 &
watcher=$!
for _ in $(seq 1 100); do grep -q 'Watching both tunnels' "$OUT" 2>/dev/null && break; sleep 0.1; done
gw_old="$(cat "$T/repo/.dev/urls/gateway")"
echo "$gw_old" >"$T/dead-urls"
wait "$watcher" 2>/dev/null
[[ -n "${SHOW_WATCH_OUT:-}" ]] && cat "$OUT"
check "dev-up --watch restarts a dead gateway tunnel" contains "$OUT" "THE GATEWAY TUNNEL IS DOWN"
check "it announces the new URL" bash -c "grep -q 'NEW gateway URL: https://stub-' '$OUT' && [[ \$(cat '$T/repo/.dev/urls/gateway') != '$gw_old' ]]"
(cd / && bash "$DOWN") >/dev/null 2>&1

# --- dev-env.sh: ask for each variable, then hand the values to dev-up ------------
# A second temp repo whose dev-up.sh is a stub. The stub compares each variable inside its
# own process against a checksum the test wrote outside the repo, so no value is stored.

mkdir -p "$T/repo2/scripts" "$T/expect"
cp "$SRC"/dev-lib.sh "$SRC"/dev-env.sh "$T/repo2/scripts/"
cat >"$T/repo2/scripts/dev-up.sh" <<STUB
#!/usr/bin/env bash
{
  echo "args: \$*"
  for n in AGENTOS_OWNER_DB_PASSWORD AGENTOS_APP_DB_PASSWORD SLACK_BOT_TOKEN SLACK_SIGNING_SECRET SLACK_CLIENT_ID \\
    SLACK_CLIENT_SECRET NOTION_TOKEN AGENTOS_LLM_PROVIDER QWEN_API_KEY QWEN_BASE_URL ANTHROPIC_API_KEY AGENTOS_MODEL; do
    if [[ -f "$T/expect/\$n" ]]; then
      if [[ "\$(printf %s "\${!n:-}" | cksum)" == "\$(cat "$T/expect/\$n")" ]]; then echo "\$n ok"; else echo "\$n WRONG"; fi
    else
      if [[ -z "\${!n:-}" ]]; then echo "\$n unset-ok"; else echo "\$n SHOULD-BE-UNSET"; fi
    fi
  done
  if ps -ww -o args= -p \$\$ | grep -q "Sentin[e]l"; then echo "ps LEAK"; else echo "ps ok"; fi
} >"$T/up-result"
STUB
cat >"$T/bin/fakeshell" <<STUB
#!/usr/bin/env bash
echo "shell args: \$*" >"$T/shell-result"
echo "token: \$([[ "\$SLACK_BOT_TOKEN" == xoxb-SentinelShell ]] && echo ok || echo WRONG)" >>"$T/shell-result"
STUB
chmod +x "$T/repo2/scripts/dev-up.sh" "$T/bin/fakeshell"

ENV_SCRIPT="$T/repo2/scripts/dev-env.sh"
LIVE_VARS="AGENTOS_OWNER_DB_PASSWORD AGENTOS_APP_DB_PASSWORD SLACK_BOT_TOKEN SLACK_SIGNING_SECRET SLACK_CLIENT_ID SLACK_CLIENT_SECRET NOTION_TOKEN AGENTOS_LLM_PROVIDER QWEN_API_KEY QWEN_BASE_URL ANTHROPIC_API_KEY AGENTOS_MODEL AGENTOS_DATABASE_URL"
clear_vars() { local n; for n in $LIVE_VARS; do unset "$n"; done; rm -f "$T/expect"/* "$T/up-result" "$T/shell-result"; }
expect() { printf %s "$2" | cksum >"$T/expect/$1"; }
# feed STDIN [args...]: give the lines to dev-env.sh (stdin is not a terminal) from another directory.
feed() { local input="$1"; shift; (cd / && printf '%b' "$input" | bash "$ENV_SCRIPT" "$@") >"$OUT" 2>&1; }
all_ok() { ! grep -qE 'WRONG|LEAK|SHOULD-BE' "$T/up-result" && grep -q '^ps ok' "$T/up-result"; }

COMMON_EXPECT() {
  expect AGENTOS_OWNER_DB_PASSWORD OwnerSentinel1; expect AGENTOS_APP_DB_PASSWORD AppSentinel2
  expect SLACK_BOT_TOKEN xoxb-SentinelBotToken3; expect SLACK_SIGNING_SECRET SigSentinel4
  expect SLACK_CLIENT_ID ClientId6; expect SLACK_CLIENT_SECRET ClientSecretSentinel7
  expect NOTION_TOKEN NotionSentinel5
}
BASE_IN='OwnerSentinel1\nAppSentinel2\nxoxb-SentinelBotToken3\nSigSentinel4\nClientId6\nClientSecretSentinel7\nNotionSentinel5\n'
QWEN_IN="${BASE_IN}"'\nQwenSentinel9\nhttps://qwen.example/v1\n\n'
set_qwen_expect() { COMMON_EXPECT; expect AGENTOS_LLM_PROVIDER qwen; expect QWEN_API_KEY QwenSentinel9; expect QWEN_BASE_URL https://qwen.example/v1; }

clear_vars; set_qwen_expect
feed "$QWEN_IN" --watch; rc=$?
check "dev-env hands every typed value to dev-up's environment, with its options" \
  bash -c "[[ $rc -eq 0 ]] && grep -q '^args: --watch' '$T/up-result' && [[ \$(grep -c ' ok\$' '$T/up-result') -ge 10 ]]"
check "no typed value was wrong, and none was on dev-up's command line" all_ok

clear_vars; set_qwen_expect
feed 'OwnerSentinel1\np@ss\nAppSentinel2\nxoxb-SentinelBotToken3xoxb-SentinelBotToken3\nxoxb-SentinelBotToken3\nSigSentinel4\nClientId6\nClientSecretSentinel7\nNotionSentinel5\n\nQwenSentinel9\nhttps://qwen.example/v1\n\n'; rc=$?
check "a rejected value is re-asked and the second one is used" \
  bash -c "[[ $rc -eq 0 ]] && grep -q 'breaks a database URL' '$OUT' && grep -q 'more than once' '$OUT'"
check "after the re-ask dev-up got the second values" all_ok

# Enter keeps a variable that is already set; --reset asks for it regardless.
clear_vars; set_qwen_expect
export SLACK_BOT_TOKEN=xoxb-SentinelKept
expect SLACK_BOT_TOKEN xoxb-SentinelKept
feed 'OwnerSentinel1\nAppSentinel2\n\nSigSentinel4\nClientId6\nClientSecretSentinel7\nNotionSentinel5\n\nQwenSentinel9\nhttps://qwen.example/v1\n\n'
check "an already-set variable is kept on Enter" bash -c "grep -q 'SLACK_BOT_TOKEN already set, keeping it' '$OUT'"
check "the kept value is what dev-up got" all_ok
clear_vars; set_qwen_expect
export SLACK_BOT_TOKEN=xoxb-SentinelKept
feed "$QWEN_IN" --reset
check "--reset replaces an already-set variable with the typed one" all_ok

# The provider decides which key is asked for.
for provider in qwen anthropic; do
  clear_vars; COMMON_EXPECT; expect AGENTOS_LLM_PROVIDER "$provider"
  if [[ "$provider" == anthropic ]]; then
    expect ANTHROPIC_API_KEY AnthropicSentinel8
    tail_in='AnthropicSentinel8\n\n'; asked=ANTHROPIC_API_KEY; not_asked=QWEN_API_KEY
  else
    expect QWEN_API_KEY QwenSentinel9; expect QWEN_BASE_URL https://qwen.example/v1
    tail_in='QwenSentinel9\nhttps://qwen.example/v1\n\n'; asked=QWEN_API_KEY; not_asked=ANTHROPIC_API_KEY
  fi
  feed "${BASE_IN}${provider}\n${tail_in}"
  check "provider $provider asks for $asked and not $not_asked" \
    bash -c "grep -q '^$asked>' '$OUT' && ! grep -q '^$not_asked>' '$OUT'"
  check "provider $provider reaches dev-up with only its own key set" all_ok
done

# No secret in the output or in any file of the temp repo (the stub checks the command line).
clear_vars; set_qwen_expect
feed "$QWEN_IN"
check "dev-env prints no secret value" no_secret_in "$OUT"
check "dev-env writes no secret value to any file in its repo" bash -c "! grep -rqE 'Sentinel' '$T/repo2'"

# --shell opens the user's shell with the values.
clear_vars
SHELL="$T/bin/fakeshell" feed 'OwnerSentinel1\nAppSentinel2\nxoxb-SentinelShell\nSigSentinel4\nClientId6\nClientSecretSentinel7\nNotionSentinel5\nanthropic\nAnthropicSentinel8\n\n' --shell
check "--shell says what it is and opens the shell with the values" \
  bash -c "grep -q 'a shell with the variables; type exit to leave it' '$OUT' && grep -q 'shell args: -i' '$T/shell-result' && grep -q 'token: ok' '$T/shell-result'"

# Three bad tries name the variable and stop.
clear_vars
feed 'OwnerSentinel1\np@a\np@b\np@c\n'; rc=$?
check "three rejected tries exit non-zero naming the variable" bash -c "[[ $rc -ne 0 ]] && grep -q 'AGENTOS_APP_DB_PASSWORD: three tries failed' '$OUT'"

# --- railway-status.sh: a failed deployment is named and fails the run ---------

good_env; export STUB_RAILWAY_BAD_SERVICE=ollama
(cd / && bash "$T/repo/scripts/railway-status.sh") >"$OUT" 2>&1; rc=$?
check "railway-status names a failed deployment and exits non-zero" \
  bash -c "[[ $rc -ne 0 ]] && grep -q 'deploy ollama: FAILED' '$OUT'"
unset STUB_RAILWAY_BAD_SERVICE

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
