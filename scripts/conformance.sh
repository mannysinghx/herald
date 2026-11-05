#!/usr/bin/env bash
# Differential test: runs the same scenarios against a reference ntfy binary and herald, on fresh
# data, and diffs the normalized transcripts. Usage:
#   REF=/path/to/ntfy scripts/conformance.sh [workdir]
# Without REF it compares herald against the golden transcripts in tools/conformance/golden/.
# Binds only 127.0.0.1 on ports 28181-28186; it refuses to run if one of them is taken.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
WORK=${1:-$(mktemp -d)}
GOLDEN=$ROOT/tools/conformance/golden
mkdir -p "$WORK" "$GOLDEN"
go build -o "$WORK/herald" .
go build -o "$WORK/conformance" ./tools/conformance
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

wait_up() { for _ in $(seq 1 50); do curl -sf -o /dev/null "http://127.0.0.1:$1/v1/health" && return 0; sleep 0.1; done; echo "server on $1 did not start"; exit 1; }
for p in 28181 28182 28183 28184 28185 28186; do
  if lsof -nP -iTCP:$p -sTCP:LISTEN >/dev/null 2>&1; then echo "port $p is in use, not touching it"; exit 1; fi
done

start_ref() { # name port extra-args...
  local name=$1 port=$2; shift 2
  local dir="$WORK/ref-$name"; rm -rf "$dir"; mkdir -p "$dir"
  "$REF" serve --listen-http "127.0.0.1:$port" --base-url "http://127.0.0.1:$port" --cache-file "$dir/cache.db" \
    --attachment-cache-dir "$dir/att" "$@" >"$dir/log" 2>&1 &
  PIDS+=($!); wait_up "$port"
}
start_herald() {
  local name=$1 port=$2; shift 2
  local dir="$WORK/herald-$name"; rm -rf "$dir"; mkdir -p "$dir"
  "$WORK/herald" serve -listen "127.0.0.1:$port" -base-url "http://127.0.0.1:$port" -db "$dir/h.db" \
    -attachment-dir "$dir/att" "$@" >"$dir/log" 2>&1 &
  PIDS+=($!); wait_up "$port"
}

status=0
compare() { # scenario
  local s=$1
  if diff -u "$GOLDEN/$s.txt" "$WORK/herald-$s.txt" >"$WORK/$s.diff"; then
    echo "conformance $s: identical ($(grep -c '^## ' "$WORK/herald-$s.txt") steps)"
  else
    echo "conformance $s: DIFFERS, see $WORK/$s.diff"; cat "$WORK/$s.diff"; status=1
  fi
}

if [ -n "${REF:-}" ]; then
  start_ref protocol 28181 --visitor-request-limit-exempt-hosts 127.0.0.1
  "$WORK/conformance" -url http://127.0.0.1:28181 -scenario protocol >"$GOLDEN/protocol.txt"
  start_ref limits 28182
  "$WORK/conformance" -url http://127.0.0.1:28182 -scenario limits >"$GOLDEN/limits.txt"
  dir="$WORK/ref-auth"; rm -rf "$dir"; mkdir -p "$dir"
  export NTFY_AUTH_FILE="$dir/auth.db" NTFY_AUTH_DEFAULT_ACCESS=deny-all
  # ntfy only creates its auth file when the server starts, so start it once for that
  "$REF" serve --listen-http 127.0.0.1:28183 --cache-file "$dir/cache.db" >"$dir/log" 2>&1 &
  bootstrap=$!; wait_up 28183; kill "$bootstrap"; wait "$bootstrap" 2>/dev/null || true
  NTFY_PASSWORD=conformance-pass "$REF" user add alice >/dev/null 2>&1
  "$REF" access alice 'alerts*' rw >/dev/null 2>&1
  "$REF" access alice 'readonly*' ro >/dev/null 2>&1
  "$REF" access alice 'writeonly*' wo >/dev/null 2>&1
  token=$("$REF" token add alice 2>&1 | grep -o 'tk_[A-Za-z0-9]*' | head -1)
  "$REF" serve --listen-http 127.0.0.1:28183 --base-url http://127.0.0.1:28183 --cache-file "$dir/cache.db" \
    --visitor-request-limit-exempt-hosts 127.0.0.1 >"$dir/log" 2>&1 &
  PIDS+=($!); wait_up 28183
  "$WORK/conformance" -url http://127.0.0.1:28183 -scenario auth -token "$token" >"$GOLDEN/auth.txt"
  unset NTFY_AUTH_FILE NTFY_AUTH_DEFAULT_ACCESS
fi

start_herald protocol 28184 -default-access rw -rate-limit-exempt 127.0.0.1/32
"$WORK/conformance" -url http://127.0.0.1:28184 -scenario protocol >"$WORK/herald-protocol.txt"
compare protocol
start_herald limits 28185 -default-access rw
"$WORK/conformance" -url http://127.0.0.1:28185 -scenario limits >"$WORK/herald-limits.txt"
compare limits
dir="$WORK/herald-auth"; rm -rf "$dir"; mkdir -p "$dir"
token=$("$WORK/herald" token add -db "$dir/h.db" -name conformance -allow 'alerts*:rw,readonly*:ro,writeonly*:wo' | grep -o 'tk_[A-Za-z0-9]*')
"$WORK/herald" serve -listen 127.0.0.1:28186 -base-url http://127.0.0.1:28186 -db "$dir/h.db" -rate-limit-exempt 127.0.0.1/32 >"$dir/log" 2>&1 &
PIDS+=($!); wait_up 28186
"$WORK/conformance" -url http://127.0.0.1:28186 -scenario auth -token "$token" >"$WORK/herald-auth.txt"
compare auth
exit $status
