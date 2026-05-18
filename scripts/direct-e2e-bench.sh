#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

LISTEN="${LISTEN:-127.0.0.1:21211}"
BODY_BYTES="${BODY_BYTES:-786432}"
ARTICLE_BYTES="${ARTICLE_BYTES:-786432}"
PENDING_WRITE_BYTES="${PENDING_WRITE_BYTES:-819200}"
SERVER_THREADS="${SERVER_THREADS:-4}"
MAX_PIPELINE_DEPTH="${MAX_PIPELINE_DEPTH:-256}"

TRANSFER_BYTES="${TRANSFER_BYTES:-100000000000}"
CONNECTIONS="${CONNECTIONS:-16}"
CLIENT_THREADS="${CLIENT_THREADS:-8}"
PIPELINE_DEPTH="${PIPELINE_DEPTH:-128}"
COMMAND_MIX="${COMMAND_MIX:-article}"
RUNS="${RUNS:-10}"

if [[ "${BUILD:-1}" != "0" ]]; then
    cargo build --release --bin nntpbench
fi

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" || true
    fi
    if [[ -n "${SERVER_LOG:-}" ]]; then
        rm -f "$SERVER_LOG"
    fi
}
trap cleanup EXIT

SERVER_LOG="$(mktemp "$PROJECT_DIR/target/direct-e2e-server.XXXXXX.log")"
./target/release/nntpbench server \
    --listen "$LISTEN" \
    --body-bytes "$BODY_BYTES" \
    --article-bytes "$ARTICLE_BYTES" \
    --threads "$SERVER_THREADS" \
    --max-pipeline-depth "$MAX_PIPELINE_DEPTH" \
    --pending-write-bytes "$PENDING_WRITE_BYTES" \
    --stats-interval-secs 0 >"$SERVER_LOG" 2>&1 &
SERVER_PID="$!"

while ! grep -q "server listening" "$SERVER_LOG"; do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        cat "$SERVER_LOG"
        exit 1
    fi
    sleep 0.05
done
grep -m1 "server listening" "$SERVER_LOG"

for run in $(seq 1 "$RUNS"); do
    printf 'run=%s body_bytes=%s article_bytes=%s pending_write_bytes=%s command_mix=%s\n' \
        "$run" "$BODY_BYTES" "$ARTICLE_BYTES" "$PENDING_WRITE_BYTES" "$COMMAND_MIX"
    ./target/release/nntpbench client \
        --connect "$LISTEN" \
        --transfer-bytes "$TRANSFER_BYTES" \
        --connections "$CONNECTIONS" \
        --threads "$CLIENT_THREADS" \
        --pipeline-depth "$PIPELINE_DEPTH" \
        --command-mix "$COMMAND_MIX" \
        --stats-interval-secs 0 \
        --csv
done
