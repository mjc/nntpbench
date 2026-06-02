#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}"

LISTEN="${LISTEN:-127.0.0.1:21211}"
BODY_BYTES="${BODY_BYTES:-786432}"
ARTICLE_BYTES="${ARTICLE_BYTES:-786432}"
PENDING_WRITE_BYTES="${PENDING_WRITE_BYTES:-819200}"
SERVER_THREADS="${SERVER_THREADS:-4}"
MAX_PIPELINE_DEPTH="${MAX_PIPELINE_DEPTH:-256}"
if [[ "$(uname -s)" == "Darwin" ]]; then
    DEFAULT_SOCKET_RECV_BUFFER=1048576
    DEFAULT_SOCKET_SEND_BUFFER=1048576
else
    DEFAULT_SOCKET_RECV_BUFFER=16777216
    DEFAULT_SOCKET_SEND_BUFFER=16777216
fi
SERVER_SOCKET_RECV_BUFFER="${SERVER_SOCKET_RECV_BUFFER:-${SOCKET_RECV_BUFFER:-$DEFAULT_SOCKET_RECV_BUFFER}}"
SERVER_SOCKET_SEND_BUFFER="${SERVER_SOCKET_SEND_BUFFER:-${SOCKET_SEND_BUFFER:-$DEFAULT_SOCKET_SEND_BUFFER}}"

TRANSFER_BYTES="${TRANSFER_BYTES:-100000000000}"
CONNECTIONS="${CONNECTIONS:-16}"
CLIENT_THREADS="${CLIENT_THREADS:-8}"
PIPELINE_DEPTH="${PIPELINE_DEPTH:-128}"
COMMAND_MIX="${COMMAND_MIX:-article}"
RUNS="${RUNS:-10}"
CLIENT_SOCKET_RECV_BUFFER="${CLIENT_SOCKET_RECV_BUFFER:-${SOCKET_RECV_BUFFER:-$DEFAULT_SOCKET_RECV_BUFFER}}"
CLIENT_SOCKET_SEND_BUFFER="${CLIENT_SOCKET_SEND_BUFFER:-${SOCKET_SEND_BUFFER:-$DEFAULT_SOCKET_SEND_BUFFER}}"

if [[ "${BUILD:-1}" != "0" ]]; then
    cargo build --release --bin nntpbench
fi
mkdir -p "$TARGET_DIR"

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

SERVER_LOG="$(mktemp "$TARGET_DIR/direct-e2e-server.XXXXXX.log")"
"$TARGET_DIR/release/nntpbench" server \
    --listen "$LISTEN" \
    --body-bytes "$BODY_BYTES" \
    --article-bytes "$ARTICLE_BYTES" \
    --threads "$SERVER_THREADS" \
    --max-pipeline-depth "$MAX_PIPELINE_DEPTH" \
    --pending-write-bytes "$PENDING_WRITE_BYTES" \
    --socket-recv-buffer "$SERVER_SOCKET_RECV_BUFFER" \
    --socket-send-buffer "$SERVER_SOCKET_SEND_BUFFER" \
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
    "$TARGET_DIR/release/nntpbench" typed-client \
        --connect "$LISTEN" \
        --transfer-bytes "$TRANSFER_BYTES" \
        --connections "$CONNECTIONS" \
        --threads "$CLIENT_THREADS" \
        --pipeline-depth "$PIPELINE_DEPTH" \
        --command-mix "$COMMAND_MIX" \
        --socket-recv-buffer "$CLIENT_SOCKET_RECV_BUFFER" \
        --socket-send-buffer "$CLIENT_SOCKET_SEND_BUFFER" \
        --stats-interval-secs 0 \
        --csv
done
