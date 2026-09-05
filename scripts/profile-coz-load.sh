#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}"
OUTPUT="${OUTPUT:-$TARGET_DIR/profiling/profile-load.coz}"
LISTEN="${LISTEN:-127.0.0.1:21211}"
TRANSFER_BYTES="${TRANSFER_BYTES:-100000000000}"
CONNECTIONS="${CONNECTIONS:-16}"
CLIENT_THREADS="${CLIENT_THREADS:-8}"
PIPELINE_DEPTH="${PIPELINE_DEPTH:-128}"
COMMAND_MIX="${COMMAND_MIX:-article}"
FIXED_LINE="${FIXED_LINE:-}"
FIXED_SPEEDUP="${FIXED_SPEEDUP:-}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    echo "Usage: $0"
    echo ""
    echo "Build nntpbench with coz markers and run the direct client load under coz."
    echo ""
    echo "Environment:"
    echo "  OUTPUT           profile path, default target/profiling/profile-load.coz"
    echo "  TRANSFER_BYTES   transfer target, default 100000000000"
    echo "  LISTEN           server address, default 127.0.0.1:21211"
    echo "  CONNECTIONS      client connections, default 16"
    echo "  CLIENT_THREADS   client threads, default 8"
    echo "  PIPELINE_DEPTH   request pipeline depth, default 128"
    echo "  COMMAND_MIX      article, body, or alternate; default article"
    echo "  FIXED_LINE       optional source line for fixed-line experiments"
    echo "  FIXED_SPEEDUP    optional fixed speedup percentage for FIXED_LINE"
    exit 0
fi

if ! command -v coz &> /dev/null; then
    echo "Error: coz is not installed. Use nix develop or install coz first."
    exit 1
fi

mkdir -p "$(dirname "$OUTPUT")"

echo "Building nntpbench with coz markers and frame pointers..."
RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native -C force-frame-pointers=yes}" \
    cargo build --profile profiling --features coz --bin nntpbench

server_log="$(mktemp "$TARGET_DIR/coz-load-server.XXXXXX.log")"
./target/profiling/nntpbench server \
    --listen "$LISTEN" \
    --threads 4 \
    --max-connections 4096 \
    --max-pipeline-depth 256 \
    --backlog 8192 \
    --nodelay \
    --socket-recv-buffer 16777216 \
    --socket-send-buffer 16777216 \
    --stats-interval-secs 0 \
    --flush >"$server_log" 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true; rm -f "$server_log"' EXIT

for _ in $(seq 1 100); do
    if grep -q "server listening" "$server_log"; then
        break
    fi
    sleep 0.1
done
grep -m1 "server listening" "$server_log"

coz_args=(
    run
    --source-scope "$PROJECT_DIR/src/%" \
    --output "$OUTPUT" \
)
if [[ -n "$FIXED_LINE" ]]; then
    coz_args+=(--fixed-line "$FIXED_LINE")
fi
if [[ -n "$FIXED_SPEEDUP" ]]; then
    coz_args+=(--fixed-speedup "$FIXED_SPEEDUP")
fi

coz "${coz_args[@]}" \
    --- ./target/profiling/nntpbench client \
    --connect "$LISTEN" \
    --transfer-bytes "$TRANSFER_BYTES" \
    --connections "$CONNECTIONS" \
    --threads "$CLIENT_THREADS" \
    --pipeline-depth "$PIPELINE_DEPTH" \
    --command-mix "$COMMAND_MIX" \
    --read-buffer-bytes 262144 \
    --socket-recv-buffer 16777216 \
    --socket-send-buffer 16777216 \
    --stats-interval-secs 0 \
    --csv

kill "$server_pid" 2>/dev/null || true
wait "$server_pid" 2>/dev/null || true
rm -f "$server_log"
trap - EXIT

echo "Wrote $OUTPUT"
