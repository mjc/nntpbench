#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}"

BENCH_NAME="typed_client_roundtrip"
BENCH_ARGS=("$@")

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    echo "Usage: $0 [DIVAN_ARGS...]"
    echo ""
    echo "Build and profile the typed client benchmark target with perf."
    echo "Any extra arguments are passed directly to the divan bench binary."
    echo ""
    echo "Examples:"
    echo "  $0"
    echo "  $0 sequential_roundtrip::article_64k"
    echo "  $0 pipelined_roundtrip::mixed_4"
    exit 0
fi

if ! command -v inferno-collapse-perf &> /dev/null; then
    echo "Installing inferno..."
    cargo install inferno
fi

echo 0 | sudo tee /proc/sys/kernel/kptr_restrict > /dev/null
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid > /dev/null

printf '\033]0;perf: nntpbench typed bench\007'

echo "Building $BENCH_NAME..."
RUSTFLAGS="-C target-cpu=native -C force-frame-pointers=yes" \
    cargo bench --bench "$BENCH_NAME" --no-run

mapfile -t bench_bins < <(find "$TARGET_DIR" -path "*/deps/${BENCH_NAME}-*" -type f -executable)
if [[ "${#bench_bins[@]}" -eq 0 ]]; then
    echo "Error: could not locate built bench binary for $BENCH_NAME"
    exit 1
fi

BENCH_BIN="$(ls -t "${bench_bins[@]}" | head -n1)"

echo "Profiling: $BENCH_BIN ${BENCH_ARGS[*]}"
echo ""
perf record -g --call-graph fp -F 997 "$BENCH_BIN" "${BENCH_ARGS[@]}"

echo ""
echo "Generating flamegraph from perf.data..."
perf script 2>/dev/null | inferno-collapse-perf | inferno-flamegraph > flamegraph.svg

echo "Done: flamegraph.svg"
echo "Analyze with: ./scripts/parse_flamegraph flamegraph.svg summary"
