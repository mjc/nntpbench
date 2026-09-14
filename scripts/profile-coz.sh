#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

BENCH_NAME="client_roundtrip"
BENCH_ARGS=("$@")
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    echo "Usage: $0 [DIVAN_ARGS...]"
    echo ""
    echo "Build the client benchmark with coz markers, then run it under coz."
    echo "The benchmark binary is executed directly so the profiler sees the same"
    echo "NNTP request path that the normal bench run exercises."
    echo ""
    echo "Examples:"
    echo "  $0"
    echo "  $0 sequential_roundtrip::article_64k"
    echo "  $0 pipelined_roundtrip::mixed_4"
    echo ""
    echo "Outputs are written by coz itself; run coz plot after enough samples."
    exit 0
fi

if ! command -v coz &> /dev/null; then
    echo "Error: coz is not installed. Use `nix develop` or install coz first."
    exit 1
fi

echo "Building $BENCH_NAME with coz markers..."
RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native -C force-frame-pointers=yes}" \
    cargo bench --bench "$BENCH_NAME" --features coz --no-run

mapfile -t bench_bins < <(find "$TARGET_DIR" -path "*/deps/${BENCH_NAME}-*" -type f -executable)
if [[ "${#bench_bins[@]}" -eq 0 ]]; then
    echo "Error: could not locate built bench binary for $BENCH_NAME"
    exit 1
fi

BENCH_BIN="$(ls -t "${bench_bins[@]}" | head -n1)"

echo "Running under coz: $BENCH_BIN ${BENCH_ARGS[*]}"
echo "When the profiler has enough samples, stop the run and open the report with coz plot."
echo ""
coz run --source-scope "$PROJECT_DIR/src/%" --- "$BENCH_BIN" "${BENCH_ARGS[@]}"
