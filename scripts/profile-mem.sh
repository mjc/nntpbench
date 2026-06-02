#!/usr/bin/env bash
set -e

# Heap profiling for nntpbench
#
# Uses the native heap profiler:
#   Linux: heaptrack, then valgrind massif fallback
#   macOS: MallocStackLogging + leaks live snapshot
# The default target runs `nntpbench server`; pass server args after the target.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

PLATFORM="$(uname -s)"
FIRST_ARG="${1:-}"
TARGET="server"
if [ $# -gt 0 ]; then
    case "$1" in
        server|nntpbench|/*|./*|../*)
            TARGET="$1"
            shift
            ;;
    esac
fi
EXTRA_ARGS=("$@")

if [ "$FIRST_ARG" = "-h" ] || [ "$FIRST_ARG" = "--help" ]; then
    echo "Usage: $0 [TARGET] [ARGS...]"
    echo ""
    echo "Profile nntpbench heap allocations"
    echo ""
    echo "Arguments:"
    echo '  TARGET    server (default), nntpbench, or a custom path'
    echo "  ARGS...   Extra arguments passed to the binary"
    echo ""
    echo "Tools (in preference order):"
    echo "  Linux: heaptrack, then valgrind massif"
    echo "  macOS: MallocStackLogging + leaks"
    echo ""
    echo "Environment:"
    echo "  PROFILE_SECONDS    macOS live snapshot delay, default 10"
    echo ""
    echo "Examples:"
    echo "  ./scripts/profile-mem.sh"
    echo "  ./scripts/profile-mem.sh"
    echo "  ./scripts/profile-mem.sh server --listen 127.0.0.1:1199"
    echo "  ./scripts/profile-mem.sh nntpbench server --listen 127.0.0.1:1199"
    echo ""
    echo "Stop nntpbench normally to generate the report."
    exit 0
fi

# Resolve binary name
case "$TARGET" in
    server)
        BINARY="$PROJECT_DIR/target/profiling/nntpbench"
        BIN_NAME="nntpbench"
        RUN_ARGS=("server")
        ;;
    nntpbench)
        BINARY="$PROJECT_DIR/target/profiling/nntpbench"
        BIN_NAME="nntpbench"
        RUN_ARGS=()
        ;;
    *)
        BINARY="$TARGET"
        BIN_NAME=""  # Custom path, skip build
        RUN_ARGS=()
        ;;
esac

build_profile_binary() {
    if [ -n "$BIN_NAME" ]; then
        echo "Building $BIN_NAME..."
        RUSTFLAGS="-C target-cpu=native -C force-frame-pointers=yes" cargo build --profile profiling --bin "$BIN_NAME"
    fi
}

echo "=== Heap Profiling ==="
echo ""

case "$PLATFORM" in
  Darwin)
    if ! command -v leaks &> /dev/null; then
        echo "Error: macOS leaks tool not found"
        exit 1
    fi

    build_profile_binary
    SECONDS_TO_RUN="${PROFILE_SECONDS:-10}"
    LEAKS_OUTPUT="leaks.txt"
    MEMGRAPH_DIR="$PROJECT_DIR/target/profiling"
    mkdir -p "$MEMGRAPH_DIR"

    echo "Using MallocStackLogging + leaks..."
    echo "Profiling: $BINARY ${RUN_ARGS[*]} ${EXTRA_ARGS[*]}"
    echo "Taking live memory snapshot after ${SECONDS_TO_RUN}s."
    echo ""

    MallocStackLogging=1 "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" &
    APP_PID=$!
    sleep "$SECONDS_TO_RUN"

    if kill -0 "$APP_PID" 2>/dev/null; then
        leaks --fullStacks --fullStackHistory --outputGraph="$MEMGRAPH_DIR" "$APP_PID" > "$LEAKS_OUTPUT" 2>&1 || true
        kill -INT "$APP_PID" 2>/dev/null || true
        wait "$APP_PID" 2>/dev/null || true
        echo "Leaks report: $LEAKS_OUTPUT"
        echo "Memory graph directory: $MEMGRAPH_DIR"
    else
        wait "$APP_PID" 2>/dev/null || true
        echo "Process exited before the live leaks snapshot could be collected."
        echo "For short-lived commands, run: MallocStackLogging=1 leaks --atExit -- <command>"
    fi
    ;;

  Linux)
    build_profile_binary

    if command -v heaptrack &> /dev/null; then
        echo "Using heaptrack..."
        echo "Profiling: $BINARY ${RUN_ARGS[*]} ${EXTRA_ARGS[*]}"
        echo "Stop nntpbench to generate report."
        echo ""

        heaptrack "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" || true

        # Find the most recent heaptrack output
        HEAPTRACK_FILE=$(ls -t heaptrack.*.zst 2>/dev/null | head -1)

        if [ -n "$HEAPTRACK_FILE" ]; then
            echo ""
            echo "Heaptrack data: $HEAPTRACK_FILE"
            echo ""

            if command -v heaptrack_print &> /dev/null; then
                echo "Generating text summary..."
                heaptrack_print "$HEAPTRACK_FILE" -F stacks.txt > heaptrack-summary.txt 2>&1 || true
                echo "Summary: heaptrack-summary.txt"
                echo "Stacks:  stacks.txt"
            fi

            if command -v heaptrack_gui &> /dev/null; then
                echo ""
                echo "Open GUI with: heaptrack_gui $HEAPTRACK_FILE"
            fi
        else
            echo "Warning: No heaptrack output file found"
        fi

    elif command -v valgrind &> /dev/null; then
        echo "heaptrack not found, falling back to valgrind massif..."
        echo "Profiling: $BINARY ${RUN_ARGS[*]} ${EXTRA_ARGS[*]}"
        echo "Stop nntpbench to generate report."
        echo ""

        valgrind --tool=massif --pages-as-heap=yes \
            "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" || true

        # Find the most recent massif output
        MASSIF_FILE=$(ls -t massif.out.* 2>/dev/null | head -1)

        if [ -n "$MASSIF_FILE" ]; then
            echo ""
            echo "Massif data: $MASSIF_FILE"

            if command -v ms_print &> /dev/null; then
                echo ""
                echo "=== Massif Summary ==="
                ms_print "$MASSIF_FILE" | head -50
                echo ""
                echo "Full report: ms_print $MASSIF_FILE | less"
            fi
        else
            echo "Warning: No massif output file found"
        fi

    else
        echo "Error: Neither heaptrack nor valgrind found"
        echo ""
        echo "Install one of:"
        echo "  heaptrack (recommended): sudo apt install heaptrack"
        echo "  valgrind:                sudo apt install valgrind"
        exit 1
    fi
    ;;

  *)
    echo "Error: unsupported platform: $PLATFORM"
    exit 1
    ;;
esac
