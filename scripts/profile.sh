#!/usr/bin/env bash
set -e

# CPU profiling for nntpbench
#
# Builds with frame pointers, runs under perf, generates flamegraph.
# The default target runs `nntpbench server`; pass server args after the target.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

ATTACH_PID=""
TARGET="server"
EXTRA_ARGS=()

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            echo "Usage: $0 [--pid PID] [TARGET] [ARGS...]"
            echo ""
            echo "Profile nntpbench CPU usage"
            echo ""
            echo "Options:"
            echo "  --pid PID   Attach to an already-running process instead of launching one"
            echo ""
            echo "Arguments:"
            echo '  TARGET    server (default), nntpbench, or a custom path'
            echo "  ARGS...   Extra arguments passed to the binary"
            echo ""
            echo "Examples:"
            echo "  ./scripts/profile.sh"
            echo "  ./scripts/profile.sh server --listen 127.0.0.1:1199"
            echo "  ./scripts/profile.sh nntpbench server --listen 127.0.0.1:1199"
            echo "  ./scripts/profile.sh --pid 12345"
            echo ""
            echo "Stop nntpbench normally with Ctrl-C to generate flamegraph.svg"
            exit 0
            ;;
        --pid)
            ATTACH_PID="$2"
            shift 2
            ;;
        *)
            if [ -z "${BIN_SET:-}" ]; then
                case "$1" in
                    server|nntpbench|/*|./*|../*)
                        TARGET="$1"
                        BIN_SET=1
                        shift
                        ;;
                    *)
                        EXTRA_ARGS+=("$1")
                        shift
                        ;;
                esac
            else
                EXTRA_ARGS+=("$1")
                shift
            fi
            ;;
    esac
done

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

# Check deps
if ! command -v inferno-collapse-perf &> /dev/null; then
    echo "Installing inferno..."
    cargo install inferno
fi

# Fix perf permissions
echo 0 | sudo tee /proc/sys/kernel/kptr_restrict > /dev/null
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid > /dev/null

# Set terminal title for tmux/terminal identification
printf '\033]0;perf: nntpbench CPU\007'

# Build with native CPU + frame pointers (only the binary we need)
if [ -z "$ATTACH_PID" ] && [ -n "$BIN_NAME" ]; then
    echo "Building $BIN_NAME..."
    RUSTFLAGS="-C target-cpu=native -C force-frame-pointers=yes" cargo build --profile profiling --bin "$BIN_NAME"
fi

# Record using frame pointers
set +e
if [ -n "$ATTACH_PID" ]; then
    echo "Attaching to PID $ATTACH_PID..."
    echo "Press Ctrl-C to stop recording and generate flamegraph."
    echo ""
    perf record -g --call-graph fp -F 997 -p "$ATTACH_PID"
else
    echo "Profiling: $BINARY ${RUN_ARGS[*]} ${EXTRA_ARGS[*]}"
    echo "Stop nntpbench normally to generate flamegraph."
    echo ""
    perf record -g --call-graph fp -F 997 "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}"
fi
set -e

echo ""
echo "Generating flamegraph from perf.data..."

if [ ! -f perf.data ]; then
    echo "Error: perf.data not found"
    exit 1
fi

perf script 2>/dev/null | inferno-collapse-perf | inferno-flamegraph > flamegraph.svg

echo "Done: flamegraph.svg"
echo ""
echo "Open with: firefox flamegraph.svg"
echo "Analyze with: ./scripts/parse_flamegraph flamegraph.svg summary"
