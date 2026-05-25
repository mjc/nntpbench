#!/usr/bin/env bash
set -e

# Latency-focused profiling for nntpbench
#
# Shows WHERE TIME IS SPENT WAITING - syscall latency, off-CPU time, etc.
# The default target runs `nntpbench server`; pass server args after the target.
#
# Outputs:
#   strace mode:  strace.log + summary
#   offcpu mode:  flamegraph-offcpu.svg
#   macOS sample mode: latency-sample.txt
#   macOS dtrace mode: dtrace-syscalls.log

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

PLATFORM="$(uname -s)"
case "${1:-}" in
    -h|--help|strace|offcpu|sample|dtrace)
        MODE="$1"
        shift
        ;;
    *)
        case "$PLATFORM" in
            Darwin) MODE="sample" ;;
            *) MODE="strace" ;;
        esac
        ;;
esac

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

if [ "$MODE" = "-h" ] || [ "$MODE" = "--help" ]; then
    echo "Usage: $0 [MODE] [TARGET] [ARGS...]"
    echo ""
    echo "Profile nntpbench latency and waiting patterns"
    echo ""
    echo "Modes:"
    echo "  strace  - Linux syscall latency"
    echo "  offcpu  - Linux off-CPU flamegraph"
    echo "  sample  - macOS sampled wait/CPU stacks, default on Darwin"
    echo "  dtrace  - macOS syscall latency aggregation"
    echo ""
    echo "Environment:"
    echo "  PROFILE_SECONDS       macOS sample/dtrace duration, default 10"
    echo "  SAMPLE_INTERVAL_MS    macOS sample interval, default 1"
    echo ""
    echo "Arguments:"
    echo '  TARGET    server (default), nntpbench, or a custom path'
    echo "  ARGS...   Extra arguments passed to the binary"
    echo ""
    echo "Examples:"
    echo "  ./scripts/profile-latency.sh strace"
    echo "  ./scripts/profile-latency.sh strace server --listen 127.0.0.1:1199"
    echo "  ./scripts/profile-latency.sh offcpu server --listen 127.0.0.1:1199"
    echo ""
    echo "Stop nntpbench normally to generate reports."
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

echo "=== Latency Profile Mode: $MODE ==="
echo ""

run_macos_sample() {
    local output="latency-sample.txt"
    local seconds="${PROFILE_SECONDS:-10}"
    local interval_ms="${SAMPLE_INTERVAL_MS:-1}"

    if ! command -v sample &> /dev/null; then
        echo "Error: macOS sample tool not found"
        exit 1
    fi

    build_profile_binary
    echo "Profiling: $BINARY ${RUN_ARGS[*]} ${EXTRA_ARGS[*]}"
    "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" &
    APP_PID=$!
    set +e
    echo "Sampling PID $APP_PID for ${seconds}s at ${interval_ms}ms intervals..."
    sample "$APP_PID" "$seconds" "$interval_ms" -mayDie -file "$output"
    SAMPLE_STATUS=$?
    if kill -0 "$APP_PID" 2>/dev/null; then
        kill -INT "$APP_PID" 2>/dev/null || true
    fi
    wait "$APP_PID" 2>/dev/null || true
    set -e
    echo "Done: $output"
    exit "$SAMPLE_STATUS"
}

run_macos_dtrace() {
    local output="dtrace-syscalls.log"
    local seconds="${PROFILE_SECONDS:-10}"

    if ! command -v dtrace &> /dev/null; then
        echo "Error: macOS dtrace tool not found"
        exit 1
    fi

    build_profile_binary
    echo "Profiling: $BINARY ${RUN_ARGS[*]} ${EXTRA_ARGS[*]}"
    "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" &
    APP_PID=$!
    sleep 0.2

    echo "Recording syscall latency for ${seconds}s with dtrace..."
    echo "This may require sudo and may be limited by SIP on macOS."
    sudo -v
    set +e
    sudo dtrace -q -p "$APP_PID" -n '
        syscall:::entry /pid == $target/ {
            self->start = timestamp;
            self->name = probefunc;
        }
        syscall:::return /self->start/ {
            @latency_ms[self->name] = quantize((timestamp - self->start) / 1000000);
            @count[self->name] = count();
            self->start = 0;
            self->name = 0;
        }' > "$output" &
    DTRACE_PID=$!
    sleep "$seconds"
    kill -INT "$DTRACE_PID" 2>/dev/null || true
    wait "$DTRACE_PID" 2>/dev/null
    DTRACE_STATUS=$?
    if kill -0 "$APP_PID" 2>/dev/null; then
        kill -INT "$APP_PID" 2>/dev/null || true
    fi
    wait "$APP_PID" 2>/dev/null || true
    set -e

    echo "Done: $output"
    exit "$DTRACE_STATUS"
}

case "$PLATFORM:$MODE" in
  Darwin:sample)
    run_macos_sample
    ;;

  Darwin:dtrace)
    run_macos_dtrace
    ;;

  Darwin:*)
    echo "Error: mode '$MODE' is not supported on macOS. Use sample or dtrace."
    exit 1
    ;;

  Linux:strace)
    build_profile_binary

    echo "Recording syscall latency with strace..."
    echo "Stop nntpbench to generate report."
    echo ""

    # -T: show time spent in syscall
    # -f: follow forks
    # -tt: microsecond timestamps
    # -e: trace I/O and network syscalls
    strace -T -f -tt \
      -e read,write,recvfrom,sendto,poll,epoll_wait,epoll_ctl,pselect6,open,openat,close,pread64,pwrite64,io_uring_enter \
      -o strace.log \
      "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" || true

    echo ""
    echo "=== Syscall Summary ==="
    echo ""

    echo "Top syscalls by total time:"
    grep -oP '<[0-9.]+>' strace.log 2>/dev/null | tr -d '<>' | \
      awk '{sum+=$1; count++} END {if(count>0) printf "Total: %.3fs across %d calls (avg %.3fms)\n", sum, count, (sum/count)*1000}' || echo "(no data)"

    echo ""
    echo "Breakdown by syscall type:"
    for syscall in read write recvfrom sendto poll epoll_wait epoll_ctl pselect6 open openat close pread64 pwrite64 io_uring_enter; do
      if grep -q "^[0-9].*$syscall(" strace.log 2>/dev/null; then
        grep "$syscall(" strace.log 2>/dev/null | grep -oP '<[0-9.]+>' | tr -d '<>' | \
          awk -v name="$syscall" '{sum+=$1; count++} END {if(count>0) printf "  %-18s: %.3fs total, %6d calls, avg %.3fms\n", name, sum, count, (sum/count)*1000}'
      fi
    done

    echo ""
    echo "Slowest individual syscalls (>1ms):"
    grep -oP '^[0-9]+\s+[0-9:.]+\s+\S+\(.*<[0-9.]+>' strace.log 2>/dev/null | \
      awk -F'<' '{time=$2; gsub(/>.*/, "", time); if(time+0 > 0.001) print time, $1}' | \
      sort -rn | head -20 || echo "(no data)"

    echo ""
    echo "Full logs: strace.log"
    ;;

  Linux:offcpu)
    echo 0 | sudo tee /proc/sys/kernel/kptr_restrict > /dev/null
    echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid > /dev/null
    sudo chmod -R a+rx /sys/kernel/tracing 2>/dev/null || true
    sudo chmod -R a+rx /sys/kernel/debug/tracing 2>/dev/null || true
    build_profile_binary

    echo "Recording off-CPU time (what we're waiting on)..."
    echo "Stop nntpbench to generate flamegraph."
    echo ""

    OFFCPU_METHOD=""

    if perf record -e sched:sched_switch -a -- sleep 0.01 2>/dev/null; then
      rm -f perf.data
      echo "Using perf sched:sched_switch..."
      OFFCPU_METHOD="perf-sched"
    elif perf record -e cpu-clock -a -- sleep 0.01 2>/dev/null; then
      rm -f perf.data
      echo "Using perf cpu-clock (less accurate, shows on-CPU not off-CPU)..."
      OFFCPU_METHOD="perf-cpu"
    else
      echo "Error: No off-CPU profiling method available"
      echo ""
      echo "Try fixing perf permissions:"
      echo "  sudo sh -c 'echo 0 > /proc/sys/kernel/perf_event_paranoid'"
      echo "  sudo chmod -R a+rx /sys/kernel/tracing"
      exit 1
    fi

    set +e
    case "$OFFCPU_METHOD" in
      perf-sched)
        "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" &
        APP_PID=$!
        sleep 0.5
        perf sched record -p $APP_PID -o perf-offcpu.data
        wait $APP_PID || true
        ;;

      perf-cpu)
        "$BINARY" "${RUN_ARGS[@]}" "${EXTRA_ARGS[@]}" &
        APP_PID=$!
        sleep 0.5
        perf record -p $APP_PID -e cpu-clock -g --call-graph fp -F 997 -o perf-offcpu.data
        wait $APP_PID || true
        ;;
    esac
    set -e

    echo ""
    echo "Generating reports..."

    if [ -f perf-offcpu.data ]; then
      if [ "$OFFCPU_METHOD" = "perf-sched" ]; then
        echo ""
        echo "=== Scheduler Latency Summary ==="
        echo ""
        echo "Top threads by scheduling latency (wait time before running):"
        perf sched timehist -i perf-offcpu.data 2>&1 | \
          awk 'NR>2 {print $5 " " $4}' | grep -v '^$' | sort -rn | head -30 || true
        echo ""
        echo "For detailed analysis:"
        echo "  perf sched timehist -i perf-offcpu.data | less"
      else
        if command -v inferno-collapse-perf &> /dev/null; then
          perf script -i perf-offcpu.data 2>/dev/null | \
            inferno-collapse-perf | \
            inferno-flamegraph --title "Off-CPU Time" > flamegraph-offcpu.svg
          echo "Done: flamegraph-offcpu.svg"
        else
          echo "inferno not found, skipping flamegraph generation"
          echo "Install with: cargo install inferno"
        fi
      fi
    else
      echo "Warning: Could not generate perf data"
    fi

    echo ""
    echo "Output files: perf-offcpu.data (and flamegraph-offcpu.svg if generated)"
    ;;

  *)
    echo "Error: mode '$MODE' is not supported on $PLATFORM"
    exit 1
    ;;
esac
