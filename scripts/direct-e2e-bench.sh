#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}"
export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}"

PINNING_SUPPORTED=0
if [[ "$(uname -s)" == "Linux" ]] && command -v taskset >/dev/null 2>&1; then
    PINNING_SUPPORTED=1
fi

DEFAULT_SERVER_TASKSET=""
DEFAULT_CLIENT_TASKSET=""
if [[ "$PINNING_SUPPORTED" -eq 1 ]]; then
    AVAILABLE_CPUS="$(nproc 2>/dev/null || echo 1)"
    if [[ "$AVAILABLE_CPUS" -ge 2 ]]; then
        DEFAULT_SERVER_TASKSET="0"
        DEFAULT_CLIENT_TASKSET="1"
    elif [[ "$AVAILABLE_CPUS" -eq 1 ]]; then
        DEFAULT_SERVER_TASKSET="0"
    fi
fi

SERVER_TASKSET="${SERVER_TASKSET:-$DEFAULT_SERVER_TASKSET}"
CLIENT_TASKSET="${CLIENT_TASKSET:-$DEFAULT_CLIENT_TASKSET}"

run_with_optional_taskset() {
    local cpu_list="$1"
    shift

    if [[ -n "$cpu_list" && "$PINNING_SUPPORTED" -eq 1 ]]; then
        taskset -c "$cpu_list" "$@"
    else
        "$@"
    fi
}

CHURN_MODE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --churn)
            CHURN_MODE=1
            ;;
        -h|--help)
            cat <<'EOF'
Usage: ./scripts/direct-e2e-bench.sh [--churn]

Options:
  --churn   run a one-request-per-connection workload and report connections/sec

Environment:
  SERVER_TASKSET   optional CPU list for the server process, default 0 on Linux with >=2 CPUs
  CLIENT_TASKSET   optional CPU list for the client process, default 1 on Linux with >=2 CPUs
EOF
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            exit 1
            ;;
    esac
    shift
done

LISTEN="${LISTEN:-127.0.0.1:21211}"
BODY_BYTES="${BODY_BYTES:-786432}"
ARTICLE_BYTES="${ARTICLE_BYTES:-786432}"
PENDING_WRITE_BYTES="${PENDING_WRITE_BYTES:-65536}"
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
CHURN_TOTAL_CONNECTIONS="${CHURN_TOTAL_CONNECTIONS:-$CONNECTIONS}"
CLIENT_THREADS="${CLIENT_THREADS:-8}"
PIPELINE_DEPTH="${PIPELINE_DEPTH:-128}"
COMMAND_MIX="${COMMAND_MIX:-article}"
RUNS="${RUNS:-10}"
CLIENT_READ_BUFFER_BYTES="${CLIENT_READ_BUFFER_BYTES:-262144}"
CLIENT_SOCKET_RECV_BUFFER="${CLIENT_SOCKET_RECV_BUFFER:-${SOCKET_RECV_BUFFER:-$DEFAULT_SOCKET_RECV_BUFFER}}"
CLIENT_SOCKET_SEND_BUFFER="${CLIENT_SOCKET_SEND_BUFFER:-${SOCKET_SEND_BUFFER:-$DEFAULT_SOCKET_SEND_BUFFER}}"
PENDING_WRITE_BYTES_VALUES="${PENDING_WRITE_BYTES_VALUES:-$PENDING_WRITE_BYTES}"
CONNECTIONS_VALUES="${CONNECTIONS_VALUES:-$CONNECTIONS}"
PIPELINE_DEPTH_VALUES="${PIPELINE_DEPTH_VALUES:-$PIPELINE_DEPTH}"
COMMAND_MIX_VALUES="${COMMAND_MIX_VALUES:-$COMMAND_MIX}"
RESULTS_DIR="${RESULTS_DIR:-$TARGET_DIR/profiling/direct-e2e-bench}"

IFS=, read -r -a PENDING_WRITE_BYTES_SET <<< "$PENDING_WRITE_BYTES_VALUES"
IFS=, read -r -a CONNECTIONS_SET <<< "$CONNECTIONS_VALUES"
IFS=, read -r -a PIPELINE_DEPTH_SET <<< "$PIPELINE_DEPTH_VALUES"
IFS=, read -r -a COMMAND_MIX_SET <<< "$COMMAND_MIX_VALUES"

mkdir -p "$RESULTS_DIR"

SUMMARY_CSV="$RESULTS_DIR/summary.csv"
SUMMARY_JSONL="$RESULTS_DIR/summary.jsonl"
SUMMARY_JSON="$RESULTS_DIR/summary.json"
SUMMARY_HEADER='pending_write_bytes,connections,pipeline_depth,command_mix,mode,runs,throughput_gib_s_mean,throughput_gib_s_median,throughput_gib_s_min,throughput_gib_s_max,throughput_gib_s_stddev,throughput_gib_s_cv,elapsed_s_mean,elapsed_s_median,elapsed_s_min,elapsed_s_max,elapsed_s_stddev,elapsed_s_cv,cpu_s_mean,cpu_s_median,cpu_s_min,cpu_s_max,cpu_s_stddev,cpu_s_cv,rss_kib_mean,rss_kib_median,rss_kib_min,rss_kib_max,rss_kib_stddev,rss_kib_cv'
if [[ "$CHURN_MODE" -eq 1 ]]; then
    SUMMARY_HEADER='pending_write_bytes,total_connections,connections,pipeline_depth,command_mix,mode,runs,throughput_gib_s_mean,throughput_gib_s_median,throughput_gib_s_min,throughput_gib_s_max,throughput_gib_s_stddev,throughput_gib_s_cv,connections_per_s_mean,connections_per_s_median,connections_per_s_min,connections_per_s_max,connections_per_s_stddev,connections_per_s_cv,elapsed_s_mean,elapsed_s_median,elapsed_s_min,elapsed_s_max,elapsed_s_stddev,elapsed_s_cv,cpu_s_mean,cpu_s_median,cpu_s_min,cpu_s_max,cpu_s_stddev,cpu_s_cv,rss_kib_mean,rss_kib_median,rss_kib_min,rss_kib_max,rss_kib_stddev,rss_kib_cv'
fi
if [[ ! -f "$SUMMARY_CSV" || "$(sed -n '1p' "$SUMMARY_CSV" 2>/dev/null)" != "$SUMMARY_HEADER" ]]; then
    printf '%s\n' "$SUMMARY_HEADER" >"$SUMMARY_CSV"
fi
: >"$SUMMARY_JSONL"

json_escape() {
    printf '%s' "$1" | awk 'BEGIN { ORS=""; } {
        gsub(/\\/,"\\\\");
        gsub(/"/,"\\\"");
        gsub(/\t/,"\\t");
        gsub(/\r/,"\\r");
        print;
    }'
}

slugify_case() {
    printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//; s/-{2,}/-/g'
}

series_stats() {
    local values_file="$1"
    local count median mean stdev min max lower upper

    count=$(wc -l <"$values_file" | tr -d ' ')
    if [[ "$count" -eq 0 ]]; then
        printf '0 0 0 0 0\n'
        return
    fi

    mean=$(awk '{sum += $1} END { if (NR) printf "%.6f", sum / NR; else printf "0.000000"; }' "$values_file")
    stdev=$(awk -v mean="$mean" '{sum += ($1 - mean) ^ 2} END { if (NR > 1) printf "%.6f", sqrt(sum / (NR - 1)); else printf "0.000000"; }' "$values_file")
    cv=$(awk -v mean="$mean" -v stdev="$stdev" 'BEGIN { if (mean != 0) printf "%.6f", stdev / mean; else printf "0.000000"; }')

    local sorted_file
    sorted_file=$(mktemp)
    sort -n "$values_file" >"$sorted_file"
    min=$(sed -n '1p' "$sorted_file")
    max=$(sed -n '$p' "$sorted_file")

    if (( count % 2 == 1 )); then
        median=$(sed -n "$(((count / 2) + 1))p" "$sorted_file")
    else
        lower=$(sed -n "$((count / 2))p" "$sorted_file")
        upper=$(sed -n "$(((count / 2) + 1))p" "$sorted_file")
        median=$(awk -v a="$lower" -v b="$upper" 'BEGIN { printf "%.6f", (a + b) / 2 }')
    fi

    rm -f "$sorted_file"
    printf '%s %s %s %s %s %s\n' "$mean" "$median" "$min" "$max" "$stdev" "$cv"
}

summarize_case() {
    local raw_csv="$1"
    local pending_write_bytes="$2"
    local total_connections="$3"
    local connections="$4"
    local pipeline_depth="$5"
    local command_mix="$6"
    local case_name="$7"
    local mode="$8"
    local churn_mode="$9"

    local throughput_file elapsed_file cpu_file rss_file
    throughput_file=$(mktemp)
    elapsed_file=$(mktemp)
    cpu_file=$(mktemp)
    rss_file=$(mktemp)
    local connections_per_s_file

    awk -F, 'NF >= 5 && $3 > 0 { printf "%.6f\n", ($2 / $3) / (1024 * 1024 * 1024) }' "$raw_csv" >"$throughput_file"
    awk -F, 'NF >= 5 { print $3 }' "$raw_csv" >"$elapsed_file"
    awk -F, 'NF >= 5 { print $4 }' "$raw_csv" >"$cpu_file"
    awk -F, 'NF >= 5 { print $5 }' "$raw_csv" >"$rss_file"
    if [[ "$churn_mode" -eq 1 ]]; then
        connections_per_s_file=$(mktemp)
        awk -F, 'NF >= 5 && $3 > 0 { printf "%.6f\n", $1 / $3 }' "$raw_csv" >"$connections_per_s_file"
    fi

    printf '  raw_csv=%s\n' "$raw_csv"
    read -r throughput_mean throughput_median throughput_min throughput_max throughput_stdev throughput_cv < <(series_stats "$throughput_file")
    if [[ "$churn_mode" -eq 1 ]]; then
        read -r connections_per_s_mean connections_per_s_median connections_per_s_min connections_per_s_max connections_per_s_stdev connections_per_s_cv < <(series_stats "$connections_per_s_file")
    fi
    read -r elapsed_mean elapsed_median elapsed_min elapsed_max elapsed_stdev elapsed_cv < <(series_stats "$elapsed_file")
    read -r cpu_mean cpu_median cpu_min cpu_max cpu_stdev cpu_cv < <(series_stats "$cpu_file")
    read -r rss_mean rss_median rss_min rss_max rss_stdev rss_cv < <(series_stats "$rss_file")

    if [[ "$churn_mode" -eq 1 ]]; then
        printf '\ncase=%s pending_write_bytes=%s total_connections=%s connections=%s pipeline_depth=%s command_mix=%s mode=%s runs=%s\n' \
            "$case_name" "$pending_write_bytes" "$total_connections" "$connections" "$pipeline_depth" "$command_mix" "$mode" "$RUNS"
    else
        printf '\ncase=%s pending_write_bytes=%s connections=%s pipeline_depth=%s command_mix=%s mode=%s runs=%s\n' \
            "$case_name" "$pending_write_bytes" "$connections" "$pipeline_depth" "$command_mix" "$mode" "$RUNS"
    fi
    printf '  throughput_gib_s: mean=%s median=%s min=%s max=%s stdev=%s\n' \
        "$throughput_mean" "$throughput_median" "$throughput_min" "$throughput_max" "$throughput_stdev"
    printf '  throughput_gib_s: best=%s worst=%s\n' "$throughput_max" "$throughput_min"
    printf '  throughput_gib_s: cv=%s\n' "$throughput_cv"
    if [[ "$churn_mode" -eq 1 ]]; then
        printf '  connections_per_s: mean=%s median=%s min=%s max=%s stdev=%s\n' \
            "$connections_per_s_mean" "$connections_per_s_median" "$connections_per_s_min" "$connections_per_s_max" "$connections_per_s_stdev"
        printf '  connections_per_s: best=%s worst=%s\n' "$connections_per_s_max" "$connections_per_s_min"
        printf '  connections_per_s: cv=%s\n' "$connections_per_s_cv"
    fi
    printf '  elapsed_s:        mean=%s median=%s min=%s max=%s stdev=%s\n' \
        "$elapsed_mean" "$elapsed_median" "$elapsed_min" "$elapsed_max" "$elapsed_stdev"
    printf '  elapsed_s:        best=%s worst=%s\n' "$elapsed_min" "$elapsed_max"
    printf '  elapsed_s:        cv=%s\n' "$elapsed_cv"
    printf '  cpu_s:            mean=%s median=%s min=%s max=%s stdev=%s\n' \
        "$cpu_mean" "$cpu_median" "$cpu_min" "$cpu_max" "$cpu_stdev"
    printf '  cpu_s:            best=%s worst=%s\n' "$cpu_min" "$cpu_max"
    printf '  cpu_s:            cv=%s\n' "$cpu_cv"
    printf '  rss_kib:          mean=%s median=%s min=%s max=%s stdev=%s\n' \
        "$rss_mean" "$rss_median" "$rss_min" "$rss_max" "$rss_stdev"
    printf '  rss_kib:          best=%s worst=%s\n' "$rss_min" "$rss_max"
    printf '  rss_kib:          cv=%s\n' "$rss_cv"

    if [[ "$churn_mode" -eq 1 ]]; then
        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$pending_write_bytes" "$total_connections" "$connections" "$pipeline_depth" "$command_mix" "$mode" "$RUNS" \
            "$throughput_mean" "$throughput_median" "$throughput_min" "$throughput_max" "$throughput_stdev" "$throughput_cv" \
            "$connections_per_s_mean" "$connections_per_s_median" "$connections_per_s_min" "$connections_per_s_max" "$connections_per_s_stdev" "$connections_per_s_cv" \
            "$elapsed_mean" "$elapsed_median" "$elapsed_min" "$elapsed_max" "$elapsed_stdev" "$elapsed_cv" \
            "$cpu_mean" "$cpu_median" "$cpu_min" "$cpu_max" "$cpu_stdev" "$cpu_cv" \
            "$rss_mean" "$rss_median" "$rss_min" "$rss_max" "$rss_stdev" "$rss_cv" >>"$SUMMARY_CSV"
    else
        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$pending_write_bytes" "$connections" "$pipeline_depth" "$command_mix" "$mode" "$RUNS" \
            "$throughput_mean" "$throughput_median" "$throughput_min" "$throughput_max" "$throughput_stdev" "$throughput_cv" \
            "$elapsed_mean" "$elapsed_median" "$elapsed_min" "$elapsed_max" "$elapsed_stdev" "$elapsed_cv" \
            "$cpu_mean" "$cpu_median" "$cpu_min" "$cpu_max" "$cpu_stdev" "$cpu_cv" \
            "$rss_mean" "$rss_median" "$rss_min" "$rss_max" "$rss_stdev" "$rss_cv" >>"$SUMMARY_CSV"
    fi
    if [[ "$churn_mode" -eq 1 ]]; then
        printf '{"case_name":"%s","pending_write_bytes":%s,"total_connections":%s,"connections":%s,"pipeline_depth":%s,"command_mix":"%s","mode":"%s","runs":%s,"throughput_gib_s":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s},"connections_per_s":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s},"elapsed_s":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s},"cpu_s":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s},"rss_kib":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s}}\n' \
            "$(json_escape "$case_name")" "$pending_write_bytes" "$total_connections" "$connections" "$pipeline_depth" "$(json_escape "$command_mix")" "$(json_escape "$mode")" "$RUNS" \
            "$throughput_mean" "$throughput_median" "$throughput_max" "$throughput_min" "$throughput_stdev" "$throughput_cv" \
            "$connections_per_s_mean" "$connections_per_s_median" "$connections_per_s_max" "$connections_per_s_min" "$connections_per_s_stdev" "$connections_per_s_cv" \
            "$elapsed_mean" "$elapsed_median" "$elapsed_min" "$elapsed_max" "$elapsed_stdev" "$elapsed_cv" \
            "$cpu_mean" "$cpu_median" "$cpu_min" "$cpu_max" "$cpu_stdev" "$cpu_cv" \
            "$rss_mean" "$rss_median" "$rss_min" "$rss_max" "$rss_stdev" "$rss_cv" >>"$SUMMARY_JSONL"
    else
        printf '{"case_name":"%s","pending_write_bytes":%s,"connections":%s,"pipeline_depth":%s,"command_mix":"%s","mode":"%s","runs":%s,"throughput_gib_s":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s},"elapsed_s":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s},"cpu_s":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s},"rss_kib":{"mean":%s,"median":%s,"best":%s,"worst":%s,"stddev":%s,"cv":%s}}\n' \
            "$(json_escape "$case_name")" "$pending_write_bytes" "$connections" "$pipeline_depth" "$(json_escape "$command_mix")" "$(json_escape "$mode")" "$RUNS" \
            "$throughput_mean" "$throughput_median" "$throughput_max" "$throughput_min" "$throughput_stdev" "$throughput_cv" \
            "$elapsed_mean" "$elapsed_median" "$elapsed_min" "$elapsed_max" "$elapsed_stdev" "$elapsed_cv" \
            "$cpu_mean" "$cpu_median" "$cpu_min" "$cpu_max" "$cpu_stdev" "$cpu_cv" \
            "$rss_mean" "$rss_median" "$rss_min" "$rss_max" "$rss_stdev" "$rss_cv" >>"$SUMMARY_JSONL"
    fi

    rm -f "$throughput_file" "$elapsed_file" "$cpu_file" "$rss_file"
    if [[ "$churn_mode" -eq 1 ]]; then
        rm -f "$connections_per_s_file"
    fi
}

if [[ "${BUILD:-1}" != "0" ]]; then
    nix develop -c cargo build --profile profiling --bin nntpbench
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

for pending_write_bytes in "${PENDING_WRITE_BYTES_SET[@]}"; do
    for connections in "${CONNECTIONS_SET[@]}"; do
        for pipeline_depth in "${PIPELINE_DEPTH_SET[@]}"; do
            for command_mix in "${COMMAND_MIX_SET[@]}"; do
                case_name="$(slugify_case "pw=${pending_write_bytes}-conn=${connections}-pipe=${pipeline_depth}-mix=${command_mix}")"
                mode="hot"
                total_connections="$connections"
                if [[ "$CHURN_MODE" -eq 1 ]]; then
                    mode="churn"
                    total_connections="$CHURN_TOTAL_CONNECTIONS"
                    case_name="$(slugify_case "${case_name}-mode=${mode}")"
                    case_name="$(slugify_case "${case_name}-total=${total_connections}")"
                fi
                case_dir="$RESULTS_DIR/$case_name"
                raw_csv="$case_dir/raw.csv"
                mkdir -p "$case_dir"
                : >"$raw_csv"

                SERVER_LOG="$(mktemp "$TARGET_DIR/direct-e2e-server.XXXXXX.log")"
                run_with_optional_taskset "$SERVER_TASKSET" "$TARGET_DIR/profiling/nntpbench" server \
                    --listen "$LISTEN" \
                    --body-bytes "$BODY_BYTES" \
                    --article-bytes "$ARTICLE_BYTES" \
                    --threads "$SERVER_THREADS" \
                    --max-pipeline-depth "$MAX_PIPELINE_DEPTH" \
                    --pending-write-bytes "$pending_write_bytes" \
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
                    printf 'run=%s body_bytes=%s article_bytes=%s pending_write_bytes=%s total_connections=%s connections=%s pipeline_depth=%s command_mix=%s mode=%s\n' \
                        "$run" "$BODY_BYTES" "$ARTICLE_BYTES" "$pending_write_bytes" "$total_connections" "$connections" "$pipeline_depth" "$command_mix" "$mode"
                    if [[ "$CHURN_MODE" -eq 1 ]]; then
                        remaining_connections="$total_connections"
                        total_commands=0
                        total_bytes=0
                        total_elapsed=0
                        total_cpu=0
                        total_rss=0
                        peak_rss=0
                        while [[ "$remaining_connections" -gt 0 ]]; do
                            batch_connections="$connections"
                            if [[ "$remaining_connections" -lt "$batch_connections" ]]; then
                                batch_connections="$remaining_connections"
                            fi
                            batch_output="$(run_with_optional_taskset "$CLIENT_TASKSET" "$TARGET_DIR/profiling/nntpbench" client \
                                --connect "$LISTEN" \
                                --requests "$batch_connections" \
                                --transfer-bytes 0 \
                                --connections "$batch_connections" \
                                --threads "$CLIENT_THREADS" \
                                --pipeline-depth "$pipeline_depth" \
                                --command-mix "$command_mix" \
                                --read-buffer-bytes "$CLIENT_READ_BUFFER_BYTES" \
                                --socket-recv-buffer "$CLIENT_SOCKET_RECV_BUFFER" \
                                --socket-send-buffer "$CLIENT_SOCKET_SEND_BUFFER" \
                                --stats-interval-secs 0 \
                                --csv)"
                            IFS=, read -r batch_commands batch_bytes batch_elapsed batch_cpu batch_rss <<<"$batch_output"
                            total_commands=$((total_commands + batch_commands))
                            total_bytes=$((total_bytes + batch_bytes))
                            total_elapsed=$(awk -v a="$total_elapsed" -v b="$batch_elapsed" 'BEGIN { printf "%.9f", a + b }')
                            total_cpu=$(awk -v a="$total_cpu" -v b="$batch_cpu" 'BEGIN { printf "%.9f", a + b }')
                            if [[ "$batch_rss" -gt "$peak_rss" ]]; then
                                peak_rss="$batch_rss"
                            fi
                            remaining_connections=$((remaining_connections - batch_connections))
                        done
                        run_output="$total_commands,$total_bytes,$total_elapsed,$total_cpu,$peak_rss"
                    else
                        run_output="$(run_with_optional_taskset "$CLIENT_TASKSET" "$TARGET_DIR/profiling/nntpbench" client \
                            --connect "$LISTEN" \
                            --transfer-bytes "$TRANSFER_BYTES" \
                            --connections "$connections" \
                            --threads "$CLIENT_THREADS" \
                            --pipeline-depth "$pipeline_depth" \
                            --command-mix "$command_mix" \
                            --read-buffer-bytes "$CLIENT_READ_BUFFER_BYTES" \
                            --socket-recv-buffer "$CLIENT_SOCKET_RECV_BUFFER" \
                            --socket-send-buffer "$CLIENT_SOCKET_SEND_BUFFER" \
                            --stats-interval-secs 0 \
                            --csv)"
                    fi
                    printf '%s\n' "$run_output"
                    printf '%s\n' "$run_output" >>"$raw_csv"
                done

                kill -INT "$SERVER_PID" 2>/dev/null || true
                wait "$SERVER_PID" || true
                SERVER_PID=""
                rm -f "$SERVER_LOG"

                summarize_case "$raw_csv" "$pending_write_bytes" "$total_connections" "$connections" "$pipeline_depth" "$command_mix" "$case_name" "$mode" "$CHURN_MODE"
            done
        done
    done
done

{
    printf '{\n'
    printf '  "generated_at": "%s",\n' "$(date -u +%FT%TZ)"
    printf '  "cases": [\n'
    sed '$!s/$/,/' "$SUMMARY_JSONL"
    printf '  ]\n'
    printf '}\n'
} >"$SUMMARY_JSON"
