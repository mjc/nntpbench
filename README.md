# nntpbench

Small async mock NNTP server and client for throughput, latency, and profiling work.

The current focus is hot-path throughput and protocol/client work, with the longer-term goal of growing this into a more fully featured NNTP implementation.

For the current scope boundary, see [docs/protocol-scope.md](docs/protocol-scope.md).

## CLI

Run the mock server:

```bash
nix develop -c cargo run -- server --listen 127.0.0.1:2119
```

Send one request and print the raw NNTP response. `client` is the supported
request/future client command; `fetch` remains as a compatibility alias for the
same one-request path. Extension requests use the client capability preflight
before sending the selected command.

```bash
nix develop -c cargo run -- client --connect 127.0.0.1:2119 --request article --message-id '<article.1@nntpbench.local>'
nix develop -c cargo run -- fetch --connect 127.0.0.1:2119 --request body --selector 1
```

For repeatable client throughput work, use the criterion benchmarks:

```bash
nix develop -c cargo bench --bench client_roundtrip
```

For the current end-to-end throughput snapshot, run the direct benchmark on a
native build:

```bash
BUILD=0 RUNS=10 ./scripts/direct-e2e-bench.sh
```

The script keeps the per-run CSV in
`target/profiling/direct-e2e-bench/<case>/raw.csv` and appends one summary row
per case to `target/profiling/direct-e2e-bench/summary.csv`. It also writes a
JSON summary to `target/profiling/direct-e2e-bench/summary.json`. To sweep a
small matrix, set comma-separated values such as
`PENDING_WRITE_BYTES_VALUES=65536,131072` or
`CONNECTIONS_VALUES=8,16,32`.

For connection churn, add the `--churn` flag. That switches the existing
benchmark into a one-request-per-connection mode and adds connections/sec to
the summary output. Set `CHURN_TOTAL_CONNECTIONS=<n>` if you want the run to
open and close more connections than the per-batch concurrency value:

```bash
CHURN_TOTAL_CONNECTIONS=256 BUILD=0 RUNS=10 ./scripts/direct-e2e-bench.sh --churn
```

The direct benchmark also pins the server and client to separate CPUs on Linux
when `taskset` is available. Override that with `SERVER_TASKSET=` or
`CLIENT_TASKSET=` if you want to change or disable pinning.

On this machine, the latest 10-run sweep looked like this:

- 128,509 requests on average, ranging from 128,384 to 128,704
- 101.06 GB transferred on average, ranging from 100.97 GB to 101.22 GB
- 4.81 s elapsed on average, ranging from 4.00 s to 5.89 s
- 9.73 CPU s on average, ranging from 7.27 s to 13.51 s
- 3,126 KiB RSS on average, ranging from 3,096 KiB to 3,164 KiB
- about 21.0 GB/s average throughput by bytes over elapsed time

## Profiling

The profiling binaries are built with native CPU and frame pointers:

```bash
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  nix develop -c cargo build --profile profiling --bin nntpbench
```

The profiling scripts choose native tooling by platform:

- `./scripts/profile-coz.sh` builds the client benchmark with `coz`
  markers, runs it under `coz`, and leaves report generation to `coz plot`.
- `./scripts/profile.sh` uses Linux `perf`/Inferno and writes
  `target/profiling/profile/flamegraph.svg`; on macOS it uses `sample` and
  writes `target/profiling/profile/sample.txt`.
- `./scripts/profile-latency.sh` uses Linux `strace` or `perf` off-CPU mode;
  on macOS it defaults to `sample` and also has a `dtrace` syscall-latency
  mode when DTrace is permitted by the host. Outputs land in
  `target/profiling/profile-latency/`.
- `./scripts/profile-mem.sh` uses Linux `heaptrack` or Valgrind Massif; on
  macOS it runs with `MallocStackLogging` and captures a live `leaks` report.
  Outputs land in `target/profiling/profile-mem/`.

The profiling scripts accept `OUTPUT_DIR=<path>` if you want to redirect the
artifacts while keeping the build step on the native `nix develop` toolchain.

For the Tokio fixed-worker exploration notes, see
[docs/fixed-worker-evaluation.md](docs/fixed-worker-evaluation.md).

On macOS, set `PROFILE_SECONDS=<seconds>` to control the sampling window.
