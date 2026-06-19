# nntpbench

Small async mock NNTP server and client for throughput, latency, and profiling work.

The current focus is hot-path throughput and protocol/client work, with the longer-term goal of growing this into a more fully featured NNTP implementation.

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

## Profiling

The profiling binaries are built with native CPU and frame pointers:

```bash
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  nix develop -c cargo build --profile profiling --bin nntpbench
```

The profiling scripts choose native tooling by platform:

- `./scripts/profile.sh` uses Linux `perf`/Inferno and writes
  `flamegraph.svg`; on macOS it uses `sample` and writes `sample.txt`.
- `./scripts/profile-latency.sh` uses Linux `strace` or `perf` off-CPU mode;
  on macOS it defaults to `sample` and also has a `dtrace` syscall-latency mode
  when DTrace is permitted by the host.
- `./scripts/profile-mem.sh` uses Linux `heaptrack` or Valgrind Massif; on
  macOS it runs with `MallocStackLogging` and captures a live `leaks` report.

On macOS, set `PROFILE_SECONDS=<seconds>` to control the sampling window.
