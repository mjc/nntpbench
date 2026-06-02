# nntpbench

Small async mock NNTP server and client for throughput, latency, and profiling work.

The current focus is hot-path throughput and protocol/client work, with the longer-term goal of growing this into a more fully featured NNTP implementation.

## Latest direct benchmark single-run snapshot

Single direct `nntpbench client -> nntpbench server` run with no proxy in the
middle. Snapshot from macOS on 2026-05-19, after the generated-response hot-path
change. This records one representative run; the benchmark script defaults to
10 runs for repeatable local comparisons.

Shape:

- server: `4` Tokio threads, `max-pipeline-depth=256`, `body-bytes=786432`, `article-bytes=786432`, `pending-write-bytes=819200`
- client: `16` connections, `4` Tokio threads, `pipeline-depth=128`, `command-mix=article`
- socket buffers: Darwin default from this repo, `1 MiB` send and receive buffers
- workload: `129203` requests, `101609890508` response bytes, about `100GB`
- runs: `1`

Results:

| metric | value |
| --- | ---: |
| elapsed | `13.625552 s` |
| throughput | `7111.84 MiB/s` |
| throughput (GiB/s) | `6.945 GiB/s` |
| throughput (GB/s) | `7.457 GB/s` |

The same 100GB shape on `main` was `18.052482 s`, about `5282.84 MiB/s`, when
run with Darwin-safe OS-managed socket buffers. A pre-fix run on this branch with
the same OS-managed socket buffers was `18.328546 s`, about `5286.98 MiB/s`.

Command shape:

```bash
RUNS=1 SERVER_THREADS=4 CLIENT_THREADS=4 ./scripts/direct-e2e-bench.sh
```

The script defaults both `BODY_BYTES` and `ARTICLE_BYTES` to `786432` bytes
(`768 KiB`), uses `PENDING_WRITE_BYTES=819200` (`800 KiB`), and runs
`COMMAND_MIX=article` unless overridden.
On macOS it defaults socket send/receive buffers to `1 MiB`, below the local
Darwin `kern.ipc.maxsockbuf` cap and faster than larger buffers for this loopback
shape; Linux keeps the `16 MiB` high-throughput default. Override with
`SOCKET_RECV_BUFFER` and `SOCKET_SEND_BUFFER`, or the `SERVER_*` / `CLIENT_*`
variants, when needed.

Darwin socket-buffer notes from this host:

- `kern.ipc.maxsockbuf` reports `8388608`, so accepted TCP socket-buffer requests
  are effectively capped at `8 MiB`.
- `net.inet.tcp.sendspace` and `net.inet.tcp.recvspace` report `131072`.
- For this 16-connection loopback workload, larger buffers were not better:
  `1 MiB` beat OS defaults, `512 KiB`, `2 MiB`, and `4 MiB` in the sampled
  100GB runs.

The server generated-response path now prebuilds synthetic ARTICLE/BODY frames
once per `ServerConfig`. That removes the per-request response construction and
large repeated copies that showed up as `_platform_memmove` in macOS sampling.

## Profiling

The profiling binaries are built with native CPU and frame pointers:

```bash
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  cargo build --profile profiling --bin nntpbench
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
