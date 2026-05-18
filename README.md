# nntpbench

Small async mock NNTP server and client for throughput, latency, and profiling work.

The current focus is hot-path throughput and typed protocol/client work, with the longer-term goal of growing this into a more fully featured NNTP implementation.

## Latest direct benchmark snapshot

Direct `nntpbench client -> nntpbench server` run with no proxy in the middle.

Shape:

- server: `4` Tokio threads, `max-pipeline-depth=256`, `body-bytes=786432`, `article-bytes=786432`, `pending-write-bytes=819200`
- client: `16` connections, `8` Tokio threads, `pipeline-depth=128`, `command-mix=article`
- workload: `129203` requests, `101609890508` response bytes, about `100GB`
- runs: `10`

Results:

| metric | value |
| --- | ---: |
| mean throughput | `20148.33 MiB/s` |
| median throughput | `20114.88 MiB/s` |
| std dev | `1351.44 MiB/s` |
| min | `18242.37 MiB/s` |
| max | `22728.69 MiB/s` |
| mean throughput (GiB/s) | `19.676 GiB/s` |
| mean throughput (GB/s) | `21.127 GB/s` |
| mean elapsed | `4.828573 s` |

Command shape:

```bash
RUNS=10 ./scripts/direct-e2e-bench.sh
```

The script defaults both `BODY_BYTES` and `ARTICLE_BYTES` to `786432` bytes
(`768 KiB`), uses `PENDING_WRITE_BYTES=819200` (`800 KiB`), and runs
`COMMAND_MIX=article` unless overridden.

## Profiling

The profiling binaries are built with native CPU and frame pointers:

```bash
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  cargo build --profile profiling --bin nntpbench
```
