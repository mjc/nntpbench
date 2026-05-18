# nntpbench

Small async mock NNTP server and client for throughput, latency, and profiling work.

The current focus is hot-path throughput and typed protocol/client work, with the longer-term goal of growing this into a more fully featured NNTP implementation.

## Latest direct benchmark snapshot

Direct `nntpbench client -> nntpbench server` run with no proxy in the middle.

Shape:

- server: `4` Tokio threads, `max-pipeline-depth=256`, `body-bytes=786432`, `article-bytes=786432`
- client: `16` connections, `8` Tokio threads, `pipeline-depth=128`, `command-mix=body`
- workload: `129204` requests, `101610418536` response bytes, about `100GB`
- runs: `10`

Results:

| metric | value |
| --- | ---: |
| mean throughput | `11780.62 MiB/s` |
| std dev | `75.85 MiB/s` |
| min | `11662.06 MiB/s` |
| max | `11865.78 MiB/s` |
| mean throughput (GiB/s) | `11.505 GiB/s` |
| mean throughput (GB/s) | `12.353 GB/s` |
| mean elapsed | `8.225994 s` |

Command shape:

```bash
COMMAND_MIX=body ./scripts/direct-e2e-bench.sh
```

The script defaults both `BODY_BYTES` and `ARTICLE_BYTES` to `786432` bytes
(`768 KiB`) and uses `PENDING_WRITE_BYTES=819200` (`800 KiB`).

## Profiling

The profiling binaries are built with native CPU and frame pointers:

```bash
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  cargo build --profile profiling --bin nntpbench
```
