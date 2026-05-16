# nntpbench

Small async mock NNTP server and client for throughput, latency, and profiling work.

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
./target/release/nntpbench server \
  --listen 127.0.0.1:21211 \
  --body-bytes 786432 \
  --article-bytes 786432 \
  --threads 4 \
  --max-pipeline-depth 256

./target/release/nntpbench client \
  --connect 127.0.0.1:21211 \
  --transfer-bytes 100000000000 \
  --connections 16 \
  --threads 8 \
  --pipeline-depth 128 \
  --command-mix body \
  --stats-interval-secs 0 \
  --csv
```

Profiles from earlier direct localhost runs:

- [server flamegraph](./profiles/nntpbench-direct-server-run3.svg)
- [client flamegraph](./profiles/nntpbench-direct-client-run7.svg)

## Profiling

The profiling binaries are built with native CPU and frame pointers:

```bash
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  cargo build --profile profiling --bin nntpbench
```
