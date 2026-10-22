# nntpbench

Small async mock NNTP server and client for throughput, latency, and profiling work.

The current focus is hot-path throughput and protocol/client work, with the longer-term goal of growing this into a more fully featured NNTP implementation.

## CLI

Run the mock server:

```bash
nix develop -c cargo run -- server --listen 127.0.0.1:2119
```

The server generates deterministic yEnc-encoded binary payloads by default.
`--body-bytes` and `--article-bytes` set the decoded payload size in this mode;
encoded responses include yEnc headers, escaping, checksums, and NNTP framing.
Payloads are encoded once at startup and reused during transfers.

Use `--payload-mode plain` for the repeated-text payload. In plain mode the
size options retain their approximate response wire-size meaning.

Send one request and print the raw NNTP response. `client` is the supported
request/future client command; `fetch` remains as a compatibility alias for the
same one-request path. Extension requests use the client capability preflight
before sending the selected command.

```bash
nix develop -c cargo run -- client --connect 127.0.0.1:2119 --request article --message-id '<article.1@nntpbench.local>'
nix develop -c cargo run -- fetch --connect 127.0.0.1:2119 --request body --selector 1
```

For repeatable client throughput work, use the Divan benchmarks:

```bash
nix develop -c cargo bench --bench client_roundtrip
```

For the current end-to-end throughput snapshot, run the direct benchmark on a
native build:

```bash
BUILD=0 RUNS=10 ./scripts/direct-e2e-bench.sh
```

On macOS, the CLI requests user-initiated QoS for the runtime's calling thread
and worker threads. This gives benchmark work a consistent requested priority
on Apple Silicon; macOS controls which cores execute each thread.

`Client::connect` defaults to `ArticleValidation::FramingOnly`: typed article
and body calls check framing and the expected success status without scanning
all headers or body lines. Set `ClientOptions::article_validation` to
`ArticleValidation::Full` to validate before each typed response is returned.
Raw request methods always return framed responses.

`OwnedArticle::article()` now returns `Result<Article, ArticleParseError>` and
validates when a parsed view is requested. `OwnedArticle::validate()` and
`OwnedResponse::into_article()` retain a validated layout for repeated access;
`as_bytes()` and `content()` do not trigger semantic validation.

The public-client TCP benchmarks use normal typed calls with the default policy.
To include the full validation scan explicitly:

```bash
cargo bench --bench client_roundtrip -- --ignored article_validated
```

## Response ownership

The public client's `BufferedResponseReceiver::receive` owns pending input and
exclusively controls its request-scoped decoder. It extracts and freezes exactly
one framed prefix, retains packed following input, and validates generic
response semantics. Article semantics are validated only when an article is
promoted or accessed as an article view; framing alone does not certify article
content. Cancellation after polling or a decode/read error makes the receiver
unavailable; dropping an unpolled receive does not consume it.

`protocol::response_receiver` is the trusted extraction/ownership boundary.
Its private `ChunkConsumed` counts bytes from the latest scanner push;
`FrameEnd` is an exclusive position from the accumulated response's start.
Coordinate translation and split/freeze remain inside this boundary; callers do
not reconstruct them from scanner results.

Article layouts remain private and associated with immutable bytes. Full
validation retains parsed metadata and transformation requirements; subsequent
plain-body access borrows without semantic revalidation or a discovery scan.
Framing-only responses validate on each parsed access until explicitly promoted.

The equivalent proxy operation borrows a pooled window and streams it; it does
not need whole-article ownership. Mutable operations consume their permission;
immutable validated views remain reusable in both designs.

The ownership contracts are exercised by the compile-fail doctests and the
fragmentation/ownership tests in the normal Rust test suite.

## Profiling

The profiling binaries are built with native CPU and frame pointers:

```bash
RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes' \
  nix develop -c cargo build --profile profiling --bin nntpbench
```

The profiling scripts choose native tooling by platform:

- `./scripts/profile.sh` uses Linux `perf`/Inferno and writes
  `target/profiling/profile/flamegraph.svg`; on macOS it uses `sample` and
  writes `target/profiling/profile/sample.txt`.
- `./scripts/profile-latency.sh` uses Linux `strace` or `perf` off-CPU mode;
  on macOS it defaults to `sample` and also has a `dtrace` syscall-latency
  mode when DTrace is permitted by the host.
- `./scripts/profile-mem.sh` uses Linux `heaptrack` or Valgrind Massif; on
  macOS it runs with `MallocStackLogging` and captures a live `leaks` report.

On macOS, set `PROFILE_SECONDS=<seconds>` to control the sampling window.
