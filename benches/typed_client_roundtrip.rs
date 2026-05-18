//! End-to-end typed client benchmarks.
//!
//! These benches drive the new request/future client surface against the real
//! mock NNTP server over localhost TCP.
//!
//! Run with: `cargo bench --bench typed_client_roundtrip`

use std::net::SocketAddr;
use std::sync::Arc;

use divan::{Bencher, black_box};
use nntpbench::{
    Client, ServerArgs, ServerConfig, Stats, TypedClientOptions, bind_listener, serve_session,
};
use tokio::runtime::Builder;

fn main() {
    divan::main();
}

const ARTICLE_64K: usize = 64 * 1024;
const BODY_64K: usize = 64 * 1024;

fn server_args(body_bytes: usize, article_bytes: usize) -> ServerArgs {
    ServerArgs {
        listen: "127.0.0.1:0".parse().unwrap(),
        body_bytes,
        article_bytes,
        max_connections: 4096,
        threads: 1,
        max_pipeline_depth: 64,
        backlog: 8192,
        reuse_port: false,
        nodelay: true,
        socket_recv_buffer: 0,
        socket_send_buffer: 0,
        stats_interval_secs: 0,
        flush: false,
        pending_write_bytes: 1024 * 1024,
    }
}

async fn spawn_server(body_bytes: usize, article_bytes: usize) -> SocketAddr {
    let args = server_args(body_bytes, article_bytes);
    let listener = bind_listener(args.listen, args.backlog, args.reuse_port).unwrap();
    let addr = listener.local_addr().unwrap();
    let config = Arc::new(ServerConfig::from_args(args));
    let stats = Arc::new(Stats::new());

    tokio::spawn(async move {
        loop {
            let Ok((stream, peer_addr)) = listener.accept().await else {
                break;
            };
            stream.set_nodelay(true).unwrap();
            let config = config.clone();
            let stats = stats.clone();
            tokio::spawn(async move {
                let _ = serve_session(stream, peer_addr, config, stats).await;
            });
        }
    });

    addr
}

struct TypedHarness {
    rt: tokio::runtime::Runtime,
    client: Client,
}

impl TypedHarness {
    fn start(body_bytes: usize, article_bytes: usize, pipeline_depth: usize) -> Self {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let addr = rt.block_on(spawn_server(body_bytes, article_bytes));
        let client = rt
            .block_on(Client::connect_with_options(
                addr,
                TypedClientOptions {
                    pipeline_depth,
                    ..TypedClientOptions::default()
                },
            ))
            .unwrap();
        Self { rt, client }
    }
}

fn bench_public_article_roundtrip(bencher: Bencher) {
    let harness = TypedHarness::start(BODY_64K, ARTICLE_64K, 1);
    bencher
        .counter(divan::counter::BytesCount::new(ARTICLE_64K))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                harness
                    .client
                    .article("bench.1@nntpbench.local")
                    .await
                    .unwrap()
                    .as_bytes()
                    .len()
            }))
        });
}

fn bench_raw_capabilities_roundtrip(bencher: Bencher) {
    let harness = TypedHarness::start(BODY_64K, ARTICLE_64K, 1);
    bencher
        .counter(divan::counter::ItemsCount::new(1usize))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                harness
                    .client
                    .capabilities()
                    .await
                    .unwrap()
                    .as_bytes()
                    .len()
            }))
        });
}

fn bench_pipelined_body_burst(bencher: Bencher) {
    let harness = TypedHarness::start(BODY_64K, ARTICLE_64K, 4);
    bencher
        .counter(divan::counter::ItemsCount::new(4usize))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                let (a, b, c, d) = tokio::try_join!(
                    harness.client.body("bench.1@nntpbench.local"),
                    harness.client.body("bench.2@nntpbench.local"),
                    harness.client.body("bench.3@nntpbench.local"),
                    harness.client.body("bench.4@nntpbench.local"),
                )
                .unwrap();

                a.as_bytes().len() + b.as_bytes().len() + c.as_bytes().len() + d.as_bytes().len()
            }))
        });
}

fn bench_pipelined_mixed_requests(bencher: Bencher) {
    let harness = TypedHarness::start(BODY_64K, ARTICLE_64K, 4);
    bencher
        .counter(divan::counter::ItemsCount::new(4usize))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                let (body, article, capabilities, date) = tokio::try_join!(
                    harness.client.body("bench.body@nntpbench.local"),
                    harness.client.article("bench.article@nntpbench.local"),
                    harness.client.capabilities(),
                    harness.client.date(),
                )
                .unwrap();

                body.as_bytes().len()
                    + article.as_bytes().len()
                    + capabilities.as_bytes().len()
                    + date.as_bytes().len()
            }))
        });
}

fn bench_pipelined_rfc_grammar_requests(bencher: Bencher) {
    let harness = TypedHarness::start(BODY_64K, ARTICLE_64K, 4);
    bencher
        .counter(divan::counter::ItemsCount::new(4usize))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                let (body, article, over, listgroup) = tokio::try_join!(
                    harness.client.body_selector("42"),
                    harness.client.article_current(),
                    harness.client.over("<overview@nntpbench.local>"),
                    harness
                        .client
                        .listgroup_group_range("alt.binaries.bench", "1-10"),
                )
                .unwrap();

                body.as_bytes().len()
                    + article.as_bytes().len()
                    + over.as_bytes().len()
                    + listgroup.as_bytes().len()
            }))
        });
}

mod sequential_roundtrip {
    use super::{Bencher, bench_public_article_roundtrip, bench_raw_capabilities_roundtrip};

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn article_64k(bencher: Bencher) {
        bench_public_article_roundtrip(bencher);
    }

    #[divan::bench(sample_count = 100, sample_size = 100)]
    fn capabilities(bencher: Bencher) {
        bench_raw_capabilities_roundtrip(bencher);
    }
}

mod pipelined_roundtrip {
    use super::{
        Bencher, bench_pipelined_body_burst, bench_pipelined_mixed_requests,
        bench_pipelined_rfc_grammar_requests,
    };

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn body_burst_4(bencher: Bencher) {
        bench_pipelined_body_burst(bencher);
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn mixed_4(bencher: Bencher) {
        bench_pipelined_mixed_requests(bencher);
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn rfc_grammar_4(bencher: Bencher) {
        bench_pipelined_rfc_grammar_requests(bencher);
    }
}
