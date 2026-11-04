//! Microbenchmarks for client command coordination.
//!
//! The public client talks to the mock server on localhost. Responses stay
//! small so the samples are pipeline and barrier time, not article bytes.
//! The runtime is current-thread: the server is not polled until the joined
//! client futures have queued their commands.
//!
//! `date_pipeline_8` is the throughput path. `check_pipeline_8` pipelines CHECK.
//! `date_then_authinfo` queues DATE and then AUTHINFO USER, which takes the
//! connection exclusively. `mode_reader_then_date` holds the send barrier until
//! MODE READER completes.
//!
//! POST and IHAVE are not used. Before answering those commands, the server
//! peeks the next buffered line to see whether an article body follows, and
//! that peek waits for more socket data when the command is the last one
//! buffered.
//!
//! Run with: `cargo bench --bench client_coordination`

use std::net::SocketAddr;
use std::sync::Arc;

use divan::{Bencher, black_box};
use nntpbench::{
    Client, ClientOptions, ServerArgs, ServerConfig, Stats, bind_listener, serve_session,
};
use tokio::runtime::Builder;

fn main() {
    divan::main();
}

const PIPELINE: usize = 8;

fn server_args() -> ServerArgs {
    ServerArgs {
        listen: "127.0.0.1:0".parse().unwrap(),
        body_bytes: 128,
        article_bytes: 128,
        payload_mode: nntpbench::PayloadMode::Yenc,
        article_dir: None,
        max_connections: 16,
        threads: 1,
        max_pipeline_depth: 64,
        backlog: 128,
        reuse_port: false,
        nodelay: true,
        socket_recv_buffer: 0,
        socket_send_buffer: 0,
        stats_interval_secs: 0,
        flush: false,
        pending_write_bytes: 8 * 1024,
        json: false,
    }
}

async fn spawn_server() -> SocketAddr {
    let args = server_args();
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
            let config = Arc::clone(&config);
            let stats = Arc::clone(&stats);
            tokio::spawn(async move {
                let _ = serve_session(stream, peer_addr, config, stats).await;
            });
        }
    });

    addr
}

struct Harness {
    rt: tokio::runtime::Runtime,
    client: Client,
}

impl Harness {
    fn start() -> Self {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let addr = rt.block_on(spawn_server());
        let client = rt
            .block_on(Client::connect_with_options(
                addr,
                ClientOptions {
                    pipeline_depth: PIPELINE,
                    ..ClientOptions::default()
                },
            ))
            .unwrap();
        rt.block_on(client.capabilities()).unwrap();
        Self { rt, client }
    }
}

#[divan::bench(sample_count = 30, sample_size = 10)]
fn date_pipeline_8(bencher: Bencher) {
    let harness = Harness::start();
    bencher
        .counter(divan::counter::ItemsCount::new(PIPELINE))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                let (a, b, c, d, e, f, g, h) = tokio::join!(
                    harness.client.date(),
                    harness.client.date(),
                    harness.client.date(),
                    harness.client.date(),
                    harness.client.date(),
                    harness.client.date(),
                    harness.client.date(),
                    harness.client.date(),
                );
                a.unwrap().as_bytes().len()
                    + b.unwrap().as_bytes().len()
                    + c.unwrap().as_bytes().len()
                    + d.unwrap().as_bytes().len()
                    + e.unwrap().as_bytes().len()
                    + f.unwrap().as_bytes().len()
                    + g.unwrap().as_bytes().len()
                    + h.unwrap().as_bytes().len()
            }))
        });
}

#[divan::bench(sample_count = 30, sample_size = 10)]
fn date_sequential_8(bencher: Bencher) {
    let harness = Harness::start();
    bencher
        .counter(divan::counter::ItemsCount::new(PIPELINE))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                let mut total = 0;
                for _ in 0..PIPELINE {
                    total += harness.client.date().await.unwrap().as_bytes().len();
                }
                total
            }))
        });
}

#[divan::bench(sample_count = 30, sample_size = 10)]
fn mode_reader_then_date(bencher: Bencher) {
    let harness = Harness::start();
    bencher
        .counter(divan::counter::ItemsCount::new(2usize))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                let mode = harness.client.mode_reader();
                let date = harness.client.date();
                let (mode, date) = tokio::join!(mode, date);
                mode.unwrap().as_bytes().len() + date.unwrap().as_bytes().len()
            }))
        });
}

#[divan::bench(sample_count = 30, sample_size = 10)]
fn check_pipeline_8(bencher: Bencher) {
    let harness = Harness::start();
    bencher
        .counter(divan::counter::ItemsCount::new(PIPELINE))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                let (a, b, c, d, e, f, g, h) = tokio::join!(
                    harness.client.check("check.1@nntpbench.local"),
                    harness.client.check("check.2@nntpbench.local"),
                    harness.client.check("check.3@nntpbench.local"),
                    harness.client.check("check.4@nntpbench.local"),
                    harness.client.check("check.5@nntpbench.local"),
                    harness.client.check("check.6@nntpbench.local"),
                    harness.client.check("check.7@nntpbench.local"),
                    harness.client.check("check.8@nntpbench.local"),
                );
                a.unwrap().as_bytes().len()
                    + b.unwrap().as_bytes().len()
                    + c.unwrap().as_bytes().len()
                    + d.unwrap().as_bytes().len()
                    + e.unwrap().as_bytes().len()
                    + f.unwrap().as_bytes().len()
                    + g.unwrap().as_bytes().len()
                    + h.unwrap().as_bytes().len()
            }))
        });
}

#[divan::bench(sample_count = 30, sample_size = 10)]
fn date_then_authinfo(bencher: Bencher) {
    let harness = Harness::start();
    bencher
        .counter(divan::counter::ItemsCount::new(2usize))
        .bench_local(|| {
            black_box(harness.rt.block_on(async {
                let date = harness.client.date();
                let auth = harness.client.authinfo_user("bench");
                let (date, auth) = tokio::join!(date, auth);
                date.unwrap().as_bytes().len() + auth.unwrap().as_bytes().len()
            }))
        });
}
