//! End-to-end mock NNTP server benchmarks.
//!
//! These benches drive the real server session path over localhost TCP.
//!
//! Run with: `cargo bench --bench server_roundtrip`

use std::net::SocketAddr;
use std::sync::Arc;

use divan::{Bencher, black_box};
use nntpbench::{GREETING, ServerArgs, ServerConfig, Stats, bind_listener, serve_session};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Builder;

fn main() {
    divan::main();
}

const ARTICLE_64K: usize = 64 * 1024;
const ARTICLE_768K: usize = 768 * 1024;
const BODY_64K: usize = 64 * 1024;
const BODY_768K: usize = 768 * 1024;

fn server_args(body_bytes: usize, article_bytes: usize) -> ServerArgs {
    ServerArgs {
        listen: "127.0.0.1:0".parse().unwrap(),
        body_bytes,
        article_bytes,
        max_connections: 4096,
        max_pipeline_depth: 64,
        backlog: 8192,
        reuse_port: false,
        nodelay: true,
        stats_interval_secs: 0,
        flush: false,
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

struct BenchClient {
    stream: TcpStream,
    response_buffer: Box<[u8]>,
}

impl BenchClient {
    async fn connect(addr: SocketAddr, response_capacity: usize) -> Self {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();

        let mut greeting = [0_u8; GREETING.len()];
        stream.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, GREETING);

        Self {
            stream,
            response_buffer: vec![0; response_capacity + 1024].into_boxed_slice(),
        }
    }

    async fn command_roundtrip(&mut self, command: &[u8], multiline: bool) -> usize {
        self.stream.write_all(command).await.unwrap();
        if multiline {
            read_multiline_into(&mut self.stream, &mut self.response_buffer).await
        } else {
            read_line_into(&mut self.stream, &mut self.response_buffer).await
        }
    }
}

async fn read_line_into(stream: &mut TcpStream, buffer: &mut [u8]) -> usize {
    let mut total = 0;
    loop {
        stream
            .read_exact(&mut buffer[total..total + 1])
            .await
            .unwrap();
        total += 1;
        if buffer[total - 1] == b'\n' {
            return total;
        }
    }
}

async fn read_multiline_into(stream: &mut TcpStream, buffer: &mut [u8]) -> usize {
    let mut total = 0;
    loop {
        let n = stream.read(&mut buffer[total..]).await.unwrap();
        assert_ne!(n, 0, "server closed during multiline response");
        total += n;
        if buffer[..total].ends_with(b"\r\n.\r\n") {
            return total;
        }
    }
}

struct RoundtripHarness {
    rt: tokio::runtime::Runtime,
    client: BenchClient,
}

impl RoundtripHarness {
    fn start(body_bytes: usize, article_bytes: usize, response_capacity: usize) -> Self {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        let addr = rt.block_on(spawn_server(body_bytes, article_bytes));
        let client = rt.block_on(BenchClient::connect(addr, response_capacity));
        Self { rt, client }
    }
}

fn bench_persistent_roundtrip(
    bencher: Bencher,
    command: &'static [u8],
    multiline: bool,
    body_bytes: usize,
    article_bytes: usize,
    response_capacity: usize,
) {
    let mut harness = RoundtripHarness::start(body_bytes, article_bytes, response_capacity);
    bencher
        .counter(divan::counter::BytesCount::new(response_capacity))
        .bench_local(|| {
            black_box(
                harness
                    .rt
                    .block_on(harness.client.command_roundtrip(command, multiline)),
            )
        });
}

fn bench_connecting_command(
    bencher: Bencher,
    command: &'static [u8],
    body_bytes: usize,
    article_bytes: usize,
    response_capacity: usize,
) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let addr = rt.block_on(spawn_server(body_bytes, article_bytes));
    bencher.bench_local(|| {
        black_box(rt.block_on(async {
            let mut client = BenchClient::connect(addr, response_capacity).await;
            client.command_roundtrip(command, false).await
        }))
    });
}

fn bench_pipeline_mixed(bencher: Bencher) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let addr = rt.block_on(spawn_server(BODY_64K, ARTICLE_64K));
    let request = b"BODY <body@nntpbench.local>\r\n\
ARTICLE <article@nntpbench.local>\r\n\
CAPABILITIES\r\n\
DATE\r\n\
MODE READER\r\n\
HEAD 1\r\n\
QUIT\r\n";

    bencher
        .counter(divan::counter::ItemsCount::new(7usize))
        .bench_local(|| {
            black_box(rt.block_on(async {
                let mut client = BenchClient::connect(addr, BODY_64K + ARTICLE_64K + 2048).await;
                client.stream.write_all(request).await.unwrap();
                let mut response = Vec::with_capacity(BODY_64K + ARTICLE_64K + 2048);
                client.stream.read_to_end(&mut response).await.unwrap();
                response.len()
            }))
        });
}

mod article_roundtrip {
    use super::{ARTICLE_64K, ARTICLE_768K, BODY_64K, BODY_768K};
    use super::{Bencher, bench_persistent_roundtrip};

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn article_64k(bencher: Bencher) {
        bench_persistent_roundtrip(
            bencher,
            b"ARTICLE <bench@nntpbench.local>\r\n",
            true,
            BODY_64K,
            ARTICLE_64K,
            ARTICLE_64K,
        );
    }

    #[divan::bench(sample_count = 100, sample_size = 10)]
    fn article_768k(bencher: Bencher) {
        bench_persistent_roundtrip(
            bencher,
            b"ARTICLE <bench@nntpbench.local>\r\n",
            true,
            BODY_768K,
            ARTICLE_768K,
            ARTICLE_768K,
        );
    }
}

mod body_roundtrip {
    use super::{ARTICLE_64K, ARTICLE_768K, BODY_64K, BODY_768K};
    use super::{Bencher, bench_persistent_roundtrip};

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn body_64k(bencher: Bencher) {
        bench_persistent_roundtrip(
            bencher,
            b"BODY <bench@nntpbench.local>\r\n",
            true,
            BODY_64K,
            ARTICLE_64K,
            BODY_64K,
        );
    }

    #[divan::bench(sample_count = 100, sample_size = 10)]
    fn body_768k(bencher: Bencher) {
        bench_persistent_roundtrip(
            bencher,
            b"BODY <bench@nntpbench.local>\r\n",
            true,
            BODY_768K,
            ARTICLE_768K,
            BODY_768K,
        );
    }
}

mod command_roundtrip {
    use super::{ARTICLE_64K, BODY_64K};
    use super::{Bencher, bench_connecting_command, bench_persistent_roundtrip};

    #[divan::bench(sample_count = 100, sample_size = 100)]
    fn capabilities(bencher: Bencher) {
        bench_persistent_roundtrip(
            bencher,
            b"CAPABILITIES\r\n",
            true,
            BODY_64K,
            ARTICLE_64K,
            128,
        );
    }

    #[divan::bench(sample_count = 100, sample_size = 100)]
    fn unknown(bencher: Bencher) {
        bench_persistent_roundtrip(bencher, b"HEAD 1\r\n", false, BODY_64K, ARTICLE_64K, 64);
    }

    #[divan::bench(sample_count = 100, sample_size = 100)]
    fn date(bencher: Bencher) {
        bench_persistent_roundtrip(bencher, b"DATE\r\n", false, BODY_64K, ARTICLE_64K, 64);
    }

    #[divan::bench(sample_count = 100, sample_size = 100)]
    fn mode_reader(bencher: Bencher) {
        bench_persistent_roundtrip(
            bencher,
            b"MODE READER\r\n",
            false,
            BODY_64K,
            ARTICLE_64K,
            64,
        );
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn quit(bencher: Bencher) {
        bench_connecting_command(bencher, b"QUIT\r\n", BODY_64K, ARTICLE_64K, 64);
    }
}

mod pipelined_roundtrip {
    use super::{Bencher, bench_pipeline_mixed};

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn mixed_full_command_set(bencher: Bencher) {
        bench_pipeline_mixed(bencher);
    }
}
