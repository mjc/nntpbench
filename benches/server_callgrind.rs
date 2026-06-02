//! IAI-Callgrind benchmarks for deterministic mock NNTP server hot paths.
//!
//! These benches avoid live TCP so instruction counts focus on command parsing,
//! response selection, stats accounting, and buffer writes.
//!
//! Run with: `cargo bench --bench server_callgrind`

macro_rules! supported {
    ($($item:item)*) => {
        $(
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            $item
        )*
    };
}

supported! {
    use iai_callgrind::{
        Callgrind, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main,
    };
    use nntpbench::{
        RequestKind, RequestLine, ServerArgs, ServerConfig, Stats, for_each_request_line_in_batch,
        process_request_to_buffer,
    };
    use std::hint::black_box;
    use std::sync::Arc;

    const ARTICLE_64K: usize = 64 * 1024;
    const ARTICLE_768K: usize = 768 * 1024;
    const BODY_64K: usize = 64 * 1024;
    const BODY_768K: usize = 768 * 1024;

    fn server_args(body_bytes: usize, article_bytes: usize) -> ServerArgs {
        ServerArgs {
            listen: "127.0.0.1:0".parse().unwrap(),
            body_bytes,
            article_bytes,
            article_dir: None,
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
            pending_write_bytes: 800 * 1024,
        }
    }

    struct ProcessHarness {
        config: Arc<ServerConfig>,
        stats: Stats,
        output: Vec<u8>,
    }

    fn setup_64k() -> ProcessHarness {
        ProcessHarness {
            config: Arc::new(ServerConfig::from_args(server_args(BODY_64K, ARTICLE_64K))),
            stats: Stats::new(),
            output: Vec::with_capacity(BODY_64K + ARTICLE_64K + 1024),
        }
    }

    fn setup_768k() -> ProcessHarness {
        ProcessHarness {
            config: Arc::new(ServerConfig::from_args(server_args(BODY_768K, ARTICLE_768K))),
            stats: Stats::new(),
            output: Vec::with_capacity(BODY_768K + ARTICLE_768K + 1024),
        }
    }

    struct PipelineHarness {
        config: Arc<ServerConfig>,
        stats: Stats,
        commands: Vec<RequestKind>,
        output: Vec<u8>,
    }

    fn setup_pipeline() -> PipelineHarness {
        PipelineHarness {
            config: Arc::new(ServerConfig::from_args(server_args(BODY_64K, ARTICLE_64K))),
            stats: Stats::new(),
            commands: Vec::with_capacity(8),
            output: Vec::with_capacity(BODY_64K + ARTICLE_64K + 2048),
        }
    }

    #[library_benchmark]
    #[bench::article(args = (b"ARTICLE <bench@nntpbench.local>\r\n"))]
    #[bench::body(args = (b"BODY <bench@nntpbench.local>\r\n"))]
    #[bench::capabilities(args = (b"CAPABILITIES\r\n"))]
    #[bench::date(args = (b"DATE\r\n"))]
    #[bench::mode_reader(args = (b"MODE READER\r\n"))]
    #[bench::unknown(args = (b"XYZZY 1\r\n"))]
    #[bench::quit(args = (b"QUIT\r\n"))]
    fn parse_command(command: &[u8]) -> RequestKind {
        black_box(RequestLine::parse(black_box(command)).kind())
    }

    #[library_benchmark]
    #[bench::article_64k(setup = setup_64k)]
    #[bench::article_768k(setup = setup_768k)]
    fn process_article(mut harness: ProcessHarness) -> usize {
        process_request_to_buffer(
            RequestLine::parse(b"ARTICLE <bench@nntpbench.local>\r\n"),
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::body_64k(setup = setup_64k)]
    #[bench::body_768k(setup = setup_768k)]
    fn process_body(mut harness: ProcessHarness) -> usize {
        process_request_to_buffer(
            RequestLine::parse(b"BODY <bench@nntpbench.local>\r\n"),
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::capabilities(setup = setup_64k)]
    fn process_capabilities(mut harness: ProcessHarness) -> usize {
        process_request_to_buffer(
            RequestLine::parse(b"CAPABILITIES\r\n"),
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::unknown(setup = setup_64k)]
    fn process_unknown(mut harness: ProcessHarness) -> usize {
        process_request_to_buffer(
            RequestLine::parse(b"XYZZY 1\r\n"),
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::date(setup = setup_64k)]
    fn process_date(mut harness: ProcessHarness) -> usize {
        process_request_to_buffer(
            RequestLine::parse(b"DATE\r\n"),
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::mode_reader(setup = setup_64k)]
    fn process_mode_reader(mut harness: ProcessHarness) -> usize {
        process_request_to_buffer(
            RequestLine::parse(b"MODE READER\r\n"),
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::quit(setup = setup_64k)]
    fn process_quit(mut harness: ProcessHarness) -> bool {
        black_box(process_request_to_buffer(
            RequestLine::parse(b"QUIT\r\n"),
            &harness.config,
            &harness.stats,
            &mut harness.output,
        ))
    }

    #[library_benchmark]
    #[bench::mixed_full_command_set(setup = setup_pipeline)]
    fn process_pipelined_batch(mut harness: PipelineHarness) -> usize {
        let request = black_box(
            b"BODY <body@nntpbench.local>\r\n\
ARTICLE <article@nntpbench.local>\r\n\
CAPABILITIES\r\n\
DATE\r\n\
MODE READER\r\n\
XYZZY 1\r\n\
QUIT\r\n",
        );
        let consumed = for_each_request_line_in_batch(request, 64, |request| {
            harness.commands.push(request.kind());
            let _ = process_request_to_buffer(
                request,
                &harness.config,
                &harness.stats,
                &mut harness.output,
            );
        });
        black_box(consumed + harness.output.len())
    }

    library_benchmark_group!(
        name = server_hot_paths;
        benchmarks =
            parse_command,
            process_article,
            process_body,
            process_capabilities,
            process_unknown,
            process_date,
            process_mode_reader,
            process_quit,
            process_pipelined_batch
    );

    main!(
        config = LibraryBenchmarkConfig::default().tool(
            Callgrind::with_args([
                "--branch-sim=yes",
                "--cache-sim=yes",
            ])
        );
        library_benchmark_groups = server_hot_paths
    );
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
fn main() {
    eprintln!("server_callgrind is disabled on this target");
}
