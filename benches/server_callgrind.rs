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
        CommandLine, ParsedCommand, ServerArgs, ServerConfig, Stats, parse_command_batch_bytes,
        process_command_to_buffer,
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
            max_connections: 4096,
            threads: 1,
            max_pipeline_depth: 64,
            backlog: 8192,
            reuse_port: false,
            nodelay: true,
            stats_interval_secs: 0,
            flush: false,
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
        commands: Vec<ParsedCommand>,
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
    #[bench::article(args = (b"ARTICLE <bench@nntpbench.local>"))]
    #[bench::body(args = (b"BODY <bench@nntpbench.local>"))]
    #[bench::capabilities(args = (b"CAPABILITIES"))]
    #[bench::date(args = (b"DATE"))]
    #[bench::mode_reader(args = (b"MODE READER"))]
    #[bench::unknown(args = (b"HEAD 1"))]
    #[bench::quit(args = (b"QUIT"))]
    fn parse_command(command: &[u8]) -> ParsedCommand {
        black_box(CommandLine::parse(black_box(command)))
    }

    #[library_benchmark]
    #[bench::article_64k(setup = setup_64k)]
    #[bench::article_768k(setup = setup_768k)]
    fn process_article(mut harness: ProcessHarness) -> usize {
        process_command_to_buffer(
            ParsedCommand::Article,
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
        process_command_to_buffer(
            ParsedCommand::Body,
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::capabilities(setup = setup_64k)]
    fn process_capabilities(mut harness: ProcessHarness) -> usize {
        process_command_to_buffer(
            ParsedCommand::Capabilities,
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::unknown(setup = setup_64k)]
    fn process_unknown(mut harness: ProcessHarness) -> usize {
        process_command_to_buffer(
            ParsedCommand::Unknown,
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::date(setup = setup_64k)]
    fn process_date(mut harness: ProcessHarness) -> usize {
        process_command_to_buffer(
            ParsedCommand::Date,
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::mode_reader(setup = setup_64k)]
    fn process_mode_reader(mut harness: ProcessHarness) -> usize {
        process_command_to_buffer(
            ParsedCommand::ModeReader,
            &harness.config,
            &harness.stats,
            &mut harness.output,
        );
        black_box(harness.output.len())
    }

    #[library_benchmark]
    #[bench::quit(setup = setup_64k)]
    fn process_quit(mut harness: ProcessHarness) -> bool {
        black_box(process_command_to_buffer(
            ParsedCommand::Quit,
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
HEAD 1\r\n\
QUIT\r\n",
        );
        let consumed = parse_command_batch_bytes(request, 64, &mut harness.commands);
        for command in &harness.commands {
            if process_command_to_buffer(
                *command,
                &harness.config,
                &harness.stats,
                &mut harness.output,
            ) {
                break;
            }
        }
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
