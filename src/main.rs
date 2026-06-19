#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::io;

use clap::Parser;
use nntpbench::{FetchArgs, ServerArgs, run_fetch, run_server};
use tokio::runtime::{Builder, Runtime};

#[derive(Debug, Parser)]
#[command(author, version, about = "Small async mock NNTP benchmark server")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Send one NNTP request and print the raw response.
    Fetch(FetchArgs),

    /// Send one NNTP request through the request/future client and print the raw response.
    Client(FetchArgs),

    /// Run the mock NNTP server.
    Server(ServerArgs),
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn main() -> io::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Fetch(args) => build_runtime(args.threads)?.block_on(run_fetch(args)),
        Command::Client(args) => build_runtime(args.threads)?.block_on(run_fetch(args)),
        Command::Server(args) => build_runtime(args.threads)?.block_on(run_server(args)),
    }
}

fn build_runtime(threads: usize) -> io::Result<Runtime> {
    let threads = threads.max(1);
    let mut builder = if threads == 1 {
        Builder::new_current_thread()
    } else {
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(threads);
        builder
    };

    builder.enable_all().build()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn builds_current_thread_runtime_when_threads_is_zero_or_one() {
        let runtime = build_runtime(0).unwrap();
        assert_eq!(runtime.block_on(async { 7 }), 7);

        let runtime = build_runtime(1).unwrap();
        assert_eq!(runtime.block_on(async { 11 }), 11);
    }

    #[test]
    fn builds_multi_thread_runtime_when_threads_exceeds_one() {
        let runtime = build_runtime(2).unwrap();
        assert_eq!(runtime.block_on(async { 13 }), 13);
    }

    #[test]
    fn cli_help_labels_client_and_fetch_as_request_commands() {
        let help = Args::command().render_long_help().to_string();
        assert!(
            help.contains("Send one NNTP request") && !help.contains("raw NNTP request probe"),
            "help text should describe client and fetch without contradicting capability preflight behavior"
        );
    }
}
