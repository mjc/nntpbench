#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::io;

use clap::Parser;
use nntpbench::{
    ClientArgs, ServerArgs, TypedClientArgs, TypedFetchArgs, run_client, run_server,
    run_typed_client, run_typed_fetch,
};
use tokio::runtime::{Builder, Runtime};

#[derive(Debug, Parser)]
#[command(author, version, about = "Small async mock NNTP benchmark server")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Run the benchmark NNTP client.
    Client(ClientArgs),

    /// Send one typed ARTICLE/BODY request and print the raw response.
    Fetch(TypedFetchArgs),

    /// Run the typed request/future benchmark client.
    TypedClient(TypedClientArgs),

    /// Run the mock NNTP server.
    Server(ServerArgs),
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn main() -> io::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Client(args) => build_runtime(args.threads)?.block_on(run_client(args)),
        Command::Fetch(args) => build_runtime(args.threads)?.block_on(run_typed_fetch(args)),
        Command::TypedClient(args) => build_runtime(args.threads)?.block_on(run_typed_client(args)),
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
}
