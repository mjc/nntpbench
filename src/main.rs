#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::io;

use clap::Parser;
use nntpbench::{ClientArgs, ServerArgs, run_client, run_server};
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

    /// Run the mock NNTP server.
    Server(ServerArgs),
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn main() -> io::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Client(args) => build_runtime(args.threads)?.block_on(run_client(args)),
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
