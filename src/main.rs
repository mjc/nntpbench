#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::io;

use clap::Parser;
use nntpbench::{ServerArgs, run_server};

#[derive(Debug, Parser)]
#[command(author, version, about = "Small async mock NNTP benchmark server")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Run the mock NNTP server.
    Server(ServerArgs),
}

#[tokio::main]
#[cfg_attr(coverage_nightly, coverage(off))]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Server(args) => run_server(args).await,
    }
}
