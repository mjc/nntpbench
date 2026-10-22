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
    #[cfg(target_os = "macos")]
    request_benchmark_qos()?;

    let threads = threads.max(1);
    let mut builder = if threads == 1 {
        Builder::new_current_thread()
    } else {
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(threads);
        builder
    };

    #[cfg(target_os = "macos")]
    builder.on_thread_start(|| {
        if let Err(error) = request_benchmark_qos() {
            eprintln!("cannot request benchmark worker QoS: {error}");
        }
    });

    builder.enable_all().build()
}

#[cfg(target_os = "macos")]
fn request_benchmark_qos() -> io::Result<()> {
    // Keep benchmark work at a consistent requested priority on Apple Silicon;
    // macOS still decides which cores run each thread.
    // SAFETY: UserInitiated and relative priority zero are valid API values.
    // The call affects only the calling thread and takes no pointers.
    let status = unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INITIATED, 0)
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status))
    }
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

    #[cfg(target_os = "macos")]
    fn requested_qos_class() -> libc::qos_class_t {
        let mut class = libc::qos_class_t::QOS_CLASS_UNSPECIFIED;
        let mut priority = 0;
        // SAFETY: pthread_self identifies this live thread and both output
        // pointers refer to initialized values owned by this call.
        let status = unsafe {
            libc::pthread_get_qos_class_np(libc::pthread_self(), &mut class, &mut priority)
        };
        assert_eq!(status, 0);
        class
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn runtime_requests_user_initiated_qos_for_current_thread() {
        // SAFETY: Utility is a valid QoS class and zero is a valid relative
        // priority. This changes only this test's calling thread.
        let status =
            unsafe { libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_UTILITY, 0) };
        assert_eq!(status, 0);
        let runtime = build_runtime(1).unwrap();
        assert_eq!(
            runtime.block_on(async { requested_qos_class() }) as u32,
            libc::qos_class_t::QOS_CLASS_USER_INITIATED as u32,
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn runtime_workers_request_user_initiated_qos() {
        let runtime = build_runtime(2).unwrap();
        let class = runtime.block_on(async {
            tokio::spawn(async { requested_qos_class() as u32 })
                .await
                .unwrap()
        });
        assert_eq!(class, libc::qos_class_t::QOS_CLASS_USER_INITIATED as u32);
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
