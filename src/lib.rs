#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::collections::VecDeque;
use std::fs;
use std::future::poll_fn;
use std::io::{self, IoSlice, Write};
use std::net::SocketAddr;
use std::ops::Range;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrayvec::ArrayVec;
use clap::{ArgAction, Parser, ValueEnum};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time;

pub mod protocol;
pub mod tail_buffer;
pub mod typed_client;

pub use protocol::{
    Article, ArticleNumber, ArticleParseError, ArticleSelector, ArticleTransfer, AuthInfoKind,
    AuthInfoValue, GroupName, HeaderIter, HeaderName, Headers, MessageId, NntpDate, NntpTime,
    Request, RequestKind, RequestLine, StatusCode, Wildmat,
};
pub use typed_client::{
    Client, OwnedArticle, OwnedArticleExchange, OwnedExchange, OwnedResponse,
    TypedClientConnection, TypedClientError, TypedClientOptions, TypedClientResponseMode,
};

pub const CRLF: &[u8] = b"\r\n";
pub const TERMINATOR: &[u8] = b"\r\n.\r\n";
pub const GREETING: &[u8] = b"201 nntpbench mock server ready\r\n";
pub const MODE_READER_RESPONSE: &[u8] = b"201 posting not permitted\r\n";
pub const DATE_RESPONSE: &[u8] = b"111 20260515120000\r\n";
pub const LIST_RESPONSE: &[u8] =
    b"215 list of newsgroups follows\r\ncomp.lang.rust 0000000001 0000000001 y\r\n.\r\n";
pub const GROUP_RESPONSE: &[u8] = b"211 3 1 3 alt.test\r\n";
pub const LISTGROUP_RESPONSE: &[u8] = b"211 3 1 3 alt.test\r\n1\r\n2\r\n3\r\n.\r\n";
pub const LAST_RESPONSE: &[u8] =
    b"223 1 <prev@alt.test> article retrieved - request text separately\r\n";
pub const NEXT_RESPONSE: &[u8] =
    b"223 2 <next@alt.test> article retrieved - request text separately\r\n";
pub const NEWGROUPS_RESPONSE: &[u8] =
    b"231 list of new newsgroups follows\r\ncomp.lang.rust 0000000003 0000000001 y\r\nalt.test 0000000003 0000000001 y\r\n.\r\n";
pub const NEWNEWS_RESPONSE: &[u8] =
    b"230 list of new articles follows\r\n<one@alt.test>\r\n<two@alt.test>\r\n.\r\n";
pub const POST_RESPONSE: &[u8] = b"340 send article to be posted\r\n";
pub const IHAVE_RESPONSE: &[u8] = b"335 send article to be transferred\r\n";
pub const CHECK_RESPONSE: &[u8] = b"238 send article to be transferred\r\n";
pub const TAKETHIS_RESPONSE: &[u8] = b"239 article transferred ok\r\n";
pub const AUTHINFO_USER_RESPONSE: &[u8] = b"381 more authentication information required\r\n";
pub const AUTHINFO_RESPONSE: &[u8] = b"281 authentication accepted\r\n";
pub const STARTTLS_RESPONSE: &[u8] = b"382 continue with TLS negotiation\r\n";
pub const OVER_RESPONSE: &[u8] = b"224 Overview information follows\r\n1\tSubject one\tone@example.com\tFri, 16 May 2026 12:00:00 +0000\t<one@example.com>\t\t123\t4\r\n.\r\n";
pub const XOVER_RESPONSE: &[u8] = b"224 Overview information follows\r\n2\tSubject two\ttwo@example.com\tFri, 16 May 2026 12:00:01 +0000\t<two@example.com>\t<ref@example.com>\t456\t8\r\n.\r\n";
pub const HDR_RESPONSE: &[u8] =
    b"225 headers follow\r\n1 Subject: example one\r\n2 Subject: example two\r\n.\r\n";
pub const XHDR_RESPONSE: &[u8] =
    b"225 headers follow\r\n1 <one@example>\r\n2 <two@example>\r\n.\r\n";
pub const HELP_RESPONSE: &[u8] =
    b"100 help text follows\r\nLIST\r\nCAPABILITIES\r\nDATE\r\nMODE-READER\r\nQUIT\r\n.\r\n";
pub const QUIT_RESPONSE: &[u8] = b"205 closing connection\r\n";
const MAX_COMMAND_LINE_BYTES: usize = 1024;
const MAX_SERVER_PIPELINE_DEPTH: usize = 1024;
const SERVER_READER_CAPACITY: usize = 256 * 1024;
const CLIENT_READER_CAPACITY: usize = 256 * 1024;
const MAX_PENDING_WRITE_BYTES: usize = 64 * 1024;
const MAX_CLIENT_COMMAND_BYTES: usize = 64;
const HIGH_THROUGHPUT_SOCKET_BUFFER: usize = 16 * 1024 * 1024;
const PROCESS_CLOCK_TICK: Duration = Duration::from_millis(10);
const TCP_LINGER_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(target_os = "linux")]
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(target_os = "linux")]
const TOS_THROUGHPUT: u32 = 0x08;

#[derive(Debug, Parser, Clone)]
pub struct ServerArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:1199")]
    pub listen: SocketAddr,

    /// Approximate body bytes returned for BODY.
    #[arg(long, default_value_t = 64 * 1024)]
    pub body_bytes: usize,

    /// Approximate full article bytes returned for ARTICLE, including headers.
    #[arg(long, default_value_t = 68 * 1024)]
    pub article_bytes: usize,

    /// Maximum concurrent accepted sessions.
    #[arg(long, default_value_t = 4096)]
    pub max_connections: usize,

    /// Tokio runtime worker threads. Use 1 for the current-thread runtime.
    #[arg(long, default_value_t = 1)]
    pub threads: usize,

    /// Maximum complete commands consumed from one pipelined read batch.
    #[arg(long, default_value_t = MAX_SERVER_PIPELINE_DEPTH)]
    pub max_pipeline_depth: usize,

    /// Listen backlog passed to the OS.
    #[arg(long, default_value_t = 8192)]
    pub backlog: i32,

    /// Enable SO_REUSEPORT on Unix for accept sharding across server processes.
    #[arg(long, default_value_t = false)]
    pub reuse_port: bool,

    /// Set TCP_NODELAY on accepted sockets.
    #[arg(long, default_value_t = true)]
    pub nodelay: bool,

    /// Socket receive buffer size for accepted sockets. Use 0 to leave the OS default.
    #[arg(long, default_value_t = HIGH_THROUGHPUT_SOCKET_BUFFER)]
    pub socket_recv_buffer: usize,

    /// Socket send buffer size for accepted sockets. Use 0 to leave the OS default.
    #[arg(long, default_value_t = HIGH_THROUGHPUT_SOCKET_BUFFER)]
    pub socket_send_buffer: usize,

    /// Print benchmark statistics at this interval. Use 0 to disable periodic output.
    #[arg(long, default_value_t = 1)]
    pub stats_interval_secs: u64,

    /// Flush after each response.
    #[arg(long, default_value_t = false)]
    pub flush: bool,
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub async fn run_server(args: ServerArgs) -> io::Result<()> {
    let listener = bind_listener(args.listen, args.backlog, args.reuse_port)?;
    let local_addr = listener.local_addr()?;
    let config = Arc::new(ServerConfig::from_args(args));
    let stats = Arc::new(Stats::new());
    let limiter = Arc::new(Semaphore::new(config.max_connections));

    eprintln!(
        "nntpbench server listening on {local_addr} body_bytes={} article_bytes={} max_connections={}",
        config.body_bytes, config.article_bytes, config.max_connections
    );

    if config.stats_interval != Duration::ZERO {
        tokio::spawn(report_stats(stats.clone(), config.stats_interval));
    }

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, peer_addr) = result?;
                optimize_server_socket(&stream, &config)?;
                let Ok(permit) = limiter.clone().try_acquire_owned() else {
                    stats.refused_connections.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                stats.accepted_connections.fetch_add(1, Ordering::Relaxed);
                stats.active_connections.fetch_add(1, Ordering::Relaxed);

                tokio::spawn({
                    let config = config.clone();
                    let stats = stats.clone();
                    async move {
                        let _permit = permit;
                        if let Err(err) = serve_session(stream, peer_addr, config, stats.clone()).await {
                            stats.errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("session {peer_addr} error: {err}");
                        }
                        stats.active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                });
            }
            shutdown = tokio::signal::ctrl_c() => {
                shutdown?;
                eprintln!("shutdown requested");
                stats.print_snapshot("final");
                return Ok(());
            }
        }
    }
}

#[derive(Debug, Parser, Clone)]
pub struct ClientArgs {
    /// Address to connect to.
    #[arg(long, default_value = "127.0.0.1:1199")]
    pub connect: SocketAddr,

    /// Comma-separated target ports. When set, clients rotate across these ports on the connect host.
    #[arg(long, value_delimiter = ',')]
    pub ports: Vec<u16>,

    /// Tab-separated segment file. Lines are SIZE<TAB>MSGID; MSGID is normalized into angle brackets.
    #[arg(long)]
    pub segments: Option<PathBuf>,

    /// Total ARTICLE/BODY commands to complete. Use 0 to disable this limit.
    #[arg(long, default_value_t = 0)]
    pub requests: u64,

    /// Received response bytes to complete. Use 0 to disable this limit.
    #[arg(long, default_value_t = 0)]
    pub transfer_bytes: u64,

    /// Seconds to run. Use 0 to disable this limit.
    #[arg(long, default_value_t = 0)]
    pub duration_secs: u64,

    /// Concurrent TCP connections.
    #[arg(long, default_value_t = 1)]
    pub connections: usize,

    /// Offset used when this process owns a shard of the total client set.
    #[arg(long, default_value_t = 0)]
    pub client_offset: usize,

    /// Total clients across all cooperating client processes. Use 0 to mean --connections.
    #[arg(long, default_value_t = 0)]
    pub total_clients: usize,

    /// Tokio runtime worker threads. Use 1 for the current-thread runtime.
    #[arg(long, default_value_t = 1)]
    pub threads: usize,

    /// Commands written before draining responses.
    #[arg(long, default_value_t = 64)]
    pub pipeline_depth: usize,

    /// Command mix generated by each connection.
    #[arg(long, value_enum, default_value_t = ClientCommandMix::Alternate)]
    pub command_mix: ClientCommandMix,

    /// First numeric article id used in generated Message-IDs.
    #[arg(long, default_value_t = 1)]
    pub start_id: u64,

    /// Per-connection read buffer size.
    #[arg(long, default_value_t = CLIENT_READER_CAPACITY)]
    pub read_buffer_bytes: usize,

    /// Set TCP_NODELAY on client sockets.
    #[arg(long, default_value_t = true)]
    pub nodelay: bool,

    /// Socket receive buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = HIGH_THROUGHPUT_SOCKET_BUFFER)]
    pub socket_recv_buffer: usize,

    /// Socket send buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = HIGH_THROUGHPUT_SOCKET_BUFFER)]
    pub socket_send_buffer: usize,

    /// Print final machine-readable CSV: requests,bytes,elapsed_s,cpu_s,rss_kib.
    #[arg(long, default_value_t = false)]
    pub csv: bool,

    /// Print benchmark statistics at this interval. Use 0 to disable periodic output.
    #[arg(long, default_value_t = 1)]
    pub stats_interval_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClientCommandMix {
    Article,
    Body,
    Alternate,
}

#[derive(Debug, Parser, Clone)]
pub struct TypedClientArgs {
    /// Address to connect to.
    #[arg(long, default_value = "127.0.0.1:1199")]
    pub connect: SocketAddr,

    /// Comma-separated target ports. When set, clients rotate across these ports on the connect host.
    #[arg(long, value_delimiter = ',')]
    pub ports: Vec<u16>,

    /// Tab-separated segment file. Lines are SIZE<TAB>MSGID; MSGID is normalized into angle brackets.
    #[arg(long)]
    pub segments: Option<PathBuf>,

    /// Total typed ARTICLE/BODY requests to complete. Use 0 to disable this limit.
    #[arg(long, default_value_t = 0)]
    pub requests: u64,

    /// Received response bytes to complete. Use 0 to disable this limit.
    #[arg(long, default_value_t = 0)]
    pub transfer_bytes: u64,

    /// Seconds to run. Use 0 to disable this limit.
    #[arg(long, default_value_t = 0)]
    pub duration_secs: u64,

    /// Concurrent TCP connections.
    #[arg(long, default_value_t = 1)]
    pub connections: usize,

    /// Offset used when this process owns a shard of the total client set.
    #[arg(long, default_value_t = 0)]
    pub client_offset: usize,

    /// Total clients across all cooperating client processes. Use 0 to mean --connections.
    #[arg(long, default_value_t = 0)]
    pub total_clients: usize,

    /// Tokio runtime worker threads. Use 1 for the current-thread runtime.
    #[arg(long, default_value_t = 1)]
    pub threads: usize,

    /// Maximum in-flight typed requests allowed on each connection.
    #[arg(long, default_value_t = 64)]
    pub pipeline_depth: usize,

    /// Command mix generated by each connection.
    #[arg(long, value_enum, default_value_t = ClientCommandMix::Alternate)]
    pub command_mix: ClientCommandMix,

    /// First numeric article id used for per-request accounting.
    #[arg(long, default_value_t = 1)]
    pub start_id: u64,

    /// Per-connection read buffer size.
    #[arg(long, default_value_t = CLIENT_READER_CAPACITY)]
    pub read_buffer_bytes: usize,

    /// Set TCP_NODELAY on client sockets.
    #[arg(long, default_value_t = true)]
    pub nodelay: bool,

    /// Socket receive buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = HIGH_THROUGHPUT_SOCKET_BUFFER)]
    pub socket_recv_buffer: usize,

    /// Socket send buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = HIGH_THROUGHPUT_SOCKET_BUFFER)]
    pub socket_send_buffer: usize,

    /// Print final machine-readable CSV: requests,bytes,elapsed_s,cpu_s,rss_kib.
    #[arg(long, default_value_t = false)]
    pub csv: bool,

    /// Print benchmark statistics at this interval. Use 0 to disable periodic output.
    #[arg(long, default_value_t = 1)]
    pub stats_interval_secs: u64,
}

#[derive(Debug, Parser, Clone)]
pub struct TypedFetchArgs {
    /// Address to connect to.
    #[arg(long, default_value = "127.0.0.1:1199")]
    pub connect: SocketAddr,

    /// Request kind to send.
    #[arg(long, value_enum)]
    pub request: TypedRequestKind,

    /// Message-ID to request for ARTICLE/BODY/HEAD/STAT. Bare IDs are wrapped in angle brackets.
    #[arg(long)]
    pub message_id: Option<String>,

    /// Article body for TAKETHIS requests.
    #[arg(long)]
    pub article_body: Option<String>,

    /// AUTHINFO value for AUTHINFO USER/PASS requests.
    #[arg(long)]
    pub auth_value: Option<String>,

    /// Header name for HDR/XHDR requests.
    #[arg(long)]
    pub header: Option<String>,

    /// Group name for GROUP/LISTGROUP requests.
    #[arg(long)]
    pub group: Option<String>,

    /// Wildmat pattern for NEWNEWS requests.
    #[arg(long)]
    pub wildmat: Option<String>,

    /// NNTP date for NEWGROUPS/NEWNEWS, in YYMMDD or YYYYMMDD form.
    #[arg(long)]
    pub date: Option<String>,

    /// NNTP time for NEWGROUPS/NEWNEWS, in HHMMSS form.
    #[arg(long)]
    pub time: Option<String>,

    /// Treat NEWGROUPS/NEWNEWS timestamps as UTC.
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    pub gmt: bool,

    /// Article selector for OVER/XOVER/HDR/XHDR requests, such as 1, 1-10, or <message@id>.
    #[arg(long)]
    pub selector: Option<String>,

    /// Tokio runtime worker threads. Use 1 for the current-thread runtime.
    #[arg(long, default_value_t = 1)]
    pub threads: usize,

    /// Per-connection read buffer size.
    #[arg(long, default_value_t = CLIENT_READER_CAPACITY)]
    pub read_buffer_bytes: usize,

    /// Maximum in-flight typed requests allowed on the connection.
    #[arg(long, default_value_t = 64)]
    pub pipeline_depth: usize,

    /// Set TCP_NODELAY on client sockets.
    #[arg(long, default_value_t = true)]
    pub nodelay: bool,

    /// Socket receive buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = HIGH_THROUGHPUT_SOCKET_BUFFER)]
    pub socket_recv_buffer: usize,

    /// Socket send buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = HIGH_THROUGHPUT_SOCKET_BUFFER)]
    pub socket_send_buffer: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TypedRequestKind {
    Article,
    Body,
    Head,
    Stat,
    Group,
    Listgroup,
    Last,
    Next,
    Newgroups,
    Newnews,
    Post,
    Ihave,
    Check,
    Takethis,
    AuthinfoUser,
    AuthinfoPass,
    Starttls,
    Over,
    Xover,
    Hdr,
    Xhdr,
    List,
    Help,
    Capabilities,
    Date,
    ModeReader,
    Quit,
}

#[derive(Debug)]
struct SegmentSet {
    ids: Box<[Box<[u8]>]>,
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub async fn run_client(args: ClientArgs) -> io::Result<()> {
    let config = TypedClientConfig::from_client_args(args)?;
    run_typed_workload(config).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub async fn run_typed_client(args: TypedClientArgs) -> io::Result<()> {
    let config = TypedClientConfig::from_args(args)?;
    run_typed_workload(config).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_typed_workload(config: TypedClientConfig) -> io::Result<()> {
    let start_cpu_ticks = process_cpu_ticks();
    let started = Instant::now();
    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));

    eprintln!(
        "nntpbench client connecting to {} requests={} transfer_bytes={} duration_secs={} connections={} total_clients={} client_offset={} pipeline_depth={} command_mix={:?}",
        config.connect,
        config.requests,
        config.transfer_bytes,
        config.duration.as_secs(),
        config.connections,
        config.total_clients,
        config.client_offset,
        config.pipeline_depth,
        config.command_mix
    );

    if config.stats_interval != Duration::ZERO {
        tokio::spawn(report_stats(stats.clone(), config.stats_interval));
    }

    tokio::spawn({
        let stop = stop.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                stop.store(true, Ordering::Release);
            }
        }
    });

    if config.duration != Duration::ZERO {
        tokio::spawn({
            let stop = stop.clone();
            let duration = config.duration;
            async move {
                time::sleep(duration).await;
                stop.store(true, Ordering::Release);
            }
        });
    }

    let mut sessions = JoinSet::new();
    let mut next_start_id = config.start_id;
    for connection_index in 0..config.connections {
        let global_index = config.client_offset + connection_index;
        let requests = requests_for_connection(config.requests, config.total_clients, global_index);
        let session = TypedClientSession::new(&config, global_index, next_start_id, requests);
        next_start_id = next_start_id.wrapping_add(requests);
        let stats = stats.clone();
        let stop = stop.clone();
        sessions.spawn(async move { session.run(stats, stop).await });
    }

    while let Some(result) = sessions.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                stop.store(true, Ordering::Release);
                sessions.abort_all();
                return Err(err);
            }
            Err(err) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                stop.store(true, Ordering::Release);
                sessions.abort_all();
                return Err(io::Error::other(err));
            }
        }
    }

    if config.csv {
        let snapshot = stats.snapshot();
        println!(
            "{},{},{:.9},{:.9},{}",
            snapshot.commands,
            snapshot.bytes_sent,
            started.elapsed().as_secs_f64(),
            cpu_seconds_since(start_cpu_ticks),
            process_rss_kib()
        );
    } else {
        stats.print_snapshot("final");
    }
    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub async fn run_typed_fetch(args: TypedFetchArgs) -> io::Result<()> {
    let response = fetch_typed_response(&args)
        .await
        .map_err(map_typed_client_error)?;
    io::stdout().write_all(response.as_bytes())
}

pub async fn fetch_typed_response(
    args: &TypedFetchArgs,
) -> Result<OwnedResponse, TypedClientError> {
    let request = typed_fetch_request(args)?;
    let connection = TypedClientConnection::connect_with_options(
        args.connect,
        TypedClientOptions {
            read_buffer_bytes: args
                .read_buffer_bytes
                .max(tail_buffer::TERMINATOR_TAIL_SIZE),
            nodelay: args.nodelay,
            socket_recv_buffer: args.socket_recv_buffer,
            socket_send_buffer: args.socket_send_buffer,
            pipeline_depth: args.pipeline_depth.clamp(1, 4096),
            response_mode: TypedClientResponseMode::Owned,
        },
    )
    .await?;

    connection.execute(request).await
}

fn typed_fetch_request(args: &TypedFetchArgs) -> Result<Request<'static>, TypedClientError> {
    match args.request {
        TypedRequestKind::Article => {
            typed_fetch_message_id_request(args, |message_id| Request::article(message_id))
        }
        TypedRequestKind::Body => {
            typed_fetch_message_id_request(args, |message_id| Request::body(message_id))
        }
        TypedRequestKind::Head => {
            typed_fetch_message_id_request(args, |message_id| Request::head(message_id))
        }
        TypedRequestKind::Stat => {
            typed_fetch_message_id_request(args, |message_id| Request::stat(message_id))
        }
        TypedRequestKind::Group => typed_fetch_group_request(args, |group| Request::group(group)),
        TypedRequestKind::Listgroup => {
            typed_fetch_group_request(args, |group| Request::listgroup(group))
        }
        TypedRequestKind::Last => Ok(Request::last()),
        TypedRequestKind::Next => Ok(Request::next()),
        TypedRequestKind::Newgroups => typed_fetch_newgroups_request(args),
        TypedRequestKind::Newnews => typed_fetch_newnews_request(args),
        TypedRequestKind::Post => Ok(Request::post()),
        TypedRequestKind::Ihave => {
            typed_fetch_message_id_request(args, |message_id| Request::ihave(message_id))
        }
        TypedRequestKind::Check => {
            typed_fetch_message_id_request(args, |message_id| Request::check(message_id))
        }
        TypedRequestKind::Takethis => typed_fetch_takethis_request(args),
        TypedRequestKind::AuthinfoUser => {
            typed_fetch_authinfo_request(args, |value| Request::authinfo_user(value))
        }
        TypedRequestKind::AuthinfoPass => {
            typed_fetch_authinfo_request(args, |value| Request::authinfo_pass(value))
        }
        TypedRequestKind::Starttls => Ok(Request::starttls()),
        TypedRequestKind::Over => {
            typed_fetch_selector_request(args, |selector| Request::over(selector))
        }
        TypedRequestKind::Xover => {
            typed_fetch_selector_request(args, |selector| Request::xover(selector))
        }
        TypedRequestKind::Hdr => {
            typed_fetch_header_request(args, |header, selector| Request::hdr(header, selector))
        }
        TypedRequestKind::Xhdr => {
            typed_fetch_header_request(args, |header, selector| Request::xhdr(header, selector))
        }
        TypedRequestKind::List => Ok(Request::list()),
        TypedRequestKind::Help => Ok(Request::help()),
        TypedRequestKind::Capabilities => Ok(Request::capabilities()),
        TypedRequestKind::Date => Ok(Request::date()),
        TypedRequestKind::ModeReader => Ok(Request::mode_reader()),
        TypedRequestKind::Quit => Ok(Request::quit()),
    }
}

fn typed_fetch_message_id_request<F>(
    args: &TypedFetchArgs,
    build: F,
) -> Result<Request<'static>, TypedClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, protocol::InvalidMessageId>,
{
    let message_id = args
        .message_id
        .as_deref()
        .ok_or(TypedClientError::MissingMessageId)?;
    build(message_id).map_err(|_| TypedClientError::InvalidMessageId)
}

fn typed_fetch_header_request<F>(
    args: &TypedFetchArgs,
    build: F,
) -> Result<Request<'static>, TypedClientError>
where
    F: FnOnce(&str, &str) -> Result<Request<'static>, crate::protocol::InvalidHeaderQuery>,
{
    let header = args
        .header
        .as_deref()
        .ok_or(TypedClientError::MissingHeaderName)?;
    let selector = args
        .selector
        .as_deref()
        .ok_or(TypedClientError::MissingArticleSelector)?;
    build(header, selector).map_err(TypedClientError::from)
}

fn typed_fetch_group_request<F>(
    args: &TypedFetchArgs,
    build: F,
) -> Result<Request<'static>, TypedClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, crate::protocol::InvalidGroupName>,
{
    let group = args
        .group
        .as_deref()
        .ok_or(TypedClientError::MissingGroupName)?;
    build(group).map_err(|_| TypedClientError::InvalidGroupName)
}

fn typed_fetch_newgroups_request(
    args: &TypedFetchArgs,
) -> Result<Request<'static>, TypedClientError> {
    let date = args.date.as_deref().ok_or(TypedClientError::MissingDate)?;
    let time = args.time.as_deref().ok_or(TypedClientError::MissingTime)?;
    Request::newgroups(date, time, args.gmt).map_err(TypedClientError::from)
}

fn typed_fetch_newnews_request(
    args: &TypedFetchArgs,
) -> Result<Request<'static>, TypedClientError> {
    let wildmat = args
        .wildmat
        .as_deref()
        .ok_or(TypedClientError::MissingWildmat)?;
    let date = args.date.as_deref().ok_or(TypedClientError::MissingDate)?;
    let time = args.time.as_deref().ok_or(TypedClientError::MissingTime)?;
    Request::newnews(wildmat, date, time, args.gmt).map_err(TypedClientError::from)
}

fn typed_fetch_selector_request<F>(
    args: &TypedFetchArgs,
    build: F,
) -> Result<Request<'static>, TypedClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, crate::protocol::InvalidArticleSelector>,
{
    let selector = args
        .selector
        .as_deref()
        .ok_or(TypedClientError::MissingArticleSelector)?;
    build(selector).map_err(|_| TypedClientError::InvalidArticleSelector)
}

fn typed_fetch_authinfo_request<F>(
    args: &TypedFetchArgs,
    build: F,
) -> Result<Request<'static>, TypedClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, crate::protocol::InvalidAuthInfoValue>,
{
    let value = args
        .auth_value
        .as_deref()
        .ok_or(TypedClientError::MissingAuthInfoValue)?;
    build(value).map_err(TypedClientError::from)
}

fn typed_fetch_takethis_request(
    args: &TypedFetchArgs,
) -> Result<Request<'static>, TypedClientError> {
    let message_id = args
        .message_id
        .as_deref()
        .ok_or(TypedClientError::MissingMessageId)?;
    let article = args
        .article_body
        .as_deref()
        .ok_or(TypedClientError::MissingArticleBody)?;
    Request::takethis(message_id, article.as_bytes())
        .map_err(|_| TypedClientError::InvalidMessageId)
}

fn map_typed_client_error(err: TypedClientError) -> io::Error {
    match err {
        TypedClientError::Io(err) => err,
        TypedClientError::UnexpectedEof => io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "server closed before completing response",
        ),
        TypedClientError::ConnectionClosed => {
            io::Error::new(io::ErrorKind::BrokenPipe, "connection engine closed")
        }
        other => io::Error::new(io::ErrorKind::InvalidData, other),
    }
}

#[derive(Debug, Clone)]
struct TypedClientConfig {
    connect: SocketAddr,
    ports: Box<[u16]>,
    segments: Option<Arc<SegmentSet>>,
    requests: u64,
    transfer_bytes: u64,
    duration: Duration,
    connections: usize,
    client_offset: usize,
    total_clients: usize,
    pipeline_depth: usize,
    command_mix: ClientCommandMix,
    start_id: u64,
    read_buffer_bytes: usize,
    nodelay: bool,
    socket_recv_buffer: usize,
    socket_send_buffer: usize,
    csv: bool,
    stats_interval: Duration,
}

impl TypedClientConfig {
    fn from_client_args(args: ClientArgs) -> io::Result<Self> {
        Self::build(
            args.connect,
            args.ports.into_boxed_slice(),
            args.segments,
            args.requests,
            args.transfer_bytes,
            args.duration_secs,
            args.connections,
            args.client_offset,
            args.total_clients,
            args.pipeline_depth,
            args.command_mix,
            args.start_id,
            args.read_buffer_bytes,
            args.nodelay,
            args.socket_recv_buffer,
            args.socket_send_buffer,
            args.csv,
            args.stats_interval_secs,
        )
    }

    fn from_args(args: TypedClientArgs) -> io::Result<Self> {
        Self::build(
            args.connect,
            args.ports.into_boxed_slice(),
            args.segments,
            args.requests,
            args.transfer_bytes,
            args.duration_secs,
            args.connections,
            args.client_offset,
            args.total_clients,
            args.pipeline_depth,
            args.command_mix,
            args.start_id,
            args.read_buffer_bytes,
            args.nodelay,
            args.socket_recv_buffer,
            args.socket_send_buffer,
            args.csv,
            args.stats_interval_secs,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        connect: SocketAddr,
        ports: Box<[u16]>,
        segments: Option<PathBuf>,
        requests: u64,
        transfer_bytes: u64,
        duration_secs: u64,
        connections: usize,
        client_offset: usize,
        total_clients: usize,
        pipeline_depth: usize,
        command_mix: ClientCommandMix,
        start_id: u64,
        read_buffer_bytes: usize,
        nodelay: bool,
        socket_recv_buffer: usize,
        socket_send_buffer: usize,
        csv: bool,
        stats_interval_secs: u64,
    ) -> io::Result<Self> {
        let connections = connections.max(1);
        let total_clients = if total_clients == 0 {
            connections
        } else {
            total_clients
        };
        let segments = segments
            .as_deref()
            .map(read_segments)
            .transpose()?
            .map(Arc::new);

        Ok(Self {
            connect,
            ports,
            segments,
            requests,
            transfer_bytes,
            duration: Duration::from_secs(duration_secs),
            connections,
            client_offset,
            total_clients,
            pipeline_depth: pipeline_depth.clamp(1, 4096),
            command_mix,
            start_id,
            read_buffer_bytes: read_buffer_bytes.max(tail_buffer::TERMINATOR_TAIL_SIZE),
            nodelay,
            socket_recv_buffer,
            socket_send_buffer,
            csv,
            stats_interval: Duration::from_secs(stats_interval_secs),
        })
    }

    fn endpoint_for(&self, global_index: usize) -> SocketAddr {
        let mut connect = self.connect;
        if !self.ports.is_empty() {
            connect.set_port(self.ports[global_index % self.ports.len()]);
        }
        connect
    }
}

#[derive(Debug)]
struct TypedClientSession {
    connect: SocketAddr,
    segments: Option<Arc<SegmentSet>>,
    client_index: usize,
    total_clients: usize,
    requests: u64,
    transfer_bytes: u64,
    next_id: u64,
    pipeline_depth: usize,
    command_mix: ClientCommandMix,
    read_buffer_bytes: usize,
    nodelay: bool,
    socket_recv_buffer: usize,
    socket_send_buffer: usize,
}

impl TypedClientSession {
    fn new(config: &TypedClientConfig, global_index: usize, start_id: u64, requests: u64) -> Self {
        Self {
            connect: config.endpoint_for(global_index),
            segments: config.segments.clone(),
            client_index: global_index,
            total_clients: config.total_clients,
            requests,
            transfer_bytes: config.transfer_bytes,
            next_id: start_id,
            pipeline_depth: config.pipeline_depth,
            command_mix: config.command_mix,
            read_buffer_bytes: config.read_buffer_bytes,
            nodelay: config.nodelay,
            socket_recv_buffer: config.socket_recv_buffer,
            socket_send_buffer: config.socket_send_buffer,
        }
    }

    async fn run(self, stats: Arc<Stats>, stop: Arc<AtomicBool>) -> io::Result<()> {
        stats.accepted_connections.fetch_add(1, Ordering::Relaxed);
        stats.active_connections.fetch_add(1, Ordering::Relaxed);

        let result = self.run_inner(&stats, &stop).await;
        stats.active_connections.fetch_sub(1, Ordering::Relaxed);
        result
    }

    async fn run_inner(self, stats: &Stats, stop: &AtomicBool) -> io::Result<()> {
        let connection = TypedClientConnection::connect_with_options(
            self.connect,
            TypedClientOptions {
                read_buffer_bytes: self.read_buffer_bytes,
                nodelay: self.nodelay,
                socket_recv_buffer: self.socket_recv_buffer,
                socket_send_buffer: self.socket_send_buffer,
                pipeline_depth: self.pipeline_depth,
                response_mode: TypedClientResponseMode::Drained,
            },
        )
        .await
        .map_err(map_typed_client_error)?;

        let mut pending: VecDeque<(u64, crate::typed_client::PendingDrainedResponse)> =
            VecDeque::with_capacity(self.pipeline_depth);
        let mut issued = 0_u64;

        self.fill_typed_pipeline(&connection, &mut pending, &mut issued, stats, stop)
            .await?;
        if pending.len() > 1 {
            stats.pipeline_batches.fetch_add(1, Ordering::Relaxed);
        }

        while let Some((command_id, pending_response)) = pending.pop_front() {
            let response = pending_response
                .receive()
                .await
                .map_err(map_typed_client_error)?;

            stats
                .bytes_sent
                .fetch_add(response.bytes_len() as u64, Ordering::Relaxed);
            stats.commands.fetch_add(1, Ordering::Relaxed);
            match client_command_kind(command_id, self.command_mix) {
                ClientCommandMix::Article | ClientCommandMix::Alternate => {
                    stats.article_requests.fetch_add(1, Ordering::Relaxed);
                }
                ClientCommandMix::Body => {
                    stats.body_requests.fetch_add(1, Ordering::Relaxed);
                }
            }

            if self.transfer_bytes != 0
                && stats.bytes_sent.load(Ordering::Relaxed) >= self.transfer_bytes
            {
                stop.store(true, Ordering::Release);
            }

            let before = pending.len();
            self.fill_typed_pipeline(&connection, &mut pending, &mut issued, stats, stop)
                .await?;
            if pending.len().saturating_sub(before) > 1 {
                stats.pipeline_batches.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    async fn fill_typed_pipeline(
        &self,
        connection: &TypedClientConnection,
        pending: &mut VecDeque<(u64, crate::typed_client::PendingDrainedResponse)>,
        issued: &mut u64,
        stats: &Stats,
        stop: &AtomicBool,
    ) -> io::Result<()> {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }

        while pending.len() < self.pipeline_depth
            && (self.requests == 0 || *issued < self.requests)
            && !transfer_limit_reached(stats, self.transfer_bytes)
        {
            let command_id = self.next_id.wrapping_add(*issued);
            let request_index = *issued;
            let request = typed_request_for_command(
                command_id,
                request_index,
                self.command_mix,
                self.segments.as_deref(),
                self.client_index,
                self.total_clients,
            )?;
            let pending_response = connection
                .queue_request_drained(request)
                .await
                .map_err(map_typed_client_error)?;
            pending.push_back((command_id, pending_response));
            *issued = issued.wrapping_add(1);
        }

        Ok(())
    }
}

fn transfer_limit_reached(stats: &Stats, transfer_bytes: u64) -> bool {
    transfer_bytes != 0 && stats.bytes_sent.load(Ordering::Relaxed) >= transfer_bytes
}

fn typed_request_for_command(
    command_id: u64,
    request_index: u64,
    mix: ClientCommandMix,
    segments: Option<&SegmentSet>,
    client_index: usize,
    total_clients: usize,
) -> io::Result<Request<'static>> {
    let kind = client_command_kind(command_id, mix);
    let message_id = request_message_id_for_command(
        kind,
        command_id,
        request_index,
        segments,
        client_index,
        total_clients,
    )?;
    match kind {
        ClientCommandMix::Article | ClientCommandMix::Alternate => Request::article(message_id)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid article message-id")),
        ClientCommandMix::Body => Request::body(message_id)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid body message-id")),
    }
}

fn request_message_id_for_command(
    kind: ClientCommandMix,
    command_id: u64,
    request_index: u64,
    segments: Option<&SegmentSet>,
    client_index: usize,
    total_clients: usize,
) -> io::Result<String> {
    if let Some(segments) = segments {
        let message_id = std::str::from_utf8(segment_for_request(
            segments,
            client_index,
            total_clients,
            request_index,
        ))
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "segment message-id is not utf-8",
            )
        })?;
        return Ok(message_id.to_owned());
    }

    let prefix = match kind {
        ClientCommandMix::Article | ClientCommandMix::Alternate => "bench.",
        ClientCommandMix::Body => "bench.",
    };
    Ok(format!("<{prefix}{command_id}@nntpbench.local>"))
}

fn requests_for_connection(total: u64, connections: usize, index: usize) -> u64 {
    if total == 0 {
        return 0;
    }

    let base = total / connections as u64;
    let remainder = total % connections as u64;
    base + u64::from((index as u64) < remainder)
}

fn read_segments(path: &std::path::Path) -> io::Result<SegmentSet> {
    let contents = fs::read_to_string(path)?;
    let mut ids = Vec::new();

    for (line_index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let (_size, msgid) = line.split_once('\t').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid segment line {}: expected SIZE<TAB>MSGID",
                    line_index + 1
                ),
            )
        })?;
        let id = normalize_msgid(msgid.trim());
        ids.push(id.into_boxed_slice());
    }

    if ids.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no segments found in {}", path.display()),
        ));
    }

    Ok(SegmentSet {
        ids: ids.into_boxed_slice(),
    })
}

fn normalize_msgid(value: &str) -> Vec<u8> {
    if value.starts_with('<') && value.ends_with('>') {
        return value.as_bytes().to_vec();
    }

    let mut normalized = Vec::with_capacity(value.len() + 2);
    normalized.push(b'<');
    normalized.extend_from_slice(value.as_bytes());
    normalized.push(b'>');
    normalized
}

async fn connect_client_socket(
    addr: SocketAddr,
    nodelay: bool,
    recv_buffer: usize,
    send_buffer: usize,
) -> io::Result<TcpStream> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    if recv_buffer != 0 {
        socket.set_recv_buffer_size(socket_buffer_size_u32(recv_buffer)?)?;
    }
    if send_buffer != 0 {
        socket.set_send_buffer_size(socket_buffer_size_u32(send_buffer)?)?;
    }
    socket.set_nodelay(nodelay)?;
    socket.connect(addr).await
}

fn socket_buffer_size_u32(size: usize) -> io::Result<u32> {
    u32::try_from(size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket buffer size exceeds u32::MAX",
        )
    })
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn optimize_server_socket(stream: &TcpStream, config: &ServerConfig) -> io::Result<()> {
    stream.set_nodelay(config.nodelay)?;

    let socket = SockRef::from(stream);
    if config.socket_recv_buffer != 0 {
        socket.set_recv_buffer_size(config.socket_recv_buffer)?;
    }
    if config.socket_send_buffer != 0 {
        socket.set_send_buffer_size(config.socket_send_buffer)?;
    }
    socket.set_linger(Some(TCP_LINGER_TIMEOUT))?;

    #[cfg(target_os = "linux")]
    {
        let _ = socket.set_tcp_user_timeout(Some(TCP_USER_TIMEOUT));
        let _ = socket.set_tos_v4(TOS_THROUGHPUT);
    }

    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn process_cpu_ticks() -> Option<u64> {
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    let rest = stat.rsplit_once(") ")?.1;
    let mut fields = rest.split_whitespace();
    let utime = fields.nth(11)?.parse::<u64>().ok()?;
    let stime = fields.next()?.parse::<u64>().ok()?;
    Some(utime + stime)
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn cpu_seconds_since(start: Option<u64>) -> f64 {
    let Some(start) = start else {
        return 0.0;
    };
    let Some(end) = process_cpu_ticks() else {
        return 0.0;
    };

    end.saturating_sub(start) as f64 * PROCESS_CLOCK_TICK.as_secs_f64()
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn process_rss_kib() -> u64 {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return 0;
    };

    let mut rss = 0;
    for line in status.lines() {
        if let Some(value) = line
            .strip_prefix("VmHWM:")
            .or_else(|| line.strip_prefix("VmRSS:"))
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
        {
            rss = value;
            if line.starts_with("VmHWM:") {
                break;
            }
        }
    }
    rss
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn read_greeting(stream: &mut TcpStream, buffer: &mut [u8]) -> io::Result<()> {
    let mut total = 0;
    loop {
        if total == buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "server greeting exceeded read buffer",
            ));
        }

        let read = stream.read(&mut buffer[total..]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server closed before greeting",
            ));
        }

        let end = total + read;
        if memchr::memchr(b'\n', &buffer[total..end]).is_some() {
            return Ok(());
        }
        total = end;
    }
}

fn segment_for_request(
    segments: &SegmentSet,
    client_index: usize,
    total_clients: usize,
    request_id: u64,
) -> &[u8] {
    let index =
        (client_index + (request_id as usize).wrapping_mul(total_clients)) % segments.ids.len();
    &segments.ids[index]
}

fn client_command_kind(id: u64, mix: ClientCommandMix) -> ClientCommandMix {
    match mix {
        ClientCommandMix::Alternate if id.is_multiple_of(2) => ClientCommandMix::Body,
        ClientCommandMix::Alternate => ClientCommandMix::Article,
        command => command,
    }
}

pub async fn serve_session(
    stream: TcpStream,
    peer_addr: SocketAddr,
    config: Arc<ServerConfig>,
    stats: Arc<Stats>,
) -> io::Result<()> {
    let mut session_stats = SessionStats::default();
    let result = serve_session_inner(stream, peer_addr, config, &mut session_stats).await;
    stats.add_session(&session_stats);
    result
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn serve_session_inner(
    mut stream: TcpStream,
    _peer_addr: SocketAddr,
    config: Arc<ServerConfig>,
    session_stats: &mut SessionStats,
) -> io::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::with_capacity(SERVER_READER_CAPACITY, reader);
    let max_pipeline_depth = config.max_pipeline_depth.min(MAX_SERVER_PIPELINE_DEPTH);
    let mut command_arena =
        Vec::with_capacity(MAX_COMMAND_LINE_BYTES.saturating_mul(max_pipeline_depth));
    let mut command_scratch = Vec::with_capacity(MAX_COMMAND_LINE_BYTES);
    let mut command_batch = CommandBatch::new();
    let mut pending_write = PendingWrite::new();

    send_greeting(&mut writer, &config, session_stats).await?;

    loop {
        if !read_command_batch(
            &mut reader,
            &mut command_arena,
            &mut command_scratch,
            &mut command_batch,
            max_pipeline_depth,
        )
        .await?
        {
            break;
        }

        if process_command_batch(
            &command_batch,
            &command_arena,
            &config,
            session_stats,
            &mut writer,
            &mut pending_write,
        )
        .await?
        .should_close()
        {
            return Ok(());
        }
    }

    flush_session_writer(&mut writer, &mut pending_write, &config).await?;

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchOutcome {
    Continue,
    Close,
}

impl BatchOutcome {
    const fn should_close(self) -> bool {
        matches!(self, Self::Close)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn send_greeting<W>(
    writer: &mut W,
    config: &ServerConfig,
    session_stats: &mut SessionStats,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(GREETING).await?;
    session_stats.bytes_sent += GREETING.len() as u64;
    flush_writer_if_needed(writer, config.flush).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn process_command_batch<W>(
    command_batch: &CommandBatch,
    command_arena: &[u8],
    config: &ServerConfig,
    session_stats: &mut SessionStats,
    writer: &mut W,
    pending_write: &mut PendingWrite,
) -> io::Result<BatchOutcome>
where
    W: AsyncWrite + Unpin,
{
    session_stats.pipeline_batches += u64::from(command_batch.len() > 1);

    for command in command_batch {
        if handle_command(
            command,
            command_arena,
            config,
            session_stats,
            writer,
            pending_write,
        )
        .await?
        {
            flush_session_writer(writer, pending_write, config).await?;
            return Ok(BatchOutcome::Close);
        }
    }

    flush_session_writer(writer, pending_write, config).await?;
    Ok(BatchOutcome::Continue)
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn handle_command<W>(
    command: &ParsedCommand,
    command_arena: &[u8],
    config: &ServerConfig,
    session_stats: &mut SessionStats,
    writer: &mut W,
    pending_write: &mut PendingWrite,
) -> io::Result<bool>
where
    W: AsyncWrite + Unpin,
{
    session_stats.commands += 1;
    let request = command.request_line(command_arena);

    match request.kind() {
        RequestKind::Article => {
            session_stats.article_requests += 1;
            write_response(
                writer,
                pending_write,
                config.article_response(),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Body => {
            session_stats.body_requests += 1;
            write_response(writer, pending_write, config.body_response(), session_stats).await?;
            Ok(false)
        }
        RequestKind::Group => {
            write_response(writer, pending_write, GROUP_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::ListGroup => {
            write_response(writer, pending_write, LISTGROUP_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Last => {
            write_response(writer, pending_write, LAST_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Next => {
            write_response(writer, pending_write, NEXT_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::NewGroups => {
            write_response(writer, pending_write, NEWGROUPS_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::NewNews => {
            write_response(writer, pending_write, NEWNEWS_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Post => {
            write_response(writer, pending_write, POST_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Ihave => {
            write_response(writer, pending_write, IHAVE_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Check => {
            write_response(writer, pending_write, CHECK_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::TakeThis => {
            write_response(writer, pending_write, TAKETHIS_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::AuthInfoUser => {
            write_response(writer, pending_write, AUTHINFO_USER_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::AuthInfoPass => {
            write_response(writer, pending_write, AUTHINFO_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::AuthInfo => {
            write_response(writer, pending_write, AUTHINFO_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::StartTls => {
            write_response(writer, pending_write, STARTTLS_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Over => {
            write_response(writer, pending_write, OVER_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Xover => {
            write_response(writer, pending_write, XOVER_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Hdr => {
            write_response(writer, pending_write, HDR_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Xhdr => {
            write_response(writer, pending_write, XHDR_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Capabilities => {
            write_response(
                writer,
                pending_write,
                b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::List => {
            write_response(writer, pending_write, LIST_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Help => {
            write_response(writer, pending_write, HELP_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Date => {
            write_response(writer, pending_write, DATE_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::ModeReader => {
            write_response(writer, pending_write, MODE_READER_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Quit => {
            write_response(writer, pending_write, QUIT_RESPONSE, session_stats).await?;
            Ok(true)
        }
        _ => {
            write_response(
                writer,
                pending_write,
                b"500 unknown command\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
    }
}

struct PendingWrite {
    buf: Box<[u8; MAX_PENDING_WRITE_BYTES]>,
    len: usize,
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl PendingWrite {
    fn new() -> Self {
        Self {
            buf: Box::new([0; MAX_PENDING_WRITE_BYTES]),
            len: 0,
        }
    }

    async fn push<W>(&mut self, writer: &mut W, response: &[u8]) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        if response.len() > self.buf.len() {
            self.write_with_response(writer, response).await?;
            return Ok(());
        }

        if self.len + response.len() > self.buf.len() {
            self.flush(writer).await?;
        }

        let end = self.len + response.len();
        self.buf[self.len..end].copy_from_slice(response);
        self.len = end;
        Ok(())
    }

    async fn write_with_response<W>(&mut self, writer: &mut W, response: &[u8]) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        if self.len == 0 {
            writer.write_all(response).await?;
            return Ok(());
        }

        let mut slices = [IoSlice::new(&self.buf[..self.len]), IoSlice::new(response)];
        write_all_vectored(writer, &mut slices).await?;
        self.len = 0;
        Ok(())
    }

    async fn flush<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        if self.len == 0 {
            return Ok(());
        }

        writer.write_all(&self.buf[..self.len]).await?;
        self.len = 0;
        Ok(())
    }
}

async fn write_all_vectored<W>(writer: &mut W, slices: &mut [IoSlice<'_>]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut remaining = slices;
    while !remaining.is_empty() {
        let written =
            poll_fn(|cx| Pin::new(&mut *writer).poll_write_vectored(cx, remaining)).await?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write buffers",
            ));
        }
        IoSlice::advance_slices(&mut remaining, written);
    }

    Ok(())
}

pub fn process_request_to_buffer(
    request: RequestLine<'_>,
    config: &ServerConfig,
    stats: &Stats,
    output: &mut Vec<u8>,
) -> bool {
    stats.commands.fetch_add(1, Ordering::Relaxed);

    let response = match request.kind() {
        RequestKind::Article => {
            stats.article_requests.fetch_add(1, Ordering::Relaxed);
            config.article_response()
        }
        RequestKind::Body => {
            stats.body_requests.fetch_add(1, Ordering::Relaxed);
            config.body_response()
        }
        RequestKind::Group => GROUP_RESPONSE,
        RequestKind::ListGroup => LISTGROUP_RESPONSE,
        RequestKind::Last => LAST_RESPONSE,
        RequestKind::Next => NEXT_RESPONSE,
        RequestKind::NewGroups => NEWGROUPS_RESPONSE,
        RequestKind::NewNews => NEWNEWS_RESPONSE,
        RequestKind::Post => POST_RESPONSE,
        RequestKind::Ihave => IHAVE_RESPONSE,
        RequestKind::Check => CHECK_RESPONSE,
        RequestKind::TakeThis => TAKETHIS_RESPONSE,
        RequestKind::AuthInfoUser => AUTHINFO_RESPONSE,
        RequestKind::AuthInfoPass => AUTHINFO_RESPONSE,
        RequestKind::AuthInfo => AUTHINFO_RESPONSE,
        RequestKind::StartTls => STARTTLS_RESPONSE,
        RequestKind::Over => OVER_RESPONSE,
        RequestKind::Xover => XOVER_RESPONSE,
        RequestKind::Hdr => HDR_RESPONSE,
        RequestKind::Xhdr => XHDR_RESPONSE,
        RequestKind::List => LIST_RESPONSE,
        RequestKind::Capabilities => b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        RequestKind::Help => HELP_RESPONSE,
        RequestKind::Date => DATE_RESPONSE,
        RequestKind::ModeReader => MODE_READER_RESPONSE,
        RequestKind::Quit => QUIT_RESPONSE,
        _ => b"500 unknown command\r\n",
    };

    stats
        .bytes_sent
        .fetch_add(response.len() as u64, Ordering::Relaxed);
    output.extend_from_slice(response);
    matches!(request.kind(), RequestKind::Quit)
}

pub fn for_each_request_line_in_batch<F>(
    input: &[u8],
    max_pipeline_depth: usize,
    mut visit: F,
) -> usize
where
    F: FnMut(RequestLine<'_>),
{
    let mut start = 0;
    let mut count = 0;

    while count < max_pipeline_depth {
        let Some(relative_lf) = memchr::memchr(b'\n', &input[start..]) else {
            break;
        };
        let end = start + relative_lf + 1;
        let line = trim_line_slice(&input[start..end]);
        let request = RequestLine::parse(line);
        visit(request);
        count += 1;
        if matches!(request.kind(), RequestKind::TakeThis) {
            let Some(body_end) = find_dot_terminated_block_end(input, end) else {
                break;
            };
            start = body_end;
            continue;
        }
        start = end;
    }

    start
}

fn find_dot_terminated_block_end(input: &[u8], start: usize) -> Option<usize> {
    let mut cursor = start;
    loop {
        let relative_lf = memchr::memchr(b'\n', &input[cursor..])?;
        let end = cursor + relative_lf + 1;
        if trim_line_slice(&input[cursor..end]) == b"." {
            return Some(end);
        }
        cursor = end;
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn write_response<W>(
    writer: &mut W,
    pending_write: &mut PendingWrite,
    response: &[u8],
    session_stats: &mut SessionStats,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    session_stats.bytes_sent += response.len() as u64;

    if response.len() <= 4096 {
        pending_write.push(writer, response).await?;
        return Ok(());
    }

    pending_write.write_with_response(writer, response).await
}

async fn flush_pending<W>(writer: &mut W, pending_write: &mut PendingWrite) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    pending_write.flush(writer).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn flush_session_writer<W>(
    writer: &mut W,
    pending_write: &mut PendingWrite,
    config: &ServerConfig,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    flush_pending(writer, pending_write).await?;
    flush_writer_if_needed(writer, config.flush).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn flush_writer_if_needed<W>(writer: &mut W, flush: bool) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if flush {
        #[cfg(not(coverage_nightly))]
        writer.flush().await?;
        #[cfg(coverage_nightly)]
        let _ = writer.flush().await;
    }

    Ok(())
}

#[derive(Debug)]
pub struct ServerConfig {
    pub body_bytes: usize,
    pub article_bytes: usize,
    pub max_connections: usize,
    pub max_pipeline_depth: usize,
    pub stats_interval: Duration,
    pub flush: bool,
    pub nodelay: bool,
    pub socket_recv_buffer: usize,
    pub socket_send_buffer: usize,
    body: Box<[u8]>,
    article: Box<[u8]>,
}

impl ServerConfig {
    pub fn from_args(args: ServerArgs) -> Self {
        let body = generate_body(args.body_bytes);
        let article = generate_article(args.article_bytes, &body);

        Self {
            body_bytes: args.body_bytes,
            article_bytes: args.article_bytes,
            max_connections: args.max_connections,
            max_pipeline_depth: args.max_pipeline_depth.clamp(1, 1024),
            stats_interval: Duration::from_secs(args.stats_interval_secs),
            flush: args.flush,
            nodelay: args.nodelay,
            socket_recv_buffer: args.socket_recv_buffer,
            socket_send_buffer: args.socket_send_buffer,
            body,
            article,
        }
    }

    pub fn article_response(&self) -> &[u8] {
        &self.article
    }

    pub fn body_response(&self) -> &[u8] {
        &self.body
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub fn bind_listener(addr: SocketAddr, backlog: i32, reuse_port: bool) -> io::Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;

    #[cfg(unix)]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }

    #[cfg(not(unix))]
    if reuse_port {
        eprintln!("SO_REUSEPORT requested but this build target does not support it");
    }

    socket.bind(&addr.into())?;
    socket.listen(backlog)?;
    TcpListener::from_std(socket.into())
}

#[derive(Debug)]
pub struct Stats {
    started: Instant,
    accepted_connections: AtomicU64,
    refused_connections: AtomicU64,
    active_connections: AtomicUsize,
    commands: AtomicU64,
    pipeline_batches: AtomicU64,
    article_requests: AtomicU64,
    body_requests: AtomicU64,
    bytes_sent: AtomicU64,
    errors: AtomicU64,
}

#[derive(Debug, Default)]
struct SessionStats {
    commands: u64,
    pipeline_batches: u64,
    article_requests: u64,
    body_requests: u64,
    bytes_sent: u64,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            accepted_connections: AtomicU64::new(0),
            refused_connections: AtomicU64::new(0),
            active_connections: AtomicUsize::new(0),
            commands: AtomicU64::new(0),
            pipeline_batches: AtomicU64::new(0),
            article_requests: AtomicU64::new(0),
            body_requests: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            at: Instant::now(),
            accepted_connections: self.accepted_connections.load(Ordering::Relaxed),
            refused_connections: self.refused_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            commands: self.commands.load(Ordering::Relaxed),
            pipeline_batches: self.pipeline_batches.load(Ordering::Relaxed),
            article_requests: self.article_requests.load(Ordering::Relaxed),
            body_requests: self.body_requests.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    pub fn print_snapshot(&self, label: &str) {
        let snapshot = self.snapshot();
        snapshot.print(label, self.started, None);
    }

    fn add_session(&self, session: &SessionStats) {
        self.commands.fetch_add(session.commands, Ordering::Relaxed);
        self.pipeline_batches
            .fetch_add(session.pipeline_batches, Ordering::Relaxed);
        self.article_requests
            .fetch_add(session.article_requests, Ordering::Relaxed);
        self.body_requests
            .fetch_add(session.body_requests, Ordering::Relaxed);
        self.bytes_sent
            .fetch_add(session.bytes_sent, Ordering::Relaxed);
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub at: Instant,
    pub accepted_connections: u64,
    pub refused_connections: u64,
    pub active_connections: usize,
    pub commands: u64,
    pub pipeline_batches: u64,
    pub article_requests: u64,
    pub body_requests: u64,
    pub bytes_sent: u64,
    pub errors: u64,
}

impl Snapshot {
    pub fn print(self, label: &str, started: Instant, previous: Option<Self>) {
        let elapsed = self.at.duration_since(started).as_secs_f64().max(0.001);
        let window = previous
            .map(|previous| self.at.duration_since(previous.at).as_secs_f64().max(0.001))
            .unwrap_or(elapsed);

        let previous_connections = previous.map_or(0, |snapshot| snapshot.accepted_connections);
        let previous_commands = previous.map_or(0, |snapshot| snapshot.commands);
        let previous_articles = previous.map_or(0, |snapshot| snapshot.article_requests);
        let previous_bodies = previous.map_or(0, |snapshot| snapshot.body_requests);
        let previous_bytes = previous.map_or(0, |snapshot| snapshot.bytes_sent);

        eprintln!(
            "{label} elapsed={elapsed:.1}s active={} conns={} conn/s={:.0} cmds={} cmd/s={:.0} pipeline_batches={} article/s={:.0} body/s={:.0} throughput={:.2} MiB/s refused={} errors={}",
            self.active_connections,
            self.accepted_connections,
            rate(self.accepted_connections - previous_connections, window),
            self.commands,
            rate(self.commands - previous_commands, window),
            self.pipeline_batches,
            rate(self.article_requests - previous_articles, window),
            rate(self.body_requests - previous_bodies, window),
            rate(self.bytes_sent - previous_bytes, window) / (1024.0 * 1024.0),
            self.refused_connections,
            self.errors,
        );
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn report_stats(stats: Arc<Stats>, interval: Duration) {
    let mut ticker = time::interval(interval);
    let mut previous = stats.snapshot();

    loop {
        ticker.tick().await;
        let snapshot = stats.snapshot();
        snapshot.print("stats", stats.started, Some(previous));
        previous = snapshot;
    }
}

pub fn rate(count: u64, seconds: f64) -> f64 {
    count as f64 / seconds
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedCommand {
    kind: RequestKind,
    line_range: Range<usize>,
}

impl ParsedCommand {
    fn request_line<'a>(&self, command_arena: &'a [u8]) -> RequestLine<'a> {
        RequestLine::parse(&command_arena[self.line_range.clone()])
    }
}

type CommandBatch = ArrayVec<ParsedCommand, MAX_SERVER_PIPELINE_DEPTH>;

#[cfg_attr(coverage_nightly, coverage(off))]
async fn read_command_batch<R>(
    reader: &mut BufReader<R>,
    command_arena: &mut Vec<u8>,
    command_scratch: &mut Vec<u8>,
    command_batch: &mut CommandBatch,
    max_pipeline_depth: usize,
) -> io::Result<bool>
where
    R: tokio::io::AsyncRead + Unpin,
{
    command_batch.clear();
    command_arena.clear();
    let line_start = command_arena.len();
    let read = reader.read_until(b'\n', command_arena).await?;
    if read == 0 {
        return Ok(false);
    }
    push_command(
        reader,
        command_arena,
        line_start,
        command_scratch,
        command_batch,
    )
    .await?;

    while command_batch.len() < max_pipeline_depth {
        if memchr::memchr(b'\n', reader.buffer()).is_none() {
            break;
        }

        let line_start = command_arena.len();
        reader.read_until(b'\n', command_arena).await?;
        push_command(
            reader,
            command_arena,
            line_start,
            command_scratch,
            command_batch,
        )
        .await?;
    }

    Ok(true)
}

async fn push_command<R>(
    reader: &mut BufReader<R>,
    command_arena: &[u8],
    line_start: usize,
    command_scratch: &mut Vec<u8>,
    command_batch: &mut CommandBatch,
) -> io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let command = parse_command_line(command_arena, line_start);
    let kind = command.kind;
    if matches!(kind, RequestKind::TakeThis) {
        read_dot_terminated_body(reader, command_scratch).await?;
    }
    command_batch.push(command);
    Ok(())
}

fn parse_command_line(command_arena: &[u8], line_start: usize) -> ParsedCommand {
    let line_end = line_start + trim_command_len(&command_arena[line_start..]);
    ParsedCommand {
        kind: RequestLine::parse(&command_arena[line_start..line_end]).kind(),
        line_range: line_start..line_end,
    }
}

fn trim_command_len(line: &[u8]) -> usize {
    let mut end = line.len();
    while end > 0 && matches!(line[end - 1], b'\r' | b'\n') {
        end -= 1;
    }
    end
}

#[cfg(test)]
fn trim_command_line(line: &mut Vec<u8>) {
    while matches!(line.last(), Some(b'\r' | b'\n')) {
        line.pop();
    }
}

fn trim_line_slice(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r\n")
        .or_else(|| line.strip_suffix(b"\n"))
        .unwrap_or(line)
}

async fn read_dot_terminated_body<R>(
    reader: &mut BufReader<R>,
    command_scratch: &mut Vec<u8>,
) -> io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        command_scratch.clear();
        let read = reader.read_until(b'\n', command_scratch).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated TAKETHIS body",
            ));
        }
        if trim_command_terminator(command_scratch) == b"." {
            return Ok(());
        }
    }
}

fn trim_command_terminator(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r\n")
        .or_else(|| line.strip_suffix(b"\n"))
        .unwrap_or(line)
}

pub fn generate_article(target_bytes: usize, body: &[u8]) -> Box<[u8]> {
    let mut response = Vec::with_capacity(target_bytes.max(256) + TERMINATOR.len());
    response.extend_from_slice(b"220 1 <article.1@nntpbench.local> article follows\r\n");
    response.extend_from_slice(b"Path: nntpbench.local!mock\r\n");
    response.extend_from_slice(b"From: Bench User <bench@nntpbench.local>\r\n");
    response.extend_from_slice(b"Newsgroups: alt.binaries.bench\r\n");
    response.extend_from_slice(b"Subject: nntpbench synthetic article\r\n");
    response.extend_from_slice(b"Message-ID: <article.1@nntpbench.local>\r\n");
    response.extend_from_slice(b"Date: Fri, 15 May 2026 00:00:00 +0000\r\n");
    response.extend_from_slice(b"\r\n");
    append_repeated_payload(&mut response, body_payload(body), target_bytes);
    ensure_terminated(&mut response);
    response.into_boxed_slice()
}

pub fn generate_body(target_bytes: usize) -> Box<[u8]> {
    let mut response = Vec::with_capacity(target_bytes.max(128) + TERMINATOR.len());
    response.extend_from_slice(b"222 1 <article.1@nntpbench.local> body follows\r\n");
    append_repeated_payload(&mut response, BODY_LINE, target_bytes);
    ensure_terminated(&mut response);
    response.into_boxed_slice()
}

fn body_payload(body: &[u8]) -> &[u8] {
    let header_end = memchr::memmem::find(body, CRLF).map_or(0, |idx| idx + CRLF.len());
    body[header_end..]
        .strip_suffix(b".\r\n")
        .unwrap_or(&body[header_end..])
}

const BODY_LINE: &[u8] =
    b"This is synthetic NNTP article payload for throughput and latency benchmarking\r\n";

fn append_repeated_payload(response: &mut Vec<u8>, line: &[u8], target_bytes: usize) {
    let target_before_dot_line = target_bytes.saturating_sub(b".\r\n".len());
    while response.len() + line.len() <= target_before_dot_line {
        response.extend_from_slice(line);
    }

    let remaining = target_before_dot_line.saturating_sub(response.len());
    if remaining > 0 {
        response.extend_from_slice(&line[..remaining.min(line.len())]);
    }
}

fn ensure_terminated(response: &mut Vec<u8>) {
    if !response.ends_with(CRLF) {
        response.extend_from_slice(CRLF);
    }
    response.extend_from_slice(b".\r\n");
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncReadExt;

    fn test_args() -> ServerArgs {
        ServerArgs {
            listen: "127.0.0.1:0".parse().unwrap(),
            body_bytes: 1024,
            article_bytes: 2048,
            max_connections: 16,
            threads: 1,
            max_pipeline_depth: 8,
            backlog: 128,
            reuse_port: false,
            nodelay: true,
            socket_recv_buffer: HIGH_THROUGHPUT_SOCKET_BUFFER,
            socket_send_buffer: HIGH_THROUGHPUT_SOCKET_BUFFER,
            stats_interval_secs: 0,
            flush: false,
        }
    }

    fn test_config() -> Arc<ServerConfig> {
        Arc::new(ServerConfig::from_args(test_args()))
    }

    fn test_client_args() -> ClientArgs {
        ClientArgs {
            connect: "127.0.0.1:1199".parse().unwrap(),
            ports: Vec::new(),
            segments: None,
            requests: 0,
            transfer_bytes: 0,
            duration_secs: 0,
            connections: 1,
            client_offset: 0,
            total_clients: 0,
            threads: 1,
            pipeline_depth: 64,
            command_mix: ClientCommandMix::Alternate,
            start_id: 1,
            read_buffer_bytes: CLIENT_READER_CAPACITY,
            nodelay: true,
            socket_recv_buffer: HIGH_THROUGHPUT_SOCKET_BUFFER,
            socket_send_buffer: HIGH_THROUGHPUT_SOCKET_BUFFER,
            csv: false,
            stats_interval_secs: 0,
        }
    }

    fn test_typed_fetch_args() -> TypedFetchArgs {
        TypedFetchArgs {
            connect: "127.0.0.1:1199".parse().unwrap(),
            request: TypedRequestKind::Article,
            message_id: Some("bench@test".to_string()),
            article_body: None,
            auth_value: None,
            header: None,
            group: None,
            wildmat: None,
            date: None,
            time: None,
            gmt: true,
            selector: None,
            threads: 1,
            read_buffer_bytes: CLIENT_READER_CAPACITY,
            pipeline_depth: 64,
            nodelay: true,
            socket_recv_buffer: HIGH_THROUGHPUT_SOCKET_BUFFER,
            socket_send_buffer: HIGH_THROUGHPUT_SOCKET_BUFFER,
        }
    }

    fn test_typed_client_args() -> TypedClientArgs {
        TypedClientArgs {
            connect: "127.0.0.1:1199".parse().unwrap(),
            ports: Vec::new(),
            segments: None,
            requests: 0,
            transfer_bytes: 0,
            duration_secs: 0,
            connections: 1,
            client_offset: 0,
            total_clients: 0,
            threads: 1,
            pipeline_depth: 64,
            command_mix: ClientCommandMix::Alternate,
            start_id: 1,
            read_buffer_bytes: CLIENT_READER_CAPACITY,
            nodelay: true,
            socket_recv_buffer: HIGH_THROUGHPUT_SOCKET_BUFFER,
            socket_send_buffer: HIGH_THROUGHPUT_SOCKET_BUFFER,
            csv: false,
            stats_interval_secs: 0,
        }
    }

    fn write_temp_segments(name: &str, contents: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = format!(
            "nntpbench-{name}-{}-{}.segments",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        path.push(unique);
        fs::write(&path, contents).unwrap();
        path
    }

    async fn run_session_with_input(
        config: Arc<ServerConfig>,
        input: &[u8],
    ) -> (Vec<u8>, Arc<Stats>) {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(Stats::new());

        let server = tokio::spawn({
            let config = config.clone();
            let stats = stats.clone();
            async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                serve_session(stream, peer_addr, config, stats)
                    .await
                    .unwrap();
            }
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(input).await.unwrap();
        client.shutdown().await.unwrap();

        let mut output = Vec::new();
        client.read_to_end(&mut output).await.unwrap();
        server.await.unwrap();

        (output, stats)
    }

    #[tokio::test]
    async fn pending_write_flushes_when_full_and_writes_oversized_directly() {
        let mut sink = tokio::io::sink();
        let mut pending = PendingWrite::new();
        let first = [b'a'; 40 * 1024];
        let second = [b'b'; 40 * 1024];
        pending.push(&mut sink, &first).await.unwrap();
        assert_eq!(pending.len, first.len());
        pending.push(&mut sink, &second).await.unwrap();
        assert_eq!(pending.len, second.len());

        let huge = [b'c'; MAX_PENDING_WRITE_BYTES + 1];
        pending.push(&mut sink, &huge).await.unwrap();
        assert_eq!(pending.len, 0);
    }

    #[tokio::test]
    async fn pending_write_handles_pending_writer() {
        let mut writer = PendingOnceWriter::default();
        let mut pending = PendingWrite::new();

        pending
            .push(&mut writer, b"CAPABILITIES\r\n")
            .await
            .unwrap();
        pending.flush(&mut writer).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();

        assert_eq!(pending.len, 0);
        assert_eq!(writer.written, b"CAPABILITIES\r\n".len());
    }

    #[tokio::test]
    async fn pending_write_vectors_pending_prefix_with_large_response() {
        let mut writer = VectoredWriter::default();
        let mut pending = PendingWrite::new();

        pending.push(&mut writer, DATE_RESPONSE).await.unwrap();
        pending
            .write_with_response(&mut writer, b"220 large response\r\nbody\r\n.\r\n")
            .await
            .unwrap();

        assert_eq!(pending.len, 0);
        assert_eq!(writer.vectored_writes, 1);
        assert_eq!(writer.write_calls, 1);
        assert_eq!(
            writer.bytes,
            [DATE_RESPONSE, b"220 large response\r\nbody\r\n.\r\n"].concat()
        );
    }

    #[tokio::test]
    async fn handle_command_reports_large_response_write_error() {
        let mut args = test_args();
        args.article_bytes = 8192;
        let config = ServerConfig::from_args(args);
        let mut stats = SessionStats::default();
        let mut pending = PendingWrite::new();
        let mut writer = FailingWriter;
        let command_arena = b"ARTICLE 1".to_vec();
        let command = ParsedCommand {
            kind: RequestKind::Article,
            line_range: 0..command_arena.len(),
        };

        let err = handle_command(
            &command,
            &command_arena,
            &config,
            &mut stats,
            &mut writer,
            &mut pending,
        )
        .await
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn fixed_command_reader_handles_split_line_and_closed_sender() {
        let server = ChunkedRead::new(&[b"ART".as_slice(), b"ICLE 1\r\nBODY 2\r\n".as_slice()]);
        let mut reader = BufReader::with_capacity(32, server);
        let mut command_buf = Vec::with_capacity(MAX_COMMAND_LINE_BYTES);
        let mut command_scratch = Vec::with_capacity(MAX_COMMAND_LINE_BYTES);
        let mut command_batch = CommandBatch::new();
        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                4,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            command_batch
                .iter()
                .map(|command| command.kind)
                .collect::<Vec<_>>(),
            vec![RequestKind::Article, RequestKind::Body]
        );
        assert!(
            !read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                4,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn read_command_batch_reports_reader_error() {
        let mut reader = BufReader::with_capacity(32, FailingRead);
        let mut command_buf = Vec::new();
        let mut command_scratch = Vec::new();
        let mut command_batch = CommandBatch::new();

        let err = read_command_batch(
            &mut reader,
            &mut command_buf,
            &mut command_scratch,
            &mut command_batch,
            1,
        )
        .await
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[derive(Default)]
    struct PendingOnceWriter {
        pending_once: bool,
        written: usize,
    }

    #[derive(Default)]
    struct VectoredWriter {
        write_calls: usize,
        vectored_writes: usize,
        bytes: Vec<u8>,
    }

    struct FailingWriter;

    impl tokio::io::AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "fail")))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl tokio::io::AsyncWrite for PendingOnceWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if !self.pending_once {
                self.pending_once = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            self.written += buf.len();
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl tokio::io::AsyncWrite for VectoredWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.write_calls += 1;
            self.bytes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.write_calls += 1;
            self.vectored_writes += 1;
            let mut written = 0;
            for buf in bufs {
                self.bytes.extend_from_slice(buf);
                written += buf.len();
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct ChunkedRead {
        chunks: Vec<&'static [u8]>,
        index: usize,
    }

    struct FailingRead;

    impl tokio::io::AsyncRead for FailingRead {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "fail")))
        }
    }

    impl ChunkedRead {
        fn new(chunks: &[&'static [u8]]) -> Self {
            Self {
                chunks: chunks.to_vec(),
                index: 0,
            }
        }
    }

    impl tokio::io::AsyncRead for ChunkedRead {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let Some(chunk) = self.chunks.get(self.index).copied() else {
                return Poll::Ready(Ok(()));
            };
            self.index += 1;
            buf.put_slice(chunk);
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn parses_request_kinds_case_insensitively() {
        assert_eq!(
            RequestLine::parse(b"ARTICLE <x@y>").kind(),
            RequestKind::Article
        );
        assert_eq!(RequestLine::parse(b"ARTICLE").kind(), RequestKind::Article);
        assert_eq!(RequestLine::parse(b"body 123").kind(), RequestKind::Body);
        assert_eq!(RequestLine::parse(b"BoDy").kind(), RequestKind::Body);
        assert_eq!(
            RequestLine::parse(b"CAPABILITIES").kind(),
            RequestKind::Capabilities
        );
        assert_eq!(
            RequestLine::parse(b"capabilities").kind(),
            RequestKind::Capabilities
        );
        assert_eq!(RequestLine::parse(b"DATE").kind(), RequestKind::Date);
        assert_eq!(RequestLine::parse(b"date").kind(), RequestKind::Date);
        assert_eq!(
            RequestLine::parse(b"MODE READER").kind(),
            RequestKind::ModeReader
        );
        assert_eq!(
            RequestLine::parse(b"mode reader").kind(),
            RequestKind::ModeReader
        );
        assert_eq!(RequestLine::parse(b"QUIT").kind(), RequestKind::Quit);
        assert_eq!(RequestLine::parse(b"quit").kind(), RequestKind::Quit);
        assert_eq!(RequestLine::parse(b"HEAD 1").kind(), RequestKind::Head);
        assert_eq!(
            RequestLine::parse(b"ARTICLEZ 1").kind(),
            RequestKind::Unknown
        );
        assert_eq!(
            RequestLine::parse(b"MODE TRANSIT").kind(),
            RequestKind::Unknown
        );
    }

    #[test]
    fn trims_crlf_command_lines() {
        let mut line = b"ARTICLE 1\r\n".to_vec();
        trim_command_line(&mut line);
        assert_eq!(line, b"ARTICLE 1");

        let mut bare_lf = b"BODY 1\n".to_vec();
        trim_command_line(&mut bare_lf);
        assert_eq!(bare_lf, b"BODY 1");

        let mut unchanged = b"QUIT".to_vec();
        trim_command_line(&mut unchanged);
        assert_eq!(unchanged, b"QUIT");
    }

    #[test]
    fn generated_responses_are_multiline_terminated() {
        let body = generate_body(1024);
        let article = generate_article(2048, &body);

        assert!(body.starts_with(b"222 "));
        assert!(article.starts_with(b"220 "));
        assert!(body.ends_with(b"\r\n.\r\n"));
        assert!(article.ends_with(b"\r\n.\r\n"));
        assert!(
            article
                .windows(b"\r\n\r\n".len())
                .any(|window| window == b"\r\n\r\n")
        );
    }

    #[test]
    fn generated_multiline_payload_avoids_false_dot_candidates() {
        let body = generate_body(4096);
        let body_content = body_payload(&body);
        assert!(!body_content.contains(&b'.'));

        let article = generate_article(8192, &body);
        let header_end = memchr::memmem::find(&article, b"\r\n\r\n").unwrap() + b"\r\n\r\n".len();
        let article_content = article[header_end..].strip_suffix(b".\r\n").unwrap();
        assert!(!article_content.contains(&b'.'));
    }

    #[test]
    fn generated_responses_are_near_target_size() {
        let body = generate_body(4096);
        let article = generate_article(8192, &body);

        assert!(body.len() >= 4096);
        assert!(body.len() < 4096 + BODY_LINE.len() + TERMINATOR.len());
        assert!(article.len() >= 8192);
        assert!(article.len() < 8192 + BODY_LINE.len() + TERMINATOR.len());
    }

    #[test]
    fn generated_responses_handle_tiny_targets_and_existing_crlf() {
        let body = generate_body(0);
        let article = generate_article(0, &body);
        assert!(body.ends_with(b"\r\n.\r\n"));
        assert!(article.ends_with(b"\r\n.\r\n"));

        let mut already_crlf = b"222 body follows\r\n".to_vec();
        ensure_terminated(&mut already_crlf);
        assert!(already_crlf.ends_with(b"222 body follows\r\n.\r\n"));

        let mut exact = Vec::new();
        append_repeated_payload(&mut exact, b"abc\r\n", 2);
        assert!(exact.is_empty());
    }

    #[test]
    fn body_payload_skips_first_status_line_or_returns_whole_input() {
        assert_eq!(body_payload(b"222 body follows\r\npayload"), b"payload");
        assert_eq!(
            body_payload(b"payload-without-crlf"),
            b"payload-without-crlf"
        );
        assert_eq!(
            body_payload(b"222 body follows\r\npayload\r\n.\r\n"),
            b"payload\r\n"
        );
    }

    #[test]
    fn server_config_clamps_pipeline_depth_and_keeps_responses() {
        let mut low = test_args();
        low.max_pipeline_depth = 0;
        let config = ServerConfig::from_args(low);
        assert_eq!(config.max_pipeline_depth, 1);
        assert!(config.body_response().starts_with(b"222 "));
        assert!(config.article_response().starts_with(b"220 "));

        let mut high = test_args();
        high.max_pipeline_depth = 4096;
        high.socket_recv_buffer = 4096;
        high.socket_send_buffer = 8192;
        let config = ServerConfig::from_args(high);
        assert_eq!(config.max_pipeline_depth, 1024);
        assert_eq!(config.socket_recv_buffer, 4096);
        assert_eq!(config.socket_send_buffer, 8192);
    }

    #[test]
    fn stats_snapshot_and_rates_reflect_counters() {
        let stats = Stats::default();
        stats.accepted_connections.store(2, Ordering::Relaxed);
        stats.refused_connections.store(1, Ordering::Relaxed);
        stats.active_connections.store(3, Ordering::Relaxed);
        stats.commands.store(4, Ordering::Relaxed);
        stats.pipeline_batches.store(5, Ordering::Relaxed);
        stats.article_requests.store(6, Ordering::Relaxed);
        stats.body_requests.store(7, Ordering::Relaxed);
        stats.bytes_sent.store(8, Ordering::Relaxed);
        stats.errors.store(9, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.accepted_connections, 2);
        assert_eq!(snapshot.refused_connections, 1);
        assert_eq!(snapshot.active_connections, 3);
        assert_eq!(snapshot.commands, 4);
        assert_eq!(snapshot.pipeline_batches, 5);
        assert_eq!(snapshot.article_requests, 6);
        assert_eq!(snapshot.body_requests, 7);
        assert_eq!(snapshot.bytes_sent, 8);
        assert_eq!(snapshot.errors, 9);
        assert_eq!(rate(10, 2.0), 5.0);
        snapshot.print("test", stats.started, None);
        snapshot.print("test", stats.started, Some(snapshot));
        stats.print_snapshot("test");
    }

    #[test]
    fn client_config_from_args_clamps_loads_segments_and_rotates_ports() {
        let path = write_temp_segments("config", "123\tbare@example.test\n456\t<wrapped@test>\n");
        let mut args = test_client_args();
        args.connect = "127.0.0.1:1199".parse().unwrap();
        args.ports = vec![1200, 1201];
        args.segments = Some(path.clone());
        args.requests = 17;
        args.transfer_bytes = 8192;
        args.duration_secs = 3;
        args.connections = 0;
        args.client_offset = 4;
        args.total_clients = 0;
        args.pipeline_depth = 0;
        args.command_mix = ClientCommandMix::Body;
        args.start_id = 99;
        args.read_buffer_bytes = 1;
        args.nodelay = false;
        args.socket_recv_buffer = 4096;
        args.socket_send_buffer = 8192;
        args.csv = true;
        args.stats_interval_secs = 2;

        let config = TypedClientConfig::from_client_args(args).unwrap();
        fs::remove_file(path).unwrap();

        assert_eq!(config.requests, 17);
        assert_eq!(config.transfer_bytes, 8192);
        assert_eq!(config.duration, Duration::from_secs(3));
        assert_eq!(config.connections, 1);
        assert_eq!(config.client_offset, 4);
        assert_eq!(config.total_clients, 1);
        assert_eq!(config.pipeline_depth, 1);
        assert_eq!(config.command_mix, ClientCommandMix::Body);
        assert_eq!(config.start_id, 99);
        assert_eq!(config.read_buffer_bytes, tail_buffer::TERMINATOR_TAIL_SIZE);
        assert!(!config.nodelay);
        assert_eq!(config.socket_recv_buffer, 4096);
        assert_eq!(config.socket_send_buffer, 8192);
        assert!(config.csv);
        assert_eq!(config.stats_interval, Duration::from_secs(2));
        assert_eq!(config.endpoint_for(0).port(), 1200);
        assert_eq!(config.endpoint_for(1).port(), 1201);
        assert_eq!(config.endpoint_for(2).port(), 1200);
        assert!(config.segments.is_some());
    }

    #[test]
    fn client_config_rejects_invalid_or_empty_segment_files() {
        let invalid = write_temp_segments("invalid", "missing-tab\n");
        let mut args = test_client_args();
        args.segments = Some(invalid.clone());
        let err = TypedClientConfig::from_client_args(args).unwrap_err();
        fs::remove_file(invalid).unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let empty = write_temp_segments("empty", "\n\n");
        let mut args = test_client_args();
        args.segments = Some(empty.clone());
        let err = TypedClientConfig::from_client_args(args).unwrap_err();
        fs::remove_file(empty).unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let mut args = test_client_args();
        args.segments = Some(std::env::temp_dir().join("nntpbench-missing-segments"));
        let err = TypedClientConfig::from_client_args(args).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn client_session_new_copies_connection_distribution() {
        let path = write_temp_segments("session", "1\tone@test\n22\t<two@test>\n");
        let mut args = test_client_args();
        args.connect = "127.0.0.1:1199".parse().unwrap();
        args.ports = vec![1300, 1301];
        args.segments = Some(path.clone());
        args.requests = 10;
        args.transfer_bytes = 100;
        args.connections = 2;
        args.total_clients = 8;
        args.pipeline_depth = 3;
        args.command_mix = ClientCommandMix::Article;
        args.read_buffer_bytes = 64;
        args.nodelay = false;
        args.socket_recv_buffer = 1024;
        args.socket_send_buffer = 2048;

        let config = TypedClientConfig::from_client_args(args).unwrap();
        fs::remove_file(path).unwrap();
        let session = TypedClientSession::new(&config, 3, 55, 7);

        assert_eq!(session.connect.port(), 1301);
        assert_eq!(session.client_index, 3);
        assert_eq!(session.total_clients, 8);
        assert_eq!(session.requests, 7);
        assert_eq!(session.transfer_bytes, 100);
        assert_eq!(session.next_id, 55);
        assert_eq!(session.pipeline_depth, 3);
        assert_eq!(session.command_mix, ClientCommandMix::Article);
        assert_eq!(session.read_buffer_bytes, 64);
        assert!(!session.nodelay);
        assert_eq!(session.socket_recv_buffer, 1024);
        assert_eq!(session.socket_send_buffer, 2048);
        assert!(session.segments.is_some());
    }

    #[test]
    fn typed_client_config_from_args_clamps_and_copies_fields() {
        let mut args = test_typed_client_args();
        args.requests = 17;
        args.transfer_bytes = 8192;
        args.duration_secs = 3;
        args.connections = 0;
        args.client_offset = 4;
        args.total_clients = 0;
        args.pipeline_depth = 0;
        args.command_mix = ClientCommandMix::Body;
        args.start_id = 99;
        args.read_buffer_bytes = 1;
        args.nodelay = false;
        args.socket_recv_buffer = 4096;
        args.socket_send_buffer = 8192;
        args.csv = true;
        args.stats_interval_secs = 2;

        let config = TypedClientConfig::from_args(args).unwrap();

        assert_eq!(config.requests, 17);
        assert_eq!(config.transfer_bytes, 8192);
        assert_eq!(config.duration, Duration::from_secs(3));
        assert_eq!(config.connections, 1);
        assert_eq!(config.client_offset, 4);
        assert_eq!(config.total_clients, 1);
        assert_eq!(config.pipeline_depth, 1);
        assert_eq!(config.command_mix, ClientCommandMix::Body);
        assert_eq!(config.start_id, 99);
        assert_eq!(config.read_buffer_bytes, tail_buffer::TERMINATOR_TAIL_SIZE);
        assert!(!config.nodelay);
        assert_eq!(config.socket_recv_buffer, 4096);
        assert_eq!(config.socket_send_buffer, 8192);
        assert!(config.csv);
        assert_eq!(config.stats_interval, Duration::from_secs(2));
    }

    #[test]
    fn client_limit_and_distribution_helpers_cover_edge_cases() {
        let stats = Stats::new();
        assert!(!transfer_limit_reached(&stats, 0));
        assert!(!transfer_limit_reached(&stats, 10));
        stats.bytes_sent.store(10, Ordering::Relaxed);
        assert!(transfer_limit_reached(&stats, 10));

        assert_eq!(requests_for_connection(0, 3, 0), 0);
        assert_eq!(requests_for_connection(10, 3, 0), 4);
        assert_eq!(requests_for_connection(10, 3, 1), 3);
        assert_eq!(requests_for_connection(10, 3, 2), 3);
    }

    #[test]
    fn segment_parsing_normalizes_msgids_and_selects_by_stride() {
        let path = write_temp_segments("normalize", "1\tbare@test\n2\t<wrapped@test>\n");
        let segments = read_segments(&path).unwrap();
        fs::remove_file(path).unwrap();

        assert_eq!(normalize_msgid("bare@test"), b"<bare@test>");
        assert_eq!(normalize_msgid("<wrapped@test>"), b"<wrapped@test>");
        assert_eq!(segment_for_request(&segments, 0, 2, 0), b"<bare@test>");
        assert_eq!(segment_for_request(&segments, 1, 2, 0), b"<wrapped@test>");
    }

    #[test]
    fn process_metrics_helpers_return_sensible_values() {
        assert_eq!(cpu_seconds_since(None), 0.0);
        let start = process_cpu_ticks();
        assert!(cpu_seconds_since(start) >= 0.0);
        assert!(process_rss_kib() > 0);
    }

    #[tokio::test]
    async fn binds_ipv4_listener_and_rejects_duplicate_bind() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        assert!(addr.is_ipv4());
        assert!(bind_listener(addr, 16, false).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn binds_reuse_port_listener_on_unix() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, true).unwrap();
        assert!(listener.local_addr().unwrap().is_ipv4());
    }

    #[tokio::test]
    async fn bind_listener_selects_ipv6_domain() {
        let _ = bind_listener("[::1]:0".parse().unwrap(), 16, false);
    }

    #[test]
    fn socket_buffer_size_rejects_values_too_large_for_tcp_socket() {
        assert_eq!(socket_buffer_size_u32(4096).unwrap(), 4096);
        assert_eq!(
            socket_buffer_size_u32(u32::MAX as usize + 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[tokio::test]
    async fn optimize_server_socket_accepts_high_throughput_buffers() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr);
        let accepted = listener.accept();
        let (client, accepted) = tokio::join!(client, accepted);
        let _client = client.unwrap();
        let (server, _) = accepted.unwrap();

        let mut args = test_args();
        args.socket_recv_buffer = 4096;
        args.socket_send_buffer = 4096;
        let config = ServerConfig::from_args(args);
        optimize_server_socket(&server, &config).unwrap();

        let mut args = test_args();
        args.nodelay = false;
        args.socket_recv_buffer = 0;
        args.socket_send_buffer = 0;
        let config = ServerConfig::from_args(args);
        optimize_server_socket(&server, &config).unwrap();
    }

    #[tokio::test]
    async fn read_greeting_reports_eof_and_overlong_greeting() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut buffer = [0_u8; 16];
        let err = read_greeting(&mut client, &mut buffer).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        server.await.unwrap();

        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 no newline yet").await.unwrap();
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut buffer = [0_u8; 8];
        let err = read_greeting(&mut client, &mut buffer).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_session_runs_pipelined_requests_against_loopback_server() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let mut pending = Vec::new();
            let mut commands = 0;
            while commands < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);

                while let Some(line_end) = memchr::memchr(b'\n', &pending) {
                    let line_len = line_end + 1;
                    let response: &[u8] = if pending[..line_len].starts_with(b"ARTICLE ") {
                        b"220 1 article follows\r\nbody\r\n.\r\n"
                    } else {
                        b"222 1 body follows\r\nbody\r\n.\r\n"
                    };
                    stream.write_all(response).await.unwrap();
                    pending.drain(..line_len);
                    commands += 1;
                }
            }
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 2;
        args.pipeline_depth = 2;
        args.command_mix = ClientCommandMix::Alternate;
        args.read_buffer_bytes = 64;
        let config = TypedClientConfig::from_client_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 2);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        session.run(stats.clone(), stop.clone()).await.unwrap();
        server.await.unwrap();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.accepted_connections, 1);
        assert_eq!(snapshot.active_connections, 0);
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.article_requests, 1);
        assert_eq!(snapshot.body_requests, 1);
        assert_eq!(snapshot.pipeline_batches, 1);
        assert!(snapshot.bytes_sent > 0);
        assert!(!stop.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn typed_client_session_runs_pipelined_requests_against_loopback_server() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let mut pending = Vec::new();
            while pending.iter().filter(|byte| **byte == b'\n').count() < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }

            assert_eq!(
                &pending[..],
                b"ARTICLE <bench.1@nntpbench.local>\r\nBODY <bench.2@nntpbench.local>\r\n"
            );

            stream
                .write_all(
                    b"220 1 <bench.1@nntpbench.local> article follows\r\nSubject: Bench\r\n\r\none\r\n.\r\n",
                )
                .await
                .unwrap();
            stream
                .write_all(b"222 1 <bench.2@nntpbench.local> body follows\r\ntwo\r\n.\r\n")
                .await
                .unwrap();
        });

        let mut args = test_typed_client_args();
        args.connect = addr;
        args.requests = 2;
        args.pipeline_depth = 2;
        args.command_mix = ClientCommandMix::Alternate;
        args.read_buffer_bytes = 64;
        let config = TypedClientConfig::from_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 2);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        session.run(stats.clone(), stop.clone()).await.unwrap();
        server.await.unwrap();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.accepted_connections, 1);
        assert_eq!(snapshot.active_connections, 0);
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.article_requests, 1);
        assert_eq!(snapshot.body_requests, 1);
        assert_eq!(snapshot.pipeline_batches, 1);
        assert!(snapshot.bytes_sent > 0);
        assert!(!stop.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn client_session_refills_pipeline_before_batch_is_drained() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let mut pending = Vec::new();
            while pending.iter().filter(|byte| **byte == b'\n').count() < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }

            stream
                .write_all(b"220 1 article follows\r\nbody\r\n.\r\n")
                .await
                .unwrap();

            time::timeout(Duration::from_secs(1), async {
                while pending.iter().filter(|byte| **byte == b'\n').count() < 3 {
                    let read = stream.read(&mut scratch).await.unwrap();
                    assert_ne!(read, 0);
                    pending.extend_from_slice(&scratch[..read]);
                }
            })
            .await
            .expect("client should refill pipeline before draining the original batch");

            stream
                .write_all(b"222 1 body follows\r\nbody\r\n.\r\n")
                .await
                .unwrap();
            stream
                .write_all(b"220 1 article follows\r\nbody\r\n.\r\n")
                .await
                .unwrap();
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 3;
        args.pipeline_depth = 2;
        args.command_mix = ClientCommandMix::Alternate;
        args.read_buffer_bytes = 64;
        let config = TypedClientConfig::from_client_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 3);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        session.run(stats.clone(), stop.clone()).await.unwrap();
        server.await.unwrap();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 3);
        assert_eq!(snapshot.article_requests, 2);
        assert_eq!(snapshot.body_requests, 1);
    }

    #[tokio::test]
    async fn client_session_reports_connect_error() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 1;
        args.pipeline_depth = 1;
        let config = TypedClientConfig::from_client_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 1);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        let err = session.run(stats.clone(), stop).await.unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
        assert_eq!(stats.snapshot().active_connections, 0);
    }

    #[tokio::test]
    async fn client_session_sets_stop_after_transfer_limit() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let read = stream.read(&mut scratch).await.unwrap();
            assert_ne!(read, 0);
            stream
                .write_all(b"222 1 body follows\r\nresponse payload\r\n.\r\n")
                .await
                .unwrap();
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 0;
        args.transfer_bytes = 1;
        args.pipeline_depth = 1;
        args.command_mix = ClientCommandMix::Body;
        args.read_buffer_bytes = 64;
        let config = TypedClientConfig::from_client_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 0);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        session.run(stats.clone(), stop.clone()).await.unwrap();
        server.await.unwrap();

        assert!(stop.load(Ordering::Acquire));
        assert_eq!(stats.snapshot().commands, 1);
    }

    #[tokio::test]
    async fn client_session_reports_eof_during_response() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();
            let mut scratch = [0_u8; 128];
            let read = stream.read(&mut scratch).await.unwrap();
            assert_ne!(read, 0);
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 1;
        args.pipeline_depth = 1;
        args.read_buffer_bytes = 64;
        let config = TypedClientConfig::from_client_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 1);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        let err = session.run(stats.clone(), stop).await.unwrap_err();
        server.await.unwrap();

        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(stats.snapshot().active_connections, 0);
    }

    #[tokio::test]
    async fn client_session_reports_socket_error_during_response() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();
            let mut scratch = [0_u8; 128];
            let read = stream.read(&mut scratch).await.unwrap();
            assert_ne!(read, 0);
            socket2::SockRef::from(&stream)
                .set_linger(Some(Duration::ZERO))
                .unwrap();
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 1;
        args.pipeline_depth = 1;
        args.read_buffer_bytes = 64;
        let config = TypedClientConfig::from_client_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 1);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        let err = session.run(stats.clone(), stop).await.unwrap_err();
        server.await.unwrap();

        assert_ne!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(stats.snapshot().active_connections, 0);
    }

    #[tokio::test]
    async fn client_session_reports_greeting_eof() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 1;
        args.pipeline_depth = 1;
        args.read_buffer_bytes = 64;
        let config = TypedClientConfig::from_client_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 1);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        let err = session.run(stats.clone(), stop).await.unwrap_err();
        server.await.unwrap();

        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(stats.snapshot().active_connections, 0);
    }

    #[tokio::test]
    async fn client_session_reports_write_error_after_greeting_reset() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();
            socket2::SockRef::from(&stream)
                .set_linger(Some(Duration::ZERO))
                .unwrap();
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 1;
        args.pipeline_depth = 1;
        args.read_buffer_bytes = 64;
        let config = TypedClientConfig::from_client_args(args).unwrap();
        let session = TypedClientSession::new(&config, 0, 1, 1);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        let err = session.run(stats.clone(), stop).await.unwrap_err();
        server.await.unwrap();

        assert_ne!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(stats.snapshot().active_connections, 0);
    }

    #[tokio::test]
    async fn fetch_typed_response_uses_typed_connection_path() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"BODY <typed-fetch@test>\r\n");

            stream
                .write_all(b"222 1 <typed-fetch@test> body follows\r\nbody payload\r\n.\r\n")
                .await
                .unwrap();
        });

        let mut args = test_typed_fetch_args();
        args.connect = addr;
        args.request = TypedRequestKind::Body;
        args.message_id = Some("typed-fetch@test".to_string());
        args.read_buffer_bytes = 64;

        let response = fetch_typed_response(&args).await.unwrap();
        let article = response.parse_article().unwrap();

        assert_eq!(response.kind(), RequestKind::Body);
        assert_eq!(response.status().as_u16(), 222);
        assert_eq!(article.message_id.as_str(), "<typed-fetch@test>");
        assert_eq!(article.body, Some(&b"body payload\r\n"[..]));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_typed_response_supports_head_and_stat_requests() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut head_stream, _) = listener.accept().await.unwrap();
            head_stream.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut first = [0_u8; 128];
            let read = head_stream.read(&mut first).await.unwrap();
            assert_eq!(&first[..read], b"HEAD <typed-head@test>\r\n");
            head_stream
                .write_all(b"221 1 <typed-head@test> article retrieved\r\nSubject: Head\r\n.\r\n")
                .await
                .unwrap();

            let (mut stat_stream, _) = listener.accept().await.unwrap();
            stat_stream.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut second = [0_u8; 128];
            let read = stat_stream.read(&mut second).await.unwrap();
            assert_eq!(&second[..read], b"STAT <typed-stat@test>\r\n");
            stat_stream
                .write_all(b"223 1 <typed-stat@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let mut head_args = test_typed_fetch_args();
        head_args.connect = addr;
        head_args.request = TypedRequestKind::Head;
        head_args.message_id = Some("typed-head@test".to_string());
        let head = fetch_typed_response(&head_args).await.unwrap();
        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.status().as_u16(), 221);

        let mut stat_args = test_typed_fetch_args();
        stat_args.connect = addr;
        stat_args.request = TypedRequestKind::Stat;
        stat_args.message_id = Some("typed-stat@test".to_string());
        let stat = fetch_typed_response(&stat_args).await.unwrap();
        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.status().as_u16(), 223);
        assert_eq!(
            stat.parse_article().unwrap().message_id.as_str(),
            "<typed-stat@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_typed_response_rejects_invalid_message_id() {
        let mut args = test_typed_fetch_args();
        args.message_id = Some("<bad id>".to_string());

        let err = fetch_typed_response(&args).await.unwrap_err();
        assert!(matches!(err, TypedClientError::InvalidMessageId));
    }

    #[tokio::test]
    async fn fetch_typed_response_clamps_pipeline_depth_for_typed_engine() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"ARTICLE <clamp@test>\r\n");

            stream
                .write_all(
                    b"220 1 <clamp@test> article follows\r\nSubject: Clamp\r\n\r\nbody\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let mut args = test_typed_fetch_args();
        args.connect = addr;
        args.message_id = Some("clamp@test".to_string());
        args.pipeline_depth = 0;

        let response = fetch_typed_response(&args).await.unwrap();
        assert_eq!(response.status().as_u16(), 220);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_typed_response_supports_general_request_kinds_without_message_id() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            first.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = first.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST\r\n");
            first.write_all(LIST_RESPONSE).await.unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            second.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = second.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"HELP\r\n");
            second.write_all(HELP_RESPONSE).await.unwrap();

            let (mut third, _) = listener.accept().await.unwrap();
            third.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = third.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"CAPABILITIES\r\n");
            third
                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                .await
                .unwrap();

            let (mut fourth, _) = listener.accept().await.unwrap();
            fourth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fourth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"DATE\r\n");
            fourth.write_all(b"111 20260515120000\r\n").await.unwrap();

            let (mut fifth, _) = listener.accept().await.unwrap();
            fifth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fifth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"MODE READER\r\n");
            fifth
                .write_all(b"201 posting not permitted\r\n")
                .await
                .unwrap();

            let (mut sixth, _) = listener.accept().await.unwrap();
            sixth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = sixth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"QUIT\r\n");
            sixth.write_all(QUIT_RESPONSE).await.unwrap();
        });

        let mut list_args = test_typed_fetch_args();
        list_args.connect = addr;
        list_args.request = TypedRequestKind::List;
        list_args.message_id = None;
        let list = fetch_typed_response(&list_args).await.unwrap();
        assert_eq!(list.kind(), RequestKind::List);
        assert_eq!(list.status().as_u16(), 215);

        let mut help_args = test_typed_fetch_args();
        help_args.connect = addr;
        help_args.request = TypedRequestKind::Help;
        help_args.message_id = None;
        let help = fetch_typed_response(&help_args).await.unwrap();
        assert_eq!(help.kind(), RequestKind::Help);
        assert_eq!(help.status().as_u16(), 100);

        let mut capabilities_args = test_typed_fetch_args();
        capabilities_args.connect = addr;
        capabilities_args.request = TypedRequestKind::Capabilities;
        capabilities_args.message_id = None;
        let capabilities = fetch_typed_response(&capabilities_args).await.unwrap();
        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert_eq!(capabilities.status().as_u16(), 101);

        let mut date_args = test_typed_fetch_args();
        date_args.connect = addr;
        date_args.request = TypedRequestKind::Date;
        date_args.message_id = None;
        let date = fetch_typed_response(&date_args).await.unwrap();
        assert_eq!(date.kind(), RequestKind::Date);
        assert_eq!(date.status().as_u16(), 111);

        let mut mode_reader_args = test_typed_fetch_args();
        mode_reader_args.connect = addr;
        mode_reader_args.request = TypedRequestKind::ModeReader;
        mode_reader_args.message_id = None;
        let mode_reader = fetch_typed_response(&mode_reader_args).await.unwrap();
        assert_eq!(mode_reader.kind(), RequestKind::ModeReader);
        assert_eq!(mode_reader.status().as_u16(), 201);

        let mut quit_args = test_typed_fetch_args();
        quit_args.connect = addr;
        quit_args.request = TypedRequestKind::Quit;
        quit_args.message_id = None;
        let quit = fetch_typed_response(&quit_args).await.unwrap();
        assert_eq!(quit.kind(), RequestKind::Quit);
        assert_eq!(quit.status().as_u16(), 205);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_typed_response_supports_header_query_request_kinds() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            first.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = first.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"HDR Subject 1-10\r\n");
            first.write_all(HDR_RESPONSE).await.unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            second.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = second.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"XHDR Message-ID <headers@test>\r\n");
            second.write_all(XHDR_RESPONSE).await.unwrap();
        });

        let mut hdr_args = test_typed_fetch_args();
        hdr_args.connect = addr;
        hdr_args.request = TypedRequestKind::Hdr;
        hdr_args.message_id = None;
        hdr_args.header = Some("Subject".to_string());
        hdr_args.selector = Some("1-10".to_string());
        let hdr = fetch_typed_response(&hdr_args).await.unwrap();
        assert_eq!(hdr.kind(), RequestKind::Hdr);
        assert_eq!(hdr.status().as_u16(), 225);

        let mut xhdr_args = test_typed_fetch_args();
        xhdr_args.connect = addr;
        xhdr_args.request = TypedRequestKind::Xhdr;
        xhdr_args.message_id = None;
        xhdr_args.header = Some("Message-ID".to_string());
        xhdr_args.selector = Some("<headers@test>".to_string());
        let xhdr = fetch_typed_response(&xhdr_args).await.unwrap();
        assert_eq!(xhdr.kind(), RequestKind::Xhdr);
        assert_eq!(xhdr.status().as_u16(), 225);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_typed_response_supports_overview_request_kinds() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            first.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = first.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"OVER 1-10\r\n");
            first.write_all(OVER_RESPONSE).await.unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            second.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = second.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"XOVER <overview@test>\r\n");
            second.write_all(XOVER_RESPONSE).await.unwrap();
        });

        let mut over_args = test_typed_fetch_args();
        over_args.connect = addr;
        over_args.request = TypedRequestKind::Over;
        over_args.message_id = None;
        over_args.selector = Some("1-10".to_string());
        let over = fetch_typed_response(&over_args).await.unwrap();
        assert_eq!(over.kind(), RequestKind::Over);
        assert_eq!(over.status().as_u16(), 224);

        let mut xover_args = test_typed_fetch_args();
        xover_args.connect = addr;
        xover_args.request = TypedRequestKind::Xover;
        xover_args.message_id = None;
        xover_args.selector = Some("<overview@test>".to_string());
        let xover = fetch_typed_response(&xover_args).await.unwrap();
        assert_eq!(xover.kind(), RequestKind::Xover);
        assert_eq!(xover.status().as_u16(), 224);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_typed_response_supports_group_navigation_and_discovery_request_kinds() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            first.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = first.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"GROUP alt.test\r\n");
            first.write_all(GROUP_RESPONSE).await.unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            second.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = second.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LISTGROUP alt.test\r\n");
            second.write_all(LISTGROUP_RESPONSE).await.unwrap();

            let (mut third, _) = listener.accept().await.unwrap();
            third.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = third.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LAST\r\n");
            third.write_all(LAST_RESPONSE).await.unwrap();

            let (mut fourth, _) = listener.accept().await.unwrap();
            fourth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fourth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"NEXT\r\n");
            fourth.write_all(NEXT_RESPONSE).await.unwrap();

            let (mut fifth, _) = listener.accept().await.unwrap();
            fifth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fifth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"NEWGROUPS 20260101 000000 GMT\r\n");
            fifth.write_all(NEWGROUPS_RESPONSE).await.unwrap();

            let (mut sixth, _) = listener.accept().await.unwrap();
            sixth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = sixth.read(&mut request).await.unwrap();
            assert_eq!(
                &request[..read],
                b"NEWNEWS comp.lang.*,alt.test 20260101 000000\r\n"
            );
            sixth.write_all(NEWNEWS_RESPONSE).await.unwrap();
        });

        let mut group_args = test_typed_fetch_args();
        group_args.connect = addr;
        group_args.request = TypedRequestKind::Group;
        group_args.message_id = None;
        group_args.group = Some("alt.test".to_string());
        let group = fetch_typed_response(&group_args).await.unwrap();
        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(group.status().as_u16(), 211);

        let mut listgroup_args = test_typed_fetch_args();
        listgroup_args.connect = addr;
        listgroup_args.request = TypedRequestKind::Listgroup;
        listgroup_args.message_id = None;
        listgroup_args.group = Some("alt.test".to_string());
        let listgroup = fetch_typed_response(&listgroup_args).await.unwrap();
        assert_eq!(listgroup.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup.status().as_u16(), 211);

        let mut last_args = test_typed_fetch_args();
        last_args.connect = addr;
        last_args.request = TypedRequestKind::Last;
        last_args.message_id = None;
        let last = fetch_typed_response(&last_args).await.unwrap();
        assert_eq!(last.kind(), RequestKind::Last);
        assert_eq!(last.status().as_u16(), 223);

        let mut next_args = test_typed_fetch_args();
        next_args.connect = addr;
        next_args.request = TypedRequestKind::Next;
        next_args.message_id = None;
        let next = fetch_typed_response(&next_args).await.unwrap();
        assert_eq!(next.kind(), RequestKind::Next);
        assert_eq!(next.status().as_u16(), 223);

        let mut newgroups_args = test_typed_fetch_args();
        newgroups_args.connect = addr;
        newgroups_args.request = TypedRequestKind::Newgroups;
        newgroups_args.message_id = None;
        newgroups_args.date = Some("20260101".to_string());
        newgroups_args.time = Some("000000".to_string());
        let newgroups = fetch_typed_response(&newgroups_args).await.unwrap();
        assert_eq!(newgroups.kind(), RequestKind::NewGroups);
        assert_eq!(newgroups.status().as_u16(), 231);

        let mut newnews_args = test_typed_fetch_args();
        newnews_args.connect = addr;
        newnews_args.request = TypedRequestKind::Newnews;
        newnews_args.message_id = None;
        newnews_args.wildmat = Some("comp.lang.*,alt.test".to_string());
        newnews_args.date = Some("20260101".to_string());
        newnews_args.time = Some("000000".to_string());
        newnews_args.gmt = false;
        let newnews = fetch_typed_response(&newnews_args).await.unwrap();
        assert_eq!(newnews.kind(), RequestKind::NewNews);
        assert_eq!(newnews.status().as_u16(), 230);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_typed_response_supports_remaining_rfc_request_kinds() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            first.write_all(b"201 fetch ready\r\n").await.unwrap();
            let mut request = [0_u8; 256];
            let read = first.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"POST\r\n");
            first.write_all(POST_RESPONSE).await.unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            second.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = second.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"IHAVE <ihave@test>\r\n");
            second.write_all(IHAVE_RESPONSE).await.unwrap();

            let (mut third, _) = listener.accept().await.unwrap();
            third.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = third.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"CHECK <check@test>\r\n");
            third.write_all(CHECK_RESPONSE).await.unwrap();

            let (mut fourth, _) = listener.accept().await.unwrap();
            fourth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fourth.read(&mut request).await.unwrap();
            assert_eq!(
                &request[..read],
                b"TAKETHIS <take@test>\r\nSubject: Taken\r\n\r\n..dot line\r\npayload\r\n.\r\n"
            );
            fourth.write_all(TAKETHIS_RESPONSE).await.unwrap();

            let (mut fifth, _) = listener.accept().await.unwrap();
            fifth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fifth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"AUTHINFO USER bench user\r\n");
            fifth.write_all(AUTHINFO_RESPONSE).await.unwrap();

            let (mut sixth, _) = listener.accept().await.unwrap();
            sixth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = sixth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"AUTHINFO PASS bench pass\r\n");
            sixth.write_all(AUTHINFO_RESPONSE).await.unwrap();

            let (mut seventh, _) = listener.accept().await.unwrap();
            seventh.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = seventh.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"STARTTLS\r\n");
            seventh.write_all(STARTTLS_RESPONSE).await.unwrap();
        });

        let mut post_args = test_typed_fetch_args();
        post_args.connect = addr;
        post_args.request = TypedRequestKind::Post;
        post_args.message_id = None;
        let post = fetch_typed_response(&post_args).await.unwrap();
        assert_eq!(post.kind(), RequestKind::Post);
        assert_eq!(post.status().as_u16(), 340);

        let mut ihave_args = test_typed_fetch_args();
        ihave_args.connect = addr;
        ihave_args.request = TypedRequestKind::Ihave;
        ihave_args.message_id = Some("ihave@test".to_string());
        let ihave = fetch_typed_response(&ihave_args).await.unwrap();
        assert_eq!(ihave.kind(), RequestKind::Ihave);
        assert_eq!(ihave.status().as_u16(), 335);

        let mut check_args = test_typed_fetch_args();
        check_args.connect = addr;
        check_args.request = TypedRequestKind::Check;
        check_args.message_id = Some("check@test".to_string());
        let check = fetch_typed_response(&check_args).await.unwrap();
        assert_eq!(check.kind(), RequestKind::Check);
        assert_eq!(check.status().as_u16(), 238);

        let mut takethis_args = test_typed_fetch_args();
        takethis_args.connect = addr;
        takethis_args.request = TypedRequestKind::Takethis;
        takethis_args.message_id = Some("take@test".to_string());
        takethis_args.article_body = Some("Subject: Taken\r\n\r\n.dot line\r\npayload".to_string());
        let takethis = fetch_typed_response(&takethis_args).await.unwrap();
        assert_eq!(takethis.kind(), RequestKind::TakeThis);
        assert_eq!(takethis.status().as_u16(), 239);

        let mut auth_user_args = test_typed_fetch_args();
        auth_user_args.connect = addr;
        auth_user_args.request = TypedRequestKind::AuthinfoUser;
        auth_user_args.message_id = None;
        auth_user_args.auth_value = Some("bench user".to_string());
        let auth_user = fetch_typed_response(&auth_user_args).await.unwrap();
        assert_eq!(auth_user.kind(), RequestKind::AuthInfoUser);
        assert_eq!(auth_user.status().as_u16(), 281);

        let mut auth_pass_args = test_typed_fetch_args();
        auth_pass_args.connect = addr;
        auth_pass_args.request = TypedRequestKind::AuthinfoPass;
        auth_pass_args.message_id = None;
        auth_pass_args.auth_value = Some("bench pass".to_string());
        let auth_pass = fetch_typed_response(&auth_pass_args).await.unwrap();
        assert_eq!(auth_pass.kind(), RequestKind::AuthInfoPass);
        assert_eq!(auth_pass.status().as_u16(), 281);

        let mut starttls_args = test_typed_fetch_args();
        starttls_args.connect = addr;
        starttls_args.request = TypedRequestKind::Starttls;
        starttls_args.message_id = None;
        let starttls = fetch_typed_response(&starttls_args).await.unwrap();
        assert_eq!(starttls.kind(), RequestKind::StartTls);
        assert_eq!(starttls.status().as_u16(), 382);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_typed_response_rejects_missing_message_id_for_article_style_requests() {
        let mut args = test_typed_fetch_args();
        args.message_id = None;

        let err = fetch_typed_response(&args).await.unwrap_err();
        assert!(matches!(err, TypedClientError::MissingMessageId));
    }

    #[tokio::test]
    async fn fetch_typed_response_rejects_missing_header_query_arguments() {
        let mut args = test_typed_fetch_args();
        args.request = TypedRequestKind::Hdr;
        args.message_id = None;
        args.header = None;
        args.selector = Some("1-10".to_string());
        assert!(matches!(
            fetch_typed_response(&args).await.unwrap_err(),
            TypedClientError::MissingHeaderName
        ));

        args.header = Some("Subject".to_string());
        args.selector = None;
        assert!(matches!(
            fetch_typed_response(&args).await.unwrap_err(),
            TypedClientError::MissingArticleSelector
        ));
    }

    #[tokio::test]
    async fn fetch_typed_response_rejects_missing_overview_selector() {
        let mut args = test_typed_fetch_args();
        args.request = TypedRequestKind::Over;
        args.message_id = None;
        args.selector = None;

        assert!(matches!(
            fetch_typed_response(&args).await.unwrap_err(),
            TypedClientError::MissingArticleSelector
        ));
    }

    #[tokio::test]
    async fn fetch_typed_response_rejects_missing_group_argument() {
        let mut args = test_typed_fetch_args();
        args.request = TypedRequestKind::Group;
        args.message_id = None;
        args.group = None;

        assert!(matches!(
            fetch_typed_response(&args).await.unwrap_err(),
            TypedClientError::MissingGroupName
        ));
    }

    #[tokio::test]
    async fn fetch_typed_response_rejects_missing_discovery_arguments() {
        let mut newgroups_args = test_typed_fetch_args();
        newgroups_args.request = TypedRequestKind::Newgroups;
        newgroups_args.message_id = None;
        newgroups_args.date = None;
        newgroups_args.time = Some("000000".to_string());
        assert!(matches!(
            fetch_typed_response(&newgroups_args).await.unwrap_err(),
            TypedClientError::MissingDate
        ));

        newgroups_args.date = Some("20260101".to_string());
        newgroups_args.time = None;
        assert!(matches!(
            fetch_typed_response(&newgroups_args).await.unwrap_err(),
            TypedClientError::MissingTime
        ));

        let mut newnews_args = test_typed_fetch_args();
        newnews_args.request = TypedRequestKind::Newnews;
        newnews_args.message_id = None;
        newnews_args.wildmat = None;
        newnews_args.date = Some("20260101".to_string());
        newnews_args.time = Some("000000".to_string());
        assert!(matches!(
            fetch_typed_response(&newnews_args).await.unwrap_err(),
            TypedClientError::MissingWildmat
        ));
    }

    #[tokio::test]
    async fn fetch_typed_response_rejects_missing_remaining_rfc_arguments() {
        let mut takethis_args = test_typed_fetch_args();
        takethis_args.request = TypedRequestKind::Takethis;
        takethis_args.article_body = None;
        assert!(matches!(
            fetch_typed_response(&takethis_args).await.unwrap_err(),
            TypedClientError::MissingArticleBody
        ));

        let mut auth_args = test_typed_fetch_args();
        auth_args.request = TypedRequestKind::AuthinfoUser;
        auth_args.message_id = None;
        auth_args.auth_value = None;
        assert!(matches!(
            fetch_typed_response(&auth_args).await.unwrap_err(),
            TypedClientError::MissingAuthInfoValue
        ));
    }

    #[tokio::test]
    async fn reads_pipelined_commands_in_fifo_order() {
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"ARTICLE <a@test>\r\nBODY <b@test>\r\nDATE\r\nMODE READER\r\nQUIT\r\n")
            .await
            .unwrap();

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = Vec::with_capacity(1024);
        let mut command_scratch = Vec::with_capacity(1024);
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                8,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            command_batch
                .iter()
                .map(|command| command.kind)
                .collect::<Vec<_>>(),
            vec![
                RequestKind::Article,
                RequestKind::Body,
                RequestKind::Date,
                RequestKind::ModeReader,
                RequestKind::Quit
            ]
        );
    }

    #[tokio::test]
    async fn read_command_batch_returns_false_on_clean_eof() {
        let (client, server) = tokio::io::duplex(1024);
        drop(client);
        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = Vec::with_capacity(1024);
        let mut command_scratch = Vec::with_capacity(1024);
        let mut command_batch = CommandBatch::new();

        assert!(
            !read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                8,
            )
            .await
            .unwrap()
        );
        assert!(command_batch.is_empty());
    }

    #[tokio::test]
    async fn read_command_batch_stops_at_partial_buffered_command() {
        let (mut client, server) = tokio::io::duplex(1024);
        client.write_all(b"ARTICLE 1\r\nBODY 2").await.unwrap();

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = Vec::with_capacity(1024);
        let mut command_scratch = Vec::with_capacity(1024);
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                8,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            command_batch
                .iter()
                .map(|command| command.kind)
                .collect::<Vec<_>>(),
            vec![RequestKind::Article]
        );
    }

    #[tokio::test]
    async fn read_command_batch_respects_max_pipeline_depth() {
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"ARTICLE 1\r\nBODY 2\r\nQUIT\r\n")
            .await
            .unwrap();

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = Vec::with_capacity(1024);
        let mut command_scratch = Vec::with_capacity(1024);
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                2,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            command_batch
                .iter()
                .map(|command| command.kind)
                .collect::<Vec<_>>(),
            vec![RequestKind::Article, RequestKind::Body]
        );
    }

    #[tokio::test]
    async fn read_command_batch_clears_reused_batch_between_reads() {
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"ARTICLE 1\r\nBODY 2\r\nQUIT\r\n")
            .await
            .unwrap();

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = Vec::with_capacity(1024);
        let mut command_scratch = Vec::with_capacity(1024);
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                2,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            command_batch
                .iter()
                .map(|command| command.kind)
                .collect::<Vec<_>>(),
            vec![RequestKind::Article, RequestKind::Body]
        );

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                2,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            command_batch
                .iter()
                .map(|command| command.kind)
                .collect::<Vec<_>>(),
            vec![RequestKind::Quit]
        );
    }

    #[tokio::test]
    async fn read_command_batch_consumes_takethis_payload_as_one_command() {
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"TAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\r\n.\r\nQUIT\r\n")
            .await
            .unwrap();

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = Vec::with_capacity(1024);
        let mut command_scratch = Vec::with_capacity(1024);
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                &mut command_scratch,
                &mut command_batch,
                8,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            command_batch
                .iter()
                .map(|command| command.kind)
                .collect::<Vec<_>>(),
            vec![RequestKind::TakeThis, RequestKind::Quit]
        );
    }

    #[tokio::test]
    async fn serve_session_returns_pipelined_responses_in_order_and_counts_stats() {
        let (output, stats) = run_session_with_input(
            test_config(),
            b"BODY 2\r\nARTICLE 1\r\nCAPABILITIES\r\nDATE\r\nMODE READER\r\nHEAD 1\r\nQUIT\r\n",
        )
        .await;

        let text = String::from_utf8_lossy(&output);
        let body = text.find("222 1").unwrap();
        let article = text.find("220 1").unwrap();
        let caps = text.find("101 Capability").unwrap();
        let date = text.find("111 20260515120000").unwrap();
        let mode = text.find("201 posting not permitted").unwrap();
        let unknown = text.find("500 unknown").unwrap();
        let quit = text.find("205 closing").unwrap();
        assert!(body < article);
        assert!(article < caps);
        assert!(caps < date);
        assert!(date < mode);
        assert!(mode < unknown);
        assert!(unknown < quit);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 7);
        assert_eq!(snapshot.body_requests, 1);
        assert_eq!(snapshot.article_requests, 1);
        assert_eq!(snapshot.pipeline_batches, 1);
        assert_eq!(snapshot.bytes_sent, output.len() as u64);
    }

    #[tokio::test]
    async fn serve_session_supports_backend_negotiation_commands() {
        let (output, stats) =
            run_session_with_input(test_config(), b"MODE READER\r\nDATE\r\nQUIT\r\n").await;

        assert_eq!(
            output,
            [
                GREETING,
                MODE_READER_RESPONSE,
                DATE_RESPONSE,
                b"205 closing connection\r\n".as_slice()
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 3);
    }

    #[tokio::test]
    async fn serve_session_supports_help_and_quit_commands() {
        let (output, stats) =
            run_session_with_input(test_config(), b"LIST\r\nHELP\r\nQUIT\r\n").await;

        assert_eq!(
            output,
            [GREETING, LIST_RESPONSE, HELP_RESPONSE, QUIT_RESPONSE].concat()
        );
        assert_eq!(stats.snapshot().commands, 3);
    }

    #[tokio::test]
    async fn serve_session_supports_overview_commands() {
        let (output, stats) =
            run_session_with_input(test_config(), b"OVER 1-10\r\nXOVER <overview@test>\r\n").await;

        assert_eq!(output, [GREETING, OVER_RESPONSE, XOVER_RESPONSE].concat());
        assert_eq!(stats.snapshot().commands, 2);
    }

    #[tokio::test]
    async fn serve_session_supports_group_navigation_commands() {
        let (output, stats) = run_session_with_input(
            test_config(),
            b"GROUP alt.test\r\nLISTGROUP alt.test\r\nLAST\r\nNEXT\r\n",
        )
        .await;

        assert_eq!(
            output,
            [
                GREETING,
                GROUP_RESPONSE,
                LISTGROUP_RESPONSE,
                LAST_RESPONSE,
                NEXT_RESPONSE,
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 4);
    }

    #[tokio::test]
    async fn serve_session_supports_discovery_commands() {
        let (output, stats) = run_session_with_input(
            test_config(),
            b"NEWGROUPS 20260101 000000 GMT\r\nNEWNEWS comp.lang.* 20260101 000000\r\n",
        )
        .await;

        assert_eq!(
            output,
            [GREETING, NEWGROUPS_RESPONSE, NEWNEWS_RESPONSE].concat()
        );
        assert_eq!(stats.snapshot().commands, 2);
    }

    #[tokio::test]
    async fn serve_session_supports_remaining_rfc_commands() {
        let (output, stats) = run_session_with_input(
            test_config(),
            b"POST\r\nIHAVE <ihave@test>\r\nCHECK <check@test>\r\nTAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\r\n.\r\nAUTHINFO USER bench\r\nAUTHINFO PASS bench\r\nSTARTTLS\r\n",
        )
        .await;

        assert_eq!(
            output,
            [
                GREETING,
                POST_RESPONSE,
                IHAVE_RESPONSE,
                CHECK_RESPONSE,
                TAKETHIS_RESPONSE,
                AUTHINFO_USER_RESPONSE,
                AUTHINFO_RESPONSE,
                STARTTLS_RESPONSE,
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 7);
    }

    #[tokio::test]
    async fn serve_session_handles_large_response_after_pending_small_response() {
        let mut args = test_args();
        args.body_bytes = 8192;
        args.article_bytes = 8192;
        args.flush = true;
        let (output, stats) = run_session_with_input(
            Arc::new(ServerConfig::from_args(args)),
            b"CAPABILITIES\r\nBODY 1\r\n",
        )
        .await;

        let text = String::from_utf8_lossy(&output);
        assert!(text.find("101 Capability").unwrap() < text.find("222 1").unwrap());
        assert!(output.ends_with(b"\r\n.\r\n"));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.body_requests, 1);
    }

    #[tokio::test]
    async fn serve_session_flushes_small_response_when_client_closes() {
        let (output, stats) = run_session_with_input(test_config(), b"CAPABILITIES\r\n").await;
        assert!(output.starts_with(GREETING));
        assert!(output.ends_with(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n"));
        assert_eq!(stats.snapshot().commands, 1);
    }

    #[tokio::test]
    async fn serve_session_flushes_on_quit_when_configured() {
        let mut args = test_args();
        args.flush = true;
        let (output, stats) =
            run_session_with_input(Arc::new(ServerConfig::from_args(args)), b"QUIT\r\n").await;
        assert_eq!(
            output,
            [GREETING, b"205 closing connection\r\n".as_slice()].concat()
        );
        assert_eq!(stats.snapshot().commands, 1);
    }

    #[tokio::test]
    async fn serve_session_writes_large_response_with_no_pending_prefix() {
        let mut args = test_args();
        args.body_bytes = 8192;
        let (output, stats) =
            run_session_with_input(Arc::new(ServerConfig::from_args(args)), b"BODY 1\r\n").await;
        assert!(output.starts_with(GREETING));
        assert!(output[GREETING.len()..].starts_with(b"222 1"));
        assert!(output.ends_with(b"\r\n.\r\n"));
        assert_eq!(stats.snapshot().body_requests, 1);
    }

    #[tokio::test]
    async fn serve_session_exits_after_greeting_when_client_closes() {
        let (output, stats) = run_session_with_input(test_config(), b"").await;
        assert_eq!(output, GREETING);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 0);
        assert_eq!(snapshot.bytes_sent, GREETING.len() as u64);
    }

    #[tokio::test]
    async fn serve_session_reports_greeting_write_error() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(Stats::new());
        let server = tokio::spawn({
            let stats = stats.clone();
            async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                let mut args = test_args();
                args.article_bytes = 8192;
                serve_session(
                    stream,
                    peer_addr,
                    Arc::new(ServerConfig::from_args(args)),
                    stats,
                )
                .await
            }
        });

        let client = TcpStream::connect(addr).await.unwrap();
        socket2::SockRef::from(&client)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        drop(client);

        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn serve_session_reports_read_error_after_greeting() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(Stats::new());
        let server = tokio::spawn({
            let stats = stats.clone();
            async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                serve_session(stream, peer_addr, test_config(), stats).await
            }
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut greeting = [0_u8; GREETING.len()];
        client.read_exact(&mut greeting).await.unwrap();
        socket2::SockRef::from(&client)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        drop(client);

        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn serve_session_reports_response_write_error() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(Stats::new());
        let server = tokio::spawn({
            let stats = stats.clone();
            async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                serve_session(stream, peer_addr, test_config(), stats).await
            }
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut greeting = [0_u8; GREETING.len()];
        client.read_exact(&mut greeting).await.unwrap();
        client.write_all(b"ARTICLE 1\r\n").await.unwrap();
        socket2::SockRef::from(&client)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        drop(client);

        assert!(server.await.unwrap().is_err());
    }

    #[test]
    fn for_each_request_line_in_batch_returns_consumed_prefix() {
        let mut batch = Vec::with_capacity(8);
        let consumed =
            for_each_request_line_in_batch(b"ARTICLE 1\r\nBODY 2\r\nQUIT", 8, |request| {
                batch.push(request.kind());
            });

        assert_eq!(consumed, b"ARTICLE 1\r\nBODY 2\r\n".len());
        assert_eq!(batch, vec![RequestKind::Article, RequestKind::Body]);

        batch.clear();
        let consumed = for_each_request_line_in_batch(b"DATE\nMODE READER\n", 8, |request| {
            batch.push(request.kind());
        });
        assert_eq!(consumed, b"DATE\nMODE READER\n".len());
        assert_eq!(batch, vec![RequestKind::Date, RequestKind::ModeReader]);

        batch.clear();
        let consumed = for_each_request_line_in_batch(
            b"TAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\r\n.\r\nQUIT\r\n",
            8,
            |request| batch.push(request.kind()),
        );
        assert_eq!(
            consumed,
            b"TAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\r\n.\r\nQUIT\r\n".len()
        );
        assert_eq!(batch, vec![RequestKind::TakeThis, RequestKind::Quit]);
    }

    #[test]
    fn process_request_to_buffer_counts_and_returns_quit() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"ARTICLE <bench@nntpbench.local>"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(process_request_to_buffer(
            RequestLine::parse(b"QUIT"),
            &config,
            &stats,
            &mut output,
        ));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.article_requests, 1);
        assert!(output.starts_with(b"220 "));
        assert!(output.ends_with(b"205 closing connection\r\n"));
    }

    #[test]
    fn process_request_to_buffer_supports_negotiation_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LIST ACTIVE"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"HELP"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"MODE READER"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"DATE"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(
            output,
            [
                LIST_RESPONSE,
                HELP_RESPONSE,
                MODE_READER_RESPONSE,
                DATE_RESPONSE
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 4);
    }

    #[test]
    fn process_request_to_buffer_supports_remaining_rfc_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"POST"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"IHAVE <article@test>"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"CHECK <article@test>"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"TAKETHIS <article@test>"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"AUTHINFO USER bench"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"AUTHINFO PASS bench"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"AUTHINFO SASL bench"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"STARTTLS"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(
            output,
            [
                POST_RESPONSE,
                IHAVE_RESPONSE,
                CHECK_RESPONSE,
                TAKETHIS_RESPONSE,
                AUTHINFO_RESPONSE,
                AUTHINFO_RESPONSE,
                AUTHINFO_RESPONSE,
                STARTTLS_RESPONSE,
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 8);
    }

    #[test]
    fn process_request_to_buffer_supports_overview_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"OVER 1-10"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"XOVER 1-10"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(output, [OVER_RESPONSE, XOVER_RESPONSE].concat());
        assert_eq!(stats.snapshot().commands, 2);
    }

    #[test]
    fn process_request_to_buffer_supports_group_navigation_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"GROUP alt.binaries.test"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LISTGROUP alt.binaries.test"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LAST"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"NEXT"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(
            output,
            [
                GROUP_RESPONSE,
                LISTGROUP_RESPONSE,
                LAST_RESPONSE,
                NEXT_RESPONSE
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 4);
    }

    #[test]
    fn process_request_to_buffer_supports_discovery_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"NEWGROUPS 231231 235959 GMT"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"NEWNEWS alt.binaries.test 231231 235959 GMT"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(output, [NEWGROUPS_RESPONSE, NEWNEWS_RESPONSE].concat());
        assert_eq!(stats.snapshot().commands, 2);
    }

    #[test]
    fn process_request_to_buffer_supports_body_capabilities_and_unknown() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"BODY <body@test>"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"CAPABILITIES"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"HEAD 1"),
            &config,
            &stats,
            &mut output,
        ));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 3);
        assert_eq!(snapshot.body_requests, 1);
        assert!(output.starts_with(b"222 "));
        assert!(output.ends_with(b"500 unknown command\r\n"));
    }
}
