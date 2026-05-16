#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::fs;
use std::future::poll_fn;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrayvec::ArrayVec;
use clap::{Parser, ValueEnum};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time;

pub mod tail_buffer;

pub const CRLF: &[u8] = b"\r\n";
pub const TERMINATOR: &[u8] = b"\r\n.\r\n";
pub const GREETING: &[u8] = b"201 nntpbench mock server ready\r\n";
pub const MODE_READER_RESPONSE: &[u8] = b"201 posting not permitted\r\n";
pub const DATE_RESPONSE: &[u8] = b"111 20260515120000\r\n";
const MAX_COMMAND_LINE_BYTES: usize = 1024;
const MAX_SERVER_PIPELINE_DEPTH: usize = 1024;
const SERVER_READER_CAPACITY: usize = 64 * 1024;
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
    #[arg(long, default_value_t = 64)]
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
    #[arg(long, default_value_t = 256 * 1024)]
    pub read_buffer_bytes: usize,

    /// Set TCP_NODELAY on client sockets.
    #[arg(long, default_value_t = true)]
    pub nodelay: bool,

    /// Socket receive buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = 0)]
    pub socket_recv_buffer: usize,

    /// Socket send buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = 0)]
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

#[derive(Debug)]
struct SegmentSet {
    ids: Box<[Box<[u8]>]>,
    max_id_len: usize,
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub async fn run_client(args: ClientArgs) -> io::Result<()> {
    let config = ClientConfig::from_args(args)?;
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
        let session = ClientSession::new(&config, global_index, next_start_id, requests);
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

#[derive(Debug, Clone)]
struct ClientConfig {
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

impl ClientConfig {
    fn from_args(args: ClientArgs) -> io::Result<Self> {
        let connections = args.connections.max(1);
        let total_clients = if args.total_clients == 0 {
            connections
        } else {
            args.total_clients
        };
        let pipeline_depth = args.pipeline_depth.clamp(1, 4096);
        let read_buffer_bytes = args
            .read_buffer_bytes
            .max(tail_buffer::TERMINATOR_TAIL_SIZE);
        let segments = args
            .segments
            .as_deref()
            .map(read_segments)
            .transpose()?
            .map(Arc::new);

        Ok(Self {
            connect: args.connect,
            ports: args.ports.into_boxed_slice(),
            segments,
            requests: args.requests,
            transfer_bytes: args.transfer_bytes,
            duration: Duration::from_secs(args.duration_secs),
            connections,
            client_offset: args.client_offset,
            total_clients,
            pipeline_depth,
            command_mix: args.command_mix,
            start_id: args.start_id,
            read_buffer_bytes,
            nodelay: args.nodelay,
            socket_recv_buffer: args.socket_recv_buffer,
            socket_send_buffer: args.socket_send_buffer,
            csv: args.csv,
            stats_interval: Duration::from_secs(args.stats_interval_secs),
        })
    }

    fn endpoint_for(&self, global_index: usize) -> SocketAddr {
        let mut connect = self.connect;
        if !self.ports.is_empty() {
            connect.set_port(self.ports[global_index % self.ports.len()]);
        }
        connect
    }

    fn command_capacity(&self) -> usize {
        let max_command = self
            .segments
            .as_ref()
            .map_or(MAX_CLIENT_COMMAND_BYTES, |segments| {
                b"ARTICLE "
                    .len()
                    .max(b"BODY ".len())
                    .saturating_add(segments.max_id_len)
                    .saturating_add(CRLF.len())
            });
        self.pipeline_depth.saturating_mul(max_command)
    }
}

#[derive(Debug)]
struct ClientSession {
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
    command_buffer_bytes: usize,
    nodelay: bool,
    socket_recv_buffer: usize,
    socket_send_buffer: usize,
}

impl ClientSession {
    fn new(config: &ClientConfig, global_index: usize, start_id: u64, requests: u64) -> Self {
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
            command_buffer_bytes: config.command_capacity(),
            nodelay: config.nodelay,
            socket_recv_buffer: config.socket_recv_buffer,
            socket_send_buffer: config.socket_send_buffer,
        }
    }

    async fn run(mut self, stats: Arc<Stats>, stop: Arc<AtomicBool>) -> io::Result<()> {
        stats.accepted_connections.fetch_add(1, Ordering::Relaxed);
        stats.active_connections.fetch_add(1, Ordering::Relaxed);

        let result = self.run_inner(&stats, &stop).await;
        stats.active_connections.fetch_sub(1, Ordering::Relaxed);
        result
    }

    async fn run_inner(&mut self, stats: &Stats, stop: &AtomicBool) -> io::Result<()> {
        let mut stream = TcpStream::connect(self.connect).await?;
        #[cfg(not(coverage_nightly))]
        self.optimize_stream(&stream)?;
        #[cfg(coverage_nightly)]
        let _ = self.optimize_stream(&stream);

        let mut read_buffer = vec![0; self.read_buffer_bytes].into_boxed_slice();
        read_greeting(&mut stream, &mut read_buffer).await?;

        let mut write_buffer = vec![0; self.command_buffer_bytes].into_boxed_slice();
        let mut scanner = MultilineScanner::default();
        let mut completed = 0_u64;

        while !stop.load(Ordering::Acquire)
            && (self.requests == 0 || completed < self.requests)
            && !transfer_limit_reached(stats, self.transfer_bytes)
        {
            let remaining = self.requests.saturating_sub(completed);
            let batch = if self.requests == 0 {
                self.pipeline_depth
            } else {
                remaining.min(self.pipeline_depth as u64) as usize
            };

            let written = fill_client_command_batch(
                &mut write_buffer,
                CommandBatchSpec {
                    start_command_id: self.next_id,
                    start_request_index: completed,
                    count: batch,
                    mix: self.command_mix,
                    segments: self.segments.as_deref(),
                    client_index: self.client_index,
                    total_clients: self.total_clients,
                },
            );
            stream.write_all(&write_buffer[..written]).await?;
            stats
                .pipeline_batches
                .fetch_add(u64::from(batch > 1), Ordering::Relaxed);

            let mut responses = 0_usize;
            let mut received = 0_u64;
            while responses < batch {
                let read = stream
                    .readable()
                    .await
                    .and_then(|()| stream.try_read(&mut read_buffer));
                let read = match read {
                    Ok(read) => read,
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(err) => return Err(err),
                };
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server closed during multiline response",
                    ));
                }

                received += read as u64;
                responses += scanner.count_terminators(&read_buffer[..read]);
            }

            let batch = batch as u64;
            let (articles, bodies) = count_client_commands(self.next_id, batch, self.command_mix);
            self.next_id = self.next_id.wrapping_add(batch);
            completed += batch;

            stats.commands.fetch_add(batch, Ordering::Relaxed);
            stats
                .article_requests
                .fetch_add(articles, Ordering::Relaxed);
            stats.body_requests.fetch_add(bodies, Ordering::Relaxed);
            let previous_bytes = stats.bytes_sent.fetch_add(received, Ordering::Relaxed);
            if self.transfer_bytes != 0
                && previous_bytes.saturating_add(received) >= self.transfer_bytes
            {
                stop.store(true, Ordering::Release);
            }
        }

        Ok(())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    fn optimize_stream(&self, stream: &TcpStream) -> io::Result<()> {
        optimize_client_socket(
            stream,
            self.nodelay,
            self.socket_recv_buffer,
            self.socket_send_buffer,
        )
    }
}

fn transfer_limit_reached(stats: &Stats, transfer_bytes: u64) -> bool {
    transfer_bytes != 0 && stats.bytes_sent.load(Ordering::Relaxed) >= transfer_bytes
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
    let mut max_id_len = 0;

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
        max_id_len = max_id_len.max(id.len());
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
        max_id_len,
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

#[cfg_attr(coverage_nightly, coverage(off))]
fn optimize_client_socket(
    stream: &TcpStream,
    nodelay: bool,
    recv_buffer: usize,
    send_buffer: usize,
) -> io::Result<()> {
    stream.set_nodelay(nodelay)?;
    let socket = socket2::SockRef::from(stream);
    if recv_buffer != 0 {
        socket.set_recv_buffer_size(recv_buffer)?;
    }
    if send_buffer != 0 {
        socket.set_send_buffer_size(send_buffer)?;
    }
    Ok(())
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

        let read = stream
            .readable()
            .await
            .and_then(|()| stream.try_read(&mut buffer[total..]));
        let read = match read {
            Ok(read) => read,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        };
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

#[derive(Clone, Copy)]
struct CommandBatchSpec<'a> {
    start_command_id: u64,
    start_request_index: u64,
    count: usize,
    mix: ClientCommandMix,
    segments: Option<&'a SegmentSet>,
    client_index: usize,
    total_clients: usize,
}

fn fill_client_command_batch(output: &mut [u8], spec: CommandBatchSpec<'_>) -> usize {
    let mut written = 0;
    for offset in 0..spec.count {
        let id = spec.start_command_id.wrapping_add(offset as u64);
        let request_index = spec.start_request_index.wrapping_add(offset as u64);
        let kind = client_command_kind(id, spec.mix);
        written += if let Some(segments) = spec.segments {
            let segment = segment_for_request(
                segments,
                spec.client_index,
                spec.total_clients,
                request_index,
            );
            write_segment_command(&mut output[written..], kind, segment)
        } else {
            write_synthetic_command(&mut output[written..], kind, id)
        };
    }
    written
}

fn write_synthetic_command(output: &mut [u8], kind: ClientCommandMix, id: u64) -> usize {
    let prefix: &[u8] = match kind {
        ClientCommandMix::Article | ClientCommandMix::Alternate => b"ARTICLE <bench.",
        ClientCommandMix::Body => b"BODY <bench.",
    };
    let suffix = b"@nntpbench.local>\r\n";

    let mut written = 0;
    output[..prefix.len()].copy_from_slice(prefix);
    written += prefix.len();
    written += write_u64_ascii(&mut output[written..], id);
    output[written..written + suffix.len()].copy_from_slice(suffix);
    written + suffix.len()
}

fn write_segment_command(output: &mut [u8], kind: ClientCommandMix, segment: &[u8]) -> usize {
    let prefix: &[u8] = match kind {
        ClientCommandMix::Article | ClientCommandMix::Alternate => b"ARTICLE ",
        ClientCommandMix::Body => b"BODY ",
    };

    let mut written = 0;
    output[..prefix.len()].copy_from_slice(prefix);
    written += prefix.len();
    output[written..written + segment.len()].copy_from_slice(segment);
    written += segment.len();
    output[written..written + CRLF.len()].copy_from_slice(CRLF);
    written + CRLF.len()
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

fn write_u64_ascii(output: &mut [u8], value: u64) -> usize {
    let mut digits = [0_u8; 20];
    let mut cursor = digits.len();
    let mut value = value;

    loop {
        cursor -= 1;
        digits[cursor] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }

    let len = digits.len() - cursor;
    output[..len].copy_from_slice(&digits[cursor..]);
    len
}

fn client_command_kind(id: u64, mix: ClientCommandMix) -> ClientCommandMix {
    match mix {
        ClientCommandMix::Alternate if id.is_multiple_of(2) => ClientCommandMix::Body,
        ClientCommandMix::Alternate => ClientCommandMix::Article,
        command => command,
    }
}

fn count_client_commands(start_id: u64, count: u64, mix: ClientCommandMix) -> (u64, u64) {
    match mix {
        ClientCommandMix::Article => (count, 0),
        ClientCommandMix::Body => (0, count),
        ClientCommandMix::Alternate => {
            let odd_ids =
                count / 2 + u64::from(!count.is_multiple_of(2) && !start_id.is_multiple_of(2));
            (odd_ids, count - odd_ids)
        }
    }
}

#[derive(Debug, Default)]
struct MultilineScanner {
    tail: tail_buffer::TailBuffer,
}

impl MultilineScanner {
    fn count_terminators(&mut self, input: &[u8]) -> usize {
        let mut count = 0;
        let mut remaining = input;

        while !remaining.is_empty() {
            match self.tail.detect_terminator(remaining) {
                tail_buffer::TerminatorStatus::FoundAt(end) => {
                    count += 1;
                    self.tail = tail_buffer::TailBuffer::default();
                    remaining = &remaining[end..];
                }
                tail_buffer::TerminatorStatus::NotFound => {
                    self.tail.update(remaining);
                    break;
                }
            }
        }
        count
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
    stream: TcpStream,
    _peer_addr: SocketAddr,
    config: Arc<ServerConfig>,
    session_stats: &mut SessionStats,
) -> io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::with_capacity(SERVER_READER_CAPACITY, reader);
    let max_pipeline_depth = config.max_pipeline_depth.min(MAX_SERVER_PIPELINE_DEPTH);
    let mut command_buf = Vec::with_capacity(MAX_COMMAND_LINE_BYTES);
    let mut command_batch = CommandBatch::new();
    let mut pending_write = PendingWrite::new();

    writer.write_all(GREETING).await?;
    session_stats.bytes_sent += GREETING.len() as u64;
    if config.flush {
        #[cfg(not(coverage_nightly))]
        writer.flush().await?;
        #[cfg(coverage_nightly)]
        let _ = writer.flush().await;
    }

    loop {
        if !read_command_batch(
            &mut reader,
            &mut command_buf,
            &mut command_batch,
            max_pipeline_depth,
        )
        .await?
        {
            break;
        }

        session_stats.pipeline_batches += u64::from(command_batch.len() > 1);
        for command in &command_batch {
            let should_close = handle_command(
                command,
                &config,
                session_stats,
                &mut writer,
                &mut pending_write,
            )
            .await?;
            if should_close {
                flush_pending(&mut writer, &mut pending_write).await?;
                if config.flush {
                    #[cfg(not(coverage_nightly))]
                    writer.flush().await?;
                    #[cfg(coverage_nightly)]
                    let _ = writer.flush().await;
                }
                return Ok(());
            }
        }

        flush_pending(&mut writer, &mut pending_write).await?;
        if config.flush {
            #[cfg(not(coverage_nightly))]
            writer.flush().await?;
            #[cfg(coverage_nightly)]
            let _ = writer.flush().await;
        }
    }

    flush_pending(&mut writer, &mut pending_write).await?;
    if config.flush {
        #[cfg(not(coverage_nightly))]
        writer.flush().await?;
        #[cfg(coverage_nightly)]
        let _ = writer.flush().await;
    }

    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn handle_command<W>(
    command: &ParsedCommand,
    config: &ServerConfig,
    session_stats: &mut SessionStats,
    writer: &mut W,
    pending_write: &mut PendingWrite,
) -> io::Result<bool>
where
    W: AsyncWrite + Unpin,
{
    session_stats.commands += 1;

    match command {
        ParsedCommand::Article => {
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
        ParsedCommand::Body => {
            session_stats.body_requests += 1;
            write_response(writer, pending_write, config.body_response(), session_stats).await?;
            Ok(false)
        }
        ParsedCommand::Capabilities => {
            write_response(
                writer,
                pending_write,
                b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        ParsedCommand::Date => {
            write_response(writer, pending_write, DATE_RESPONSE, session_stats).await?;
            Ok(false)
        }
        ParsedCommand::ModeReader => {
            write_response(writer, pending_write, MODE_READER_RESPONSE, session_stats).await?;
            Ok(false)
        }
        ParsedCommand::Quit => {
            write_response(
                writer,
                pending_write,
                b"205 closing connection\r\n",
                session_stats,
            )
            .await?;
            Ok(true)
        }
        ParsedCommand::Unknown => {
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

pub fn process_command_to_buffer(
    command: ParsedCommand,
    config: &ServerConfig,
    stats: &Stats,
    output: &mut Vec<u8>,
) -> bool {
    stats.commands.fetch_add(1, Ordering::Relaxed);

    let response = match command {
        ParsedCommand::Article => {
            stats.article_requests.fetch_add(1, Ordering::Relaxed);
            config.article_response()
        }
        ParsedCommand::Body => {
            stats.body_requests.fetch_add(1, Ordering::Relaxed);
            config.body_response()
        }
        ParsedCommand::Capabilities => b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
        ParsedCommand::Date => DATE_RESPONSE,
        ParsedCommand::ModeReader => MODE_READER_RESPONSE,
        ParsedCommand::Quit => b"205 closing connection\r\n",
        ParsedCommand::Unknown => b"500 unknown command\r\n",
    };

    stats
        .bytes_sent
        .fetch_add(response.len() as u64, Ordering::Relaxed);
    output.extend_from_slice(response);
    matches!(command, ParsedCommand::Quit)
}

pub fn parse_command_batch_bytes(
    input: &[u8],
    max_pipeline_depth: usize,
    command_batch: &mut Vec<ParsedCommand>,
) -> usize {
    command_batch.clear();
    let mut start = 0;

    while command_batch.len() < max_pipeline_depth {
        let Some(relative_lf) = memchr::memchr(b'\n', &input[start..]) else {
            break;
        };
        let end = start + relative_lf + 1;
        command_batch.push(CommandLine::parse(trim_line_slice(&input[start..end])));
        start = end;
    }

    start
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedCommand {
    Article,
    Body,
    Capabilities,
    Date,
    ModeReader,
    Quit,
    Unknown,
}

type CommandBatch = ArrayVec<ParsedCommand, MAX_SERVER_PIPELINE_DEPTH>;

pub struct CommandLine;

impl CommandLine {
    pub fn parse(line: &[u8]) -> ParsedCommand {
        let (verb, arg) = split_once_space(line).map_or((line, &[][..]), |(verb, arg)| (verb, arg));

        if verb_eq(verb, b"ARTICLE") {
            ParsedCommand::Article
        } else if verb_eq(verb, b"BODY") {
            ParsedCommand::Body
        } else if verb_eq(verb, b"CAPABILITIES") {
            ParsedCommand::Capabilities
        } else if verb_eq(verb, b"DATE") {
            ParsedCommand::Date
        } else if verb_eq(verb, b"MODE") && verb_eq(arg, b"READER") {
            ParsedCommand::ModeReader
        } else if verb_eq(verb, b"QUIT") {
            ParsedCommand::Quit
        } else {
            ParsedCommand::Unknown
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn read_command_batch<R>(
    reader: &mut BufReader<R>,
    command_buf: &mut Vec<u8>,
    command_batch: &mut CommandBatch,
    max_pipeline_depth: usize,
) -> io::Result<bool>
where
    R: tokio::io::AsyncRead + Unpin,
{
    command_batch.clear();
    command_buf.clear();
    let read = reader.read_until(b'\n', command_buf).await?;
    if read == 0 {
        return Ok(false);
    }
    push_command(command_buf, command_batch);

    while command_batch.len() < max_pipeline_depth {
        if memchr::memchr(b'\n', reader.buffer()).is_none() {
            break;
        }

        command_buf.clear();
        reader.read_until(b'\n', command_buf).await?;
        push_command(command_buf, command_batch);
    }

    Ok(true)
}

fn push_command(command_buf: &mut Vec<u8>, command_batch: &mut CommandBatch) {
    command_batch.push(parse_command_buf(command_buf));
}

fn parse_command_buf(command_buf: &mut Vec<u8>) -> ParsedCommand {
    trim_command_line(command_buf);
    CommandLine::parse(command_buf)
}

fn split_once_space(line: &[u8]) -> Option<(&[u8], &[u8])> {
    let pos = memchr::memchr(b' ', line)?;
    let verb = &line[..pos];
    let arg = line[pos + 1..].trim_ascii();
    Some((verb, arg))
}

fn verb_eq(actual: &[u8], expected_upper: &[u8]) -> bool {
    actual.len() == expected_upper.len()
        && actual
            .iter()
            .zip(expected_upper)
            .all(|(left, right)| left.to_ascii_uppercase() == *right)
}

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
    &body[header_end..]
}

const BODY_LINE: &[u8] =
    b"This is synthetic NNTP article payload for throughput and latency benchmarking.\r\n";

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
            socket_recv_buffer: 0,
            socket_send_buffer: 0,
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
            read_buffer_bytes: 256 * 1024,
            nodelay: true,
            socket_recv_buffer: 0,
            socket_send_buffer: 0,
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

        let err = handle_command(
            &ParsedCommand::Article,
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
        let mut command_batch = CommandBatch::new();
        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 4)
                .await
                .unwrap()
        );
        assert_eq!(
            command_batch.as_slice(),
            &[ParsedCommand::Article, ParsedCommand::Body]
        );
        assert!(
            !read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 4)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn read_command_batch_reports_reader_error() {
        let mut reader = BufReader::with_capacity(32, FailingRead);
        let mut command_buf = Vec::new();
        let mut command_batch = CommandBatch::new();

        let err = read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 1)
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
    fn parses_supported_commands_case_insensitively() {
        assert_eq!(CommandLine::parse(b"ARTICLE <x@y>"), ParsedCommand::Article);
        assert_eq!(CommandLine::parse(b"ARTICLE"), ParsedCommand::Article);
        assert_eq!(CommandLine::parse(b"body 123"), ParsedCommand::Body);
        assert_eq!(CommandLine::parse(b"BoDy"), ParsedCommand::Body);
        assert_eq!(
            CommandLine::parse(b"CAPABILITIES"),
            ParsedCommand::Capabilities
        );
        assert_eq!(
            CommandLine::parse(b"capabilities"),
            ParsedCommand::Capabilities
        );
        assert_eq!(CommandLine::parse(b"DATE"), ParsedCommand::Date);
        assert_eq!(CommandLine::parse(b"date"), ParsedCommand::Date);
        assert_eq!(
            CommandLine::parse(b"MODE READER"),
            ParsedCommand::ModeReader
        );
        assert_eq!(
            CommandLine::parse(b"mode reader"),
            ParsedCommand::ModeReader
        );
        assert_eq!(CommandLine::parse(b"QUIT"), ParsedCommand::Quit);
        assert_eq!(CommandLine::parse(b"quit"), ParsedCommand::Quit);
        assert_eq!(CommandLine::parse(b"HEAD 1"), ParsedCommand::Unknown);
        assert_eq!(CommandLine::parse(b"ARTICLEZ 1"), ParsedCommand::Unknown);
        assert_eq!(CommandLine::parse(b"MODE TRANSIT"), ParsedCommand::Unknown);
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

        let config = ClientConfig::from_args(args).unwrap();
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
        assert!(config.command_capacity() >= b"BODY <wrapped@test>\r\n".len());
    }

    #[test]
    fn client_config_rejects_invalid_or_empty_segment_files() {
        let invalid = write_temp_segments("invalid", "missing-tab\n");
        let mut args = test_client_args();
        args.segments = Some(invalid.clone());
        let err = ClientConfig::from_args(args).unwrap_err();
        fs::remove_file(invalid).unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let empty = write_temp_segments("empty", "\n\n");
        let mut args = test_client_args();
        args.segments = Some(empty.clone());
        let err = ClientConfig::from_args(args).unwrap_err();
        fs::remove_file(empty).unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let mut args = test_client_args();
        args.segments = Some(std::env::temp_dir().join("nntpbench-missing-segments"));
        let err = ClientConfig::from_args(args).unwrap_err();
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

        let config = ClientConfig::from_args(args).unwrap();
        fs::remove_file(path).unwrap();
        let session = ClientSession::new(&config, 3, 55, 7);

        assert_eq!(session.connect.port(), 1301);
        assert_eq!(session.client_index, 3);
        assert_eq!(session.total_clients, 8);
        assert_eq!(session.requests, 7);
        assert_eq!(session.transfer_bytes, 100);
        assert_eq!(session.next_id, 55);
        assert_eq!(session.pipeline_depth, 3);
        assert_eq!(session.command_mix, ClientCommandMix::Article);
        assert_eq!(session.read_buffer_bytes, 64);
        assert!(session.command_buffer_bytes >= b"ARTICLE <two@test>\r\n".len() * 3);
        assert!(!session.nodelay);
        assert_eq!(session.socket_recv_buffer, 1024);
        assert_eq!(session.socket_send_buffer, 2048);
        assert!(session.segments.is_some());
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
        assert_eq!(segments.max_id_len, b"<wrapped@test>".len());
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

    #[test]
    fn write_u64_ascii_handles_zero_and_large_values() {
        let mut output = [0_u8; 20];
        let written = write_u64_ascii(&mut output, 0);
        assert_eq!(&output[..written], b"0");

        let written = write_u64_ascii(&mut output, u64::MAX);
        assert_eq!(&output[..written], b"18446744073709551615");
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

    #[tokio::test]
    async fn optimize_client_socket_accepts_explicit_buffers() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr);
        let accepted = listener.accept();
        let (client, accepted) = tokio::join!(client, accepted);
        let client = client.unwrap();
        let (_server, _) = accepted.unwrap();

        optimize_client_socket(&client, true, 4096, 4096).unwrap();
        optimize_client_socket(&client, false, 0, 0).unwrap();
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
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 2);
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
    async fn client_session_reports_connect_error() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 1;
        args.pipeline_depth = 1;
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 1);
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
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 0);
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
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 1);
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
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 1);
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
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 1);
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
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 1);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        let err = session.run(stats.clone(), stop).await.unwrap_err();
        server.await.unwrap();

        assert_ne!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(stats.snapshot().active_connections, 0);
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
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 8)
                .await
                .unwrap()
        );
        assert_eq!(
            command_batch.as_slice(),
            &[
                ParsedCommand::Article,
                ParsedCommand::Body,
                ParsedCommand::Date,
                ParsedCommand::ModeReader,
                ParsedCommand::Quit
            ]
        );
    }

    #[tokio::test]
    async fn read_command_batch_returns_false_on_clean_eof() {
        let (client, server) = tokio::io::duplex(1024);
        drop(client);
        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = Vec::with_capacity(1024);
        let mut command_batch = CommandBatch::new();

        assert!(
            !read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 8)
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
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 8)
                .await
                .unwrap()
        );
        assert_eq!(command_batch.as_slice(), &[ParsedCommand::Article]);
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
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 2)
                .await
                .unwrap()
        );
        assert_eq!(
            command_batch.as_slice(),
            &[ParsedCommand::Article, ParsedCommand::Body]
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
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 2)
                .await
                .unwrap()
        );
        assert_eq!(
            command_batch.as_slice(),
            &[ParsedCommand::Article, ParsedCommand::Body]
        );

        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 2)
                .await
                .unwrap()
        );
        assert_eq!(command_batch.as_slice(), &[ParsedCommand::Quit]);
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
    fn parse_command_batch_bytes_returns_consumed_prefix() {
        let mut batch = Vec::with_capacity(8);
        let consumed = parse_command_batch_bytes(b"ARTICLE 1\r\nBODY 2\r\nQUIT", 8, &mut batch);

        assert_eq!(consumed, b"ARTICLE 1\r\nBODY 2\r\n".len());
        assert_eq!(batch, vec![ParsedCommand::Article, ParsedCommand::Body]);

        let consumed = parse_command_batch_bytes(b"DATE\nMODE READER\n", 8, &mut batch);
        assert_eq!(consumed, b"DATE\nMODE READER\n".len());
        assert_eq!(batch, vec![ParsedCommand::Date, ParsedCommand::ModeReader]);
    }

    #[test]
    fn process_command_to_buffer_counts_and_returns_quit() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_command_to_buffer(
            ParsedCommand::Article,
            &config,
            &stats,
            &mut output,
        ));
        assert!(process_command_to_buffer(
            ParsedCommand::Quit,
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
    fn process_command_to_buffer_supports_negotiation_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_command_to_buffer(
            ParsedCommand::ModeReader,
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_command_to_buffer(
            ParsedCommand::Date,
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(output, [MODE_READER_RESPONSE, DATE_RESPONSE].concat());
        assert_eq!(stats.snapshot().commands, 2);
    }

    #[test]
    fn process_command_to_buffer_supports_body_capabilities_and_unknown() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_command_to_buffer(
            ParsedCommand::Body,
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_command_to_buffer(
            ParsedCommand::Capabilities,
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_command_to_buffer(
            ParsedCommand::Unknown,
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

    #[test]
    fn client_command_batch_generates_article_and_body_without_growth() {
        let mut output = [0_u8; 4 * MAX_CLIENT_COMMAND_BYTES];
        let written = fill_client_command_batch(
            &mut output,
            CommandBatchSpec {
                start_command_id: 1,
                start_request_index: 0,
                count: 4,
                mix: ClientCommandMix::Alternate,
                segments: None,
                client_index: 0,
                total_clients: 1,
            },
        );

        assert_eq!(
            &output[..written],
            b"ARTICLE <bench.1@nntpbench.local>\r\n\
BODY <bench.2@nntpbench.local>\r\n\
ARTICLE <bench.3@nntpbench.local>\r\n\
BODY <bench.4@nntpbench.local>\r\n"
        );
    }

    #[test]
    fn client_command_batch_can_use_segment_message_ids() {
        let segments = SegmentSet {
            ids: vec![
                b"<a@nntpbench.local>".to_vec().into_boxed_slice(),
                b"<b@nntpbench.local>".to_vec().into_boxed_slice(),
            ]
            .into_boxed_slice(),
            max_id_len: b"<a@nntpbench.local>".len(),
        };
        let mut output = [0_u8; 128];
        let written = fill_client_command_batch(
            &mut output,
            CommandBatchSpec {
                start_command_id: 0,
                start_request_index: 0,
                count: 2,
                mix: ClientCommandMix::Alternate,
                segments: Some(&segments),
                client_index: 0,
                total_clients: 1,
            },
        );

        assert_eq!(
            &output[..written],
            b"BODY <a@nntpbench.local>\r\nARTICLE <b@nntpbench.local>\r\n"
        );
    }

    #[test]
    fn client_command_counts_match_alternating_id_parity() {
        assert_eq!(
            count_client_commands(1, 5, ClientCommandMix::Alternate),
            (3, 2)
        );
        assert_eq!(
            count_client_commands(2, 5, ClientCommandMix::Alternate),
            (2, 3)
        );
        assert_eq!(
            count_client_commands(7, 5, ClientCommandMix::Article),
            (5, 0)
        );
        assert_eq!(count_client_commands(7, 5, ClientCommandMix::Body), (0, 5));
    }

    #[test]
    fn multiline_scanner_uses_tail_buffer_across_boundaries() {
        let mut scanner = MultilineScanner::default();

        assert_eq!(scanner.count_terminators(b"222 body\r\npayload\r"), 0);
        assert_eq!(scanner.count_terminators(b"\n.\r"), 0);
        assert_eq!(scanner.count_terminators(b"\n220 article\r\n.\r\n"), 2);
    }
}
