#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time;

pub const CRLF: &[u8] = b"\r\n";
pub const TERMINATOR: &[u8] = b"\r\n.\r\n";
pub const GREETING: &[u8] = b"201 nntpbench mock server ready\r\n";
pub const MODE_READER_RESPONSE: &[u8] = b"201 posting not permitted\r\n";
pub const DATE_RESPONSE: &[u8] = b"111 20260515120000\r\n";

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
                stream.set_nodelay(config.nodelay)?;
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

pub async fn serve_session(
    stream: TcpStream,
    _peer_addr: SocketAddr,
    config: Arc<ServerConfig>,
    stats: Arc<Stats>,
) -> io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::with_capacity(16 * 1024, reader);
    let mut command_buf = Vec::with_capacity(1024);
    let mut command_batch = [ParsedCommand::Unknown; 1024];
    let mut pending_write = Vec::with_capacity(config.max_pipeline_depth.saturating_mul(64));

    writer.write_all(GREETING).await?;
    stats
        .bytes_sent
        .fetch_add(GREETING.len() as u64, Ordering::Relaxed);
    if config.flush {
        writer.flush().await?;
    }

    loop {
        let batch_len = read_command_batch_array(
            &mut reader,
            &mut command_buf,
            &mut command_batch[..config.max_pipeline_depth],
        )
        .await?;

        if batch_len == 0 {
            return Ok(());
        }

        stats
            .pipeline_batches
            .fetch_add((batch_len > 1) as u64, Ordering::Relaxed);

        for command in &command_batch[..batch_len] {
            let should_close =
                handle_command(command, &config, &stats, &mut writer, &mut pending_write).await?;

            if should_close {
                flush_pending(&mut writer, &mut pending_write).await?;
                if config.flush {
                    writer.flush().await?;
                }
                return Ok(());
            }
        }

        flush_pending(&mut writer, &mut pending_write).await?;

        if config.flush {
            writer.flush().await?;
        }
    }
}

async fn handle_command(
    command: &ParsedCommand,
    config: &ServerConfig,
    stats: &Stats,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    pending_write: &mut Vec<u8>,
) -> io::Result<bool> {
    stats.commands.fetch_add(1, Ordering::Relaxed);

    match command {
        ParsedCommand::Article => {
            stats.article_requests.fetch_add(1, Ordering::Relaxed);
            write_response(writer, pending_write, config.article_response(), stats).await?;
            Ok(false)
        }
        ParsedCommand::Body => {
            stats.body_requests.fetch_add(1, Ordering::Relaxed);
            write_response(writer, pending_write, config.body_response(), stats).await?;
            Ok(false)
        }
        ParsedCommand::Capabilities => {
            write_response(
                writer,
                pending_write,
                b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
                stats,
            )
            .await?;
            Ok(false)
        }
        ParsedCommand::Date => {
            write_response(writer, pending_write, DATE_RESPONSE, stats).await?;
            Ok(false)
        }
        ParsedCommand::ModeReader => {
            write_response(writer, pending_write, MODE_READER_RESPONSE, stats).await?;
            Ok(false)
        }
        ParsedCommand::Quit => {
            write_response(writer, pending_write, b"205 closing connection\r\n", stats).await?;
            Ok(true)
        }
        ParsedCommand::Unknown => {
            write_response(writer, pending_write, b"500 unknown command\r\n", stats).await?;
            Ok(false)
        }
    }
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

async fn write_response(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    pending_write: &mut Vec<u8>,
    response: &[u8],
    stats: &Stats,
) -> io::Result<()> {
    stats
        .bytes_sent
        .fetch_add(response.len() as u64, Ordering::Relaxed);

    if response.len() <= 4096 {
        pending_write.extend_from_slice(response);
        return Ok(());
    }

    if !pending_write.is_empty() {
        writer.write_all(pending_write).await?;
        pending_write.clear();
    }
    writer.write_all(response).await
}

async fn flush_pending(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    pending_write: &mut Vec<u8>,
) -> io::Result<()> {
    if pending_write.is_empty() {
        return Ok(());
    }

    writer.write_all(pending_write).await?;
    pending_write.clear();
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

pub async fn read_command_batch<R>(
    reader: &mut BufReader<R>,
    command_buf: &mut Vec<u8>,
    command_batch: &mut Vec<ParsedCommand>,
    max_pipeline_depth: usize,
) -> io::Result<bool>
where
    R: tokio::io::AsyncRead + Unpin,
{
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

async fn read_command_batch_array<R>(
    reader: &mut BufReader<R>,
    command_buf: &mut Vec<u8>,
    command_batch: &mut [ParsedCommand],
) -> io::Result<usize>
where
    R: tokio::io::AsyncRead + Unpin,
{
    command_buf.clear();
    let read = reader.read_until(b'\n', command_buf).await?;
    if read == 0 {
        return Ok(0);
    }

    let mut len = 1;
    command_batch[0] = parse_command_buf(command_buf);

    while len < command_batch.len() {
        let buffer = reader.buffer();
        let Some(relative_lf) = memchr::memchr(b'\n', buffer) else {
            break;
        };
        let end = relative_lf + 1;
        command_batch[len] = CommandLine::parse(trim_line_slice(&buffer[..end]));
        reader.consume(end);
        len += 1;
    }

    Ok(len)
}

fn push_command(command_buf: &mut Vec<u8>, command_batch: &mut Vec<ParsedCommand>) {
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
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn test_args() -> ServerArgs {
        ServerArgs {
            listen: "127.0.0.1:0".parse().unwrap(),
            body_bytes: 1024,
            article_bytes: 2048,
            max_connections: 16,
            max_pipeline_depth: 8,
            backlog: 128,
            reuse_port: false,
            nodelay: true,
            stats_interval_secs: 0,
            flush: false,
        }
    }

    fn test_config() -> Arc<ServerConfig> {
        Arc::new(ServerConfig::from_args(test_args()))
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
        let config = ServerConfig::from_args(high);
        assert_eq!(config.max_pipeline_depth, 1024);
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
    async fn reads_pipelined_commands_in_fifo_order() {
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"ARTICLE <a@test>\r\nBODY <b@test>\r\nDATE\r\nMODE READER\r\nQUIT\r\n")
            .await
            .unwrap();

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = Vec::with_capacity(1024);
        let mut command_batch = Vec::with_capacity(8);

        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 8)
                .await
                .unwrap()
        );
        assert_eq!(
            command_batch,
            vec![
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
        let mut command_batch = Vec::with_capacity(8);

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
        let mut command_batch = Vec::with_capacity(8);

        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 8)
                .await
                .unwrap()
        );
        assert_eq!(command_batch, vec![ParsedCommand::Article]);
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
        let mut command_batch = Vec::with_capacity(8);

        assert!(
            read_command_batch(&mut reader, &mut command_buf, &mut command_batch, 2)
                .await
                .unwrap()
        );
        assert_eq!(
            command_batch,
            vec![ParsedCommand::Article, ParsedCommand::Body]
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
    async fn serve_session_supports_proxy_backend_negotiation_commands() {
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
}
