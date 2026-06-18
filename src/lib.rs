#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

#[cfg(test)]
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(test)]
use std::cell::Cell;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::future::poll_fn;
use std::io::{self, BufRead, IoSlice, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrayvec::ArrayVec;
use clap::{ArgAction, Parser, ValueEnum};
use socket2::{Domain, Protocol, SockRef, Socket, Type};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time;

use crate::terminator::{
    BoundedResponseLineStatus, DOT_TERMINATOR, ResponseLineStatus, append_dot_terminator,
    detect_bounded_response_line_end, detect_response_line_end_from, find_crlf_line_end,
    find_dot_terminated_block_end, strip_complete_crlf_line,
};
#[cfg(test)]
use crate::terminator::{strip_dot_terminator_suffix, target_before_dot_terminator};

#[cfg(test)]
#[global_allocator]
static TEST_ALLOCATOR: CountingAllocator = CountingAllocator;

#[cfg(test)]
struct CountingAllocator;

#[cfg(test)]
static TEST_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
thread_local! {
    static COUNT_TEST_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        COUNT_TEST_ALLOCATIONS.with(|enabled| {
            if enabled.get() {
                TEST_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        COUNT_TEST_ALLOCATIONS.with(|enabled| {
            if enabled.get() {
                TEST_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            }
        });
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

mod article_store;
pub mod client;
pub mod protocol;
pub mod terminator;

use article_store::{
    ArticleDownloadTarget, ArticleStoreKey, article_ref_for_download_target, download_target_label,
    open_article_response_into, read_article_targets, verify_article_response_file_into,
    write_article_response_file_into, write_failed_article_response_file_into,
};
#[cfg(test)]
use article_store::{
    article_download_target_path_into, article_id_tree_path, message_id_tree_path,
};

pub use client::{
    Client, ClientConnection, ClientError, ClientOptions, OwnedArticle, OwnedArticleExchange,
    OwnedExchange, OwnedResponse,
};
pub use protocol::{
    Article, ArticleNumber, ArticleParseError, ArticleRef, ArticleSelector, ArticleTransfer,
    AuthInfoKind, AuthInfoValue, GroupName, HeaderIter, HeaderName, Headers, ListGroupRange,
    ListKind, MessageId, NntpDate, NntpTime, Request, RequestKind, RequestLine, StatusCode,
    Wildmat,
};

pub const CRLF: &[u8] = b"\r\n";
pub const TERMINATOR: &[u8] = b"\r\n.\r\n";
pub const GREETING: &[u8] = b"201 nntpbench mock server ready\r\n";
pub const MODE_READER_RESPONSE: &[u8] = b"201 posting not permitted\r\n";
pub const DATE_RESPONSE: &[u8] = b"111 20260602120000\r\n";
pub const LIST_RESPONSE: &[u8] =
    b"215 list of newsgroups follows\r\ncomp.lang.rust 0000000001 0000000001 y\r\nalt.test 0000000003 0000000001 y\r\n.\r\n";
const LIST_ACTIVE_COMP_RESPONSE: &[u8] =
    b"215 list of newsgroups follows\r\ncomp.lang.rust 0000000001 0000000001 y\r\n.\r\n";
const LIST_ACTIVE_ALT_RESPONSE: &[u8] =
    b"215 list of newsgroups follows\r\nalt.test 0000000003 0000000001 y\r\n.\r\n";
const LIST_EMPTY_RESPONSE: &[u8] = b"215 list of newsgroups follows\r\n.\r\n";
pub const LIST_ACTIVE_TIMES_RESPONSE: &[u8] =
    b"215 information follows\r\ncomp.lang.rust 1715904000 admin@nntpbench.local\r\nalt.test 1715907600 admin@nntpbench.local\r\n.\r\n";
const LIST_ACTIVE_TIMES_COMP_RESPONSE: &[u8] =
    b"215 information follows\r\ncomp.lang.rust 1715904000 admin@nntpbench.local\r\n.\r\n";
const LIST_ACTIVE_TIMES_ALT_RESPONSE: &[u8] =
    b"215 information follows\r\nalt.test 1715907600 admin@nntpbench.local\r\n.\r\n";
const LIST_INFO_EMPTY_RESPONSE: &[u8] = b"215 information follows\r\n.\r\n";
pub const LIST_NEWSGROUPS_RESPONSE: &[u8] =
    b"215 information follows\r\ncomp.lang.rust The Rust programming language\r\nalt.test Synthetic benchmark group\r\n.\r\n";
const LIST_NEWSGROUPS_COMP_RESPONSE: &[u8] =
    b"215 information follows\r\ncomp.lang.rust The Rust programming language\r\n.\r\n";
const LIST_NEWSGROUPS_ALT_RESPONSE: &[u8] =
    b"215 information follows\r\nalt.test Synthetic benchmark group\r\n.\r\n";
pub const LIST_OVERVIEW_FMT_RESPONSE: &[u8] =
    b"215 Order of fields in overview database\r\nSubject:\r\nFrom:\r\nDate:\r\nMessage-ID:\r\nReferences:\r\n:bytes\r\n:lines\r\n.\r\n";
pub const LIST_HEADERS_RESPONSE: &[u8] =
    b"215 headers supported\r\n:bytes\r\n:lines\r\nSubject\r\nFrom\r\nDate\r\nMessage-ID\r\nReferences\r\n.\r\n";
pub const LIST_DISTRIB_PATS_RESPONSE: &[u8] =
    b"215 distribution patterns\r\n1:*:world\r\n2:*.local:local\r\n.\r\n";
pub const GROUP_RESPONSE: &[u8] = b"211 3 1 3 alt.test\r\n";
const GROUP_COMP_RESPONSE: &[u8] = b"211 1 1 1 comp.lang.rust\r\n";
pub const LISTGROUP_RESPONSE: &[u8] = b"211 3 1 3 alt.test\r\n1\r\n2\r\n3\r\n.\r\n";
const LISTGROUP_COMP_RESPONSE: &[u8] = b"211 1 1 1 comp.lang.rust\r\n1\r\n.\r\n";
const LISTGROUP_COMP_EMPTY_RESPONSE: &[u8] = b"211 1 1 1 comp.lang.rust\r\n.\r\n";
const LISTGROUP_EMPTY_RESPONSE: &[u8] = b"211 3 1 3 alt.test\r\n.\r\n";
const LISTGROUP_2_3_RESPONSE: &[u8] = b"211 3 1 3 alt.test\r\n2\r\n3\r\n.\r\n";
pub const LAST_RESPONSE: &[u8] =
    b"223 1 <prev@alt.test> article retrieved - request text separately\r\n";
pub const NEXT_RESPONSE: &[u8] =
    b"223 2 <next@alt.test> article retrieved - request text separately\r\n";
const NAVIGATION_ARTICLE_2_RESPONSE: &[u8] =
    b"223 2 <article.2@alt.test> article retrieved - request text separately\r\n";
const NAVIGATION_ARTICLE_3_RESPONSE: &[u8] =
    b"223 3 <article.3@alt.test> article retrieved - request text separately\r\n";
pub const NEWGROUPS_RESPONSE: &[u8] =
    b"231 list of new newsgroups follows\r\ncomp.lang.rust 0000000001 0000000001 y\r\nalt.test 0000000003 0000000001 y\r\n.\r\n";
const NEWGROUPS_EMPTY_RESPONSE: &[u8] = b"231 list of new newsgroups follows\r\n.\r\n";
pub const NEWNEWS_RESPONSE: &[u8] =
    b"230 list of new articles follows\r\n<one@alt.test>\r\n<two@alt.test>\r\n.\r\n";
const NEWNEWS_EMPTY_RESPONSE: &[u8] = b"230 list of new articles follows\r\n.\r\n";
pub const POST_RESPONSE: &[u8] = b"340 send article to be posted\r\n";
pub const IHAVE_RESPONSE: &[u8] = b"335 send article to be transferred\r\n";
pub const CHECK_RESPONSE: &[u8] = b"238 <check@test> send article to be transferred\r\n";
pub const TAKETHIS_RESPONSE: &[u8] = b"239 <take@test> article transferred ok\r\n";
pub const AUTHINFO_USER_RESPONSE: &[u8] = b"381 more authentication information required\r\n";
pub const AUTHINFO_RESPONSE: &[u8] = b"281 authentication accepted\r\n";
pub const STARTTLS_RESPONSE: &[u8] = b"382 continue with TLS negotiation\r\n";
pub const OVER_RESPONSE: &[u8] = b"224 Overview information follows\r\n1\tSubject one\tone@example.com\tFri, 16 May 2026 12:00:00 +0000\t<one@example.com>\t\t123\t4\r\n.\r\n";
const OVER_MESSAGE_ID_RESPONSE: &[u8] = b"224 Overview information follows\r\n0\tSubject one\tone@example.com\tFri, 16 May 2026 12:00:00 +0000\t<one@example.com>\t\t123\t4\r\n.\r\n";
const OVER_2_RESPONSE: &[u8] = b"224 Overview information follows\r\n2\tSubject two\ttwo@example.com\tFri, 16 May 2026 12:00:01 +0000\t<two@example.com>\t<ref@example.com>\t456\t8\r\n.\r\n";
const OVER_RANGE_RESPONSE: &[u8] = b"224 Overview information follows\r\n1\tSubject one\tone@example.com\tFri, 16 May 2026 12:00:00 +0000\t<one@example.com>\t\t123\t4\r\n2\tSubject two\ttwo@example.com\tFri, 16 May 2026 12:00:01 +0000\t<two@example.com>\t<ref@example.com>\t456\t8\r\n.\r\n";
pub const XOVER_RESPONSE: &[u8] = OVER_RANGE_RESPONSE;
pub const HDR_RESPONSE: &[u8] = b"225 headers follow\r\n1 example one\r\n2 example two\r\n.\r\n";
const HDR_SUBJECT_MESSAGE_ID_RESPONSE: &[u8] = b"225 headers follow\r\n0 example one\r\n.\r\n";
const HDR_SUBJECT_1_RESPONSE: &[u8] = b"225 headers follow\r\n1 example one\r\n.\r\n";
const HDR_SUBJECT_2_RESPONSE: &[u8] = b"225 headers follow\r\n2 example two\r\n.\r\n";
pub const XHDR_RESPONSE: &[u8] = b"221 headers follow\r\n1 example one\r\n2 example two\r\n.\r\n";
const XHDR_SUBJECT_MESSAGE_ID_RESPONSE: &[u8] = b"221 headers follow\r\n0 example one\r\n.\r\n";
const XHDR_SUBJECT_1_RESPONSE: &[u8] = b"221 headers follow\r\n1 example one\r\n.\r\n";
const XHDR_SUBJECT_2_RESPONSE: &[u8] = b"221 headers follow\r\n2 example two\r\n.\r\n";
const HDR_MESSAGE_ID_RESPONSE: &[u8] =
    b"225 headers follow\r\n1 <one@example.com>\r\n2 <two@example.com>\r\n.\r\n";
const HDR_MESSAGE_ID_MESSAGE_ID_RESPONSE: &[u8] =
    b"225 headers follow\r\n0 <one@example.com>\r\n.\r\n";
const HDR_MESSAGE_ID_1_RESPONSE: &[u8] = b"225 headers follow\r\n1 <one@example.com>\r\n.\r\n";
const HDR_MESSAGE_ID_2_RESPONSE: &[u8] = b"225 headers follow\r\n2 <two@example.com>\r\n.\r\n";
const HDR_BYTES_RESPONSE: &[u8] = b"225 headers follow\r\n1 123\r\n2 456\r\n.\r\n";
const HDR_BYTES_MESSAGE_ID_RESPONSE: &[u8] = b"225 headers follow\r\n0 123\r\n.\r\n";
const HDR_BYTES_1_RESPONSE: &[u8] = b"225 headers follow\r\n1 123\r\n.\r\n";
const HDR_BYTES_2_RESPONSE: &[u8] = b"225 headers follow\r\n2 456\r\n.\r\n";
const HDR_LINES_RESPONSE: &[u8] = b"225 headers follow\r\n1 4\r\n2 8\r\n.\r\n";
const HDR_LINES_MESSAGE_ID_RESPONSE: &[u8] = b"225 headers follow\r\n0 4\r\n.\r\n";
const HDR_LINES_1_RESPONSE: &[u8] = b"225 headers follow\r\n1 4\r\n.\r\n";
const HDR_LINES_2_RESPONSE: &[u8] = b"225 headers follow\r\n2 8\r\n.\r\n";
const XHDR_MESSAGE_ID_RESPONSE: &[u8] =
    b"221 headers follow\r\n1 <one@example.com>\r\n2 <two@example.com>\r\n.\r\n";
const XHDR_MESSAGE_ID_MESSAGE_ID_RESPONSE: &[u8] =
    b"221 headers follow\r\n0 <one@example.com>\r\n.\r\n";
const XHDR_MESSAGE_ID_1_RESPONSE: &[u8] = b"221 headers follow\r\n1 <one@example.com>\r\n.\r\n";
const XHDR_MESSAGE_ID_2_RESPONSE: &[u8] = b"221 headers follow\r\n2 <two@example.com>\r\n.\r\n";
const XHDR_BYTES_RESPONSE: &[u8] = b"221 headers follow\r\n1 123\r\n2 456\r\n.\r\n";
const XHDR_BYTES_MESSAGE_ID_RESPONSE: &[u8] = b"221 headers follow\r\n0 123\r\n.\r\n";
const XHDR_BYTES_1_RESPONSE: &[u8] = b"221 headers follow\r\n1 123\r\n.\r\n";
const XHDR_BYTES_2_RESPONSE: &[u8] = b"221 headers follow\r\n2 456\r\n.\r\n";
const XHDR_LINES_RESPONSE: &[u8] = b"221 headers follow\r\n1 4\r\n2 8\r\n.\r\n";
const XHDR_LINES_MESSAGE_ID_RESPONSE: &[u8] = b"221 headers follow\r\n0 4\r\n.\r\n";
const XHDR_LINES_1_RESPONSE: &[u8] = b"221 headers follow\r\n1 4\r\n.\r\n";
const XHDR_LINES_2_RESPONSE: &[u8] = b"221 headers follow\r\n2 8\r\n.\r\n";
pub const HEAD_RESPONSE: &[u8] = b"221 1 <article.1@nntpbench.local> article retrieved\r\nPath: nntpbench.local!mock\r\nFrom: Bench User <bench@nntpbench.local>\r\nNewsgroups: alt.binaries.bench\r\nSubject: nntpbench synthetic article\r\nMessage-ID: <article.1@nntpbench.local>\r\nDate: Fri, 15 May 2026 00:00:00 +0000\r\n.\r\n";
pub const STAT_RESPONSE: &[u8] = b"223 1 <article.1@nntpbench.local> article retrieved\r\n";
pub const HELP_RESPONSE: &[u8] =
    b"100 help text follows\r\nARTICLE\r\nAUTHINFO\r\nBODY\r\nCAPABILITIES\r\nCHECK\r\nDATE\r\nGROUP\r\nHDR\r\nHEAD\r\nHELP\r\nIHAVE\r\nLAST\r\nLIST\r\nLISTGROUP\r\nMODE READER\r\nNEWGROUPS\r\nNEWNEWS\r\nNEXT\r\nOVER\r\nPOST\r\nQUIT\r\nSTARTTLS\r\nSTAT\r\nTAKETHIS\r\nXHDR\r\nXOVER\r\n.\r\n";
pub const CAPABILITIES_RESPONSE: &[u8] =
    b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nMODE-READER\r\nLIST ACTIVE ACTIVE.TIMES DISTRIB.PATS NEWSGROUPS OVERVIEW.FMT HEADERS\r\nOVER MSGID\r\nHDR\r\nNEWNEWS\r\nAUTHINFO\r\n.\r\n";
pub const QUIT_RESPONSE: &[u8] = b"205 closing connection\r\n";
pub const ARTICLE_NOT_FOUND_RESPONSE: &[u8] = b"430 no article with that number\r\n";
const BODY_RESPONSE_PREFIX: &[u8] = b"222 1 <article.1@nntpbench.local> body follows\r\n";
const ARTICLE_RESPONSE_PREFIX: &[u8] = b"220 1 <article.1@nntpbench.local> article follows\r\nPath: nntpbench.local!mock\r\nFrom: Bench User <bench@nntpbench.local>\r\nNewsgroups: alt.binaries.bench\r\nSubject: nntpbench synthetic article\r\nMessage-ID: <article.1@nntpbench.local>\r\nDate: Fri, 15 May 2026 00:00:00 +0000\r\n\r\n";
const MAX_COMMAND_LINE_BYTES: usize = protocol::MAX_INITIAL_RESPONSE_LINE_BYTES;
const MAX_SERVER_PIPELINE_DEPTH: usize = 1024;
const SERVER_READER_CAPACITY: usize = 256 * 1024;
const CLIENT_READER_CAPACITY: usize = 256 * 1024;
const DEFAULT_PENDING_WRITE_BYTES: usize = 800 * 1024;
#[cfg_attr(target_os = "macos", allow(dead_code))]
const HIGH_THROUGHPUT_SOCKET_BUFFER: usize = 16 * 1024 * 1024;
#[cfg(target_os = "macos")]
const DEFAULT_SOCKET_BUFFER: usize = 1024 * 1024;
#[cfg(not(target_os = "macos"))]
const DEFAULT_SOCKET_BUFFER: usize = HIGH_THROUGHPUT_SOCKET_BUFFER;
const PROCESS_CLOCK_TICK: Duration = Duration::from_millis(10);
const TCP_LINGER_TIMEOUT: Duration = Duration::from_secs(5);
const FAR_FUTURE_DATE_KEY: u32 = 99_991_231;
#[cfg(target_os = "linux")]
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(30);

fn current_date_response() -> [u8; 20] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    date_response_for_unix_seconds(now)
}

fn date_response_for_unix_seconds(seconds: u64) -> [u8; 20] {
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = seconds % 86_400;
    let (year, month, day) = civil_from_unix_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    let mut response = *b"111 00000000000000\r\n";
    write_four_digits(&mut response[4..8], year as u32);
    write_two_digits(&mut response[8..10], month);
    write_two_digits(&mut response[10..12], day);
    write_two_digits(&mut response[12..14], hour as u32);
    write_two_digits(&mut response[14..16], minute as u32);
    write_two_digits(&mut response[16..18], second as u32);
    response
}

fn current_utc_year() -> u16 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    let days = (now / 86_400) as i64;
    let (year, _, _) = civil_from_unix_days(days);
    year as u16
}

fn civil_from_unix_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

fn write_four_digits(output: &mut [u8], value: u32) {
    output[0] = b'0' + ((value / 1_000) % 10) as u8;
    output[1] = b'0' + ((value / 100) % 10) as u8;
    output[2] = b'0' + ((value / 10) % 10) as u8;
    output[3] = b'0' + (value % 10) as u8;
}

fn write_two_digits(output: &mut [u8], value: u32) {
    output[0] = b'0' + ((value / 10) % 10) as u8;
    output[1] = b'0' + (value % 10) as u8;
}
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

    /// Directory tree containing complete ARTICLE response frames keyed by ARTICLE selector.
    #[arg(long)]
    pub article_dir: Option<PathBuf>,

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
    #[arg(long, default_value_t = DEFAULT_SOCKET_BUFFER)]
    pub socket_recv_buffer: usize,

    /// Socket send buffer size for accepted sockets. Use 0 to leave the OS default.
    #[arg(long, default_value_t = DEFAULT_SOCKET_BUFFER)]
    pub socket_send_buffer: usize,

    /// Print benchmark statistics at this interval. Use 0 to disable periodic output.
    #[arg(long, default_value_t = 1)]
    pub stats_interval_secs: u64,

    /// Flush after each response.
    #[arg(long, default_value_t = false)]
    pub flush: bool,

    /// Per-session pending write buffer used to coalesce small generated response chunks.
    #[arg(long, default_value_t = DEFAULT_PENDING_WRITE_BYTES)]
    pub pending_write_bytes: usize,
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::string::string_regex;

    fn message_id_strategy() -> BoxedStrategy<String> {
        (
            string_regex("[A-Za-z0-9][A-Za-z0-9_-]{0,7}").unwrap(),
            string_regex("[A-Za-z0-9][A-Za-z0-9_-]{0,7}").unwrap(),
        )
            .prop_map(|(local, domain)| format!("<{local}@{domain}.test>"))
            .boxed()
    }

    fn segment_set_strategy() -> BoxedStrategy<SegmentSet> {
        vec(message_id_strategy(), 1..=4)
            .prop_map(|ids| SegmentSet {
                ids: ids
                    .into_iter()
                    .map(|id| MessageId::from_shared(Arc::<str>::from(id)).unwrap())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            })
            .boxed()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        #[test]
        fn client_request_for_command_uses_numeric_selectors_for_nonzero_synthetic_ids(
            command_id in 1_u64..=10_000,
            mix in prop_oneof![
                Just(ClientCommandMix::Article),
                Just(ClientCommandMix::Body),
                Just(ClientCommandMix::Alternate),
            ],
        ) {
            let request = client_request_for_command(command_id, 0, mix, None, 0, 1).unwrap();
            let expected_kind = client_command_kind(command_id, mix);

            match expected_kind {
                ClientCommandMix::Article => {
                    prop_assert_eq!(request.kind(), RequestKind::Article);
                    prop_assert_eq!(request.article_ref(), Some(&ArticleRef::Number(command_id)));
                }
                ClientCommandMix::Body => {
                    prop_assert_eq!(request.kind(), RequestKind::Body);
                    prop_assert_eq!(request.article_ref(), Some(&ArticleRef::Number(command_id)));
                }
                ClientCommandMix::Alternate => {
                    unreachable!("client_command_kind should normalize Alternate")
                }
            }
            prop_assert!(request.message_id().is_none());
        }

        #[test]
        fn client_request_for_command_uses_message_ids_for_zero_and_segment_inputs(
            mix in prop_oneof![
                Just(ClientCommandMix::Article),
                Just(ClientCommandMix::Body),
                Just(ClientCommandMix::Alternate),
            ],
            request_index in 0_u64..32,
            client_index in 0_usize..4,
            total_clients in 1_usize..4,
            segments in segment_set_strategy(),
        ) {
            let zero_request = client_request_for_command(0, request_index, mix, None, client_index, total_clients).unwrap();
            prop_assert!(zero_request.message_id().is_some());
            prop_assert_eq!(zero_request.message_id().unwrap().as_str(), "<bench.0@nntpbench.local>");

            let segmented = client_request_for_command(17, request_index, mix, Some(&segments), client_index, total_clients).unwrap();
            let expected = segment_for_request(&segments, client_index, total_clients, request_index);
            prop_assert_eq!(segmented.message_id().unwrap().as_str(), expected.as_str());
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClientCommandMix {
    Article,
    Body,
    Alternate,
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

    /// AUTHINFO USER value sent once per connection before workload requests.
    #[arg(long)]
    pub auth_user: Option<String>,

    /// AUTHINFO PASS value sent once per connection before workload requests.
    #[arg(long)]
    pub auth_pass: Option<String>,

    /// File containing ARTICLE numbers or message-IDs to fetch, one per line.
    #[arg(long)]
    pub article_ids: Option<PathBuf>,

    /// Directory tree where fetched ARTICLE responses are stored by ARTICLE selector.
    #[arg(long)]
    pub article_output_dir: Option<PathBuf>,

    /// Directory tree containing expected ARTICLE response frames to verify with MD5 when present.
    #[arg(long)]
    pub article_verify_dir: Option<PathBuf>,

    /// Total ARTICLE/BODY requests to complete. Use 0 to disable this limit.
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

    /// Maximum in-flight requests allowed on each connection.
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
    #[arg(long, default_value_t = DEFAULT_SOCKET_BUFFER)]
    pub socket_recv_buffer: usize,

    /// Socket send buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = DEFAULT_SOCKET_BUFFER)]
    pub socket_send_buffer: usize,

    /// Print final machine-readable CSV: requests,bytes,elapsed_s,cpu_s,rss_kib.
    #[arg(long, default_value_t = false)]
    pub csv: bool,

    /// Print benchmark statistics at this interval. Use 0 to disable periodic output.
    #[arg(long, default_value_t = 1)]
    pub stats_interval_secs: u64,
}

#[derive(Debug, Parser, Clone)]
pub struct FetchArgs {
    /// Address to connect to.
    #[arg(long, default_value = "127.0.0.1:1199")]
    pub connect: SocketAddr,

    /// Request kind to send.
    #[arg(long, value_enum)]
    pub request: FetchRequestKind,

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

    /// Wildmat pattern for NEWNEWS and LIST ACTIVE/ACTIVE.TIMES/NEWSGROUPS requests.
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

    /// Maximum in-flight requests allowed on the connection.
    #[arg(long, default_value_t = 64)]
    pub pipeline_depth: usize,

    /// Set TCP_NODELAY on client sockets.
    #[arg(long, default_value_t = true)]
    pub nodelay: bool,

    /// Socket receive buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = DEFAULT_SOCKET_BUFFER)]
    pub socket_recv_buffer: usize,

    /// Socket send buffer size in bytes. Use 0 to leave the OS default.
    #[arg(long, default_value_t = DEFAULT_SOCKET_BUFFER)]
    pub socket_send_buffer: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FetchRequestKind {
    Article,
    Body,
    Head,
    Stat,
    ListActive,
    ListActiveTimes,
    ListNewsgroups,
    ListOverviewFmt,
    ListHeaders,
    ListDistribPats,
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
    ids: Box<[MessageId<'static>]>,
}

#[cfg_attr(coverage_nightly, coverage(off))]
pub async fn run_client(args: ClientArgs) -> io::Result<()> {
    let config = ClientConfig::from_args(args)?;
    run_client_workload(config).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_client_workload(config: ClientConfig) -> io::Result<()> {
    if config.article_targets.is_some() {
        return run_article_id_download_workload(config).await;
    }

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

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_article_id_download_workload(config: ClientConfig) -> io::Result<()> {
    let start_cpu_ticks = process_cpu_ticks();
    let started = Instant::now();
    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let article_targets = config
        .article_targets
        .clone()
        .expect("article id workload requires targets");
    let output_dir = config
        .article_output_dir
        .clone()
        .expect("article id workload requires output dir");

    eprintln!(
        "nntpbench client connecting to {} article_targets={} output_dir={} connections={} total_clients={} client_offset={}",
        config.connect,
        article_targets.len(),
        output_dir.display(),
        config.connections,
        config.total_clients,
        config.client_offset
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
    for connection_index in 0..config.connections {
        let global_index = config.client_offset + connection_index;
        let session = ArticleIdDownloadSession::new(
            &config,
            global_index,
            article_targets.clone(),
            output_dir.clone(),
        );
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
pub async fn run_fetch(args: FetchArgs) -> io::Result<()> {
    let response = fetch_response(&args).await.map_err(map_client_error)?;
    io::stdout().write_all(response.as_bytes())
}

pub async fn fetch_response(args: &FetchArgs) -> Result<OwnedResponse, ClientError> {
    let request = fetch_request(args)?;
    let connection = ClientConnection::connect_with_options(
        args.connect,
        ClientOptions {
            read_buffer_bytes: args.read_buffer_bytes.max(terminator::TERMINATOR_TAIL_SIZE),
            nodelay: args.nodelay,
            socket_recv_buffer: args.socket_recv_buffer,
            socket_send_buffer: args.socket_send_buffer,
            pipeline_depth: args.pipeline_depth.clamp(1, 4096),
        },
    )
    .await?;

    connection.execute(request).await
}

fn fetch_request(args: &FetchArgs) -> Result<Request<'static>, ClientError> {
    match args.request {
        FetchRequestKind::Article => fetch_article_request(
            args,
            |message_id| Request::article(message_id),
            |selector| Request::article_selector(selector),
            Request::article_current,
        ),
        FetchRequestKind::Body => fetch_article_request(
            args,
            |message_id| Request::body(message_id),
            |selector| Request::body_selector(selector),
            Request::body_current,
        ),
        FetchRequestKind::Head => fetch_article_request(
            args,
            |message_id| Request::head(message_id),
            |selector| Request::head_selector(selector),
            Request::head_current,
        ),
        FetchRequestKind::Stat => fetch_article_request(
            args,
            |message_id| Request::stat(message_id),
            |selector| Request::stat_selector(selector),
            Request::stat_current,
        ),
        FetchRequestKind::ListActive => match args.wildmat.as_deref() {
            Some(wildmat) => {
                Request::list_active_wildmat(wildmat).map_err(|_| ClientError::InvalidWildmat)
            }
            None => Ok(Request::list_active()),
        },
        FetchRequestKind::ListActiveTimes => match args.wildmat.as_deref() {
            Some(wildmat) => {
                Request::list_active_times_wildmat(wildmat).map_err(|_| ClientError::InvalidWildmat)
            }
            None => Ok(Request::list_active_times()),
        },
        FetchRequestKind::ListNewsgroups => match args.wildmat.as_deref() {
            Some(wildmat) => {
                Request::list_newsgroups_wildmat(wildmat).map_err(|_| ClientError::InvalidWildmat)
            }
            None => Ok(Request::list_newsgroups()),
        },
        FetchRequestKind::ListOverviewFmt => Ok(Request::list_overview_fmt()),
        FetchRequestKind::ListHeaders => Ok(Request::list_headers()),
        FetchRequestKind::ListDistribPats => Ok(Request::list_distrib_pats()),
        FetchRequestKind::Group => fetch_group_request(args, |group| Request::group(group)),
        FetchRequestKind::Listgroup => fetch_listgroup_request(args),
        FetchRequestKind::Last => Ok(Request::last()),
        FetchRequestKind::Next => Ok(Request::next()),
        FetchRequestKind::Newgroups => fetch_newgroups_request(args),
        FetchRequestKind::Newnews => fetch_newnews_request(args),
        FetchRequestKind::Post => Ok(Request::post()),
        FetchRequestKind::Ihave => {
            fetch_message_id_request(args, |message_id| Request::ihave(message_id))
        }
        FetchRequestKind::Check => {
            fetch_message_id_request(args, |message_id| Request::check(message_id))
        }
        FetchRequestKind::Takethis => fetch_takethis_request(args),
        FetchRequestKind::AuthinfoUser => {
            fetch_authinfo_request(args, |value| Request::authinfo_user(value))
        }
        FetchRequestKind::AuthinfoPass => {
            fetch_authinfo_request(args, |value| Request::authinfo_pass(value))
        }
        FetchRequestKind::Starttls => Ok(Request::starttls()),
        FetchRequestKind::Over => fetch_selector_request(args, |selector| Request::over(selector)),
        FetchRequestKind::Xover => {
            fetch_selector_request(args, |selector| Request::xover(selector))
        }
        FetchRequestKind::Hdr => {
            fetch_header_request(args, |header, selector| Request::hdr(header, selector))
        }
        FetchRequestKind::Xhdr => {
            fetch_header_request(args, |header, selector| Request::xhdr(header, selector))
        }
        FetchRequestKind::List => Ok(Request::list()),
        FetchRequestKind::Help => Ok(Request::help()),
        FetchRequestKind::Capabilities => Ok(Request::capabilities()),
        FetchRequestKind::Date => Ok(Request::date()),
        FetchRequestKind::ModeReader => Ok(Request::mode_reader()),
        FetchRequestKind::Quit => Ok(Request::quit()),
    }
}

fn fetch_message_id_request<F>(args: &FetchArgs, build: F) -> Result<Request<'static>, ClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, protocol::InvalidMessageId>,
{
    let message_id = args
        .message_id
        .as_deref()
        .ok_or(ClientError::MissingMessageId)?;
    build(message_id).map_err(|_| ClientError::InvalidMessageId)
}

fn fetch_article_request<F, G, H>(
    args: &FetchArgs,
    build_message_id: F,
    build_selector: G,
    build_current: H,
) -> Result<Request<'static>, ClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, protocol::InvalidMessageId>,
    G: FnOnce(&str) -> Result<Request<'static>, protocol::InvalidArticleRef>,
    H: FnOnce() -> Request<'static>,
{
    if let Some(message_id) = args.message_id.as_deref() {
        return build_message_id(message_id).map_err(|_| ClientError::InvalidMessageId);
    }

    if let Some(selector) = args.selector.as_deref() {
        return build_selector(selector).map_err(|_| ClientError::InvalidArticleSelector);
    }

    Ok(build_current())
}

fn fetch_header_request<F>(args: &FetchArgs, build: F) -> Result<Request<'static>, ClientError>
where
    F: FnOnce(&str, &str) -> Result<Request<'static>, crate::protocol::InvalidHeaderQuery>,
{
    let header = args
        .header
        .as_deref()
        .ok_or(ClientError::MissingHeaderName)?;
    let selector = args
        .selector
        .as_deref()
        .ok_or(ClientError::MissingArticleSelector)?;
    build(header, selector).map_err(ClientError::from)
}

fn fetch_group_request<F>(args: &FetchArgs, build: F) -> Result<Request<'static>, ClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, crate::protocol::InvalidGroupName>,
{
    let group = args.group.as_deref().ok_or(ClientError::MissingGroupName)?;
    build(group).map_err(|_| ClientError::InvalidGroupName)
}

fn fetch_listgroup_request(args: &FetchArgs) -> Result<Request<'static>, ClientError> {
    match (args.group.as_deref(), args.selector.as_deref()) {
        (Some(group), Some(range)) => {
            Request::listgroup_group_range(group, range).map_err(ClientError::from)
        }
        (Some(group), None) => Request::listgroup(group).map_err(|_| ClientError::InvalidGroupName),
        (None, Some(range)) => {
            Request::listgroup_range(range).map_err(|_| ClientError::InvalidListGroupRange)
        }
        (None, None) => Ok(Request::listgroup_current()),
    }
}

fn fetch_newgroups_request(args: &FetchArgs) -> Result<Request<'static>, ClientError> {
    let date = args.date.as_deref().ok_or(ClientError::MissingDate)?;
    let time = args.time.as_deref().ok_or(ClientError::MissingTime)?;
    Request::newgroups(date, time, args.gmt).map_err(ClientError::from)
}

fn fetch_newnews_request(args: &FetchArgs) -> Result<Request<'static>, ClientError> {
    let wildmat = args.wildmat.as_deref().ok_or(ClientError::MissingWildmat)?;
    let date = args.date.as_deref().ok_or(ClientError::MissingDate)?;
    let time = args.time.as_deref().ok_or(ClientError::MissingTime)?;
    Request::newnews(wildmat, date, time, args.gmt).map_err(ClientError::from)
}

fn fetch_selector_request<F>(args: &FetchArgs, build: F) -> Result<Request<'static>, ClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, crate::protocol::InvalidArticleSelector>,
{
    let selector = args
        .selector
        .as_deref()
        .ok_or(ClientError::MissingArticleSelector)?;
    build(selector).map_err(|_| ClientError::InvalidArticleSelector)
}

fn fetch_authinfo_request<F>(args: &FetchArgs, build: F) -> Result<Request<'static>, ClientError>
where
    F: FnOnce(&str) -> Result<Request<'static>, crate::protocol::InvalidAuthInfoValue>,
{
    let value = args
        .auth_value
        .as_deref()
        .ok_or(ClientError::MissingAuthInfoValue)?;
    build(value).map_err(ClientError::from)
}

fn fetch_takethis_request(args: &FetchArgs) -> Result<Request<'static>, ClientError> {
    let message_id = args
        .message_id
        .as_deref()
        .ok_or(ClientError::MissingMessageId)?;
    let article = args
        .article_body
        .as_deref()
        .ok_or(ClientError::MissingArticleBody)?;
    Request::takethis(message_id, article.as_bytes()).map_err(|_| ClientError::InvalidMessageId)
}

fn map_client_error(err: ClientError) -> io::Error {
    match err {
        ClientError::Io(err) => err,
        ClientError::UnexpectedEof => io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "server closed before completing response",
        ),
        ClientError::ConnectionClosed => {
            io::Error::new(io::ErrorKind::BrokenPipe, "connection engine closed")
        }
        other => io::Error::new(io::ErrorKind::InvalidData, other),
    }
}

fn validate_client_auth_value(
    value: Option<String>,
    name: &'static str,
) -> io::Result<Option<AuthInfoValue<'static>>> {
    value
        .map(|value| {
            AuthInfoValue::from_owned(value).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("invalid {name} value"))
            })
        })
        .transpose()
}

#[derive(Debug, Clone)]
struct ClientConfig {
    connect: SocketAddr,
    ports: Box<[u16]>,
    segments: Option<Arc<SegmentSet>>,
    auth_user: Option<AuthInfoValue<'static>>,
    auth_pass: Option<AuthInfoValue<'static>>,
    article_targets: Option<Arc<[ArticleDownloadTarget]>>,
    article_output_dir: Option<Arc<PathBuf>>,
    article_verify_dir: Option<Arc<PathBuf>>,
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
        Self::build(
            args.connect,
            args.ports.into_boxed_slice(),
            args.segments,
            args.auth_user,
            args.auth_pass,
            args.article_ids,
            args.article_output_dir,
            args.article_verify_dir,
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
        auth_user: Option<String>,
        auth_pass: Option<String>,
        article_ids: Option<PathBuf>,
        article_output_dir: Option<PathBuf>,
        article_verify_dir: Option<PathBuf>,
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
        let auth_user = validate_client_auth_value(auth_user, "auth user")?;
        let auth_pass = validate_client_auth_value(auth_pass, "auth pass")?;
        let article_targets = if let Some(path) = article_ids.as_deref() {
            Some(Arc::from(read_article_targets(path)?.into_boxed_slice()))
        } else {
            None
        };
        if article_targets.is_some() && article_output_dir.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--article-output-dir is required with --article-ids",
            ));
        }
        if article_targets.is_none() && article_output_dir.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--article-ids is required with --article-output-dir",
            ));
        }
        if article_targets.is_none() && article_verify_dir.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--article-ids is required with --article-verify-dir",
            ));
        }
        if let Some(output_dir) = article_output_dir.as_ref() {
            fs::create_dir_all(output_dir)?;
        }

        Ok(Self {
            connect,
            ports,
            segments,
            auth_user,
            auth_pass,
            article_targets,
            article_output_dir: article_output_dir.map(Arc::new),
            article_verify_dir: article_verify_dir.map(Arc::new),
            requests,
            transfer_bytes,
            duration: Duration::from_secs(duration_secs),
            connections,
            client_offset,
            total_clients,
            pipeline_depth: pipeline_depth.clamp(1, 4096),
            command_mix,
            start_id,
            read_buffer_bytes: read_buffer_bytes.max(terminator::TERMINATOR_TAIL_SIZE),
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
struct ArticleIdDownloadSession {
    connect: SocketAddr,
    auth_user: Option<AuthInfoValue<'static>>,
    auth_pass: Option<AuthInfoValue<'static>>,
    article_targets: Arc<[ArticleDownloadTarget]>,
    output_dir: Arc<PathBuf>,
    verify_dir: Option<Arc<PathBuf>>,
    client_index: usize,
    total_clients: usize,
    read_buffer_bytes: usize,
    pipeline_depth: usize,
    nodelay: bool,
    socket_recv_buffer: usize,
    socket_send_buffer: usize,
}

impl ArticleIdDownloadSession {
    fn new(
        config: &ClientConfig,
        global_index: usize,
        article_targets: Arc<[ArticleDownloadTarget]>,
        output_dir: Arc<PathBuf>,
    ) -> Self {
        Self {
            connect: config.endpoint_for(global_index),
            auth_user: config.auth_user.clone(),
            auth_pass: config.auth_pass.clone(),
            article_targets,
            output_dir,
            verify_dir: config.article_verify_dir.clone(),
            client_index: global_index,
            total_clients: config.total_clients,
            read_buffer_bytes: config.read_buffer_bytes,
            pipeline_depth: config.pipeline_depth,
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
        let mut stream = connect_client_socket(
            self.connect,
            self.nodelay,
            self.socket_recv_buffer,
            self.socket_send_buffer,
        )
        .await?;
        read_greeting(&mut stream).await?;
        let mut response_reader = crate::client::DrainedResponseReader::new(self.read_buffer_bytes);
        authenticate_client_stream(
            &mut stream,
            &mut response_reader,
            self.auth_user,
            self.auth_pass,
        )
        .await?;

        let mut output_path = PathBuf::with_capacity(1024);
        let mut verify_path = PathBuf::with_capacity(1024);
        let mut in_flight = 0_usize;
        let mut next_send_index = self.client_index;
        let mut next_receive_index = self.client_index;

        loop {
            while !stop.load(Ordering::Acquire)
                && in_flight < self.pipeline_depth
                && next_send_index < self.article_targets.len()
            {
                let target = &self.article_targets[next_send_index];
                let article_ref = article_ref_for_download_target(target)?;
                crate::client::write_article_request_wire(&mut stream, &article_ref).await?;
                in_flight += 1;
                next_send_index = next_send_index.saturating_add(self.total_clients);
            }

            if in_flight == 0 {
                break;
            }

            let index = next_receive_index;
            next_receive_index = next_receive_index.saturating_add(self.total_clients);
            in_flight -= 1;
            let target = &self.article_targets[index];
            let response = response_reader
                .read_response_frame(&mut stream, RequestKind::Article)
                .await
                .map_err(map_client_error)?;

            stats.commands.fetch_add(1, Ordering::Relaxed);
            stats.article_requests.fetch_add(1, Ordering::Relaxed);
            stats
                .bytes_sent
                .fetch_add(response.as_bytes().len() as u64, Ordering::Relaxed);

            handle_article_id_response(
                target,
                response.status(),
                response.as_bytes(),
                &self.output_dir,
                self.verify_dir.as_deref().map(PathBuf::as_path),
                &mut output_path,
                &mut verify_path,
            )?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct ClientSession {
    connect: SocketAddr,
    segments: Option<Arc<SegmentSet>>,
    auth_user: Option<AuthInfoValue<'static>>,
    auth_pass: Option<AuthInfoValue<'static>>,
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

impl ClientSession {
    fn new(config: &ClientConfig, global_index: usize, start_id: u64, requests: u64) -> Self {
        Self {
            connect: config.endpoint_for(global_index),
            segments: config.segments.clone(),
            auth_user: config.auth_user.clone(),
            auth_pass: config.auth_pass.clone(),
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
        let mut stream = connect_client_socket(
            self.connect,
            self.nodelay,
            self.socket_recv_buffer,
            self.socket_send_buffer,
        )
        .await?;
        read_greeting(&mut stream).await?;
        let mut response_reader = crate::client::DrainedResponseReader::new(self.read_buffer_bytes);
        self.authenticate(&mut stream, &mut response_reader).await?;

        let mut in_flight = 0_usize;
        let mut issued = 0_u64;
        let mut received = 0_u64;
        let mut request_buffer = Vec::with_capacity(self.pipeline_depth.saturating_mul(32));

        let initial_fill = self
            .fill_pipeline(
                &mut stream,
                &mut request_buffer,
                in_flight,
                issued,
                stats,
                stop,
            )
            .await?;
        in_flight += initial_fill;
        issued = issued.wrapping_add(initial_fill as u64);
        if in_flight > 1 {
            stats.pipeline_batches.fetch_add(1, Ordering::Relaxed);
        }

        while in_flight > 0 {
            let command_id = self.next_id.wrapping_add(received);
            let kind = request_kind_for_client_command(command_id, self.command_mix);
            let response = response_reader
                .read_response(&mut stream, kind)
                .await
                .map_err(map_client_error)?;
            in_flight -= 1;
            received = received.wrapping_add(1);

            stats
                .bytes_sent
                .fetch_add(response.bytes_len() as u64, Ordering::Relaxed);
            stats.commands.fetch_add(1, Ordering::Relaxed);
            match client_command_kind(command_id, self.command_mix) {
                ClientCommandMix::Article => {
                    stats.article_requests.fetch_add(1, Ordering::Relaxed);
                }
                ClientCommandMix::Body => {
                    stats.body_requests.fetch_add(1, Ordering::Relaxed);
                }
                ClientCommandMix::Alternate => {
                    unreachable!("client_command_kind should normalize Alternate")
                }
            }

            if self.transfer_bytes != 0
                && stats.bytes_sent.load(Ordering::Relaxed) >= self.transfer_bytes
            {
                stop.store(true, Ordering::Release);
            }

            if in_flight <= self.pipeline_refill_low_water() {
                let filled = self
                    .fill_pipeline(
                        &mut stream,
                        &mut request_buffer,
                        in_flight,
                        issued,
                        stats,
                        stop,
                    )
                    .await?;
                in_flight += filled;
                issued = issued.wrapping_add(filled as u64);
                if filled > 1 {
                    stats.pipeline_batches.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Ok(())
    }

    async fn authenticate(
        &self,
        stream: &mut TcpStream,
        response_reader: &mut crate::client::DrainedResponseReader,
    ) -> io::Result<()> {
        let mut user_status = None;
        if let Some(value) = self.auth_user.as_ref() {
            let response =
                send_authinfo(stream, response_reader, AuthInfoKind::User, value).await?;
            let status = response.status().as_u16();
            if status != 281 && status != 381 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("AUTHINFO USER rejected with status {status}"),
                ));
            }
            user_status = Some(status);
        }

        if self.auth_pass.is_none() && user_status == Some(381) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "AUTHINFO USER requires AUTHINFO PASS",
            ));
        }

        if let (Some(381), Some(value)) = (user_status, self.auth_pass.as_ref()) {
            let response =
                send_authinfo(stream, response_reader, AuthInfoKind::Pass, value).await?;
            let status = response.status().as_u16();
            if status != 281 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("AUTHINFO PASS rejected with status {status}"),
                ));
            }
        }

        Ok(())
    }

    async fn fill_pipeline(
        &self,
        stream: &mut TcpStream,
        request_buffer: &mut Vec<u8>,
        in_flight: usize,
        issued: u64,
        stats: &Stats,
        stop: &AtomicBool,
    ) -> io::Result<usize> {
        if stop.load(Ordering::Acquire) {
            return Ok(0);
        }

        request_buffer.clear();
        let mut filled = 0usize;
        while in_flight + filled < self.pipeline_depth
            && (self.requests == 0 || issued.wrapping_add(filled as u64) < self.requests)
            && !transfer_limit_reached(stats, self.transfer_bytes)
        {
            let request_index = issued.wrapping_add(filled as u64);
            let command_id = self.next_id.wrapping_add(request_index);
            append_client_workload_request(
                request_buffer,
                command_id,
                request_index,
                self.command_mix,
                self.segments.as_deref(),
                self.client_index,
                self.total_clients,
            )?;
            filled += 1;
        }

        if filled != 0 {
            stream.write_all(request_buffer).await?;
        }

        Ok(filled)
    }

    fn pipeline_refill_low_water(&self) -> usize {
        (self.pipeline_depth / 2).max(1)
    }
}

async fn send_authinfo(
    stream: &mut TcpStream,
    response_reader: &mut crate::client::DrainedResponseReader,
    kind: AuthInfoKind,
    value: &AuthInfoValue<'_>,
) -> io::Result<crate::client::DrainedResponse> {
    let request_kind = match kind {
        AuthInfoKind::User => RequestKind::AuthInfoUser,
        AuthInfoKind::Pass => RequestKind::AuthInfoPass,
    };
    crate::client::write_authinfo_wire(stream, kind, value).await?;
    response_reader
        .read_response(stream, request_kind)
        .await
        .map_err(map_client_error)
}

async fn authenticate_client_stream(
    stream: &mut TcpStream,
    response_reader: &mut crate::client::DrainedResponseReader,
    auth_user: Option<AuthInfoValue<'static>>,
    auth_pass: Option<AuthInfoValue<'static>>,
) -> io::Result<()> {
    let mut user_status = None;
    if let Some(value) = auth_user.as_ref() {
        let response = send_authinfo(stream, response_reader, AuthInfoKind::User, value).await?;
        let status = response.status().as_u16();
        if status != 281 && status != 381 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("AUTHINFO USER rejected with status {status}"),
            ));
        }
        user_status = Some(status);
    }

    if auth_pass.is_none() && user_status == Some(381) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "AUTHINFO USER requires AUTHINFO PASS",
        ));
    }

    if let (Some(381), Some(value)) = (user_status, auth_pass.as_ref()) {
        let response = send_authinfo(stream, response_reader, AuthInfoKind::Pass, value).await?;
        let status = response.status().as_u16();
        if status != 281 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("AUTHINFO PASS rejected with status {status}"),
            ));
        }
    }

    Ok(())
}

fn handle_article_id_response(
    target: &ArticleDownloadTarget,
    status: StatusCode,
    response: &[u8],
    output_dir: &Path,
    verify_dir: Option<&Path>,
    output_path: &mut PathBuf,
    verify_path: &mut PathBuf,
) -> io::Result<()> {
    match status.as_u16() {
        220 => {
            if let Err(err) =
                verify_article_response_file_into(verify_dir, target, response, verify_path)
            {
                if err.kind() == io::ErrorKind::InvalidData {
                    write_failed_article_response_file_into(
                        output_dir,
                        target,
                        response,
                        output_path,
                    )?;
                    eprintln!(
                        "ARTICLE {} verification failed; saved received response to {}",
                        download_target_label(target),
                        output_path.display()
                    );
                }
                return Err(err);
            }
            write_article_response_file_into(output_dir, target, response, output_path)
        }
        430 => Ok(()),
        status => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "ARTICLE {} returned unexpected status {status}",
                download_target_label(target)
            ),
        )),
    }
}

fn transfer_limit_reached(stats: &Stats, transfer_bytes: u64) -> bool {
    transfer_bytes != 0 && stats.bytes_sent.load(Ordering::Relaxed) >= transfer_bytes
}

fn append_client_workload_request(
    buffer: &mut Vec<u8>,
    command_id: u64,
    request_index: u64,
    mix: ClientCommandMix,
    segments: Option<&SegmentSet>,
    client_index: usize,
    total_clients: usize,
) -> io::Result<RequestKind> {
    let kind = client_command_kind(command_id, mix);
    let request_kind = request_kind_for_normalized_client_command(kind);

    if let Some(segments) = segments {
        let message_id =
            segment_ref_for_request(segments, client_index, total_clients, request_index);
        append_client_message_id_request(buffer, kind, message_id);
        return Ok(request_kind);
    }

    if (1..=crate::protocol::MAX_ARTICLE_NUMBER).contains(&command_id) {
        append_client_number_request(buffer, kind, command_id)?;
        return Ok(request_kind);
    }

    let mut synthetic = arrayvec::ArrayString::<64>::new();
    write!(&mut synthetic, "<bench.{command_id}@nntpbench.local>")
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid synthetic message-id"))?;
    let message_id = MessageId::from_borrowed(synthetic.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid synthetic message-id"))?;
    append_client_message_id_request(buffer, kind, &message_id);
    Ok(request_kind)
}

fn append_client_number_request(
    buffer: &mut Vec<u8>,
    kind: ClientCommandMix,
    number: u64,
) -> io::Result<()> {
    let mut number_buf = arrayvec::ArrayString::<20>::new();
    write!(&mut number_buf, "{number}")
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid article number"))?;
    append_client_request_prefix(buffer, kind);
    buffer.extend_from_slice(number_buf.as_bytes());
    buffer.extend_from_slice(crate::CRLF);
    Ok(())
}

fn append_client_message_id_request(
    buffer: &mut Vec<u8>,
    kind: ClientCommandMix,
    message_id: &MessageId<'_>,
) {
    append_client_request_prefix(buffer, kind);
    buffer.extend_from_slice(message_id.as_str().as_bytes());
    buffer.extend_from_slice(crate::CRLF);
}

fn append_client_request_prefix(buffer: &mut Vec<u8>, kind: ClientCommandMix) {
    match kind {
        ClientCommandMix::Article => buffer.extend_from_slice(b"ARTICLE "),
        ClientCommandMix::Body => buffer.extend_from_slice(b"BODY "),
        ClientCommandMix::Alternate => {
            unreachable!("client_command_kind should normalize Alternate")
        }
    }
}

fn request_kind_for_client_command(command_id: u64, mix: ClientCommandMix) -> RequestKind {
    request_kind_for_normalized_client_command(client_command_kind(command_id, mix))
}

fn request_kind_for_normalized_client_command(kind: ClientCommandMix) -> RequestKind {
    match kind {
        ClientCommandMix::Article => RequestKind::Article,
        ClientCommandMix::Body => RequestKind::Body,
        ClientCommandMix::Alternate => {
            unreachable!("client_command_kind should normalize Alternate")
        }
    }
}

fn client_request_for_command(
    command_id: u64,
    request_index: u64,
    mix: ClientCommandMix,
    segments: Option<&SegmentSet>,
    client_index: usize,
    total_clients: usize,
) -> io::Result<Request<'static>> {
    let kind = client_command_kind(command_id, mix);
    if segments.is_none() && (1..=crate::protocol::MAX_ARTICLE_NUMBER).contains(&command_id) {
        return match kind {
            ClientCommandMix::Article => Request::article_number(command_id)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid article number")),
            ClientCommandMix::Body => Request::body_number(command_id)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid body number")),
            ClientCommandMix::Alternate => {
                unreachable!("client_command_kind should normalize Alternate")
            }
        };
    }

    let message_id = request_message_id_for_command(
        kind,
        command_id,
        request_index,
        segments,
        client_index,
        total_clients,
    )?;
    match kind {
        ClientCommandMix::Article => Ok(Request::Article {
            article_ref: ArticleRef::MessageId(message_id),
        }),
        ClientCommandMix::Body => Ok(Request::Body {
            article_ref: ArticleRef::MessageId(message_id),
        }),
        ClientCommandMix::Alternate => {
            unreachable!("client_command_kind should normalize Alternate")
        }
    }
}

#[doc(hidden)]
pub fn bench_client_request_for_command(
    command_id: u64,
    request_index: u64,
    mix: ClientCommandMix,
) -> io::Result<Request<'static>> {
    client_request_for_command(command_id, request_index, mix, None, 0, 1)
}

#[doc(hidden)]
pub fn bench_client_segment_request_for_command(
    segment_message_id: MessageId<'static>,
    mix: ClientCommandMix,
) -> io::Result<Request<'static>> {
    match mix {
        ClientCommandMix::Article => Ok(Request::Article {
            article_ref: ArticleRef::MessageId(segment_message_id),
        }),
        ClientCommandMix::Body => Ok(Request::Body {
            article_ref: ArticleRef::MessageId(segment_message_id),
        }),
        ClientCommandMix::Alternate => {
            unreachable!("client_command_kind should normalize Alternate")
        }
    }
}

fn request_message_id_for_command(
    kind: ClientCommandMix,
    command_id: u64,
    request_index: u64,
    segments: Option<&SegmentSet>,
    client_index: usize,
    total_clients: usize,
) -> io::Result<MessageId<'static>> {
    if let Some(segments) = segments {
        return Ok(segment_for_request(
            segments,
            client_index,
            total_clients,
            request_index,
        ));
    }

    match kind {
        ClientCommandMix::Article | ClientCommandMix::Body => {}
        ClientCommandMix::Alternate => {
            unreachable!("client_command_kind should normalize Alternate")
        }
    };
    let mut message_id = arrayvec::ArrayString::<64>::new();
    write!(&mut message_id, "<bench.{command_id}@nntpbench.local>")
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid synthetic message-id"))?;
    MessageId::from_shared(Arc::<str>::from(message_id.as_str()))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid synthetic message-id"))
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
    let file = fs::File::open(path)?;
    let mut reader = io::BufReader::new(file);
    let mut line_buf = Vec::with_capacity(512);
    let mut ids = Vec::new();
    let mut line_index = 0;

    loop {
        line_buf.clear();
        let read = reader.read_until(b'\n', &mut line_buf)?;
        if read == 0 {
            break;
        }
        line_index += 1;
        let line = trim_ascii_line(&line_buf);
        if line.is_empty() {
            continue;
        }

        let tab = memchr::memchr(b'\t', line).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid segment line {line_index}: expected SIZE<TAB>MSGID"),
            )
        })?;
        let msgid = trim_ascii_line(&line[tab + 1..]);
        let id = shared_message_id_from_bytes(msgid)?;
        ids.push(id);
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

fn shared_message_id_from_bytes(value: &[u8]) -> io::Result<MessageId<'static>> {
    let value = std::str::from_utf8(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "segment message-id is not utf-8",
        )
    })?;
    if value.starts_with('<') && value.ends_with('>') {
        return MessageId::from_shared(Arc::<str>::from(value))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid segment message-id"));
    }

    let mut normalized = arrayvec::ArrayString::<512>::new();
    write!(&mut normalized, "<{value}>")
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "segment message-id too long"))?;
    MessageId::from_shared(Arc::<str>::from(normalized.as_str()))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid segment message-id"))
}

fn trim_ascii_line(mut line: &[u8]) -> &[u8] {
    while matches!(line.last(), Some(b'\n' | b'\r' | b' ' | b'\t')) {
        line = &line[..line.len() - 1];
    }
    while matches!(line.first(), Some(b' ' | b'\t')) {
        line = &line[1..];
    }
    line
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
    let mut file = fs::File::open("/proc/self/stat").ok()?;
    let mut buffer = [0_u8; 1024];
    let read = file.read(&mut buffer).ok()?;
    let stat = std::str::from_utf8(&buffer[..read]).ok()?;
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

#[cfg(target_os = "linux")]
#[cfg_attr(coverage_nightly, coverage(off))]
fn process_rss_kib() -> u64 {
    let Ok(mut file) = fs::File::open("/proc/self/status") else {
        return 0;
    };
    let mut buffer = [0_u8; 4096];
    let Ok(read) = file.read(&mut buffer) else {
        return 0;
    };
    let Ok(status) = std::str::from_utf8(&buffer[..read]) else {
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

#[cfg(target_os = "macos")]
#[cfg_attr(coverage_nightly, coverage(off))]
fn process_rss_kib() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return 0;
    }

    let max_rss_bytes = unsafe { usage.assume_init() }.ru_maxrss;
    u64::try_from(max_rss_bytes).unwrap_or(0) / 1024
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[cfg_attr(coverage_nightly, coverage(off))]
fn process_rss_kib() -> u64 {
    0
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn read_greeting(stream: &mut TcpStream) -> io::Result<()> {
    let mut buffer = [0_u8; protocol::MAX_INITIAL_RESPONSE_LINE_BYTES];
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

        total += read;
        match detect_bounded_response_line_end(
            &buffer[..total],
            protocol::MAX_INITIAL_RESPONSE_LINE_BYTES,
        ) {
            BoundedResponseLineStatus::CompleteAt(_) => return Ok(()),
            BoundedResponseLineStatus::NeedMore => {}
            BoundedResponseLineStatus::Invalid => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "server greeting missing CRLF terminator",
                ));
            }
            BoundedResponseLineStatus::TooLong => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "server greeting exceeded RFC response-line limit",
                ));
            }
        }
    }
}

fn segment_for_request(
    segments: &SegmentSet,
    client_index: usize,
    total_clients: usize,
    request_id: u64,
) -> MessageId<'static> {
    segment_ref_for_request(segments, client_index, total_clients, request_id).clone()
}

fn segment_ref_for_request(
    segments: &SegmentSet,
    client_index: usize,
    total_clients: usize,
    request_id: u64,
) -> &MessageId<'static> {
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
    let mut command_line = [0; MAX_COMMAND_LINE_BYTES];
    let mut command_lines = Some(CommandLineBatch::default());
    let mut command_batch: Box<CommandBatch> = Box::default();
    let mut pending_write = PendingWrite::new(config.pending_write_bytes);
    let mut article_path = PathBuf::with_capacity(1024);
    let mut session_state = SessionState::default();

    send_greeting(&mut writer, &config, session_stats).await?;

    loop {
        if !read_command_batch(
            &mut reader,
            &mut command_line,
            command_lines.as_mut(),
            &mut command_batch,
            max_pipeline_depth,
        )
        .await?
        {
            break;
        }

        if process_command_batch(
            &command_batch,
            command_lines.as_ref(),
            &config,
            session_stats,
            &mut writer,
            &mut pending_write,
            &mut article_path,
            &mut session_state,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixtureGroup {
    AltTest,
    CompLangRust,
}

#[derive(Debug, Default)]
struct SessionState {
    group_selected: bool,
    selected_group: Option<FixtureGroup>,
    current_article: Option<u64>,
}

impl SessionState {
    fn article_count(&self) -> u64 {
        self.selected_group
            .map(FixtureGroup::article_count)
            .unwrap_or(FixtureGroup::AltTest.article_count())
    }
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
#[allow(clippy::too_many_arguments)]
async fn process_command_batch<W>(
    command_batch: &CommandBatch,
    command_lines: Option<&CommandLineBatch>,
    config: &ServerConfig,
    session_stats: &mut SessionStats,
    writer: &mut W,
    pending_write: &mut PendingWrite,
    article_path: &mut PathBuf,
    session_state: &mut SessionState,
) -> io::Result<BatchOutcome>
where
    W: AsyncWrite + Unpin,
{
    session_stats.pipeline_batches += u64::from(command_batch.len() > 1);

    for command in command_batch {
        if handle_command(
            command,
            command_lines,
            config,
            session_stats,
            writer,
            pending_write,
            article_path,
            session_state,
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
#[allow(clippy::too_many_arguments)]
async fn handle_command<W>(
    command: &ParsedCommand,
    command_lines: Option<&CommandLineBatch>,
    config: &ServerConfig,
    session_stats: &mut SessionStats,
    writer: &mut W,
    pending_write: &mut PendingWrite,
    article_path: &mut PathBuf,
    session_state: &mut SessionState,
) -> io::Result<bool>
where
    W: AsyncWrite + Unpin,
{
    session_stats.commands += 1;

    match command.kind {
        RequestKind::Article => {
            session_stats.article_requests += 1;
            if let Some(response) =
                article_selector_error(command, command_lines, session_state, true)
            {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            if config.article_dir.is_some() {
                let message_id = command_lines.and_then(|lines| command_message_id(command, lines));
                if write_stored_article_response(
                    writer,
                    pending_write,
                    config,
                    command.article_id,
                    message_id.as_ref(),
                    session_stats,
                    article_path,
                )
                .await?
                {
                    if let Some(article_id) = command.article_id {
                        session_state.current_article = Some(article_id);
                    }
                    return Ok(false);
                }
                write_response(
                    writer,
                    pending_write,
                    article_not_found_response(command.article_id, message_id.as_ref()),
                    session_stats,
                )
                .await?;
                return Ok(false);
            }
            if let Some(message_id) =
                command_lines.and_then(|lines| command_message_id(command, lines))
            {
                let response = build_message_id_article_response(&message_id, config.article_bytes);
                write_response(writer, pending_write, &response, session_stats).await?;
                return Ok(false);
            }
            let article_id = command
                .article_id
                .or(session_state.current_article)
                .unwrap_or(1);
            session_state.current_article = Some(article_id);
            if article_id == 1 {
                write_response(
                    writer,
                    pending_write,
                    config.article_response(),
                    session_stats,
                )
                .await?;
            } else {
                let response = build_selected_article_response(article_id, config.article_bytes);
                write_response(writer, pending_write, &response, session_stats).await?;
            }
            Ok(false)
        }
        RequestKind::Head => {
            session_stats.article_requests += 1;
            if let Some(response) =
                article_selector_error(command, command_lines, session_state, true)
            {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            if let Some(message_id) =
                command_lines.and_then(|lines| command_message_id(command, lines))
            {
                let response = build_message_id_head_response(&message_id);
                write_response(writer, pending_write, &response, session_stats).await?;
                return Ok(false);
            }
            let article_id = command
                .article_id
                .or(session_state.current_article)
                .unwrap_or(1);
            session_state.current_article = Some(article_id);
            if article_id == 1 {
                write_response(writer, pending_write, config.head_response(), session_stats)
                    .await?;
            } else {
                let response = build_selected_head_response(article_id);
                write_response(writer, pending_write, &response, session_stats).await?;
            }
            Ok(false)
        }
        RequestKind::Stat => {
            session_stats.article_requests += 1;
            if let Some(response) =
                article_selector_error(command, command_lines, session_state, true)
            {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            if let Some(message_id) =
                command_lines.and_then(|lines| command_message_id(command, lines))
            {
                let response = build_message_id_stat_response(&message_id);
                write_response(writer, pending_write, &response, session_stats).await?;
                return Ok(false);
            }
            let article_id = command
                .article_id
                .or(session_state.current_article)
                .unwrap_or(1);
            session_state.current_article = Some(article_id);
            if article_id == 1 {
                write_response(writer, pending_write, config.stat_response(), session_stats)
                    .await?;
            } else {
                let response = build_selected_stat_response(article_id);
                write_response(writer, pending_write, &response, session_stats).await?;
            }
            Ok(false)
        }
        RequestKind::Body => {
            session_stats.body_requests += 1;
            if let Some(response) =
                article_selector_error(command, command_lines, session_state, true)
            {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            if let Some(message_id) =
                command_lines.and_then(|lines| command_message_id(command, lines))
            {
                let response = build_message_id_body_response(&message_id, config.body_bytes);
                write_response(writer, pending_write, &response, session_stats).await?;
                return Ok(false);
            }
            let article_id = command
                .article_id
                .or(session_state.current_article)
                .unwrap_or(1);
            session_state.current_article = Some(article_id);
            if article_id == 1 {
                write_response(writer, pending_write, config.body_response(), session_stats)
                    .await?;
            } else {
                let response = build_selected_body_response(article_id, config.body_bytes);
                write_response(writer, pending_write, &response, session_stats).await?;
            }
            Ok(false)
        }
        RequestKind::List => {
            write_response(writer, pending_write, LIST_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::ListActive => {
            let response = list_active_response(list_variant_wildmat_args(
                RequestKind::ListActive,
                command_args(command, command_lines).unwrap_or_default(),
            ));
            write_response(writer, pending_write, response, session_stats).await?;
            Ok(false)
        }
        RequestKind::ListActiveTimes => {
            let response = list_active_times_response(list_variant_wildmat_args(
                RequestKind::ListActiveTimes,
                command_args(command, command_lines).unwrap_or_default(),
            ));
            write_response(writer, pending_write, response, session_stats).await?;
            Ok(false)
        }
        RequestKind::ListNewsgroups => {
            let response = list_newsgroups_response(list_variant_wildmat_args(
                RequestKind::ListNewsgroups,
                command_args(command, command_lines).unwrap_or_default(),
            ));
            write_response(writer, pending_write, response, session_stats).await?;
            Ok(false)
        }
        RequestKind::ListOverviewFmt => {
            write_response(
                writer,
                pending_write,
                LIST_OVERVIEW_FMT_RESPONSE,
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::ListHeaders => {
            write_response(writer, pending_write, LIST_HEADERS_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::ListDistribPats => {
            write_response(
                writer,
                pending_write,
                LIST_DISTRIB_PATS_RESPONSE,
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Group => {
            let args = command_args(command, command_lines).unwrap_or_default();
            let Some(group) = fixture_group_from_name(args) else {
                write_response(
                    writer,
                    pending_write,
                    b"411 no such newsgroup\r\n",
                    session_stats,
                )
                .await?;
                return Ok(false);
            };
            session_state.group_selected = true;
            session_state.selected_group = Some(group);
            session_state.current_article = Some(1);
            write_response(
                writer,
                pending_write,
                group_response_for_group(group),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::ListGroup => {
            let args = command_args(command, command_lines).unwrap_or_default();
            if let Some(response) = listgroup_state_error(args, session_state) {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            let group = listgroup_selected_group(args, session_state.selected_group)
                .unwrap_or(FixtureGroup::AltTest);
            session_state.group_selected = true;
            session_state.selected_group = Some(group);
            session_state.current_article = Some(1);
            write_response(
                writer,
                pending_write,
                listgroup_response_for_args(args, Some(group)),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Last => {
            if !session_state.group_selected {
                write_response(
                    writer,
                    pending_write,
                    b"412 no newsgroup selected\r\n",
                    session_stats,
                )
                .await?;
                return Ok(false);
            }
            let Some(current_article) = session_state.current_article else {
                write_response(
                    writer,
                    pending_write,
                    b"420 no current article selected\r\n",
                    session_stats,
                )
                .await?;
                return Ok(false);
            };
            if current_article <= 1 {
                write_response(
                    writer,
                    pending_write,
                    b"422 no previous article in this group\r\n",
                    session_stats,
                )
                .await?;
                return Ok(false);
            }
            let previous_article = current_article - 1;
            session_state.current_article = Some(previous_article);
            write_response(
                writer,
                pending_write,
                last_response_for_article(previous_article),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Next => {
            if !session_state.group_selected {
                write_response(
                    writer,
                    pending_write,
                    b"412 no newsgroup selected\r\n",
                    session_stats,
                )
                .await?;
                return Ok(false);
            }
            let Some(current_article) = session_state.current_article else {
                write_response(
                    writer,
                    pending_write,
                    b"420 no current article selected\r\n",
                    session_stats,
                )
                .await?;
                return Ok(false);
            };
            if current_article >= session_state.article_count() {
                write_response(
                    writer,
                    pending_write,
                    b"421 no next article in this group\r\n",
                    session_stats,
                )
                .await?;
                return Ok(false);
            }
            let next_article = current_article + 1;
            session_state.current_article = Some(next_article);
            write_response(
                writer,
                pending_write,
                next_response_for_article(next_article),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::NewGroups => {
            let response = if command_args(command, command_lines)
                .and_then(newgroups_date_key)
                .is_some_and(|date| date >= FAR_FUTURE_DATE_KEY)
            {
                NEWGROUPS_EMPTY_RESPONSE
            } else {
                NEWGROUPS_RESPONSE
            };
            write_response(writer, pending_write, response, session_stats).await?;
            Ok(false)
        }
        RequestKind::NewNews => {
            let response =
                newnews_response(command_args(command, command_lines).unwrap_or_default());
            write_response(writer, pending_write, response, session_stats).await?;
            Ok(false)
        }
        RequestKind::Post => {
            write_response(
                writer,
                pending_write,
                b"440 posting not permitted\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Ihave => {
            write_response(
                writer,
                pending_write,
                b"435 article not wanted\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Check => {
            write_response(
                writer,
                pending_write,
                b"502 command unavailable\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::TakeThis => {
            write_response(
                writer,
                pending_write,
                b"502 command unavailable\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::AuthInfoUser => {
            write_response(
                writer,
                pending_write,
                b"483 command unavailable until TLS has been negotiated\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::AuthInfoPass => {
            write_response(
                writer,
                pending_write,
                b"483 command unavailable until TLS has been negotiated\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::AuthInfo => {
            write_response(
                writer,
                pending_write,
                b"502 command unavailable\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::StartTls => {
            write_response(
                writer,
                pending_write,
                b"502 command unavailable\r\n",
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Over => {
            if let Some(response) = overview_selector_error(command, command_lines, session_state) {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            write_response(
                writer,
                pending_write,
                over_response(command, command_lines, session_state),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Xover => {
            if let Some(response) = overview_selector_error(command, command_lines, session_state) {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            write_response(
                writer,
                pending_write,
                xover_response(command, command_lines, session_state),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Hdr => {
            if let Some(response) = overview_selector_error(command, command_lines, session_state) {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            write_response(
                writer,
                pending_write,
                hdr_response(command, command_lines, session_state),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Xhdr => {
            if let Some(response) = overview_selector_error(command, command_lines, session_state) {
                write_response(writer, pending_write, response, session_stats).await?;
                return Ok(false);
            }
            write_response(
                writer,
                pending_write,
                xhdr_response(command, command_lines, session_state),
                session_stats,
            )
            .await?;
            Ok(false)
        }
        RequestKind::Capabilities => {
            write_response(writer, pending_write, CAPABILITIES_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Help => {
            write_response(writer, pending_write, HELP_RESPONSE, session_stats).await?;
            Ok(false)
        }
        RequestKind::Date => {
            let response = current_date_response();
            write_response(writer, pending_write, &response, session_stats).await?;
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
        RequestKind::Unknown if command.syntax_error => {
            let response = if command.line_too_long {
                b"501 command line too long\r\n".as_slice()
            } else {
                b"501 command syntax error\r\n".as_slice()
            };
            write_response(writer, pending_write, response, session_stats).await?;
            Ok(false)
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
    buf: Box<[u8]>,
    len: usize,
}

struct GeneratedResponse<'a> {
    prefix: &'a [u8],
    target_bytes: usize,
}

impl<'a> GeneratedResponse<'a> {
    const fn new(prefix: &'a [u8], target_bytes: usize) -> Self {
        Self {
            prefix,
            target_bytes,
        }
    }
}

fn build_generated_response(prefix: &[u8], target_bytes: usize) -> Box<[u8]> {
    let response = GeneratedResponse::new(prefix, target_bytes);
    let mut buffer = Vec::with_capacity(response.total_len());
    buffer.extend_from_slice(response.prefix);

    if response.prefix.len() < response.target_bytes {
        append_repeated_payload_at_least(
            &mut buffer,
            BODY_LINE,
            response.target_bytes - response.prefix.len(),
        );
    }

    append_dot_terminator(&mut buffer);
    buffer.into_boxed_slice()
}

fn append_synthetic_article_headers(buffer: &mut Vec<u8>, message_id: &str) {
    write!(
        buffer,
        "Path: nntpbench.local!mock\r\nFrom: Bench User <bench@nntpbench.local>\r\nNewsgroups: alt.binaries.bench\r\nSubject: nntpbench synthetic article\r\nMessage-ID: {message_id}\r\nDate: Fri, 15 May 2026 00:00:00 +0000\r\n"
    )
    .expect("write to Vec cannot fail");
}

fn build_selected_article_response(article_id: u64, target_bytes: usize) -> Box<[u8]> {
    let message_id = format!("<article.{article_id}@nntpbench.local>");
    build_article_response(article_id, &message_id, target_bytes)
}

fn build_message_id_article_response(message_id: &MessageId<'_>, target_bytes: usize) -> Box<[u8]> {
    build_article_response(0, message_id.as_str(), target_bytes)
}

fn build_article_response(article_id: u64, message_id: &str, target_bytes: usize) -> Box<[u8]> {
    let mut buffer = Vec::with_capacity(target_bytes.max(ARTICLE_RESPONSE_PREFIX.len()));
    write!(buffer, "220 {article_id} {message_id} article follows\r\n")
        .expect("write to Vec cannot fail");
    append_synthetic_article_headers(&mut buffer, message_id);
    buffer.extend_from_slice(CRLF);

    if buffer.len() < target_bytes {
        let missing = target_bytes - buffer.len();
        append_repeated_payload_at_least(&mut buffer, BODY_LINE, missing);
    }

    append_dot_terminator(&mut buffer);
    buffer.into_boxed_slice()
}

fn build_selected_body_response(article_id: u64, target_bytes: usize) -> Box<[u8]> {
    let message_id = format!("<article.{article_id}@nntpbench.local>");
    build_body_response(article_id, &message_id, target_bytes)
}

fn build_message_id_body_response(message_id: &MessageId<'_>, target_bytes: usize) -> Box<[u8]> {
    build_body_response(0, message_id.as_str(), target_bytes)
}

fn build_body_response(article_id: u64, message_id: &str, target_bytes: usize) -> Box<[u8]> {
    let mut buffer = Vec::with_capacity(target_bytes.max(BODY_RESPONSE_PREFIX.len()));
    write!(buffer, "222 {article_id} {message_id} body follows\r\n")
        .expect("write to Vec cannot fail");

    if buffer.len() < target_bytes {
        let missing = target_bytes - buffer.len();
        append_repeated_payload_at_least(&mut buffer, BODY_LINE, missing);
    }

    append_dot_terminator(&mut buffer);
    buffer.into_boxed_slice()
}

fn build_selected_head_response(article_id: u64) -> Box<[u8]> {
    let message_id = format!("<article.{article_id}@nntpbench.local>");
    build_head_response(article_id, &message_id)
}

fn build_message_id_head_response(message_id: &MessageId<'_>) -> Box<[u8]> {
    build_head_response(0, message_id.as_str())
}

fn build_head_response(article_id: u64, message_id: &str) -> Box<[u8]> {
    let mut buffer = Vec::new();
    write!(
        buffer,
        "221 {article_id} {message_id} article retrieved\r\n"
    )
    .expect("write to Vec cannot fail");
    append_synthetic_article_headers(&mut buffer, message_id);
    append_dot_terminator(&mut buffer);
    buffer.into_boxed_slice()
}

fn build_selected_stat_response(article_id: u64) -> Box<[u8]> {
    let message_id = format!("<article.{article_id}@nntpbench.local>");
    build_stat_response(article_id, &message_id)
}

fn build_message_id_stat_response(message_id: &MessageId<'_>) -> Box<[u8]> {
    build_stat_response(0, message_id.as_str())
}

fn build_stat_response(article_id: u64, message_id: &str) -> Box<[u8]> {
    format!("223 {article_id} {message_id} article retrieved\r\n")
        .into_bytes()
        .into_boxed_slice()
}

fn append_repeated_payload_at_least(buffer: &mut Vec<u8>, line: &[u8], min_bytes: usize) {
    if line.is_empty() {
        return;
    }

    let copies = min_bytes.div_ceil(line.len());
    buffer.reserve(copies * line.len());
    for _ in 0..copies {
        buffer.extend_from_slice(line);
    }
}

impl GeneratedResponse<'_> {
    fn total_len(&self) -> usize {
        let payload_bytes = if self.prefix.len() < self.target_bytes {
            let missing = self.target_bytes - self.prefix.len();
            missing.div_ceil(BODY_LINE.len()) * BODY_LINE.len()
        } else {
            0
        };
        self.prefix.len() + payload_bytes + DOT_TERMINATOR.len()
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl PendingWrite {
    fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0; capacity.max(1)].into_boxed_slice(),
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

pub fn process_request_to_buffer<W>(
    request: RequestLine<'_>,
    config: &ServerConfig,
    stats: &Stats,
    output: &mut W,
) -> bool
where
    W: Write,
{
    stats.commands.fetch_add(1, Ordering::Relaxed);

    if matches!(request.kind(), RequestKind::Date) {
        let response = current_date_response();
        stats
            .bytes_sent
            .fetch_add(response.len() as u64, Ordering::Relaxed);
        output.write_all(&response).expect("response write failed");
        return false;
    }

    let response = match request.kind() {
        RequestKind::Article => {
            stats.article_requests.fetch_add(1, Ordering::Relaxed);
            if config.article_dir.is_some() {
                if write_stored_article_response_to(
                    output,
                    config,
                    parse_article_id_arg(request.args()),
                    request.message_id().as_ref(),
                    stats,
                )
                .expect("response write failed")
                {
                    return false;
                }
                stats.bytes_sent.fetch_add(
                    article_not_found_response(
                        parse_article_id_arg(request.args()),
                        request.message_id().as_ref(),
                    )
                    .len() as u64,
                    Ordering::Relaxed,
                );
                output
                    .write_all(article_not_found_response(
                        parse_article_id_arg(request.args()),
                        request.message_id().as_ref(),
                    ))
                    .expect("response write failed");
                return false;
            }
            if let Some(message_id) = request.message_id() {
                let response = build_message_id_article_response(&message_id, config.article_bytes);
                stats
                    .bytes_sent
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                output.write_all(&response).expect("response write failed");
                return false;
            }
            if let Some(article_id) = parse_article_id_arg(request.args()).filter(|id| *id != 1) {
                let response = build_selected_article_response(article_id, config.article_bytes);
                stats
                    .bytes_sent
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                output.write_all(&response).expect("response write failed");
                return false;
            }
            stats
                .bytes_sent
                .fetch_add(config.article_response().len() as u64, Ordering::Relaxed);
            output
                .write_all(config.article_response())
                .expect("response write failed");
            return false;
        }
        RequestKind::Head => {
            stats.article_requests.fetch_add(1, Ordering::Relaxed);
            if let Some(message_id) = request.message_id() {
                let response = build_message_id_head_response(&message_id);
                stats
                    .bytes_sent
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                output.write_all(&response).expect("response write failed");
                return false;
            }
            if let Some(article_id) = parse_article_id_arg(request.args()).filter(|id| *id != 1) {
                let response = build_selected_head_response(article_id);
                stats
                    .bytes_sent
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                output.write_all(&response).expect("response write failed");
                return false;
            }
            config.head_response()
        }
        RequestKind::Stat => {
            stats.article_requests.fetch_add(1, Ordering::Relaxed);
            if let Some(message_id) = request.message_id() {
                let response = build_message_id_stat_response(&message_id);
                stats
                    .bytes_sent
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                output.write_all(&response).expect("response write failed");
                return false;
            }
            if let Some(article_id) = parse_article_id_arg(request.args()).filter(|id| *id != 1) {
                let response = build_selected_stat_response(article_id);
                stats
                    .bytes_sent
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                output.write_all(&response).expect("response write failed");
                return false;
            }
            config.stat_response()
        }
        RequestKind::Body => {
            stats.body_requests.fetch_add(1, Ordering::Relaxed);
            if let Some(message_id) = request.message_id() {
                let response = build_message_id_body_response(&message_id, config.body_bytes);
                stats
                    .bytes_sent
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                output.write_all(&response).expect("response write failed");
                return false;
            }
            if let Some(article_id) = parse_article_id_arg(request.args()).filter(|id| *id != 1) {
                let response = build_selected_body_response(article_id, config.body_bytes);
                stats
                    .bytes_sent
                    .fetch_add(response.len() as u64, Ordering::Relaxed);
                output.write_all(&response).expect("response write failed");
                return false;
            }
            stats
                .bytes_sent
                .fetch_add(config.body_response().len() as u64, Ordering::Relaxed);
            output
                .write_all(config.body_response())
                .expect("response write failed");
            return false;
        }
        RequestKind::List => LIST_RESPONSE,
        RequestKind::ListActive => {
            list_active_response(list_variant_wildmat_args(request.kind(), request.args()))
        }
        RequestKind::ListActiveTimes => {
            list_active_times_response(list_variant_wildmat_args(request.kind(), request.args()))
        }
        RequestKind::ListNewsgroups => {
            list_newsgroups_response(list_variant_wildmat_args(request.kind(), request.args()))
        }
        RequestKind::ListOverviewFmt => LIST_OVERVIEW_FMT_RESPONSE,
        RequestKind::ListHeaders => LIST_HEADERS_RESPONSE,
        RequestKind::ListDistribPats => LIST_DISTRIB_PATS_RESPONSE,
        RequestKind::Group => group_response_for_args(request.args()),
        RequestKind::ListGroup => listgroup_response_for_args(request.args(), None),
        RequestKind::Last => LAST_RESPONSE,
        RequestKind::Next => NEXT_RESPONSE,
        RequestKind::NewGroups => NEWGROUPS_RESPONSE,
        RequestKind::NewNews => newnews_response(request.args()),
        RequestKind::Post => b"440 posting not permitted\r\n",
        RequestKind::Ihave => b"435 article not wanted\r\n",
        RequestKind::Check | RequestKind::TakeThis | RequestKind::StartTls => {
            b"502 command unavailable\r\n"
        }
        RequestKind::AuthInfoUser | RequestKind::AuthInfoPass => {
            b"483 command unavailable until TLS has been negotiated\r\n"
        }
        RequestKind::AuthInfo => b"502 command unavailable\r\n",
        RequestKind::Over => over_response_for_args(request.args()),
        RequestKind::Xover => xover_response_for_args(request.args()),
        RequestKind::Hdr => hdr_response_for_args(request.args()),
        RequestKind::Xhdr => xhdr_response_for_args(request.args()),
        RequestKind::Capabilities => CAPABILITIES_RESPONSE,
        RequestKind::Help => HELP_RESPONSE,
        RequestKind::ModeReader => MODE_READER_RESPONSE,
        RequestKind::Quit => QUIT_RESPONSE,
        RequestKind::Unknown if is_known_request_syntax_error(request) => {
            b"501 command syntax error\r\n"
        }
        _ => b"500 unknown command\r\n",
    };

    stats
        .bytes_sent
        .fetch_add(response.len() as u64, Ordering::Relaxed);
    output.write_all(response).expect("response write failed");
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
        let end = match detect_response_line_end_from(input, start) {
            ResponseLineStatus::CompleteAt(end) => end,
            ResponseLineStatus::NeedMore | ResponseLineStatus::Invalid => break,
        };
        let request = RequestLine::parse(&input[start..end]);
        if matches!(request.kind(), RequestKind::TakeThis) {
            let Some(body_end) = find_dot_terminated_block_end(input, end) else {
                break;
            };
            visit(request);
            count += 1;
            start = body_end;
            continue;
        }
        visit(request);
        count += 1;
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

fn open_stored_article_response(
    config: &ServerConfig,
    article_id: Option<u64>,
    message_id: Option<&MessageId<'_>>,
    article_path: &mut PathBuf,
) -> io::Result<Option<fs::File>> {
    let Some(root) = config.article_dir.as_ref() else {
        return Ok(None);
    };
    if let Some(article_id) = article_id
        && let Some(file) =
            open_article_response_into(root, ArticleStoreKey::Number(article_id), article_path)?
    {
        return Ok(Some(file));
    }
    if let Some(message_id) = message_id {
        return open_article_response_into(
            root,
            ArticleStoreKey::MessageId(message_id),
            article_path,
        );
    }
    Ok(None)
}

fn article_not_found_response(
    article_id: Option<u64>,
    message_id: Option<&MessageId<'_>>,
) -> &'static [u8] {
    if message_id.is_some() {
        b"430 no article with that message-id\r\n"
    } else if article_id.is_some() {
        b"423 no article with that number\r\n"
    } else {
        b"420 no current article selected\r\n"
    }
}

async fn write_stored_article_response<W>(
    writer: &mut W,
    pending_write: &mut PendingWrite,
    config: &ServerConfig,
    article_id: Option<u64>,
    message_id: Option<&MessageId<'_>>,
    session_stats: &mut SessionStats,
    article_path: &mut PathBuf,
) -> io::Result<bool>
where
    W: AsyncWrite + Unpin,
{
    let Some(file) = open_stored_article_response(config, article_id, message_id, article_path)?
    else {
        return Ok(false);
    };

    pending_write.flush(writer).await?;
    write_file_response_to_async(writer, file, session_stats).await?;
    Ok(true)
}

async fn write_file_response_to_async<W>(
    writer: &mut W,
    mut file: fs::File,
    session_stats: &mut SessionStats,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        writer.write_all(&buffer[..read]).await?;
        session_stats.bytes_sent += read as u64;
    }
}

fn write_stored_article_response_to<W>(
    output: &mut W,
    config: &ServerConfig,
    article_id: Option<u64>,
    message_id: Option<&MessageId<'_>>,
    stats: &Stats,
) -> io::Result<bool>
where
    W: Write,
{
    let mut article_path = PathBuf::with_capacity(1024);
    let Some(file) =
        open_stored_article_response(config, article_id, message_id, &mut article_path)?
    else {
        return Ok(false);
    };
    write_file_response_to(output, file, stats)?;
    Ok(true)
}

fn write_file_response_to<W>(output: &mut W, mut file: fs::File, stats: &Stats) -> io::Result<()>
where
    W: Write,
{
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        output.write_all(&buffer[..read])?;
        stats.bytes_sent.fetch_add(read as u64, Ordering::Relaxed);
    }
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
    body_response: Box<[u8]>,
    article_response: Box<[u8]>,
    pub article_dir: Option<Arc<PathBuf>>,
    pub max_connections: usize,
    pub max_pipeline_depth: usize,
    pub stats_interval: Duration,
    pub flush: bool,
    pub pending_write_bytes: usize,
    pub nodelay: bool,
    pub socket_recv_buffer: usize,
    pub socket_send_buffer: usize,
}

impl ServerConfig {
    pub fn from_args(args: ServerArgs) -> Self {
        let body_response = build_generated_response(BODY_RESPONSE_PREFIX, args.body_bytes);
        let article_response =
            build_generated_response(ARTICLE_RESPONSE_PREFIX, args.article_bytes);
        Self {
            body_bytes: args.body_bytes,
            article_bytes: args.article_bytes,
            body_response,
            article_response,
            article_dir: args.article_dir.map(Arc::new),
            max_connections: args.max_connections,
            max_pipeline_depth: args.max_pipeline_depth.clamp(1, 1024),
            stats_interval: Duration::from_secs(args.stats_interval_secs),
            flush: args.flush,
            pending_write_bytes: args.pending_write_bytes.max(1),
            nodelay: args.nodelay,
            socket_recv_buffer: args.socket_recv_buffer,
            socket_send_buffer: args.socket_send_buffer,
        }
    }

    pub fn head_response(&self) -> &[u8] {
        HEAD_RESPONSE
    }

    pub fn stat_response(&self) -> &[u8] {
        STAT_RESPONSE
    }

    pub fn body_response(&self) -> &[u8] {
        &self.body_response
    }

    pub fn article_response(&self) -> &[u8] {
        &self.article_response
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
    article_id: Option<u64>,
    line_slot: u16,
    message_id: Option<ParsedMessageId>,
    syntax_error: bool,
    has_transfer_body: bool,
    line_too_long: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedMessageId {
    start: u16,
    len: u16,
}

type CommandBatch = ArrayVec<ParsedCommand, MAX_SERVER_PIPELINE_DEPTH>;

struct CommandLineBatch {
    lines: Box<[[u8; MAX_COMMAND_LINE_BYTES]]>,
}

impl Default for CommandLineBatch {
    fn default() -> Self {
        let mut lines = Vec::with_capacity(MAX_SERVER_PIPELINE_DEPTH);
        lines.resize_with(MAX_SERVER_PIPELINE_DEPTH, || [0; MAX_COMMAND_LINE_BYTES]);
        Self {
            lines: lines.into_boxed_slice(),
        }
    }
}

impl CommandLineBatch {
    fn copy_line(&mut self, slot: usize, line: &[u8]) {
        self.lines[slot][..line.len()].copy_from_slice(line);
    }

    fn line_slice(&self, slot: usize, start: usize, len: usize) -> &[u8] {
        &self.lines[slot][start..start + len]
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn read_command_batch<R>(
    reader: &mut BufReader<R>,
    command_line: &mut [u8; MAX_COMMAND_LINE_BYTES],
    mut command_lines: Option<&mut CommandLineBatch>,
    command_batch: &mut CommandBatch,
    max_pipeline_depth: usize,
) -> io::Result<bool>
where
    R: tokio::io::AsyncRead + Unpin,
{
    command_batch.clear();
    let Some(line_len) = read_crlf_line_into(reader, command_line).await? else {
        return Ok(false);
    };
    push_command(
        reader,
        &command_line[..line_len],
        command_lines.as_deref_mut(),
        command_batch,
    )
    .await?;

    while command_batch.len() < max_pipeline_depth {
        if find_crlf_line_end(reader.buffer(), 0).is_none() {
            break;
        }

        let Some(line_len) = read_crlf_line_into(reader, command_line).await? else {
            break;
        };
        push_command(
            reader,
            &command_line[..line_len],
            command_lines.as_deref_mut(),
            command_batch,
        )
        .await?;
    }

    Ok(true)
}

async fn read_crlf_line_into<R>(reader: &mut R, line: &mut [u8]) -> io::Result<Option<usize>>
where
    R: AsyncBufRead + Unpin,
{
    let mut len = 0;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if len == 0 {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated line",
            ));
        }

        let take = memchr::memchr(b'\n', available).map_or(available.len(), |index| index + 1);
        if len + take > line.len() {
            reader.consume(take);
            line[..14].copy_from_slice(b"DATE too-long\n");
            return Ok(Some(14));
        }

        line[len..len + take].copy_from_slice(&available[..take]);
        reader.consume(take);
        len += take;

        if line[..len].ends_with(b"\n") {
            return Ok(Some(len));
        }
    }
}

async fn push_command<R>(
    reader: &mut BufReader<R>,
    line: &[u8],
    command_lines: Option<&mut CommandLineBatch>,
    command_batch: &mut CommandBatch,
) -> io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let line_slot = command_batch.len();
    let mut command = parse_command_line(line, line_slot);
    if let Some(command_lines) = command_lines {
        command_lines.copy_line(line_slot, line);
    }
    if should_read_transfer_body(reader, line, &command).await? {
        read_dot_terminated_body(reader).await?;
        command.has_transfer_body = true;
    }
    command_batch.push(command);
    Ok(())
}

async fn should_read_transfer_body<R>(
    reader: &mut BufReader<R>,
    line: &[u8],
    command: &ParsedCommand,
) -> io::Result<bool>
where
    R: tokio::io::AsyncRead + Unpin,
{
    if matches!(command.kind, RequestKind::TakeThis) || is_known_transfer_command(line, b"TAKETHIS")
    {
        return Ok(true);
    }
    if !(matches!(command.kind, RequestKind::Post | RequestKind::Ihave)
        || is_known_transfer_command(line, b"POST")
        || is_known_transfer_command(line, b"IHAVE"))
    {
        return Ok(false);
    }
    let available = reader.fill_buf().await?;
    let next_line = available
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    Ok(next_line.contains(&b':'))
}

fn is_known_transfer_command(line: &[u8], verb: &[u8]) -> bool {
    let line = strip_complete_crlf_line(line).unwrap_or(line);
    let verb_end = line
        .iter()
        .position(|byte| byte.is_ascii_whitespace())
        .unwrap_or(line.len());
    line[..verb_end].eq_ignore_ascii_case(verb)
}

fn parse_command_line(line: &[u8], line_slot: usize) -> ParsedCommand {
    let request = RequestLine::parse(line);
    ParsedCommand {
        kind: request.kind(),
        article_id: parse_article_id_arg(request.args()),
        line_slot: line_slot.try_into().unwrap_or(u16::MAX),
        message_id: parsed_message_id_range(line, request.message_id()),
        syntax_error: matches!(request.kind(), RequestKind::Unknown) && is_known_command_line(line),
        has_transfer_body: false,
        line_too_long: line == b"DATE too-long\n",
    }
}

fn is_known_command_line(line: &[u8]) -> bool {
    let line = strip_complete_crlf_line(line).unwrap_or(line);
    let verb_end = line
        .iter()
        .position(|byte| byte.is_ascii_whitespace())
        .unwrap_or(line.len());
    let verb = &line[..verb_end];
    [
        b"ARTICLE".as_slice(),
        b"AUTHINFO",
        b"BODY",
        b"CAPABILITIES",
        b"CHECK",
        b"DATE",
        b"GROUP",
        b"HDR",
        b"HEAD",
        b"HELP",
        b"IHAVE",
        b"LAST",
        b"LIST",
        b"LISTGROUP",
        b"MODE",
        b"NEWGROUPS",
        b"NEWNEWS",
        b"NEXT",
        b"OVER",
        b"POST",
        b"QUIT",
        b"STARTTLS",
        b"STAT",
        b"TAKETHIS",
        b"XHDR",
        b"XOVER",
    ]
    .iter()
    .any(|known| verb.eq_ignore_ascii_case(known))
}

fn is_known_request_syntax_error(request: RequestLine<'_>) -> bool {
    matches!(request.kind(), RequestKind::Unknown) && is_known_command_line(request.verb())
}

fn parsed_message_id_range(
    line: &[u8],
    message_id: Option<MessageId<'_>>,
) -> Option<ParsedMessageId> {
    let message_id = message_id?;
    let message_id_bytes = message_id.as_str().as_bytes();
    let line_start = line.as_ptr() as usize;
    let message_start = message_id_bytes.as_ptr() as usize;
    let start = message_start.checked_sub(line_start)?;
    if start + message_id_bytes.len() > line.len() {
        return None;
    }
    Some(ParsedMessageId {
        start: start.try_into().ok()?,
        len: message_id_bytes.len().try_into().ok()?,
    })
}

fn command_message_id<'a>(
    command: &ParsedCommand,
    command_lines: &'a CommandLineBatch,
) -> Option<MessageId<'a>> {
    let message_id = command.message_id?;
    let line_slot = usize::from(command.line_slot);
    let bytes = command_lines.line_slice(
        line_slot,
        usize::from(message_id.start),
        usize::from(message_id.len),
    );
    let value = std::str::from_utf8(bytes).ok()?;
    MessageId::from_borrowed(value).ok()
}

fn command_args<'a>(
    command: &ParsedCommand,
    command_lines: Option<&'a CommandLineBatch>,
) -> Option<&'a [u8]> {
    let command_lines = command_lines?;
    let line_slot = usize::from(command.line_slot);
    let line = command_lines.line_slice(line_slot, 0, MAX_COMMAND_LINE_BYTES);
    let end = line
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(line.len());
    let line = strip_complete_crlf_line(&line[..end]).unwrap_or(&line[..end]);
    let split = line.iter().position(|byte| *byte == b' ')?;
    line.get(split + 1..)
}

fn list_active_response(args: &[u8]) -> &'static [u8] {
    let matches_comp = args.is_empty() || wildmat_matches(args, b"comp.lang.rust");
    let matches_alt = args.is_empty() || wildmat_matches(args, b"alt.test");
    match (matches_comp, matches_alt) {
        (true, true) => LIST_RESPONSE,
        (true, false) => LIST_ACTIVE_COMP_RESPONSE,
        (false, true) => LIST_ACTIVE_ALT_RESPONSE,
        (false, false) => LIST_EMPTY_RESPONSE,
    }
}

fn list_variant_wildmat_args(kind: RequestKind, args: &[u8]) -> &[u8] {
    match kind {
        RequestKind::ListActive => strip_list_variant_args(args, b"ACTIVE"),
        RequestKind::ListActiveTimes => strip_list_variant_args(args, b"ACTIVE.TIMES"),
        RequestKind::ListNewsgroups => strip_list_variant_args(args, b"NEWSGROUPS"),
        _ => args,
    }
}

fn strip_list_variant_args<'a>(args: &'a [u8], keyword: &[u8]) -> &'a [u8] {
    if args.eq_ignore_ascii_case(keyword) {
        return b"";
    }
    if args.len() > keyword.len()
        && args[..keyword.len()].eq_ignore_ascii_case(keyword)
        && args[keyword.len()] == b' '
    {
        &args[keyword.len() + 1..]
    } else {
        args
    }
}

fn list_active_times_response(args: &[u8]) -> &'static [u8] {
    list_info_response_for_groups(
        args,
        LIST_ACTIVE_TIMES_RESPONSE,
        LIST_ACTIVE_TIMES_COMP_RESPONSE,
        LIST_ACTIVE_TIMES_ALT_RESPONSE,
    )
}

fn list_newsgroups_response(args: &[u8]) -> &'static [u8] {
    list_info_response_for_groups(
        args,
        LIST_NEWSGROUPS_RESPONSE,
        LIST_NEWSGROUPS_COMP_RESPONSE,
        LIST_NEWSGROUPS_ALT_RESPONSE,
    )
}

fn list_info_response_for_groups(
    args: &[u8],
    full: &'static [u8],
    comp: &'static [u8],
    alt: &'static [u8],
) -> &'static [u8] {
    let matches_comp = args.is_empty() || wildmat_matches(args, b"comp.lang.rust");
    let matches_alt = args.is_empty() || wildmat_matches(args, b"alt.test");
    match (matches_comp, matches_alt) {
        (true, true) => full,
        (true, false) => comp,
        (false, true) => alt,
        (false, false) => LIST_INFO_EMPTY_RESPONSE,
    }
}

fn wildmat_matches(patterns: &[u8], group: &[u8]) -> bool {
    let Ok(patterns) = std::str::from_utf8(patterns) else {
        return false;
    };
    let Ok(group) = std::str::from_utf8(group) else {
        return false;
    };
    let mut matched = None;
    for pattern in patterns.split(',') {
        let (negated, pattern) = pattern
            .strip_prefix('!')
            .map_or((false, pattern), |pattern| (true, pattern));
        if wildmat_pattern_matches(pattern, group) {
            matched = Some(!negated);
        }
    }
    matched.unwrap_or(false)
}

fn wildmat_pattern_matches(pattern: &str, group: &str) -> bool {
    fn split_first_char(value: &str) -> Option<(char, &str)> {
        let mut chars = value.chars();
        let ch = chars.next()?;
        Some((ch, chars.as_str()))
    }

    fn matches_from(pattern: &str, group: &str) -> bool {
        match (split_first_char(pattern), split_first_char(group)) {
            (None, None) => true,
            (None, Some(_)) => false,
            (Some(('*', rest)), _) => {
                matches_from(rest, group)
                    || split_first_char(group)
                        .is_some_and(|(_, group_rest)| matches_from(pattern, group_rest))
            }
            (Some(('?', rest)), Some((_, group_rest))) => matches_from(rest, group_rest),
            (Some((expected, rest)), Some((actual, group_rest))) if expected == actual => {
                matches_from(rest, group_rest)
            }
            _ => false,
        }
    }

    matches_from(pattern, group)
}

fn newnews_response(args: &[u8]) -> &'static [u8] {
    if newnews_date_key(args).is_some_and(|date| date >= FAR_FUTURE_DATE_KEY) {
        return NEWNEWS_EMPTY_RESPONSE;
    }
    let wildmat = args.split(|byte| *byte == b' ').next().unwrap_or_default();
    if wildmat.is_empty() || wildmat_matches(wildmat, b"alt.test") {
        NEWNEWS_RESPONSE
    } else {
        NEWNEWS_EMPTY_RESPONSE
    }
}

fn newgroups_date_key(args: &[u8]) -> Option<u32> {
    let date = args.split(|byte| *byte == b' ').next()?;
    normalized_nntp_date_key(date, current_utc_year())
}

fn newnews_date_key(args: &[u8]) -> Option<u32> {
    let date = args.split(|byte| *byte == b' ').nth(1)?;
    normalized_nntp_date_key(date, current_utc_year())
}

fn normalized_nntp_date_key(date: &[u8], current_year: u16) -> Option<u32> {
    if !matches!(date.len(), 6 | 8) || !date.iter().all(u8::is_ascii_digit) {
        return None;
    }

    let (year, month, day) = if date.len() == 8 {
        (
            parse_ascii_decimal(&date[..4])?,
            parse_ascii_decimal(&date[4..6])?,
            parse_ascii_decimal(&date[6..])?,
        )
    } else {
        let short_year = parse_ascii_decimal(&date[..2])?;
        let current_century = current_year / 100 * 100;
        let current_short_year = current_year % 100;
        let year = if short_year <= current_short_year {
            current_century + short_year
        } else {
            current_century.saturating_sub(100) + short_year
        };
        (
            year,
            parse_ascii_decimal(&date[2..4])?,
            parse_ascii_decimal(&date[4..])?,
        )
    };

    Some(u32::from(year) * 10_000 + u32::from(month) * 100 + u32::from(day))
}

fn parse_ascii_decimal(value: &[u8]) -> Option<u16> {
    value.iter().try_fold(0_u16, |acc, byte| {
        Some(acc * 10 + u16::from(byte.checked_sub(b'0')?))
    })
}

fn article_selector_error(
    command: &ParsedCommand,
    command_lines: Option<&CommandLineBatch>,
    session_state: &SessionState,
    current_selector_allowed: bool,
) -> Option<&'static [u8]> {
    let args = command_args(command, command_lines).unwrap_or_default();
    if args.is_empty() && current_selector_allowed {
        if !session_state.group_selected {
            return Some(b"412 no newsgroup selected\r\n");
        }
        if session_state.current_article.is_none() {
            return Some(b"420 no current article selected\r\n");
        }
        return None;
    }
    if command.article_id.is_some() && !session_state.group_selected {
        return Some(b"412 no newsgroup selected\r\n");
    }
    if command
        .article_id
        .is_some_and(|article_id| article_id > session_state.article_count())
    {
        return Some(b"423 no article with that number\r\n");
    }
    if command.message_id.is_some() && contains_subslice(args, b"missing") {
        return Some(b"430 no article with that message-id\r\n");
    }
    None
}

fn overview_selector_error(
    command: &ParsedCommand,
    command_lines: Option<&CommandLineBatch>,
    session_state: &SessionState,
) -> Option<&'static [u8]> {
    let args = command_args(command, command_lines).unwrap_or_default();
    if matches!(command.kind, RequestKind::Hdr | RequestKind::Xhdr)
        && header_query_name_from_args(args).is_some_and(|header| !hdr_field_is_supported(header))
    {
        return Some(b"503 HDR field unavailable\r\n");
    }
    let current_selector = match command.kind {
        RequestKind::Over | RequestKind::Xover => args.is_empty(),
        RequestKind::Hdr | RequestKind::Xhdr => args.split(|byte| *byte == b' ').count() == 1,
        _ => false,
    };
    if current_selector {
        if !session_state.group_selected {
            return Some(b"412 no newsgroup selected\r\n");
        }
        if session_state.current_article.is_none() {
            return Some(b"420 no current article selected\r\n");
        }
        return None;
    }
    let selector = overview_selector_arg(command.kind, args);
    if selector.is_some_and(overview_selector_requires_group) && !session_state.group_selected {
        return Some(b"412 no newsgroup selected\r\n");
    }
    if selector.is_some_and(|selector| {
        overview_selector_is_range(selector)
            && !overview_selector_range_selects_articles(selector, session_state.article_count())
    }) {
        return Some(b"423 no articles in that range\r\n");
    }
    if selector.is_some_and(|selector| {
        selector.iter().all(|byte| byte.is_ascii_digit())
            && std::str::from_utf8(selector)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|article_id| article_id > session_state.article_count())
    }) || command
        .article_id
        .is_some_and(|article_id| article_id > session_state.article_count())
    {
        return Some(b"423 no article with that number\r\n");
    }
    if (command.message_id.is_some() || selector.is_some_and(|value| value.starts_with(b"<")))
        && contains_subslice(args, b"missing")
    {
        return Some(b"430 no article with that message-id\r\n");
    }
    None
}

fn overview_selector_requires_group(selector: &[u8]) -> bool {
    !selector.starts_with(b"<")
}

fn overview_selector_arg(kind: RequestKind, args: &[u8]) -> Option<&[u8]> {
    match kind {
        RequestKind::Over | RequestKind::Xover if !args.is_empty() => Some(args),
        RequestKind::Hdr | RequestKind::Xhdr => args
            .split(|byte| *byte == b' ')
            .nth(1)
            .filter(|selector| !selector.is_empty()),
        _ => None,
    }
}

fn over_response_for_args(args: &[u8]) -> &'static [u8] {
    if overview_selector_is_message_id(args) {
        OVER_MESSAGE_ID_RESPONSE
    } else if overview_selector_starts_at_second(args) {
        OVER_2_RESPONSE
    } else if overview_selector_includes_first_two(args) {
        OVER_RANGE_RESPONSE
    } else {
        OVER_RESPONSE
    }
}

fn xover_response_for_args(args: &[u8]) -> &'static [u8] {
    if args.is_empty() {
        OVER_RESPONSE
    } else if overview_selector_is_message_id(args) {
        OVER_MESSAGE_ID_RESPONSE
    } else if overview_selector_starts_at_second(args) {
        OVER_2_RESPONSE
    } else if overview_selector_includes_first_two(args) {
        OVER_RANGE_RESPONSE
    } else {
        XOVER_RESPONSE
    }
}

fn overview_selector_starts_at_second(selector: &[u8]) -> bool {
    selector == b"2" || selector.starts_with(b"2-")
}

fn overview_selector_selects_first_only(selector: &[u8]) -> bool {
    selector == b"1"
}

fn overview_selector_includes_first_two(selector: &[u8]) -> bool {
    let Ok(selector) = std::str::from_utf8(selector) else {
        return false;
    };
    let Some((start, end)) = selector.split_once('-') else {
        return false;
    };
    let Ok(start) = start.parse::<u64>() else {
        return false;
    };
    let end = if end.is_empty() {
        u64::MAX
    } else {
        match end.parse::<u64>() {
            Ok(end) => end,
            Err(_) => return false,
        }
    };
    start <= 1 && end >= 2
}

fn overview_selector_is_range(selector: &[u8]) -> bool {
    selector.contains(&b'-')
}

fn overview_selector_range_selects_articles(selector: &[u8], article_count: u64) -> bool {
    let Some((start, end)) = listgroup_range_bounds(selector) else {
        return false;
    };
    let end = end.unwrap_or(article_count).min(article_count);
    start <= end && start <= article_count
}

fn overview_selector_is_message_id(selector: &[u8]) -> bool {
    selector.starts_with(b"<")
}

fn listgroup_state_error(args: &[u8], session_state: &SessionState) -> Option<&'static [u8]> {
    if listgroup_explicit_group_arg(args)
        .is_some_and(|group| fixture_group_from_name(group).is_none())
    {
        return Some(b"411 no such newsgroup\r\n");
    }
    if !session_state.group_selected && listgroup_uses_current_group(args) {
        return Some(b"412 no newsgroup selected\r\n");
    }
    None
}

fn listgroup_uses_current_group(args: &[u8]) -> bool {
    if args.is_empty() {
        return true;
    }
    let Some(first) = args.split(|byte| *byte == b' ').next() else {
        return true;
    };
    first.iter().all(|byte| byte.is_ascii_digit()) || first.contains(&b'-')
}

fn listgroup_explicit_group_arg(args: &[u8]) -> Option<&[u8]> {
    let first = args
        .split(|byte| *byte == b' ')
        .next()
        .filter(|part| !part.is_empty())?;
    if first.iter().all(|byte| byte.is_ascii_digit()) || first.contains(&b'-') {
        None
    } else {
        Some(first)
    }
}

fn fixture_group_from_name(name: &[u8]) -> Option<FixtureGroup> {
    if name.eq_ignore_ascii_case(b"alt.test") {
        Some(FixtureGroup::AltTest)
    } else if name.eq_ignore_ascii_case(b"comp.lang.rust") {
        Some(FixtureGroup::CompLangRust)
    } else {
        None
    }
}

impl FixtureGroup {
    const fn article_count(self) -> u64 {
        match self {
            Self::AltTest => 3,
            Self::CompLangRust => 1,
        }
    }
}

fn listgroup_selected_group(
    args: &[u8],
    current_group: Option<FixtureGroup>,
) -> Option<FixtureGroup> {
    listgroup_explicit_group_arg(args)
        .and_then(fixture_group_from_name)
        .or(current_group)
}

fn group_response_for_group(group: FixtureGroup) -> &'static [u8] {
    match group {
        FixtureGroup::AltTest => GROUP_RESPONSE,
        FixtureGroup::CompLangRust => GROUP_COMP_RESPONSE,
    }
}

fn group_response_for_args(args: &[u8]) -> &'static [u8] {
    fixture_group_from_name(args)
        .map(group_response_for_group)
        .unwrap_or(b"411 no such newsgroup\r\n")
}

fn last_response_for_article(article_id: u64) -> &'static [u8] {
    match article_id {
        1 => LAST_RESPONSE,
        2 => NAVIGATION_ARTICLE_2_RESPONSE,
        _ => b"423 no article with that number\r\n",
    }
}

fn next_response_for_article(article_id: u64) -> &'static [u8] {
    match article_id {
        2 => NEXT_RESPONSE,
        3 => NAVIGATION_ARTICLE_3_RESPONSE,
        _ => b"423 no article with that number\r\n",
    }
}

fn listgroup_response_for_args(args: &[u8], current_group: Option<FixtureGroup>) -> &'static [u8] {
    if listgroup_explicit_group_arg(args)
        .is_some_and(|group| fixture_group_from_name(group).is_none())
    {
        return b"411 no such newsgroup\r\n";
    }
    let group = listgroup_selected_group(args, current_group).unwrap_or(FixtureGroup::AltTest);
    let range = listgroup_range_arg(args);
    if group == FixtureGroup::CompLangRust {
        if range.is_some_and(|range| !listgroup_range_selects_articles(range, group)) {
            return LISTGROUP_COMP_EMPTY_RESPONSE;
        }
        return LISTGROUP_COMP_RESPONSE;
    }
    if let Some(range) = range {
        if !listgroup_range_selects_articles(range, group) {
            return LISTGROUP_EMPTY_RESPONSE;
        }
        if listgroup_range_starts_at_second(range) {
            return LISTGROUP_2_3_RESPONSE;
        }
    }
    LISTGROUP_RESPONSE
}

fn listgroup_range_arg(args: &[u8]) -> Option<&[u8]> {
    let mut parts = args.split(|byte| *byte == b' ');
    let first = parts.next().filter(|part| !part.is_empty())?;
    if listgroup_explicit_group_arg(args).is_some() {
        parts.next()
    } else if listgroup_uses_current_group(args) {
        Some(first)
    } else {
        None
    }
}

fn listgroup_range_selects_articles(range: &[u8], group: FixtureGroup) -> bool {
    let Some((start, end)) = listgroup_range_bounds(range) else {
        return false;
    };
    let high = group.article_count();
    let end = end.unwrap_or(high).min(high);
    start <= end && start <= high
}

fn listgroup_range_starts_at_second(range: &[u8]) -> bool {
    listgroup_range_bounds(range).is_some_and(|(start, _)| start == 2)
}

fn listgroup_range_bounds(range: &[u8]) -> Option<(u64, Option<u64>)> {
    let value = std::str::from_utf8(range).ok()?;
    if let Some((start, end)) = value.split_once('-') {
        let start = start.parse::<u64>().ok()?;
        let end = (!end.is_empty()).then(|| end.parse::<u64>().ok()).flatten();
        Some((start, end))
    } else {
        value.parse::<u64>().ok().map(|value| (value, Some(value)))
    }
}

fn header_query_name_from_args(args: &[u8]) -> Option<&[u8]> {
    args.split(|byte| *byte == b' ')
        .next()
        .filter(|header| !header.is_empty())
}

fn hdr_field_is_supported(field: &[u8]) -> bool {
    [
        b":bytes".as_slice(),
        b":lines".as_slice(),
        b"Subject".as_slice(),
        b"From".as_slice(),
        b"Date".as_slice(),
        b"Message-ID".as_slice(),
        b"References".as_slice(),
    ]
    .iter()
    .any(|supported| field.eq_ignore_ascii_case(supported))
}

fn hdr_response_for_args(args: &[u8]) -> &'static [u8] {
    let selector = overview_selector_arg(RequestKind::Hdr, args);
    if let Some(header) = header_query_name_from_args(args) {
        if header.eq_ignore_ascii_case(b"Message-ID") {
            if selector.is_some_and(overview_selector_is_message_id) {
                return HDR_MESSAGE_ID_MESSAGE_ID_RESPONSE;
            }
            if selector.is_some_and(overview_selector_selects_first_only) {
                return HDR_MESSAGE_ID_1_RESPONSE;
            }
            if selector.is_some_and(overview_selector_starts_at_second) {
                return HDR_MESSAGE_ID_2_RESPONSE;
            }
            return HDR_MESSAGE_ID_RESPONSE;
        }
        if header.eq_ignore_ascii_case(b":bytes") {
            if selector.is_some_and(overview_selector_is_message_id) {
                return HDR_BYTES_MESSAGE_ID_RESPONSE;
            }
            if selector.is_some_and(overview_selector_selects_first_only) {
                return HDR_BYTES_1_RESPONSE;
            }
            if selector.is_some_and(overview_selector_starts_at_second) {
                return HDR_BYTES_2_RESPONSE;
            }
            return HDR_BYTES_RESPONSE;
        }
        if header.eq_ignore_ascii_case(b":lines") {
            if selector.is_some_and(overview_selector_is_message_id) {
                return HDR_LINES_MESSAGE_ID_RESPONSE;
            }
            if selector.is_some_and(overview_selector_selects_first_only) {
                return HDR_LINES_1_RESPONSE;
            }
            if selector.is_some_and(overview_selector_starts_at_second) {
                return HDR_LINES_2_RESPONSE;
            }
            return HDR_LINES_RESPONSE;
        }
    }
    if selector.is_some_and(overview_selector_is_message_id) {
        return HDR_SUBJECT_MESSAGE_ID_RESPONSE;
    }
    if selector.is_some_and(overview_selector_selects_first_only) {
        return HDR_SUBJECT_1_RESPONSE;
    }
    if selector.is_some_and(overview_selector_starts_at_second) {
        return HDR_SUBJECT_2_RESPONSE;
    }
    HDR_RESPONSE
}

fn xhdr_response_for_args(args: &[u8]) -> &'static [u8] {
    let selector = overview_selector_arg(RequestKind::Xhdr, args);
    if let Some(header) = header_query_name_from_args(args) {
        if header.eq_ignore_ascii_case(b"Message-ID") {
            if selector.is_some_and(overview_selector_is_message_id) {
                return XHDR_MESSAGE_ID_MESSAGE_ID_RESPONSE;
            }
            if selector.is_some_and(overview_selector_selects_first_only) {
                return XHDR_MESSAGE_ID_1_RESPONSE;
            }
            if selector.is_some_and(overview_selector_starts_at_second) {
                return XHDR_MESSAGE_ID_2_RESPONSE;
            }
            return XHDR_MESSAGE_ID_RESPONSE;
        }
        if header.eq_ignore_ascii_case(b":bytes") {
            if selector.is_some_and(overview_selector_is_message_id) {
                return XHDR_BYTES_MESSAGE_ID_RESPONSE;
            }
            if selector.is_some_and(overview_selector_selects_first_only) {
                return XHDR_BYTES_1_RESPONSE;
            }
            if selector.is_some_and(overview_selector_starts_at_second) {
                return XHDR_BYTES_2_RESPONSE;
            }
            return XHDR_BYTES_RESPONSE;
        }
        if header.eq_ignore_ascii_case(b":lines") {
            if selector.is_some_and(overview_selector_is_message_id) {
                return XHDR_LINES_MESSAGE_ID_RESPONSE;
            }
            if selector.is_some_and(overview_selector_selects_first_only) {
                return XHDR_LINES_1_RESPONSE;
            }
            if selector.is_some_and(overview_selector_starts_at_second) {
                return XHDR_LINES_2_RESPONSE;
            }
            return XHDR_LINES_RESPONSE;
        }
    }
    if selector.is_some_and(overview_selector_is_message_id) {
        return XHDR_SUBJECT_MESSAGE_ID_RESPONSE;
    }
    if selector.is_some_and(overview_selector_selects_first_only) {
        return XHDR_SUBJECT_1_RESPONSE;
    }
    if selector.is_some_and(overview_selector_starts_at_second) {
        return XHDR_SUBJECT_2_RESPONSE;
    }
    XHDR_RESPONSE
}

fn over_response(
    command: &ParsedCommand,
    command_lines: Option<&CommandLineBatch>,
    session_state: &SessionState,
) -> &'static [u8] {
    let args = command_args(command, command_lines).unwrap_or_default();
    over_response_for_args(current_overview_args(args, session_state).unwrap_or(args))
}

fn xover_response(
    command: &ParsedCommand,
    command_lines: Option<&CommandLineBatch>,
    session_state: &SessionState,
) -> &'static [u8] {
    let args = command_args(command, command_lines).unwrap_or_default();
    xover_response_for_args(current_overview_args(args, session_state).unwrap_or(args))
}

fn hdr_response(
    command: &ParsedCommand,
    command_lines: Option<&CommandLineBatch>,
    session_state: &SessionState,
) -> &'static [u8] {
    let args = command_args(command, command_lines).unwrap_or_default();
    hdr_response_for_args(current_header_args(args, session_state).unwrap_or(args))
}

fn xhdr_response(
    command: &ParsedCommand,
    command_lines: Option<&CommandLineBatch>,
    session_state: &SessionState,
) -> &'static [u8] {
    let args = command_args(command, command_lines).unwrap_or_default();
    xhdr_response_for_args(current_header_args(args, session_state).unwrap_or(args))
}

fn current_overview_args(args: &[u8], session_state: &SessionState) -> Option<&'static [u8]> {
    if args.is_empty() {
        return current_article_selector_args(session_state);
    }
    None
}

fn current_header_args(args: &[u8], session_state: &SessionState) -> Option<&'static [u8]> {
    if args.split(|byte| *byte == b' ').count() != 1 {
        return None;
    }
    let current = current_article_selector_args(session_state)?;
    match args {
        b"Subject" => match current {
            b"1" => Some(b"Subject 1"),
            b"2" => Some(b"Subject 2"),
            _ => None,
        },
        b"Message-ID" => match current {
            b"1" => Some(b"Message-ID 1"),
            b"2" => Some(b"Message-ID 2"),
            _ => None,
        },
        b":bytes" => match current {
            b"1" => Some(b":bytes 1"),
            b"2" => Some(b":bytes 2"),
            _ => None,
        },
        b":lines" => match current {
            b"1" => Some(b":lines 1"),
            b"2" => Some(b":lines 2"),
            _ => None,
        },
        _ => None,
    }
}

fn current_article_selector_args(session_state: &SessionState) -> Option<&'static [u8]> {
    match session_state.current_article {
        Some(1) => Some(b"1"),
        Some(2) => Some(b"2"),
        _ => None,
    }
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn parse_article_id_arg(args: &[u8]) -> Option<u64> {
    let arg = std::str::from_utf8(args).ok()?.trim();
    if arg.is_empty() || arg.bytes().any(|byte| !byte.is_ascii_digit()) {
        return None;
    }
    arg.parse::<u64>().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TakeThisBodyState {
    LineStart,
    LineStartDot,
    LineStartDotCr,
    Line,
    LineCr,
}

async fn read_dot_terminated_body<R>(reader: &mut BufReader<R>) -> io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut state = TakeThisBodyState::LineStart;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated TAKETHIS body",
            ));
        }

        let mut consumed = 0;
        for &byte in available {
            consumed += 1;
            state = match (state, byte) {
                (TakeThisBodyState::LineStart, b'.') => TakeThisBodyState::LineStartDot,
                (TakeThisBodyState::LineStart, b'\r') => TakeThisBodyState::LineCr,
                (TakeThisBodyState::LineStart, b'\n') => {
                    reader.consume(consumed);
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TAKETHIS body line missing CRLF terminator",
                    ));
                }
                (TakeThisBodyState::LineStart, _) => TakeThisBodyState::Line,

                (TakeThisBodyState::LineStartDot, b'\r') => TakeThisBodyState::LineStartDotCr,
                (TakeThisBodyState::LineStartDot, b'\n') => {
                    reader.consume(consumed);
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TAKETHIS body line missing CRLF terminator",
                    ));
                }
                (TakeThisBodyState::LineStartDot, _) => TakeThisBodyState::Line,

                (TakeThisBodyState::LineStartDotCr, b'\n') => {
                    reader.consume(consumed);
                    return Ok(());
                }
                (TakeThisBodyState::LineStartDotCr, _) => {
                    reader.consume(consumed);
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TAKETHIS body line missing CRLF terminator",
                    ));
                }

                (TakeThisBodyState::Line, b'\r') => TakeThisBodyState::LineCr,
                (TakeThisBodyState::Line, b'\n') => {
                    reader.consume(consumed);
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TAKETHIS body line missing CRLF terminator",
                    ));
                }
                (TakeThisBodyState::Line, _) => TakeThisBodyState::Line,

                (TakeThisBodyState::LineCr, b'\n') => TakeThisBodyState::LineStart,
                (TakeThisBodyState::LineCr, _) => {
                    reader.consume(consumed);
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TAKETHIS body line missing CRLF terminator",
                    ));
                }
            };
        }
        reader.consume(consumed);
    }
}

#[cfg(test)]
pub fn generate_article(target_bytes: usize, body: &[u8]) -> Box<[u8]> {
    let mut response = Vec::with_capacity(target_bytes.max(256) + TERMINATOR.len());
    response.extend_from_slice(b"220 1 <article.1@nntpbench.local> article follows\r\n");
    response.extend_from_slice(b"Path: nntpbench.local!mock\r\n");
    response.extend_from_slice(b"From: Bench User <bench@nntpbench.local>\r\n");
    response.extend_from_slice(b"Newsgroups: alt.binaries.bench\r\n");
    response.extend_from_slice(b"Subject: nntpbench synthetic article\r\n");
    response.extend_from_slice(b"Message-ID: <article.1@nntpbench.local>\r\n");
    response.extend_from_slice(b"Date: Fri, 15 May 2026 00:00:00 +0000\r\n");
    crate::terminator::append_crlf(&mut response);
    append_repeated_payload(&mut response, body_payload(body), target_bytes);
    ensure_terminated(&mut response);
    response.into_boxed_slice()
}

#[cfg(test)]
pub fn generate_body(target_bytes: usize) -> Box<[u8]> {
    let mut response = Vec::with_capacity(target_bytes.max(128) + TERMINATOR.len());
    response.extend_from_slice(b"222 1 <article.1@nntpbench.local> body follows\r\n");
    append_repeated_payload(&mut response, BODY_LINE, target_bytes);
    ensure_terminated(&mut response);
    response.into_boxed_slice()
}

#[cfg(test)]
fn body_payload(body: &[u8]) -> &[u8] {
    let header_end = find_crlf_line_end(body, 0).unwrap_or(0);
    let payload = &body[header_end..];
    strip_dot_terminator_suffix(payload).unwrap_or(payload)
}

const BODY_LINE: &[u8] =
    b"This is synthetic NNTP article payload for throughput and latency benchmarking\r\n";

#[cfg(test)]
fn append_repeated_payload(response: &mut Vec<u8>, line: &[u8], target_bytes: usize) {
    let target_before_dot_line = target_before_dot_terminator(target_bytes);
    while response.len() + line.len() <= target_before_dot_line {
        response.extend_from_slice(line);
    }

    let remaining = target_before_dot_line.saturating_sub(response.len());
    if remaining > 0 {
        response.extend_from_slice(&line[..remaining.min(line.len())]);
    }
}

#[cfg(test)]
fn ensure_terminated(response: &mut Vec<u8>) {
    append_dot_terminator(response);
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncReadExt;

    fn dangerous_takethis_body_bytes() -> impl Strategy<Value = u8> {
        prop_oneof![
            Just(b'\r'),
            Just(b'\n'),
            Just(b'.'),
            Just(b' '),
            b'0'..=b'9',
            b'a'..=b'z',
        ]
    }

    fn remove_rfc_dot_terminators(buffer: &mut [u8]) {
        while let Some(start) = buffer
            .windows(TERMINATOR.len())
            .position(|window| window == TERMINATOR)
        {
            buffer[start + 2] = b'x';
        }
    }

    fn test_args() -> ServerArgs {
        ServerArgs {
            listen: "127.0.0.1:0".parse().unwrap(),
            body_bytes: 1024,
            article_bytes: 2048,
            article_dir: None,
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
            pending_write_bytes: DEFAULT_PENDING_WRITE_BYTES,
        }
    }

    fn test_config() -> Arc<ServerConfig> {
        Arc::new(ServerConfig::from_args(test_args()))
    }

    fn assert_date_response(response: &[u8]) {
        assert_eq!(response.len(), 20);
        assert!(response.starts_with(b"111 "));
        assert!(response.ends_with(b"\r\n"));
        assert!(response[4..18].iter().all(u8::is_ascii_digit));
    }

    fn assert_current_date_response(response: &[u8], before: [u8; 20], after: [u8; 20]) {
        assert_date_response(response);
        assert!(
            response[4..12] == before[4..12] || response[4..12] == after[4..12],
            "DATE response should use current UTC day, got {:?}, before {:?}, after {:?}",
            String::from_utf8_lossy(response),
            String::from_utf8_lossy(&before),
            String::from_utf8_lossy(&after)
        );
    }

    struct AllocationCountGuard;

    impl AllocationCountGuard {
        fn start() -> Self {
            COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
            TEST_ALLOCATIONS.store(0, Ordering::Relaxed);
            COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));
            Self
        }

        fn finish(self, label: &str) {
            COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
            let allocations = TEST_ALLOCATIONS.load(Ordering::Relaxed);
            assert_eq!(allocations, 0, "{label} allocated {allocations} times");
        }
    }

    impl Drop for AllocationCountGuard {
        fn drop(&mut self) {
            COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        }
    }

    fn assert_no_allocations(label: &str, run: impl FnOnce()) {
        let guard = AllocationCountGuard::start();
        run();
        guard.finish(label);
    }

    fn client_command_mix_strategy() -> impl Strategy<Value = ClientCommandMix> {
        prop_oneof![
            Just(ClientCommandMix::Article),
            Just(ClientCommandMix::Body),
            Just(ClientCommandMix::Alternate),
        ]
    }

    fn expected_client_command_counts(
        start_id: u64,
        requests: u64,
        mix: ClientCommandMix,
    ) -> (u64, u64) {
        let mut articles = 0;
        let mut bodies = 0;
        for offset in 0..requests {
            match client_command_kind(start_id.wrapping_add(offset), mix) {
                ClientCommandMix::Article => articles += 1,
                ClientCommandMix::Body => bodies += 1,
                ClientCommandMix::Alternate => {
                    unreachable!("client_command_kind should normalize Alternate")
                }
            }
        }
        (articles, bodies)
    }

    fn direct_e2e_proptest_config(cases: u32) -> ProptestConfig {
        ProptestConfig {
            cases,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_direct_client_server_case(
        body_bytes: usize,
        article_bytes: usize,
        max_pipeline_depth: usize,
        requests: u64,
        transfer_bytes: u64,
        pipeline_depth: usize,
        mix: ClientCommandMix,
        start_id: u64,
        read_buffer_bytes: usize,
    ) -> io::Result<(Snapshot, Snapshot)> {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();

        let mut server_args = test_args();
        server_args.body_bytes = body_bytes;
        server_args.article_bytes = article_bytes;
        server_args.max_pipeline_depth = max_pipeline_depth;
        let server_config = Arc::new(ServerConfig::from_args(server_args));
        let server_stats = Arc::new(Stats::new());

        let server_task = tokio::spawn({
            let server_config = server_config.clone();
            let server_stats = server_stats.clone();
            async move {
                let (stream, peer_addr) = listener.accept().await.unwrap();
                serve_session(stream, peer_addr, server_config, server_stats).await
            }
        });

        let mut client_args = test_client_args();
        client_args.connect = addr;
        client_args.requests = requests;
        client_args.transfer_bytes = transfer_bytes;
        client_args.pipeline_depth = pipeline_depth;
        client_args.command_mix = mix;
        client_args.start_id = start_id;
        client_args.read_buffer_bytes = read_buffer_bytes;
        client_args.socket_recv_buffer = 0;
        client_args.socket_send_buffer = 0;
        let client_config = ClientConfig::from_args(client_args)?;
        let client_session = ClientSession::new(&client_config, 0, start_id, requests);
        let client_stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        client_session.run(client_stats.clone(), stop).await?;
        server_task.await.unwrap()?;

        Ok((client_stats.snapshot(), server_stats.snapshot()))
    }

    struct FixedWrite<const N: usize> {
        data: [u8; N],
        len: usize,
    }

    impl<const N: usize> FixedWrite<N> {
        const fn new() -> Self {
            Self {
                data: [0; N],
                len: 0,
            }
        }

        fn clear(&mut self) {
            self.len = 0;
        }

        fn as_slice(&self) -> &[u8] {
            &self.data[..self.len]
        }
    }

    impl<const N: usize> Write for FixedWrite<N> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.len + buf.len() > self.data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "fixed test sink overflow",
                ));
            }
            let end = self.len + buf.len();
            self.data[self.len..end].copy_from_slice(buf);
            self.len = end;
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn test_fetch_args() -> FetchArgs {
        FetchArgs {
            connect: "127.0.0.1:1199".parse().unwrap(),
            request: FetchRequestKind::Article,
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

    fn test_client_args() -> ClientArgs {
        ClientArgs {
            connect: "127.0.0.1:1199".parse().unwrap(),
            ports: Vec::new(),
            segments: None,
            auth_user: None,
            auth_pass: None,
            article_ids: None,
            article_output_dir: None,
            article_verify_dir: None,
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

    fn unique_temp_dir(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = format!(
            "nntpbench-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        path.push(unique);
        fs::create_dir_all(&path).unwrap();
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

    mod rfc_compliance {
        use super::*;

        fn without_greeting(output: &[u8]) -> &[u8] {
            assert!(
                output.starts_with(GREETING),
                "server output did not start with RFC 3977 section 5.1 greeting: {:?}",
                String::from_utf8_lossy(output)
            );
            &output[GREETING.len()..]
        }

        fn assert_single_response(input: &[u8], expected: &[u8], output: &[u8], rfc: &str) {
            assert_eq!(
                without_greeting(output),
                expected,
                "{rfc}: input {:?} should produce {:?}, got {:?}",
                String::from_utf8_lossy(input),
                String::from_utf8_lossy(expected),
                String::from_utf8_lossy(without_greeting(output))
            );
        }

        async fn run_session_with_input_allowing_server_error(
            config: Arc<ServerConfig>,
            input: &[u8],
        ) -> (Vec<u8>, Option<String>) {
            let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 128, false).unwrap();
            let addr = listener.local_addr().unwrap();
            let stats = Arc::new(Stats::new());

            let server = tokio::spawn({
                let stats = stats.clone();
                async move {
                    let (stream, peer_addr) = listener.accept().await.unwrap();
                    serve_session(stream, peer_addr, config, stats).await
                }
            });

            let mut client = TcpStream::connect(addr).await.unwrap();
            client.write_all(input).await.unwrap();
            client.shutdown().await.unwrap();

            let mut output = Vec::new();
            client.read_to_end(&mut output).await.unwrap();
            let error = match server.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => Some(error.to_string()),
            };

            (output, error)
        }

        fn assert_red_invalid_values(cases: &[(&str, &str, bool)]) {
            let failures = cases
                .iter()
                .filter_map(|(name, _, is_invalid)| (!is_invalid).then_some(*name))
                .collect::<Vec<_>>();

            assert!(
                failures.is_empty(),
                "expected RFC-invalid values to be rejected; passed unexpectedly: {failures:?}\n{}",
                cases
                    .iter()
                    .map(|(name, reference, _)| format!("{name}: {reference}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }

        fn assert_red_request_line_unknown_cases(cases: &[(&str, &'static [u8], &str)]) {
            let failures = cases
                .iter()
                .filter_map(|(name, input, _)| {
                    (RequestLine::parse(input).kind() != RequestKind::Unknown).then_some(*name)
                })
                .collect::<Vec<_>>();

            assert!(
                failures.is_empty(),
                "expected request-line parser to reject cases; passed unexpectedly: {failures:?}\n{}",
                cases
                    .iter()
                    .map(|(name, input, reference)| {
                        format!("{name}: {:?} {reference}", String::from_utf8_lossy(input))
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }

        fn assert_red_request_line_known_cases(cases: &[(&str, &'static [u8], RequestKind, &str)]) {
            let failures = cases
                .iter()
                .filter_map(|(name, input, expected, reference)| {
                    let actual = RequestLine::parse(input).kind();
                    (actual != *expected).then(|| {
                        format!("{name}: expected {expected:?}, got {actual:?} ({reference})")
                    })
                })
                .collect::<Vec<_>>();

            assert!(
                failures.is_empty(),
                "expected request-line parser to accept RFC-valid cases:\n{}",
                failures.join("\n")
            );
        }

        struct ServerResponseCase {
            name: &'static str,
            reference: &'static str,
            input: &'static [u8],
            expected: &'static [u8],
        }

        async fn assert_red_server_response_cases(cases: &[ServerResponseCase]) {
            assert_red_server_response_cases_with_config(test_config(), cases).await;
        }

        async fn assert_red_server_response_cases_with_config(
            config: Arc<ServerConfig>,
            cases: &[ServerResponseCase],
        ) {
            let mut failures = Vec::new();
            for case in cases {
                let (output, _) = run_session_with_input(config.clone(), case.input).await;
                let actual = without_greeting(&output);
                if actual != case.expected {
                    failures.push(format!(
                        "{}: expected {:?}, got {:?} ({})",
                        case.name,
                        String::from_utf8_lossy(case.expected),
                        String::from_utf8_lossy(actual),
                        case.reference
                    ));
                }
            }

            assert!(
                failures.is_empty(),
                "server response RFC audit cases passed unexpectedly:\n{}",
                failures.join("\n")
            );
        }

        async fn assert_red_server_response_tail_cases(cases: &[ServerResponseCase]) {
            let mut failures = Vec::new();
            for case in cases {
                let (output, _) = run_session_with_input(test_config(), case.input).await;
                let actual = without_greeting(&output);
                if !actual.ends_with(case.expected) {
                    failures.push(format!(
                        "{}: expected tail {:?}, got {:?} ({})",
                        case.name,
                        String::from_utf8_lossy(case.expected),
                        String::from_utf8_lossy(actual),
                        case.reference
                    ));
                }
            }

            assert!(
                failures.is_empty(),
                "server response RFC audit tail cases failed:\n{}",
                failures.join("\n")
            );
        }

        struct ResponseFrameCase {
            name: &'static str,
            reference: &'static str,
            kind: RequestKind,
            frame: &'static [u8],
        }

        fn assert_red_response_frame_invalid_cases(cases: &[ResponseFrameCase]) {
            let failures = cases
                .iter()
                .filter(|case| {
                    protocol::ResponseFrame::parse(case.kind, case.frame)
                        != protocol::ResponseFrameParse::Invalid
                })
                .map(|case| format!("{}: {}", case.name, case.reference))
                .collect::<Vec<_>>();

            assert!(
                failures.is_empty(),
                "expected response frames to be invalid; passed unexpectedly:\n{}",
                failures.join("\n")
            );
        }

        fn assert_red_response_frame_valid_cases(cases: &[ResponseFrameCase]) {
            let failures = cases
                .iter()
                .filter(|case| {
                    !matches!(
                        protocol::ResponseFrame::parse(case.kind, case.frame),
                        protocol::ResponseFrameParse::Complete(_)
                    )
                })
                .map(|case| format!("{}: {}", case.name, case.reference))
                .collect::<Vec<_>>();

            assert!(
                failures.is_empty(),
                "expected response frames to be valid; rejected unexpectedly:\n{}",
                failures.join("\n")
            );
        }

        #[test]
        fn rfc3977_red_article_parser_unstuffs_dot_prefixed_body_lines() {
            // RFC 3977 section 3.1.1 requires clients to remove one leading dot
            // from dot-stuffed data lines in multi-line data blocks:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            for (name, input, expected) in [
                (
                    "BODY first line",
                    b"222 1 <dot@test>\r\n..starts-with-dot\r\n.\r\n".as_slice(),
                    b".starts-with-dot\r\n".as_slice(),
                ),
                (
                    "BODY later line",
                    b"222 1 <dot@test>\r\nplain\r\n..middle-dot\r\n.more\r\n.\r\n".as_slice(),
                    b"plain\r\n.middle-dot\r\nmore\r\n".as_slice(),
                ),
                (
                    "ARTICLE body lines",
                    b"220 1 <dot@test>\r\nSubject: dots\r\n\r\n..article-dot\r\nplain\r\n.more\r\n.\r\n"
                        .as_slice(),
                    b".article-dot\r\nplain\r\nmore\r\n".as_slice(),
                ),
            ] {
                let article = Article::parse(input).unwrap();
                assert_eq!(
                    article.body.as_deref(),
                    Some(expected),
                    "{name}: RFC 3977 body lines should be dot-unstuffed"
                );
            }
        }

        #[test]
        fn rfc3977_red_article_parser_rejects_invalid_success_line_arguments() {
            // RFC 3977 sections 6.2.1 through 6.2.4 define successful
            // ARTICLE/HEAD/BODY/STAT responses as status, article number,
            // and message-id, with response arguments separated by single spaces:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1
            for (name, input) in [
                (
                    "ARTICLE missing article number",
                    b"220 <missing-number@test>\r\nSubject: bad\r\n\r\nbody\r\n.\r\n".as_slice(),
                ),
                (
                    "HEAD non-numeric article number",
                    b"221 abc <bad-number@test>\r\nSubject: bad\r\n.\r\n".as_slice(),
                ),
                (
                    "BODY overflowing article number",
                    b"222 999999999999999999999999 <overflow@test>\r\nbody\r\n.\r\n".as_slice(),
                ),
                (
                    "STAT message-id not separated from text",
                    b"223 1 <bad-space@test>extra\r\n".as_slice(),
                ),
            ] {
                assert!(
                    Article::parse(input).is_err(),
                    "{name}: RFC 3977 article-family success line should reject invalid arguments"
                );
            }
        }

        #[tokio::test]
        async fn rfc3977_red_server_returns_501_for_syntax_error_not_article_body() {
            // RFC 3977 sections 3.2.1 and 6.2.1 reserve 501 for command syntax
            // errors. A malformed ARTICLE line must not be treated as a valid fetch:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.2.1
            let input = b"ARTICLE <a@b> extra\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_group_unknown_group_returns_411() {
            // RFC 3977 section 6.1.1 defines 411 for a nonexistent newsgroup:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1
            let input = b"GROUP no.such.group\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"411 no such newsgroup\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_group_and_listgroup_responses_match_selected_group() {
            // RFC 3977 sections 6.1.1 and 6.1.2 require GROUP and LISTGROUP
            // responses to describe the requested selected group, not a fixed
            // fixture group:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "GROUP comp.lang.rust describes comp.lang.rust",
                    reference: "RFC 3977 section 6.1.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1",
                    input: b"GROUP comp.lang.rust\r\n",
                    expected: GROUP_COMP_RESPONSE,
                },
                ServerResponseCase {
                    name: "GROUP valid but nonexistent group returns 411",
                    reference: "RFC 3977 section 6.1.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1",
                    input: b"GROUP example.valid\r\n",
                    expected: b"411 no such newsgroup\r\n",
                },
                ServerResponseCase {
                    name: "LISTGROUP comp.lang.rust describes comp.lang.rust",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"LISTGROUP comp.lang.rust\r\n",
                    expected: LISTGROUP_COMP_RESPONSE,
                },
                ServerResponseCase {
                    name: "LISTGROUP valid but nonexistent group returns 411",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"LISTGROUP example.valid\r\n",
                    expected: b"411 no such newsgroup\r\n",
                },
                ServerResponseCase {
                    name: "current LISTGROUP follows selected comp.lang.rust",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"GROUP comp.lang.rust\r\nLISTGROUP\r\n",
                    expected: b"211 1 1 1 comp.lang.rust\r\n211 1 1 1 comp.lang.rust\r\n1\r\n.\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_selected_group_article_bounds_are_enforced() {
            // RFC 3977 sections 6.1, 6.2, and 8 require article-number
            // selectors and navigation to be scoped to the selected group:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1
            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "NEXT at end of one-article group",
                    reference: "RFC 3977 section 6.1.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4",
                    input: b"GROUP comp.lang.rust\r\nNEXT\r\n",
                    expected: b"421 no next article in this group\r\n",
                },
                ServerResponseCase {
                    name: "ARTICLE outside one-article group",
                    reference: "RFC 3977 section 6.2.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1",
                    input: b"GROUP comp.lang.rust\r\nARTICLE 2\r\n",
                    expected: b"423 no article with that number\r\n",
                },
                ServerResponseCase {
                    name: "HEAD outside one-article group",
                    reference: "RFC 3977 section 6.2.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.2",
                    input: b"GROUP comp.lang.rust\r\nHEAD 2\r\n",
                    expected: b"423 no article with that number\r\n",
                },
                ServerResponseCase {
                    name: "BODY outside one-article group",
                    reference: "RFC 3977 section 6.2.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.3",
                    input: b"GROUP comp.lang.rust\r\nBODY 2\r\n",
                    expected: b"423 no article with that number\r\n",
                },
                ServerResponseCase {
                    name: "STAT outside one-article group",
                    reference: "RFC 3977 section 6.2.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.4",
                    input: b"GROUP comp.lang.rust\r\nSTAT 2\r\n",
                    expected: b"423 no article with that number\r\n",
                },
                ServerResponseCase {
                    name: "OVER outside one-article group",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"GROUP comp.lang.rust\r\nOVER 2\r\n",
                    expected: b"423 no article with that number\r\n",
                },
                ServerResponseCase {
                    name: "HDR outside one-article group",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP comp.lang.rust\r\nHDR Subject 2\r\n",
                    expected: b"423 no article with that number\r\n",
                },
                ServerResponseCase {
                    name: "LISTGROUP range outside one-article group",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"GROUP comp.lang.rust\r\nLISTGROUP 2-3\r\n",
                    expected: LISTGROUP_COMP_EMPTY_RESPONSE,
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_last_before_group_returns_412_no_group_selected() {
            // RFC 3977 section 6.1.3 defines 412 when LAST is used before a group
            // has been selected:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.3
            let input = b"LAST\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_next_before_group_returns_412_no_group_selected() {
            // RFC 3977 section 6.1.4 defines 412 when NEXT is used before a group
            // has been selected:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4
            let input = b"NEXT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_last_next_current_article_state_matrix() {
            // RFC 3977 sections 6.1.1, 6.1.3, and 6.1.4 require GROUP to set
            // the current article to the first article, then LAST/NEXT must
            // apply first/last article edge responses:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.3
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4
            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "LAST after GROUP at first article",
                    reference: "RFC 3977 section 6.1.3",
                    input: b"GROUP alt.test\r\nLAST\r\n",
                    expected: b"422 no previous article in this group\r\n",
                },
                ServerResponseCase {
                    name: "NEXT after GROUP advances from first article",
                    reference: "RFC 3977 section 6.1.4",
                    input: b"GROUP alt.test\r\nNEXT\r\n",
                    expected: NEXT_RESPONSE,
                },
                ServerResponseCase {
                    name: "LAST at first article",
                    reference: "RFC 3977 section 6.1.3",
                    input: b"GROUP alt.test\r\nARTICLE 1\r\nLAST\r\n",
                    expected: b"422 no previous article in this group\r\n",
                },
                ServerResponseCase {
                    name: "NEXT at last article",
                    reference: "RFC 3977 section 6.1.4",
                    input: b"GROUP alt.test\r\nARTICLE 3\r\nNEXT\r\n",
                    expected: b"421 no next article in this group\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_last_next_move_relative_to_current_article() {
            // RFC 3977 sections 6.1.3 and 6.1.4 define LAST and NEXT as moving
            // to the previous or next article relative to the current article,
            // not as fixed jumps to one canned article number:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.3
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4
            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "LAST from article 2 moves to article 1",
                    reference: "RFC 3977 section 6.1.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.3",
                    input: b"GROUP alt.test\r\nARTICLE 2\r\nLAST\r\n",
                    expected: LAST_RESPONSE,
                },
                ServerResponseCase {
                    name: "LAST from article 3 moves to article 2",
                    reference: "RFC 3977 section 6.1.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.3",
                    input: b"GROUP alt.test\r\nARTICLE 3\r\nLAST\r\n",
                    expected: NAVIGATION_ARTICLE_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEXT from article 2 moves to article 3",
                    reference: "RFC 3977 section 6.1.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4",
                    input: b"GROUP alt.test\r\nARTICLE 2\r\nNEXT\r\n",
                    expected: NAVIGATION_ARTICLE_3_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEXT in one-article group has no next article",
                    reference: "RFC 3977 section 6.1.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4",
                    input: b"GROUP comp.lang.rust\r\nNEXT\r\n",
                    expected: b"421 no next article in this group\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_current_article_fetch_before_group_returns_412() {
            // RFC 3977 section 6.2.1 requires current-article retrieval to fail
            // when no group is selected:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1
            let input = b"ARTICLE\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_article_numeric_before_group_returns_412() {
            // RFC 3977 section 6.2 requires numeric article selectors to fail
            // with 412 when no group is selected:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "ARTICLE numeric before GROUP",
                    reference: "RFC 3977 section 6.2.1",
                    input: b"ARTICLE 2\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "HEAD numeric before GROUP",
                    reference: "RFC 3977 section 6.2.2",
                    input: b"HEAD 2\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "BODY numeric before GROUP",
                    reference: "RFC 3977 section 6.2.3",
                    input: b"BODY 2\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "STAT numeric before GROUP",
                    reference: "RFC 3977 section 6.2.4",
                    input: b"STAT 2\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_article_file_backend_preserves_selector_state_and_errors() {
            // RFC 3977 section 6.2.1 keeps numeric ARTICLE selectors scoped to
            // the selected group even when articles are served from a local store:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1
            let article_dir = unique_temp_dir("rfc-article-dir");
            let response =
                b"220 2 <article.2@test> article follows\r\nSubject: stored\r\n\r\nbody\r\n.\r\n";
            let path = article_id_tree_path(&article_dir, 2);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, response).unwrap();

            let message_response =
                b"220 1 <stored@ngPost> article follows\r\nSubject: message\r\n\r\nbody\r\n.\r\n";
            let message_id = MessageId::from_borrowed("<stored@ngPost>").unwrap();
            let message_path = message_id_tree_path(&article_dir, &message_id);
            fs::create_dir_all(message_path.parent().unwrap()).unwrap();
            fs::write(&message_path, message_response).unwrap();

            let mut args = test_args();
            args.article_dir = Some(article_dir.clone());
            let config = Arc::new(ServerConfig::from_args(args));
            assert_red_server_response_cases_with_config(
                config.clone(),
                &[
                    ServerResponseCase {
                        name: "stored numeric ARTICLE before GROUP",
                        reference: "RFC 3977 section 6.2.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1",
                        input: b"ARTICLE 2\r\n",
                        expected: b"412 no newsgroup selected\r\n",
                    },
                    ServerResponseCase {
                        name: "stored missing message-id ARTICLE",
                        reference: "RFC 3977 section 6.2.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1",
                        input: b"ARTICLE <missing@ngPost>\r\n",
                        expected: b"430 no article with that message-id\r\n",
                    },
                ],
            )
            .await;

            let (output, _) =
                run_session_with_input(config.clone(), b"GROUP alt.test\r\nARTICLE 3\r\n").await;
            assert!(
                without_greeting(&output).ends_with(b"423 no article with that number\r\n"),
                "RFC 3977 missing stored numeric ARTICLE should end with 423, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );

            let (output, _) = run_session_with_input(
                config,
                b"GROUP alt.test\r\nARTICLE 2\r\nARTICLE <stored@ngPost>\r\nSTAT\r\n",
            )
            .await;
            assert!(
                without_greeting(&output)
                    .ends_with(b"223 2 <article.2@nntpbench.local> article retrieved\r\n"),
                "RFC 3977 stored numeric ARTICLE must set current article, while message-id ARTICLE must not change it, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );

            fs::remove_dir_all(article_dir).unwrap();
        }

        #[tokio::test]
        async fn rfc3977_red_missing_article_number_returns_423() {
            // RFC 3977 section 6.2.1 defines 423 when an article number cannot
            // be found in the selected group:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1
            let input = b"GROUP alt.test\r\nARTICLE 999999\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert!(
                without_greeting(&output).ends_with(b"423 no article with that number\r\n"),
                "RFC 3977 missing numeric ARTICLE should end with 423, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_article_family_numeric_selectors_echo_selected_article() {
            // RFC 3977 sections 6.2.1 through 6.2.4 require successful
            // ARTICLE/HEAD/BODY/STAT responses to include the selected article
            // number and message-id, not a fixed fixture identity:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2
            for (name, input, expected_prefix) in [
                (
                    "ARTICLE numeric selector",
                    b"GROUP alt.test\r\nARTICLE 2\r\n".as_slice(),
                    b"220 2 <article.2@nntpbench.local> article follows\r\n".as_slice(),
                ),
                (
                    "HEAD numeric selector",
                    b"GROUP alt.test\r\nHEAD 2\r\n".as_slice(),
                    b"221 2 <article.2@nntpbench.local> article retrieved\r\n".as_slice(),
                ),
                (
                    "BODY numeric selector",
                    b"GROUP alt.test\r\nBODY 2\r\n".as_slice(),
                    b"222 2 <article.2@nntpbench.local> body follows\r\n".as_slice(),
                ),
                (
                    "STAT numeric selector",
                    b"GROUP alt.test\r\nSTAT 2\r\n".as_slice(),
                    b"223 2 <article.2@nntpbench.local> article retrieved\r\n".as_slice(),
                ),
            ] {
                let (output, _) = run_session_with_input(test_config(), input).await;
                let actual = without_greeting(&output);
                assert!(
                    actual
                        .windows(expected_prefix.len())
                        .any(|window| window == expected_prefix),
                    "{name}: expected RFC 3977 response prefix {:?}, got {:?}",
                    String::from_utf8_lossy(expected_prefix),
                    String::from_utf8_lossy(actual)
                );
            }

            let input = b"GROUP alt.test\r\nARTICLE 2\r\nSTAT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert!(
                without_greeting(&output)
                    .ends_with(b"223 2 <article.2@nntpbench.local> article retrieved\r\n"),
                "RFC 3977 current STAT should report current article 2, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_article_family_message_id_selectors_do_not_change_current_article() {
            // RFC 3977 section 6.2 requires message-id forms to return the
            // requested message-id with article number 0 when group membership
            // is not asserted, and to leave current group/article state alone:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2
            for (name, input, expected_prefix) in [
                (
                    "ARTICLE message-id selector",
                    b"ARTICLE <by-id@nntpbench.local>\r\n".as_slice(),
                    b"220 0 <by-id@nntpbench.local> article follows\r\n".as_slice(),
                ),
                (
                    "HEAD message-id selector",
                    b"HEAD <by-id@nntpbench.local>\r\n".as_slice(),
                    b"221 0 <by-id@nntpbench.local> article retrieved\r\n".as_slice(),
                ),
                (
                    "BODY message-id selector",
                    b"BODY <by-id@nntpbench.local>\r\n".as_slice(),
                    b"222 0 <by-id@nntpbench.local> body follows\r\n".as_slice(),
                ),
                (
                    "STAT message-id selector",
                    b"STAT <by-id@nntpbench.local>\r\n".as_slice(),
                    b"223 0 <by-id@nntpbench.local> article retrieved\r\n".as_slice(),
                ),
            ] {
                let (output, _) = run_session_with_input(test_config(), input).await;
                let actual = without_greeting(&output);
                assert!(
                    actual.starts_with(expected_prefix),
                    "{name}: expected RFC 3977 message-id response prefix {:?}, got {:?}",
                    String::from_utf8_lossy(expected_prefix),
                    String::from_utf8_lossy(actual)
                );
            }

            let input =
                b"GROUP alt.test\r\nARTICLE 2\r\nARTICLE <by-id@nntpbench.local>\r\nSTAT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert!(
                without_greeting(&output)
                    .ends_with(b"223 2 <article.2@nntpbench.local> article retrieved\r\n"),
                "RFC 3977 message-id ARTICLE must not change current article, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_posting_disabled_returns_440_for_post() {
            // RFC 3977 section 6.3.1 defines 440 when posting is not permitted:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.3.1
            let input = b"POST\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"440 posting not permitted\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_ihave_rejected_on_reader_server_without_transfer_state() {
            // RFC 3977 section 6.3.2 defines IHAVE transfer responses including
            // 435/436 rejections. A mock reader server should not invite transfer:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.3.2
            let input = b"IHAVE <article@test>\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"435 article not wanted\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_newgroups_invalid_date_returns_501() {
            // RFC 3977 sections 7.3.1 and 9.8 require syntactically valid date
            // and time arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.3.1
            let input = b"NEWGROUPS 20261301 000000 GMT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_newnews_invalid_wildmat_returns_501() {
            // RFC 3977 sections 4 and 7.4.1 require a valid wildmat argument:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1
            let input = b"NEWNEWS ! 20260101 000000 GMT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_date_response_reflects_current_utc_date() {
            // RFC 3977 section 7.1 says DATE returns the server's current date
            // and time in UTC, not a fixed fixture timestamp:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.1
            let input = b"DATE\r\n";
            let before = current_date_response();
            let (output, _) = run_session_with_input(test_config(), input).await;
            let after = current_date_response();
            assert_current_date_response(without_greeting(&output), before, after);
        }

        #[tokio::test]
        async fn rfc3977_red_over_invalid_selector_returns_501() {
            // RFC 3977 sections 8.3.1 and 9.8 require an article range,
            // message-id, or current article selector for OVER:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.3.1
            let input = b"OVER 0\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_hdr_invalid_header_name_returns_501() {
            // RFC 3977 section 8.5.1 requires a valid header field name.
            // A trailing colon is not part of the field-name token:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5.1
            let input = b"HDR Subject: 1\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_hdr_accepts_metadata_names_with_leading_colon() {
            // RFC 3977 section 8.5.2 permits metadata item names with a leading
            // colon, such as ":bytes":
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2
            let input = b"GROUP alt.test\r\nHDR :bytes 1\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            let text = String::from_utf8_lossy(without_greeting(&output));
            assert!(
                text.ends_with("\r\n225 headers follow\r\n1 123\r\n.\r\n"),
                "RFC 3977 HDR metadata names must be accepted, got {text:?}"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_capabilities_do_not_advertise_unavailable_extensions() {
            // RFC 3977 section 3.3 requires CAPABILITIES to describe available
            // protocol extensions. RFC 4642 STARTTLS, RFC 4643 SASL, and RFC 4644
            // STREAMING must not be advertised when their commands are unavailable:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.3
            let (output, _) = run_session_with_input(test_config(), b"CAPABILITIES\r\n").await;
            let text = String::from_utf8_lossy(without_greeting(&output));
            assert!(
                text.contains("VERSION 2")
                    && text.contains("READER\r\n")
                    && text.contains("MODE-READER\r\n"),
                "missing required RFC 3977 base/reader capabilities, got {text:?}"
            );
            assert!(
                text.contains("AUTHINFO\r\n"),
                "missing RFC 4643 AUTHINFO capability marker, got {text:?}"
            );
            assert!(
                !text.contains("AUTHINFO USER")
                    && !text.contains("STARTTLS")
                    && !text.contains("SASL")
                    && !text.contains("STREAMING"),
                "advertised unavailable extension capability, got {text:?}"
            );
        }

        #[tokio::test]
        async fn rfc4643_red_plaintext_user_pass_not_advertised_or_accepted() {
            // RFC 4643 sections 2.1 and 2.3.2 say AUTHINFO USER/PASS should
            // not be advertised without an active strong encryption layer, and
            // PASS must not be implemented without TLS support. RFC 3977 section
            // 3.2.1 defines 483 for insufficient security:
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.3.2
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "AUTHINFO USER rejected before TLS",
                    reference: "RFC 4643 section 2.3.2 https://www.rfc-editor.org/rfc/rfc4643#section-2.3.2",
                    input: b"AUTHINFO USER bench\r\n",
                    expected: b"483 command unavailable until TLS has been negotiated\r\n",
                },
                ServerResponseCase {
                    name: "AUTHINFO PASS rejected before TLS",
                    reference: "RFC 4643 section 2.3.2 https://www.rfc-editor.org/rfc/rfc4643#section-2.3.2",
                    input: b"AUTHINFO PASS pass\r\n",
                    expected: b"483 command unavailable until TLS has been negotiated\r\n",
                },
                ServerResponseCase {
                    name: "AUTHINFO USER does not enable PASS without TLS",
                    reference: "RFC 4643 section 2.3.2 https://www.rfc-editor.org/rfc/rfc4643#section-2.3.2",
                    input: b"AUTHINFO USER bench\r\nAUTHINFO PASS pass\r\n",
                    expected: b"483 command unavailable until TLS has been negotiated\r\n483 command unavailable until TLS has been negotiated\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc4642_red_starttls_without_tls_support_returns_502() {
            // RFC 4642 section 2.2 requires STARTTLS to begin TLS negotiation after
            // a 382 response. A server that cannot negotiate TLS must reject it:
            // https://www.rfc-editor.org/rfc/rfc4642#section-2.2
            let input = b"STARTTLS\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"502 command unavailable\r\n", &output, "RFC 4642");
        }

        #[tokio::test]
        async fn rfc4643_red_authinfo_pass_before_tls_is_rejected() {
            // RFC 4643 section 2.3.2 says AUTHINFO PASS must not be
            // implemented without TLS support, and 483 reports an insufficiently
            // secure datastream:
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.3.2
            let input = b"AUTHINFO PASS bench-pass\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(
                input,
                b"483 command unavailable until TLS has been negotiated\r\n",
                &output,
                "RFC 4643",
            );
        }

        #[tokio::test]
        async fn rfc4643_red_authinfo_sasl_unavailable_with_valid_syntax() {
            // RFC 4643 section 2.1 says AUTHINFO without arguments means no
            // authentication commands are permitted in the current state.
            // Section 2.4.1 still allows AUTHINFO SASL to include an optional
            // base64 initial response; valid syntax should reach the same
            // unavailable-command response, not a syntax error:
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.1
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.4.1
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "SASL mechanism unavailable when AUTHINFO has no arguments",
                    reference: "RFC 4643 sections 2.1 and 2.4.1 https://www.rfc-editor.org/rfc/rfc4643#section-2.1",
                    input: b"AUTHINFO SASL MADE-UP-MECHANISM\r\n",
                    expected: b"502 command unavailable\r\n",
                },
                ServerResponseCase {
                    name: "SASL zero-length initial response marker",
                    reference: "RFC 4643 section 2.4.1 https://www.rfc-editor.org/rfc/rfc4643#section-2.4.1",
                    input: b"AUTHINFO SASL MADE-UP-MECHANISM =\r\n",
                    expected: b"502 command unavailable\r\n",
                },
                ServerResponseCase {
                    name: "SASL padded base64 initial response",
                    reference: "RFC 4643 section 2.4.1 https://www.rfc-editor.org/rfc/rfc4643#section-2.4.1",
                    input: b"AUTHINFO SASL MADE-UP-MECHANISM YQ==\r\n",
                    expected: b"502 command unavailable\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc4643_red_authinfo_sasl_rejects_malformed_initial_response() {
            // RFC 4643 section 2.4.2 requires SASL initial responses to use
            // valid base64 syntax; padding cannot appear in the middle:
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.4.2
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "SASL padding in middle",
                    reference: "RFC 4643 section 2.4.2 https://www.rfc-editor.org/rfc/rfc4643#section-2.4.2",
                    input: b"AUTHINFO SASL MADE-UP-MECHANISM AAA=BBB\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "SASL excessive padding",
                    reference: "RFC 4643 section 2.4.2 https://www.rfc-editor.org/rfc/rfc4643#section-2.4.2",
                    input: b"AUTHINFO SASL MADE-UP-MECHANISM Y===\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "SASL non-quad base64 length",
                    reference: "RFC 4643 section 2.4.2 https://www.rfc-editor.org/rfc/rfc4643#section-2.4.2",
                    input: b"AUTHINFO SASL MADE-UP-MECHANISM Y\r\n",
                    expected: b"501 command syntax error\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc4643_red_authinfo_sasl_rejects_malformed_mechanism_names() {
            // RFC 4643 section 2.2 uses SASL mechanism names. RFC 4422 section
            // 3.1 defines those names as 1 to 20 ASCII uppercase letters,
            // digits, hyphens, and underscores:
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.2
            // https://www.rfc-editor.org/rfc/rfc4422#section-3.1
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "SASL mechanism rejects lowercase",
                    reference: "RFC 4422 section 3.1 https://www.rfc-editor.org/rfc/rfc4422#section-3.1",
                    input: b"AUTHINFO SASL plain\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "SASL mechanism rejects dot",
                    reference: "RFC 4422 section 3.1 https://www.rfc-editor.org/rfc/rfc4422#section-3.1",
                    input: b"AUTHINFO SASL PLAIN.TEST\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "SASL mechanism rejects over 20 characters",
                    reference: "RFC 4422 section 3.1 https://www.rfc-editor.org/rfc/rfc4422#section-3.1",
                    input: b"AUTHINFO SASL ABCDEFGHIJKLMNOPQRSTU\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "SASL mechanism rejects lowercase with initial response",
                    reference: "RFC 4422 section 3.1 https://www.rfc-editor.org/rfc/rfc4422#section-3.1",
                    input: b"AUTHINFO SASL plain YQ==\r\n",
                    expected: b"501 command syntax error\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc4644_red_check_not_advertised_as_streaming_returns_502() {
            // RFC 4644 section 2 makes CHECK part of the streaming extension. A
            // server that does not advertise STREAMING should reject CHECK:
            // https://www.rfc-editor.org/rfc/rfc4644#section-2
            let input = b"CHECK <check@test>\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"502 command unavailable\r\n", &output, "RFC 4644");
        }

        #[tokio::test]
        async fn rfc4644_red_takethis_not_advertised_as_streaming_returns_502() {
            // RFC 4644 section 2 makes TAKETHIS part of the streaming extension.
            // Without STREAMING capability advertisement, TAKETHIS is unavailable:
            // https://www.rfc-editor.org/rfc/rfc4644#section-2
            let input = b"TAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\r\n.\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"502 command unavailable\r\n", &output, "RFC 4644");
        }

        #[tokio::test]
        async fn rfc2980_red_xhdr_uses_221_response_code() {
            // RFC 2980 section 2.1.6 specifies XHDR responses with response code
            // 221, not the RFC 3977 HDR 225 code:
            // https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6
            let (output, _) =
                run_session_with_input(test_config(), b"GROUP alt.test\r\nXHDR Subject 1\r\n")
                    .await;
            assert!(
                without_greeting(&output)
                    .windows(b"\r\n221 ".len())
                    .any(|window| window == b"\r\n221 "),
                "RFC 2980 XHDR should use 221, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_capabilities_keyword_argument_matrix() {
            // RFC 3977 section 5.2 defines CAPABILITIES [keyword]. Unknown
            // keyword arguments still receive the normal 101 capability list,
            // while non-keyword or extra arguments are syntax errors:
            // https://www.rfc-editor.org/rfc/rfc3977#section-5.2
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "CAPABILITIES unknown keyword",
                    reference: "RFC 3977 section 5.2 https://www.rfc-editor.org/rfc/rfc3977#section-5.2",
                    input: b"CAPABILITIES AUTOUPDATE\r\n",
                    expected: CAPABILITIES_RESPONSE,
                },
                ServerResponseCase {
                    name: "CAPABILITIES lowercase keyword",
                    reference: "RFC 3977 sections 5.2 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-5.2",
                    input: b"CAPABILITIES autoupdate\r\n",
                    expected: CAPABILITIES_RESPONSE,
                },
                ServerResponseCase {
                    name: "CAPABILITIES one-character non-keyword",
                    reference: "RFC 3977 sections 5.2 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                    input: b"CAPABILITIES X\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "CAPABILITIES numeric non-keyword",
                    reference: "RFC 3977 sections 5.2 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                    input: b"CAPABILITIES 123\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "CAPABILITIES extra keyword token",
                    reference: "RFC 3977 sections 5.2 and 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-5.2",
                    input: b"CAPABILITIES AUTOUPDATE EXTRA\r\n",
                    expected: b"501 command syntax error\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_date_with_arguments_returns_501() {
            // RFC 3977 sections 7.1 and 9.2 define DATE with no arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.1
            let input = b"DATE now\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_mode_transit_returns_501_not_unknown_command() {
            // RFC 3977 section 5.3 defines only MODE READER. A recognized MODE
            // command with an invalid argument is a 501 syntax error, not 500:
            // https://www.rfc-editor.org/rfc/rfc3977#section-5.3
            let input = b"MODE TRANSIT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_group_without_name_returns_501() {
            // RFC 3977 section 6.1.1 requires GROUP to carry a newsgroup name:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1
            let input = b"GROUP\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_group_sets_current_article_to_first_article() {
            // RFC 3977 section 6.1.1 requires GROUP to set the current article
            // number to the first article in a non-empty group:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1
            let input = b"GROUP alt.test\r\nARTICLE\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            let expected = b"220 1 <article.1@nntpbench.local> article follows\r\n";
            assert!(
                without_greeting(&output)
                    .windows(expected.len())
                    .any(|window| window == expected),
                "RFC 3977 current ARTICLE after GROUP should return article 1, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_missing_numeric_body_returns_423_not_body_payload() {
            // RFC 3977 section 6.2.3 defines 423 for a nonexistent article
            // number selector on BODY:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.3
            let input = b"GROUP alt.test\r\nBODY 999999\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert!(
                without_greeting(&output).ends_with(b"423 no article with that number\r\n"),
                "RFC 3977 missing numeric BODY should end with 423, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_missing_message_id_head_returns_430_not_headers() {
            // RFC 3977 section 6.2.2 defines 430 for a nonexistent message-id
            // selector on HEAD:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.2
            let input = b"HEAD <missing@nntpbench.local>\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(
                input,
                b"430 no article with that message-id\r\n",
                &output,
                "RFC 3977",
            );
        }

        #[tokio::test]
        async fn rfc3977_red_posting_disabled_rejects_pipelined_body_before_next_command() {
            // RFC 3977 section 6.3.1 requires 440 when posting is prohibited.
            // The mock server may still discard an already-sent body to recover
            // the next command, but it must not emit 340/240:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.3.1
            let input = b"POST\r\nSubject: one\r\n\r\nbody\r\n.\r\nQUIT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_eq!(
                without_greeting(&output),
                b"440 posting not permitted\r\n205 closing connection\r\n",
                "RFC 3977 disabled POST should reject without 340/240 and recover QUIT"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_ihave_rejection_consumes_pipelined_body_before_next_command() {
            // RFC 3977 section 6.3.2 permits 435 when an article is not wanted.
            // The mock server may discard an already-sent body to recover the
            // next command, but it must not emit 335/235 when rejecting:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.3.2
            let input = b"IHAVE <article@test>\r\nSubject: one\r\n\r\nbody\r\n.\r\nQUIT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_eq!(
                without_greeting(&output),
                b"435 article not wanted\r\n205 closing connection\r\n",
                "RFC 3977 rejected IHAVE should not invite transfer and should recover QUIT"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_newnews_results_depend_on_requested_datetime() {
            // RFC 3977 section 7.4 requires NEWNEWS results to be selected from
            // the supplied wildmat and date/time arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.4
            let (old_output, _) =
                run_session_with_input(test_config(), b"NEWNEWS alt.* 700101 000000 GMT\r\n").await;
            let (previous_century_output, _) =
                run_session_with_input(test_config(), b"NEWNEWS alt.* 991231 235959 GMT\r\n").await;
            let (future_output, _) =
                run_session_with_input(test_config(), b"NEWNEWS alt.* 99991231 235959 GMT\r\n")
                    .await;
            assert_eq!(
                without_greeting(&old_output),
                without_greeting(&previous_century_output),
                "RFC 3977 six-digit 99 date should map to the previous century"
            );
            assert_ne!(
                without_greeting(&old_output),
                without_greeting(&future_output),
                "RFC 3977 NEWNEWS should depend on requested date/time, not return a fixture"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_newnews_wildmat_filters_results() {
            // RFC 3977 section 7.4 requires NEWNEWS to apply the supplied
            // wildmat to the groups searched for new articles:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.4
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "NEWNEWS matching alt wildmat",
                    reference: "RFC 3977 section 7.4 https://www.rfc-editor.org/rfc/rfc3977#section-7.4",
                    input: b"NEWNEWS alt.* 20260101 000000 GMT\r\n",
                    expected: NEWNEWS_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEWNEWS nonmatching comp wildmat",
                    reference: "RFC 3977 section 7.4 https://www.rfc-editor.org/rfc/rfc3977#section-7.4",
                    input: b"NEWNEWS comp.lang.* 20260101 000000 GMT\r\n",
                    expected: NEWNEWS_EMPTY_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEWNEWS nonmatching exact wildmat",
                    reference: "RFC 3977 section 7.4 https://www.rfc-editor.org/rfc/rfc3977#section-7.4",
                    input: b"NEWNEWS comp.lang.rust 20260101 000000 GMT\r\n",
                    expected: NEWNEWS_EMPTY_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEWNEWS rightmost negated wildmat excludes alt",
                    reference: "RFC 3977 sections 4.2 and 7.4 https://www.rfc-editor.org/rfc/rfc3977#section-4.2",
                    input: b"NEWNEWS *,!alt.* 20260101 000000 GMT\r\n",
                    expected: NEWNEWS_EMPTY_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEWNEWS question wildcard matches alt",
                    reference: "RFC 3977 sections 4.2 and 7.4 https://www.rfc-editor.org/rfc/rfc3977#section-4.2",
                    input: b"NEWNEWS alt.?est 20260101 000000 GMT\r\n",
                    expected: NEWNEWS_RESPONSE,
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_help_lists_supported_commands_consistently() {
            // RFC 3977 section 7.2 says HELP returns help text for commands that
            // are understood. The command names should match the actual syntax:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.2
            let (output, _) = run_session_with_input(test_config(), b"HELP\r\n").await;
            let text = String::from_utf8_lossy(without_greeting(&output));
            for command in [
                "ARTICLE",
                "AUTHINFO",
                "BODY",
                "CAPABILITIES",
                "CHECK",
                "DATE",
                "GROUP",
                "HDR",
                "HEAD",
                "HELP",
                "IHAVE",
                "LAST",
                "LIST",
                "LISTGROUP",
                "MODE READER",
                "NEWGROUPS",
                "NEWNEWS",
                "NEXT",
                "OVER",
                "POST",
                "QUIT",
                "STARTTLS",
                "STAT",
                "TAKETHIS",
                "XHDR",
                "XOVER",
            ] {
                assert!(
                    text.contains(command),
                    "RFC 3977 HELP should list understood command {command}, got {text:?}"
                );
            }
        }

        #[tokio::test]
        async fn rfc3977_red_listgroup_unknown_group_returns_411() {
            // RFC 3977 section 6.1.2 defines 411 for LISTGROUP on an unknown
            // group name:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2
            let input = b"LISTGROUP no.such.group\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"411 no such newsgroup\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_listgroup_current_before_group_returns_412() {
            // RFC 3977 section 6.1.2 uses the current selected group when
            // LISTGROUP omits an explicit group. Without one, response 412 applies:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2
            let input = b"LISTGROUP\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_listgroup_current_range_before_group_returns_412() {
            // RFC 3977 section 6.1.2 treats a bare range as applying to the current
            // selected group, so it must also fail with 412 before GROUP:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2
            let input = b"LISTGROUP 2-3\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_listgroup_range_filters_article_numbers() {
            // RFC 3977 section 6.1.2 says LISTGROUP's optional range limits the
            // article numbers returned in the multi-line body. If the end is
            // less than the start, or the range is beyond the selected group,
            // the command still succeeds with an empty list:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "LISTGROUP explicit group range filters article numbers",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"LISTGROUP alt.test 2-3\r\n",
                    expected: LISTGROUP_2_3_RESPONSE,
                },
                ServerResponseCase {
                    name: "LISTGROUP reversed range is valid and empty",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"LISTGROUP alt.test 3-2\r\n",
                    expected: LISTGROUP_EMPTY_RESPONSE,
                },
                ServerResponseCase {
                    name: "LISTGROUP range above high water is valid and empty",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"LISTGROUP alt.test 4-\r\n",
                    expected: LISTGROUP_EMPTY_RESPONSE,
                },
                ServerResponseCase {
                    name: "LISTGROUP current group reversed range is valid and empty",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"GROUP alt.test\r\nLISTGROUP 3-2\r\n",
                    expected: b"211 3 1 3 alt.test\r\n211 3 1 3 alt.test\r\n.\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_listgroup_zero_range_returns_501() {
            // RFC 3977 sections 6.1.2 and 9.8 require valid article-number or
            // article-range syntax. Article number zero is invalid:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2
            let input = b"LISTGROUP 0\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_list_active_unknown_wildmat_filters_results() {
            // RFC 3977 section 7.6.3 requires LIST ACTIVE to apply the optional
            // wildmat pattern to the returned newsgroups:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3
            let input = b"LIST ACTIVE no.such.*\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(
                input,
                b"215 list of newsgroups follows\r\n.\r\n",
                &output,
                "RFC 3977",
            );
        }

        #[tokio::test]
        async fn rfc4643_red_authinfo_user_without_value_returns_501() {
            // RFC 4643 section 2.1 requires AUTHINFO USER to include a username
            // argument:
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.1
            let input = b"AUTHINFO USER\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 4643");
        }

        #[tokio::test]
        async fn rfc4642_red_starttls_with_arguments_returns_501() {
            // RFC 4642 section 2.2 defines STARTTLS without arguments:
            // https://www.rfc-editor.org/rfc/rfc4642#section-2.2
            let input = b"STARTTLS extra\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 4642");
        }

        #[tokio::test]
        async fn rfc4644_red_check_invalid_message_id_returns_501() {
            // RFC 4644 section 2.3 requires CHECK to carry a valid message-id:
            // https://www.rfc-editor.org/rfc/rfc4644#section-2.3
            let input = b"CHECK not-a-message-id\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 4644");
        }

        #[tokio::test]
        async fn rfc3977_red_help_with_arguments_returns_501() {
            // RFC 3977 sections 7.2 and 9.2 define HELP with no arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.2
            let input = b"HELP verbose\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_quit_with_arguments_returns_501() {
            // RFC 3977 sections 5.4 and 9.2 define QUIT with no arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-5.4
            let input = b"QUIT now\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_mode_reader_with_extra_argument_returns_501() {
            // RFC 3977 section 5.3 defines MODE READER exactly. Extra arguments
            // after READER are a syntax error:
            // https://www.rfc-editor.org/rfc/rfc3977#section-5.3
            let input = b"MODE READER extra\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_stat_current_before_group_returns_412() {
            // RFC 3977 section 6.2.4 requires current STAT retrieval to fail
            // when no group is selected:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.4
            let input = b"STAT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_head_current_before_group_returns_412() {
            // RFC 3977 section 6.2.2 requires current HEAD retrieval to fail
            // when no group is selected:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.2
            let input = b"HEAD\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_body_current_before_group_returns_412() {
            // RFC 3977 section 6.2.3 requires current BODY retrieval to fail
            // when no group is selected:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.3
            let input = b"BODY\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_head_missing_numeric_article_returns_423() {
            // RFC 3977 section 6.2.2 defines 423 when HEAD names an article
            // number that does not exist in the selected group:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.2
            let input = b"GROUP alt.test\r\nHEAD 999999\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert!(
                without_greeting(&output).ends_with(b"423 no article with that number\r\n"),
                "RFC 3977 missing numeric HEAD should end with 423, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_stat_missing_numeric_article_returns_423() {
            // RFC 3977 section 6.2.4 defines 423 when STAT names an article
            // number that does not exist in the selected group:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.4
            let input = b"GROUP alt.test\r\nSTAT 999999\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert!(
                without_greeting(&output).ends_with(b"423 no article with that number\r\n"),
                "RFC 3977 missing numeric STAT should end with 423, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_over_missing_message_id_returns_430() {
            // RFC 3977 section 8.3.2 defines 430 when an OVER message-id
            // selector is unknown:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2
            let input = b"OVER <missing@nntpbench.local>\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(
                input,
                b"430 no article with that message-id\r\n",
                &output,
                "RFC 3977",
            );
        }

        #[tokio::test]
        async fn rfc3977_red_hdr_missing_numeric_article_returns_423() {
            // RFC 3977 section 8.5.2 defines 423 for an HDR article-number
            // selector that does not exist:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2
            let input = b"GROUP alt.test\r\nHDR Subject 999999\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert!(
                without_greeting(&output).ends_with(b"423 no article with that number\r\n"),
                "RFC 3977 missing numeric HDR should end with 423, got {:?}",
                String::from_utf8_lossy(without_greeting(&output))
            );
        }

        #[tokio::test]
        async fn rfc3977_red_list_active_times_unknown_wildmat_filters_results() {
            // RFC 3977 section 7.6.4 requires LIST ACTIVE.TIMES to apply the
            // optional wildmat pattern:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.6.4
            let input = b"LIST ACTIVE.TIMES no.such.*\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(
                input,
                b"215 information follows\r\n.\r\n",
                &output,
                "RFC 3977",
            );
        }

        #[tokio::test]
        async fn rfc3977_red_list_newsgroups_unknown_wildmat_filters_results() {
            // RFC 3977 section 7.6.6 requires LIST NEWSGROUPS to apply the
            // optional wildmat pattern:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.6.6
            let input = b"LIST NEWSGROUPS no.such.*\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(
                input,
                b"215 information follows\r\n.\r\n",
                &output,
                "RFC 3977",
            );
        }

        #[tokio::test]
        async fn rfc3977_red_list_wildmat_filters_matching_groups() {
            // RFC 3977 sections 7.6.3, 7.6.4, and 7.6.6 require LIST wildmat
            // arguments to filter returned groups, not just validate syntax:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "LIST ACTIVE unmatched valid wildmat",
                    reference: "RFC 3977 section 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3",
                    input: b"LIST ACTIVE comp.lang.python\r\n",
                    expected: LIST_EMPTY_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST ACTIVE comp wildmat",
                    reference: "RFC 3977 section 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3",
                    input: b"LIST ACTIVE comp.lang.*\r\n",
                    expected: LIST_ACTIVE_COMP_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST ACTIVE alt wildmat",
                    reference: "RFC 3977 section 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3",
                    input: b"LIST ACTIVE alt.*\r\n",
                    expected: LIST_ACTIVE_ALT_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST ACTIVE rightmost negated wildmat excludes alt",
                    reference: "RFC 3977 sections 4.2 and 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-4.2",
                    input: b"LIST ACTIVE *,!alt.*\r\n",
                    expected: LIST_ACTIVE_COMP_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST ACTIVE rightmost positive wildmat re-includes alt",
                    reference: "RFC 3977 sections 4.2 and 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-4.2",
                    input: b"LIST ACTIVE !alt.*,alt.*\r\n",
                    expected: LIST_ACTIVE_ALT_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST ACTIVE question wildcard matches comp",
                    reference: "RFC 3977 sections 4.2 and 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-4.2",
                    input: b"LIST ACTIVE comp.lang.rus?\r\n",
                    expected: LIST_ACTIVE_COMP_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST ACTIVE.TIMES comp wildmat",
                    reference: "RFC 3977 section 7.6.4 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.4",
                    input: b"LIST ACTIVE.TIMES comp.lang.*\r\n",
                    expected: LIST_ACTIVE_TIMES_COMP_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST ACTIVE.TIMES alt wildmat",
                    reference: "RFC 3977 section 7.6.4 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.4",
                    input: b"LIST ACTIVE.TIMES alt.*\r\n",
                    expected: LIST_ACTIVE_TIMES_ALT_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST NEWSGROUPS comp wildmat",
                    reference: "RFC 3977 section 7.6.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.6",
                    input: b"LIST NEWSGROUPS comp.lang.*\r\n",
                    expected: LIST_NEWSGROUPS_COMP_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST NEWSGROUPS rightmost negated wildmat excludes comp",
                    reference: "RFC 3977 sections 4.2 and 7.6.6 https://www.rfc-editor.org/rfc/rfc3977#section-4.2",
                    input: b"LIST NEWSGROUPS *,!comp.*\r\n",
                    expected: LIST_NEWSGROUPS_ALT_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST NEWSGROUPS alt wildmat",
                    reference: "RFC 3977 section 7.6.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.6",
                    input: b"LIST NEWSGROUPS alt.*\r\n",
                    expected: LIST_NEWSGROUPS_ALT_RESPONSE,
                },
            ])
            .await;

            assert!(
                wildmat_matches("caf?".as_bytes(), "café".as_bytes()),
                "RFC 3977 section 4.2 requires ? to match one UTF-8 character"
            );
            assert!(
                wildmat_matches("ca*é".as_bytes(), "café".as_bytes()),
                "RFC 3977 section 4.2 requires * to match UTF-8 character-aligned text"
            );
            assert!(
                !wildmat_matches("*,!café".as_bytes(), "café".as_bytes()),
                "RFC 3977 section 4.2 applies rightmost matching pattern semantics to UTF-8"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_newgroups_results_depend_on_requested_datetime() {
            // RFC 3977 section 7.3 requires NEWGROUPS results to be selected
            // from the supplied date/time arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.3
            let (old_output, _) =
                run_session_with_input(test_config(), b"NEWGROUPS 700101 000000 GMT\r\n").await;
            let (future_output, _) =
                run_session_with_input(test_config(), b"NEWGROUPS 99991231 235959 GMT\r\n").await;
            assert_ne!(
                without_greeting(&old_output),
                without_greeting(&future_output),
                "RFC 3977 NEWGROUPS should depend on requested date/time, not return a fixture"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_newgroups_uses_active_group_line_format() {
            // RFC 3977 section 7.3 says NEWGROUPS results use the same line
            // format as LIST ACTIVE. Section 7.6.3 defines those lines as
            // newsgroup high-water low-water status:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.3
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "NEWGROUPS active-format group state",
                    reference: "RFC 3977 sections 7.3 and 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-7.3",
                    input: b"NEWGROUPS 700101 000000 GMT\r\n",
                    expected: NEWGROUPS_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEWGROUPS six-digit 99 maps to previous century",
                    reference: "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                    input: b"NEWGROUPS 991231 235959 GMT\r\n",
                    expected: NEWGROUPS_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEWGROUPS empty list after explicit future timestamp",
                    reference: "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                    input: b"NEWGROUPS 99991231 235959 GMT\r\n",
                    expected: NEWGROUPS_EMPTY_RESPONSE,
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_newgroups_invalid_timezone_returns_501() {
            // RFC 3977 section 7.3.1 permits an optional GMT token, not arbitrary
            // timezone labels:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.3.1
            let input = b"NEWGROUPS 20260101 000000 LOCAL\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[test]
        fn rfc3977_red_short_date_century_mapping_matrix() {
            // RFC 3977 section 7.3.2 maps six-digit dates to the current
            // century when yy is <= the current year, and the previous
            // century otherwise:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2
            assert_eq!(normalized_nntp_date_key(b"991231", 2026), Some(19991231));
            assert_eq!(normalized_nntp_date_key(b"260101", 2026), Some(20260101));
            assert_eq!(normalized_nntp_date_key(b"270101", 2026), Some(19270101));
            assert_eq!(normalized_nntp_date_key(b"20260101", 2026), Some(20260101));
        }

        #[tokio::test]
        async fn rfc3977_red_newnews_missing_time_returns_501() {
            // RFC 3977 section 7.4.1 requires NEWNEWS to include wildmat, date,
            // and time arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1
            let input = b"NEWNEWS comp.lang.* 20260101\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc2980_red_xover_zero_range_returns_501() {
            // RFC 2980 section 2.1.7 uses an article range for XOVER. Article
            // number zero is outside the RFC 3977 article-number grammar:
            // https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7
            let input = b"XOVER 0\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 2980");
        }

        #[tokio::test]
        async fn rfc2980_red_xhdr_invalid_header_name_returns_501() {
            // RFC 2980 section 2.1.6 requires XHDR to name a header field. A
            // trailing colon is not part of the field-name token:
            // https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6
            let input = b"XHDR Subject: 1\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 2980");
        }

        #[tokio::test]
        async fn rfc4643_red_authinfo_pass_without_value_returns_501() {
            // RFC 4643 section 2.1 requires AUTHINFO PASS to include a password
            // argument:
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.1
            let input = b"AUTHINFO PASS\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 4643");
        }

        #[tokio::test]
        async fn rfc4643_red_authinfo_sasl_without_mechanism_returns_501() {
            // RFC 4643 section 2.2 requires AUTHINFO SASL to include a SASL
            // mechanism name:
            // https://www.rfc-editor.org/rfc/rfc4643#section-2.2
            let input = b"AUTHINFO SASL\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 4643");
        }

        #[tokio::test]
        async fn rfc4644_red_takethis_invalid_message_id_returns_501() {
            // RFC 4644 section 2.4 requires TAKETHIS to carry a valid message-id
            // before the streamed article data:
            // https://www.rfc-editor.org/rfc/rfc4644#section-2.4
            let input = b"TAKETHIS not-a-message-id\r\nHeader: value\r\n\r\nbody\r\n.\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 4644");
        }

        #[tokio::test]
        async fn rfc3977_red_over_current_before_group_returns_412() {
            // RFC 3977 section 8.3.2 defines 412 when OVER without an argument is
            // used before selecting a newsgroup:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2
            let input = b"OVER\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_hdr_current_before_group_returns_412() {
            // RFC 3977 section 8.5.2 defines 412 when HDR uses the current
            // article but no newsgroup has been selected:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2
            let input = b"HDR Subject\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"412 no newsgroup selected\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_overview_group_scoped_selectors_before_group_return_412() {
            // RFC 3977 sections 8.3.2 and 8.5.2 define numeric and range
            // overview/header selectors against the currently selected group:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "OVER numeric selector before GROUP",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"OVER 1\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "OVER range selector before GROUP",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"OVER 1-3\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "XOVER range selector before GROUP",
                    reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                    input: b"XOVER 1-3\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "HDR numeric selector before GROUP",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"HDR Subject 1\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "HDR range selector before GROUP",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"HDR Subject 1-3\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "XHDR range selector before GROUP",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"XHDR Subject 1-3\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_hdr_subject_omits_header_name_from_values() {
            // RFC 3977 section 8.5 returns the requested header metadata value,
            // not a repeated "Header-Name:" prefix in each result line:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5
            let input = b"GROUP alt.test\r\nHDR Subject 1\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            let text = String::from_utf8_lossy(without_greeting(&output));
            assert!(
                text.contains("\r\n225 ") && !text.contains(" Subject:"),
                "RFC 3977 HDR Subject should return header values without field names, got {text:?}"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_hdr_message_id_returns_valid_message_id_values() {
            // RFC 3977 section 8.5 returns the requested header metadata. When
            // Message-ID is requested, each value should use the message-id grammar:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5
            let input = b"GROUP alt.test\r\nHDR Message-ID 1\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            let text = String::from_utf8_lossy(without_greeting(&output));
            assert!(
                text.ends_with("\r\n225 headers follow\r\n1 <one@example.com>\r\n.\r\n"),
                "RFC 3977 HDR Message-ID should return valid selected message-id values, got {text:?}"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_hdr_unsupported_fields_return_503() {
            // RFC 3977 section 8.5.2 permits a server to restrict HDR to a
            // limited field set. When it does, unsupported valid fields must
            // return 503 instead of successful empty or fabricated results:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2
            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "HDR unsupported header field",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR Content-Type 1\r\n",
                    expected: b"503 HDR field unavailable\r\n",
                },
                ServerResponseCase {
                    name: "HDR unsupported metadata field",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR :unknown 1\r\n",
                    expected: b"503 HDR field unavailable\r\n",
                },
                ServerResponseCase {
                    name: "XHDR unsupported header field",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR Content-Type 1\r\n",
                    expected: b"503 HDR field unavailable\r\n",
                },
                ServerResponseCase {
                    name: "XHDR unsupported metadata field",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR :unknown 1\r\n",
                    expected: b"503 HDR field unavailable\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_hdr_metadata_fields_return_metadata_values() {
            // RFC 3977 sections 8.1, 8.4, and 8.5 distinguish calculated
            // metadata items such as :bytes and :lines from header fields.
            // HDR must return the calculated metadata value, not a Subject
            // fixture or any header text:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.1
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2
            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "HDR :bytes numeric selector",
                    reference: "RFC 3977 section 8.1.1 https://www.rfc-editor.org/rfc/rfc3977#section-8.1.1",
                    input: b"GROUP alt.test\r\nHDR :bytes 1\r\n",
                    expected: HDR_BYTES_1_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR :lines numeric selector",
                    reference: "RFC 3977 section 8.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.1.2",
                    input: b"GROUP alt.test\r\nHDR :lines 2\r\n",
                    expected: HDR_LINES_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR :bytes message-id selector",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"HDR :bytes <one@example.com>\r\n",
                    expected: HDR_BYTES_MESSAGE_ID_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR :lines range selector",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR :lines 2-\r\n",
                    expected: HDR_LINES_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR :bytes numeric selector",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR :bytes 1\r\n",
                    expected: XHDR_BYTES_1_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR :lines message-id selector",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"XHDR :lines <one@example.com>\r\n",
                    expected: XHDR_LINES_MESSAGE_ID_RESPONSE,
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_overview_and_header_numeric_selectors_filter_results() {
            // RFC 3977 sections 8.3.2 and 8.5.2 require overview and header
            // metadata responses to match the supplied article-number selector:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2
            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "OVER numeric selector 2",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"GROUP alt.test\r\nOVER 2\r\n",
                    expected: OVER_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XOVER numeric selector 2",
                    reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                    input: b"GROUP alt.test\r\nXOVER 2\r\n",
                    expected: OVER_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR numeric selector 1",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR Subject 1\r\n",
                    expected: HDR_SUBJECT_1_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR numeric selector 2",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR Subject 2\r\n",
                    expected: HDR_SUBJECT_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR numeric selector 1",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR Subject 1\r\n",
                    expected: XHDR_SUBJECT_1_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR numeric selector 2",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR Subject 2\r\n",
                    expected: XHDR_SUBJECT_2_RESPONSE,
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_overview_and_header_range_selectors_filter_results() {
            // RFC 3977 sections 8.3.2 and 8.5.2 apply article-range selectors
            // to overview and header metadata. Ranges must include every
            // matching article row, and a lower bound of 2 must not return
            // article 1 rows:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2
            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "OVER range selector 1-2 returns both article rows",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"GROUP alt.test\r\nOVER 1-2\r\n",
                    expected: OVER_RANGE_RESPONSE,
                },
                ServerResponseCase {
                    name: "OVER open range selector 1- returns both article rows",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"GROUP alt.test\r\nOVER 1-\r\n",
                    expected: OVER_RANGE_RESPONSE,
                },
                ServerResponseCase {
                    name: "XOVER range selector 1-2 returns both article rows",
                    reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                    input: b"GROUP alt.test\r\nXOVER 1-2\r\n",
                    expected: OVER_RANGE_RESPONSE,
                },
                ServerResponseCase {
                    name: "OVER range selector 2-",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"GROUP alt.test\r\nOVER 2-\r\n",
                    expected: OVER_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XOVER range selector 2-",
                    reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                    input: b"GROUP alt.test\r\nXOVER 2-\r\n",
                    expected: OVER_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR Subject range selector 2-",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR Subject 2-\r\n",
                    expected: HDR_SUBJECT_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR Message-ID range selector 2-",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR Message-ID 2-\r\n",
                    expected: HDR_MESSAGE_ID_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR Subject range selector 2-",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR Subject 2-\r\n",
                    expected: XHDR_SUBJECT_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR Message-ID range selector 2-",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR Message-ID 2-\r\n",
                    expected: XHDR_MESSAGE_ID_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "OVER empty range returns 423",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"GROUP alt.test\r\nOVER 4-5\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "OVER reversed range returns 423",
                    reference: "RFC 3977 sections 6.1.2 and 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"GROUP alt.test\r\nOVER 3-1\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "XOVER empty range returns 423",
                    reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                    input: b"GROUP alt.test\r\nXOVER 4-\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "XOVER reversed range returns 423",
                    reference: "RFC 3977 section 6.1.2 and RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"GROUP alt.test\r\nXOVER 3-1\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "HDR Subject empty range returns 423",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR Subject 4-5\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "HDR Subject reversed range returns 423",
                    reference: "RFC 3977 sections 6.1.2 and 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"GROUP alt.test\r\nHDR Subject 3-1\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "HDR metadata empty range returns 423",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR :bytes 4-\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "XHDR Subject empty range returns 423",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR Subject 4-5\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "XHDR Subject reversed range returns 423",
                    reference: "RFC 3977 section 6.1.2 and RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"GROUP alt.test\r\nXHDR Subject 3-1\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
                ServerResponseCase {
                    name: "XHDR metadata empty range returns 423",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR :lines 4-\r\n",
                    expected: b"423 no articles in that range\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_overview_and_header_message_id_selectors_use_zero_article_number() {
            // RFC 3977 sections 8.3.2 and 8.5.2 require message-id selectors
            // to return the selected article. When group membership is not
            // asserted, the article number is replaced with 0:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2
            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "OVER message-id selector uses article number 0",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"OVER <one@example.com>\r\n",
                    expected: OVER_MESSAGE_ID_RESPONSE,
                },
                ServerResponseCase {
                    name: "XOVER message-id selector uses article number 0",
                    reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                    input: b"XOVER <one@example.com>\r\n",
                    expected: OVER_MESSAGE_ID_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR Subject message-id selector uses article number 0",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"HDR Subject <one@example.com>\r\n",
                    expected: HDR_SUBJECT_MESSAGE_ID_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR Message-ID message-id selector uses article number 0",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"HDR Message-ID <one@example.com>\r\n",
                    expected: HDR_MESSAGE_ID_MESSAGE_ID_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR Subject message-id selector uses article number 0",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"XHDR Subject <one@example.com>\r\n",
                    expected: XHDR_SUBJECT_MESSAGE_ID_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR Message-ID message-id selector uses article number 0",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"XHDR Message-ID <one@example.com>\r\n",
                    expected: XHDR_MESSAGE_ID_MESSAGE_ID_RESPONSE,
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc2980_red_xhdr_subject_returns_subject_values_not_message_ids() {
            // RFC 2980 section 2.1.6 returns values for the requested header.
            // XHDR Subject must not return Message-ID-shaped values:
            // https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6
            let input = b"GROUP alt.test\r\nXHDR Subject 1\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            let text = String::from_utf8_lossy(without_greeting(&output));
            assert!(
                text.contains("\r\n221 ")
                    && text.contains("1 example one\r\n")
                    && !text.contains("<one@example"),
                "RFC 2980 XHDR Subject should return subject values, got {text:?}"
            );
        }

        #[tokio::test]
        async fn rfc2980_red_xhdr_message_id_returns_valid_message_id_values() {
            // RFC 2980 section 2.1.6 returns values for the requested header.
            // Message-ID values should still satisfy the RFC 3977 message-id grammar:
            // https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6
            let input = b"GROUP alt.test\r\nXHDR Message-ID 1\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            let text = String::from_utf8_lossy(without_greeting(&output));
            assert!(
                text.contains("\r\n221 ")
                    && text.contains("1 <one@example.com>\r\n")
                    && !text.contains("2 <two@example.com>\r\n"),
                "RFC 2980 XHDR Message-ID should return valid selected message-id values, got {text:?}"
            );
        }

        #[tokio::test]
        async fn rfc3977_red_list_overview_fmt_with_arguments_returns_501() {
            // RFC 3977 section 8.4 defines LIST OVERVIEW.FMT without arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.4
            let input = b"LIST OVERVIEW.FMT extra\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_list_headers_invalid_argument_returns_501() {
            // RFC 3977 section 8.6.1 only permits the MSGID and RANGE
            // arguments for LIST HEADERS:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.6
            let input = b"LIST HEADERS Subject\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_single_response(input, b"501 command syntax error\r\n", &output, "RFC 3977");
        }

        #[tokio::test]
        async fn rfc3977_red_list_headers_accepts_msgid_and_range_arguments() {
            // RFC 3977 section 8.6.1 defines LIST HEADERS [MSGID|RANGE].
            // Section 8.6.2 says servers that do not vary HDR fields by form
            // must ignore either argument and return the same results:
            // https://www.rfc-editor.org/rfc/rfc3977#section-8.6
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "LIST HEADERS MSGID accepted",
                    reference: "RFC 3977 section 8.6.1 https://www.rfc-editor.org/rfc/rfc3977#section-8.6.1",
                    input: b"LIST HEADERS MSGID\r\n",
                    expected: LIST_HEADERS_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST HEADERS RANGE accepted",
                    reference: "RFC 3977 section 8.6.1 https://www.rfc-editor.org/rfc/rfc3977#section-8.6.1",
                    input: b"LIST HEADERS RANGE\r\n",
                    expected: LIST_HEADERS_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST HEADERS lower-case MSGID accepted",
                    reference: "RFC 3977 section 9.1 https://www.rfc-editor.org/rfc/rfc3977#section-9.1",
                    input: b"LIST HEADERS msgid\r\n",
                    expected: LIST_HEADERS_RESPONSE,
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_list_distrib_pats_response_and_syntax_matrix() {
            // RFC 3977 section 9.6 defines LIST DISTRIB.PATS content as
            // priority:wildmat:distribution lines, and section 7.6.5 defines
            // the command without arguments:
            // https://www.rfc-editor.org/rfc/rfc3977#section-7.6.5
            // https://www.rfc-editor.org/rfc/rfc3977#section-9.6
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "LIST DISTRIB.PATS returns priority wildmat distribution triples",
                    reference: "RFC 3977 section 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                    input: b"LIST DISTRIB.PATS\r\n",
                    expected: LIST_DISTRIB_PATS_RESPONSE,
                },
                ServerResponseCase {
                    name: "LIST DISTRIB.PATS rejects arguments",
                    reference: "RFC 3977 section 7.6.5 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.5",
                    input: b"LIST DISTRIB.PATS world\r\n",
                    expected: b"501 command syntax error\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_too_long_command_line_returns_501() {
            // RFC 3977 section 3.1 limits command lines to 512 octets including
            // CRLF. An overlong line should get a 501 response, not a silent close:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let mut input = b"DATE ".to_vec();
            input.extend(std::iter::repeat_n(b'x', MAX_COMMAND_LINE_BYTES + 1));
            input.extend_from_slice(b"\r\n");
            let (output, error) =
                run_session_with_input_allowing_server_error(test_config(), &input).await;
            assert!(
                error.is_none() && without_greeting(&output) == b"501 command line too long\r\n",
                "RFC 3977 overlong command should produce a 501 response, got error {:?} and output {:?}",
                error,
                String::from_utf8_lossy(&output)
            );
        }

        #[tokio::test]
        async fn rfc3977_red_bare_lf_command_line_returns_501_and_recovers() {
            // RFC 3977 section 3.1 requires command lines to end in CRLF. A bare
            // LF must be rejected as syntax and must not be recovered as DATE:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let input = b"DATE\nQUIT\r\n";
            let (output, error) =
                run_session_with_input_allowing_server_error(test_config(), input).await;
            assert!(
                error.is_none()
                    && without_greeting(&output)
                        == b"501 command syntax error\r\n205 closing connection\r\n",
                "RFC 3977 bare-LF command line should be rejected before QUIT, got error {:?} and output {:?}",
                error,
                String::from_utf8_lossy(&output)
            );
        }

        #[test]
        fn rfc3977_red_article_parser_rejects_stat_multiline_terminator() {
            // RFC 3977 section 6.2.4 defines STAT as a single-line response
            // with no multi-line data block:
            // https://www.rfc-editor.org/rfc/rfc3977#section-6.2.4
            assert!(Article::parse(b"223 1 <stat@test> article exists\r\n.\r\n").is_err());
        }

        #[test]
        fn rfc3977_red_request_line_accepts_rfc_ws_and_eol_matrix() {
            assert_red_request_line_known_cases(&[
                (
                    "GROUP accepts TAB as WS separator",
                    b"GROUP\talt.test\r\n",
                    RequestKind::Group,
                    "RFC 3977 sections 9.2 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                ),
                (
                    "HEAD accepts repeated SP as WS separator",
                    b"HEAD  1\r\n",
                    RequestKind::Head,
                    "RFC 3977 sections 9.2 and 6.2.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                ),
                (
                    "DATE accepts EOL trailing whitespace",
                    b"DATE \t\r\n",
                    RequestKind::Date,
                    "RFC 3977 sections 9.2 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                ),
                (
                    "LIST accepts TAB before subcommand",
                    b"LIST\tACTIVE\r\n",
                    RequestKind::ListActive,
                    "RFC 3977 sections 7.6 and 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                ),
                (
                    "NEWNEWS accepts mixed WS separators",
                    b"NEWNEWS\talt.*  20260101\t000000 GMT\r\n",
                    RequestKind::NewNews,
                    "RFC 3977 sections 7.4.1 and 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                ),
                (
                    "AUTHINFO accepts TAB between subcommand and value",
                    b"AUTHINFO USER\tbench\r\n",
                    RequestKind::AuthInfoUser,
                    "RFC 4643 section 2 and RFC 3977 section 9.2 https://www.rfc-editor.org/rfc/rfc4643#section-2",
                ),
            ]);
        }

        #[test]
        fn rfc3977_red_request_line_rejects_invalid_command_shapes_matrix() {
            assert_red_request_line_unknown_cases(&[
                (
                    "LAST argument",
                    b"LAST 1\r\n",
                    "RFC 3977 section 6.1.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.3",
                ),
                (
                    "NEXT argument",
                    b"NEXT 1\r\n",
                    "RFC 3977 section 6.1.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4",
                ),
                (
                    "BODY extra argument",
                    b"BODY 1 2\r\n",
                    "RFC 3977 section 6.2.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.3",
                ),
                (
                    "HEAD extra argument",
                    b"HEAD <one@test> extra\r\n",
                    "RFC 3977 section 6.2.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.2",
                ),
                (
                    "STAT extra argument",
                    b"STAT 1 2\r\n",
                    "RFC 3977 section 6.2.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.4",
                ),
                (
                    "IHAVE without message-id",
                    b"IHAVE\r\n",
                    "RFC 3977 section 6.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.3.2",
                ),
                (
                    "CHECK without message-id",
                    b"CHECK\r\n",
                    "RFC 4644 section 2.3 https://www.rfc-editor.org/rfc/rfc4644#section-2.3",
                ),
                (
                    "TAKETHIS without message-id",
                    b"TAKETHIS\r\n",
                    "RFC 4644 section 2.4 https://www.rfc-editor.org/rfc/rfc4644#section-2.4",
                ),
                (
                    "LIST unknown keyword",
                    b"LIST FROBNICATE\r\n",
                    "RFC 3977 section 7.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.6",
                ),
                (
                    "LIST ACTIVE extra argument",
                    b"LIST ACTIVE comp.* extra\r\n",
                    "RFC 3977 section 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3",
                ),
                (
                    "AUTHINFO USER extra argument",
                    b"AUTHINFO USER bench extra\r\n",
                    "RFC 4643 section 2.1 https://www.rfc-editor.org/rfc/rfc4643#section-2.1",
                ),
            ]);
        }

        #[tokio::test]
        async fn rfc3977_red_server_selector_and_state_response_matrix() {
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "BODY invalid message-id",
                    reference: "RFC 3977 sections 6.2.3 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.3",
                    input: b"BODY <missing-at-sign>\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "HEAD zero selector",
                    reference: "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                    input: b"HEAD 0\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "STAT zero selector",
                    reference: "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                    input: b"STAT 0\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "XOVER current before GROUP",
                    reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                    input: b"XOVER\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "XHDR current before GROUP",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"XHDR Subject\r\n",
                    expected: b"412 no newsgroup selected\r\n",
                },
                ServerResponseCase {
                    name: "BODY extra argument",
                    reference: "RFC 3977 section 6.2.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.3",
                    input: b"BODY 1 2\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "HEAD extra argument",
                    reference: "RFC 3977 section 6.2.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.2",
                    input: b"HEAD 1 2\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "STAT extra argument",
                    reference: "RFC 3977 section 6.2.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.4",
                    input: b"STAT 1 2\r\n",
                    expected: b"501 command syntax error\r\n",
                },
            ])
            .await;

            assert_red_server_response_tail_cases(&[
                ServerResponseCase {
                    name: "OVER after GROUP uses first article",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"GROUP alt.test\r\nOVER\r\n",
                    expected: OVER_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR after GROUP uses first article",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nHDR Subject\r\n",
                    expected: HDR_SUBJECT_1_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR after GROUP uses first article",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nXHDR Subject\r\n",
                    expected: XHDR_SUBJECT_1_RESPONSE,
                },
                ServerResponseCase {
                    name: "OVER current article follows ARTICLE 2",
                    reference: "RFC 3977 section 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                    input: b"GROUP alt.test\r\nARTICLE 2\r\nOVER\r\n",
                    expected: OVER_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XOVER current article follows ARTICLE 2",
                    reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                    input: b"GROUP alt.test\r\nARTICLE 2\r\nXOVER\r\n",
                    expected: OVER_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR current article follows ARTICLE 2",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nARTICLE 2\r\nHDR Subject\r\n",
                    expected: HDR_SUBJECT_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "HDR metadata current article follows ARTICLE 2",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    input: b"GROUP alt.test\r\nARTICLE 2\r\nHDR :bytes\r\n",
                    expected: HDR_BYTES_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR current article follows ARTICLE 2",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nARTICLE 2\r\nXHDR Subject\r\n",
                    expected: XHDR_SUBJECT_2_RESPONSE,
                },
                ServerResponseCase {
                    name: "XHDR metadata current article follows ARTICLE 2",
                    reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                    input: b"GROUP alt.test\r\nARTICLE 2\r\nXHDR :bytes\r\n",
                    expected: XHDR_BYTES_2_RESPONSE,
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_server_list_and_discovery_syntax_matrix() {
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "LIST unknown keyword",
                    reference: "RFC 3977 section 7.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.6",
                    input: b"LIST FROBNICATE\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "LIST ACTIVE extra argument",
                    reference: "RFC 3977 section 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3",
                    input: b"LIST ACTIVE comp.* extra\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "LIST ACTIVE.TIMES invalid wildmat",
                    reference: "RFC 3977 sections 4 and 7.6.4 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.4",
                    input: b"LIST ACTIVE.TIMES !\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "LIST NEWSGROUPS invalid wildmat",
                    reference: "RFC 3977 sections 4 and 7.6.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.6",
                    input: b"LIST NEWSGROUPS !\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "NEWGROUPS without arguments",
                    reference: "RFC 3977 section 7.3.1 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.1",
                    input: b"NEWGROUPS\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "NEWGROUPS missing time",
                    reference: "RFC 3977 section 7.3.1 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.1",
                    input: b"NEWGROUPS 20260101\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "NEWGROUPS invalid time",
                    reference: "RFC 3977 sections 7.3.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.1",
                    input: b"NEWGROUPS 20260101 246060 GMT\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "NEWGROUPS accepts leap second",
                    reference: "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                    input: b"NEWGROUPS 20260101 235960 GMT\r\n",
                    expected: NEWGROUPS_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEWNEWS invalid date",
                    reference: "RFC 3977 sections 7.4.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1",
                    input: b"NEWNEWS comp.lang.* 20260230 000000 GMT\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "NEWNEWS invalid time",
                    reference: "RFC 3977 sections 7.4.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1",
                    input: b"NEWNEWS comp.lang.* 20260101 246060 GMT\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "NEWNEWS accepts leap second",
                    reference: "RFC 3977 sections 7.3.2 and 7.4.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                    input: b"NEWNEWS comp.lang.* 20260101 235960 GMT\r\n",
                    expected: NEWNEWS_EMPTY_RESPONSE,
                },
                ServerResponseCase {
                    name: "NEWNEWS invalid timezone",
                    reference: "RFC 3977 section 7.4.1 https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1",
                    input: b"NEWNEWS comp.lang.* 20260101 000000 LOCAL\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "LAST with argument",
                    reference: "RFC 3977 section 6.1.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.3",
                    input: b"LAST 1\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "NEXT with argument",
                    reference: "RFC 3977 section 6.1.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4",
                    input: b"NEXT 1\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "GROUP invalid name",
                    reference: "RFC 3977 section 6.1.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1",
                    input: b"GROUP alt.*\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "LISTGROUP invalid name",
                    reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                    input: b"LISTGROUP alt.*\r\n",
                    expected: b"501 command syntax error\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc3977_red_server_post_auth_and_streaming_syntax_matrix() {
            assert_red_server_response_cases(&[
                ServerResponseCase {
                    name: "POST with argument",
                    reference: "RFC 3977 section 6.3.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.3.1",
                    input: b"POST extra\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "IHAVE without message-id",
                    reference: "RFC 3977 section 6.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.3.2",
                    input: b"IHAVE\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "IHAVE invalid message-id",
                    reference: "RFC 3977 sections 6.3.2 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-6.3.2",
                    input: b"IHAVE not-a-message-id\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "CHECK without message-id",
                    reference: "RFC 4644 section 2.3 https://www.rfc-editor.org/rfc/rfc4644#section-2.3",
                    input: b"CHECK\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "TAKETHIS without message-id",
                    reference: "RFC 4644 section 2.4 https://www.rfc-editor.org/rfc/rfc4644#section-2.4",
                    input: b"TAKETHIS\r\n.\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "AUTHINFO USER extra argument",
                    reference: "RFC 4643 section 2.1 https://www.rfc-editor.org/rfc/rfc4643#section-2.1",
                    input: b"AUTHINFO USER bench extra\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "AUTHINFO PASS extra argument",
                    reference: "RFC 4643 section 2.1 https://www.rfc-editor.org/rfc/rfc4643#section-2.1",
                    input: b"AUTHINFO PASS bench extra\r\n",
                    expected: b"501 command syntax error\r\n",
                },
                ServerResponseCase {
                    name: "AUTHINFO unknown subcommand",
                    reference: "RFC 4643 sections 2.1 and 2.2 https://www.rfc-editor.org/rfc/rfc4643#section-2",
                    input: b"AUTHINFO FROBNICATE bench\r\n",
                    expected: b"501 command syntax error\r\n",
                },
            ])
            .await;
        }

        #[tokio::test]
        async fn rfc4642_red_starttls_with_buffered_plaintext_still_returns_502() {
            // RFC 4642 section 2.2 requires a 382 response to begin actual TLS
            // negotiation. A server without TLS support must not emit 382 just
            // because another plaintext command is already buffered:
            // https://www.rfc-editor.org/rfc/rfc4642#section-2.2
            let input = b"STARTTLS\r\nQUIT\r\n";
            let (output, _) = run_session_with_input(test_config(), input).await;
            assert_eq!(
                without_greeting(&output),
                b"502 command unavailable\r\n205 closing connection\r\n",
                "RFC 4642 unavailable STARTTLS must not emit 382 without TLS support"
            );
        }

        mod value_validation {
            use super::*;

            #[test]
            fn rfc3977_red_value_validation_matrix() {
                assert_red_invalid_values(&[
                    (
                        "message-id requires @",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        MessageId::from_borrowed("<local-only>").is_err(),
                    ),
                    (
                        "message-id rejects empty left side",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        MessageId::from_borrowed("<@example.com>").is_err(),
                    ),
                    (
                        "message-id rejects empty right side",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        MessageId::from_borrowed("<local@>").is_err(),
                    ),
                    (
                        "message-id rejects multiple @ signs",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        MessageId::from_borrowed("<local@@example.com>").is_err(),
                    ),
                    (
                        "message-id rejects empty domain label",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        MessageId::from_borrowed("<local@example..com>").is_err(),
                    ),
                    (
                        "wrapped message-id rejects invalid unbracketed value",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        MessageId::from_str_or_wrap("@example.com").is_err(),
                    ),
                    (
                        "HDR metadata name accepts leading colon",
                        "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                        HeaderName::from_borrowed(":bytes").is_ok(),
                    ),
                    (
                        "HDR metadata name rejects bare colon",
                        "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                        HeaderName::from_borrowed(":").is_err(),
                    ),
                    (
                        "AUTHINFO value rejects space",
                        "RFC 4643 section 2.3 https://www.rfc-editor.org/rfc/rfc4643#section-2.3",
                        AuthInfoValue::from_borrowed("bench user").is_err(),
                    ),
                    (
                        "AUTHINFO value rejects tab",
                        "RFC 4643 section 2.3 https://www.rfc-editor.org/rfc/rfc4643#section-2.3",
                        AuthInfoValue::from_borrowed("bench\tuser").is_err(),
                    ),
                    (
                        "group name rejects comma separator",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        GroupName::from_borrowed("alt.test,comp.test").is_err(),
                    ),
                    (
                        "group name rejects colon",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        GroupName::from_borrowed("alt:test").is_err(),
                    ),
                    (
                        "group name rejects slash",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        GroupName::from_borrowed("alt/test").is_err(),
                    ),
                    (
                        "group name rejects @",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        GroupName::from_borrowed("alt@test").is_err(),
                    ),
                    (
                        "group name rejects !",
                        "RFC 3977 sections 4.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        GroupName::from_borrowed("alt!test").is_err(),
                    ),
                    (
                        "article selector rejects zero",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleSelector::from_borrowed("0").is_err(),
                    ),
                    (
                        "article reference selector rejects leading-zero number",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleRef::from_selector("0001").is_err(),
                    ),
                    (
                        "article selector rejects zero range start",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleSelector::from_borrowed("0-10").is_err(),
                    ),
                    (
                        "article selector rejects zero range end",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleSelector::from_borrowed("1-0").is_err(),
                    ),
                    (
                        "article selector rejects invalid message-id shape",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleSelector::from_borrowed("<missing-at-sign>").is_err(),
                    ),
                    (
                        "article selector rejects bare atom",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleSelector::from_borrowed("not-a-selector").is_err(),
                    ),
                    (
                        "article selector accepts reversed range as empty",
                        "RFC 3977 sections 6.1.2, 8.3.2, and 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                        ArticleSelector::from_borrowed("10-1").is_ok(),
                    ),
                    (
                        "article selector rejects double hyphen range",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleSelector::from_borrowed("1--10").is_err(),
                    ),
                    (
                        "article selector rejects open-start range",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleSelector::from_borrowed("-10").is_err(),
                    ),
                    (
                        "article selector rejects over max article number",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ArticleSelector::from_borrowed("2147483648").is_err(),
                    ),
                    (
                        "LISTGROUP range accepts reversed range as empty",
                        "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                        ListGroupRange::from_borrowed("10-1").is_ok(),
                    ),
                    (
                        "LISTGROUP range rejects leading-zero number",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ListGroupRange::from_borrowed("0001").is_err(),
                    ),
                    (
                        "LISTGROUP range rejects leading-zero endpoint",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        ListGroupRange::from_borrowed("1-0002").is_err(),
                    ),
                    (
                        "date accepts earliest RFC 3977 four-digit year",
                        "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                        NntpDate::from_borrowed("19000101").is_ok(),
                    ),
                    (
                        "date accepts far-future RFC 3977 four-digit year",
                        "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                        NntpDate::from_borrowed("99991231").is_ok(),
                    ),
                    (
                        "date rejects pre-1900 four-digit year",
                        "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                        NntpDate::from_borrowed("18991231").is_err(),
                    ),
                    (
                        "date rejects zero four-digit year",
                        "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                        NntpDate::from_borrowed("00010101").is_err(),
                    ),
                    (
                        "date rejects impossible February day",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        NntpDate::from_borrowed("20260230").is_err(),
                    ),
                    (
                        "date rejects April 31",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        NntpDate::from_borrowed("20260431").is_err(),
                    ),
                    (
                        "date rejects non-leap February 29",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        NntpDate::from_borrowed("20230229").is_err(),
                    ),
                    (
                        "short date rejects non-leap February 29",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        NntpDate::from_borrowed("230229").is_err(),
                    ),
                    (
                        "date rejects November 31",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        NntpDate::from_borrowed("20261131").is_err(),
                    ),
                    (
                        "time accepts leap second before midnight",
                        "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                        NntpTime::from_borrowed("235960").is_ok(),
                    ),
                    (
                        "time accepts leap second",
                        "RFC 3977 section 7.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.2",
                        NntpTime::from_borrowed("000060").is_ok(),
                    ),
                    (
                        "wildmat rejects ! outside negation marker",
                        "RFC 3977 section 4.1 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        Wildmat::from_borrowed("alt!test").is_err(),
                    ),
                    (
                        "wildmat rejects [",
                        "RFC 3977 section 4.1 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        Wildmat::from_borrowed("alt[test").is_err(),
                    ),
                    (
                        "wildmat rejects \\",
                        "RFC 3977 section 4.1 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        Wildmat::from_borrowed(r"alt\test").is_err(),
                    ),
                    (
                        "wildmat rejects ]",
                        "RFC 3977 section 4.1 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        Wildmat::from_borrowed("alt]test").is_err(),
                    ),
                    (
                        "wildmat accepts ! as comma pattern negation marker",
                        "RFC 3977 sections 4.1 and 4.2 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        Wildmat::from_borrowed("alt.*,!alt.test").is_ok(),
                    ),
                    (
                        "wildmat accepts UTF-8 non-ASCII exact text",
                        "RFC 3977 sections 4.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        Wildmat::from_borrowed("comp.lang.rust.é").is_ok(),
                    ),
                ]);
            }
        }

        mod request_line_grammar {
            use super::*;

            #[test]
            fn rfc3977_red_request_line_valid_ws_matrix() {
                assert_red_request_line_known_cases(&[
                    (
                        "GROUP tab separator",
                        b"GROUP\talt.test\r\n",
                        RequestKind::Group,
                        "RFC 3977 section 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                    ),
                    (
                        "DATE trailing EOL spaces",
                        b"DATE  \r\n",
                        RequestKind::Date,
                        "RFC 3977 section 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                    ),
                    (
                        "GROUP multiple token separators",
                        b"GROUP  alt.test\r\n",
                        RequestKind::Group,
                        "RFC 3977 section 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                    ),
                    (
                        "BODY tab separator",
                        b"BODY\t1\r\n",
                        RequestKind::Body,
                        "RFC 3977 section 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                    ),
                    (
                        "HEAD double space before selector",
                        b"HEAD  1\r\n",
                        RequestKind::Head,
                        "RFC 3977 section 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                    ),
                    (
                        "GROUP trailing EOL space after argument",
                        b"GROUP alt.test \r\n",
                        RequestKind::Group,
                        "RFC 3977 section 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                    ),
                    (
                        "LIST tab separator",
                        b"LIST\tACTIVE\r\n",
                        RequestKind::ListActive,
                        "RFC 3977 sections 7.6 and 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                    ),
                    (
                        "AUTHINFO tab separator",
                        b"AUTHINFO\tUSER bench\r\n",
                        RequestKind::AuthInfoUser,
                        "RFC 4643 section 2 and RFC 3977 section 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.2",
                    ),
                ]);
            }

            #[test]
            fn rfc3977_red_request_line_grammar_matrix() {
                assert_red_request_line_unknown_cases(&[
                    (
                        "ARTICLE extra argument",
                        b"ARTICLE <a@b> extra\r\n",
                        "RFC 3977 sections 6.2.1.1 and 9.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1.1",
                    ),
                    (
                        "ARTICLE zero selector",
                        b"ARTICLE 0\r\n",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                    ),
                    (
                        "GROUP non-UTF-8 argument",
                        b"GROUP alt.\xff\r\n",
                        "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                    ),
                    (
                        "HDR non-UTF-8 header name",
                        b"HDR Subj\xffct 1\r\n",
                        "RFC 3977 sections 8.5.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.1",
                    ),
                    (
                        "NEWNEWS non-UTF-8 wildmat",
                        b"NEWNEWS alt.\xff 20260101 000000\r\n",
                        "RFC 3977 sections 7.4.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1",
                    ),
                    (
                        "GROUP excludes ! from newsgroup-name",
                        b"GROUP alt!test\r\n",
                        "RFC 3977 sections 4.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                    ),
                    (
                        "LIST ACTIVE excludes [ from wildmat",
                        b"LIST ACTIVE alt[test\r\n",
                        "RFC 3977 sections 4.1 and 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                    ),
                    (
                        "NEWNEWS excludes \\ from wildmat",
                        b"NEWNEWS alt\\test 20260101 000000\r\n",
                        "RFC 3977 sections 4.1 and 7.4.1 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                    ),
                    (
                        "NEWGROUPS extra argument",
                        b"NEWGROUPS 20260101 000000 GMT extra\r\n",
                        "RFC 3977 section 7.3.1 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.1",
                    ),
                    (
                        "NEWNEWS extra argument",
                        b"NEWNEWS alt.* 20260101 000000 GMT extra\r\n",
                        "RFC 3977 section 7.4.1 https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1",
                    ),
                ]);
            }
        }

        mod server_syntax_responses {
            use super::*;

            #[tokio::test]
            async fn rfc3977_red_server_syntax_response_matrix() {
                assert_red_server_response_cases(&[
                    ServerResponseCase {
                        name: "LIST unknown keyword with trailing token",
                        reference: "RFC 3977 section 7.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.6",
                        input: b"LIST FROBNICATE extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "GROUP non-UTF-8 argument",
                        reference: "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        input: b"GROUP alt.\xff\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "HDR non-UTF-8 header name",
                        reference: "RFC 3977 sections 8.5.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.1",
                        input: b"HDR Subj\xffct 1\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "NEWNEWS non-UTF-8 wildmat",
                        reference: "RFC 3977 sections 7.4.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1",
                        input: b"NEWNEWS alt.\xff 20260101 000000\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "GROUP excludes ! from newsgroup-name",
                        reference: "RFC 3977 sections 4.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        input: b"GROUP alt!test\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "LIST ACTIVE excludes [ from wildmat",
                        reference: "RFC 3977 sections 4.1 and 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        input: b"LIST ACTIVE alt[test\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "NEWNEWS excludes \\ from wildmat",
                        reference: "RFC 3977 sections 4.1 and 7.4.1 https://www.rfc-editor.org/rfc/rfc3977#section-4.1",
                        input: b"NEWNEWS alt\\test 20260101 000000\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "LIST ACTIVE.TIMES extra argument",
                        reference: "RFC 3977 section 7.6.4 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.4",
                        input: b"LIST ACTIVE.TIMES comp.* extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "LIST NEWSGROUPS extra argument",
                        reference: "RFC 3977 section 7.6.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.6",
                        input: b"LIST NEWSGROUPS comp.* extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "NEWGROUPS extra argument",
                        reference: "RFC 3977 section 7.3.1 https://www.rfc-editor.org/rfc/rfc3977#section-7.3.1",
                        input: b"NEWGROUPS 20260101 000000 GMT extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "NEWNEWS extra argument",
                        reference: "RFC 3977 section 7.4.1 https://www.rfc-editor.org/rfc/rfc3977#section-7.4.1",
                        input: b"NEWNEWS alt.* 20260101 000000 GMT extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "OVER extra argument",
                        reference: "RFC 3977 section 8.3.1 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.1",
                        input: b"OVER 1-2 extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "HDR extra argument",
                        reference: "RFC 3977 section 8.5.1 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.1",
                        input: b"HDR Subject 1 extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "XOVER extra argument",
                        reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                        input: b"XOVER 1-2 extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "XHDR extra argument",
                        reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                        input: b"XHDR Subject 1 extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "CHECK extra argument",
                        reference: "RFC 4644 section 2.3 https://www.rfc-editor.org/rfc/rfc4644#section-2.3",
                        input: b"CHECK <check@test> extra\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                    ServerResponseCase {
                        name: "TAKETHIS extra argument",
                        reference: "RFC 4644 section 2.4 https://www.rfc-editor.org/rfc/rfc4644#section-2.4",
                        input: b"TAKETHIS <take@test> extra\r\nHeader: value\r\n\r\nbody\r\n.\r\n",
                        expected: b"501 command syntax error\r\n",
                    },
                ])
                .await;
            }
        }

        mod response_frame_validation {
            use super::*;

            #[test]
            fn rfc3977_red_response_frame_valid_shape_matrix() {
                assert_red_response_frame_valid_cases(&[ResponseFrameCase {
                    name: "HDR accepts omitted space for empty field content",
                    reference: "RFC 3977 section 8.5.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.2",
                    kind: RequestKind::Hdr,
                    frame: b"225 headers follow\r\n1\r\n2 value\r\n.\r\n",
                }]);
            }

            #[test]
            fn rfc3977_red_response_frame_validation_matrix() {
                assert_red_response_frame_invalid_cases(&[
                    ResponseFrameCase {
                        name: "response line requires separator after status",
                        reference: "RFC 3977 sections 3.1 and 9.4 https://www.rfc-editor.org/rfc/rfc3977#section-9.4",
                        kind: RequestKind::Body,
                        frame: b"222body follows\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "STAT rejects ARTICLE success code",
                        reference: "RFC 3977 sections 6.2.1 and 6.2.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.2",
                        kind: RequestKind::Stat,
                        frame:
                            b"220 1 <a@test> article follows\r\nSubject: one\r\n\r\nbody\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "STAT rejects dot block",
                        reference: "RFC 3977 section 6.2.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.4",
                        kind: RequestKind::Stat,
                        frame: b"223 1 <stat@test> article exists\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "STAT rejects missing article number",
                        reference: "RFC 3977 section 6.2.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.4",
                        kind: RequestKind::Stat,
                        frame: b"223 <stat@test> article exists\r\n",
                    },
                    ResponseFrameCase {
                        name: "LAST rejects malformed message-id argument",
                        reference: "RFC 3977 section 6.1.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.3",
                        kind: RequestKind::Last,
                        frame: b"223 1 stat@test article exists\r\n",
                    },
                    ResponseFrameCase {
                        name: "NEXT rejects non-numeric article number",
                        reference: "RFC 3977 section 6.1.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.4",
                        kind: RequestKind::Next,
                        frame: b"223 next <stat@test> article exists\r\n",
                    },
                    ResponseFrameCase {
                        name: "NEXT rejects overlong article number",
                        reference: "RFC 3977 section 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        kind: RequestKind::Next,
                        frame: b"223 12345678901234567 <stat@test> article exists\r\n",
                    },
                    ResponseFrameCase {
                        name: "ARTICLE rejects STAT success code",
                        reference: "RFC 3977 sections 6.2.1 and 6.2.4 https://www.rfc-editor.org/rfc/rfc3977#section-6.2",
                        kind: RequestKind::Article,
                        frame: b"223 1 <a@test> article exists\r\n",
                    },
                    ResponseFrameCase {
                        name: "ARTICLE rejects missing article number",
                        reference: "RFC 3977 section 6.2.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.1",
                        kind: RequestKind::Article,
                        frame: b"220 <a@test> article follows\r\nSubject: bad\r\n\r\nbody\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "ARTICLE rejects overlong article number",
                        reference: "RFC 3977 sections 6.2.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        kind: RequestKind::Article,
                        frame: b"220 12345678901234567 <a@test> article follows\r\nSubject: bad\r\n\r\nbody\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "HEAD rejects non-numeric article number",
                        reference: "RFC 3977 section 6.2.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.2",
                        kind: RequestKind::Head,
                        frame: b"221 one <a@test> article retrieved\r\nSubject: bad\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "BODY rejects malformed message-id argument",
                        reference: "RFC 3977 section 6.2.3 https://www.rfc-editor.org/rfc/rfc3977#section-6.2.3",
                        kind: RequestKind::Body,
                        frame: b"222 1 a@test body follows\r\nbody\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "HELP rejects LIST success code",
                        reference: "RFC 3977 sections 7.2 and 7.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.2",
                        kind: RequestKind::Help,
                        frame: b"215 list follows\r\nalt.test 1 1 y\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST rejects CAPABILITIES success code",
                        reference: "RFC 3977 sections 5.2 and 7.6 https://www.rfc-editor.org/rfc/rfc3977#section-5.2",
                        kind: RequestKind::List,
                        frame: b"101 Capability list:\r\nVERSION 2\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "DATE rejects multiline payload",
                        reference: "RFC 3977 section 7.1 https://www.rfc-editor.org/rfc/rfc3977#section-7.1",
                        kind: RequestKind::Date,
                        frame: b"111 20260602120000\r\nextra\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "DATE rejects malformed timestamp",
                        reference: "RFC 3977 sections 7.1 and 7.5 https://www.rfc-editor.org/rfc/rfc3977#section-7.1",
                        kind: RequestKind::Date,
                        frame: b"111 20260230000000\r\n",
                    },
                    ResponseFrameCase {
                        name: "DATE rejects non-timestamp response text",
                        reference: "RFC 3977 sections 7.1 and 9.4.2 https://www.rfc-editor.org/rfc/rfc3977#section-7.1",
                        kind: RequestKind::Date,
                        frame: b"111 server date follows\r\n",
                    },
                    ResponseFrameCase {
                        name: "generic 401 rejects missing capability label",
                        reference: "RFC 3977 sections 3.2.1 and 9.4.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.4.2",
                        kind: RequestKind::Article,
                        frame: b"401\r\n",
                    },
                    ResponseFrameCase {
                        name: "generic 401 rejects invalid capability label",
                        reference: "RFC 3977 sections 3.2.1 and 9.4.2 https://www.rfc-editor.org/rfc/rfc3977#section-9.4.2",
                        kind: RequestKind::Capabilities,
                        frame: b"401 READER_required\r\n",
                    },
                    ResponseFrameCase {
                        name: "GROUP rejects multiline payload",
                        reference: "RFC 3977 section 6.1.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1",
                        kind: RequestKind::Group,
                        frame: b"211 3 1 3 alt.test\r\n1\r\n2\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "GROUP rejects missing count/low/high/group arguments",
                        reference: "RFC 3977 section 6.1.1 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1",
                        kind: RequestKind::Group,
                        frame: b"211 group selected\r\n",
                    },
                    ResponseFrameCase {
                        name: "GROUP rejects invalid group name argument",
                        reference: "RFC 3977 sections 6.1.1 and 9.4.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.1",
                        kind: RequestKind::Group,
                        frame: b"211 3 1 3 alt!test\r\n",
                    },
                    ResponseFrameCase {
                        name: "GROUP rejects overlong high-water article number",
                        reference: "RFC 3977 sections 6.1.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        kind: RequestKind::Group,
                        frame: b"211 3 1 12345678901234567 alt.test\r\n",
                    },
                    ResponseFrameCase {
                        name: "LISTGROUP rejects invalid article count argument",
                        reference: "RFC 3977 sections 6.1.2 and 9.4.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                        kind: RequestKind::ListGroup,
                        frame: b"211 three 1 3 alt.test\r\n1\r\n2\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LISTGROUP rejects extra space between required arguments",
                        reference: "RFC 3977 sections 3.2 and 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-3.2",
                        kind: RequestKind::ListGroup,
                        frame: b"211 3  1 3 alt.test\r\n1\r\n2\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LISTGROUP rejects non-numeric article line",
                        reference: "RFC 3977 section 6.1.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.1.2",
                        kind: RequestKind::ListGroup,
                        frame: b"211 3 1 3 alt.test\r\none\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LISTGROUP rejects overlong article number line",
                        reference: "RFC 3977 sections 6.1.2 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        kind: RequestKind::ListGroup,
                        frame: b"211 3 1 3 alt.test\r\n12345678901234567\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "NEWNEWS rejects non-message-id article line",
                        reference: "RFC 3977 sections 7.4 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-7.4",
                        kind: RequestKind::NewNews,
                        frame: b"230 list of new articles follows\r\none@alt.test\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST ACTIVE rejects invalid group name line",
                        reference: "RFC 3977 section 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3",
                        kind: RequestKind::ListActive,
                        frame: b"215 list of newsgroups follows\r\nalt.* 3 1 y\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST ACTIVE rejects invalid posting status token",
                        reference: "RFC 3977 sections 7.6.3 and 9.7 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.3",
                        kind: RequestKind::ListActive,
                        frame: b"215 list of newsgroups follows\r\nalt.test 3 1 open\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST ACTIVE rejects overlong high-water article number",
                        reference: "RFC 3977 sections 7.6.3 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        kind: RequestKind::ListActive,
                        frame: b"215 list of newsgroups follows\r\nalt.test 12345678901234567 1 y\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST ACTIVE rejects tab field separator",
                        reference: "RFC 3977 sections 7.6.3 and 9.4.3 https://www.rfc-editor.org/rfc/rfc3977#section-9.4.3",
                        kind: RequestKind::ListActive,
                        frame: b"215 list of newsgroups follows\r\nalt.test\t3 1 y\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST ACTIVE rejects extra active row field",
                        reference: "RFC 3977 sections 7.6.3 and 9.4.3 https://www.rfc-editor.org/rfc/rfc3977#section-9.4.3",
                        kind: RequestKind::ListActive,
                        frame: b"215 list of newsgroups follows\r\nalt.test 3 1 y extra\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST ACTIVE.TIMES rejects non-numeric timestamp",
                        reference: "RFC 3977 section 7.6.4 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.4",
                        kind: RequestKind::ListActiveTimes,
                        frame: b"215 information follows\r\nalt.test yesterday admin@test\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST ACTIVE.TIMES rejects empty creator text",
                        reference: "RFC 3977 sections 7.6.4 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListActiveTimes,
                        frame: b"215 information follows\r\nalt.test 1715907600 \r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST ACTIVE.TIMES rejects creator text without leading P-CHAR",
                        reference: "RFC 3977 sections 7.6.4 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListActiveTimes,
                        frame: b"215 information follows\r\nalt.test 1715907600 \tadmin@test\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST NEWSGROUPS rejects invalid group name line",
                        reference: "RFC 3977 section 7.6.6 https://www.rfc-editor.org/rfc/rfc3977#section-7.6.6",
                        kind: RequestKind::ListNewsgroups,
                        frame: b"215 information follows\r\nalt.* Synthetic group\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST NEWSGROUPS rejects empty description",
                        reference: "RFC 3977 sections 7.6.6 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListNewsgroups,
                        frame: b"215 information follows\r\nalt.test \r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST NEWSGROUPS rejects description without leading S-CHAR",
                        reference: "RFC 3977 sections 7.6.6 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListNewsgroups,
                        frame: b"215 information follows\r\nalt.test \0Synthetic group\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "NEWGROUPS rejects malformed active row",
                        reference: "RFC 3977 sections 7.3 and 7.6.3 https://www.rfc-editor.org/rfc/rfc3977#section-7.3",
                        kind: RequestKind::NewGroups,
                        frame: b"231 list of new newsgroups follows\r\nalt.test 3 1 open\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "NEWGROUPS rejects extra active row field",
                        reference: "RFC 3977 sections 7.3 and 9.4.3 https://www.rfc-editor.org/rfc/rfc3977#section-9.4.3",
                        kind: RequestKind::NewGroups,
                        frame: b"231 list of new newsgroups follows\r\nalt.test 3 1 y extra\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST OVERVIEW.FMT rejects invalid overview field name",
                        reference: "RFC 3977 sections 8.4 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListOverviewFmt,
                        frame: b"215 overview format follows\r\nBad Header:\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST OVERVIEW.FMT rejects missing required overview fields",
                        reference: "RFC 3977 section 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListOverviewFmt,
                        frame: b"215 overview format follows\r\nSubject:\r\n:bytes\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST OVERVIEW.FMT rejects wrong required field order",
                        reference: "RFC 3977 section 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListOverviewFmt,
                        frame: b"215 overview format follows\r\nFrom:\r\nSubject:\r\nDate:\r\nMessage-ID:\r\nReferences:\r\n:bytes\r\n:lines\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST OVERVIEW.FMT rejects reversed required metadata order",
                        reference: "RFC 3977 section 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListOverviewFmt,
                        frame: b"215 overview format follows\r\nSubject:\r\nFrom:\r\nDate:\r\nMessage-ID:\r\nReferences:\r\n:lines\r\n:bytes\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST OVERVIEW.FMT rejects optional header without full marker",
                        reference: "RFC 3977 section 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListOverviewFmt,
                        frame: b"215 overview format follows\r\nSubject:\r\nFrom:\r\nDate:\r\nMessage-ID:\r\nReferences:\r\n:bytes\r\n:lines\r\nXref:\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST HEADERS rejects overview field colon suffix",
                        reference: "RFC 3977 section 8.6.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.6.2",
                        kind: RequestKind::ListHeaders,
                        frame: b"215 headers supported\r\nSubject:\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST HEADERS rejects all-headers marker mixed with header names",
                        reference: "RFC 3977 sections 8.6.2 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListHeaders,
                        frame: b"215 headers supported\r\n:\r\nSubject\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST DISTRIB.PATS rejects non-numeric priority",
                        reference: "RFC 3977 sections 7.6.5 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListDistribPats,
                        frame: b"215 distribution patterns\r\nfirst:*:world\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST DISTRIB.PATS rejects invalid wildmat field",
                        reference: "RFC 3977 sections 4.1 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListDistribPats,
                        frame: b"215 distribution patterns\r\n1:alt[0-9].*:world\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "LIST DISTRIB.PATS rejects distribution token with space",
                        reference: "RFC 3977 sections 7.6.5 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::ListDistribPats,
                        frame: b"215 distribution patterns\r\n1:*:local world\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "CAPABILITIES rejects empty capability token",
                        reference: "RFC 3977 sections 5.2 and 9.5 https://www.rfc-editor.org/rfc/rfc3977#section-5.2",
                        kind: RequestKind::Capabilities,
                        frame: b"101 Capability list:\r\nVERSION 2\r\nBAD  TOKEN\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "CAPABILITIES rejects missing first VERSION line",
                        reference: "RFC 3977 sections 9.4.3 and 9.5 https://www.rfc-editor.org/rfc/rfc3977#section-9.4.3",
                        kind: RequestKind::Capabilities,
                        frame: b"101 Capability list:\r\nREADER\r\nVERSION 2\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "CAPABILITIES rejects zero version number",
                        reference: "RFC 3977 section 9.5 https://www.rfc-editor.org/rfc/rfc3977#section-9.5",
                        kind: RequestKind::Capabilities,
                        frame: b"101 Capability list:\r\nVERSION 0\r\nREADER\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "OVER rejects body row without tab-separated overview fields",
                        reference: "RFC 3977 section 8.3.1 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.1",
                        kind: RequestKind::Over,
                        frame: b"224 overview follows\r\n1 Subject without tabs\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "OVER rejects missing required overview fields",
                        reference: "RFC 3977 sections 8.3.1 and 9.6 https://www.rfc-editor.org/rfc/rfc3977#section-9.6",
                        kind: RequestKind::Over,
                        frame: b"224 overview follows\r\n1\tSubject\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "OVER rejects unlabeled optional overview field",
                        reference: "RFC 3977 sections 8.3.2 and 9.4.3 https://www.rfc-editor.org/rfc/rfc3977#section-9.4.3",
                        kind: RequestKind::Over,
                        frame: b"224 overview follows\r\n1\tSubject\tfrom@test\tdate\t<one@test>\t\t1\t1\toptional without label\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "OVER rejects overlong article number",
                        reference: "RFC 3977 sections 8.3.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        kind: RequestKind::Over,
                        frame: b"224 overview follows\r\n12345678901234567\tSubject\tfrom@test\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "OVER rejects labeled bytes metadata field",
                        reference: "RFC 3977 sections 8.1.1 and 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                        kind: RequestKind::Over,
                        frame: b"224 overview follows\r\n1\tSubject\tfrom@test\tdate\t<one@test>\t\tbytes 1\t1\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "XOVER rejects missing required overview fields",
                        reference: "RFC 2980 section 2.1.7 and RFC 3977 section 9.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                        kind: RequestKind::Xover,
                        frame: b"224 overview follows\r\n1\tSubject\tfrom@test\tdate\t<one@test>\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "XOVER rejects body row without numeric article number",
                        reference: "RFC 2980 section 2.1.7 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.7",
                        kind: RequestKind::Xover,
                        frame: b"224 overview follows\r\none\tSubject\tfrom@test\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "XOVER rejects labeled lines metadata field",
                        reference: "RFC 2980 section 2.1.7 and RFC 3977 sections 8.1.2 and 8.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-8.3.2",
                        kind: RequestKind::Xover,
                        frame: b"224 overview follows\r\n1\tSubject\tfrom@test\tdate\t<one@test>\t\t1\tlines 1\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "XOVER rejects optional overview field without label separator",
                        reference: "RFC 3977 sections 8.3.2 and 9.4.3 https://www.rfc-editor.org/rfc/rfc3977#section-9.4.3",
                        kind: RequestKind::Xover,
                        frame: b"224 overview follows\r\n1\tSubject\tfrom@test\tdate\t<one@test>\t\t1\t1\tXref:missing-space\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "HDR rejects tab separator before header value",
                        reference: "RFC 3977 section 8.5.1 https://www.rfc-editor.org/rfc/rfc3977#section-8.5.1",
                        kind: RequestKind::Hdr,
                        frame: b"225 headers follow\r\n1\tvalue\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "HDR rejects overlong article number",
                        reference: "RFC 3977 sections 8.5.1 and 9.8 https://www.rfc-editor.org/rfc/rfc3977#section-9.8",
                        kind: RequestKind::Hdr,
                        frame: b"225 headers follow\r\n12345678901234567 value\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "XHDR rejects body row without numeric article number",
                        reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                        kind: RequestKind::Xhdr,
                        frame: b"221 headers follow\r\none value\r\n.\r\n",
                    },
                    ResponseFrameCase {
                        name: "CHECK rejects success response without message-id",
                        reference: "RFC 4644 section 2.4.1 https://www.rfc-editor.org/rfc/rfc4644#section-2.4.1",
                        kind: RequestKind::Check,
                        frame: b"238 send article to be transferred\r\n",
                    },
                    ResponseFrameCase {
                        name: "CHECK rejects retry error without message-id",
                        reference: "RFC 4644 section 2.4.1 https://www.rfc-editor.org/rfc/rfc4644#section-2.4.1",
                        kind: RequestKind::Check,
                        frame: b"431 transfer not possible; try again later\r\n",
                    },
                    ResponseFrameCase {
                        name: "CHECK rejects not-wanted error without valid message-id",
                        reference: "RFC 4644 section 2.4.1 https://www.rfc-editor.org/rfc/rfc4644#section-2.4.1",
                        kind: RequestKind::Check,
                        frame: b"438 check@test article not wanted\r\n",
                    },
                    ResponseFrameCase {
                        name: "TAKETHIS rejects malformed success response message-id",
                        reference: "RFC 4644 section 2.5.1 https://www.rfc-editor.org/rfc/rfc4644#section-2.5.1",
                        kind: RequestKind::TakeThis,
                        frame: b"239 take@test article transferred ok\r\n",
                    },
                    ResponseFrameCase {
                        name: "TAKETHIS rejects error response without message-id",
                        reference: "RFC 4644 section 2.5.1 https://www.rfc-editor.org/rfc/rfc4644#section-2.5.1",
                        kind: RequestKind::TakeThis,
                        frame: b"439 transfer rejected; do not retry\r\n",
                    },
                    ResponseFrameCase {
                        name: "POST rejects IHAVE continuation",
                        reference: "RFC 3977 sections 6.3.1 and 6.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.3",
                        kind: RequestKind::Post,
                        frame: b"335 send it\r\n",
                    },
                    ResponseFrameCase {
                        name: "IHAVE rejects POST continuation",
                        reference: "RFC 3977 sections 6.3.1 and 6.3.2 https://www.rfc-editor.org/rfc/rfc3977#section-6.3",
                        kind: RequestKind::Ihave,
                        frame: b"340 send article\r\n",
                    },
                    ResponseFrameCase {
                        name: "AUTHINFO PASS rejects USER challenge",
                        reference: "RFC 4643 section 2.1 https://www.rfc-editor.org/rfc/rfc4643#section-2.1",
                        kind: RequestKind::AuthInfoPass,
                        frame: b"381 password required\r\n",
                    },
                    ResponseFrameCase {
                        name: "AUTHINFO SASL 283 rejects zero-length challenge marker",
                        reference: "RFC 4643 sections 3.3 and 4 https://www.rfc-editor.org/rfc/rfc4643#section-3.3",
                        kind: RequestKind::AuthInfo,
                        frame: b"283 =\r\n",
                    },
                    ResponseFrameCase {
                        name: "AUTHINFO SASL 383 rejects missing challenge argument",
                        reference: "RFC 4643 sections 3.3 and 4 https://www.rfc-editor.org/rfc/rfc4643#section-3.3",
                        kind: RequestKind::AuthInfo,
                        frame: b"383 \r\n",
                    },
                    ResponseFrameCase {
                        name: "AUTHINFO SASL rejects whitespace inside challenge",
                        reference: "RFC 4643 sections 3.3 and 3.5 https://www.rfc-editor.org/rfc/rfc4643#section-3.3",
                        kind: RequestKind::AuthInfo,
                        frame: b"383 c2VydmVy challenge\r\n",
                    },
                    ResponseFrameCase {
                        name: "XHDR rejects HDR response code",
                        reference: "RFC 2980 section 2.1.6 https://www.rfc-editor.org/rfc/rfc2980#section-2.1.6",
                        kind: RequestKind::Xhdr,
                        frame: b"225 headers follow\r\n1 subject\r\n.\r\n",
                    },
                ]);
            }
        }
    }

    #[tokio::test]
    async fn pending_write_flushes_when_full_and_writes_oversized_directly() {
        let mut sink = tokio::io::sink();
        let mut pending = PendingWrite::new(DEFAULT_PENDING_WRITE_BYTES);
        let first = vec![b'a'; DEFAULT_PENDING_WRITE_BYTES - 16];
        let second = vec![b'b'; 32];
        pending.push(&mut sink, &first).await.unwrap();
        assert_eq!(pending.len, first.len());
        pending.push(&mut sink, &second).await.unwrap();
        assert_eq!(pending.len, second.len());

        let huge = vec![b'c'; DEFAULT_PENDING_WRITE_BYTES + 1];
        pending.push(&mut sink, &huge).await.unwrap();
        assert_eq!(pending.len, 0);
    }

    #[tokio::test]
    async fn pending_write_handles_pending_writer() {
        let mut writer = PendingOnceWriter::default();
        let mut pending = PendingWrite::new(DEFAULT_PENDING_WRITE_BYTES);

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
        let mut pending = PendingWrite::new(DEFAULT_PENDING_WRITE_BYTES);

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
        args.article_bytes = DEFAULT_PENDING_WRITE_BYTES + BODY_LINE.len();
        let config = ServerConfig::from_args(args);
        let mut stats = SessionStats::default();
        let mut pending = PendingWrite::new(DEFAULT_PENDING_WRITE_BYTES);
        let mut article_path = PathBuf::new();
        let mut session_state = SessionState {
            group_selected: true,
            selected_group: Some(FixtureGroup::AltTest),
            current_article: Some(1),
        };
        let mut writer = FailingWriter;
        let command = ParsedCommand {
            kind: RequestKind::Article,
            article_id: Some(1),
            line_slot: 0,
            message_id: None,
            syntax_error: false,
            has_transfer_body: false,
            line_too_long: false,
        };
        let command_lines = CommandLineBatch::default();

        let err = handle_command(
            &command,
            Some(&command_lines),
            &config,
            &mut stats,
            &mut writer,
            &mut pending,
            &mut article_path,
            &mut session_state,
        )
        .await
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn fixed_command_reader_handles_split_line_and_closed_sender() {
        let server = ChunkedRead::new(&[b"ART".as_slice(), b"ICLE 1\r\nBODY 2\r\n".as_slice()]);
        let mut reader = BufReader::with_capacity(32, server);
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();
        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                4
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
                Some(&mut command_lines),
                &mut command_batch,
                4
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn read_command_batch_reports_reader_error() {
        let mut reader = BufReader::with_capacity(32, FailingRead);
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        let err = read_command_batch(
            &mut reader,
            &mut command_buf,
            Some(&mut command_lines),
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
        let article_content = strip_dot_terminator_suffix(&article[header_end..]).unwrap();
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
        assert_eq!(config.body_bytes, 1024);
        assert_eq!(config.article_bytes, 2048);

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
        args.auth_user = Some("bench-user".to_string());
        args.auth_pass = Some("bench-pass".to_string());
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
        assert_eq!(config.read_buffer_bytes, terminator::TERMINATOR_TAIL_SIZE);
        assert!(!config.nodelay);
        assert_eq!(config.socket_recv_buffer, 4096);
        assert_eq!(config.socket_send_buffer, 8192);
        assert!(config.csv);
        assert_eq!(config.stats_interval, Duration::from_secs(2));
        assert_eq!(config.endpoint_for(0).port(), 1200);
        assert_eq!(config.endpoint_for(1).port(), 1201);
        assert_eq!(config.endpoint_for(2).port(), 1200);
        assert!(config.segments.is_some());
        assert_eq!(
            config.auth_user.as_ref().map(AuthInfoValue::as_str),
            Some("bench-user")
        );
        assert_eq!(
            config.auth_pass.as_ref().map(AuthInfoValue::as_str),
            Some("bench-pass")
        );
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
    fn client_config_rejects_invalid_authinfo_values() {
        let mut args = test_client_args();
        args.auth_user = Some("bad\ruser".to_string());
        let err = ClientConfig::from_args(args).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let mut args = test_client_args();
        args.auth_pass = Some("bad\npass".to_string());
        let err = ClientConfig::from_args(args).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
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
        args.auth_user = Some("session-user".to_string());
        args.auth_pass = Some("session-pass".to_string());
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
        assert!(!session.nodelay);
        assert_eq!(session.socket_recv_buffer, 1024);
        assert_eq!(session.socket_send_buffer, 2048);
        assert!(session.segments.is_some());
        assert_eq!(
            session.auth_user.as_ref().map(AuthInfoValue::as_str),
            Some("session-user")
        );
        assert_eq!(
            session.auth_pass.as_ref().map(AuthInfoValue::as_str),
            Some("session-pass")
        );
    }

    #[test]
    fn client_config_from_args_clamps_and_copies_fields() {
        let mut args = test_client_args();
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

        assert_eq!(config.requests, 17);
        assert_eq!(config.transfer_bytes, 8192);
        assert_eq!(config.duration, Duration::from_secs(3));
        assert_eq!(config.connections, 1);
        assert_eq!(config.client_offset, 4);
        assert_eq!(config.total_clients, 1);
        assert_eq!(config.pipeline_depth, 1);
        assert_eq!(config.command_mix, ClientCommandMix::Body);
        assert_eq!(config.start_id, 99);
        assert_eq!(config.read_buffer_bytes, terminator::TERMINATOR_TAIL_SIZE);
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

        assert_eq!(
            segment_for_request(&segments, 0, 2, 0).as_str(),
            "<bare@test>"
        );
        assert_eq!(
            segment_for_request(&segments, 1, 2, 0).as_str(),
            "<wrapped@test>"
        );
    }

    #[test]
    fn client_request_for_command_prefers_numeric_refs_without_segments() {
        let article =
            client_request_for_command(42, 0, ClientCommandMix::Article, None, 0, 1).unwrap();
        let body = client_request_for_command(7, 0, ClientCommandMix::Body, None, 0, 1).unwrap();
        let alternate_article =
            client_request_for_command(1, 0, ClientCommandMix::Alternate, None, 0, 1).unwrap();
        let alternate_body =
            client_request_for_command(2, 0, ClientCommandMix::Alternate, None, 0, 1).unwrap();

        assert_eq!(article.kind(), RequestKind::Article);
        assert_eq!(article.article_ref(), Some(&ArticleRef::Number(42)));
        assert!(article.message_id().is_none());
        assert_eq!(body.kind(), RequestKind::Body);
        assert_eq!(body.article_ref(), Some(&ArticleRef::Number(7)));
        assert!(body.message_id().is_none());
        assert_eq!(alternate_article.kind(), RequestKind::Article);
        assert_eq!(
            alternate_article.article_ref(),
            Some(&ArticleRef::Number(1))
        );
        assert!(alternate_article.message_id().is_none());
        assert_eq!(alternate_body.kind(), RequestKind::Body);
        assert_eq!(alternate_body.article_ref(), Some(&ArticleRef::Number(2)));
        assert!(alternate_body.message_id().is_none());
    }

    #[test]
    fn client_request_for_command_keeps_message_ids_for_segments_and_zero_id() {
        let path = write_temp_segments("client-request", "1\tsegment@test\n2\tbody@test\n");
        let segments = read_segments(&path).unwrap();
        fs::remove_file(path).unwrap();

        let segmented =
            client_request_for_command(11, 0, ClientCommandMix::Article, Some(&segments), 0, 1)
                .unwrap();
        let segmented_body =
            client_request_for_command(12, 1, ClientCommandMix::Body, Some(&segments), 0, 1)
                .unwrap();
        let zero_article =
            client_request_for_command(0, 0, ClientCommandMix::Article, None, 0, 1).unwrap();
        let zero_body =
            client_request_for_command(0, 0, ClientCommandMix::Body, None, 0, 1).unwrap();
        let zero_alternate =
            client_request_for_command(0, 0, ClientCommandMix::Alternate, None, 0, 1).unwrap();

        assert_eq!(
            segmented.message_id().map(MessageId::as_str),
            Some("<segment@test>")
        );
        assert_eq!(
            segmented_body.message_id().map(MessageId::as_str),
            Some("<body@test>")
        );
        assert_eq!(
            zero_article.message_id().map(MessageId::as_str),
            Some("<bench.0@nntpbench.local>")
        );
        assert_eq!(
            zero_body.message_id().map(MessageId::as_str),
            Some("<bench.0@nntpbench.local>")
        );
        assert_eq!(zero_alternate.kind(), RequestKind::Body);
        assert_eq!(
            zero_alternate.message_id().map(MessageId::as_str),
            Some("<bench.0@nntpbench.local>")
        );
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
        let err = read_greeting(&mut client).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        server.await.unwrap();

        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let greeting = vec![b'x'; protocol::MAX_INITIAL_RESPONSE_LINE_BYTES + 1];
            stream.write_all(&greeting).await.unwrap();
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        let err = read_greeting(&mut client).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn read_greeting_accepts_exact_rfc_initial_line_limit() {
        // RFC 3977 section 3.1 permits a response initial line of exactly
        // 512 octets, including the terminating CRLF.
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut greeting = Vec::from(b"201 ".as_slice());
            greeting.resize(protocol::MAX_INITIAL_RESPONSE_LINE_BYTES - 2, b'x');
            greeting.extend_from_slice(b"\r\n");
            stream.write_all(&greeting).await.unwrap();
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        read_greeting(&mut client).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn read_greeting_rejects_malformed_crlf_recovery() {
        // RFC 3977 section 3.1 defines response lines as CRLF-terminated. A greeting
        // with bare LF, bare CR, or malformed CR before a later CRLF must fail at the
        // malformed byte instead of resynchronizing:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        for greeting in [
            b"201 nntpbench mock server ready\n".as_slice(),
            b"201 nntpbench mock server ready\r later\r\n".as_slice(),
            b"201 nntpbench mock server ready\r\r\n".as_slice(),
        ] {
            let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.write_all(greeting).await.unwrap();
            });
            let mut client = TcpStream::connect(addr).await.unwrap();
            let err = read_greeting(&mut client).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{greeting:?}");
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn client_session_writes_expected_pipeline_wire_to_loopback_server() {
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

                while let Some(line_len) = find_crlf_line_end(&pending, 0) {
                    let response: &[u8] = if pending[..line_len].starts_with(b"ARTICLE ") {
                        b"220 1 <loopback-article@test> article follows\r\nbody\r\n.\r\n"
                    } else {
                        b"222 1 <loopback-body@test> body follows\r\nbody\r\n.\r\n"
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

    #[test]
    fn direct_client_server_request_limited_roundtrips_match_generated_e2e_shapes() {
        let mut runner = proptest::test_runner::TestRunner::new(direct_e2e_proptest_config(16));
        let strategy = (
            128_usize..=4096,
            256_usize..=8192,
            1_usize..=8,
            1_u64..=16,
            1_usize..=8,
            client_command_mix_strategy(),
            0_u64..=4,
            64_usize..=256,
        );

        runner
            .run(
                &strategy,
                |(
                    body_bytes,
                    article_bytes,
                    max_pipeline_depth,
                    requests,
                    pipeline_depth,
                    mix,
                    start_id,
                    read_buffer_bytes,
                )| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    let (client, server) = runtime
                        .block_on(run_direct_client_server_case(
                            body_bytes,
                            article_bytes,
                            max_pipeline_depth,
                            requests,
                            0,
                            pipeline_depth,
                            mix,
                            start_id,
                            read_buffer_bytes,
                        ))
                        .unwrap();

                    let (expected_articles, expected_bodies) =
                        expected_client_command_counts(start_id, requests, mix);
                    prop_assert_eq!(client.accepted_connections, 1);
                    prop_assert_eq!(client.active_connections, 0);
                    prop_assert_eq!(client.commands, requests);
                    prop_assert_eq!(client.article_requests, expected_articles);
                    prop_assert_eq!(client.body_requests, expected_bodies);
                    prop_assert_eq!(server.commands, requests);
                    prop_assert_eq!(server.article_requests, expected_articles);
                    prop_assert_eq!(server.body_requests, expected_bodies);
                    prop_assert_eq!(server.errors, 0);
                    prop_assert!(client.bytes_sent > 0);
                    prop_assert!(server.bytes_sent >= client.bytes_sent);
                    if requests > 1 && pipeline_depth > 1 {
                        prop_assert!(client.pipeline_batches > 0);
                    }
                    Ok(())
                },
            )
            .unwrap();
    }

    #[test]
    fn direct_client_server_transfer_limited_roundtrips_cross_generated_byte_targets() {
        let mut runner = proptest::test_runner::TestRunner::new(direct_e2e_proptest_config(12));
        let strategy = (
            128_usize..=4096,
            256_usize..=8192,
            1_usize..=8,
            1_u64..=16_384,
            1_usize..=6,
            client_command_mix_strategy(),
            0_u64..=4,
            64_usize..=256,
        );

        runner
            .run(
                &strategy,
                |(
                    body_bytes,
                    article_bytes,
                    max_pipeline_depth,
                    transfer_bytes,
                    pipeline_depth,
                    mix,
                    start_id,
                    read_buffer_bytes,
                )| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    let (client, server) = runtime
                        .block_on(run_direct_client_server_case(
                            body_bytes,
                            article_bytes,
                            max_pipeline_depth,
                            0,
                            transfer_bytes,
                            pipeline_depth,
                            mix,
                            start_id,
                            read_buffer_bytes,
                        ))
                        .unwrap();

                    prop_assert_eq!(client.accepted_connections, 1);
                    prop_assert_eq!(client.active_connections, 0);
                    prop_assert!(client.commands >= 1);
                    prop_assert!(client.bytes_sent >= transfer_bytes);
                    prop_assert_eq!(server.commands, client.commands);
                    prop_assert_eq!(server.article_requests, client.article_requests);
                    prop_assert_eq!(server.body_requests, client.body_requests);
                    prop_assert_eq!(server.errors, 0);
                    prop_assert!(server.bytes_sent >= client.bytes_sent);
                    Ok(())
                },
            )
            .unwrap();
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
            while pending.iter().filter(|byte| **byte == b'\n').count() < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }

            assert_eq!(&pending[..], b"ARTICLE 1\r\nBODY 2\r\n");

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
    async fn client_session_sends_authinfo_before_workload_requests() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let mut pending = Vec::new();
            while pending.iter().filter(|byte| **byte == b'\n').count() < 1 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }
            assert_eq!(&pending[..], b"AUTHINFO USER bench-user\r\n");
            pending.clear();
            stream
                .write_all(b"381 authentication information required\r\n")
                .await
                .unwrap();

            while pending.iter().filter(|byte| **byte == b'\n').count() < 1 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }
            assert_eq!(&pending[..], b"AUTHINFO PASS bench-pass\r\n");
            pending.clear();
            stream
                .write_all(b"281 authentication accepted\r\n")
                .await
                .unwrap();

            while pending.iter().filter(|byte| **byte == b'\n').count() < 2 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }
            assert_eq!(&pending[..], b"ARTICLE 1\r\nBODY 2\r\n");

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

        let mut args = test_client_args();
        args.connect = addr;
        args.auth_user = Some("bench-user".to_string());
        args.auth_pass = Some("bench-pass".to_string());
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
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.article_requests, 1);
        assert_eq!(snapshot.body_requests, 1);
        assert_eq!(snapshot.pipeline_batches, 1);
    }

    #[tokio::test]
    async fn client_session_skips_authinfo_pass_after_user_success() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();
            let mut reader = BufReader::new(stream);

            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).await.unwrap();
            assert_eq!(line, b"AUTHINFO USER bench-user\r\n");
            reader
                .get_mut()
                .write_all(b"281 authentication accepted\r\n")
                .await
                .unwrap();

            line.clear();
            reader.read_until(b'\n', &mut line).await.unwrap();
            assert_eq!(line, b"ARTICLE 1\r\n");
            reader
                .get_mut()
                .write_all(
                    b"220 1 <bench.1@nntpbench.local> article follows\r\nSubject: Bench\r\n\r\none\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.auth_user = Some("bench-user".to_string());
        args.auth_pass = Some("bench-pass".to_string());
        args.requests = 1;
        args.pipeline_depth = 1;
        args.command_mix = ClientCommandMix::Article;
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 1);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        session.run(stats, stop).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn download_authentication_skips_authinfo_pass_after_user_success() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);

            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).await.unwrap();
            assert_eq!(line, b"AUTHINFO USER bench-user\r\n");
            reader
                .get_mut()
                .write_all(b"281 authentication accepted\r\n")
                .await
                .unwrap();

            line.clear();
            reader.read_until(b'\n', &mut line).await.unwrap();
            assert_eq!(line, b"ARTICLE 1\r\n");
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut response_reader = crate::client::DrainedResponseReader::new(CLIENT_READER_CAPACITY);
        authenticate_client_stream(
            &mut stream,
            &mut response_reader,
            Some(AuthInfoValue::from_borrowed("bench-user").unwrap()),
            Some(AuthInfoValue::from_borrowed("bench-pass").unwrap()),
        )
        .await
        .unwrap();
        stream.write_all(b"ARTICLE 1\r\n").await.unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_article_id_mode_writes_220_responses_and_skips_430() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let response =
            b"220 1 <article.1@test> article follows\r\nSubject: one\r\n\r\nbody\r\n.\r\n";
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
            assert_eq!(&pending[..], b"ARTICLE 1\r\nARTICLE 2\r\n");
            stream.write_all(response).await.unwrap();
            stream.write_all(ARTICLE_NOT_FOUND_RESPONSE).await.unwrap();
        });

        let id_file = write_temp_segments("article-ids", "1\n2\n");
        let output_dir = unique_temp_dir("article-output");
        let verify_dir = unique_temp_dir("article-verify");
        let verify_path = article_id_tree_path(&verify_dir, 1);
        fs::create_dir_all(verify_path.parent().unwrap()).unwrap();
        fs::write(&verify_path, response).unwrap();
        let mut args = test_client_args();
        args.connect = addr;
        args.article_ids = Some(id_file.clone());
        args.article_output_dir = Some(output_dir.clone());
        args.article_verify_dir = Some(verify_dir.clone());
        args.connections = 1;
        args.pipeline_depth = 2;
        args.stats_interval_secs = 0;

        run_client(args).await.unwrap();
        server.await.unwrap();

        let stored = fs::read(article_id_tree_path(&output_dir, 1)).unwrap();
        assert_eq!(stored, response);
        assert!(!article_id_tree_path(&output_dir, 2).exists());

        fs::remove_file(id_file).unwrap();
        fs::remove_dir_all(output_dir).unwrap();
        fs::remove_dir_all(verify_dir).unwrap();
    }

    #[tokio::test]
    async fn client_article_id_mode_rejects_md5_mismatch_when_verify_file_exists() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let response =
            b"220 1 <article.1@test> article follows\r\nSubject: one\r\n\r\nactual\r\n.\r\n";
        let expected =
            b"220 1 <article.1@test> article follows\r\nSubject: one\r\n\r\nexpected\r\n.\r\n";
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let mut pending = Vec::new();
            while pending.iter().filter(|byte| **byte == b'\n').count() < 1 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }
            assert_eq!(&pending[..], b"ARTICLE 1\r\n");
            stream.write_all(response).await.unwrap();
        });

        let id_file = write_temp_segments("article-ids-md5", "1\n");
        let output_dir = unique_temp_dir("article-output-md5");
        let verify_dir = unique_temp_dir("article-verify-md5");
        let verify_path = article_id_tree_path(&verify_dir, 1);
        fs::create_dir_all(verify_path.parent().unwrap()).unwrap();
        fs::write(&verify_path, expected).unwrap();

        let mut args = test_client_args();
        args.connect = addr;
        args.article_ids = Some(id_file.clone());
        args.article_output_dir = Some(output_dir.clone());
        args.article_verify_dir = Some(verify_dir.clone());
        args.connections = 1;
        args.stats_interval_secs = 0;

        let err = run_client(args).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("MD5 mismatch"));
        assert!(!article_id_tree_path(&output_dir, 1).exists());
        assert_eq!(
            fs::read(article_id_tree_path(&output_dir.join("failed"), 1)).unwrap(),
            response
        );
        server.await.unwrap();

        fs::remove_file(id_file).unwrap();
        fs::remove_dir_all(output_dir).unwrap();
        fs::remove_dir_all(verify_dir).unwrap();
    }

    #[tokio::test]
    async fn client_article_id_file_accepts_message_ids() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let response =
            b"220 1 <abc123@ngPost> article follows\r\nSubject: message id\r\n\r\nbody\r\n.\r\n";
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 loopback ready\r\n").await.unwrap();

            let mut scratch = [0_u8; 128];
            let mut pending = Vec::new();
            while pending.iter().filter(|byte| **byte == b'\n').count() < 1 {
                let read = stream.read(&mut scratch).await.unwrap();
                assert_ne!(read, 0);
                pending.extend_from_slice(&scratch[..read]);
            }
            assert_eq!(&pending[..], b"ARTICLE <abc123@ngPost>\r\n");
            stream.write_all(response).await.unwrap();
        });

        let ids = write_temp_segments("message-ids", "abc123@ngPost\n");
        let output_dir = unique_temp_dir("message-id-output");
        let mut args = test_client_args();
        args.connect = addr;
        args.article_ids = Some(ids.clone());
        args.article_output_dir = Some(output_dir.clone());
        args.connections = 1;
        args.stats_interval_secs = 0;

        run_client(args).await.unwrap();
        server.await.unwrap();

        let message_id = MessageId::from_borrowed("<abc123@ngPost>").unwrap();
        let stored = fs::read(message_id_tree_path(&output_dir, &message_id)).unwrap();
        assert_eq!(stored, response);

        fs::remove_file(ids).unwrap();
        fs::remove_dir_all(output_dir).unwrap();
    }

    #[tokio::test]
    async fn client_session_uses_segment_message_ids_on_wire() {
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
                b"ARTICLE <segment-1@test>\r\nBODY <segment-2@test>\r\n"
            );

            stream
                .write_all(
                    b"220 1 <segment-1@test> article follows\r\nSubject: Bench\r\n\r\none\r\n.\r\n",
                )
                .await
                .unwrap();
            stream
                .write_all(b"222 1 <segment-2@test> body follows\r\ntwo\r\n.\r\n")
                .await
                .unwrap();
        });

        let path = write_temp_segments(
            "client-session-wire",
            "1\tsegment-1@test\n2\tsegment-2@test\n",
        );
        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 2;
        args.pipeline_depth = 2;
        args.command_mix = ClientCommandMix::Alternate;
        args.read_buffer_bytes = 64;
        args.segments = Some(path.clone());
        let config = ClientConfig::from_args(args).unwrap();
        fs::remove_file(path).unwrap();
        let session = ClientSession::new(&config, 0, 1, 2);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        session.run(stats.clone(), stop.clone()).await.unwrap();
        server.await.unwrap();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.article_requests, 1);
        assert_eq!(snapshot.body_requests, 1);
        assert_eq!(snapshot.pipeline_batches, 1);
    }

    #[tokio::test]
    async fn client_session_zero_start_id_falls_back_per_request() {
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
                b"BODY <bench.0@nntpbench.local>\r\nARTICLE 1\r\n"
            );

            stream
                .write_all(b"222 0 <bench.0@nntpbench.local> body follows\r\nzero\r\n.\r\n")
                .await
                .unwrap();
            stream
                .write_all(
                    b"220 1 <bench.1@nntpbench.local> article follows\r\nSubject: Bench\r\n\r\none\r\n.\r\n",
                )
                .await
                .unwrap();
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 2;
        args.pipeline_depth = 2;
        args.command_mix = ClientCommandMix::Alternate;
        args.start_id = 0;
        args.read_buffer_bytes = 64;
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 0, 2);
        let stats = Arc::new(Stats::new());
        let stop = Arc::new(AtomicBool::new(false));

        session.run(stats.clone(), stop.clone()).await.unwrap();
        server.await.unwrap();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.article_requests, 1);
        assert_eq!(snapshot.body_requests, 1);
        assert_eq!(snapshot.pipeline_batches, 1);
    }

    #[test]
    fn client_request_for_command_uses_message_id_after_numeric_article_range() {
        let request = client_request_for_command(
            i32::MAX as u64 + 1,
            0,
            ClientCommandMix::Article,
            None,
            0,
            1,
        )
        .unwrap();

        match request {
            Request::Article { article_ref } => assert_eq!(
                article_ref.message_id().map(MessageId::as_str),
                Some("<bench.2147483648@nntpbench.local>")
            ),
            other => panic!("expected message-id ARTICLE fallback, got {other:?}"),
        }

        let request =
            client_request_for_command(i32::MAX as u64 + 2, 0, ClientCommandMix::Body, None, 0, 1)
                .unwrap();

        match request {
            Request::Body { article_ref } => assert_eq!(
                article_ref.message_id().map(MessageId::as_str),
                Some("<bench.2147483649@nntpbench.local>")
            ),
            other => panic!("expected message-id BODY fallback, got {other:?}"),
        }
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
                .write_all(b"220 1 <pipeline-1@test> article follows\r\nbody\r\n.\r\n")
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
                .write_all(b"222 1 <pipeline-2@test> body follows\r\nbody\r\n.\r\n")
                .await
                .unwrap();
            stream
                .write_all(b"220 1 <pipeline-3@test> article follows\r\nbody\r\n.\r\n")
                .await
                .unwrap();
        });

        let mut args = test_client_args();
        args.connect = addr;
        args.requests = 3;
        args.pipeline_depth = 2;
        args.command_mix = ClientCommandMix::Alternate;
        args.read_buffer_bytes = 64;
        let config = ClientConfig::from_args(args).unwrap();
        let session = ClientSession::new(&config, 0, 1, 3);
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
                .write_all(b"222 1 <transfer@test> body follows\r\nresponse payload\r\n.\r\n")
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
    async fn fetch_response_uses_client_connection_path() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = stream.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"BODY <client-fetch@test>\r\n");

            stream
                .write_all(b"222 1 <client-fetch@test> body follows\r\nbody payload\r\n.\r\n")
                .await
                .unwrap();
        });

        let mut args = test_fetch_args();
        args.connect = addr;
        args.request = FetchRequestKind::Body;
        args.message_id = Some("client-fetch@test".to_string());
        args.read_buffer_bytes = 64;

        let response = fetch_response(&args).await.unwrap();
        let article = response.parse_article().unwrap();

        assert_eq!(response.kind(), RequestKind::Body);
        assert_eq!(response.status().as_u16(), 222);
        assert_eq!(article.message_id.as_str(), "<client-fetch@test>");
        assert_eq!(article.body.as_deref(), Some(&b"body payload\r\n"[..]));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_supports_head_and_stat_requests() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut head_stream, _) = listener.accept().await.unwrap();
            head_stream.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut first = [0_u8; 128];
            let read = head_stream.read(&mut first).await.unwrap();
            assert_eq!(&first[..read], b"HEAD <client-head@test>\r\n");
            head_stream
                .write_all(b"221 1 <client-head@test> article retrieved\r\nSubject: Head\r\n.\r\n")
                .await
                .unwrap();

            let (mut stat_stream, _) = listener.accept().await.unwrap();
            stat_stream.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut second = [0_u8; 128];
            let read = stat_stream.read(&mut second).await.unwrap();
            assert_eq!(&second[..read], b"STAT <client-stat@test>\r\n");
            stat_stream
                .write_all(b"223 1 <client-stat@test> article retrieved\r\n")
                .await
                .unwrap();
        });

        let mut head_args = test_fetch_args();
        head_args.connect = addr;
        head_args.request = FetchRequestKind::Head;
        head_args.message_id = Some("client-head@test".to_string());
        let head = fetch_response(&head_args).await.unwrap();
        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.status().as_u16(), 221);

        let mut stat_args = test_fetch_args();
        stat_args.connect = addr;
        stat_args.request = FetchRequestKind::Stat;
        stat_args.message_id = Some("client-stat@test".to_string());
        let stat = fetch_response(&stat_args).await.unwrap();
        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.status().as_u16(), 223);
        assert_eq!(
            stat.parse_article().unwrap().message_id.as_str(),
            "<client-stat@test>"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn serve_session_supports_head_and_stat_requests() {
        let config = test_config();
        let (output, stats) = run_session_with_input(
            config,
            b"HEAD <client-head@test>\r\nSTAT <client-stat@test>\r\n",
        )
        .await;

        let head_id = MessageId::from_borrowed("<client-head@test>").unwrap();
        let stat_id = MessageId::from_borrowed("<client-stat@test>").unwrap();
        assert_eq!(
            output,
            [
                GREETING,
                &build_message_id_head_response(&head_id),
                &build_message_id_stat_response(&stat_id),
            ]
            .concat()
        );

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.article_requests, 2);
        assert_eq!(snapshot.body_requests, 0);
    }

    #[tokio::test]
    async fn serve_session_supports_list_subcommand_requests() {
        let config = test_config();
        let (output, stats) = run_session_with_input(
            config,
            b"LIST ACTIVE\r\nLIST ACTIVE.TIMES\r\nLIST ACTIVE.TIMES comp.lang.*\r\nLIST NEWSGROUPS\r\nLIST NEWSGROUPS comp.lang.*\r\nLIST OVERVIEW.FMT\r\nLIST HEADERS\r\nLIST DISTRIB.PATS\r\n",
        )
        .await;

        assert_eq!(
            output,
            [
                GREETING,
                LIST_RESPONSE,
                LIST_ACTIVE_TIMES_RESPONSE,
                LIST_ACTIVE_TIMES_COMP_RESPONSE,
                LIST_NEWSGROUPS_RESPONSE,
                LIST_NEWSGROUPS_COMP_RESPONSE,
                LIST_OVERVIEW_FMT_RESPONSE,
                LIST_HEADERS_RESPONSE,
                LIST_DISTRIB_PATS_RESPONSE,
            ]
            .concat()
        );

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 8);
        assert_eq!(snapshot.article_requests, 0);
        assert_eq!(snapshot.body_requests, 0);
    }

    #[tokio::test]
    async fn fetch_response_rejects_invalid_message_id() {
        let mut args = test_fetch_args();
        args.message_id = Some("<bad id>".to_string());

        let err = fetch_response(&args).await.unwrap_err();
        assert!(matches!(err, ClientError::InvalidMessageId));
    }

    #[tokio::test]
    async fn fetch_response_rejects_invalid_article_selector() {
        let mut args = test_fetch_args();
        args.message_id = None;
        args.selector = Some("0001".to_string());

        let err = fetch_response(&args).await.unwrap_err();
        assert!(matches!(err, ClientError::InvalidArticleSelector));
    }

    #[tokio::test]
    async fn fetch_response_clamps_pipeline_depth_for_client_engine() {
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

        let mut args = test_fetch_args();
        args.connect = addr;
        args.message_id = Some("clamp@test".to_string());
        args.pipeline_depth = 0;

        let response = fetch_response(&args).await.unwrap();
        assert_eq!(response.status().as_u16(), 220);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_supports_general_request_kinds_without_message_id() {
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

        let mut list_args = test_fetch_args();
        list_args.connect = addr;
        list_args.request = FetchRequestKind::List;
        list_args.message_id = None;
        let list = fetch_response(&list_args).await.unwrap();
        assert_eq!(list.kind(), RequestKind::List);
        assert_eq!(list.status().as_u16(), 215);

        let mut help_args = test_fetch_args();
        help_args.connect = addr;
        help_args.request = FetchRequestKind::Help;
        help_args.message_id = None;
        let help = fetch_response(&help_args).await.unwrap();
        assert_eq!(help.kind(), RequestKind::Help);
        assert_eq!(help.status().as_u16(), 100);

        let mut capabilities_args = test_fetch_args();
        capabilities_args.connect = addr;
        capabilities_args.request = FetchRequestKind::Capabilities;
        capabilities_args.message_id = None;
        let capabilities = fetch_response(&capabilities_args).await.unwrap();
        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert_eq!(capabilities.status().as_u16(), 101);

        let mut date_args = test_fetch_args();
        date_args.connect = addr;
        date_args.request = FetchRequestKind::Date;
        date_args.message_id = None;
        let date = fetch_response(&date_args).await.unwrap();
        assert_eq!(date.kind(), RequestKind::Date);
        assert_eq!(date.status().as_u16(), 111);

        let mut mode_reader_args = test_fetch_args();
        mode_reader_args.connect = addr;
        mode_reader_args.request = FetchRequestKind::ModeReader;
        mode_reader_args.message_id = None;
        let mode_reader = fetch_response(&mode_reader_args).await.unwrap();
        assert_eq!(mode_reader.kind(), RequestKind::ModeReader);
        assert_eq!(mode_reader.status().as_u16(), 201);

        let mut quit_args = test_fetch_args();
        quit_args.connect = addr;
        quit_args.request = FetchRequestKind::Quit;
        quit_args.message_id = None;
        let quit = fetch_response(&quit_args).await.unwrap();
        assert_eq!(quit.kind(), RequestKind::Quit);
        assert_eq!(quit.status().as_u16(), 205);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_supports_list_subcommand_request_kinds() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            first.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = first.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST ACTIVE\r\n");
            first.write_all(LIST_RESPONSE).await.unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            second.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = second.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST ACTIVE.TIMES\r\n");
            second.write_all(LIST_ACTIVE_TIMES_RESPONSE).await.unwrap();

            let (mut third, _) = listener.accept().await.unwrap();
            third.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = third.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST ACTIVE.TIMES comp.lang.*\r\n");
            third.write_all(LIST_ACTIVE_TIMES_RESPONSE).await.unwrap();

            let (mut fourth, _) = listener.accept().await.unwrap();
            fourth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fourth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST NEWSGROUPS\r\n");
            fourth.write_all(LIST_NEWSGROUPS_RESPONSE).await.unwrap();

            let (mut fifth, _) = listener.accept().await.unwrap();
            fifth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fifth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST NEWSGROUPS comp.lang.*\r\n");
            fifth.write_all(LIST_NEWSGROUPS_RESPONSE).await.unwrap();

            let (mut sixth, _) = listener.accept().await.unwrap();
            sixth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = sixth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST OVERVIEW.FMT\r\n");
            sixth.write_all(LIST_OVERVIEW_FMT_RESPONSE).await.unwrap();

            let (mut seventh, _) = listener.accept().await.unwrap();
            seventh.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = seventh.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST HEADERS\r\n");
            seventh.write_all(LIST_HEADERS_RESPONSE).await.unwrap();

            let (mut eighth, _) = listener.accept().await.unwrap();
            eighth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = eighth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LIST DISTRIB.PATS\r\n");
            eighth.write_all(LIST_DISTRIB_PATS_RESPONSE).await.unwrap();
        });

        let mut list_active_args = test_fetch_args();
        list_active_args.connect = addr;
        list_active_args.request = FetchRequestKind::ListActive;
        list_active_args.message_id = None;
        let list_active = fetch_response(&list_active_args).await.unwrap();
        assert_eq!(list_active.kind(), RequestKind::ListActive);
        assert_eq!(list_active.status().as_u16(), 215);

        let mut list_active_times_args = test_fetch_args();
        list_active_times_args.connect = addr;
        list_active_times_args.request = FetchRequestKind::ListActiveTimes;
        list_active_times_args.message_id = None;
        let list_active_times = fetch_response(&list_active_times_args).await.unwrap();
        assert_eq!(list_active_times.kind(), RequestKind::ListActiveTimes);
        assert_eq!(list_active_times.status().as_u16(), 215);

        let mut list_active_times_wildmat_args = test_fetch_args();
        list_active_times_wildmat_args.connect = addr;
        list_active_times_wildmat_args.request = FetchRequestKind::ListActiveTimes;
        list_active_times_wildmat_args.message_id = None;
        list_active_times_wildmat_args.wildmat = Some("comp.lang.*".to_string());
        let list_active_times_wildmat = fetch_response(&list_active_times_wildmat_args)
            .await
            .unwrap();
        assert_eq!(
            list_active_times_wildmat.kind(),
            RequestKind::ListActiveTimes
        );
        assert_eq!(list_active_times_wildmat.status().as_u16(), 215);

        let mut list_newsgroups_args = test_fetch_args();
        list_newsgroups_args.connect = addr;
        list_newsgroups_args.request = FetchRequestKind::ListNewsgroups;
        list_newsgroups_args.message_id = None;
        let list_newsgroups = fetch_response(&list_newsgroups_args).await.unwrap();
        assert_eq!(list_newsgroups.kind(), RequestKind::ListNewsgroups);
        assert_eq!(list_newsgroups.status().as_u16(), 215);

        let mut list_newsgroups_wildmat_args = test_fetch_args();
        list_newsgroups_wildmat_args.connect = addr;
        list_newsgroups_wildmat_args.request = FetchRequestKind::ListNewsgroups;
        list_newsgroups_wildmat_args.message_id = None;
        list_newsgroups_wildmat_args.wildmat = Some("comp.lang.*".to_string());
        let list_newsgroups_wildmat = fetch_response(&list_newsgroups_wildmat_args).await.unwrap();
        assert_eq!(list_newsgroups_wildmat.kind(), RequestKind::ListNewsgroups);
        assert_eq!(list_newsgroups_wildmat.status().as_u16(), 215);

        let mut list_overview_fmt_args = test_fetch_args();
        list_overview_fmt_args.connect = addr;
        list_overview_fmt_args.request = FetchRequestKind::ListOverviewFmt;
        list_overview_fmt_args.message_id = None;
        let list_overview_fmt = fetch_response(&list_overview_fmt_args).await.unwrap();
        assert_eq!(list_overview_fmt.kind(), RequestKind::ListOverviewFmt);
        assert_eq!(list_overview_fmt.status().as_u16(), 215);

        let mut list_headers_args = test_fetch_args();
        list_headers_args.connect = addr;
        list_headers_args.request = FetchRequestKind::ListHeaders;
        list_headers_args.message_id = None;
        let list_headers = fetch_response(&list_headers_args).await.unwrap();
        assert_eq!(list_headers.kind(), RequestKind::ListHeaders);
        assert_eq!(list_headers.status().as_u16(), 215);

        let mut list_distrib_pats_args = test_fetch_args();
        list_distrib_pats_args.connect = addr;
        list_distrib_pats_args.request = FetchRequestKind::ListDistribPats;
        list_distrib_pats_args.message_id = None;
        let list_distrib_pats = fetch_response(&list_distrib_pats_args).await.unwrap();
        assert_eq!(list_distrib_pats.kind(), RequestKind::ListDistribPats);
        assert_eq!(list_distrib_pats.status().as_u16(), 215);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_supports_header_query_request_kinds() {
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
            assert_eq!(&request[..read], b"HDR Message-ID <headers@test>\r\n");
            second.write_all(HDR_RESPONSE).await.unwrap();

            let (mut third, _) = listener.accept().await.unwrap();
            third.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = third.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"XHDR Subject 1-10\r\n");
            third.write_all(XHDR_RESPONSE).await.unwrap();

            let (mut fourth, _) = listener.accept().await.unwrap();
            fourth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fourth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"XHDR Message-ID <headers@test>\r\n");
            fourth.write_all(XHDR_RESPONSE).await.unwrap();

            let (mut fifth, _) = listener.accept().await.unwrap();
            fifth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fifth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"HDR :bytes 1\r\n");
            fifth.write_all(HDR_RESPONSE).await.unwrap();
        });

        let mut hdr_range_args = test_fetch_args();
        hdr_range_args.connect = addr;
        hdr_range_args.request = FetchRequestKind::Hdr;
        hdr_range_args.message_id = None;
        hdr_range_args.header = Some("Subject".to_string());
        hdr_range_args.selector = Some("1-10".to_string());
        let hdr_range = fetch_response(&hdr_range_args).await.unwrap();
        assert_eq!(hdr_range.kind(), RequestKind::Hdr);
        assert_eq!(hdr_range.status().as_u16(), 225);

        let mut hdr_message_id_args = test_fetch_args();
        hdr_message_id_args.connect = addr;
        hdr_message_id_args.request = FetchRequestKind::Hdr;
        hdr_message_id_args.message_id = None;
        hdr_message_id_args.header = Some("Message-ID".to_string());
        hdr_message_id_args.selector = Some("<headers@test>".to_string());
        let hdr_message_id = fetch_response(&hdr_message_id_args).await.unwrap();
        assert_eq!(hdr_message_id.kind(), RequestKind::Hdr);
        assert_eq!(hdr_message_id.status().as_u16(), 225);

        let mut xhdr_range_args = test_fetch_args();
        xhdr_range_args.connect = addr;
        xhdr_range_args.request = FetchRequestKind::Xhdr;
        xhdr_range_args.message_id = None;
        xhdr_range_args.header = Some("Subject".to_string());
        xhdr_range_args.selector = Some("1-10".to_string());
        let xhdr_range = fetch_response(&xhdr_range_args).await.unwrap();
        assert_eq!(xhdr_range.kind(), RequestKind::Xhdr);
        assert_eq!(xhdr_range.status().as_u16(), 221);

        let mut xhdr_message_id_args = test_fetch_args();
        xhdr_message_id_args.connect = addr;
        xhdr_message_id_args.request = FetchRequestKind::Xhdr;
        xhdr_message_id_args.message_id = None;
        xhdr_message_id_args.header = Some("Message-ID".to_string());
        xhdr_message_id_args.selector = Some("<headers@test>".to_string());
        let xhdr_message_id = fetch_response(&xhdr_message_id_args).await.unwrap();
        assert_eq!(xhdr_message_id.kind(), RequestKind::Xhdr);
        assert_eq!(xhdr_message_id.status().as_u16(), 221);

        let mut hdr_metadata_args = test_fetch_args();
        hdr_metadata_args.connect = addr;
        hdr_metadata_args.request = FetchRequestKind::Hdr;
        hdr_metadata_args.message_id = None;
        hdr_metadata_args.header = Some(":bytes".to_string());
        hdr_metadata_args.selector = Some("1".to_string());
        let hdr_metadata = fetch_response(&hdr_metadata_args).await.unwrap();
        assert_eq!(hdr_metadata.kind(), RequestKind::Hdr);
        assert_eq!(hdr_metadata.status().as_u16(), 225);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_supports_overview_request_kinds() {
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
            assert_eq!(&request[..read], b"OVER <overview@test>\r\n");
            second.write_all(OVER_RESPONSE).await.unwrap();

            let (mut third, _) = listener.accept().await.unwrap();
            third.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = third.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"XOVER 1-10\r\n");
            third.write_all(XOVER_RESPONSE).await.unwrap();

            let (mut fourth, _) = listener.accept().await.unwrap();
            fourth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fourth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"XOVER <overview@test>\r\n");
            fourth.write_all(XOVER_RESPONSE).await.unwrap();
        });

        let mut over_range_args = test_fetch_args();
        over_range_args.connect = addr;
        over_range_args.request = FetchRequestKind::Over;
        over_range_args.message_id = None;
        over_range_args.selector = Some("1-10".to_string());
        let over_range = fetch_response(&over_range_args).await.unwrap();
        assert_eq!(over_range.kind(), RequestKind::Over);
        assert_eq!(over_range.status().as_u16(), 224);

        let mut over_message_id_args = test_fetch_args();
        over_message_id_args.connect = addr;
        over_message_id_args.request = FetchRequestKind::Over;
        over_message_id_args.message_id = None;
        over_message_id_args.selector = Some("<overview@test>".to_string());
        let over_message_id = fetch_response(&over_message_id_args).await.unwrap();
        assert_eq!(over_message_id.kind(), RequestKind::Over);
        assert_eq!(over_message_id.status().as_u16(), 224);

        let mut xover_range_args = test_fetch_args();
        xover_range_args.connect = addr;
        xover_range_args.request = FetchRequestKind::Xover;
        xover_range_args.message_id = None;
        xover_range_args.selector = Some("1-10".to_string());
        let xover_range = fetch_response(&xover_range_args).await.unwrap();
        assert_eq!(xover_range.kind(), RequestKind::Xover);
        assert_eq!(xover_range.status().as_u16(), 224);

        let mut xover_message_id_args = test_fetch_args();
        xover_message_id_args.connect = addr;
        xover_message_id_args.request = FetchRequestKind::Xover;
        xover_message_id_args.message_id = None;
        xover_message_id_args.selector = Some("<overview@test>".to_string());
        let xover_message_id = fetch_response(&xover_message_id_args).await.unwrap();
        assert_eq!(xover_message_id.kind(), RequestKind::Xover);
        assert_eq!(xover_message_id.status().as_u16(), 224);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_supports_group_navigation_and_discovery_request_kinds() {
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
            assert_eq!(&request[..read], b"LISTGROUP\r\n");
            second.write_all(LISTGROUP_RESPONSE).await.unwrap();

            let (mut third, _) = listener.accept().await.unwrap();
            third.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = third.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LISTGROUP 1-\r\n");
            third.write_all(LISTGROUP_RESPONSE).await.unwrap();

            let (mut fourth, _) = listener.accept().await.unwrap();
            fourth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fourth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LISTGROUP alt.test 1-10\r\n");
            fourth.write_all(LISTGROUP_RESPONSE).await.unwrap();

            let (mut fifth, _) = listener.accept().await.unwrap();
            fifth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = fifth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"LAST\r\n");
            fifth.write_all(LAST_RESPONSE).await.unwrap();

            let (mut sixth, _) = listener.accept().await.unwrap();
            sixth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = sixth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"NEXT\r\n");
            sixth.write_all(NEXT_RESPONSE).await.unwrap();

            let (mut seventh, _) = listener.accept().await.unwrap();
            seventh.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = seventh.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"NEWGROUPS 20260101 000000 GMT\r\n");
            seventh.write_all(NEWGROUPS_RESPONSE).await.unwrap();

            let (mut eighth, _) = listener.accept().await.unwrap();
            eighth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = eighth.read(&mut request).await.unwrap();
            assert_eq!(
                &request[..read],
                b"NEWNEWS comp.lang.*,alt.test 20260101 000000\r\n"
            );
            eighth.write_all(NEWNEWS_RESPONSE).await.unwrap();
        });

        let mut group_args = test_fetch_args();
        group_args.connect = addr;
        group_args.request = FetchRequestKind::Group;
        group_args.message_id = None;
        group_args.group = Some("alt.test".to_string());
        let group = fetch_response(&group_args).await.unwrap();
        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(group.status().as_u16(), 211);

        let mut listgroup_args = test_fetch_args();
        listgroup_args.connect = addr;
        listgroup_args.request = FetchRequestKind::Listgroup;
        listgroup_args.message_id = None;
        listgroup_args.group = None;
        let listgroup = fetch_response(&listgroup_args).await.unwrap();
        assert_eq!(listgroup.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup.status().as_u16(), 211);

        let mut listgroup_range_args = test_fetch_args();
        listgroup_range_args.connect = addr;
        listgroup_range_args.request = FetchRequestKind::Listgroup;
        listgroup_range_args.message_id = None;
        listgroup_range_args.group = None;
        listgroup_range_args.selector = Some("1-".to_string());
        let listgroup_range = fetch_response(&listgroup_range_args).await.unwrap();
        assert_eq!(listgroup_range.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup_range.status().as_u16(), 211);

        let mut listgroup_group_args = test_fetch_args();
        listgroup_group_args.connect = addr;
        listgroup_group_args.request = FetchRequestKind::Listgroup;
        listgroup_group_args.message_id = None;
        listgroup_group_args.group = Some("alt.test".to_string());
        listgroup_group_args.selector = Some("1-10".to_string());
        let listgroup_group = fetch_response(&listgroup_group_args).await.unwrap();
        assert_eq!(listgroup_group.kind(), RequestKind::ListGroup);
        assert_eq!(listgroup_group.status().as_u16(), 211);

        let mut last_args = test_fetch_args();
        last_args.connect = addr;
        last_args.request = FetchRequestKind::Last;
        last_args.message_id = None;
        let last = fetch_response(&last_args).await.unwrap();
        assert_eq!(last.kind(), RequestKind::Last);
        assert_eq!(last.status().as_u16(), 223);

        let mut next_args = test_fetch_args();
        next_args.connect = addr;
        next_args.request = FetchRequestKind::Next;
        next_args.message_id = None;
        let next = fetch_response(&next_args).await.unwrap();
        assert_eq!(next.kind(), RequestKind::Next);
        assert_eq!(next.status().as_u16(), 223);

        let mut newgroups_args = test_fetch_args();
        newgroups_args.connect = addr;
        newgroups_args.request = FetchRequestKind::Newgroups;
        newgroups_args.message_id = None;
        newgroups_args.date = Some("20260101".to_string());
        newgroups_args.time = Some("000000".to_string());
        let newgroups = fetch_response(&newgroups_args).await.unwrap();
        assert_eq!(newgroups.kind(), RequestKind::NewGroups);
        assert_eq!(newgroups.status().as_u16(), 231);

        let mut newnews_args = test_fetch_args();
        newnews_args.connect = addr;
        newnews_args.request = FetchRequestKind::Newnews;
        newnews_args.message_id = None;
        newnews_args.wildmat = Some("comp.lang.*,alt.test".to_string());
        newnews_args.date = Some("20260101".to_string());
        newnews_args.time = Some("000000".to_string());
        newnews_args.gmt = false;
        let newnews = fetch_response(&newnews_args).await.unwrap();
        assert_eq!(newnews.kind(), RequestKind::NewNews);
        assert_eq!(newnews.status().as_u16(), 230);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_supports_remaining_rfc_request_kinds() {
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
            assert_eq!(&request[..read], b"AUTHINFO USER bench-user\r\n");
            fifth.write_all(AUTHINFO_RESPONSE).await.unwrap();

            let (mut sixth, _) = listener.accept().await.unwrap();
            sixth.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = sixth.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"AUTHINFO PASS bench-pass\r\n");
            sixth.write_all(AUTHINFO_RESPONSE).await.unwrap();

            let (mut seventh, _) = listener.accept().await.unwrap();
            seventh.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = seventh.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"STARTTLS\r\n");
            seventh.write_all(STARTTLS_RESPONSE).await.unwrap();
        });

        let mut post_args = test_fetch_args();
        post_args.connect = addr;
        post_args.request = FetchRequestKind::Post;
        post_args.message_id = None;
        let post = fetch_response(&post_args).await.unwrap();
        assert_eq!(post.kind(), RequestKind::Post);
        assert_eq!(post.status().as_u16(), 340);

        let mut ihave_args = test_fetch_args();
        ihave_args.connect = addr;
        ihave_args.request = FetchRequestKind::Ihave;
        ihave_args.message_id = Some("ihave@test".to_string());
        let ihave = fetch_response(&ihave_args).await.unwrap();
        assert_eq!(ihave.kind(), RequestKind::Ihave);
        assert_eq!(ihave.status().as_u16(), 335);

        let mut check_args = test_fetch_args();
        check_args.connect = addr;
        check_args.request = FetchRequestKind::Check;
        check_args.message_id = Some("check@test".to_string());
        let check = fetch_response(&check_args).await.unwrap();
        assert_eq!(check.kind(), RequestKind::Check);
        assert_eq!(check.status().as_u16(), 238);

        let mut takethis_args = test_fetch_args();
        takethis_args.connect = addr;
        takethis_args.request = FetchRequestKind::Takethis;
        takethis_args.message_id = Some("take@test".to_string());
        takethis_args.article_body = Some("Subject: Taken\r\n\r\n.dot line\r\npayload".to_string());
        let takethis = fetch_response(&takethis_args).await.unwrap();
        assert_eq!(takethis.kind(), RequestKind::TakeThis);
        assert_eq!(takethis.status().as_u16(), 239);

        let mut auth_user_args = test_fetch_args();
        auth_user_args.connect = addr;
        auth_user_args.request = FetchRequestKind::AuthinfoUser;
        auth_user_args.message_id = None;
        auth_user_args.auth_value = Some("bench-user".to_string());
        let auth_user = fetch_response(&auth_user_args).await.unwrap();
        assert_eq!(auth_user.kind(), RequestKind::AuthInfoUser);
        assert_eq!(auth_user.status().as_u16(), 281);

        let mut auth_pass_args = test_fetch_args();
        auth_pass_args.connect = addr;
        auth_pass_args.request = FetchRequestKind::AuthinfoPass;
        auth_pass_args.message_id = None;
        auth_pass_args.auth_value = Some("bench-pass".to_string());
        let auth_pass = fetch_response(&auth_pass_args).await.unwrap();
        assert_eq!(auth_pass.kind(), RequestKind::AuthInfoPass);
        assert_eq!(auth_pass.status().as_u16(), 281);

        let mut starttls_args = test_fetch_args();
        starttls_args.connect = addr;
        starttls_args.request = FetchRequestKind::Starttls;
        starttls_args.message_id = None;
        let starttls = fetch_response(&starttls_args).await.unwrap();
        assert_eq!(starttls.kind(), RequestKind::StartTls);
        assert_eq!(starttls.status().as_u16(), 382);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_rejects_missing_message_id_for_message_id_only_requests() {
        let mut args = test_fetch_args();
        args.request = FetchRequestKind::Ihave;
        args.message_id = None;
        assert!(matches!(
            fetch_response(&args).await.unwrap_err(),
            ClientError::MissingMessageId
        ));
    }

    #[tokio::test]
    async fn fetch_response_rejects_range_selector_for_article_style_requests() {
        let mut args = test_fetch_args();
        args.message_id = None;
        args.selector = Some("1-10".to_string());

        assert!(matches!(
            fetch_response(&args).await.unwrap_err(),
            ClientError::InvalidArticleSelector
        ));
    }

    #[tokio::test]
    async fn fetch_response_supports_article_request_current_and_numeric_selector() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap(), 16, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            first.write_all(b"201 fetch ready\r\n").await.unwrap();

            let mut request = [0_u8; 128];
            let read = first.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"ARTICLE\r\n");
            first
                .write_all(
                    b"220 1 <article.1@nntpbench.local> article follows\r\nSubject: Current\r\n\r\nbody\r\n.\r\n",
                )
                .await
                .unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            second.write_all(b"201 fetch ready\r\n").await.unwrap();
            let read = second.read(&mut request).await.unwrap();
            assert_eq!(&request[..read], b"BODY 42\r\n");
            second
                .write_all(b"222 42 <article.42@nntpbench.local> body follows\r\nbody\r\n.\r\n")
                .await
                .unwrap();
        });

        let mut current_args = test_fetch_args();
        current_args.connect = addr;
        current_args.message_id = None;
        current_args.selector = None;
        let current = fetch_response(&current_args).await.unwrap();
        assert_eq!(current.kind(), RequestKind::Article);
        assert_eq!(current.status().as_u16(), 220);

        let mut numeric_args = test_fetch_args();
        numeric_args.connect = addr;
        numeric_args.request = FetchRequestKind::Body;
        numeric_args.message_id = None;
        numeric_args.selector = Some("42".to_string());
        let numeric = fetch_response(&numeric_args).await.unwrap();
        assert_eq!(numeric.kind(), RequestKind::Body);
        assert_eq!(numeric.status().as_u16(), 222);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_response_rejects_missing_header_query_arguments() {
        let mut args = test_fetch_args();
        args.request = FetchRequestKind::Hdr;
        args.message_id = None;
        args.header = None;
        args.selector = Some("1-10".to_string());
        assert!(matches!(
            fetch_response(&args).await.unwrap_err(),
            ClientError::MissingHeaderName
        ));

        args.header = Some("Subject".to_string());
        args.selector = None;
        assert!(matches!(
            fetch_response(&args).await.unwrap_err(),
            ClientError::MissingArticleSelector
        ));
    }

    #[tokio::test]
    async fn fetch_response_rejects_missing_overview_selector() {
        let mut args = test_fetch_args();
        args.request = FetchRequestKind::Over;
        args.message_id = None;
        args.selector = None;

        assert!(matches!(
            fetch_response(&args).await.unwrap_err(),
            ClientError::MissingArticleSelector
        ));
    }

    #[tokio::test]
    async fn fetch_response_rejects_missing_group_argument() {
        let mut args = test_fetch_args();
        args.request = FetchRequestKind::Group;
        args.message_id = None;
        args.group = None;

        assert!(matches!(
            fetch_response(&args).await.unwrap_err(),
            ClientError::MissingGroupName
        ));
    }

    #[tokio::test]
    async fn fetch_response_rejects_invalid_listgroup_range() {
        let mut args = test_fetch_args();
        args.request = FetchRequestKind::Listgroup;
        args.message_id = None;
        args.group = Some("alt.test".to_string());
        args.selector = Some("-10".to_string());

        assert!(matches!(
            fetch_response(&args).await.unwrap_err(),
            ClientError::InvalidListGroupRange
        ));
    }

    #[tokio::test]
    async fn fetch_response_rejects_missing_discovery_arguments() {
        let mut newgroups_args = test_fetch_args();
        newgroups_args.request = FetchRequestKind::Newgroups;
        newgroups_args.message_id = None;
        newgroups_args.date = None;
        newgroups_args.time = Some("000000".to_string());
        assert!(matches!(
            fetch_response(&newgroups_args).await.unwrap_err(),
            ClientError::MissingDate
        ));

        newgroups_args.date = Some("20260101".to_string());
        newgroups_args.time = None;
        assert!(matches!(
            fetch_response(&newgroups_args).await.unwrap_err(),
            ClientError::MissingTime
        ));

        let mut newnews_args = test_fetch_args();
        newnews_args.request = FetchRequestKind::Newnews;
        newnews_args.message_id = None;
        newnews_args.wildmat = None;
        newnews_args.date = Some("20260101".to_string());
        newnews_args.time = Some("000000".to_string());
        assert!(matches!(
            fetch_response(&newnews_args).await.unwrap_err(),
            ClientError::MissingWildmat
        ));
    }

    #[tokio::test]
    async fn fetch_response_rejects_missing_remaining_rfc_arguments() {
        let mut takethis_args = test_fetch_args();
        takethis_args.request = FetchRequestKind::Takethis;
        takethis_args.article_body = None;
        assert!(matches!(
            fetch_response(&takethis_args).await.unwrap_err(),
            ClientError::MissingArticleBody
        ));

        let mut auth_args = test_fetch_args();
        auth_args.request = FetchRequestKind::AuthinfoUser;
        auth_args.message_id = None;
        auth_args.auth_value = None;
        assert!(matches!(
            fetch_response(&auth_args).await.unwrap_err(),
            ClientError::MissingAuthInfoValue
        ));

        auth_args.auth_value = Some("bench user".to_string());
        assert!(matches!(
            fetch_response(&auth_args).await.unwrap_err(),
            ClientError::InvalidAuthInfoValue
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
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                8
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
    async fn read_command_batch_preserves_full_length_message_id_without_allocating_per_command() {
        // RFC 3977 section 3.1 caps command lines at 512 octets including CRLF.
        // Keep message-id parsing tied to the full command line, not a smaller
        // internal shortcut.
        let long_message = format!("<{}@example.test>", "a".repeat(480));
        let wire = format!("ARTICLE {long_message}\r\n");
        assert!(wire.len() <= MAX_COMMAND_LINE_BYTES);

        let (mut client, server) = tokio::io::duplex(1024);
        client.write_all(wire.as_bytes()).await.unwrap();

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                8
            )
            .await
            .unwrap()
        );
        assert_eq!(
            command_message_id(&command_batch[0], &command_lines)
                .unwrap()
                .as_str(),
            long_message
        );
    }

    #[tokio::test]
    async fn read_command_batch_returns_false_on_clean_eof() {
        let (client, server) = tokio::io::duplex(1024);
        drop(client);
        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        assert!(
            !read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                8
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
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                8
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
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                2
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
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                2
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
                Some(&mut command_lines),
                &mut command_batch,
                2
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
        // RFC 3977 section 3.1.1 uses "." CRLF as the multiline block terminator.
        // A complete TAKETHIS command body must be consumed as part of the TAKETHIS command
        // so the following command is parsed only after the terminating dot line:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"TAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\r\n.\r\nQUIT\r\n")
            .await
            .unwrap();

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                8
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
    async fn read_command_batch_accepts_long_takethis_body_lines() {
        // RFC 3977 section 3.1 limits command lines, but section 3.1.1
        // multiline data blocks are terminated by "." CRLF rather than a
        // 512-octet command-line bound.
        let (mut client, server) = tokio::io::duplex(2048);
        let mut wire = b"TAKETHIS <take@test>\r\nHeader: value\r\n".to_vec();
        wire.extend(std::iter::repeat_n(b'x', MAX_COMMAND_LINE_BYTES + 128));
        wire.extend_from_slice(b"\r\n.\r\nQUIT\r\n");
        client.write_all(&wire).await.unwrap();

        let mut reader = BufReader::with_capacity(128, server);
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        assert!(
            read_command_batch(
                &mut reader,
                &mut command_buf,
                Some(&mut command_lines),
                &mut command_batch,
                8
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
    async fn read_command_batch_rejects_lf_only_takethis_terminator() {
        // RFC 3977 section 3.1.1 requires the TAKETHIS data block terminator to be
        // "." CRLF. A bare-LF "." line must not end the body or expose the next command:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let (mut client, server) = tokio::io::duplex(1024);
        client
            .write_all(b"TAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\n.\nQUIT\r\n")
            .await
            .unwrap();
        drop(client);

        let mut reader = BufReader::with_capacity(1024, server);
        let mut command_buf = [0; MAX_COMMAND_LINE_BYTES];
        let mut command_lines = CommandLineBatch::default();
        let mut command_batch = CommandBatch::new();

        let err = read_command_batch(
            &mut reader,
            &mut command_buf,
            Some(&mut command_lines),
            &mut command_batch,
            8,
        )
        .await
        .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(command_batch.is_empty());
    }

    #[tokio::test]
    async fn serve_session_returns_pipelined_responses_in_order_and_counts_stats() {
        let (output, stats) = run_session_with_input(
            test_config(),
            b"GROUP alt.test\r\nBODY 2\r\nARTICLE 1\r\nCAPABILITIES\r\nDATE\r\nMODE READER\r\nHEAD 1\r\nQUIT\r\n",
        )
        .await;

        let text = String::from_utf8_lossy(&output);
        let group = text.find("211 3 1 3 alt.test").unwrap();
        let body = text.find("222 2").unwrap();
        let article = text.find("220 1").unwrap();
        let caps = text.find("101 Capability").unwrap();
        let date = text.find("111 ").unwrap();
        let mode = text.find("201 posting not permitted").unwrap();
        let head = text.find("221 1 <article.1@nntpbench.local>").unwrap();
        let quit = text.find("205 closing").unwrap();
        assert!(group < body);
        assert!(body < article);
        assert!(article < caps);
        assert!(caps < date);
        assert!(date < mode);
        assert!(mode < head);
        assert!(head < quit);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 8);
        assert_eq!(snapshot.body_requests, 1);
        assert_eq!(snapshot.article_requests, 2);
        assert_eq!(snapshot.pipeline_batches, 1);
        assert_eq!(snapshot.bytes_sent, output.len() as u64);
    }

    #[tokio::test]
    async fn serve_session_reads_article_responses_from_article_dir() {
        let article_dir = unique_temp_dir("server-articles");
        let response =
            b"220 2 <article.2@test> article follows\r\nSubject: stored\r\n\r\nbody\r\n.\r\n";
        let path = article_id_tree_path(&article_dir, 2);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, response).unwrap();
        let message_response =
            b"220 1 <stored@ngPost> article follows\r\nSubject: message\r\n\r\nbody\r\n.\r\n";
        let message_id = MessageId::from_borrowed("<stored@ngPost>").unwrap();
        let message_path = message_id_tree_path(&article_dir, &message_id);
        fs::create_dir_all(message_path.parent().unwrap()).unwrap();
        fs::write(&message_path, message_response).unwrap();

        let mut args = test_args();
        args.article_dir = Some(article_dir.clone());
        let (output, stats) = run_session_with_input(
            Arc::new(ServerConfig::from_args(args)),
            b"GROUP alt.test\r\nARTICLE 2\r\nARTICLE <stored@ngPost>\r\nARTICLE 3\r\nQUIT\r\n",
        )
        .await;

        assert_eq!(
            output,
            [
                GREETING,
                GROUP_RESPONSE,
                response,
                message_response,
                b"423 no article with that number\r\n".as_slice(),
                QUIT_RESPONSE
            ]
            .concat()
        );
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 5);
        assert_eq!(snapshot.article_requests, 3);
        assert_eq!(snapshot.bytes_sent, output.len() as u64);

        fs::remove_dir_all(article_dir).unwrap();
    }

    #[tokio::test]
    async fn serve_session_supports_backend_negotiation_commands() {
        let (output, stats) =
            run_session_with_input(test_config(), b"MODE READER\r\nDATE\r\nQUIT\r\n").await;

        let prefix = [GREETING, MODE_READER_RESPONSE].concat();
        assert!(output.starts_with(&prefix));
        let date_start = prefix.len();
        assert_date_response(&output[date_start..date_start + 20]);
        assert_eq!(
            &output[date_start + 20..],
            b"205 closing connection\r\n".as_slice()
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
        let (output, stats) = run_session_with_input(
            test_config(),
            b"GROUP alt.test\r\nOVER 1-10\r\nOVER <overview@test>\r\nXOVER 1-10\r\nXOVER <overview@test>\r\n",
        )
        .await;

        assert_eq!(
            output,
            [
                GREETING,
                GROUP_RESPONSE,
                OVER_RANGE_RESPONSE,
                OVER_MESSAGE_ID_RESPONSE,
                XOVER_RESPONSE,
                OVER_MESSAGE_ID_RESPONSE
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 5);
    }

    #[tokio::test]
    async fn serve_session_supports_group_navigation_commands() {
        let config = test_config();
        let article_response = build_selected_article_response(2, config.article_bytes);
        let (output, stats) = run_session_with_input(
            config,
            b"GROUP alt.test\r\nLISTGROUP\r\nLISTGROUP alt.test\r\nLISTGROUP 1-\r\nLISTGROUP 2-3\r\nLISTGROUP alt.test 1-10\r\nARTICLE 2\r\nLAST\r\nNEXT\r\n",
        )
        .await;

        let mut expected = Vec::new();
        expected.extend_from_slice(GREETING);
        expected.extend_from_slice(GROUP_RESPONSE);
        expected.extend_from_slice(LISTGROUP_RESPONSE);
        expected.extend_from_slice(LISTGROUP_RESPONSE);
        expected.extend_from_slice(LISTGROUP_RESPONSE);
        expected.extend_from_slice(LISTGROUP_2_3_RESPONSE);
        expected.extend_from_slice(LISTGROUP_RESPONSE);
        expected.extend_from_slice(&article_response);
        expected.extend_from_slice(LAST_RESPONSE);
        expected.extend_from_slice(NEXT_RESPONSE);
        assert_eq!(output, expected);
        assert_eq!(stats.snapshot().commands, 9);
    }

    #[tokio::test]
    async fn serve_session_supports_discovery_commands() {
        let (output, stats) = run_session_with_input(
            test_config(),
            b"NEWGROUPS 20260101 000000 GMT\r\nNEWNEWS alt.* 20260101 000000\r\n",
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
                b"440 posting not permitted\r\n".as_slice(),
                b"435 article not wanted\r\n",
                b"502 command unavailable\r\n",
                b"502 command unavailable\r\n",
                b"483 command unavailable until TLS has been negotiated\r\n",
                b"483 command unavailable until TLS has been negotiated\r\n",
                b"502 command unavailable\r\n",
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
            b"CAPABILITIES\r\nBODY <large-body@test>\r\n",
        )
        .await;

        let text = String::from_utf8_lossy(&output);
        assert!(text.find("101 Capability").unwrap() < text.find("222 0").unwrap());
        assert!(output.ends_with(b"\r\n.\r\n"));
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 2);
        assert_eq!(snapshot.body_requests, 1);
    }

    #[tokio::test]
    async fn serve_session_flushes_small_response_when_client_closes() {
        let (output, stats) = run_session_with_input(test_config(), b"CAPABILITIES\r\n").await;
        assert!(output.starts_with(GREETING));
        assert!(output.ends_with(CAPABILITIES_RESPONSE));
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
        let (output, stats) = run_session_with_input(
            Arc::new(ServerConfig::from_args(args)),
            b"BODY <large-body@test>\r\n",
        )
        .await;
        assert!(output.starts_with(GREETING));
        assert!(output[GREETING.len()..].starts_with(b"222 0 <large-body@test>"));
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
        // RFC 3977 section 3.1 makes CRLF the only line terminator:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        // The batch scanner must not keep the old LF-only command behavior.
        let consumed = for_each_request_line_in_batch(b"DATE\nMODE READER\n", 8, |request| {
            batch.push(request.kind());
        });
        assert_eq!(consumed, 0);
        assert!(batch.is_empty());

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
    fn for_each_request_line_in_batch_skips_incomplete_takethis_until_body_finishes() {
        // RFC 3977 section 3.1.1 requires a complete "." CRLF terminator before a
        // TAKETHIS body is complete. Without that dot line, the batch scanner must not
        // expose the TAKETHIS command or any following bytes as separate commands:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut batch = Vec::with_capacity(4);
        let input = b"TAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\r\nQUIT\r\n";
        let consumed = for_each_request_line_in_batch(input, 8, |request| {
            batch.push(request.kind());
        });

        assert_eq!(consumed, 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn for_each_request_line_in_batch_requires_crlf_dot_terminator_for_takethis() {
        // RFC 3977 section 3.1.1 names "." CRLF as the multiline terminator. The batch
        // scanner must not treat "." LF as a complete TAKETHIS body terminator even if a
        // later command line has a valid CRLF:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
        let mut batch = Vec::with_capacity(4);
        let input = b"TAKETHIS <take@test>\r\nHeader: value\r\n\r\nbody\n.\nQUIT\r\n";
        let consumed = for_each_request_line_in_batch(input, 8, |request| {
            batch.push(request.kind());
        });

        assert_eq!(consumed, 0);
        assert!(batch.is_empty());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn batch_scanner_requires_exact_rfc_takethis_dot_terminator(
            mut prefix in vec(dangerous_takethis_body_bytes(), 0..32),
            mut suffix in vec(dangerous_takethis_body_bytes(), 0..32),
            near_miss in prop::sample::select(vec![
                b".\n".to_vec(),
                b".\r".to_vec(),
                b".\r ".to_vec(),
                b"..\r\n".to_vec(),
                b".body\r\n".to_vec(),
                b"\n.\r\n".to_vec(),
                b"\r.\r\n".to_vec(),
                b"\r\n.\n".to_vec(),
            ]),
        ) {
            // RFC 3977 section 3.1.1 reserves only "." CRLF as the command-continuation
            // terminator. Generated TAKETHIS bodies containing bare-LF, incomplete-CR, or
            // dot-stuffed near misses must not be exposed as complete commands:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            remove_rfc_dot_terminators(&mut prefix);
            remove_rfc_dot_terminators(&mut suffix);
            let mut input = b"TAKETHIS <take@test>\r\n".to_vec();
            input.push(b'x');
            input.extend_from_slice(&prefix);
            input.extend_from_slice(&near_miss);
            input.extend_from_slice(&suffix);
            prop_assume!(find_dot_terminated_block_end(&input, b"TAKETHIS <take@test>\r\n".len()).is_none());
            input.extend_from_slice(b"QUIT\r\n");

            let mut batch = Vec::with_capacity(4);
            let consumed = for_each_request_line_in_batch(&input, 8, |request| {
                batch.push(request.kind());
            });

            prop_assert_eq!(consumed, 0, "{:?}", input);
            prop_assert!(batch.is_empty(), "{:?}", input);
        }

        #[test]
        fn batch_scanner_consumes_takethis_through_first_rfc_dot_terminator(
            mut body in vec(dangerous_takethis_body_bytes(), 0..48),
            trailer in "[A-Za-z0-9 ._-]{0,16}",
        ) {
            // RFC 3977 section 3.1.1 says the first "." CRLF line ends a TAKETHIS
            // continuation. The scanner must consume exactly through that dot line and
            // then allow the following request line to be parsed separately:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            remove_rfc_dot_terminators(&mut body);
            body.insert(0, b'x');
            body.push(b'x');
            let mut input = b"TAKETHIS <take@test>\r\n".to_vec();
            input.extend_from_slice(&body);
            input.extend_from_slice(b"\r\n");
            input.extend_from_slice(b".\r\nQUIT\r\n");
            input.extend_from_slice(trailer.as_bytes());

            let mut batch = Vec::with_capacity(4);
            let consumed = for_each_request_line_in_batch(&input, 8, |request| {
                batch.push(request.kind());
            });

            prop_assert_eq!(batch, vec![RequestKind::TakeThis, RequestKind::Quit]);
            prop_assert_eq!(
                consumed,
                b"TAKETHIS <take@test>\r\n".len() + body.len() + b"\r\n.\r\nQUIT\r\n".len()
            );
        }
    }

    #[test]
    fn process_request_to_buffer_counts_and_returns_quit() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"ARTICLE <bench@nntpbench.local>\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(process_request_to_buffer(
            RequestLine::parse(b"QUIT\r\n"),
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
            RequestLine::parse(b"LIST ACTIVE\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LIST ACTIVE.TIMES\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LIST ACTIVE.TIMES comp.lang.*\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LIST NEWSGROUPS\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LIST NEWSGROUPS comp.lang.*\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LIST OVERVIEW.FMT\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LIST HEADERS\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LIST DISTRIB.PATS\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"HELP\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"MODE READER\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"DATE\r\n"),
            &config,
            &stats,
            &mut output,
        ));

        let expected_prefix = [
            LIST_RESPONSE,
            LIST_ACTIVE_TIMES_RESPONSE,
            LIST_ACTIVE_TIMES_COMP_RESPONSE,
            LIST_NEWSGROUPS_RESPONSE,
            LIST_NEWSGROUPS_COMP_RESPONSE,
            LIST_OVERVIEW_FMT_RESPONSE,
            LIST_HEADERS_RESPONSE,
            LIST_DISTRIB_PATS_RESPONSE,
            HELP_RESPONSE,
            MODE_READER_RESPONSE,
        ]
        .concat();
        assert!(output.starts_with(&expected_prefix));
        assert_date_response(&output[expected_prefix.len()..]);
        assert_eq!(stats.snapshot().commands, 11);
    }

    #[test]
    fn process_request_to_buffer_supports_remaining_rfc_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"POST\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"IHAVE <article@test>\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"CHECK <article@test>\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"TAKETHIS <article@test>\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"AUTHINFO USER bench\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"AUTHINFO PASS bench\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"AUTHINFO SASL BENCH\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"STARTTLS\r\n"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(
            output,
            [
                b"440 posting not permitted\r\n".as_slice(),
                b"435 article not wanted\r\n".as_slice(),
                b"502 command unavailable\r\n",
                b"502 command unavailable\r\n",
                b"483 command unavailable until TLS has been negotiated\r\n",
                b"483 command unavailable until TLS has been negotiated\r\n",
                b"502 command unavailable\r\n",
                b"502 command unavailable\r\n",
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
            RequestLine::parse(b"OVER 1-10\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"OVER <overview@test>\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"XOVER 1-10\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"XOVER <overview@test>\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"HDR Subject 1\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"HDR Message-ID 1\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"XHDR Subject 1\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"XHDR Message-ID 1\r\n"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(
            output,
            [
                OVER_RANGE_RESPONSE,
                OVER_MESSAGE_ID_RESPONSE,
                XOVER_RESPONSE,
                OVER_MESSAGE_ID_RESPONSE,
                HDR_SUBJECT_1_RESPONSE,
                HDR_MESSAGE_ID_1_RESPONSE,
                XHDR_SUBJECT_1_RESPONSE,
                XHDR_MESSAGE_ID_1_RESPONSE,
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 8);
    }

    #[test]
    fn process_request_to_buffer_supports_group_navigation_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"GROUP alt.test\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LISTGROUP\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LISTGROUP comp.lang.rust\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LISTGROUP 1-\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LISTGROUP 2-3\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LISTGROUP example.valid\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"LAST\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"NEXT\r\n"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(
            output,
            [
                GROUP_RESPONSE,
                LISTGROUP_RESPONSE,
                LISTGROUP_COMP_RESPONSE,
                LISTGROUP_RESPONSE,
                LISTGROUP_2_3_RESPONSE,
                b"411 no such newsgroup\r\n".as_slice(),
                LAST_RESPONSE,
                NEXT_RESPONSE
            ]
            .concat()
        );
        assert_eq!(stats.snapshot().commands, 8);
    }

    #[test]
    fn process_request_to_buffer_supports_discovery_commands() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"NEWGROUPS 231231 235959 GMT\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"NEWNEWS alt.* 231231 235959 GMT\r\n"),
            &config,
            &stats,
            &mut output,
        ));

        assert_eq!(output, [NEWGROUPS_RESPONSE, NEWNEWS_RESPONSE].concat());
        assert_eq!(stats.snapshot().commands, 2);
    }

    #[test]
    fn process_request_to_buffer_supports_body_head_stat_capabilities_and_unknown() {
        let config = test_config();
        let stats = Stats::new();
        let mut output = Vec::new();

        assert!(!process_request_to_buffer(
            RequestLine::parse(b"BODY <body@test>\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"CAPABILITIES\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"HEAD 1\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"STAT 1\r\n"),
            &config,
            &stats,
            &mut output,
        ));
        assert!(!process_request_to_buffer(
            RequestLine::parse(b"XYZZY 1\r\n"),
            &config,
            &stats,
            &mut output,
        ));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.commands, 5);
        assert_eq!(snapshot.article_requests, 2);
        assert_eq!(snapshot.body_requests, 1);
        assert!(output.starts_with(b"222 "));
        assert!(
            output
                .windows(HEAD_RESPONSE.len())
                .any(|window| window == HEAD_RESPONSE)
        );
        assert!(
            output
                .windows(STAT_RESPONSE.len())
                .any(|window| window == STAT_RESPONSE)
        );
        assert!(output.ends_with(b"500 unknown command\r\n"));
    }

    #[test]
    fn synchronous_hot_path_units_do_not_allocate() {
        let config = ServerConfig::from_args(test_args());
        let stats = Stats::new();
        let mut output = FixedWrite::<4096>::new();
        let segments = SegmentSet {
            ids: vec![MessageId::from_shared(Arc::<str>::from("<segment@test>")).unwrap()]
                .into_boxed_slice(),
        };
        let article_target = ArticleDownloadTarget::MessageId(
            MessageId::from_shared(Arc::<str>::from("<article-path@test>")).unwrap(),
        );
        let mut article_path = PathBuf::with_capacity(2048);
        article_download_target_path_into(
            &mut article_path,
            Path::new("/tmp/nntpbench-hot-path"),
            &article_target,
        )
        .unwrap();
        let long_message = format!("<{}@example.test>", "a".repeat(480));
        let long_command = format!("ARTICLE {long_message}\r\n");
        assert!(long_command.len() <= MAX_COMMAND_LINE_BYTES);
        let mut command_lines = CommandLineBatch::default();

        assert_no_allocations(
            "request parse, scan, serialize, and generated response",
            || {
                let request = RequestLine::parse(b"BODY 1\r\n");
                assert_eq!(request.kind(), RequestKind::Body);

                let mut seen = 0;
                let consumed =
                    for_each_request_line_in_batch(b"ARTICLE 1\r\nBODY 2\r\n", 8, |_| {
                        seen += 1;
                    });
                assert_eq!(seen, 2);
                assert_eq!(consumed, b"ARTICLE 1\r\nBODY 2\r\n".len());

                output.clear();
                Request::body_number(42).unwrap().write_wire_to(&mut output);
                assert_eq!(output.as_slice(), b"BODY 42\r\n");

                let segment_request = client_request_for_command(
                    42,
                    0,
                    ClientCommandMix::Article,
                    Some(&segments),
                    0,
                    1,
                )
                .unwrap();
                assert_eq!(
                    segment_request.message_id().map(MessageId::as_str),
                    Some("<segment@test>")
                );

                let article_ref = article_ref_for_download_target(&article_target).unwrap();
                assert_eq!(
                    article_ref.message_id().map(MessageId::as_str),
                    Some("<article-path@test>")
                );
                article_download_target_path_into(
                    &mut article_path,
                    Path::new("/tmp/nntpbench-hot-path"),
                    &article_target,
                )
                .unwrap();

                let command = parse_command_line(long_command.as_bytes(), 0);
                command_lines.copy_line(0, long_command.as_bytes());
                assert_eq!(
                    command_message_id(&command, &command_lines)
                        .unwrap()
                        .as_str(),
                    long_message
                );

                output.clear();
                assert!(!process_request_to_buffer(
                    request,
                    &config,
                    &stats,
                    &mut output,
                ));
                assert!(output.as_slice().starts_with(BODY_RESPONSE_PREFIX));
                assert!(output.as_slice().ends_with(DOT_TERMINATOR));
            },
        );
    }

    #[test]
    fn generated_response_descriptor_does_not_allocate() {
        assert_no_allocations("generated response descriptor construction", || {
            let response = GeneratedResponse::new(BODY_RESPONSE_PREFIX, 1024 * 1024);
            assert_eq!(response.prefix, BODY_RESPONSE_PREFIX);
            assert_eq!(response.target_bytes, 1024 * 1024);
        });
    }
}
