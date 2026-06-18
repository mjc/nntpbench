//! Client NNTP protocol helpers.

use std::borrow::Cow;
use std::fmt::{self, Write as FmtWrite};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::sync::Arc;

#[cfg(test)]
use crate::terminator::append_crlf;
use crate::terminator::{
    BoundedResponseLineStatus, DOT_TERMINATOR, crlf_normalized_payload_lines,
    detect_bounded_response_line_end, find_dot_terminated_block, strip_complete_crlf_line,
};

pub mod article;

pub use article::{Article, ArticleNumber, ArticleParseError, HeaderIter, Headers};

pub const MAX_ARTICLE_NUMBER: u64 = 2_147_483_647;
/// RFC 3977 section 3.1 command lines and response initial lines are limited
/// to 512 octets, including the terminating CRLF pair.
pub(crate) const MAX_INITIAL_RESPONSE_LINE_BYTES: usize = 512;

/// Raw NNTP status code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StatusCode(u16);

impl StatusCode {
    /// Parse the leading 3-digit NNTP status code.
    #[must_use]
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 3 {
            return None;
        }

        let d0 = data[0].wrapping_sub(b'0');
        let d1 = data[1].wrapping_sub(b'0');
        let d2 = data[2].wrapping_sub(b'0');
        if d0 > 9 || d1 > 9 || d2 > 9 {
            return None;
        }

        Some(Self(
            u16::from(d0) * 100 + u16::from(d1) * 10 + u16::from(d2),
        ))
    }

    /// Return the raw numeric value.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Whether this status code is informational.
    #[must_use]
    pub const fn is_informational(self) -> bool {
        let code = self.0;
        code >= 100 && code < 200
    }

    /// Whether this status code is successful.
    #[must_use]
    pub const fn is_success(self) -> bool {
        let code = self.0;
        code >= 200 && code < 400
    }

    /// Whether this status code is an error.
    #[must_use]
    pub const fn is_error(self) -> bool {
        let code = self.0;
        code >= 400 && code < 600
    }
}

/// Borrowed whole NNTP response frame parsed from bytes received from the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseFrame<'a> {
    kind: RequestKind,
    descriptor: ResponseDescriptor,
    bytes: &'a [u8],
    status_line: &'a [u8],
    content: &'a [u8],
    terminator: &'a [u8],
    content_start: usize,
    content_end: usize,
    status: StatusCode,
    consumed: usize,
}

impl<'a> ResponseFrame<'a> {
    /// Parse a complete RFC 3977 response frame from the beginning of `buffer`.
    ///
    /// RFC 3977 section 9.4 defines the two response shapes: a simple response
    /// is just the initial response line, while a multi-line response is that
    /// line plus a dot-terminated data block from section 3.1.1.
    #[must_use]
    pub fn parse(kind: RequestKind, buffer: &'a [u8]) -> ResponseFrameParse<'a> {
        let status_line_end =
            match detect_bounded_response_line_end(buffer, MAX_INITIAL_RESPONSE_LINE_BYTES) {
                BoundedResponseLineStatus::CompleteAt(end) => end,
                BoundedResponseLineStatus::NeedMore => return ResponseFrameParse::NeedMore,
                BoundedResponseLineStatus::Invalid | BoundedResponseLineStatus::TooLong => {
                    return ResponseFrameParse::Invalid;
                }
            };

        let Some(status) = StatusCode::parse(buffer) else {
            return ResponseFrameParse::Invalid;
        };
        if status_line_end < 5 || buffer.get(3) != Some(&b' ') {
            return ResponseFrameParse::Invalid;
        }
        let descriptor = ResponseDescriptor::for_request_status(kind, status);
        if matches!(descriptor.framing(), ResponseFraming::Unexpected) {
            return ResponseFrameParse::Invalid;
        }

        let (consumed, content, terminator) = if descriptor.framing().is_multiline() {
            let Some(block) = find_dot_terminated_block(buffer, status_line_end) else {
                return ResponseFrameParse::NeedMore;
            };
            (block.block_end(), block.content(), block.terminator())
        } else {
            if find_dot_terminated_block(buffer, status_line_end).is_some() {
                return ResponseFrameParse::Invalid;
            }
            (
                status_line_end,
                &buffer[status_line_end..status_line_end],
                &buffer[status_line_end..status_line_end],
            )
        };

        ResponseFrameParse::Complete(Self {
            kind,
            descriptor,
            bytes: &buffer[..consumed],
            status_line: &buffer[..status_line_end],
            content,
            terminator,
            content_start: status_line_end,
            content_end: status_line_end + content.len(),
            status,
            consumed,
        })
    }

    #[must_use]
    pub const fn kind(self) -> RequestKind {
        self.kind
    }

    #[must_use]
    pub const fn descriptor(self) -> ResponseDescriptor {
        self.descriptor
    }

    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.bytes
    }

    #[must_use]
    pub const fn status_line(self) -> &'a [u8] {
        self.status_line
    }

    #[must_use]
    pub const fn content(self) -> &'a [u8] {
        self.content
    }

    #[must_use]
    pub const fn terminator(self) -> &'a [u8] {
        self.terminator
    }

    #[must_use]
    pub const fn content_start(self) -> usize {
        self.content_start
    }

    #[must_use]
    pub const fn content_end(self) -> usize {
        self.content_end
    }

    #[must_use]
    pub const fn status(self) -> StatusCode {
        self.status
    }

    #[must_use]
    pub const fn consumed(self) -> usize {
        self.consumed
    }
}

/// Parse status for a borrowed whole NNTP response frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFrameParse<'a> {
    Complete(ResponseFrame<'a>),
    NeedMore,
    Invalid,
}

/// Stateless protocol response decoder for callers that already retain pending bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResponseFrameDecoder {
    kind: RequestKind,
}

impl ResponseFrameDecoder {
    #[must_use]
    pub(crate) const fn new(kind: RequestKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub(crate) fn decode<'a>(self, buffer: &'a [u8]) -> ResponseFrameParse<'a> {
        ResponseFrame::parse(self.kind, buffer)
    }
}

/// Protocol status-line result for streaming callers that cannot retain a full frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResponseInitial {
    status: StatusCode,
    descriptor: ResponseDescriptor,
}

impl ResponseInitial {
    #[must_use]
    pub(crate) fn parse(kind: RequestKind, buffer: &[u8]) -> ResponseInitialParse {
        match detect_bounded_response_line_end(buffer, MAX_INITIAL_RESPONSE_LINE_BYTES) {
            BoundedResponseLineStatus::CompleteAt(_) => {
                let Some(status) = StatusCode::parse(buffer) else {
                    return ResponseInitialParse::Invalid;
                };
                ResponseInitialParse::Complete(Self {
                    status,
                    descriptor: ResponseDescriptor::for_request_status(kind, status),
                })
            }
            BoundedResponseLineStatus::NeedMore => ResponseInitialParse::NeedMore,
            BoundedResponseLineStatus::Invalid | BoundedResponseLineStatus::TooLong => {
                ResponseInitialParse::Invalid
            }
        }
    }

    #[must_use]
    pub(crate) const fn status(self) -> StatusCode {
        self.status
    }

    #[must_use]
    pub(crate) const fn descriptor(self) -> ResponseDescriptor {
        self.descriptor
    }
}

/// Parse status for a streaming response initial line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseInitialParse {
    Complete(ResponseInitial),
    NeedMore,
    Invalid,
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::terminator::find_crlf_line_end;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::string::string_regex;

    fn message_id_atom_strategy() -> BoxedStrategy<String> {
        string_regex("[A-Za-z0-9][A-Za-z0-9_-]{0,15}")
            .unwrap()
            .boxed()
    }

    fn message_id_inner_strategy() -> BoxedStrategy<String> {
        (
            message_id_atom_strategy(),
            message_id_atom_strategy(),
            message_id_atom_strategy(),
        )
            .prop_map(|(local, domain, tld)| format!("{local}@{domain}.{tld}"))
            .boxed()
    }

    fn message_id_strategy() -> BoxedStrategy<String> {
        message_id_inner_strategy()
            .prop_map(|inner| format!("<{inner}>"))
            .boxed()
    }

    fn invalid_message_id_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            message_id_inner_strategy(),
            message_id_inner_strategy().prop_map(|inner| format!("<{inner} >")),
            message_id_inner_strategy().prop_map(|inner| format!("<{inner}")),
            Just("<>".to_string()),
        ]
        .boxed()
    }

    fn article_number_strategy() -> BoxedStrategy<u64> {
        (1_u64..=2_147_483_647_u64).boxed()
    }

    fn article_number_token_strategy() -> BoxedStrategy<String> {
        article_number_strategy()
            .prop_map(|number| number.to_string())
            .boxed()
    }

    fn header_name_strategy() -> BoxedStrategy<String> {
        string_regex("[A-Za-z0-9-]{1,20}").unwrap().boxed()
    }

    fn invalid_header_name_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just(String::new()),
            header_name_strategy().prop_map(|name| format!("{name}:")),
            header_name_strategy().prop_map(|name| format!("{name} bad")),
        ]
        .boxed()
    }

    fn article_selector_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            message_id_strategy(),
            article_number_token_strategy(),
            article_range_strategy(),
        ]
        .boxed()
    }

    fn invalid_article_selector_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just(String::new()),
            article_selector_strategy().prop_map(|selector| format!("{selector} ")),
            Just("1 2".to_string()),
            Just("1\r\nQUIT".to_string()),
        ]
        .boxed()
    }

    fn article_range_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            article_number_token_strategy(),
            (
                article_number_token_strategy(),
                article_number_token_strategy()
            )
                .prop_map(|(left, right)| {
                    let left = left.parse::<u64>().unwrap();
                    let right = right.parse::<u64>().unwrap();
                    let (start, end) = if left <= right {
                        (left, right)
                    } else {
                        (right, left)
                    };
                    format!("{start}-{end}")
                }),
            article_number_token_strategy().prop_map(|start| format!("{start}-")),
        ]
        .boxed()
    }

    fn listgroup_range_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            article_number_token_strategy(),
            (
                article_number_token_strategy(),
                article_number_token_strategy()
            )
                .prop_map(|(start, end)| format!("{start}-{end}")),
            article_number_token_strategy().prop_map(|start| format!("{start}-")),
        ]
        .boxed()
    }

    fn invalid_listgroup_range_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just(String::new()),
            Just("0".to_string()),
            Just("-10".to_string()),
            Just("1-10-20".to_string()),
            Just("1 10".to_string()),
            Just("2147483648".to_string()),
        ]
        .boxed()
    }

    fn group_name_strategy() -> BoxedStrategy<String> {
        vec(string_regex("[A-Za-z0-9-]{1,8}").unwrap(), 1..=4)
            .prop_map(|segments| segments.join("."))
            .boxed()
    }

    fn invalid_group_name_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just(String::new()),
            group_name_strategy().prop_map(|name| format!(".{name}")),
            group_name_strategy().prop_map(|name| format!("{name}.")),
            group_name_strategy().prop_map(|name| format!("{name}..bad")),
            group_name_strategy().prop_map(|name| format!("{name}*")),
        ]
        .boxed()
    }

    fn auth_info_value_strategy() -> BoxedStrategy<String> {
        string_regex("[!-~]{1,20}").unwrap().boxed()
    }

    fn nntp_date_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            (0_u8..=99, 1_u8..=12)
                .prop_flat_map(|(year, month)| {
                    let max_day = valid_days_in_month(u16::from(year) + 2000, month);
                    (Just(year), Just(month), 1_u8..=max_day)
                })
                .prop_map(|(year, month, day)| format!("{year:02}{month:02}{day:02}")),
            (2000_u16..=2099, 1_u8..=12)
                .prop_flat_map(|(year, month)| {
                    let max_day = valid_days_in_month(year, month);
                    (Just(year), Just(month), 1_u8..=max_day)
                })
                .prop_map(|(year, month, day)| format!("{year:04}{month:02}{day:02}")),
        ]
        .boxed()
    }

    fn valid_days_in_month(year: u16, month: u8) -> u8 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if is_leap_year(year) => 29,
            2 => 28,
            _ => unreachable!("strategy only generates valid months"),
        }
    }

    fn invalid_nntp_date_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just("20261301".to_string()),
            Just("20260132".to_string()),
            Just("2026010".to_string()),
            Just("20ab0101".to_string()),
        ]
        .boxed()
    }

    fn nntp_time_strategy() -> BoxedStrategy<String> {
        (0_u8..=23, 0_u8..=59, 0_u8..=59)
            .prop_map(|(hour, minute, second)| format!("{hour:02}{minute:02}{second:02}"))
            .boxed()
    }

    fn invalid_nntp_time_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just("240000".to_string()),
            Just("126100".to_string()),
            Just("125961".to_string()),
            Just("12596".to_string()),
        ]
        .boxed()
    }

    fn wildmat_pattern_strategy() -> BoxedStrategy<String> {
        string_regex("[A-Za-z0-9*?._-]{1,12}").unwrap().boxed()
    }

    fn wildmat_strategy() -> BoxedStrategy<String> {
        vec(wildmat_pattern_strategy(), 1..=4)
            .prop_map(|patterns| patterns.join(","))
            .boxed()
    }

    fn invalid_wildmat_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just(String::new()),
            wildmat_strategy().prop_map(|wildmat| format!("{wildmat},,")),
            Just("!".to_string()),
            Just("alt.test,!".to_string()),
            Just("alt test".to_string()),
        ]
        .boxed()
    }

    fn mixed_case(input: &str, mask: &[bool]) -> String {
        let mut bytes = input.as_bytes().to_vec();
        for (byte, uppercase) in bytes.iter_mut().zip(mask.iter().copied()) {
            if byte.is_ascii_alphabetic() && uppercase {
                *byte ^= 0x20;
            }
        }
        String::from_utf8(bytes).unwrap()
    }

    #[derive(Debug, Clone, Copy)]
    enum ArticleFamily {
        Article,
        Body,
        Head,
        Stat,
    }

    impl ArticleFamily {
        fn as_verb(self) -> &'static str {
            match self {
                Self::Article => "ARTICLE",
                Self::Body => "BODY",
                Self::Head => "HEAD",
                Self::Stat => "STAT",
            }
        }

        fn as_kind(self) -> RequestKind {
            match self {
                Self::Article => RequestKind::Article,
                Self::Body => RequestKind::Body,
                Self::Head => RequestKind::Head,
                Self::Stat => RequestKind::Stat,
            }
        }
    }

    fn article_family_strategy() -> BoxedStrategy<ArticleFamily> {
        prop_oneof![
            Just(ArticleFamily::Article),
            Just(ArticleFamily::Body),
            Just(ArticleFamily::Head),
            Just(ArticleFamily::Stat),
        ]
        .boxed()
    }

    #[derive(Debug, Clone)]
    enum SelectorCase {
        Current,
        Number(String),
        MessageId(String),
    }

    fn selector_case_strategy() -> BoxedStrategy<SelectorCase> {
        prop_oneof![
            Just(SelectorCase::Current),
            article_number_token_strategy().prop_map(SelectorCase::Number),
            message_id_strategy().prop_map(SelectorCase::MessageId),
        ]
        .boxed()
    }

    fn build_article_family_request(
        family: ArticleFamily,
        selector: &SelectorCase,
    ) -> Request<'static> {
        match (family, selector) {
            (ArticleFamily::Article, SelectorCase::Current) => Request::article_current(),
            (ArticleFamily::Article, SelectorCase::Number(number)) => {
                Request::article_number(number.parse().unwrap()).unwrap()
            }
            (ArticleFamily::Article, SelectorCase::MessageId(message_id)) => {
                Request::article(message_id).unwrap()
            }
            (ArticleFamily::Body, SelectorCase::Current) => Request::body_current(),
            (ArticleFamily::Body, SelectorCase::Number(number)) => {
                Request::body_number(number.parse().unwrap()).unwrap()
            }
            (ArticleFamily::Body, SelectorCase::MessageId(message_id)) => {
                Request::body(message_id).unwrap()
            }
            (ArticleFamily::Head, SelectorCase::Current) => Request::head_current(),
            (ArticleFamily::Head, SelectorCase::Number(number)) => {
                Request::head_number(number.parse().unwrap()).unwrap()
            }
            (ArticleFamily::Head, SelectorCase::MessageId(message_id)) => {
                Request::head(message_id).unwrap()
            }
            (ArticleFamily::Stat, SelectorCase::Current) => Request::stat_current(),
            (ArticleFamily::Stat, SelectorCase::Number(number)) => {
                Request::stat_number(number.parse().unwrap()).unwrap()
            }
            (ArticleFamily::Stat, SelectorCase::MessageId(message_id)) => {
                Request::stat(message_id).unwrap()
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum FixedCommand {
        Post,
        StartTls,
        List,
        Help,
        Capabilities,
        Date,
        ModeReader,
        Quit,
        Last,
        Next,
    }

    impl FixedCommand {
        fn canonical_line(self) -> &'static str {
            match self {
                Self::Post => "POST",
                Self::StartTls => "STARTTLS",
                Self::List => "LIST",
                Self::Help => "HELP",
                Self::Capabilities => "CAPABILITIES",
                Self::Date => "DATE",
                Self::ModeReader => "MODE READER",
                Self::Quit => "QUIT",
                Self::Last => "LAST",
                Self::Next => "NEXT",
            }
        }

        fn kind(self) -> RequestKind {
            match self {
                Self::Post => RequestKind::Post,
                Self::StartTls => RequestKind::StartTls,
                Self::List => RequestKind::List,
                Self::Help => RequestKind::Help,
                Self::Capabilities => RequestKind::Capabilities,
                Self::Date => RequestKind::Date,
                Self::ModeReader => RequestKind::ModeReader,
                Self::Quit => RequestKind::Quit,
                Self::Last => RequestKind::Last,
                Self::Next => RequestKind::Next,
            }
        }

        fn request(self) -> Request<'static> {
            match self {
                Self::Post => Request::post(),
                Self::StartTls => Request::starttls(),
                Self::List => Request::list(),
                Self::Help => Request::help(),
                Self::Capabilities => Request::capabilities(),
                Self::Date => Request::date(),
                Self::ModeReader => Request::mode_reader(),
                Self::Quit => Request::quit(),
                Self::Last => Request::last(),
                Self::Next => Request::next(),
            }
        }
    }

    fn fixed_command_strategy() -> BoxedStrategy<FixedCommand> {
        prop_oneof![
            Just(FixedCommand::Post),
            Just(FixedCommand::StartTls),
            Just(FixedCommand::List),
            Just(FixedCommand::Help),
            Just(FixedCommand::Capabilities),
            Just(FixedCommand::Date),
            Just(FixedCommand::ModeReader),
            Just(FixedCommand::Quit),
            Just(FixedCommand::Last),
            Just(FixedCommand::Next),
        ]
        .boxed()
    }

    #[derive(Debug, Clone)]
    enum ListCase {
        Active(Option<String>),
        ActiveTimes(Option<String>),
        Newsgroups(Option<String>),
        OverviewFmt,
        Headers,
        DistribPats,
    }

    fn list_case_strategy() -> BoxedStrategy<ListCase> {
        prop_oneof![
            prop_oneof![Just(None), wildmat_strategy().prop_map(Some),].prop_map(ListCase::Active),
            prop_oneof![Just(None), wildmat_strategy().prop_map(Some),]
                .prop_map(ListCase::ActiveTimes),
            prop_oneof![Just(None), wildmat_strategy().prop_map(Some),]
                .prop_map(ListCase::Newsgroups),
            Just(ListCase::OverviewFmt),
            Just(ListCase::Headers),
            Just(ListCase::DistribPats),
        ]
        .boxed()
    }

    impl ListCase {
        fn canonical_line(&self) -> String {
            match self {
                Self::Active(None) => "LIST ACTIVE".to_string(),
                Self::Active(Some(wildmat)) => format!("LIST ACTIVE {wildmat}"),
                Self::ActiveTimes(None) => "LIST ACTIVE.TIMES".to_string(),
                Self::ActiveTimes(Some(wildmat)) => format!("LIST ACTIVE.TIMES {wildmat}"),
                Self::Newsgroups(None) => "LIST NEWSGROUPS".to_string(),
                Self::Newsgroups(Some(wildmat)) => format!("LIST NEWSGROUPS {wildmat}"),
                Self::OverviewFmt => "LIST OVERVIEW.FMT".to_string(),
                Self::Headers => "LIST HEADERS".to_string(),
                Self::DistribPats => "LIST DISTRIB.PATS".to_string(),
            }
        }

        fn kind(&self) -> RequestKind {
            match self {
                Self::Active(_) => RequestKind::ListActive,
                Self::ActiveTimes(_) => RequestKind::ListActiveTimes,
                Self::Newsgroups(_) => RequestKind::ListNewsgroups,
                Self::OverviewFmt => RequestKind::ListOverviewFmt,
                Self::Headers => RequestKind::ListHeaders,
                Self::DistribPats => RequestKind::ListDistribPats,
            }
        }

        fn request(&self) -> Request<'static> {
            match self {
                Self::Active(None) => Request::list_active(),
                Self::Active(Some(wildmat)) => Request::list_active_wildmat(wildmat).unwrap(),
                Self::ActiveTimes(None) => Request::list_active_times(),
                Self::ActiveTimes(Some(wildmat)) => {
                    Request::list_active_times_wildmat(wildmat).unwrap()
                }
                Self::Newsgroups(None) => Request::list_newsgroups(),
                Self::Newsgroups(Some(wildmat)) => {
                    Request::list_newsgroups_wildmat(wildmat).unwrap()
                }
                Self::OverviewFmt => Request::list_overview_fmt(),
                Self::Headers => Request::list_headers(),
                Self::DistribPats => Request::list_distrib_pats(),
            }
        }
    }

    #[derive(Debug, Clone)]
    enum AuthCase {
        User(String),
        Pass(String),
    }

    fn auth_case_strategy() -> BoxedStrategy<AuthCase> {
        prop_oneof![
            auth_info_value_strategy().prop_map(AuthCase::User),
            auth_info_value_strategy().prop_map(AuthCase::Pass),
        ]
        .boxed()
    }

    impl AuthCase {
        fn canonical_line(&self) -> String {
            match self {
                Self::User(value) => format!("AUTHINFO USER {value}"),
                Self::Pass(value) => format!("AUTHINFO PASS {value}"),
            }
        }

        fn kind(&self) -> RequestKind {
            match self {
                Self::User(_) => RequestKind::AuthInfoUser,
                Self::Pass(_) => RequestKind::AuthInfoPass,
            }
        }

        fn request(&self) -> Request<'static> {
            match self {
                Self::User(value) => Request::authinfo_user(value).unwrap(),
                Self::Pass(value) => Request::authinfo_pass(value).unwrap(),
            }
        }
    }

    #[derive(Debug, Clone)]
    enum TransferCase {
        Ihave(String),
        Check(String),
        TakeThis(String, Vec<u8>),
    }

    fn payload_line_strategy() -> BoxedStrategy<String> {
        string_regex("[A-Za-z0-9 ._!?-]{0,20}").unwrap().boxed()
    }

    fn transfer_payload_strategy() -> BoxedStrategy<Vec<u8>> {
        vec(payload_line_strategy(), 0..=5)
            .prop_map(|lines| lines.join("\n").into_bytes())
            .boxed()
    }

    fn transfer_case_strategy() -> BoxedStrategy<TransferCase> {
        prop_oneof![
            message_id_strategy().prop_map(TransferCase::Ihave),
            message_id_strategy().prop_map(TransferCase::Check),
            (message_id_strategy(), transfer_payload_strategy())
                .prop_map(|(message_id, payload)| TransferCase::TakeThis(message_id, payload)),
        ]
        .boxed()
    }

    impl TransferCase {
        fn first_line(&self) -> String {
            match self {
                Self::Ihave(message_id) => format!("IHAVE {message_id}"),
                Self::Check(message_id) => format!("CHECK {message_id}"),
                Self::TakeThis(message_id, _) => format!("TAKETHIS {message_id}"),
            }
        }

        fn kind(&self) -> RequestKind {
            match self {
                Self::Ihave(_) => RequestKind::Ihave,
                Self::Check(_) => RequestKind::Check,
                Self::TakeThis(_, _) => RequestKind::TakeThis,
            }
        }

        fn request(&self) -> Request<'static> {
            match self {
                Self::Ihave(message_id) => Request::ihave(message_id).unwrap(),
                Self::Check(message_id) => Request::check(message_id).unwrap(),
                Self::TakeThis(message_id, payload) => {
                    Request::takethis(message_id, payload).unwrap()
                }
            }
        }
    }

    fn expected_transfer_wire(first_line: &str, payload: &[u8]) -> Vec<u8> {
        let mut expected = Vec::new();
        expected.extend_from_slice(first_line.as_bytes());
        append_crlf(&mut expected);
        if payload.is_empty() {
            expected.extend_from_slice(DOT_TERMINATOR);
            return expected;
        }

        for line in crlf_normalized_payload_lines(payload) {
            if line.starts_with(b".") {
                expected.push(b'.');
            }
            expected.extend_from_slice(line);
            append_crlf(&mut expected);
        }
        expected.extend_from_slice(DOT_TERMINATOR);
        expected
    }

    fn dangerous_transfer_bytes() -> impl Strategy<Value = u8> {
        prop_oneof![
            Just(b'\r'),
            Just(b'\n'),
            Just(b'.'),
            Just(b' '),
            b'0'..=b'9',
            b'a'..=b'z',
        ]
    }

    fn request_kind_strategy() -> BoxedStrategy<RequestKind> {
        prop_oneof![
            Just(RequestKind::Article),
            Just(RequestKind::Body),
            Just(RequestKind::Head),
            Just(RequestKind::Stat),
            Just(RequestKind::ListActive),
            Just(RequestKind::ListActiveTimes),
            Just(RequestKind::ListNewsgroups),
            Just(RequestKind::ListOverviewFmt),
            Just(RequestKind::ListHeaders),
            Just(RequestKind::ListDistribPats),
            Just(RequestKind::Group),
            Just(RequestKind::ListGroup),
            Just(RequestKind::Last),
            Just(RequestKind::Next),
            Just(RequestKind::List),
            Just(RequestKind::Help),
            Just(RequestKind::Capabilities),
            Just(RequestKind::Date),
            Just(RequestKind::Over),
            Just(RequestKind::Xover),
            Just(RequestKind::Hdr),
            Just(RequestKind::Xhdr),
            Just(RequestKind::NewGroups),
            Just(RequestKind::NewNews),
            Just(RequestKind::Post),
            Just(RequestKind::Ihave),
            Just(RequestKind::Check),
            Just(RequestKind::TakeThis),
            Just(RequestKind::AuthInfoUser),
            Just(RequestKind::AuthInfoPass),
            Just(RequestKind::AuthInfo),
            Just(RequestKind::StartTls),
            Just(RequestKind::ModeReader),
            Just(RequestKind::Quit),
            Just(RequestKind::Unknown),
        ]
        .boxed()
    }

    fn expected_multiline(kind: RequestKind, code: u16) -> bool {
        if (400..600).contains(&code) {
            return false;
        }

        match kind {
            RequestKind::Article => code == 220,
            RequestKind::Head => code == 221,
            RequestKind::Body => code == 222,
            RequestKind::ListGroup => code == 211,
            RequestKind::Help => code == 100,
            RequestKind::Capabilities => code == 101,
            RequestKind::List
            | RequestKind::ListActive
            | RequestKind::ListActiveTimes
            | RequestKind::ListNewsgroups
            | RequestKind::ListOverviewFmt
            | RequestKind::ListHeaders
            | RequestKind::ListDistribPats => code == 215,
            RequestKind::Over | RequestKind::Xover => code == 224,
            RequestKind::Hdr => code == 225,
            RequestKind::Xhdr => code == 221,
            RequestKind::NewNews => code == 230,
            RequestKind::NewGroups => code == 231,
            RequestKind::Unknown => status_implies_multiline(code),
            _ => false,
        }
    }

    fn ascii_suffix_strategy() -> BoxedStrategy<Vec<u8>> {
        vec(0x20_u8..=0x7e_u8, 0..=12).boxed()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1024))]

        #[test]
        fn status_code_parse_accepts_any_three_digit_prefix(
            d0 in 0_u8..=9,
            d1 in 0_u8..=9,
            d2 in 0_u8..=9,
            suffix in ascii_suffix_strategy(),
        ) {
            let mut input = vec![b'0' + d0, b'0' + d1, b'0' + d2];
            input.extend_from_slice(&suffix);
            let expected = u16::from(d0) * 100 + u16::from(d1) * 10 + u16::from(d2);
            prop_assert_eq!(StatusCode::parse(&input), Some(StatusCode(expected)));
        }

        #[test]
        fn status_code_parse_rejects_non_digit_prefix_bytes(
            pos in 0_usize..3,
            bad in prop_oneof![0_u8..=47_u8, 58_u8..=255_u8],
            suffix in ascii_suffix_strategy(),
        ) {
            let mut input = vec![b'2', b'0', b'0'];
            input[pos] = bad;
            input.extend_from_slice(&suffix);
            prop_assert_eq!(StatusCode::parse(&input), None);
        }

        #[test]
        fn message_id_valid_inputs_round_trip(value in message_id_strategy()) {
            let borrowed = MessageId::from_borrowed(&value).unwrap();
            let owned = MessageId::from_str_or_wrap(&value).unwrap();
            let reborrowed = MessageId::from_borrowed(borrowed.as_str()).unwrap();
            prop_assert_eq!(borrowed.as_str(), value.as_str());
            prop_assert_eq!(owned.as_str(), value.as_str());
            prop_assert_eq!(reborrowed.as_str(), value.as_str());
        }

        #[test]
        fn message_id_invalid_inputs_are_rejected(value in invalid_message_id_strategy()) {
            prop_assert!(MessageId::from_borrowed(&value).is_err());
        }

        #[test]
        fn article_refs_follow_numeric_and_message_id_selectors(
            number in article_number_strategy(),
            message_id in message_id_strategy(),
        ) {
            let message_ref = ArticleRef::from_selector(&message_id).unwrap();
            prop_assert_eq!(ArticleRef::from_number(number).unwrap(), ArticleRef::Number(number));
            prop_assert_eq!(
                ArticleRef::from_selector(number.to_string()).unwrap(),
                ArticleRef::Number(number),
            );
            prop_assert_eq!(message_ref.message_id().unwrap().as_str(), message_id.as_str());
        }

        #[test]
        fn header_names_round_trip_and_invalid_forms_fail(
            valid in header_name_strategy(),
            invalid in invalid_header_name_strategy(),
        ) {
            let borrowed = HeaderName::from_borrowed(&valid).unwrap();
            let owned = HeaderName::from_owned(&valid).unwrap();
            prop_assert_eq!(borrowed.as_str(), valid.as_str());
            prop_assert_eq!(owned.as_str(), valid.as_str());
            prop_assert!(HeaderName::from_borrowed(&invalid).is_err());
        }

        #[test]
        fn article_selectors_round_trip_and_invalid_forms_fail(
            valid in article_selector_strategy(),
            invalid in invalid_article_selector_strategy(),
        ) {
            let borrowed = ArticleSelector::from_borrowed(&valid).unwrap();
            let owned = ArticleSelector::from_owned(&valid).unwrap();
            prop_assert_eq!(borrowed.as_str(), valid.as_str());
            prop_assert_eq!(owned.as_str(), valid.as_str());
            prop_assert!(ArticleSelector::from_borrowed(&invalid).is_err());
        }

        #[test]
        fn listgroup_ranges_round_trip_and_invalid_forms_fail(
            valid in listgroup_range_strategy(),
            invalid in invalid_listgroup_range_strategy(),
        ) {
            let borrowed = ListGroupRange::from_borrowed(&valid).unwrap();
            let owned = ListGroupRange::from_owned(&valid).unwrap();
            prop_assert_eq!(borrowed.as_str(), valid.as_str());
            prop_assert_eq!(owned.as_str(), valid.as_str());
            prop_assert!(ListGroupRange::from_borrowed(&invalid).is_err());
        }

        #[test]
        fn group_names_round_trip_and_invalid_forms_fail(
            valid in group_name_strategy(),
            invalid in invalid_group_name_strategy(),
        ) {
            let borrowed = GroupName::from_borrowed(&valid).unwrap();
            let owned = GroupName::from_owned(&valid).unwrap();
            prop_assert_eq!(borrowed.as_str(), valid.as_str());
            prop_assert_eq!(owned.as_str(), valid.as_str());
            prop_assert!(GroupName::from_borrowed(&invalid).is_err());
        }

        #[test]
        fn auth_info_values_round_trip(valid in auth_info_value_strategy()) {
            let borrowed = AuthInfoValue::from_borrowed(&valid).unwrap();
            let owned = AuthInfoValue::from_owned(&valid).unwrap();
            prop_assert_eq!(borrowed.as_str(), valid.as_str());
            prop_assert_eq!(owned.as_str(), valid.as_str());
        }

        #[test]
        fn discovery_arguments_accept_valid_and_reject_invalid_forms(
            date in nntp_date_strategy(),
            time in nntp_time_strategy(),
            wildmat in wildmat_strategy(),
            invalid_date in invalid_nntp_date_strategy(),
            invalid_time in invalid_nntp_time_strategy(),
            invalid_wildmat in invalid_wildmat_strategy(),
        ) {
            let valid_date = NntpDate::from_borrowed(&date).unwrap();
            let valid_time = NntpTime::from_borrowed(&time).unwrap();
            let valid_wildmat = Wildmat::from_borrowed(&wildmat).unwrap();
            prop_assert_eq!(valid_date.as_str(), date.as_str());
            prop_assert_eq!(valid_time.as_str(), time.as_str());
            prop_assert_eq!(valid_wildmat.as_str(), wildmat.as_str());
            prop_assert!(NntpDate::from_borrowed(&invalid_date).is_err());
            prop_assert!(NntpTime::from_borrowed(&invalid_time).is_err());
            prop_assert!(Wildmat::from_borrowed(&invalid_wildmat).is_err());
        }

        #[test]
        fn request_line_article_family_preserves_kind_args_and_message_id(
            family in article_family_strategy(),
            selector in selector_case_strategy(),
            mask in vec(any::<bool>(), 4..=7),
        ) {
            // RFC 3977 section 3.1 defines commands as CRLF-terminated lines:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let verb = mixed_case(family.as_verb(), &mask);
            let selector_text = match &selector {
                SelectorCase::Current => String::new(),
                SelectorCase::Number(number) | SelectorCase::MessageId(number) => number.clone(),
            };
            let line = if selector_text.is_empty() {
                format!("{verb}\r\n")
            } else {
                format!("{verb} {selector_text}\r\n")
            };

            let parsed = RequestLine::parse(line.as_bytes());
            prop_assert_eq!(parsed.kind(), family.as_kind());
            prop_assert_eq!(parsed.verb(), verb.as_bytes());
            prop_assert_eq!(parsed.args(), selector_text.as_bytes());
            match selector {
                SelectorCase::MessageId(message_id) => {
                    let parsed_message_id = parsed.message_id().unwrap();
                    prop_assert_eq!(parsed_message_id.as_str(), message_id.as_str());
                }
                SelectorCase::Current | SelectorCase::Number(_) => {
                    prop_assert!(parsed.message_id().is_none());
                }
            }
        }

        #[test]
        fn request_line_requires_rfc_crlf_framing(
            prefix in "[A-Za-z0-9][A-Za-z0-9 ._-]{0,32}",
            bad_separator in prop::sample::select(vec![
                b"".to_vec(),
                b"\n".to_vec(),
                b"\r".to_vec(),
                b"\r ".to_vec(),
                b"\r\r".to_vec(),
            ]),
            trailer in "[A-Za-z0-9 ._-]{0,16}",
        ) {
            // RFC 3977 section 3.1 defines command lines as CRLF-terminated. A direct
            // request-line parse must reject unframed input, bare LF, bare CR, and malformed
            // CRLF recovery instead of normalizing or resynchronizing:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let mut line = prefix.into_bytes();
            line.extend_from_slice(&bad_separator);
            line.extend_from_slice(trailer.as_bytes());
            if !line.ends_with(crate::CRLF) {
                prop_assert_eq!(RequestLine::parse(&line).kind(), RequestKind::Unknown);
            }
        }

        #[test]
        fn article_family_requests_preserve_rfc_wire_selector_forms(
            family in article_family_strategy(),
            selector in selector_case_strategy(),
        ) {
            let request = build_article_family_request(family, &selector);
            let mut wire = Vec::new();
            request.write_wire_to(&mut wire);
            let parsed = RequestLine::parse(&wire);
            let expected_args = match &selector {
                SelectorCase::Current => "",
                SelectorCase::Number(number) | SelectorCase::MessageId(number) => number.as_str(),
            };

            prop_assert_eq!(parsed.kind(), family.as_kind());
            prop_assert_eq!(parsed.verb(), family.as_verb().as_bytes());
            prop_assert_eq!(parsed.args(), expected_args.as_bytes());
            prop_assert!(wire.ends_with(b"\r\n"));
            prop_assert_eq!(wire.windows(2).filter(|window| *window == b"\r\n").count(), 1);
        }

        #[test]
        fn overview_and_header_requests_preserve_rfc_wire_forms(
            selector in article_selector_strategy(),
            header in header_name_strategy(),
        ) {
            let over = Request::over(&selector).unwrap();
            let xover = Request::xover(&selector).unwrap();
            let hdr = Request::hdr(&header, &selector).unwrap();
            let xhdr = Request::xhdr(&header, &selector).unwrap();

            for (request, expected_kind, expected_args) in [
                (over, RequestKind::Over, selector.clone()),
                (xover, RequestKind::Xover, selector.clone()),
                (hdr, RequestKind::Hdr, format!("{header} {selector}")),
                (xhdr, RequestKind::Xhdr, format!("{header} {selector}")),
            ] {
                let mut wire = Vec::new();
                request.write_wire_to(&mut wire);
                let parsed = RequestLine::parse(&wire);
                prop_assert_eq!(parsed.kind(), expected_kind);
                prop_assert_eq!(std::str::from_utf8(parsed.args()).unwrap(), expected_args);
            }
        }

        #[test]
        fn listgroup_and_discovery_requests_preserve_rfc_wire_forms(
            group in group_name_strategy(),
            range in listgroup_range_strategy(),
            date in nntp_date_strategy(),
            time in nntp_time_strategy(),
            wildmat in wildmat_strategy(),
            gmt in any::<bool>(),
        ) {
            let requests = [
                (
                    Request::listgroup(&group).unwrap(),
                    format!("LISTGROUP {group}\r\n"),
                ),
                (
                    Request::listgroup_range(&range).unwrap(),
                    format!("LISTGROUP {range}\r\n"),
                ),
                (
                    Request::listgroup_group_range(&group, &range).unwrap(),
                    format!("LISTGROUP {group} {range}\r\n"),
                ),
                (
                    Request::newgroups(&date, &time, gmt).unwrap(),
                    if gmt {
                        format!("NEWGROUPS {date} {time} GMT\r\n")
                    } else {
                        format!("NEWGROUPS {date} {time}\r\n")
                    },
                ),
                (
                    Request::newnews(&wildmat, &date, &time, gmt).unwrap(),
                    if gmt {
                        format!("NEWNEWS {wildmat} {date} {time} GMT\r\n")
                    } else {
                        format!("NEWNEWS {wildmat} {date} {time}\r\n")
                    },
                ),
            ];

            for (request, expected_wire) in requests {
                let mut wire = Vec::new();
                request.write_wire_to(&mut wire);
                prop_assert_eq!(std::str::from_utf8(&wire).unwrap(), expected_wire);
            }
        }

        #[test]
        fn fixed_commands_parse_case_insensitively_and_serialize_canonically(
            command in fixed_command_strategy(),
            mask in vec(any::<bool>(), 3..=12),
        ) {
            // RFC 3977 section 3.1 defines commands as CRLF-terminated lines:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let line = format!("{}\r\n", mixed_case(command.canonical_line(), &mask));
            let parsed = RequestLine::parse(line.as_bytes());
            prop_assert_eq!(parsed.kind(), command.kind());
            let mut wire = Vec::new();
            command.request().write_wire_to(&mut wire);
            prop_assert_eq!(
                std::str::from_utf8(&wire).unwrap(),
                format!("{}\r\n", command.canonical_line())
            );
        }

        #[test]
        fn list_command_matrix_parses_case_insensitively_and_serializes_canonically(
            list_case in list_case_strategy(),
            mask in vec(any::<bool>(), 4..=32),
        ) {
            let canonical = list_case.canonical_line();
            // RFC 3977 section 3.1 defines commands as CRLF-terminated lines:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let line = format!("{}\r\n", mixed_case(&canonical, &mask));
            let parsed = RequestLine::parse(line.as_bytes());
            prop_assert_eq!(parsed.kind(), list_case.kind());
            let mut wire = Vec::new();
            list_case.request().write_wire_to(&mut wire);
            prop_assert_eq!(std::str::from_utf8(&wire).unwrap(), format!("{canonical}\r\n"));
        }

        #[test]
        fn authinfo_commands_parse_case_insensitively_and_serializes_canonically(
            auth_case in auth_case_strategy(),
            mask in vec(any::<bool>(), 13..=32),
        ) {
            let canonical = auth_case.canonical_line();
            // RFC 3977 section 3.1 defines commands as CRLF-terminated lines:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let line = format!("{}\r\n", mixed_case(&canonical, &mask));
            let parsed = RequestLine::parse(line.as_bytes());
            prop_assert_eq!(parsed.kind(), auth_case.kind());
            let mut wire = Vec::new();
            auth_case.request().write_wire_to(&mut wire);
            prop_assert_eq!(std::str::from_utf8(&wire).unwrap(), format!("{canonical}\r\n"));
        }

        #[test]
        fn transfer_commands_preserve_message_id_and_wire_forms(
            transfer_case in transfer_case_strategy(),
            mask in vec(any::<bool>(), 5..=24),
        ) {
            // RFC 3977 section 3.1.1 defines command continuations such as TAKETHIS as
            // multiline data blocks: lines are CRLF-terminated, dot-prefixed data lines are
            // dot-stuffed, and the block ends with the single "." CRLF terminator:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let first_line = transfer_case.first_line();
            let (verb, rest) = first_line
                .split_once(' ')
                .map_or((first_line.as_str(), ""), |(verb, rest)| (verb, rest));
            let line = if rest.is_empty() {
                format!("{}\r\n", mixed_case(verb, &mask))
            } else {
                format!("{} {rest}\r\n", mixed_case(verb, &mask))
            };
            let parsed = RequestLine::parse(line.as_bytes());
            prop_assert_eq!(parsed.kind(), transfer_case.kind());
            let parsed_message_id = parsed.message_id().unwrap();
            let mut wire = Vec::new();
            let request = transfer_case.request();
            request.write_wire_to(&mut wire);
            prop_assert_eq!(parsed_message_id.as_str(), request.message_id().unwrap().as_str());

            match &transfer_case {
                TransferCase::Ihave(_) | TransferCase::Check(_) => {
                    prop_assert_eq!(std::str::from_utf8(&wire).unwrap(), format!("{first_line}\r\n"));
                }
                TransferCase::TakeThis(_, payload) => {
                    prop_assert_eq!(wire, expected_transfer_wire(&first_line, payload));
                }
            }
        }

        #[test]
        fn takethis_wire_normalizes_lines_dot_stuffs_and_has_one_final_terminator(
            message_id in message_id_strategy(),
            payload in vec(dangerous_transfer_bytes(), 0..96),
        ) {
            // RFC 3977 section 3.1.1 requires multiline block lines to end with CRLF,
            // forbids bare CR/LF inside those lines, requires dot-stuffing for data lines
            // beginning with ".", and reserves a single "." CRLF line as the terminator:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let request = Request::takethis(&message_id, &payload).unwrap();
            let mut wire = Vec::new();
            request.write_wire_to(&mut wire);

            let command_end = find_crlf_line_end(&wire, 0).expect("command CRLF");
            let expected_command = format!("TAKETHIS {message_id}\r\n");
            prop_assert_eq!(&wire[..command_end], expected_command.as_bytes());
            prop_assert!(wire.ends_with(b".\r\n"));

            let mut line_start = command_end;
            let mut terminator_count = 0;
            while line_start < wire.len() {
                let crlf_end = find_crlf_line_end(&wire, line_start).expect("data line CRLF");
                let line_end = crlf_end - crate::CRLF.len();
                let line = &wire[line_start..line_end];
                prop_assert!(!line.contains(&b'\r'), "{wire:?}");
                prop_assert!(!line.contains(&b'\n'), "{wire:?}");
                if line == b"." {
                    terminator_count += 1;
                    prop_assert_eq!(line_end + 2, wire.len(), "{:?}", wire);
                }
                line_start = crlf_end;
            }

            prop_assert_eq!(terminator_count, 1, "{:?}", wire);

            for line in crlf_normalized_payload_lines(&payload) {
                if line.starts_with(b".") {
                    let mut stuffed = Vec::with_capacity(line.len() + 1);
                    stuffed.push(b'.');
                    stuffed.extend_from_slice(line);
                    prop_assert!(
                        wire.windows(stuffed.len()).any(|window| window == stuffed.as_slice()),
                        "missing stuffed line {:?} in {:?}",
                        stuffed,
                        wire,
                    );
                }
            }
        }

        #[test]
        fn multiline_response_expectations_match_command_matrix(
            kind in request_kind_strategy(),
            code in 100_u16..600,
        ) {
            let status = StatusCode(code);
            prop_assert_eq!(kind.expects_multiline_response(status), expected_multiline(kind, code));
        }

        #[test]
        fn response_iterator_matches_descriptor_lookup_for_known_request_statuses(
            kind in request_kind_strategy(),
        ) {
            for descriptor in responses_for_request(kind) {
                prop_assert_eq!(
                    ResponseDescriptor::for_request_status(kind, descriptor.status_code()),
                    descriptor,
                    "{:?} {}",
                    kind,
                    descriptor.status,
                );
            }
        }

        #[test]
        fn response_framing_lookup_matches_rfc_matrix(
            kind in request_kind_strategy(),
            code in 100_u16..600,
        ) {
            let status = StatusCode(code);
            let descriptor = ResponseDescriptor::for_request_status(kind, status);
            prop_assert_eq!(descriptor.kind(), kind);
            prop_assert_eq!(descriptor.status_code(), status);
            prop_assert_eq!(
                descriptor.framing().is_multiline(),
                expected_multiline(kind, code),
            );
        }

        #[test]
        fn response_framing_iterator_has_no_duplicate_statuses_for_request(
            kind in request_kind_strategy(),
        ) {
            let mut seen = [false; 1000];
            for descriptor in responses_for_request(kind) {
                let index = usize::from(descriptor.status);
                prop_assert!(
                    !seen[index],
                    "duplicate response metadata for {kind:?} status {}",
                    descriptor.status,
                );
                seen[index] = true;
            }
        }

        #[test]
        fn request_accessors_match_constructed_payloads(
            group in group_name_strategy(),
            range in listgroup_range_strategy(),
            selector in article_selector_strategy(),
            header in header_name_strategy(),
            message_id in message_id_strategy(),
            payload in transfer_payload_strategy(),
        ) {
            let listgroup = Request::listgroup_group_range(&group, &range).unwrap();
            prop_assert_eq!(listgroup.group_name().map(GroupName::as_str), Some(group.as_str()));
            prop_assert_eq!(
                listgroup.listgroup_range_arg().map(ListGroupRange::as_str),
                Some(range.as_str())
            );

            let over = Request::over(&selector).unwrap();
            let xover = Request::xover(&selector).unwrap();
            prop_assert_eq!(
                over.overview_selector().map(ArticleSelector::as_str),
                Some(selector.as_str())
            );
            prop_assert_eq!(
                xover.overview_selector().map(ArticleSelector::as_str),
                Some(selector.as_str())
            );

            let hdr = Request::hdr(&header, &selector).unwrap();
            let xhdr = Request::xhdr(&header, &selector).unwrap();
            prop_assert_eq!(
                hdr.header_query().map(|(header, selector)| (header.as_str(), selector.as_str())),
                Some((header.as_str(), selector.as_str()))
            );
            prop_assert_eq!(
                xhdr.header_query().map(|(header, selector)| (header.as_str(), selector.as_str())),
                Some((header.as_str(), selector.as_str()))
            );

            let ihave = Request::ihave(&message_id).unwrap();
            let check = Request::check(&message_id).unwrap();
            let takethis = Request::takethis(&message_id, &payload).unwrap();
            prop_assert_eq!(ihave.message_id().map(MessageId::as_str), Some(message_id.as_str()));
            prop_assert_eq!(check.message_id().map(MessageId::as_str), Some(message_id.as_str()));
            prop_assert_eq!(takethis.message_id().map(MessageId::as_str), Some(message_id.as_str()));
            prop_assert_eq!(
                takethis.article_transfer().map(ArticleTransfer::as_bytes),
                Some(payload.as_slice())
            );
        }
    }
}

/// Validated NNTP message-id.
#[derive(Clone)]
pub struct MessageId<'a>(MessageIdStorage<'a>);

#[derive(Clone)]
enum MessageIdStorage<'a> {
    Borrowed(&'a str),
    Owned(String),
    Shared(Arc<str>),
}

impl fmt::Debug for MessageId<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MessageId").field(&self.as_str()).finish()
    }
}

impl PartialEq for MessageId<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for MessageId<'_> {}

impl Hash for MessageId<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl<'a> MessageId<'a> {
    /// Construct a borrowed message-id after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidMessageId> {
        validate_message_id(value)?;
        Ok(Self(MessageIdStorage::Borrowed(value)))
    }

    /// Construct an owned message-id, auto-wrapping in angle brackets if needed.
    pub fn from_str_or_wrap(
        value: impl AsRef<str>,
    ) -> Result<MessageId<'static>, InvalidMessageId> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(InvalidMessageId);
        }

        let wrapped = if value.starts_with('<') && value.ends_with('>') {
            value.to_owned()
        } else {
            format!("<{value}>")
        };
        validate_message_id(&wrapped)?;
        Ok(MessageId(MessageIdStorage::Owned(wrapped)))
    }

    /// Construct a shared owned message-id after validation.
    pub fn from_shared(value: Arc<str>) -> Result<MessageId<'static>, InvalidMessageId> {
        validate_message_id(&value)?;
        Ok(MessageId(MessageIdStorage::Shared(value)))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match &self.0 {
            MessageIdStorage::Borrowed(value) => value,
            MessageIdStorage::Owned(value) => value,
            MessageIdStorage::Shared(value) => value,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidMessageId;

fn validate_message_id(value: &str) -> Result<(), InvalidMessageId> {
    if value.len() < 3 || !value.starts_with('<') || !value.ends_with('>') {
        return Err(InvalidMessageId);
    }
    let inner = &value[1..value.len() - 1];
    if inner.bytes().any(|byte| {
        byte.is_ascii_whitespace() || byte.is_ascii_control() || matches!(byte, b'<' | b'>')
    }) {
        return Err(InvalidMessageId);
    }
    let Some((left, right)) = inner.split_once('@') else {
        return Err(InvalidMessageId);
    };
    if left.is_empty()
        || right.is_empty()
        || right.contains("..")
        || inner.matches('@').count() != 1
        || right.split('.').any(str::is_empty)
    {
        return Err(InvalidMessageId);
    }
    Ok(())
}

/// Validated ARTICLE/BODY/HEAD/STAT target.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArticleRef<'a> {
    Current,
    Number(u64),
    MessageId(MessageId<'a>),
}

impl<'a> ArticleRef<'a> {
    /// Construct a numeric article reference after validation.
    pub const fn from_number(value: u64) -> Result<Self, InvalidArticleRef> {
        if value == 0 || value > MAX_ARTICLE_NUMBER {
            return Err(InvalidArticleRef);
        }
        Ok(Self::Number(value))
    }

    /// Return the carried message-id when this reference is message-id based.
    #[must_use]
    pub const fn message_id(&self) -> Option<&MessageId<'a>> {
        match self {
            Self::MessageId(message_id) => Some(message_id),
            Self::Current | Self::Number(_) => None,
        }
    }
}

impl ArticleRef<'static> {
    /// Construct an owned article reference from a selector string.
    pub fn from_selector(value: impl AsRef<str>) -> Result<Self, InvalidArticleRef> {
        let value = value.as_ref();
        if value.is_empty() || value.bytes().any(|byte| byte.is_ascii_whitespace()) {
            return Err(InvalidArticleRef);
        }

        if value.bytes().all(|byte| byte.is_ascii_digit()) {
            let number = article_number_token_value(value).map_err(|_| InvalidArticleRef)?;
            return ArticleRef::from_number(number);
        }

        let message_id = MessageId::from_borrowed(value).map_err(|_| InvalidArticleRef)?;
        Ok(ArticleRef::MessageId(MessageId(MessageIdStorage::Owned(
            message_id.as_str().to_owned(),
        ))))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidArticleRef;

/// Validated NNTP header field name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HeaderName<'a>(Cow<'a, str>);

impl<'a> HeaderName<'a> {
    /// Construct a borrowed header name after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidHeaderName> {
        validate_header_name(value)?;
        Ok(Self(Cow::Borrowed(value)))
    }

    /// Construct an owned header name after validation.
    pub fn from_owned(value: impl AsRef<str>) -> Result<HeaderName<'static>, InvalidHeaderName> {
        let value = value.as_ref();
        validate_header_name(value)?;
        Ok(HeaderName(Cow::Owned(value.to_owned())))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidHeaderName;

fn validate_header_name(value: &str) -> Result<(), InvalidHeaderName> {
    let value = value.strip_prefix(':').unwrap_or(value);
    if value.is_empty() {
        return Err(InvalidHeaderName);
    }

    if value
        .bytes()
        .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'-')
    {
        return Err(InvalidHeaderName);
    }

    Ok(())
}

/// Validated article selector for range-style header queries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArticleSelector<'a>(Cow<'a, str>);

impl<'a> ArticleSelector<'a> {
    /// Construct a borrowed selector after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidArticleSelector> {
        validate_article_selector(value)?;
        Ok(Self(Cow::Borrowed(value)))
    }

    /// Construct an owned selector after validation.
    pub fn from_owned(
        value: impl AsRef<str>,
    ) -> Result<ArticleSelector<'static>, InvalidArticleSelector> {
        let value = value.as_ref();
        validate_article_selector(value)?;
        Ok(ArticleSelector(Cow::Owned(value.to_owned())))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidArticleSelector;

fn validate_article_selector(value: &str) -> Result<(), InvalidArticleSelector> {
    if value.is_empty() || value.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(InvalidArticleSelector);
    }

    if value.starts_with('<') {
        return MessageId::from_borrowed(value)
            .map(|_| ())
            .map_err(|_| InvalidArticleSelector);
    }

    if let Some((start, end)) = value.split_once('-') {
        let start = article_number_token_value(start).map_err(|_| InvalidArticleSelector)?;
        if end.is_empty() {
            return Ok(());
        }
        let end = article_number_token_value(end).map_err(|_| InvalidArticleSelector)?;
        return (start <= end).then_some(()).ok_or(InvalidArticleSelector);
    }

    article_number_token_value(value)
        .map(|_| ())
        .map_err(|_| InvalidArticleSelector)
}

/// Validated LISTGROUP range argument.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListGroupRange<'a>(Cow<'a, str>);

impl<'a> ListGroupRange<'a> {
    /// Construct a borrowed LISTGROUP range after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidListGroupRange> {
        validate_listgroup_range(value)?;
        Ok(Self(Cow::Borrowed(value)))
    }

    /// Construct an owned LISTGROUP range after validation.
    pub fn from_owned(
        value: impl AsRef<str>,
    ) -> Result<ListGroupRange<'static>, InvalidListGroupRange> {
        let value = value.as_ref();
        validate_listgroup_range(value)?;
        Ok(ListGroupRange(Cow::Owned(value.to_owned())))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidListGroupRange;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidListGroupRangeOrGroupName {
    Range(InvalidListGroupRange),
    GroupName(InvalidGroupName),
}

fn validate_listgroup_range(value: &str) -> Result<(), InvalidListGroupRange> {
    if value.is_empty() || value.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(InvalidListGroupRange);
    }

    if let Some((start, end)) = value.split_once('-') {
        article_number_token_value(start).map_err(|_| InvalidListGroupRange)?;
        if !end.is_empty() {
            article_number_token_value(end).map_err(|_| InvalidListGroupRange)?;
        }
        return Ok(());
    }

    validate_article_number_token(value).map_err(|_| InvalidListGroupRange)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InvalidArticleNumberToken;

fn validate_article_number_token(value: &str) -> Result<(), InvalidArticleNumberToken> {
    article_number_token_value(value).map(|_| ())
}

fn article_number_token_value(value: &str) -> Result<u64, InvalidArticleNumberToken> {
    if value.is_empty()
        || value.len() > 16
        || value.len() > 1 && value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(InvalidArticleNumberToken);
    }

    let number = value
        .parse::<u64>()
        .map_err(|_| InvalidArticleNumberToken)?;
    if number == 0 || number > MAX_ARTICLE_NUMBER {
        return Err(InvalidArticleNumberToken);
    }

    Ok(number)
}

/// Validated group name for GROUP/LISTGROUP requests.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupName<'a>(Cow<'a, str>);

impl<'a> GroupName<'a> {
    /// Construct a borrowed group name after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidGroupName> {
        validate_group_name(value)?;
        Ok(Self(Cow::Borrowed(value)))
    }

    /// Construct an owned group name after validation.
    pub fn from_owned(value: impl AsRef<str>) -> Result<GroupName<'static>, InvalidGroupName> {
        let value = value.as_ref();
        validate_group_name(value)?;
        Ok(GroupName(Cow::Owned(value.to_owned())))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidGroupName;

fn validate_group_name(value: &str) -> Result<(), InvalidGroupName> {
    if value.is_empty()
        || value.starts_with('.')
        || value.ends_with('.')
        || value.contains("..")
        || value.bytes().any(|byte| {
            byte.is_ascii_whitespace()
                || byte.is_ascii_control()
                || matches!(
                    byte,
                    b'<' | b'>'
                        | b'!'
                        | b'*'
                        | b'?'
                        | b'['
                        | b']'
                        | b'\\'
                        | b','
                        | b':'
                        | b'/'
                        | b'@'
                )
        })
    {
        return Err(InvalidGroupName);
    }

    Ok(())
}

/// Validated AUTHINFO argument value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuthInfoValue<'a>(Cow<'a, str>);

impl<'a> AuthInfoValue<'a> {
    /// Construct a borrowed AUTHINFO value after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidAuthInfoValue> {
        validate_auth_info_value(value)?;
        Ok(Self(Cow::Borrowed(value)))
    }

    /// Construct an owned AUTHINFO value after validation.
    pub fn from_owned(
        value: impl AsRef<str>,
    ) -> Result<AuthInfoValue<'static>, InvalidAuthInfoValue> {
        let value = value.as_ref();
        validate_auth_info_value(value)?;
        Ok(AuthInfoValue(Cow::Owned(value.to_owned())))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidAuthInfoValue;

fn validate_auth_info_value(value: &str) -> Result<(), InvalidAuthInfoValue> {
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(InvalidAuthInfoValue);
    }

    Ok(())
}

/// AUTHINFO command families currently supported by the client surface.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthInfoKind {
    User,
    Pass,
}

impl AuthInfoKind {
    #[must_use]
    pub const fn as_wire(self) -> &'static [u8] {
        match self {
            Self::User => b"USER",
            Self::Pass => b"PASS",
        }
    }
}

/// LIST subcommands currently supported by the client surface.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListKind {
    Active,
    ActiveTimes,
    Newsgroups,
    OverviewFmt,
    Headers,
    DistribPats,
}

impl ListKind {
    #[must_use]
    pub const fn as_wire(self) -> &'static [u8] {
        match self {
            Self::Active => b"ACTIVE",
            Self::ActiveTimes => b"ACTIVE.TIMES",
            Self::Newsgroups => b"NEWSGROUPS",
            Self::OverviewFmt => b"OVERVIEW.FMT",
            Self::Headers => b"HEADERS",
            Self::DistribPats => b"DISTRIB.PATS",
        }
    }
}

/// Raw TAKETHIS article payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArticleTransfer<'a>(Cow<'a, [u8]>);

impl<'a> ArticleTransfer<'a> {
    /// Construct a borrowed article transfer payload.
    #[must_use]
    pub fn from_borrowed(value: &'a [u8]) -> Self {
        Self(Cow::Borrowed(value))
    }

    /// Construct an owned article transfer payload.
    #[must_use]
    pub fn from_owned(value: impl AsRef<[u8]>) -> ArticleTransfer<'static> {
        ArticleTransfer(Cow::Owned(value.as_ref().to_vec()))
    }

    /// Borrow the raw payload bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Validated NNTP date argument for NEWGROUPS/NEWNEWS.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NntpDate<'a>(Cow<'a, str>);

impl<'a> NntpDate<'a> {
    /// Construct a borrowed date after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidNntpDate> {
        validate_nntp_date(value)?;
        Ok(Self(Cow::Borrowed(value)))
    }

    /// Construct an owned date after validation.
    pub fn from_owned(value: impl AsRef<str>) -> Result<NntpDate<'static>, InvalidNntpDate> {
        let value = value.as_ref();
        validate_nntp_date(value)?;
        Ok(NntpDate(Cow::Owned(value.to_owned())))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidNntpDate;

fn validate_nntp_date(value: &str) -> Result<(), InvalidNntpDate> {
    let bytes = value.as_bytes();
    if !matches!(bytes.len(), 6 | 8) || bytes.iter().any(|byte| !byte.is_ascii_digit()) {
        return Err(InvalidNntpDate);
    }

    let month =
        parse_two_digits(&bytes[bytes.len() - 4..bytes.len() - 2]).ok_or(InvalidNntpDate)?;
    let day = parse_two_digits(&bytes[bytes.len() - 2..]).ok_or(InvalidNntpDate)?;
    if !(1..=12).contains(&month) || day == 0 {
        return Err(InvalidNntpDate);
    }
    let year = if bytes.len() == 8 {
        std::str::from_utf8(&bytes[..4])
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or(InvalidNntpDate)?
    } else {
        let short_year = parse_two_digits(&bytes[..2]).ok_or(InvalidNntpDate)?;
        u16::from(short_year) + 2000
    };
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return Err(InvalidNntpDate),
    };
    if day > max_day {
        return Err(InvalidNntpDate);
    }

    Ok(())
}

/// Validated NNTP time argument for NEWGROUPS/NEWNEWS.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NntpTime<'a>(Cow<'a, str>);

impl<'a> NntpTime<'a> {
    /// Construct a borrowed time after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidNntpTime> {
        validate_nntp_time(value)?;
        Ok(Self(Cow::Borrowed(value)))
    }

    /// Construct an owned time after validation.
    pub fn from_owned(value: impl AsRef<str>) -> Result<NntpTime<'static>, InvalidNntpTime> {
        let value = value.as_ref();
        validate_nntp_time(value)?;
        Ok(NntpTime(Cow::Owned(value.to_owned())))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidNntpTime;

fn validate_nntp_time(value: &str) -> Result<(), InvalidNntpTime> {
    let bytes = value.as_bytes();
    if bytes.len() != 6 || bytes.iter().any(|byte| !byte.is_ascii_digit()) {
        return Err(InvalidNntpTime);
    }

    let hour = parse_two_digits(&bytes[..2]).ok_or(InvalidNntpTime)?;
    let minute = parse_two_digits(&bytes[2..4]).ok_or(InvalidNntpTime)?;
    let second = parse_two_digits(&bytes[4..]).ok_or(InvalidNntpTime)?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(InvalidNntpTime);
    }

    Ok(())
}

const fn is_leap_year(year: u16) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

/// Validated wildmat argument for NEWNEWS.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Wildmat<'a>(Cow<'a, str>);

impl<'a> Wildmat<'a> {
    /// Construct a borrowed wildmat after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidWildmat> {
        validate_wildmat(value)?;
        Ok(Self(Cow::Borrowed(value)))
    }

    /// Construct an owned wildmat after validation.
    pub fn from_owned(value: impl AsRef<str>) -> Result<Wildmat<'static>, InvalidWildmat> {
        let value = value.as_ref();
        validate_wildmat(value)?;
        Ok(Wildmat(Cow::Owned(value.to_owned())))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidWildmat;

fn validate_wildmat(value: &str) -> Result<(), InvalidWildmat> {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_ascii() && !(('\x21'..='\x7e').contains(&ch)))
    {
        return Err(InvalidWildmat);
    }

    for pattern in value.split(',') {
        let pattern = pattern.strip_prefix('!').unwrap_or(pattern);
        if pattern.is_empty()
            || pattern
                .bytes()
                .any(|byte| matches!(byte, b'!' | b',' | b'[' | b'\\' | b']'))
        {
            return Err(InvalidWildmat);
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidDiscoveryArguments {
    Wildmat(InvalidWildmat),
    Date(InvalidNntpDate),
    Time(InvalidNntpTime),
}

fn parse_two_digits(bytes: &[u8]) -> Option<u8> {
    if bytes.len() != 2 {
        return None;
    }

    let tens = bytes[0].checked_sub(b'0')?;
    let ones = bytes[1].checked_sub(b'0')?;
    if tens > 9 || ones > 9 {
        return None;
    }

    Some(tens * 10 + ones)
}

/// Client request kind for the currently-supported command set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
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
    ListGroup,
    Last,
    Next,
    List,
    Help,
    Capabilities,
    Date,
    Over,
    Xover,
    Hdr,
    Xhdr,
    NewGroups,
    NewNews,
    Post,
    Ihave,
    Check,
    TakeThis,
    AuthInfoUser,
    AuthInfoPass,
    AuthInfo,
    StartTls,
    ModeReader,
    Quit,
    Unknown,
}

impl RequestKind {
    /// Whether this request expects a multiline response for the given status.
    #[must_use]
    pub fn expects_multiline_response(self, status: StatusCode) -> bool {
        ResponseFraming::for_request_status(self, status).is_multiline()
    }
}

/// Protocol framing for a response after its status line has been parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFraming {
    SingleLine,
    Multiline,
    Unexpected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseDescriptor {
    kind: RequestKind,
    status: u16,
    framing: ResponseFraming,
}

impl ResponseDescriptor {
    #[must_use]
    pub const fn kind(self) -> RequestKind {
        self.kind
    }

    #[must_use]
    pub const fn status_code(self) -> StatusCode {
        StatusCode(self.status)
    }

    #[must_use]
    pub const fn framing(self) -> ResponseFraming {
        self.framing
    }

    /// Return the full protocol response descriptor for a request/status pair.
    #[must_use]
    pub fn for_request_status(kind: RequestKind, status: StatusCode) -> Self {
        // RFC 3977 section 3.2.1 generic errors are single-line. Keep that
        // hot path ahead of command metadata so clients do not wait for a
        // dot terminator after 4xx/5xx failures.
        if status.is_error() {
            return response_descriptor(kind, status.as_u16(), ResponseFraming::SingleLine);
        }

        responses_for_request(kind)
            .find(|descriptor| descriptor.status_code() == status)
            .unwrap_or_else(|| {
                // RFC 3977 section 3.2 says extensions keep response framing
                // code-driven except for the 211 GROUP/LISTGROUP exception.
                // Unknown request kinds therefore fall back to status metadata.
                let framing = if matches!(kind, RequestKind::Unknown)
                    && status_implies_multiline(status.as_u16())
                {
                    ResponseFraming::Multiline
                } else if matches!(kind, RequestKind::Unknown) {
                    ResponseFraming::SingleLine
                } else {
                    ResponseFraming::Unexpected
                };
                response_descriptor(kind, status.as_u16(), framing)
            })
    }
}

// RFC 3977 section 3.2: response codes normally determine framing, but 211 is
// the historical exception: GROUP returns a single-line 211 and LISTGROUP
// returns a multi-line 211. RFC 3977 section 9.4 defines the generic
// simple-response vs multi-line-response grammar.
static RESPONSE_DESCRIPTORS: &[ResponseDescriptor] = &[
    // RFC 3977 section 6.2 article retrieval commands.
    response_descriptor(RequestKind::Article, 220, ResponseFraming::Multiline),
    response_descriptor(RequestKind::Head, 221, ResponseFraming::Multiline),
    response_descriptor(RequestKind::Body, 222, ResponseFraming::Multiline),
    response_descriptor(RequestKind::Stat, 223, ResponseFraming::SingleLine),
    // RFC 3977 section 6.1 newsgroup and article selection commands.
    response_descriptor(RequestKind::Group, 211, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::ListGroup, 211, ResponseFraming::Multiline),
    response_descriptor(RequestKind::Last, 223, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::Next, 223, ResponseFraming::SingleLine),
    // RFC 3977 section 7 information commands.
    response_descriptor(RequestKind::Date, 111, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::Help, 100, ResponseFraming::Multiline),
    response_descriptor(RequestKind::NewGroups, 231, ResponseFraming::Multiline),
    response_descriptor(RequestKind::NewNews, 230, ResponseFraming::Multiline),
    response_descriptor(RequestKind::List, 215, ResponseFraming::Multiline),
    response_descriptor(RequestKind::ListActive, 215, ResponseFraming::Multiline),
    response_descriptor(
        RequestKind::ListActiveTimes,
        215,
        ResponseFraming::Multiline,
    ),
    response_descriptor(RequestKind::ListNewsgroups, 215, ResponseFraming::Multiline),
    response_descriptor(
        RequestKind::ListOverviewFmt,
        215,
        ResponseFraming::Multiline,
    ),
    response_descriptor(RequestKind::ListHeaders, 215, ResponseFraming::Multiline),
    response_descriptor(
        RequestKind::ListDistribPats,
        215,
        ResponseFraming::Multiline,
    ),
    // RFC 3977 section 8 overview and header commands.
    response_descriptor(RequestKind::Over, 224, ResponseFraming::Multiline),
    response_descriptor(RequestKind::Xover, 224, ResponseFraming::Multiline),
    response_descriptor(RequestKind::Hdr, 225, ResponseFraming::Multiline),
    // RFC 2980 section 2.1.6 specifies XHDR as returning 221 with multiline
    // data. RFC 3977 standardized HDR as 225, but real servers still expose
    // the deployed XHDR 221 form.
    response_descriptor(RequestKind::Xhdr, 221, ResponseFraming::Multiline),
    // RFC 3977 section 6.3 transfer/posting commands.
    response_descriptor(RequestKind::Post, 340, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::Post, 240, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::Ihave, 335, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::Ihave, 235, ResponseFraming::SingleLine),
    // RFC 4644 streaming extension commands.
    response_descriptor(RequestKind::Check, 238, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::TakeThis, 239, ResponseFraming::SingleLine),
    // RFC 4643 AUTHINFO USER/PASS extension.
    response_descriptor(RequestKind::AuthInfoUser, 381, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::AuthInfoUser, 281, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::AuthInfoPass, 281, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::AuthInfo, 281, ResponseFraming::SingleLine),
    // RFC 3977 section 5 connection/session commands and RFC 4642 STARTTLS.
    response_descriptor(RequestKind::Capabilities, 101, ResponseFraming::Multiline),
    response_descriptor(RequestKind::ModeReader, 200, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::ModeReader, 201, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::Quit, 205, ResponseFraming::SingleLine),
    response_descriptor(RequestKind::StartTls, 382, ResponseFraming::SingleLine),
];

const fn response_descriptor(
    kind: RequestKind,
    status: u16,
    framing: ResponseFraming,
) -> ResponseDescriptor {
    ResponseDescriptor {
        kind,
        status,
        framing,
    }
}

impl ResponseFraming {
    /// Return the RFC response framing for a request/status pair.
    #[must_use]
    pub fn for_request_status(kind: RequestKind, status: StatusCode) -> Self {
        ResponseDescriptor::for_request_status(kind, status).framing()
    }

    #[must_use]
    pub const fn is_multiline(self) -> bool {
        matches!(self, Self::Multiline)
    }
}

fn responses_for_request(kind: RequestKind) -> impl Iterator<Item = ResponseDescriptor> {
    RESPONSE_DESCRIPTORS
        .iter()
        .copied()
        .filter(move |descriptor| descriptor.kind == kind)
}

/// Client client request for the current client NNTP surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request<'a> {
    Article {
        article_ref: ArticleRef<'a>,
    },
    Body {
        article_ref: ArticleRef<'a>,
    },
    Head {
        article_ref: ArticleRef<'a>,
    },
    Stat {
        article_ref: ArticleRef<'a>,
    },
    ListVariant {
        kind: ListKind,
        wildmat: Option<Wildmat<'a>>,
    },
    Group {
        group: GroupName<'a>,
    },
    ListGroup {
        group: Option<GroupName<'a>>,
        range: Option<ListGroupRange<'a>>,
    },
    Last,
    Next,
    Over {
        selector: ArticleSelector<'a>,
    },
    Xover {
        selector: ArticleSelector<'a>,
    },
    Hdr {
        header: HeaderName<'a>,
        selector: ArticleSelector<'a>,
    },
    Xhdr {
        header: HeaderName<'a>,
        selector: ArticleSelector<'a>,
    },
    NewGroups {
        date: NntpDate<'a>,
        time: NntpTime<'a>,
        gmt: bool,
    },
    NewNews {
        wildmat: Wildmat<'a>,
        date: NntpDate<'a>,
        time: NntpTime<'a>,
        gmt: bool,
    },
    Post,
    Ihave {
        message_id: MessageId<'a>,
    },
    Check {
        message_id: MessageId<'a>,
    },
    TakeThis {
        message_id: MessageId<'a>,
        article: ArticleTransfer<'a>,
    },
    AuthInfo {
        kind: AuthInfoKind,
        value: AuthInfoValue<'a>,
    },
    StartTls,
    List,
    Help,
    Capabilities,
    Date,
    ModeReader,
    Quit,
}

impl<'a> Request<'a> {
    /// Return the request kind.
    #[must_use]
    pub const fn kind(&self) -> RequestKind {
        match self {
            Self::Article { .. } => RequestKind::Article,
            Self::Body { .. } => RequestKind::Body,
            Self::Head { .. } => RequestKind::Head,
            Self::Stat { .. } => RequestKind::Stat,
            Self::ListVariant { kind, .. } => match kind {
                ListKind::Active => RequestKind::ListActive,
                ListKind::ActiveTimes => RequestKind::ListActiveTimes,
                ListKind::Newsgroups => RequestKind::ListNewsgroups,
                ListKind::OverviewFmt => RequestKind::ListOverviewFmt,
                ListKind::Headers => RequestKind::ListHeaders,
                ListKind::DistribPats => RequestKind::ListDistribPats,
            },
            Self::Group { .. } => RequestKind::Group,
            Self::ListGroup { .. } => RequestKind::ListGroup,
            Self::Last => RequestKind::Last,
            Self::Next => RequestKind::Next,
            Self::Over { .. } => RequestKind::Over,
            Self::Xover { .. } => RequestKind::Xover,
            Self::Hdr { .. } => RequestKind::Hdr,
            Self::Xhdr { .. } => RequestKind::Xhdr,
            Self::NewGroups { .. } => RequestKind::NewGroups,
            Self::NewNews { .. } => RequestKind::NewNews,
            Self::Post => RequestKind::Post,
            Self::Ihave { .. } => RequestKind::Ihave,
            Self::Check { .. } => RequestKind::Check,
            Self::TakeThis { .. } => RequestKind::TakeThis,
            Self::AuthInfo { kind, .. } => match kind {
                AuthInfoKind::User => RequestKind::AuthInfoUser,
                AuthInfoKind::Pass => RequestKind::AuthInfoPass,
            },
            Self::StartTls => RequestKind::StartTls,
            Self::List => RequestKind::List,
            Self::Help => RequestKind::Help,
            Self::Capabilities => RequestKind::Capabilities,
            Self::Date => RequestKind::Date,
            Self::ModeReader => RequestKind::ModeReader,
            Self::Quit => RequestKind::Quit,
        }
    }

    /// Serialize the request onto the NNTP wire.
    pub fn write_wire_to<W>(&self, output: &mut W)
    where
        W: Write,
    {
        match self {
            Self::Article { article_ref } => {
                write_article_ref_request_wire(output, b"ARTICLE", article_ref)
            }
            Self::Body { article_ref } => {
                write_article_ref_request_wire(output, b"BODY", article_ref)
            }
            Self::Head { article_ref } => {
                write_article_ref_request_wire(output, b"HEAD", article_ref)
            }
            Self::Stat { article_ref } => {
                write_article_ref_request_wire(output, b"STAT", article_ref)
            }
            Self::ListVariant { kind, wildmat } => {
                write_list_request_wire(output, *kind, wildmat.as_ref())
            }
            Self::Group { group } => write_one_arg_request_wire(output, b"GROUP ", group.as_str()),
            Self::ListGroup { group, range } => {
                write_listgroup_request_wire(output, group.as_ref(), range.as_ref())
            }
            Self::Last => write_simple_request_wire(output, b"LAST"),
            Self::Next => write_simple_request_wire(output, b"NEXT"),
            Self::Over { selector } => {
                write_one_arg_request_wire(output, b"OVER ", selector.as_str())
            }
            Self::Xover { selector } => {
                write_one_arg_request_wire(output, b"XOVER ", selector.as_str())
            }
            Self::Hdr { header, selector } => {
                write_two_arg_request_wire(output, b"HDR ", header.as_str(), selector.as_str())
            }
            Self::Xhdr { header, selector } => {
                write_two_arg_request_wire(output, b"XHDR ", header.as_str(), selector.as_str())
            }
            Self::NewGroups { date, time, gmt } => write_datetime_request_wire(
                output,
                b"NEWGROUPS ",
                date.as_str(),
                time.as_str(),
                *gmt,
            ),
            Self::NewNews {
                wildmat,
                date,
                time,
                gmt,
            } => write_newnews_request_wire(
                output,
                wildmat.as_str(),
                date.as_str(),
                time.as_str(),
                *gmt,
            ),
            Self::Post => write_simple_request_wire(output, b"POST"),
            Self::Ihave { message_id } => write_request_wire(output, b"IHAVE ", message_id),
            Self::Check { message_id } => write_request_wire(output, b"CHECK ", message_id),
            Self::TakeThis {
                message_id,
                article,
            } => write_transfer_request_wire(output, b"TAKETHIS ", message_id, article),
            Self::AuthInfo { kind, value } => {
                write_authinfo_request_wire(output, *kind, value.as_str())
            }
            Self::StartTls => write_simple_request_wire(output, b"STARTTLS"),
            Self::List => write_simple_request_wire(output, b"LIST"),
            Self::Help => write_simple_request_wire(output, b"HELP"),
            Self::Capabilities => write_simple_request_wire(output, b"CAPABILITIES"),
            Self::Date => write_simple_request_wire(output, b"DATE"),
            Self::ModeReader => write_simple_request_wire(output, b"MODE READER"),
            Self::Quit => write_simple_request_wire(output, b"QUIT"),
        }
    }

    /// Borrow the validated message-id carried by this request, if any.
    #[must_use]
    pub const fn message_id(&self) -> Option<&MessageId<'a>> {
        match self {
            Self::Article { article_ref }
            | Self::Body { article_ref }
            | Self::Head { article_ref }
            | Self::Stat { article_ref } => article_ref.message_id(),
            Self::Ihave { message_id }
            | Self::Check { message_id }
            | Self::TakeThis { message_id, .. } => Some(message_id),
            Self::ListVariant { .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::AuthInfo { .. }
            | Self::StartTls => None,
            Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow the ARTICLE/BODY/HEAD/STAT reference carried by this request, if any.
    #[must_use]
    pub const fn article_ref(&self) -> Option<&ArticleRef<'a>> {
        match self {
            Self::Article { article_ref }
            | Self::Body { article_ref }
            | Self::Head { article_ref }
            | Self::Stat { article_ref } => Some(article_ref),
            Self::ListVariant { .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow the validated header query arguments carried by this request, if any.
    #[must_use]
    pub const fn header_query(&self) -> Option<(&HeaderName<'a>, &ArticleSelector<'a>)> {
        match self {
            Self::Hdr { header, selector } | Self::Xhdr { header, selector } => {
                Some((header, selector))
            }
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::ListVariant { .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow the validated overview selector carried by this request, if any.
    #[must_use]
    pub const fn overview_selector(&self) -> Option<&ArticleSelector<'a>> {
        match self {
            Self::Over { selector } | Self::Xover { selector } => Some(selector),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::ListVariant { .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow the validated group name carried by this request, if any.
    #[must_use]
    pub const fn group_name(&self) -> Option<&GroupName<'a>> {
        match self {
            Self::Group { group } => Some(group),
            Self::ListGroup { group, .. } => group.as_ref(),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::ListVariant { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow the validated LISTGROUP range carried by this request, if any.
    #[must_use]
    pub const fn listgroup_range_arg(&self) -> Option<&ListGroupRange<'a>> {
        match self {
            Self::ListGroup { range, .. } => range.as_ref(),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::ListVariant { .. }
            | Self::Group { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow the validated discovery date/time carried by this request, if any.
    #[must_use]
    pub const fn discovery_datetime(&self) -> Option<(&NntpDate<'a>, &NntpTime<'a>, bool)> {
        match self {
            Self::NewGroups { date, time, gmt }
            | Self::NewNews {
                date, time, gmt, ..
            } => Some((date, time, *gmt)),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::ListVariant { .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow the validated NEWNEWS/LIST wildmat carried by this request, if any.
    #[must_use]
    pub const fn wildmat(&self) -> Option<&Wildmat<'a>> {
        match self {
            Self::NewNews { wildmat, .. } => Some(wildmat),
            Self::ListVariant {
                wildmat: Some(wildmat),
                ..
            } => Some(wildmat),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::ListVariant { wildmat: None, .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow AUTHINFO kind/value carried by this request, if any.
    #[must_use]
    pub const fn auth_info(&self) -> Option<(AuthInfoKind, &AuthInfoValue<'a>)> {
        match self {
            Self::AuthInfo { kind, value } => Some((*kind, value)),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::ListVariant { .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow TAKETHIS article payload carried by this request, if any.
    #[must_use]
    pub const fn article_transfer(&self) -> Option<&ArticleTransfer<'a>> {
        match self {
            Self::TakeThis { article, .. } => Some(article),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::ListVariant { .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow LIST family metadata carried by this request, if any.
    #[must_use]
    pub const fn list_variant(&self) -> Option<(ListKind, Option<&Wildmat<'a>>)> {
        match self {
            Self::ListVariant { kind, wildmat } => Some((*kind, wildmat.as_ref())),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
            | Self::Post
            | Self::Ihave { .. }
            | Self::Check { .. }
            | Self::TakeThis { .. }
            | Self::AuthInfo { .. }
            | Self::StartTls
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }
}

impl Request<'static> {
    /// Build an ARTICLE request from a borrowed or bare message-id string.
    pub fn article(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Article {
            article_ref: ArticleRef::MessageId(MessageId::from_str_or_wrap(message_id)?),
        })
    }

    /// Build an ARTICLE request targeting the current article.
    #[must_use]
    pub const fn article_current() -> Self {
        Self::Article {
            article_ref: ArticleRef::Current,
        }
    }

    /// Build an ARTICLE request targeting a numeric article number.
    pub fn article_number(number: u64) -> Result<Self, InvalidArticleRef> {
        Ok(Self::Article {
            article_ref: ArticleRef::from_number(number)?,
        })
    }

    /// Build an ARTICLE request from an RFC article reference selector.
    pub fn article_selector(selector: impl AsRef<str>) -> Result<Self, InvalidArticleRef> {
        Ok(Self::Article {
            article_ref: ArticleRef::from_selector(selector)?,
        })
    }

    /// Build a BODY request from a borrowed or bare message-id string.
    pub fn body(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Body {
            article_ref: ArticleRef::MessageId(MessageId::from_str_or_wrap(message_id)?),
        })
    }

    /// Build a BODY request targeting the current article.
    #[must_use]
    pub const fn body_current() -> Self {
        Self::Body {
            article_ref: ArticleRef::Current,
        }
    }

    /// Build a BODY request targeting a numeric article number.
    pub fn body_number(number: u64) -> Result<Self, InvalidArticleRef> {
        Ok(Self::Body {
            article_ref: ArticleRef::from_number(number)?,
        })
    }

    /// Build a BODY request from an RFC article reference selector.
    pub fn body_selector(selector: impl AsRef<str>) -> Result<Self, InvalidArticleRef> {
        Ok(Self::Body {
            article_ref: ArticleRef::from_selector(selector)?,
        })
    }

    /// Build a HEAD request from a borrowed or bare message-id string.
    pub fn head(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Head {
            article_ref: ArticleRef::MessageId(MessageId::from_str_or_wrap(message_id)?),
        })
    }

    /// Build a HEAD request targeting the current article.
    #[must_use]
    pub const fn head_current() -> Self {
        Self::Head {
            article_ref: ArticleRef::Current,
        }
    }

    /// Build a HEAD request targeting a numeric article number.
    pub fn head_number(number: u64) -> Result<Self, InvalidArticleRef> {
        Ok(Self::Head {
            article_ref: ArticleRef::from_number(number)?,
        })
    }

    /// Build a HEAD request from an RFC article reference selector.
    pub fn head_selector(selector: impl AsRef<str>) -> Result<Self, InvalidArticleRef> {
        Ok(Self::Head {
            article_ref: ArticleRef::from_selector(selector)?,
        })
    }

    /// Build a STAT request from a borrowed or bare message-id string.
    pub fn stat(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Stat {
            article_ref: ArticleRef::MessageId(MessageId::from_str_or_wrap(message_id)?),
        })
    }

    /// Build a STAT request targeting the current article.
    #[must_use]
    pub const fn stat_current() -> Self {
        Self::Stat {
            article_ref: ArticleRef::Current,
        }
    }

    /// Build a STAT request targeting a numeric article number.
    pub fn stat_number(number: u64) -> Result<Self, InvalidArticleRef> {
        Ok(Self::Stat {
            article_ref: ArticleRef::from_number(number)?,
        })
    }

    /// Build a STAT request from an RFC article reference selector.
    pub fn stat_selector(selector: impl AsRef<str>) -> Result<Self, InvalidArticleRef> {
        Ok(Self::Stat {
            article_ref: ArticleRef::from_selector(selector)?,
        })
    }

    /// Build a GROUP request.
    pub fn group(group: impl AsRef<str>) -> Result<Self, InvalidGroupName> {
        Ok(Self::Group {
            group: GroupName::from_owned(group)?,
        })
    }

    /// Build a LISTGROUP request.
    pub fn listgroup(group: impl AsRef<str>) -> Result<Self, InvalidGroupName> {
        Ok(Self::ListGroup {
            group: Some(GroupName::from_owned(group)?),
            range: None,
        })
    }

    /// Build a LISTGROUP request targeting the current selected group.
    #[must_use]
    pub const fn listgroup_current() -> Self {
        Self::ListGroup {
            group: None,
            range: None,
        }
    }

    /// Build a LISTGROUP request targeting the current selected group with a range filter.
    pub fn listgroup_range(range: impl AsRef<str>) -> Result<Self, InvalidListGroupRange> {
        Ok(Self::ListGroup {
            group: None,
            range: Some(ListGroupRange::from_owned(range)?),
        })
    }

    /// Build a LISTGROUP request with explicit group and range arguments.
    pub fn listgroup_group_range(
        group: impl AsRef<str>,
        range: impl AsRef<str>,
    ) -> Result<Self, InvalidListGroupRangeOrGroupName> {
        Ok(Self::ListGroup {
            group: Some(
                GroupName::from_owned(group)
                    .map_err(InvalidListGroupRangeOrGroupName::GroupName)?,
            ),
            range: Some(
                ListGroupRange::from_owned(range)
                    .map_err(InvalidListGroupRangeOrGroupName::Range)?,
            ),
        })
    }

    /// Build a LAST request.
    #[must_use]
    pub const fn last() -> Self {
        Self::Last
    }

    /// Build a NEXT request.
    #[must_use]
    pub const fn next() -> Self {
        Self::Next
    }

    /// Build an OVER request.
    pub fn over(selector: impl AsRef<str>) -> Result<Self, InvalidArticleSelector> {
        Ok(Self::Over {
            selector: ArticleSelector::from_owned(selector)?,
        })
    }

    /// Build an XOVER request.
    pub fn xover(selector: impl AsRef<str>) -> Result<Self, InvalidArticleSelector> {
        Ok(Self::Xover {
            selector: ArticleSelector::from_owned(selector)?,
        })
    }

    /// Build an HDR request.
    pub fn hdr(
        header: impl AsRef<str>,
        selector: impl AsRef<str>,
    ) -> Result<Self, InvalidHeaderQuery> {
        Ok(Self::Hdr {
            header: HeaderName::from_owned(header).map_err(InvalidHeaderQuery::Header)?,
            selector: ArticleSelector::from_owned(selector)
                .map_err(InvalidHeaderQuery::Selector)?,
        })
    }

    /// Build an XHDR request.
    pub fn xhdr(
        header: impl AsRef<str>,
        selector: impl AsRef<str>,
    ) -> Result<Self, InvalidHeaderQuery> {
        Ok(Self::Xhdr {
            header: HeaderName::from_owned(header).map_err(InvalidHeaderQuery::Header)?,
            selector: ArticleSelector::from_owned(selector)
                .map_err(InvalidHeaderQuery::Selector)?,
        })
    }

    /// Build a NEWGROUPS request.
    pub fn newgroups(
        date: impl AsRef<str>,
        time: impl AsRef<str>,
        gmt: bool,
    ) -> Result<Self, InvalidDiscoveryArguments> {
        Ok(Self::NewGroups {
            date: NntpDate::from_owned(date).map_err(InvalidDiscoveryArguments::Date)?,
            time: NntpTime::from_owned(time).map_err(InvalidDiscoveryArguments::Time)?,
            gmt,
        })
    }

    /// Build a NEWNEWS request.
    pub fn newnews(
        wildmat: impl AsRef<str>,
        date: impl AsRef<str>,
        time: impl AsRef<str>,
        gmt: bool,
    ) -> Result<Self, InvalidDiscoveryArguments> {
        Ok(Self::NewNews {
            wildmat: Wildmat::from_owned(wildmat).map_err(InvalidDiscoveryArguments::Wildmat)?,
            date: NntpDate::from_owned(date).map_err(InvalidDiscoveryArguments::Date)?,
            time: NntpTime::from_owned(time).map_err(InvalidDiscoveryArguments::Time)?,
            gmt,
        })
    }

    /// Build a POST request.
    #[must_use]
    pub const fn post() -> Self {
        Self::Post
    }

    /// Build an IHAVE request from a borrowed or bare message-id string.
    pub fn ihave(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Ihave {
            message_id: MessageId::from_str_or_wrap(message_id)?,
        })
    }

    /// Build a CHECK request from a borrowed or bare message-id string.
    pub fn check(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Check {
            message_id: MessageId::from_str_or_wrap(message_id)?,
        })
    }

    /// Build a TAKETHIS request from a message-id and article payload.
    pub fn takethis(
        message_id: impl AsRef<str>,
        article: impl AsRef<[u8]>,
    ) -> Result<Self, InvalidMessageId> {
        Ok(Self::TakeThis {
            message_id: MessageId::from_str_or_wrap(message_id)?,
            article: ArticleTransfer::from_owned(article),
        })
    }

    /// Build an AUTHINFO USER request.
    pub fn authinfo_user(value: impl AsRef<str>) -> Result<Self, InvalidAuthInfoValue> {
        Ok(Self::AuthInfo {
            kind: AuthInfoKind::User,
            value: AuthInfoValue::from_owned(value)?,
        })
    }

    /// Build an AUTHINFO PASS request.
    pub fn authinfo_pass(value: impl AsRef<str>) -> Result<Self, InvalidAuthInfoValue> {
        Ok(Self::AuthInfo {
            kind: AuthInfoKind::Pass,
            value: AuthInfoValue::from_owned(value)?,
        })
    }

    /// Build a STARTTLS request.
    #[must_use]
    pub const fn starttls() -> Self {
        Self::StartTls
    }

    /// Build a LIST request.
    #[must_use]
    pub const fn list() -> Self {
        Self::List
    }

    /// Build a LIST ACTIVE request.
    #[must_use]
    pub const fn list_active() -> Self {
        Self::ListVariant {
            kind: ListKind::Active,
            wildmat: None,
        }
    }

    /// Build a LIST ACTIVE request with a wildmat filter.
    pub fn list_active_wildmat(wildmat: impl AsRef<str>) -> Result<Self, InvalidWildmat> {
        Ok(Self::ListVariant {
            kind: ListKind::Active,
            wildmat: Some(Wildmat::from_owned(wildmat)?),
        })
    }

    /// Build a LIST ACTIVE.TIMES request.
    #[must_use]
    pub const fn list_active_times() -> Self {
        Self::ListVariant {
            kind: ListKind::ActiveTimes,
            wildmat: None,
        }
    }

    /// Build a LIST ACTIVE.TIMES request with a wildmat filter.
    pub fn list_active_times_wildmat(wildmat: impl AsRef<str>) -> Result<Self, InvalidWildmat> {
        Ok(Self::ListVariant {
            kind: ListKind::ActiveTimes,
            wildmat: Some(Wildmat::from_owned(wildmat)?),
        })
    }

    /// Build a LIST NEWSGROUPS request.
    #[must_use]
    pub const fn list_newsgroups() -> Self {
        Self::ListVariant {
            kind: ListKind::Newsgroups,
            wildmat: None,
        }
    }

    /// Build a LIST NEWSGROUPS request with a wildmat filter.
    pub fn list_newsgroups_wildmat(wildmat: impl AsRef<str>) -> Result<Self, InvalidWildmat> {
        Ok(Self::ListVariant {
            kind: ListKind::Newsgroups,
            wildmat: Some(Wildmat::from_owned(wildmat)?),
        })
    }

    /// Build a LIST OVERVIEW.FMT request.
    #[must_use]
    pub const fn list_overview_fmt() -> Self {
        Self::ListVariant {
            kind: ListKind::OverviewFmt,
            wildmat: None,
        }
    }

    /// Build a LIST HEADERS request.
    #[must_use]
    pub const fn list_headers() -> Self {
        Self::ListVariant {
            kind: ListKind::Headers,
            wildmat: None,
        }
    }

    /// Build a LIST DISTRIB.PATS request.
    #[must_use]
    pub const fn list_distrib_pats() -> Self {
        Self::ListVariant {
            kind: ListKind::DistribPats,
            wildmat: None,
        }
    }

    /// Build a HELP request.
    #[must_use]
    pub const fn help() -> Self {
        Self::Help
    }

    /// Build a CAPABILITIES request.
    #[must_use]
    pub const fn capabilities() -> Self {
        Self::Capabilities
    }

    /// Build a DATE request.
    #[must_use]
    pub const fn date() -> Self {
        Self::Date
    }

    /// Build a MODE READER request.
    #[must_use]
    pub const fn mode_reader() -> Self {
        Self::ModeReader
    }

    /// Build a QUIT request.
    #[must_use]
    pub const fn quit() -> Self {
        Self::Quit
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidHeaderQuery {
    Header(InvalidHeaderName),
    Selector(InvalidArticleSelector),
}

/// Borrowed request-line parse result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLine<'a> {
    kind: RequestKind,
    verb: &'a [u8],
    args: &'a [u8],
}

impl<'a> RequestLine<'a> {
    /// Parse a raw command line into borrowed protocol pieces.
    #[must_use]
    pub fn parse(line: &'a [u8]) -> Self {
        let Some(line) = strip_complete_crlf_line(line) else {
            return Self {
                kind: RequestKind::Unknown,
                verb: line,
                args: &[],
            };
        };
        if command_spacing_is_invalid(line) {
            return Self {
                kind: RequestKind::Unknown,
                verb: line,
                args: &[],
            };
        }

        let split = line
            .iter()
            .position(|byte| *byte == b' ')
            .unwrap_or(line.len());
        let verb = &line[..split];
        let args = if split < line.len() {
            &line[split + 1..]
        } else {
            &[]
        };

        Self {
            kind: classify_request_kind(verb, args),
            verb,
            args,
        }
    }

    /// Return the classified request kind.
    #[must_use]
    pub const fn kind(self) -> RequestKind {
        self.kind
    }

    /// Return the raw verb bytes.
    #[must_use]
    pub const fn verb(self) -> &'a [u8] {
        self.verb
    }

    /// Return the raw arg bytes.
    #[must_use]
    pub const fn args(self) -> &'a [u8] {
        self.args
    }

    /// Return a validated borrowed message-id when args are exactly a message-id.
    #[must_use]
    pub fn message_id(self) -> Option<MessageId<'a>> {
        let (start, end) = find_message_id(self.args)?;
        let value = std::str::from_utf8(&self.args[start..end]).ok()?;
        MessageId::from_borrowed(value).ok()
    }
}

#[derive(Debug, Clone, Copy)]
struct CommandDescriptor {
    verb: &'static [u8],
    kind: CommandKind,
}

#[derive(Debug, Clone, Copy)]
enum CommandKind {
    Direct(RequestKind),
    List,
    AuthInfo,
    Mode,
}

static COMMAND_DESCRIPTORS: &[CommandDescriptor] = &[
    // RFC 3977 section 9.2 command-line ABNF lists the base NNTP verbs.
    // RFC 3977 section 3.1 says command keywords are case-insensitive.
    CommandDescriptor {
        verb: b"ARTICLE",
        kind: CommandKind::Direct(RequestKind::Article),
    },
    CommandDescriptor {
        verb: b"AUTHINFO",
        kind: CommandKind::AuthInfo,
    },
    CommandDescriptor {
        verb: b"BODY",
        kind: CommandKind::Direct(RequestKind::Body),
    },
    CommandDescriptor {
        verb: b"CAPABILITIES",
        kind: CommandKind::Direct(RequestKind::Capabilities),
    },
    CommandDescriptor {
        verb: b"CHECK",
        kind: CommandKind::Direct(RequestKind::Check),
    },
    CommandDescriptor {
        verb: b"DATE",
        kind: CommandKind::Direct(RequestKind::Date),
    },
    CommandDescriptor {
        verb: b"GROUP",
        kind: CommandKind::Direct(RequestKind::Group),
    },
    CommandDescriptor {
        verb: b"HDR",
        kind: CommandKind::Direct(RequestKind::Hdr),
    },
    CommandDescriptor {
        verb: b"HEAD",
        kind: CommandKind::Direct(RequestKind::Head),
    },
    CommandDescriptor {
        verb: b"HELP",
        kind: CommandKind::Direct(RequestKind::Help),
    },
    CommandDescriptor {
        verb: b"IHAVE",
        kind: CommandKind::Direct(RequestKind::Ihave),
    },
    CommandDescriptor {
        verb: b"LAST",
        kind: CommandKind::Direct(RequestKind::Last),
    },
    CommandDescriptor {
        verb: b"LIST",
        kind: CommandKind::List,
    },
    CommandDescriptor {
        verb: b"LISTGROUP",
        kind: CommandKind::Direct(RequestKind::ListGroup),
    },
    CommandDescriptor {
        verb: b"MODE",
        kind: CommandKind::Mode,
    },
    CommandDescriptor {
        verb: b"NEWGROUPS",
        kind: CommandKind::Direct(RequestKind::NewGroups),
    },
    CommandDescriptor {
        verb: b"NEWNEWS",
        kind: CommandKind::Direct(RequestKind::NewNews),
    },
    CommandDescriptor {
        verb: b"NEXT",
        kind: CommandKind::Direct(RequestKind::Next),
    },
    CommandDescriptor {
        verb: b"OVER",
        kind: CommandKind::Direct(RequestKind::Over),
    },
    CommandDescriptor {
        verb: b"POST",
        kind: CommandKind::Direct(RequestKind::Post),
    },
    CommandDescriptor {
        verb: b"QUIT",
        kind: CommandKind::Direct(RequestKind::Quit),
    },
    CommandDescriptor {
        verb: b"STARTTLS",
        kind: CommandKind::Direct(RequestKind::StartTls),
    },
    CommandDescriptor {
        verb: b"STAT",
        kind: CommandKind::Direct(RequestKind::Stat),
    },
    CommandDescriptor {
        verb: b"TAKETHIS",
        kind: CommandKind::Direct(RequestKind::TakeThis),
    },
    CommandDescriptor {
        verb: b"XHDR",
        kind: CommandKind::Direct(RequestKind::Xhdr),
    },
    CommandDescriptor {
        verb: b"XOVER",
        kind: CommandKind::Direct(RequestKind::Xover),
    },
];

static LIST_SUBCOMMANDS: &[(&[u8], RequestKind)] = &[
    // RFC 3977 section 7.6 defines LIST variants and notes that variants
    // such as "LIST ACTIVE" are shorthand for a LIST subcommand, not separate
    // top-level verbs.
    (b"ACTIVE", RequestKind::ListActive),
    (b"ACTIVE.TIMES", RequestKind::ListActiveTimes),
    (b"NEWSGROUPS", RequestKind::ListNewsgroups),
    (b"OVERVIEW.FMT", RequestKind::ListOverviewFmt),
    (b"HEADERS", RequestKind::ListHeaders),
    (b"DISTRIB.PATS", RequestKind::ListDistribPats),
];

fn classify_request_kind(verb: &[u8], args: &[u8]) -> RequestKind {
    let Some(descriptor) = COMMAND_DESCRIPTORS
        .iter()
        .find(|descriptor| eq_ignore_ascii_case_const(verb, descriptor.verb))
    else {
        return RequestKind::Unknown;
    };

    match descriptor.kind {
        CommandKind::Direct(kind) => classify_direct_command(kind, args),
        CommandKind::List => classify_subcommand(args, LIST_SUBCOMMANDS, RequestKind::List),
        CommandKind::AuthInfo => classify_authinfo_command(args),
        CommandKind::Mode if eq_ignore_ascii_case_const(args, b"READER") => RequestKind::ModeReader,
        CommandKind::Mode => RequestKind::Unknown,
    }
}

fn command_spacing_is_invalid(line: &[u8]) -> bool {
    line.is_empty()
        || line.starts_with(b" ")
        || line.ends_with(b" ")
        || line.windows(2).any(|window| window == b"  ")
        || line
            .iter()
            .any(|byte| byte.is_ascii_whitespace() && *byte != b' ')
}

fn classify_direct_command(kind: RequestKind, args: &[u8]) -> RequestKind {
    match kind {
        RequestKind::Article | RequestKind::Body | RequestKind::Head | RequestKind::Stat => {
            if args.is_empty() || validate_article_fetch_selector(args).is_ok() {
                kind
            } else {
                RequestKind::Unknown
            }
        }
        RequestKind::Group => {
            validate_utf8_arg(args, GroupName::from_borrowed).map_or(RequestKind::Unknown, |_| kind)
        }
        RequestKind::ListGroup => {
            if args.is_empty() {
                return kind;
            }
            let mut parts = args.split(|byte| *byte == b' ');
            match (parts.next(), parts.next(), parts.next()) {
                (Some(one), None, None) => {
                    let one = std::str::from_utf8(one).ok();
                    let Some(value) = one else {
                        return RequestKind::Unknown;
                    };
                    if (value.contains('-') || value.bytes().all(|byte| byte.is_ascii_digit()))
                        && ListGroupRange::from_borrowed(value).is_err()
                    {
                        return RequestKind::Unknown;
                    }
                    if ListGroupRange::from_borrowed(value).is_ok()
                        || GroupName::from_borrowed(value).is_ok()
                    {
                        kind
                    } else {
                        RequestKind::Unknown
                    }
                }
                (Some(group), Some(range), None) => {
                    if validate_utf8_arg(group, GroupName::from_borrowed).is_ok()
                        && validate_utf8_arg(range, ListGroupRange::from_borrowed).is_ok()
                    {
                        kind
                    } else {
                        RequestKind::Unknown
                    }
                }
                _ => RequestKind::Unknown,
            }
        }
        RequestKind::Over | RequestKind::Xover => {
            if args.is_empty() || validate_utf8_arg(args, ArticleSelector::from_borrowed).is_ok() {
                kind
            } else {
                RequestKind::Unknown
            }
        }
        RequestKind::Hdr | RequestKind::Xhdr => {
            let mut parts = args.split(|byte| *byte == b' ');
            match (parts.next(), parts.next(), parts.next()) {
                (Some(header), None, None) => {
                    if validate_utf8_arg(header, HeaderName::from_borrowed).is_ok() {
                        kind
                    } else {
                        RequestKind::Unknown
                    }
                }
                (Some(header), Some(selector), None) => {
                    if validate_utf8_arg(header, HeaderName::from_borrowed).is_ok()
                        && validate_utf8_arg(selector, ArticleSelector::from_borrowed).is_ok()
                    {
                        kind
                    } else {
                        RequestKind::Unknown
                    }
                }
                _ => RequestKind::Unknown,
            }
        }
        RequestKind::Ihave | RequestKind::Check | RequestKind::TakeThis => {
            validate_utf8_arg(args, MessageId::from_borrowed).map_or(RequestKind::Unknown, |_| kind)
        }
        RequestKind::NewGroups => {
            validate_discovery_datetime_args(args, false).map_or(RequestKind::Unknown, |_| kind)
        }
        RequestKind::NewNews => validate_newnews_args(args).map_or(RequestKind::Unknown, |_| kind),
        RequestKind::Capabilities => {
            if args.is_empty() || validate_capabilities_args(args).is_ok() {
                kind
            } else {
                RequestKind::Unknown
            }
        }
        RequestKind::Date
        | RequestKind::Help
        | RequestKind::Last
        | RequestKind::Next
        | RequestKind::Post
        | RequestKind::Quit
        | RequestKind::StartTls => {
            if args.is_empty() {
                kind
            } else {
                RequestKind::Unknown
            }
        }
        _ => kind,
    }
}

fn validate_capabilities_args(args: &[u8]) -> Result<(), ()> {
    if args.contains(&b' ') {
        return Err(());
    }
    validate_utf8_arg(args, validate_keyword).map(|_| ())
}

fn validate_keyword(value: &str) -> Result<(), ()> {
    let bytes = value.as_bytes();
    let Some((first, rest)) = bytes.split_first() else {
        return Err(());
    };
    if !first.is_ascii_alphabetic() {
        return Err(());
    }
    if rest.len() < 2 {
        return Err(());
    }
    if rest
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'-'))
    {
        Ok(())
    } else {
        Err(())
    }
}

fn validate_article_fetch_selector(args: &[u8]) -> Result<(), ()> {
    let value = std::str::from_utf8(args).map_err(|_| ())?;
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        validate_article_number_token(value).map_err(|_| ())
    } else {
        MessageId::from_borrowed(value).map(|_| ()).map_err(|_| ())
    }
}

fn validate_utf8_arg<'a, T, E>(
    args: &'a [u8],
    validate: impl FnOnce(&'a str) -> Result<T, E>,
) -> Result<T, ()> {
    let value = std::str::from_utf8(args).map_err(|_| ())?;
    validate(value).map_err(|_| ())
}

fn validate_discovery_datetime_args(args: &[u8], allow_wildmat: bool) -> Result<(), ()> {
    let mut parts = args.split(|byte| *byte == b' ');
    if allow_wildmat {
        let wildmat = parts.next().ok_or(())?;
        validate_utf8_arg(wildmat, Wildmat::from_borrowed).map_err(|_| ())?;
    }
    let date = parts.next().ok_or(())?;
    let time = parts.next().ok_or(())?;
    validate_utf8_arg(date, NntpDate::from_borrowed).map_err(|_| ())?;
    validate_utf8_arg(time, NntpTime::from_borrowed).map_err(|_| ())?;
    match (parts.next(), parts.next()) {
        (None, None) => Ok(()),
        (Some(gmt), None) if eq_ignore_ascii_case_const(gmt, b"GMT") => Ok(()),
        _ => Err(()),
    }
}

fn validate_newnews_args(args: &[u8]) -> Result<(), ()> {
    validate_discovery_datetime_args(args, true)
}

fn classify_authinfo_command(args: &[u8]) -> RequestKind {
    let mut parts = args.split(|byte| *byte == b' ');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(subcommand), Some(value), None, None)
            if eq_ignore_ascii_case_const(subcommand, b"USER")
                && validate_utf8_arg(value, AuthInfoValue::from_borrowed).is_ok() =>
        {
            RequestKind::AuthInfoUser
        }
        (Some(subcommand), Some(value), None, None)
            if eq_ignore_ascii_case_const(subcommand, b"PASS")
                && validate_utf8_arg(value, AuthInfoValue::from_borrowed).is_ok() =>
        {
            RequestKind::AuthInfoPass
        }
        (Some(subcommand), Some(value), None, None)
            if eq_ignore_ascii_case_const(subcommand, b"SASL")
                && validate_sasl_mechanism(value).is_ok() =>
        {
            RequestKind::AuthInfo
        }
        (Some(subcommand), Some(mechanism), Some(initial_response), None)
            if eq_ignore_ascii_case_const(subcommand, b"SASL")
                && validate_sasl_mechanism(mechanism).is_ok()
                && validate_sasl_initial_response(initial_response).is_ok() =>
        {
            RequestKind::AuthInfo
        }
        _ => RequestKind::Unknown,
    }
}

fn validate_sasl_mechanism(value: &[u8]) -> Result<(), ()> {
    if value.is_empty() || value.len() > 20 {
        return Err(());
    }
    for byte in value {
        match *byte {
            b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' => {}
            _ => return Err(()),
        }
    }
    Ok(())
}

fn validate_sasl_initial_response(value: &[u8]) -> Result<(), ()> {
    if value == b"=" {
        return Ok(());
    }
    if value.is_empty() || !value.len().is_multiple_of(4) {
        return Err(());
    }

    let padding = value.iter().rev().take_while(|byte| **byte == b'=').count();
    if padding > 2 {
        return Err(());
    }
    let data_len = value.len() - padding;
    for (index, byte) in value.iter().enumerate() {
        match (*byte, index >= data_len) {
            (b'=', true) => {}
            (b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/', false) => {}
            _ => return Err(()),
        }
    }
    Ok(())
}

fn classify_subcommand(
    args: &[u8],
    table: &[(&'static [u8], RequestKind)],
    default: RequestKind,
) -> RequestKind {
    let mut parts = args.split(|byte| *byte == b' ');
    let subcommand = parts.next();

    let Some(subcommand) = subcommand else {
        return default;
    };

    let Some(kind) = table
        .iter()
        .find_map(|(wire, kind)| eq_ignore_ascii_case_const(subcommand, wire).then_some(*kind))
    else {
        return if subcommand.is_empty() {
            default
        } else {
            RequestKind::Unknown
        };
    };

    match kind {
        RequestKind::ListActive | RequestKind::ListActiveTimes | RequestKind::ListNewsgroups => {
            match (parts.next(), parts.next()) {
                (None, None) => kind,
                (Some(wildmat), None)
                    if validate_utf8_arg(wildmat, Wildmat::from_borrowed).is_ok() =>
                {
                    kind
                }
                _ => RequestKind::Unknown,
            }
        }
        RequestKind::AuthInfoUser | RequestKind::AuthInfoPass => match (parts.next(), parts.next())
        {
            (Some(value), None)
                if validate_utf8_arg(value, AuthInfoValue::from_borrowed).is_ok() =>
            {
                kind
            }
            _ => RequestKind::Unknown,
        },
        RequestKind::ListHeaders => match (parts.next(), parts.next()) {
            (None, None) => kind,
            (Some(selector), None)
                if eq_ignore_ascii_case_const(selector, b"MSGID")
                    || eq_ignore_ascii_case_const(selector, b"RANGE") =>
            {
                kind
            }
            _ => RequestKind::Unknown,
        },
        RequestKind::ListOverviewFmt | RequestKind::ListDistribPats => {
            if parts.next().is_none() {
                kind
            } else {
                RequestKind::Unknown
            }
        }
        _ => kind,
    }
}

const fn eq_ignore_ascii_case_const(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut index = 0;
    while index < left.len() {
        if ascii_upper(left[index]) != ascii_upper(right[index]) {
            return false;
        }
        index += 1;
    }
    true
}

const fn ascii_upper(byte: u8) -> u8 {
    match byte {
        b'a'..=b'z' => byte - 32,
        _ => byte,
    }
}

fn find_message_id(args: &[u8]) -> Option<(usize, usize)> {
    let start = args.iter().position(|byte| !byte.is_ascii_whitespace())?;
    let end = args.iter().rposition(|byte| !byte.is_ascii_whitespace())? + 1;
    let trimmed = &args[start..end];

    if !trimmed.starts_with(b"<")
        || !trimmed.ends_with(b">")
        || trimmed[1..trimmed.len() - 1]
            .iter()
            .any(u8::is_ascii_whitespace)
    {
        return None;
    }

    MessageId::from_borrowed(std::str::from_utf8(trimmed).ok()?).ok()?;
    Some((start, end))
}

const fn status_implies_multiline(code: u16) -> bool {
    matches!(
        code,
        100 | 101 | 211 | 215 | 220 | 221 | 222 | 224 | 225 | 230 | 231 | 282 | 288
    )
}

fn write_bytes<W>(output: &mut W, bytes: &[u8])
where
    W: Write,
{
    output.write_all(bytes).expect("request wire write failed");
}

fn write_crlf<W>(output: &mut W)
where
    W: Write,
{
    write_bytes(output, b"\r\n");
}

fn write_request_wire<W>(output: &mut W, verb: &[u8], message_id: &MessageId<'_>)
where
    W: Write,
{
    write_bytes(output, verb);
    write_bytes(output, message_id.as_str().as_bytes());
    write_crlf(output);
}

fn write_article_ref_request_wire<W>(output: &mut W, verb: &[u8], article_ref: &ArticleRef<'_>)
where
    W: Write,
{
    write_bytes(output, verb);
    match article_ref {
        ArticleRef::Current => {}
        ArticleRef::Number(number) => {
            let mut number_buf = arrayvec::ArrayString::<20>::new();
            write!(&mut number_buf, "{number}").expect("article number fits fixed buffer");
            write_bytes(output, b" ");
            write_bytes(output, number_buf.as_bytes());
        }
        ArticleRef::MessageId(message_id) => {
            write_bytes(output, b" ");
            write_bytes(output, message_id.as_str().as_bytes());
        }
    }
    write_crlf(output);
}

fn write_one_arg_request_wire<W>(output: &mut W, verb: &[u8], arg: &str)
where
    W: Write,
{
    write_bytes(output, verb);
    write_bytes(output, arg.as_bytes());
    write_crlf(output);
}

fn write_two_arg_request_wire<W>(output: &mut W, verb: &[u8], left: &str, right: &str)
where
    W: Write,
{
    write_bytes(output, verb);
    write_bytes(output, left.as_bytes());
    write_bytes(output, b" ");
    write_bytes(output, right.as_bytes());
    write_crlf(output);
}

fn write_listgroup_request_wire<W>(
    output: &mut W,
    group: Option<&GroupName<'_>>,
    range: Option<&ListGroupRange<'_>>,
) where
    W: Write,
{
    write_bytes(output, b"LISTGROUP");
    if let Some(group) = group {
        write_bytes(output, b" ");
        write_bytes(output, group.as_str().as_bytes());
    }
    if let Some(range) = range {
        write_bytes(output, b" ");
        write_bytes(output, range.as_str().as_bytes());
    }
    write_crlf(output);
}

fn write_datetime_request_wire<W>(output: &mut W, verb: &[u8], date: &str, time: &str, gmt: bool)
where
    W: Write,
{
    write_bytes(output, verb);
    write_bytes(output, date.as_bytes());
    write_bytes(output, b" ");
    write_bytes(output, time.as_bytes());
    if gmt {
        write_bytes(output, b" GMT");
    }
    write_crlf(output);
}

fn write_newnews_request_wire<W>(output: &mut W, wildmat: &str, date: &str, time: &str, gmt: bool)
where
    W: Write,
{
    write_bytes(output, b"NEWNEWS ");
    write_bytes(output, wildmat.as_bytes());
    write_bytes(output, b" ");
    write_bytes(output, date.as_bytes());
    write_bytes(output, b" ");
    write_bytes(output, time.as_bytes());
    if gmt {
        write_bytes(output, b" GMT");
    }
    write_crlf(output);
}

fn write_list_request_wire<W>(output: &mut W, kind: ListKind, wildmat: Option<&Wildmat<'_>>)
where
    W: Write,
{
    write_bytes(output, b"LIST ");
    write_bytes(output, kind.as_wire());
    if let Some(wildmat) = wildmat {
        write_bytes(output, b" ");
        write_bytes(output, wildmat.as_str().as_bytes());
    }
    write_crlf(output);
}

fn write_authinfo_request_wire<W>(output: &mut W, kind: AuthInfoKind, value: &str)
where
    W: Write,
{
    write_bytes(output, b"AUTHINFO ");
    write_bytes(output, kind.as_wire());
    write_bytes(output, b" ");
    write_bytes(output, value.as_bytes());
    write_crlf(output);
}

fn write_transfer_request_wire<W>(
    output: &mut W,
    verb: &[u8],
    message_id: &MessageId<'_>,
    article: &ArticleTransfer<'_>,
) where
    W: Write,
{
    write_bytes(output, verb);
    write_bytes(output, message_id.as_str().as_bytes());
    write_crlf(output);

    let payload = article.as_bytes();
    if payload.is_empty() {
        write_bytes(output, DOT_TERMINATOR);
        return;
    }

    for line in crlf_normalized_payload_lines(payload) {
        if line.starts_with(b".") {
            write_bytes(output, b".");
        }
        write_bytes(output, line);
        write_crlf(output);
    }

    write_bytes(output, DOT_TERMINATOR);
}

fn write_simple_request_wire<W>(output: &mut W, verb: &[u8])
where
    W: Write,
{
    write_bytes(output, verb);
    write_crlf(output);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_code_parses_ascii_prefix() {
        assert_eq!(
            StatusCode::parse(b"220 article follows"),
            Some(StatusCode(220))
        );
        assert_eq!(
            StatusCode::parse(b"101 Capability list"),
            Some(StatusCode(101))
        );
        assert_eq!(StatusCode::parse(b"2x0 broken"), None);
        assert_eq!(StatusCode::parse(b""), None);
    }

    #[test]
    fn status_code_categories_and_parsing_cover_rfc_examples() {
        for code in [100, 199] {
            let status = StatusCode(code);
            assert!(status.is_informational(), "{code}");
            assert!(!status.is_success(), "{code}");
            assert!(!status.is_error(), "{code}");
        }

        for code in [200, 205, 220, 281, 299, 335, 381, 399] {
            let status = StatusCode(code);
            assert!(!status.is_informational(), "{code}");
            assert!(status.is_success(), "{code}");
            assert!(!status.is_error(), "{code}");
        }

        for code in [400, 430, 499, 500, 599] {
            let status = StatusCode(code);
            assert!(!status.is_informational(), "{code}");
            assert!(status.is_error(), "{code}");
            assert!(!status.is_success(), "{code}");
        }

        for (input, expected) in [
            (b"200 Service ready\r\n".as_slice(), Some(StatusCode(200))),
            (b"200".as_slice(), Some(StatusCode(200))),
            (b"200 ".as_slice(), Some(StatusCode(200))),
            (
                b"200 Welcome! <server@example>\r\n".as_slice(),
                Some(StatusCode(200)),
            ),
            (
                b"200  multiple  spaces  \r\n".as_slice(),
                Some(StatusCode(200)),
            ),
            (b"000".as_slice(), Some(StatusCode(0))),
            (b"999 message\r\n".as_slice(), Some(StatusCode(999))),
            (b"211 42 1 100 alt.test".as_slice(), Some(StatusCode(211))),
            ("200 Привет мир\r\n".as_bytes(), Some(StatusCode(200))),
            (b"".as_slice(), None),
            (b"20".as_slice(), None),
            (b"2X0 Error\r\n".as_slice(), None),
            (b"ABC Invalid\r\n".as_slice(), None),
            (b" 200 Error\r\n".as_slice(), None),
        ] {
            assert_eq!(StatusCode::parse(input), expected, "{input:?}");
        }

        let long_msg = format!("200 {}\r\n", "x".repeat(1000));
        assert_eq!(
            StatusCode::parse(long_msg.as_bytes()),
            Some(StatusCode(200))
        );
    }

    #[test]
    fn message_id_validates_and_wraps() {
        assert_eq!(MessageId::from_borrowed("<a@b>").unwrap().as_str(), "<a@b>");
        assert!(MessageId::from_borrowed("a@b").is_err());
        assert!(MessageId::from_borrowed("<a b>").is_err());
        assert_eq!(
            MessageId::from_str_or_wrap("a@b").unwrap().as_str(),
            "<a@b>"
        );
    }

    #[test]
    fn message_id_validation_covers_rfc_edge_shapes() {
        assert!(MessageId::from_borrowed("<a>").is_err());
        assert!(MessageId::from_borrowed("<a@b>").is_ok());
        assert!(MessageId::from_borrowed("<>").is_err());
        assert!(MessageId::from_borrowed("<no-end").is_err());
        assert!(MessageId::from_borrowed("no-start>").is_err());
        assert!(MessageId::from_str_or_wrap("").is_err());
    }

    #[test]
    fn header_query_arguments_validate() {
        assert_eq!(
            HeaderName::from_borrowed("Subject").unwrap().as_str(),
            "Subject"
        );
        assert_eq!(
            HeaderName::from_borrowed(":bytes").unwrap().as_str(),
            ":bytes"
        );
        assert_eq!(
            ArticleSelector::from_borrowed("1-10").unwrap().as_str(),
            "1-10"
        );
        assert_eq!(
            ArticleSelector::from_borrowed("<a@b>").unwrap().as_str(),
            "<a@b>"
        );
        assert!(HeaderName::from_borrowed("Bad Header").is_err());
        assert!(HeaderName::from_borrowed("Subject:").is_err());
        assert!(HeaderName::from_borrowed(":").is_err());
        assert!(ArticleSelector::from_borrowed("1 10").is_err());
        assert!(ArticleSelector::from_borrowed("1\r\nQUIT").is_err());
    }

    #[test]
    fn discovery_arguments_validate() {
        assert_eq!(
            NntpDate::from_borrowed("20260101").unwrap().as_str(),
            "20260101"
        );
        assert_eq!(
            NntpDate::from_borrowed("260101").unwrap().as_str(),
            "260101"
        );
        assert_eq!(
            NntpTime::from_borrowed("235959").unwrap().as_str(),
            "235959"
        );
        assert_eq!(
            Wildmat::from_borrowed("comp.lang.*,alt.test")
                .unwrap()
                .as_str(),
            "comp.lang.*,alt.test"
        );

        assert!(NntpDate::from_borrowed("20261301").is_err());
        assert!(NntpDate::from_borrowed("20260100").is_err());
        assert!(NntpDate::from_borrowed("202601").is_err());
        assert!(NntpTime::from_borrowed("240000").is_err());
        assert!(NntpTime::from_borrowed("126061").is_err());
        assert!(NntpTime::from_borrowed("1200").is_err());
        assert!(Wildmat::from_borrowed("").is_err());
        assert!(Wildmat::from_borrowed(",alt.test").is_err());
        assert!(Wildmat::from_borrowed("alt.test,").is_err());
        assert!(Wildmat::from_borrowed("!").is_err());
        assert!(Wildmat::from_borrowed("alt test").is_err());
    }

    #[test]
    fn auth_info_values_validate() {
        assert_eq!(
            AuthInfoValue::from_borrowed("user-name").unwrap().as_str(),
            "user-name"
        );
        assert_eq!(
            AuthInfoValue::from_borrowed("pass-word").unwrap().as_str(),
            "pass-word"
        );
        assert!(AuthInfoValue::from_borrowed("").is_err());
        assert!(AuthInfoValue::from_borrowed("user name").is_err());
        assert!(AuthInfoValue::from_borrowed("pass\tword").is_err());
        assert!(AuthInfoValue::from_borrowed("bad\rvalue").is_err());
        assert!(AuthInfoValue::from_borrowed("bad\nvalue").is_err());
    }

    #[test]
    fn request_line_parses_current_command_set() {
        assert_eq!(
            RequestLine::parse(b"ARTICLE <a@b>\r\n").kind(),
            RequestKind::Article
        );
        assert_eq!(
            RequestLine::parse(b"ARTICLE\r\n").kind(),
            RequestKind::Article
        );
        assert_eq!(
            RequestLine::parse(b"body 123\r\n").kind(),
            RequestKind::Body
        );
        assert_eq!(
            RequestLine::parse(b"HEAD <a@b>\r\n").kind(),
            RequestKind::Head
        );
        assert_eq!(RequestLine::parse(b"HELP\r\n").kind(), RequestKind::Help);
        assert_eq!(
            RequestLine::parse(b"STAT <a@b>\r\n").kind(),
            RequestKind::Stat
        );
        assert_eq!(
            RequestLine::parse(b"LIST ACTIVE\r\n").kind(),
            RequestKind::ListActive
        );
        assert_eq!(
            RequestLine::parse(b"LIST ACTIVE.TIMES comp.lang.*\r\n").kind(),
            RequestKind::ListActiveTimes
        );
        assert_eq!(
            RequestLine::parse(b"LIST NEWSGROUPS comp.lang.*\r\n").kind(),
            RequestKind::ListNewsgroups
        );
        assert_eq!(
            RequestLine::parse(b"LIST OVERVIEW.FMT\r\n").kind(),
            RequestKind::ListOverviewFmt
        );
        assert_eq!(
            RequestLine::parse(b"LIST HEADERS\r\n").kind(),
            RequestKind::ListHeaders
        );
        assert_eq!(
            RequestLine::parse(b"LIST DISTRIB.PATS\r\n").kind(),
            RequestKind::ListDistribPats
        );
        assert_eq!(
            RequestLine::parse(b"OVER 1-10\r\n").kind(),
            RequestKind::Over
        );
        assert_eq!(
            RequestLine::parse(b"XOVER 1-10\r\n").kind(),
            RequestKind::Xover
        );
        assert_eq!(
            RequestLine::parse(b"HDR Subject 1\r\n").kind(),
            RequestKind::Hdr
        );
        assert_eq!(
            RequestLine::parse(b"XHDR Subject 1\r\n").kind(),
            RequestKind::Xhdr
        );
        assert_eq!(
            RequestLine::parse(b"CAPABILITIES\r\n").kind(),
            RequestKind::Capabilities
        );
        assert_eq!(RequestLine::parse(b"DATE\r\n").kind(), RequestKind::Date);
        assert_eq!(
            RequestLine::parse(b"MODE READER\r\n").kind(),
            RequestKind::ModeReader
        );
        assert_eq!(
            RequestLine::parse(b"GROUP\talt.test\r\n").kind(),
            RequestKind::Unknown
        );
        assert_eq!(
            RequestLine::parse(b"MODE\tREADER\r\n").kind(),
            RequestKind::Unknown
        );
        assert_eq!(
            RequestLine::parse(b"MODE  READER\r\n").kind(),
            RequestKind::Unknown
        );
        assert_eq!(RequestLine::parse(b"QUIT\r\n").kind(), RequestKind::Quit);
        assert_eq!(RequestLine::parse(b"HEAD 1\r\n").kind(), RequestKind::Head);
        assert_eq!(
            RequestLine::parse(b"MODE TRANSIT\r\n").kind(),
            RequestKind::Unknown
        );
    }

    #[test]
    fn request_line_classifies_rfc_command_matrix_case_insensitively() {
        // RFC 3977 section 3.1 defines commands as CRLF-terminated lines:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        for (line, expected) in [
            (b"ARTICLE <a@b>".as_slice(), RequestKind::Article),
            (b"BODY <a@b>".as_slice(), RequestKind::Body),
            (b"HEAD <a@b>".as_slice(), RequestKind::Head),
            (b"STAT <a@b>".as_slice(), RequestKind::Stat),
            (b"ARTICLE 12345".as_slice(), RequestKind::Article),
            (b"GROUP alt.test".as_slice(), RequestKind::Group),
            (b"LISTGROUP".as_slice(), RequestKind::ListGroup),
            (b"LISTGROUP alt.test".as_slice(), RequestKind::ListGroup),
            (b"LISTGROUP 1-".as_slice(), RequestKind::ListGroup),
            (
                b"LISTGROUP alt.test 1-10".as_slice(),
                RequestKind::ListGroup,
            ),
            (b"LAST".as_slice(), RequestKind::Last),
            (b"NEXT".as_slice(), RequestKind::Next),
            (b"LIST".as_slice(), RequestKind::List),
            (b"LIST ACTIVE".as_slice(), RequestKind::ListActive),
            (
                b"LIST ACTIVE.TIMES comp.lang.*".as_slice(),
                RequestKind::ListActiveTimes,
            ),
            (
                b"LIST NEWSGROUPS comp.lang.*".as_slice(),
                RequestKind::ListNewsgroups,
            ),
            (
                b"LIST OVERVIEW.FMT".as_slice(),
                RequestKind::ListOverviewFmt,
            ),
            (b"LIST HEADERS".as_slice(), RequestKind::ListHeaders),
            (b"LIST HEADERS MSGID".as_slice(), RequestKind::ListHeaders),
            (b"LIST HEADERS RANGE".as_slice(), RequestKind::ListHeaders),
            (
                b"LIST DISTRIB.PATS".as_slice(),
                RequestKind::ListDistribPats,
            ),
            (b"DATE".as_slice(), RequestKind::Date),
            (b"HELP".as_slice(), RequestKind::Help),
            (b"CAPABILITIES".as_slice(), RequestKind::Capabilities),
            (b"MODE READER".as_slice(), RequestKind::ModeReader),
            (b"QUIT".as_slice(), RequestKind::Quit),
            (b"OVER 1-10".as_slice(), RequestKind::Over),
            (b"XOVER 1-10".as_slice(), RequestKind::Xover),
            (b"HDR Subject 1-10".as_slice(), RequestKind::Hdr),
            (b"XHDR Subject 1-10".as_slice(), RequestKind::Xhdr),
            (
                b"NEWGROUPS 20260101 000000 GMT".as_slice(),
                RequestKind::NewGroups,
            ),
            (
                b"NEWGROUPS 20260101 000000 gmt".as_slice(),
                RequestKind::NewGroups,
            ),
            (
                b"NEWNEWS * 20260101 000000 GMT".as_slice(),
                RequestKind::NewNews,
            ),
            (
                b"NEWNEWS comp.lang.* 20260101 000000".as_slice(),
                RequestKind::NewNews,
            ),
            (b"POST".as_slice(), RequestKind::Post),
            (b"IHAVE <a@b>".as_slice(), RequestKind::Ihave),
            (b"CHECK <a@b>".as_slice(), RequestKind::Check),
            (b"TAKETHIS <a@b>".as_slice(), RequestKind::TakeThis),
            (b"AUTHINFO USER test".as_slice(), RequestKind::AuthInfoUser),
            (b"AUTHINFO PASS test".as_slice(), RequestKind::AuthInfoPass),
            (b"AUTHINFO SASL TEST".as_slice(), RequestKind::AuthInfo),
            (b"AUTHINFO SASL TEST =".as_slice(), RequestKind::AuthInfo),
            (b"STARTTLS".as_slice(), RequestKind::StartTls),
            (b"article <a@b>".as_slice(), RequestKind::Article),
            (b"authinfo user test".as_slice(), RequestKind::AuthInfoUser),
            (b"quit".as_slice(), RequestKind::Quit),
            (b"XYZZY".as_slice(), RequestKind::Unknown),
        ] {
            let mut framed = line.to_vec();
            framed.extend_from_slice(b"\r\n");
            assert_eq!(RequestLine::parse(&framed).kind(), expected, "{line:?}");
        }
    }

    #[test]
    fn request_line_rejects_unframed_or_malformed_command_lines() {
        // RFC 3977 section 3.1 defines CRLF as the command line terminator:
        // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
        for line in [
            b"ARTICLE <a@b>".as_slice(),
            b"ARTICLE <a@b>\n".as_slice(),
            b"ARTICLE <a@b>\r".as_slice(),
            b"ARTICLE <a@b>\r \r\n".as_slice(),
        ] {
            assert_eq!(RequestLine::parse(line).kind(), RequestKind::Unknown);
        }
    }

    #[test]
    fn request_line_exposes_message_id_when_args_are_exact_id() {
        assert_eq!(
            RequestLine::parse(b"ARTICLE <a@b>\r\n")
                .message_id()
                .unwrap()
                .as_str(),
            "<a@b>"
        );
        assert!(RequestLine::parse(b"ARTICLE\r\n").message_id().is_none());
        assert!(RequestLine::parse(b"ARTICLE 1\r\n").message_id().is_none());
        assert!(RequestLine::parse(b"BODY 2\r\n").message_id().is_none());
        assert!(RequestLine::parse(b"HEAD 3\r\n").message_id().is_none());
        assert!(RequestLine::parse(b"STAT 4\r\n").message_id().is_none());
        assert!(
            RequestLine::parse(b"ARTICLE <a b>\r\n")
                .message_id()
                .is_none()
        );
        assert!(
            RequestLine::parse(b"ARTICLE <a@b> extra\r\n")
                .message_id()
                .is_none()
        );
    }

    #[test]
    fn request_line_preserves_rfc_selector_arguments() {
        for (line, expected_verb, expected_args) in [
            (
                b"ARTICLE\r\n".as_slice(),
                b"ARTICLE".as_slice(),
                b"".as_slice(),
            ),
            (
                b"ARTICLE 42\r\n".as_slice(),
                b"ARTICLE".as_slice(),
                b"42".as_slice(),
            ),
            (
                b"BODY <body@test>\r\n".as_slice(),
                b"BODY".as_slice(),
                b"<body@test>".as_slice(),
            ),
            (
                b"HEAD 9\r\n".as_slice(),
                b"HEAD".as_slice(),
                b"9".as_slice(),
            ),
            (
                b"STAT <stat@test>\r\n".as_slice(),
                b"STAT".as_slice(),
                b"<stat@test>".as_slice(),
            ),
        ] {
            let parsed = RequestLine::parse(line);
            assert_eq!(parsed.verb(), expected_verb, "{line:?}");
            assert_eq!(parsed.args(), expected_args, "{line:?}");
        }
    }

    #[test]
    fn request_kind_multiline_expectation_matches_supported_responses() {
        // RFC 3977 sections 3.2 and 9.4 make response framing protocol
        // metadata, not a client concern. This matrix also covers the
        // command-dependent 211 exception called out in RFC 3977 section 3.2.
        assert!(RequestKind::Article.expects_multiline_response(StatusCode(220)));
        assert!(RequestKind::Head.expects_multiline_response(StatusCode(221)));
        assert!(RequestKind::Body.expects_multiline_response(StatusCode(222)));
        assert!(!RequestKind::Group.expects_multiline_response(StatusCode(211)));
        assert!(RequestKind::ListGroup.expects_multiline_response(StatusCode(211)));
        assert!(!RequestKind::Last.expects_multiline_response(StatusCode(223)));
        assert!(!RequestKind::Next.expects_multiline_response(StatusCode(223)));
        assert!(RequestKind::List.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::ListActive.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::ListActiveTimes.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::ListNewsgroups.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::ListOverviewFmt.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::ListHeaders.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::ListDistribPats.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::Over.expects_multiline_response(StatusCode(224)));
        assert!(RequestKind::Xhdr.expects_multiline_response(StatusCode(221)));
        assert!(RequestKind::NewGroups.expects_multiline_response(StatusCode(231)));
        assert!(RequestKind::NewNews.expects_multiline_response(StatusCode(230)));
        assert!(!RequestKind::Post.expects_multiline_response(StatusCode(340)));
        assert!(!RequestKind::Ihave.expects_multiline_response(StatusCode(335)));
        assert!(!RequestKind::Check.expects_multiline_response(StatusCode(238)));
        assert!(!RequestKind::TakeThis.expects_multiline_response(StatusCode(239)));
        assert!(!RequestKind::AuthInfoUser.expects_multiline_response(StatusCode(381)));
        assert!(!RequestKind::AuthInfoPass.expects_multiline_response(StatusCode(281)));
        assert!(!RequestKind::AuthInfo.expects_multiline_response(StatusCode(281)));
        assert!(!RequestKind::StartTls.expects_multiline_response(StatusCode(382)));
        assert!(RequestKind::Capabilities.expects_multiline_response(StatusCode(101)));
        assert!(!RequestKind::Date.expects_multiline_response(StatusCode(111)));
        assert!(!RequestKind::ModeReader.expects_multiline_response(StatusCode(201)));
        assert!(!RequestKind::Quit.expects_multiline_response(StatusCode(205)));
        assert!(!RequestKind::Article.expects_multiline_response(StatusCode(430)));
    }

    #[test]
    fn response_framing_reports_multiline_single_line_and_unexpected_statuses() {
        assert_eq!(
            ResponseFraming::for_request_status(RequestKind::ListGroup, StatusCode(211)),
            ResponseFraming::Multiline
        );
        assert_eq!(
            ResponseFraming::for_request_status(RequestKind::Group, StatusCode(211)),
            ResponseFraming::SingleLine
        );
        assert_eq!(
            ResponseFraming::for_request_status(RequestKind::Article, StatusCode(430)),
            ResponseFraming::SingleLine
        );
        assert_eq!(
            ResponseFraming::for_request_status(RequestKind::Article, StatusCode(223)),
            ResponseFraming::Unexpected
        );
        assert_eq!(
            ResponseFraming::for_request_status(RequestKind::Unknown, StatusCode(220)),
            ResponseFraming::Multiline
        );
        assert_eq!(
            ResponseFraming::for_request_status(RequestKind::Xhdr, StatusCode(221)),
            ResponseFraming::Multiline
        );
    }

    #[test]
    fn response_frame_parse_returns_borrowed_whole_response_parts() {
        // RFC 3977 section 9.4 defines multi-line responses as the initial
        // response line followed by the section 3.1.1 dot-terminated block.
        let wire = b"222 1 <body@test> body follows\r\nbody line\r\n.\r\nNEXT";
        let response = match ResponseFrame::parse(RequestKind::Body, wire) {
            ResponseFrameParse::Complete(response) => response,
            other => panic!("unexpected parse result: {other:?}"),
        };

        assert_eq!(response.kind(), RequestKind::Body);
        assert_eq!(response.status(), StatusCode(222));
        assert_eq!(response.descriptor().framing(), ResponseFraming::Multiline);
        assert_eq!(
            response.status_line(),
            b"222 1 <body@test> body follows\r\n"
        );
        assert_eq!(response.content(), b"body line\r\n");
        assert_eq!(response.terminator(), b".\r\n");
        assert_eq!(
            response.bytes(),
            b"222 1 <body@test> body follows\r\nbody line\r\n.\r\n"
        );
        assert_eq!(response.consumed(), response.bytes().len());
    }

    #[test]
    fn response_frame_parse_returns_single_line_frame_without_waiting_for_dot() {
        let wire = b"430 no article with that message-id\r\nNEXT";
        let response = match ResponseFrame::parse(RequestKind::Article, wire) {
            ResponseFrameParse::Complete(response) => response,
            other => panic!("unexpected parse result: {other:?}"),
        };

        assert_eq!(response.status(), StatusCode(430));
        assert_eq!(response.descriptor().framing(), ResponseFraming::SingleLine);
        assert_eq!(
            response.status_line(),
            b"430 no article with that message-id\r\n"
        );
        assert_eq!(response.content(), b"");
        assert_eq!(response.terminator(), b"");
        assert_eq!(response.bytes(), b"430 no article with that message-id\r\n");
        assert_eq!(
            response.consumed(),
            b"430 no article with that message-id\r\n".len()
        );
    }

    #[test]
    fn response_frame_parse_reports_need_more_and_invalid_without_allocating() {
        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        crate::TEST_ALLOCATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));

        assert!(matches!(
            ResponseFrame::parse(RequestKind::Body, b"222 body follows\r\nbody"),
            ResponseFrameParse::NeedMore
        ));
        assert!(matches!(
            ResponseFrame::parse(RequestKind::Body, b"222 body follows\nbody\r\n.\r\n"),
            ResponseFrameParse::Invalid
        ));
        assert!(matches!(
            ResponseFrame::parse(RequestKind::Capabilities, b"101 capabilities follow\r\n.\r\n"),
            ResponseFrameParse::Complete(response)
                if response.content().is_empty() && response.terminator() == b".\r\n"
        ));

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        let allocations = crate::TEST_ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(allocations, 0, "borrowed response frame parsing allocated");
    }

    #[test]
    fn protocol_parse_and_framing_metadata_do_not_allocate() {
        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        crate::TEST_ALLOCATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));

        let cases = [
            (
                b"ARTICLE <a@b>\r\n".as_slice(),
                RequestKind::Article,
                StatusCode(220),
            ),
            (b"BODY 1\r\n".as_slice(), RequestKind::Body, StatusCode(222)),
            (b"HEAD 1\r\n".as_slice(), RequestKind::Head, StatusCode(221)),
            (b"STAT 1\r\n".as_slice(), RequestKind::Stat, StatusCode(223)),
            (b"LIST\r\n".as_slice(), RequestKind::List, StatusCode(215)),
            (
                b"LIST ACTIVE.TIMES comp.*\r\n".as_slice(),
                RequestKind::ListActiveTimes,
                StatusCode(215),
            ),
            (
                b"AUTHINFO USER bench\r\n".as_slice(),
                RequestKind::AuthInfoUser,
                StatusCode(381),
            ),
            (
                b"MODE READER\r\n".as_slice(),
                RequestKind::ModeReader,
                StatusCode(201),
            ),
            (
                b"CAPABILITIES\r\n".as_slice(),
                RequestKind::Capabilities,
                StatusCode(101),
            ),
            (b"QUIT\r\n".as_slice(), RequestKind::Quit, StatusCode(205)),
        ];

        for (line, kind, status) in cases {
            assert_eq!(RequestLine::parse(line).kind(), kind);
            let descriptor = ResponseDescriptor::for_request_status(kind, status);
            assert_eq!(descriptor.kind(), kind);
            assert_eq!(descriptor.status_code(), status);
            for descriptor in responses_for_request(kind) {
                let _ = descriptor.status_code();
            }
        }

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        let allocations = crate::TEST_ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(allocations, 0, "protocol metadata hot path allocated");
    }

    #[test]
    fn request_serializes_article_body_head_stat_and_simple_wire() {
        let article = Request::Article {
            article_ref: ArticleRef::MessageId(MessageId::from_borrowed("<a@b>").unwrap()),
        };
        let body = Request::Body {
            article_ref: ArticleRef::MessageId(MessageId::from_borrowed("<c@d>").unwrap()),
        };
        let head = Request::Head {
            article_ref: ArticleRef::MessageId(MessageId::from_borrowed("<e@f>").unwrap()),
        };
        let stat = Request::Stat {
            article_ref: ArticleRef::MessageId(MessageId::from_borrowed("<g@h>").unwrap()),
        };
        let group = Request::Group {
            group: GroupName::from_borrowed("alt.test").unwrap(),
        };
        let listgroup = Request::ListGroup {
            group: Some(GroupName::from_borrowed("alt.test").unwrap()),
            range: None,
        };
        let listgroup_current = Request::ListGroup {
            group: None,
            range: None,
        };
        let listgroup_current_range = Request::ListGroup {
            group: None,
            range: Some(ListGroupRange::from_borrowed("1-").unwrap()),
        };
        let listgroup_group_range = Request::ListGroup {
            group: Some(GroupName::from_borrowed("alt.test").unwrap()),
            range: Some(ListGroupRange::from_borrowed("1-10").unwrap()),
        };
        let last = Request::Last;
        let next = Request::Next;
        let over = Request::Over {
            selector: ArticleSelector::from_borrowed("1-10").unwrap(),
        };
        let xover = Request::Xover {
            selector: ArticleSelector::from_borrowed("<o@v>").unwrap(),
        };
        let hdr = Request::Hdr {
            header: HeaderName::from_borrowed("Subject").unwrap(),
            selector: ArticleSelector::from_borrowed("1-10").unwrap(),
        };
        let xhdr = Request::Xhdr {
            header: HeaderName::from_borrowed("Message-ID").unwrap(),
            selector: ArticleSelector::from_borrowed("<a@b>").unwrap(),
        };
        let newgroups = Request::NewGroups {
            date: NntpDate::from_borrowed("20260101").unwrap(),
            time: NntpTime::from_borrowed("000000").unwrap(),
            gmt: true,
        };
        let newnews = Request::NewNews {
            wildmat: Wildmat::from_borrowed("comp.lang.*,alt.test").unwrap(),
            date: NntpDate::from_borrowed("20260101").unwrap(),
            time: NntpTime::from_borrowed("000000").unwrap(),
            gmt: false,
        };
        let post = Request::Post;
        let ihave = Request::Ihave {
            message_id: MessageId::from_borrowed("<ihave@test>").unwrap(),
        };
        let check = Request::Check {
            message_id: MessageId::from_borrowed("<check@test>").unwrap(),
        };
        let takethis = Request::TakeThis {
            message_id: MessageId::from_borrowed("<take@test>").unwrap(),
            article: ArticleTransfer::from_borrowed(
                b"Subject: test\r\n\r\npayload line\r\n.leading dot\r\n",
            ),
        };
        let authinfo_user = Request::AuthInfo {
            kind: AuthInfoKind::User,
            value: AuthInfoValue::from_borrowed("user-name").unwrap(),
        };
        let authinfo_pass = Request::AuthInfo {
            kind: AuthInfoKind::Pass,
            value: AuthInfoValue::from_borrowed("pass-word").unwrap(),
        };
        let starttls = Request::StartTls;
        let list = Request::List;
        let list_active = Request::ListVariant {
            kind: ListKind::Active,
            wildmat: None,
        };
        let list_active_times = Request::ListVariant {
            kind: ListKind::ActiveTimes,
            wildmat: Some(Wildmat::from_borrowed("comp.lang.*").unwrap()),
        };
        let list_newsgroups = Request::ListVariant {
            kind: ListKind::Newsgroups,
            wildmat: Some(Wildmat::from_borrowed("comp.lang.*").unwrap()),
        };
        let list_overview_fmt = Request::ListVariant {
            kind: ListKind::OverviewFmt,
            wildmat: None,
        };
        let list_headers = Request::ListVariant {
            kind: ListKind::Headers,
            wildmat: None,
        };
        let list_distrib_pats = Request::ListVariant {
            kind: ListKind::DistribPats,
            wildmat: None,
        };
        let help = Request::Help;
        let capabilities = Request::Capabilities;
        let date = Request::Date;
        let mode_reader = Request::ModeReader;
        let quit = Request::Quit;

        let mut wire = Vec::new();
        article.write_wire_to(&mut wire);
        assert_eq!(article.kind(), RequestKind::Article);
        assert_eq!(wire, b"ARTICLE <a@b>\r\n");

        wire.clear();
        body.write_wire_to(&mut wire);
        assert_eq!(body.kind(), RequestKind::Body);
        assert_eq!(wire, b"BODY <c@d>\r\n");

        wire.clear();
        head.write_wire_to(&mut wire);
        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(wire, b"HEAD <e@f>\r\n");

        wire.clear();
        stat.write_wire_to(&mut wire);
        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(wire, b"STAT <g@h>\r\n");

        wire.clear();
        group.write_wire_to(&mut wire);
        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(wire, b"GROUP alt.test\r\n");

        wire.clear();
        listgroup.write_wire_to(&mut wire);
        assert_eq!(listgroup.kind(), RequestKind::ListGroup);
        assert_eq!(wire, b"LISTGROUP alt.test\r\n");

        wire.clear();
        listgroup_current.write_wire_to(&mut wire);
        assert_eq!(listgroup_current.kind(), RequestKind::ListGroup);
        assert_eq!(wire, b"LISTGROUP\r\n");

        wire.clear();
        listgroup_current_range.write_wire_to(&mut wire);
        assert_eq!(listgroup_current_range.kind(), RequestKind::ListGroup);
        assert_eq!(wire, b"LISTGROUP 1-\r\n");

        wire.clear();
        listgroup_group_range.write_wire_to(&mut wire);
        assert_eq!(listgroup_group_range.kind(), RequestKind::ListGroup);
        assert_eq!(wire, b"LISTGROUP alt.test 1-10\r\n");

        wire.clear();
        last.write_wire_to(&mut wire);
        assert_eq!(last.kind(), RequestKind::Last);
        assert_eq!(wire, b"LAST\r\n");

        wire.clear();
        next.write_wire_to(&mut wire);
        assert_eq!(next.kind(), RequestKind::Next);
        assert_eq!(wire, b"NEXT\r\n");

        wire.clear();
        over.write_wire_to(&mut wire);
        assert_eq!(over.kind(), RequestKind::Over);
        assert_eq!(wire, b"OVER 1-10\r\n");

        wire.clear();
        xover.write_wire_to(&mut wire);
        assert_eq!(xover.kind(), RequestKind::Xover);
        assert_eq!(wire, b"XOVER <o@v>\r\n");

        wire.clear();
        hdr.write_wire_to(&mut wire);
        assert_eq!(hdr.kind(), RequestKind::Hdr);
        assert_eq!(wire, b"HDR Subject 1-10\r\n");

        wire.clear();
        xhdr.write_wire_to(&mut wire);
        assert_eq!(xhdr.kind(), RequestKind::Xhdr);
        assert_eq!(wire, b"XHDR Message-ID <a@b>\r\n");

        wire.clear();
        newgroups.write_wire_to(&mut wire);
        assert_eq!(newgroups.kind(), RequestKind::NewGroups);
        assert_eq!(wire, b"NEWGROUPS 20260101 000000 GMT\r\n");

        wire.clear();
        newnews.write_wire_to(&mut wire);
        assert_eq!(newnews.kind(), RequestKind::NewNews);
        assert_eq!(wire, b"NEWNEWS comp.lang.*,alt.test 20260101 000000\r\n");

        wire.clear();
        post.write_wire_to(&mut wire);
        assert_eq!(post.kind(), RequestKind::Post);
        assert_eq!(wire, b"POST\r\n");

        wire.clear();
        ihave.write_wire_to(&mut wire);
        assert_eq!(ihave.kind(), RequestKind::Ihave);
        assert_eq!(wire, b"IHAVE <ihave@test>\r\n");

        wire.clear();
        check.write_wire_to(&mut wire);
        assert_eq!(check.kind(), RequestKind::Check);
        assert_eq!(wire, b"CHECK <check@test>\r\n");

        wire.clear();
        takethis.write_wire_to(&mut wire);
        assert_eq!(takethis.kind(), RequestKind::TakeThis);
        assert_eq!(
            wire,
            b"TAKETHIS <take@test>\r\nSubject: test\r\n\r\npayload line\r\n..leading dot\r\n.\r\n"
        );

        wire.clear();
        authinfo_user.write_wire_to(&mut wire);
        assert_eq!(authinfo_user.kind(), RequestKind::AuthInfoUser);
        assert_eq!(wire, b"AUTHINFO USER user-name\r\n");

        wire.clear();
        authinfo_pass.write_wire_to(&mut wire);
        assert_eq!(authinfo_pass.kind(), RequestKind::AuthInfoPass);
        assert_eq!(wire, b"AUTHINFO PASS pass-word\r\n");

        wire.clear();
        starttls.write_wire_to(&mut wire);
        assert_eq!(starttls.kind(), RequestKind::StartTls);
        assert_eq!(wire, b"STARTTLS\r\n");

        wire.clear();
        list.write_wire_to(&mut wire);
        assert_eq!(list.kind(), RequestKind::List);
        assert_eq!(wire, b"LIST\r\n");

        wire.clear();
        list_active.write_wire_to(&mut wire);
        assert_eq!(list_active.kind(), RequestKind::ListActive);
        assert_eq!(wire, b"LIST ACTIVE\r\n");

        wire.clear();
        list_active_times.write_wire_to(&mut wire);
        assert_eq!(list_active_times.kind(), RequestKind::ListActiveTimes);
        assert_eq!(wire, b"LIST ACTIVE.TIMES comp.lang.*\r\n");

        wire.clear();
        list_newsgroups.write_wire_to(&mut wire);
        assert_eq!(list_newsgroups.kind(), RequestKind::ListNewsgroups);
        assert_eq!(wire, b"LIST NEWSGROUPS comp.lang.*\r\n");

        wire.clear();
        list_overview_fmt.write_wire_to(&mut wire);
        assert_eq!(list_overview_fmt.kind(), RequestKind::ListOverviewFmt);
        assert_eq!(wire, b"LIST OVERVIEW.FMT\r\n");

        wire.clear();
        list_headers.write_wire_to(&mut wire);
        assert_eq!(list_headers.kind(), RequestKind::ListHeaders);
        assert_eq!(wire, b"LIST HEADERS\r\n");

        wire.clear();
        list_distrib_pats.write_wire_to(&mut wire);
        assert_eq!(list_distrib_pats.kind(), RequestKind::ListDistribPats);
        assert_eq!(wire, b"LIST DISTRIB.PATS\r\n");

        wire.clear();
        help.write_wire_to(&mut wire);
        assert_eq!(help.kind(), RequestKind::Help);
        assert_eq!(wire, b"HELP\r\n");

        wire.clear();
        capabilities.write_wire_to(&mut wire);
        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert_eq!(wire, b"CAPABILITIES\r\n");

        wire.clear();
        date.write_wire_to(&mut wire);
        assert_eq!(date.kind(), RequestKind::Date);
        assert_eq!(wire, b"DATE\r\n");

        wire.clear();
        mode_reader.write_wire_to(&mut wire);
        assert_eq!(mode_reader.kind(), RequestKind::ModeReader);
        assert_eq!(wire, b"MODE READER\r\n");

        wire.clear();
        quit.write_wire_to(&mut wire);
        assert_eq!(quit.kind(), RequestKind::Quit);
        assert_eq!(wire, b"QUIT\r\n");
    }

    #[test]
    fn request_constructors_wrap_and_expose_message_ids() {
        let article = Request::article("a@b").unwrap();
        let article_current = Request::article_current();
        let article_number = Request::article_number(42).unwrap();
        let body = Request::body("<c@d>").unwrap();
        let body_current = Request::body_current();
        let body_number = Request::body_number(7).unwrap();
        let head = Request::head("e@f").unwrap();
        let head_current = Request::head_current();
        let head_number = Request::head_number(9).unwrap();
        let stat = Request::stat("<g@h>").unwrap();
        let stat_current = Request::stat_current();
        let stat_number = Request::stat_number(11).unwrap();
        let group = Request::group("alt.test").unwrap();
        let listgroup = Request::listgroup("alt.test").unwrap();
        let listgroup_current = Request::listgroup_current();
        let listgroup_range = Request::listgroup_range("1-").unwrap();
        let listgroup_group_range = Request::listgroup_group_range("alt.test", "1-10").unwrap();
        let last = Request::last();
        let next = Request::next();
        let over = Request::over("1-10").unwrap();
        let xover = Request::xover("<g@h>").unwrap();
        let hdr = Request::hdr("Subject", "1-10").unwrap();
        let xhdr = Request::xhdr("Message-ID", "<g@h>").unwrap();
        let newgroups = Request::newgroups("20260101", "000000", true).unwrap();
        let newnews =
            Request::newnews("comp.lang.*,alt.test", "20260101", "000000", false).unwrap();
        let post = Request::post();
        let ihave = Request::ihave("ihave@test").unwrap();
        let check = Request::check("check@test").unwrap();
        let takethis = Request::takethis("take@test", b"Subject: one\r\n\r\nbody").unwrap();
        let authinfo_user = Request::authinfo_user("user-name").unwrap();
        let authinfo_pass = Request::authinfo_pass("pass-word").unwrap();
        let starttls = Request::starttls();
        let list = Request::list();
        let list_active = Request::list_active();
        let list_active_times = Request::list_active_times_wildmat("comp.lang.*").unwrap();
        let list_newsgroups = Request::list_newsgroups_wildmat("comp.lang.*").unwrap();
        let list_overview_fmt = Request::list_overview_fmt();
        let list_headers = Request::list_headers();
        let list_distrib_pats = Request::list_distrib_pats();
        let help = Request::help();
        let capabilities = Request::capabilities();
        let date = Request::date();
        let mode_reader = Request::mode_reader();
        let quit = Request::quit();
        let article_message_ref = ArticleRef::MessageId(MessageId::from_borrowed("<a@b>").unwrap());

        assert_eq!(article.kind(), RequestKind::Article);
        assert_eq!(article.message_id().unwrap().as_str(), "<a@b>");
        assert_eq!(article.article_ref(), Some(&article_message_ref));
        assert_eq!(article_current.article_ref(), Some(&ArticleRef::Current));
        assert_eq!(article_number.article_ref(), Some(&ArticleRef::Number(42)));
        assert_eq!(body.kind(), RequestKind::Body);
        assert_eq!(body.message_id().unwrap().as_str(), "<c@d>");
        assert_eq!(body_current.article_ref(), Some(&ArticleRef::Current));
        assert_eq!(body_number.article_ref(), Some(&ArticleRef::Number(7)));
        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.message_id().unwrap().as_str(), "<e@f>");
        assert_eq!(head_current.article_ref(), Some(&ArticleRef::Current));
        assert_eq!(head_number.article_ref(), Some(&ArticleRef::Number(9)));
        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.message_id().unwrap().as_str(), "<g@h>");
        assert_eq!(stat_current.article_ref(), Some(&ArticleRef::Current));
        assert_eq!(stat_number.article_ref(), Some(&ArticleRef::Number(11)));
        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(group.group_name().map(GroupName::as_str), Some("alt.test"));
        assert_eq!(listgroup.kind(), RequestKind::ListGroup);
        assert_eq!(
            listgroup.group_name().map(GroupName::as_str),
            Some("alt.test")
        );
        assert_eq!(listgroup_current.kind(), RequestKind::ListGroup);
        assert!(listgroup_current.group_name().is_none());
        assert_eq!(
            listgroup_range
                .listgroup_range_arg()
                .map(ListGroupRange::as_str),
            Some("1-")
        );
        assert_eq!(
            listgroup_group_range.group_name().map(GroupName::as_str),
            Some("alt.test")
        );
        assert_eq!(
            listgroup_group_range
                .listgroup_range_arg()
                .map(ListGroupRange::as_str),
            Some("1-10")
        );
        assert_eq!(last.kind(), RequestKind::Last);
        assert!(last.group_name().is_none());
        assert_eq!(next.kind(), RequestKind::Next);
        assert!(next.group_name().is_none());
        assert_eq!(over.kind(), RequestKind::Over);
        assert_eq!(
            over.overview_selector().map(ArticleSelector::as_str),
            Some("1-10")
        );
        assert_eq!(xover.kind(), RequestKind::Xover);
        assert_eq!(
            xover.overview_selector().map(ArticleSelector::as_str),
            Some("<g@h>")
        );
        assert_eq!(hdr.kind(), RequestKind::Hdr);
        assert_eq!(
            hdr.header_query()
                .map(|(header, selector)| (header.as_str(), selector.as_str())),
            Some(("Subject", "1-10"))
        );
        assert_eq!(xhdr.kind(), RequestKind::Xhdr);
        assert_eq!(
            xhdr.header_query()
                .map(|(header, selector)| (header.as_str(), selector.as_str())),
            Some(("Message-ID", "<g@h>"))
        );
        assert_eq!(newgroups.kind(), RequestKind::NewGroups);
        assert_eq!(
            newgroups.discovery_datetime().map(|(date, time, gmt)| (
                date.as_str(),
                time.as_str(),
                gmt
            )),
            Some(("20260101", "000000", true))
        );
        assert!(newgroups.wildmat().is_none());
        assert_eq!(newnews.kind(), RequestKind::NewNews);
        assert_eq!(
            newnews.wildmat().map(Wildmat::as_str),
            Some("comp.lang.*,alt.test")
        );
        assert_eq!(
            newnews.discovery_datetime().map(|(date, time, gmt)| (
                date.as_str(),
                time.as_str(),
                gmt
            )),
            Some(("20260101", "000000", false))
        );
        assert_eq!(post.kind(), RequestKind::Post);
        assert!(post.message_id().is_none());
        assert_eq!(ihave.kind(), RequestKind::Ihave);
        assert_eq!(
            ihave.message_id().map(MessageId::as_str),
            Some("<ihave@test>")
        );
        assert_eq!(check.kind(), RequestKind::Check);
        assert_eq!(
            check.message_id().map(MessageId::as_str),
            Some("<check@test>")
        );
        assert_eq!(takethis.kind(), RequestKind::TakeThis);
        assert_eq!(
            takethis.message_id().map(MessageId::as_str),
            Some("<take@test>")
        );
        assert_eq!(
            takethis.article_transfer().map(ArticleTransfer::as_bytes),
            Some(&b"Subject: one\r\n\r\nbody"[..])
        );
        assert_eq!(authinfo_user.kind(), RequestKind::AuthInfoUser);
        assert_eq!(
            authinfo_user
                .auth_info()
                .map(|(kind, value)| (kind, value.as_str())),
            Some((AuthInfoKind::User, "user-name"))
        );
        assert_eq!(authinfo_pass.kind(), RequestKind::AuthInfoPass);
        assert_eq!(
            authinfo_pass
                .auth_info()
                .map(|(kind, value)| (kind, value.as_str())),
            Some((AuthInfoKind::Pass, "pass-word"))
        );
        assert_eq!(starttls.kind(), RequestKind::StartTls);
        assert!(starttls.message_id().is_none());
        assert_eq!(list.kind(), RequestKind::List);
        assert!(list.message_id().is_none());
        assert_eq!(
            list_active
                .list_variant()
                .map(|(kind, wildmat)| (kind, wildmat.is_some())),
            Some((ListKind::Active, false))
        );
        assert_eq!(
            list_active_times
                .list_variant()
                .map(|(kind, wildmat)| (kind, wildmat.map(Wildmat::as_str))),
            Some((ListKind::ActiveTimes, Some("comp.lang.*")))
        );
        assert_eq!(
            list_newsgroups
                .list_variant()
                .map(|(kind, wildmat)| (kind, wildmat.map(Wildmat::as_str))),
            Some((ListKind::Newsgroups, Some("comp.lang.*")))
        );
        assert_eq!(
            list_overview_fmt.list_variant().map(|(kind, _)| kind),
            Some(ListKind::OverviewFmt)
        );
        assert_eq!(
            list_headers.list_variant().map(|(kind, _)| kind),
            Some(ListKind::Headers)
        );
        assert_eq!(
            list_distrib_pats.list_variant().map(|(kind, _)| kind),
            Some(ListKind::DistribPats)
        );
        assert_eq!(help.kind(), RequestKind::Help);
        assert!(help.message_id().is_none());
        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert!(capabilities.message_id().is_none());
        assert_eq!(date.kind(), RequestKind::Date);
        assert!(date.message_id().is_none());
        assert_eq!(mode_reader.kind(), RequestKind::ModeReader);
        assert!(mode_reader.message_id().is_none());
        assert_eq!(quit.kind(), RequestKind::Quit);
        assert!(quit.message_id().is_none());
        assert!(Request::article("<bad id>").is_err());
        assert!(Request::article_number(0).is_err());
        assert!(Request::article_number(MAX_ARTICLE_NUMBER + 1).is_err());
        assert!(Request::body_number(MAX_ARTICLE_NUMBER + 1).is_err());
        assert!(Request::head_number(MAX_ARTICLE_NUMBER + 1).is_err());
        assert!(Request::stat_number(MAX_ARTICLE_NUMBER + 1).is_err());
        assert!(Request::body_selector("1-10").is_err());
        assert!(Request::group("").is_err());
        assert!(Request::listgroup("<a@b>").is_err());
        assert!(Request::listgroup_range("0").is_err());
        assert!(Request::listgroup_range("-10").is_err());
        assert!(Request::listgroup_range("1-10-20").is_err());
        assert!(Request::over("1 2").is_err());
        assert!(Request::xover("").is_err());
        assert!(Request::hdr("Bad Header", "1").is_err());
        assert!(Request::xhdr("Subject", "1 2").is_err());
        assert!(Request::newgroups("20261301", "000000", true).is_err());
        assert!(Request::newnews("", "20260101", "000000", false).is_err());
        assert!(Request::list_active_wildmat("").is_err());
        assert!(Request::authinfo_user("").is_err());
        assert!(Request::authinfo_user("user name").is_err());
        assert!(Request::authinfo_pass("pass\tword").is_err());
        assert!(Request::authinfo_pass("bad\nvalue").is_err());
    }

    #[test]
    fn article_family_request_wires_follow_rfc_selector_forms() {
        let requests = [
            (Request::article_current(), b"ARTICLE\r\n".as_slice()),
            (
                Request::article_number(42).unwrap(),
                b"ARTICLE 42\r\n".as_slice(),
            ),
            (
                Request::article("a@b").unwrap(),
                b"ARTICLE <a@b>\r\n".as_slice(),
            ),
            (Request::body_current(), b"BODY\r\n".as_slice()),
            (Request::body_number(7).unwrap(), b"BODY 7\r\n".as_slice()),
            (Request::body("c@d").unwrap(), b"BODY <c@d>\r\n".as_slice()),
            (Request::head_current(), b"HEAD\r\n".as_slice()),
            (Request::head_number(9).unwrap(), b"HEAD 9\r\n".as_slice()),
            (Request::head("e@f").unwrap(), b"HEAD <e@f>\r\n".as_slice()),
            (Request::stat_current(), b"STAT\r\n".as_slice()),
            (Request::stat_number(11).unwrap(), b"STAT 11\r\n".as_slice()),
            (Request::stat("g@h").unwrap(), b"STAT <g@h>\r\n".as_slice()),
        ];

        for (request, expected_wire) in requests {
            let mut wire = Vec::new();
            request.write_wire_to(&mut wire);
            assert_eq!(wire, expected_wire);
        }
    }

    #[test]
    fn request_wire_uses_one_crlf_terminator() {
        let requests = [
            Request::article("a@b").unwrap(),
            Request::article_current(),
            Request::article_number(42).unwrap(),
            Request::body("c@d").unwrap(),
            Request::body_current(),
            Request::body_number(7).unwrap(),
            Request::head("e@f").unwrap(),
            Request::head_current(),
            Request::head_number(9).unwrap(),
            Request::stat("g@h").unwrap(),
            Request::stat_current(),
            Request::stat_number(11).unwrap(),
            Request::group("alt.test").unwrap(),
            Request::listgroup("alt.test").unwrap(),
            Request::listgroup_current(),
            Request::listgroup_range("1-").unwrap(),
            Request::listgroup_group_range("alt.test", "1-10").unwrap(),
            Request::last(),
            Request::next(),
            Request::over("1-10").unwrap(),
            Request::xover("<i@j>").unwrap(),
            Request::hdr("Subject", "1-10").unwrap(),
            Request::xhdr("Message-ID", "<i@j>").unwrap(),
            Request::newgroups("20260101", "000000", true).unwrap(),
            Request::newnews("comp.lang.*", "20260101", "000000", false).unwrap(),
            Request::post(),
            Request::ihave("ihave@test").unwrap(),
            Request::check("check@test").unwrap(),
            Request::authinfo_user("user").unwrap(),
            Request::authinfo_pass("pass").unwrap(),
            Request::starttls(),
            Request::list(),
            Request::list_active(),
            Request::list_active_times_wildmat("comp.lang.*").unwrap(),
            Request::list_newsgroups_wildmat("comp.lang.*").unwrap(),
            Request::list_overview_fmt(),
            Request::list_headers(),
            Request::list_distrib_pats(),
            Request::help(),
            Request::capabilities(),
            Request::date(),
            Request::mode_reader(),
            Request::quit(),
        ];

        for request in requests {
            let mut wire = Vec::new();
            request.write_wire_to(&mut wire);
            assert!(wire.ends_with(b"\r\n"), "{wire:?}");
            assert_eq!(
                wire.windows(2).filter(|window| *window == b"\r\n").count(),
                1,
                "{wire:?}"
            );
        }

        let mut wire = Vec::new();
        Request::takethis("take@test", b"Subject: wire\r\n\r\n.line\r\nbody")
            .unwrap()
            .write_wire_to(&mut wire);
        assert!(wire.ends_with(b".\r\n"), "{wire:?}");
        assert!(wire.starts_with(b"TAKETHIS <take@test>\r\n"), "{wire:?}");
        assert!(
            wire.windows(8).any(|window| window == b"..line\r\n"),
            "{wire:?}"
        );

        let mut wire = Vec::new();
        Request::article_current().write_wire_to(&mut wire);
        assert_eq!(wire, b"ARTICLE\r\n");

        wire.clear();
        Request::stat_number(42).unwrap().write_wire_to(&mut wire);
        assert_eq!(wire, b"STAT 42\r\n");
    }
}
