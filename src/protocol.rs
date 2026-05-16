//! Typed NNTP protocol helpers for the redesign path.
//!
//! These helpers are intentionally pure and additive so the current server/client
//! loops can adopt them incrementally without changing hot-path behavior first.

use std::borrow::Cow;

pub mod article;

pub use article::{Article, ArticleNumber, ArticleParseError, HeaderIter, Headers};

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

/// Validated NNTP message-id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId<'a>(Cow<'a, str>);

impl<'a> MessageId<'a> {
    /// Construct a borrowed message-id after validation.
    pub fn from_borrowed(value: &'a str) -> Result<Self, InvalidMessageId> {
        validate_message_id(value)?;
        Ok(Self(Cow::Borrowed(value)))
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
        Ok(MessageId(Cow::Owned(wrapped)))
    }

    /// Borrow the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidMessageId;

fn validate_message_id(value: &str) -> Result<(), InvalidMessageId> {
    if value.len() < 3 || !value.starts_with('<') || !value.ends_with('>') {
        return Err(InvalidMessageId);
    }
    if value[1..value.len() - 1]
        .bytes()
        .any(|byte| byte.is_ascii_whitespace())
    {
        return Err(InvalidMessageId);
    }
    Ok(())
}

/// Typed request kind for the currently-supported command set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    Article,
    Body,
    Head,
    Stat,
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
        if status.is_error() {
            return false;
        }

        matches!(
            (self, status.as_u16()),
            (Self::Article, 220)
                | (Self::Head, 221)
                | (Self::Body, 222)
                | (Self::ListGroup, 211)
                | (Self::Help, 100)
                | (Self::Capabilities, 101)
                | (Self::List, 215)
                | (Self::Over | Self::Xover, 224)
                | (Self::Hdr | Self::Xhdr, 225)
                | (Self::NewNews, 230)
                | (Self::NewGroups, 231)
        ) || matches!(self, Self::Unknown) && status_implies_multiline(status.as_u16())
    }
}

/// Typed client request for the current typed NNTP surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request<'a> {
    Article { message_id: MessageId<'a> },
    Body { message_id: MessageId<'a> },
    Head { message_id: MessageId<'a> },
    Stat { message_id: MessageId<'a> },
    Capabilities,
    Date,
    ModeReader,
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
            Self::Capabilities => RequestKind::Capabilities,
            Self::Date => RequestKind::Date,
            Self::ModeReader => RequestKind::ModeReader,
        }
    }

    /// Serialize the request onto the NNTP wire.
    pub fn write_wire_to(&self, output: &mut Vec<u8>) {
        match self {
            Self::Article { message_id } => write_request_wire(output, b"ARTICLE ", message_id),
            Self::Body { message_id } => write_request_wire(output, b"BODY ", message_id),
            Self::Head { message_id } => write_request_wire(output, b"HEAD ", message_id),
            Self::Stat { message_id } => write_request_wire(output, b"STAT ", message_id),
            Self::Capabilities => write_simple_request_wire(output, b"CAPABILITIES"),
            Self::Date => write_simple_request_wire(output, b"DATE"),
            Self::ModeReader => write_simple_request_wire(output, b"MODE READER"),
        }
    }

    /// Borrow the validated message-id carried by this request, if any.
    #[must_use]
    pub const fn message_id(&self) -> Option<&MessageId<'a>> {
        match self {
            Self::Article { message_id }
            | Self::Body { message_id }
            | Self::Head { message_id }
            | Self::Stat { message_id } => Some(message_id),
            Self::Capabilities | Self::Date | Self::ModeReader => None,
        }
    }
}

impl Request<'static> {
    /// Build an ARTICLE request from a borrowed or bare message-id string.
    pub fn article(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Article {
            message_id: MessageId::from_str_or_wrap(message_id)?,
        })
    }

    /// Build a BODY request from a borrowed or bare message-id string.
    pub fn body(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Body {
            message_id: MessageId::from_str_or_wrap(message_id)?,
        })
    }

    /// Build a HEAD request from a borrowed or bare message-id string.
    pub fn head(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Head {
            message_id: MessageId::from_str_or_wrap(message_id)?,
        })
    }

    /// Build a STAT request from a borrowed or bare message-id string.
    pub fn stat(message_id: impl AsRef<str>) -> Result<Self, InvalidMessageId> {
        Ok(Self::Stat {
            message_id: MessageId::from_str_or_wrap(message_id)?,
        })
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
        let line = trim_line_end(line);
        let split = memchr::memchr(b' ', line).unwrap_or(line.len());
        let verb = &line[..split];
        let args = if split < line.len() {
            line[split + 1..].trim_ascii()
        } else {
            &[]
        };

        Self {
            kind: classify_verb(verb, args),
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

fn trim_line_end(mut line: &[u8]) -> &[u8] {
    while matches!(line.last(), Some(b'\r' | b'\n')) {
        line = &line[..line.len() - 1];
    }
    line
}

fn classify_verb(verb: &[u8], arg: &[u8]) -> RequestKind {
    match verb.len() {
        3 if eq_ignore_ascii_case_const(verb, b"HDR") => RequestKind::Hdr,
        4 if eq_ignore_ascii_case_const(verb, b"BODY") => RequestKind::Body,
        4 if eq_ignore_ascii_case_const(verb, b"DATE") => RequestKind::Date,
        4 if eq_ignore_ascii_case_const(verb, b"HEAD") => RequestKind::Head,
        4 if eq_ignore_ascii_case_const(verb, b"HELP") => RequestKind::Help,
        4 if eq_ignore_ascii_case_const(verb, b"LAST") => RequestKind::Last,
        4 if eq_ignore_ascii_case_const(verb, b"LIST") => RequestKind::List,
        4 if eq_ignore_ascii_case_const(verb, b"MODE") && eq_ignore_ascii_case(arg, b"READER") => {
            RequestKind::ModeReader
        }
        4 if eq_ignore_ascii_case_const(verb, b"NEXT") => RequestKind::Next,
        4 if eq_ignore_ascii_case_const(verb, b"OVER") => RequestKind::Over,
        4 if eq_ignore_ascii_case_const(verb, b"POST") => RequestKind::Post,
        4 if eq_ignore_ascii_case_const(verb, b"QUIT") => RequestKind::Quit,
        4 if eq_ignore_ascii_case_const(verb, b"STAT") => RequestKind::Stat,
        4 if eq_ignore_ascii_case_const(verb, b"XHDR") => RequestKind::Xhdr,
        5 if eq_ignore_ascii_case_const(verb, b"CHECK") => RequestKind::Check,
        5 if eq_ignore_ascii_case_const(verb, b"GROUP") => RequestKind::Group,
        5 if eq_ignore_ascii_case_const(verb, b"IHAVE") => RequestKind::Ihave,
        5 if eq_ignore_ascii_case_const(verb, b"XOVER") => RequestKind::Xover,
        7 if eq_ignore_ascii_case_const(verb, b"ARTICLE") => RequestKind::Article,
        7 if eq_ignore_ascii_case_const(verb, b"NEWNEWS") => RequestKind::NewNews,
        8 if eq_ignore_ascii_case_const(verb, b"AUTHINFO") => RequestKind::AuthInfo,
        8 if eq_ignore_ascii_case_const(verb, b"STARTTLS") => RequestKind::StartTls,
        8 if eq_ignore_ascii_case_const(verb, b"TAKETHIS") => RequestKind::TakeThis,
        9 if eq_ignore_ascii_case_const(verb, b"LISTGROUP") => RequestKind::ListGroup,
        9 if eq_ignore_ascii_case_const(verb, b"NEWGROUPS") => RequestKind::NewGroups,
        12 if eq_ignore_ascii_case_const(verb, b"CAPABILITIES") => RequestKind::Capabilities,
        _ => RequestKind::Unknown,
    }
}

fn eq_ignore_ascii_case(actual: &[u8], expected_upper: &[u8]) -> bool {
    actual.len() == expected_upper.len()
        && actual
            .iter()
            .zip(expected_upper)
            .all(|(left, right)| left.to_ascii_uppercase() == *right)
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

fn write_request_wire(output: &mut Vec<u8>, verb: &[u8], message_id: &MessageId<'_>) {
    output.extend_from_slice(verb);
    output.extend_from_slice(message_id.as_str().as_bytes());
    output.extend_from_slice(b"\r\n");
}

fn write_simple_request_wire(output: &mut Vec<u8>, verb: &[u8]) {
    output.extend_from_slice(verb);
    output.extend_from_slice(b"\r\n");
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
    fn request_line_parses_current_command_set() {
        assert_eq!(
            RequestLine::parse(b"ARTICLE <a@b>").kind(),
            RequestKind::Article
        );
        assert_eq!(RequestLine::parse(b"ARTICLE").kind(), RequestKind::Article);
        assert_eq!(RequestLine::parse(b"body 123").kind(), RequestKind::Body);
        assert_eq!(RequestLine::parse(b"HEAD <a@b>").kind(), RequestKind::Head);
        assert_eq!(RequestLine::parse(b"STAT <a@b>").kind(), RequestKind::Stat);
        assert_eq!(RequestLine::parse(b"LIST ACTIVE").kind(), RequestKind::List);
        assert_eq!(RequestLine::parse(b"OVER 1-10").kind(), RequestKind::Over);
        assert_eq!(RequestLine::parse(b"XOVER 1-10").kind(), RequestKind::Xover);
        assert_eq!(
            RequestLine::parse(b"HDR Subject 1").kind(),
            RequestKind::Hdr
        );
        assert_eq!(
            RequestLine::parse(b"XHDR Subject 1").kind(),
            RequestKind::Xhdr
        );
        assert_eq!(
            RequestLine::parse(b"CAPABILITIES\r\n").kind(),
            RequestKind::Capabilities
        );
        assert_eq!(RequestLine::parse(b"DATE").kind(), RequestKind::Date);
        assert_eq!(
            RequestLine::parse(b"MODE READER").kind(),
            RequestKind::ModeReader
        );
        assert_eq!(RequestLine::parse(b"QUIT").kind(), RequestKind::Quit);
        assert_eq!(RequestLine::parse(b"HEAD 1").kind(), RequestKind::Head);
        assert_eq!(
            RequestLine::parse(b"MODE TRANSIT").kind(),
            RequestKind::Unknown
        );
    }

    #[test]
    fn request_line_exposes_message_id_when_args_are_exact_id() {
        assert_eq!(
            RequestLine::parse(b"ARTICLE <a@b>")
                .message_id()
                .unwrap()
                .as_str(),
            "<a@b>"
        );
        assert!(RequestLine::parse(b"ARTICLE").message_id().is_none());
        assert!(RequestLine::parse(b"ARTICLE <a b>").message_id().is_none());
        assert!(
            RequestLine::parse(b"ARTICLE <a@b> extra")
                .message_id()
                .is_none()
        );
    }

    #[test]
    fn request_kind_multiline_expectation_matches_supported_responses() {
        assert!(RequestKind::Article.expects_multiline_response(StatusCode(220)));
        assert!(RequestKind::Head.expects_multiline_response(StatusCode(221)));
        assert!(RequestKind::Body.expects_multiline_response(StatusCode(222)));
        assert!(RequestKind::List.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::Over.expects_multiline_response(StatusCode(224)));
        assert!(RequestKind::Xhdr.expects_multiline_response(StatusCode(225)));
        assert!(RequestKind::Capabilities.expects_multiline_response(StatusCode(101)));
        assert!(!RequestKind::Date.expects_multiline_response(StatusCode(111)));
        assert!(!RequestKind::ModeReader.expects_multiline_response(StatusCode(201)));
        assert!(!RequestKind::Quit.expects_multiline_response(StatusCode(205)));
        assert!(!RequestKind::Article.expects_multiline_response(StatusCode(430)));
    }

    #[test]
    fn request_serializes_article_body_head_stat_and_simple_wire() {
        let article = Request::Article {
            message_id: MessageId::from_borrowed("<a@b>").unwrap(),
        };
        let body = Request::Body {
            message_id: MessageId::from_borrowed("<c@d>").unwrap(),
        };
        let head = Request::Head {
            message_id: MessageId::from_borrowed("<e@f>").unwrap(),
        };
        let stat = Request::Stat {
            message_id: MessageId::from_borrowed("<g@h>").unwrap(),
        };
        let capabilities = Request::Capabilities;
        let date = Request::Date;
        let mode_reader = Request::ModeReader;

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
    }

    #[test]
    fn request_constructors_wrap_and_expose_message_ids() {
        let article = Request::article("a@b").unwrap();
        let body = Request::body("<c@d>").unwrap();
        let head = Request::head("e@f").unwrap();
        let stat = Request::stat("<g@h>").unwrap();
        let capabilities = Request::capabilities();
        let date = Request::date();
        let mode_reader = Request::mode_reader();

        assert_eq!(article.kind(), RequestKind::Article);
        assert_eq!(article.message_id().unwrap().as_str(), "<a@b>");
        assert_eq!(body.kind(), RequestKind::Body);
        assert_eq!(body.message_id().unwrap().as_str(), "<c@d>");
        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.message_id().unwrap().as_str(), "<e@f>");
        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.message_id().unwrap().as_str(), "<g@h>");
        assert_eq!(capabilities.kind(), RequestKind::Capabilities);
        assert!(capabilities.message_id().is_none());
        assert_eq!(date.kind(), RequestKind::Date);
        assert!(date.message_id().is_none());
        assert_eq!(mode_reader.kind(), RequestKind::ModeReader);
        assert!(mode_reader.message_id().is_none());
        assert!(Request::article("<bad id>").is_err());
    }
}
