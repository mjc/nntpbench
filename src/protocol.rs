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

    Ok(())
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
                || matches!(byte, b'<' | b'>' | b'*' | b'?' | b'[' | b']' | b'\\')
        })
    {
        return Err(InvalidGroupName);
    }

    Ok(())
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
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
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
    if hour > 23 || minute > 59 || second > 60 {
        return Err(InvalidNntpTime);
    }

    Ok(())
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
    if value.is_empty() || value.bytes().any(|byte| !(0x21..=0x7e).contains(&byte)) {
        return Err(InvalidWildmat);
    }

    for pattern in value.split(',') {
        if pattern.is_empty() || pattern == "!" {
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
    Article {
        message_id: MessageId<'a>,
    },
    Body {
        message_id: MessageId<'a>,
    },
    Head {
        message_id: MessageId<'a>,
    },
    Stat {
        message_id: MessageId<'a>,
    },
    Group {
        group: GroupName<'a>,
    },
    ListGroup {
        group: GroupName<'a>,
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
            Self::List => RequestKind::List,
            Self::Help => RequestKind::Help,
            Self::Capabilities => RequestKind::Capabilities,
            Self::Date => RequestKind::Date,
            Self::ModeReader => RequestKind::ModeReader,
            Self::Quit => RequestKind::Quit,
        }
    }

    /// Serialize the request onto the NNTP wire.
    pub fn write_wire_to(&self, output: &mut Vec<u8>) {
        match self {
            Self::Article { message_id } => write_request_wire(output, b"ARTICLE ", message_id),
            Self::Body { message_id } => write_request_wire(output, b"BODY ", message_id),
            Self::Head { message_id } => write_request_wire(output, b"HEAD ", message_id),
            Self::Stat { message_id } => write_request_wire(output, b"STAT ", message_id),
            Self::Group { group } => write_one_arg_request_wire(output, b"GROUP ", group.as_str()),
            Self::ListGroup { group } => {
                write_one_arg_request_wire(output, b"LISTGROUP ", group.as_str())
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
            Self::Article { message_id }
            | Self::Body { message_id }
            | Self::Head { message_id }
            | Self::Stat { message_id } => Some(message_id),
            Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. } => None,
            Self::List
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
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
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
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
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
            Self::Group { group } | Self::ListGroup { group } => Some(group),
            Self::Article { .. }
            | Self::Body { .. }
            | Self::Head { .. }
            | Self::Stat { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::NewGroups { .. }
            | Self::NewNews { .. }
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
            | Self::Group { .. }
            | Self::ListGroup { .. }
            | Self::Last
            | Self::Next
            | Self::Over { .. }
            | Self::Xover { .. }
            | Self::Hdr { .. }
            | Self::Xhdr { .. }
            | Self::List
            | Self::Help
            | Self::Capabilities
            | Self::Date
            | Self::ModeReader
            | Self::Quit => None,
        }
    }

    /// Borrow the validated NEWNEWS wildmat carried by this request, if any.
    #[must_use]
    pub const fn wildmat(&self) -> Option<&Wildmat<'a>> {
        match self {
            Self::NewNews { wildmat, .. } => Some(wildmat),
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

    /// Build a GROUP request.
    pub fn group(group: impl AsRef<str>) -> Result<Self, InvalidGroupName> {
        Ok(Self::Group {
            group: GroupName::from_owned(group)?,
        })
    }

    /// Build a LISTGROUP request.
    pub fn listgroup(group: impl AsRef<str>) -> Result<Self, InvalidGroupName> {
        Ok(Self::ListGroup {
            group: GroupName::from_owned(group)?,
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

    /// Build a LIST request.
    #[must_use]
    pub const fn list() -> Self {
        Self::List
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

fn write_one_arg_request_wire(output: &mut Vec<u8>, verb: &[u8], arg: &str) {
    output.extend_from_slice(verb);
    output.extend_from_slice(arg.as_bytes());
    output.extend_from_slice(b"\r\n");
}

fn write_two_arg_request_wire(output: &mut Vec<u8>, verb: &[u8], left: &str, right: &str) {
    output.extend_from_slice(verb);
    output.extend_from_slice(left.as_bytes());
    output.push(b' ');
    output.extend_from_slice(right.as_bytes());
    output.extend_from_slice(b"\r\n");
}

fn write_datetime_request_wire(
    output: &mut Vec<u8>,
    verb: &[u8],
    date: &str,
    time: &str,
    gmt: bool,
) {
    output.extend_from_slice(verb);
    output.extend_from_slice(date.as_bytes());
    output.push(b' ');
    output.extend_from_slice(time.as_bytes());
    if gmt {
        output.extend_from_slice(b" GMT");
    }
    output.extend_from_slice(b"\r\n");
}

fn write_newnews_request_wire(
    output: &mut Vec<u8>,
    wildmat: &str,
    date: &str,
    time: &str,
    gmt: bool,
) {
    output.extend_from_slice(b"NEWNEWS ");
    output.extend_from_slice(wildmat.as_bytes());
    output.push(b' ');
    output.extend_from_slice(date.as_bytes());
    output.push(b' ');
    output.extend_from_slice(time.as_bytes());
    if gmt {
        output.extend_from_slice(b" GMT");
    }
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
    fn status_code_categories_and_parsing_cover_rfc_examples() {
        for code in [100, 199] {
            let status = StatusCode(code);
            assert!(status.is_informational(), "{code}");
            assert!(!status.is_success(), "{code}");
            assert!(!status.is_error(), "{code}");
        }

        for code in [200, 205, 220, 281, 381, 399] {
            let status = StatusCode(code);
            assert!(status.is_success(), "{code}");
            assert!(!status.is_error(), "{code}");
        }

        for code in [400, 430, 500, 599] {
            let status = StatusCode(code);
            assert!(status.is_error(), "{code}");
            assert!(!status.is_success(), "{code}");
        }

        for (input, expected) in [
            (b"200 Service ready\r\n".as_slice(), Some(StatusCode(200))),
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
        assert!(MessageId::from_borrowed("<a>").is_ok());
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
            ArticleSelector::from_borrowed("1-10").unwrap().as_str(),
            "1-10"
        );
        assert_eq!(
            ArticleSelector::from_borrowed("<a@b>").unwrap().as_str(),
            "<a@b>"
        );
        assert!(HeaderName::from_borrowed("Bad Header").is_err());
        assert!(HeaderName::from_borrowed("Subject:").is_err());
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
            NntpTime::from_borrowed("235960").unwrap().as_str(),
            "235960"
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
    fn request_line_parses_current_command_set() {
        assert_eq!(
            RequestLine::parse(b"ARTICLE <a@b>").kind(),
            RequestKind::Article
        );
        assert_eq!(RequestLine::parse(b"ARTICLE").kind(), RequestKind::Article);
        assert_eq!(RequestLine::parse(b"body 123").kind(), RequestKind::Body);
        assert_eq!(RequestLine::parse(b"HEAD <a@b>").kind(), RequestKind::Head);
        assert_eq!(RequestLine::parse(b"HELP").kind(), RequestKind::Help);
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
    fn request_line_classifies_rfc_command_matrix_case_insensitively() {
        for (line, expected) in [
            (b"ARTICLE <a@b>".as_slice(), RequestKind::Article),
            (b"BODY <a@b>".as_slice(), RequestKind::Body),
            (b"HEAD <a@b>".as_slice(), RequestKind::Head),
            (b"STAT <a@b>".as_slice(), RequestKind::Stat),
            (b"ARTICLE 12345".as_slice(), RequestKind::Article),
            (b"GROUP alt.test".as_slice(), RequestKind::Group),
            (b"LISTGROUP alt.test".as_slice(), RequestKind::ListGroup),
            (b"LAST".as_slice(), RequestKind::Last),
            (b"NEXT".as_slice(), RequestKind::Next),
            (b"LIST".as_slice(), RequestKind::List),
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
            (b"AUTHINFO USER test".as_slice(), RequestKind::AuthInfo),
            (b"STARTTLS".as_slice(), RequestKind::StartTls),
            (b"article <a@b>".as_slice(), RequestKind::Article),
            (b"authinfo user test".as_slice(), RequestKind::AuthInfo),
            (b"quit".as_slice(), RequestKind::Quit),
            (b"XYZZY".as_slice(), RequestKind::Unknown),
        ] {
            assert_eq!(RequestLine::parse(line).kind(), expected, "{line:?}");
        }
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
        assert!(!RequestKind::Group.expects_multiline_response(StatusCode(211)));
        assert!(RequestKind::ListGroup.expects_multiline_response(StatusCode(211)));
        assert!(!RequestKind::Last.expects_multiline_response(StatusCode(223)));
        assert!(!RequestKind::Next.expects_multiline_response(StatusCode(223)));
        assert!(RequestKind::List.expects_multiline_response(StatusCode(215)));
        assert!(RequestKind::Over.expects_multiline_response(StatusCode(224)));
        assert!(RequestKind::Xhdr.expects_multiline_response(StatusCode(225)));
        assert!(RequestKind::NewGroups.expects_multiline_response(StatusCode(231)));
        assert!(RequestKind::NewNews.expects_multiline_response(StatusCode(230)));
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
        let group = Request::Group {
            group: GroupName::from_borrowed("alt.test").unwrap(),
        };
        let listgroup = Request::ListGroup {
            group: GroupName::from_borrowed("alt.test").unwrap(),
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
        let list = Request::List;
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
        list.write_wire_to(&mut wire);
        assert_eq!(list.kind(), RequestKind::List);
        assert_eq!(wire, b"LIST\r\n");

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
        let body = Request::body("<c@d>").unwrap();
        let head = Request::head("e@f").unwrap();
        let stat = Request::stat("<g@h>").unwrap();
        let group = Request::group("alt.test").unwrap();
        let listgroup = Request::listgroup("alt.test").unwrap();
        let last = Request::last();
        let next = Request::next();
        let over = Request::over("1-10").unwrap();
        let xover = Request::xover("<g@h>").unwrap();
        let hdr = Request::hdr("Subject", "1-10").unwrap();
        let xhdr = Request::xhdr("Message-ID", "<g@h>").unwrap();
        let newgroups = Request::newgroups("20260101", "000000", true).unwrap();
        let newnews =
            Request::newnews("comp.lang.*,alt.test", "20260101", "000000", false).unwrap();
        let list = Request::list();
        let help = Request::help();
        let capabilities = Request::capabilities();
        let date = Request::date();
        let mode_reader = Request::mode_reader();
        let quit = Request::quit();

        assert_eq!(article.kind(), RequestKind::Article);
        assert_eq!(article.message_id().unwrap().as_str(), "<a@b>");
        assert_eq!(body.kind(), RequestKind::Body);
        assert_eq!(body.message_id().unwrap().as_str(), "<c@d>");
        assert_eq!(head.kind(), RequestKind::Head);
        assert_eq!(head.message_id().unwrap().as_str(), "<e@f>");
        assert_eq!(stat.kind(), RequestKind::Stat);
        assert_eq!(stat.message_id().unwrap().as_str(), "<g@h>");
        assert_eq!(group.kind(), RequestKind::Group);
        assert_eq!(group.group_name().map(GroupName::as_str), Some("alt.test"));
        assert_eq!(listgroup.kind(), RequestKind::ListGroup);
        assert_eq!(
            listgroup.group_name().map(GroupName::as_str),
            Some("alt.test")
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
        assert_eq!(list.kind(), RequestKind::List);
        assert!(list.message_id().is_none());
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
        assert!(Request::group("").is_err());
        assert!(Request::listgroup("<a@b>").is_err());
        assert!(Request::over("1 2").is_err());
        assert!(Request::xover("").is_err());
        assert!(Request::hdr("Bad Header", "1").is_err());
        assert!(Request::xhdr("Subject", "1 2").is_err());
        assert!(Request::newgroups("20261301", "000000", true).is_err());
        assert!(Request::newnews("", "20260101", "000000", false).is_err());
    }

    #[test]
    fn request_wire_uses_one_crlf_terminator() {
        let requests = [
            Request::article("a@b").unwrap(),
            Request::body("c@d").unwrap(),
            Request::head("e@f").unwrap(),
            Request::stat("g@h").unwrap(),
            Request::group("alt.test").unwrap(),
            Request::listgroup("alt.test").unwrap(),
            Request::last(),
            Request::next(),
            Request::over("1-10").unwrap(),
            Request::xover("<i@j>").unwrap(),
            Request::hdr("Subject", "1-10").unwrap(),
            Request::xhdr("Message-ID", "<i@j>").unwrap(),
            Request::newgroups("20260101", "000000", true).unwrap(),
            Request::newnews("comp.lang.*", "20260101", "000000", false).unwrap(),
            Request::list(),
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
    }
}
