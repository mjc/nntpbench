//! Zero-copy NNTP article parsing borrowed and adapted from `nntp-proxy`.

use std::fmt;

use super::{InvalidMessageId, MessageId, StatusCode};
use crate::tail_buffer::find_terminator_content_end;

/// Article parsing error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArticleParseError {
    InvalidStatusCode(u16),
    MissingSeparator,
    MissingTerminator,
    InvalidHeader(String),
    UnexpectedBody,
    BufferTooShort,
    InvalidMessageId,
}

impl fmt::Display for ArticleParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStatusCode(code) => write!(f, "invalid status code: {code}"),
            Self::MissingSeparator => {
                write!(f, "missing blank line separator between headers and body")
            }
            Self::MissingTerminator => write!(f, "missing multiline terminator"),
            Self::InvalidHeader(reason) => write!(f, "invalid header: {reason}"),
            Self::UnexpectedBody => write!(f, "response contains unexpected body"),
            Self::BufferTooShort => write!(f, "buffer too short to contain valid response"),
            Self::InvalidMessageId => write!(f, "invalid message-id"),
        }
    }
}

impl std::error::Error for ArticleParseError {}

impl From<InvalidMessageId> for ArticleParseError {
    fn from(_: InvalidMessageId) -> Self {
        Self::InvalidMessageId
    }
}

/// Article number parsed from a response status line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArticleNumber(u64);

impl ArticleNumber {
    /// Return the numeric value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for ArticleNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Validated zero-copy header block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Headers<'a> {
    data: &'a [u8],
}

impl<'a> Headers<'a> {
    /// Parse and validate a header block.
    pub fn parse(data: &'a [u8]) -> Result<Self, ArticleParseError> {
        validate_headers(data)?;
        Ok(Self { data })
    }

    /// Return a header value by case-insensitive name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&'a [u8]> {
        let name_lower = name.to_ascii_lowercase();
        let mut pos = 0;

        while pos < self.data.len() {
            let line_end = find_line_end(self.data, pos).ok()?;
            let line = &self.data[pos..line_end];
            if line.is_empty() {
                pos = line_end + 2;
                continue;
            }
            if line[0] == b' ' || line[0] == b'\t' {
                pos = line_end + 2;
                continue;
            }

            let colon_pos = memchr::memchr(b':', line)?;
            let header_name = &line[..colon_pos];
            if header_name.eq_ignore_ascii_case(name_lower.as_bytes()) {
                let mut value_start = colon_pos + 1;
                while value_start < line.len()
                    && (line[value_start] == b' ' || line[value_start] == b'\t')
                {
                    value_start += 1;
                }
                return Some(&line[value_start..]);
            }

            pos = line_end + 2;
        }

        None
    }

    /// Iterate over parsed headers.
    #[must_use]
    pub const fn iter(&self) -> HeaderIter<'a> {
        HeaderIter {
            data: self.data,
            pos: 0,
        }
    }

    /// Return the raw header bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &'a [u8] {
        self.data
    }
}

impl<'a> IntoIterator for &Headers<'a> {
    type Item = (&'a [u8], &'a [u8]);
    type IntoIter = HeaderIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over header name/value pairs.
#[derive(Debug, Clone, Copy)]
pub struct HeaderIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.data.len() {
            let line_end = find_line_end(self.data, self.pos).ok()?;
            let line = &self.data[self.pos..line_end];
            if line.is_empty() {
                self.pos = line_end + 2;
                continue;
            }
            if line[0] == b' ' || line[0] == b'\t' {
                self.pos = line_end + 2;
                continue;
            }

            let colon_pos = memchr::memchr(b':', line)?;
            let name = &line[..colon_pos];
            let mut value_start = colon_pos + 1;
            while value_start < line.len()
                && (line[value_start] == b' ' || line[value_start] == b'\t')
            {
                value_start += 1;
            }

            self.pos = line_end + 2;
            return Some((name, &line[value_start..]));
        }

        None
    }
}

/// Parsed ARTICLE/HEAD/BODY/STAT response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Article<'a> {
    pub message_id: MessageId<'a>,
    pub article_number: Option<ArticleNumber>,
    pub headers: Option<Headers<'a>>,
    pub body: Option<&'a [u8]>,
}

impl<'a> TryFrom<&'a [u8]> for Article<'a> {
    type Error = ArticleParseError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl<'a> Article<'a> {
    /// Parse a full NNTP ARTICLE/HEAD/BODY/STAT response frame.
    pub fn parse(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let status_code = parse_status_code(buf)?;
        match status_code {
            220 => Self::parse_article(buf),
            221 => Self::parse_head(buf),
            222 => Self::parse_body(buf),
            223 => Self::parse_stat(buf),
            _ => Err(ArticleParseError::InvalidStatusCode(status_code)),
        }
    }

    fn parse_article(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end = find_line_end(buf, 0)?;
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        let content_start = first_line_end + 2;
        let separator_pos = find_blank_line(buf, content_start)?;
        let headers = Some(Headers::parse(&buf[content_start..separator_pos + 2])?);
        let body_start = separator_pos + 4;
        let body_end = find_terminator_content_end(buf, body_start)
            .ok_or(ArticleParseError::MissingTerminator)?;

        Ok(Self {
            message_id,
            article_number,
            headers,
            body: Some(&buf[body_start..body_end]),
        })
    }

    fn parse_head(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end = find_line_end(buf, 0)?;
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        let content_start = first_line_end + 2;
        if find_blank_line(buf, content_start).is_ok() {
            return Err(ArticleParseError::UnexpectedBody);
        }
        let headers_end = find_terminator_content_end(buf, content_start)
            .ok_or(ArticleParseError::MissingTerminator)?;

        Ok(Self {
            message_id,
            article_number,
            headers: Some(Headers::parse(&buf[content_start..headers_end])?),
            body: None,
        })
    }

    fn parse_body(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end = find_line_end(buf, 0)?;
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        let body_start = first_line_end + 2;
        let body_end = find_terminator_content_end(buf, body_start)
            .ok_or(ArticleParseError::MissingTerminator)?;

        Ok(Self {
            message_id,
            article_number,
            headers: None,
            body: Some(&buf[body_start..body_end]),
        })
    }

    fn parse_stat(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end = find_line_end(buf, 0)?;
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        let content_start = first_line_end + 2;
        if content_start < buf.len() && !buf[content_start..].starts_with(b".\r\n") {
            return Err(ArticleParseError::UnexpectedBody);
        }

        Ok(Self {
            message_id,
            article_number,
            headers: None,
            body: None,
        })
    }
}

fn parse_status_code(buf: &[u8]) -> Result<u16, ArticleParseError> {
    StatusCode::parse(buf)
        .map(StatusCode::as_u16)
        .ok_or(ArticleParseError::InvalidStatusCode(0))
}

fn parse_first_line(
    line: &[u8],
) -> Result<(MessageId<'_>, Option<ArticleNumber>), ArticleParseError> {
    let first_space = memchr::memchr(b' ', line).ok_or(ArticleParseError::InvalidMessageId)?;
    let second_space =
        memchr::memchr(b' ', &line[first_space + 1..]).map(|pos| first_space + 1 + pos);

    let (msgid_start_search, article_number) = match second_space {
        Some(second_space) => {
            let number = std::str::from_utf8(&line[first_space + 1..second_space])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .map(ArticleNumber::from);
            (second_space, number)
        }
        None => (first_space, None),
    };

    let msgid_start = memchr::memchr(b'<', &line[msgid_start_search..])
        .map(|pos| msgid_start_search + pos)
        .ok_or(ArticleParseError::InvalidMessageId)?;
    let msgid_end = memchr::memchr(b'>', &line[msgid_start..])
        .map(|pos| msgid_start + pos + 1)
        .ok_or(ArticleParseError::InvalidMessageId)?;
    let msgid = std::str::from_utf8(&line[msgid_start..msgid_end])
        .map_err(|_| ArticleParseError::InvalidMessageId)?;

    Ok((MessageId::from_borrowed(msgid)?, article_number))
}

fn find_line_end(buf: &[u8], start: usize) -> Result<usize, ArticleParseError> {
    for index in start..buf.len() {
        if buf[index] == b'\r' && index + 1 < buf.len() && buf[index + 1] == b'\n' {
            return Ok(index);
        }
    }
    Err(ArticleParseError::BufferTooShort)
}

fn find_blank_line(buf: &[u8], start: usize) -> Result<usize, ArticleParseError> {
    memchr::memmem::find(&buf[start..], b"\r\n\r\n")
        .map(|pos| start + pos)
        .ok_or(ArticleParseError::MissingSeparator)
}

fn validate_headers(data: &[u8]) -> Result<(), ArticleParseError> {
    let mut pos = 0;
    while pos < data.len() {
        let line_end = find_line_end(data, pos)?;
        let line = &data[pos..line_end];

        if line.is_empty() {
            pos = line_end + 2;
            continue;
        }

        if line[0] == b' ' || line[0] == b'\t' {
            if pos == 0 {
                return Err(ArticleParseError::InvalidHeader(
                    "header cannot start with folding whitespace".to_string(),
                ));
            }
            pos = line_end + 2;
            continue;
        }

        let colon_pos = memchr::memchr(b':', line).ok_or_else(|| {
            ArticleParseError::InvalidHeader(format!(
                "header missing colon: {}",
                String::from_utf8_lossy(line)
            ))
        })?;
        let name = &line[..colon_pos];
        if name.is_empty() {
            return Err(ArticleParseError::InvalidHeader(
                "empty header name".to_string(),
            ));
        }
        for &byte in name {
            if byte == b' ' || byte == b'\t' || !(33..=126).contains(&byte) {
                return Err(ArticleParseError::InvalidHeader(format!(
                    "invalid character in header name: {}",
                    String::from_utf8_lossy(name)
                )));
            }
        }

        pos = line_end + 2;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_article_response_220() {
        let buf =
            b"220 100 <test@example.com> article\r\nSubject: Test\r\nFrom: user@example.com\r\n\r\nBody content\r\n.\r\n";

        let article = Article::parse(buf).unwrap();
        assert_eq!(article.message_id.as_str(), "<test@example.com>");
        assert_eq!(article.article_number, Some(ArticleNumber(100)));
        assert_eq!(article.headers.unwrap().get("Subject"), Some(&b"Test"[..]));
        assert_eq!(article.body, Some(&b"Body content\r\n"[..]));
    }

    #[test]
    fn parse_head_response_221() {
        let buf =
            b"221 100 <test@example.com> headers\r\nSubject: Test\r\nFrom: user@example.com\r\n.\r\n";

        let article = Article::parse(buf).unwrap();
        assert_eq!(
            article.headers.unwrap().get("From"),
            Some(&b"user@example.com"[..])
        );
        assert_eq!(article.body, None);
    }

    #[test]
    fn parse_body_response_222() {
        let buf = b"222 100 <test@example.com> body\r\nBody content\r\n.\r\n";

        let article = Article::parse(buf).unwrap();
        assert_eq!(article.headers, None);
        assert_eq!(article.body, Some(&b"Body content\r\n"[..]));
    }

    #[test]
    fn parse_stat_response_223() {
        let buf = b"223 100 <test@example.com>\r\n.\r\n";
        let article = Article::parse(buf).unwrap();
        assert_eq!(article.article_number, Some(ArticleNumber(100)));
        assert!(article.headers.is_none());
        assert!(article.body.is_none());
    }

    #[test]
    fn headers_iterate_zero_copy() {
        let data = b"Subject: Test\r\nFrom: user@example.com\r\n";
        let headers = Headers::parse(data).unwrap();
        let items: Vec<_> = headers.iter().collect();
        assert_eq!(items[0], (&b"Subject"[..], &b"Test"[..]));
        assert_eq!(items[1], (&b"From"[..], &b"user@example.com"[..]));
    }

    #[test]
    fn invalid_header_is_rejected() {
        let data = b"Invalid Header\r\n";
        assert!(matches!(
            Headers::parse(data),
            Err(ArticleParseError::InvalidHeader(_))
        ));
    }
}
