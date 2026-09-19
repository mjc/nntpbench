//! Zero-copy NNTP article parsing borrowed and adapted from `nntp-proxy`.

use std::{borrow::Cow, fmt};

use bytes::Bytes;

use super::{
    InvalidMessageId, MAX_ARTICLE_NUMBER, MessageId, RequestKind, StatusCode,
    validate_optional_trailing_comment,
};
use crate::terminator::{
    DOT_TERMINATOR, find_terminator_content_end, strict_crlf_line_content_end_from,
};

/// Article parsing error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArticleParseError {
    InvalidStatusCode(u16),
    InvalidStatusPrefix,
    MissingSeparator,
    MissingTerminator,
    InvalidHeader(InvalidHeaderReason),
    InvalidBody,
    UnexpectedBody,
    BufferTooShort,
    InvalidArticleNumber,
    InvalidMessageId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidHeaderReason {
    LeadingFold,
    EmptyFold,
    MissingHeader,
    MissingColon,
    MissingSpaceAfterColon,
    EmptyName,
    InvalidName,
    InvalidContent,
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::string::string_regex;

    fn header_name_strategy() -> BoxedStrategy<String> {
        string_regex("[A-Za-z0-9-]{1,12}").unwrap().boxed()
    }

    fn header_value_strategy() -> BoxedStrategy<String> {
        string_regex("[ -~]{0,20}").unwrap().boxed()
    }

    fn folded_header_value_strategy() -> BoxedStrategy<String> {
        string_regex("[ \t]{0,4}[!-~][ -~]{0,16}").unwrap().boxed()
    }

    fn message_id_strategy() -> BoxedStrategy<String> {
        (
            string_regex("[A-Za-z0-9][A-Za-z0-9_-]{0,7}").unwrap(),
            string_regex("[A-Za-z0-9][A-Za-z0-9_-]{0,7}").unwrap(),
        )
            .prop_map(|(local, domain)| format!("<{local}@{domain}.test>"))
            .boxed()
    }

    fn body_line_strategy() -> BoxedStrategy<String> {
        string_regex("[A-Za-z0-9 !?_-]{1,24}").unwrap().boxed()
    }

    fn first_line_suffix_strategy() -> BoxedStrategy<String> {
        prop_oneof![
            Just(String::new()),
            string_regex(" [A-Za-z0-9 ._-]{1,20}").unwrap(),
        ]
        .boxed()
    }

    fn invalid_article_number_token_strategy() -> BoxedStrategy<String> {
        string_regex("[A-Za-z][A-Za-z0-9_-]{0,12}").unwrap().boxed()
    }

    fn overflowing_article_number_token_strategy() -> BoxedStrategy<String> {
        string_regex("[0-9]{21,32}")
            .unwrap()
            .prop_filter("must overflow u64", |value| value.parse::<u64>().is_err())
            .boxed()
    }

    fn header_pairs_strategy() -> BoxedStrategy<Vec<(String, String)>> {
        vec((header_name_strategy(), header_value_strategy()), 1..=6).boxed()
    }

    fn unsupported_status_strategy() -> BoxedStrategy<u16> {
        (100_u16..600_u16)
            .prop_filter("exclude article-family statuses", |code| {
                !matches!(*code, 220..=223)
            })
            .boxed()
    }

    fn invalid_status_prefix_buffer_strategy() -> BoxedStrategy<Vec<u8>> {
        vec(any::<u8>(), 0..=12)
            .prop_filter("exclude valid three-digit status prefixes", |buf| {
                buf.len() < 3 || !buf[..3].iter().all(|byte| byte.is_ascii_digit())
            })
            .boxed()
    }

    fn dangerous_wire_bytes() -> impl Strategy<Value = u8> {
        prop_oneof![
            Just(b'\r'),
            Just(b'\n'),
            Just(b'.'),
            Just(b' '),
            b'0'..=b'9',
            b'a'..=b'z',
        ]
    }

    fn body_content_bytes() -> impl Strategy<Value = u8> {
        prop_oneof![Just(b'.'), Just(b' '), b'0'..=b'9', b'a'..=b'z']
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        #[test]
        fn headers_parse_iter_and_lookup_stay_consistent(
            pairs in header_pairs_strategy(),
        ) {
            let mut data = Vec::new();
            for (name, value) in &pairs {
                data.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
            }

            let headers = Headers::parse(&data).unwrap();
            let iterated: Vec<_> = headers.iter().collect();
            prop_assert_eq!(iterated.len(), pairs.len());

            for ((expected_name, expected_value), (actual_name, actual_value)) in pairs.iter().zip(iterated.iter()) {
                let trimmed_value = expected_value.trim_start_matches([' ', '\t']);
                prop_assert_eq!(*actual_name, expected_name.as_bytes());
                prop_assert_eq!(*actual_value, trimmed_value.as_bytes());
            }

            for (name, _) in &pairs {
                let trimmed_value = pairs
                    .iter()
                    .find(|(expected_name, _)| expected_name.eq_ignore_ascii_case(name))
                    .map(|(_, expected_value)| expected_value.trim_start_matches([' ', '\t']))
                    .unwrap();
                prop_assert_eq!(headers.get(name), Some(trimmed_value.as_bytes()));
                prop_assert_eq!(
                    headers.get(&name.to_ascii_lowercase()),
                    Some(trimmed_value.as_bytes())
                );
                prop_assert_eq!(
                    headers.get(&name.to_ascii_uppercase()),
                    Some(trimmed_value.as_bytes())
                );
            }
        }

        #[test]
        fn headers_parse_remains_zero_copy_for_iter_and_lookup(
            pairs in header_pairs_strategy(),
        ) {
            let mut data = Vec::new();
            for (name, value) in &pairs {
                data.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
            }

            let headers = Headers::parse(&data).unwrap();
            let start = data.as_ptr() as usize;
            let end = start + data.len();
            let iterated: Vec<_> = headers.iter().collect();

            for (name, value) in &iterated {
                let name_ptr = name.as_ptr() as usize;
                let value_ptr = value.as_ptr() as usize;
                prop_assert!((start..end).contains(&name_ptr));
                prop_assert!((start..end).contains(&value_ptr));
            }

            for (query_name, _) in &pairs {
                let expected = iterated
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(query_name.as_bytes()))
                    .map(|(_, value)| *value);
                let actual = headers.get(query_name);
                prop_assert_eq!(actual, expected);
                if let Some(value) = actual {
                    prop_assert!((start..end).contains(&(value.as_ptr() as usize)));
                }
            }
        }

        #[test]
        fn generated_article_family_frames_parse_consistently(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=5),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));

            let article_frame = format!(
                "220 {article_number} {message_id}\r\n{header_block}\r\n{body}.\r\n"
            );
            let article = Article::parse(article_frame.as_bytes()).unwrap();
            prop_assert_eq!(article.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(article.article_number, Some(ArticleNumber::from(article_number as u64)));
            prop_assert_eq!(article.body.as_deref(), Some(body.as_bytes()));
            let parsed_headers = article.headers.unwrap();
            for (name, _) in &headers {
                let expected = headers
                    .iter()
                    .find(|(expected_name, _)| expected_name.eq_ignore_ascii_case(name))
                    .map(|(_, expected_value)| {
                        expected_value
                            .trim_start_matches([' ', '\t'])
                            .as_bytes()
                    });
                prop_assert_eq!(parsed_headers.get(name), expected);
            }

            let head_frame =
                format!("221 {article_number} {message_id}\r\n{header_block}.\r\n");
            let head = Article::parse(head_frame.as_bytes()).unwrap();
            prop_assert_eq!(head.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(head.article_number, Some(ArticleNumber::from(article_number as u64)));
            prop_assert!(head.body.is_none());
            let head_headers = head.headers.unwrap();
            for (name, _) in &headers {
                let expected = headers
                    .iter()
                    .find(|(expected_name, _)| expected_name.eq_ignore_ascii_case(name))
                    .map(|(_, expected_value)| {
                        expected_value
                            .trim_start_matches([' ', '\t'])
                            .as_bytes()
                    });
                prop_assert_eq!(head_headers.get(name), expected);
            }

            let body_frame = format!("222 {article_number} {message_id}\r\n{body}.\r\n");
            let parsed_body = Article::parse(body_frame.as_bytes()).unwrap();
            prop_assert_eq!(parsed_body.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(
                parsed_body.article_number,
                Some(ArticleNumber::from(article_number as u64))
            );
            prop_assert!(parsed_body.headers.is_none());
            prop_assert_eq!(parsed_body.body.as_deref(), Some(body.as_bytes()));

            let stat_frame = format!("223 {article_number} {message_id}\r\n");
            let stat = Article::parse(stat_frame.as_bytes()).unwrap();
            prop_assert_eq!(stat.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(stat.article_number, Some(ArticleNumber::from(article_number as u64)));
            prop_assert!(stat.headers.is_none());
            prop_assert!(stat.body.is_none());
        }

        #[test]
        fn generated_response_first_lines_preserve_message_id_and_article_number(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=3),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));
            let first_line = format!("{article_number} {message_id}");
            let expected_number = Some(ArticleNumber::from(article_number as u64));
            let article_frame = format!("220 {first_line}\r\n{header_block}\r\n{body}.\r\n");
            let head_frame = format!("221 {first_line}\r\n{header_block}.\r\n");
            let body_frame = format!("222 {first_line}\r\n{body}.\r\n");
            let stat_frame = format!("223 {first_line}\r\n");

            let article = Article::parse(article_frame.as_bytes()).unwrap();
            prop_assert_eq!(article.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(article.article_number, expected_number);

            let head = Article::parse(head_frame.as_bytes()).unwrap();
            prop_assert_eq!(head.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(head.article_number, expected_number);

            let parsed_body = Article::parse(body_frame.as_bytes()).unwrap();
            prop_assert_eq!(parsed_body.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(parsed_body.article_number, expected_number);

            let stat = Article::parse(stat_frame.as_bytes()).unwrap();
            prop_assert_eq!(stat.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(stat.article_number, expected_number);
        }

        #[test]
        fn generated_empty_content_shapes_follow_article_family_rules(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
        ) {
            let first_line = format!("{article_number} {message_id}");
            let expected_number = Some(ArticleNumber::from(article_number as u64));

            let article_frame = format!("220 {first_line}\r\n\r\n\r\n.\r\n");
            prop_assert_eq!(
                Article::parse(article_frame.as_bytes()).unwrap_err(),
                ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingHeader)
            );

            let head_frame = format!("221 {first_line}\r\n.\r\n");
            prop_assert_eq!(
                Article::parse(head_frame.as_bytes()).unwrap_err(),
                ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingHeader)
            );

            let body_frame = format!("222 {first_line}\r\n.\r\n");
            let parsed_body = Article::parse(body_frame.as_bytes()).unwrap();
            prop_assert_eq!(parsed_body.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(parsed_body.article_number, expected_number);
            prop_assert!(parsed_body.headers.is_none());
            prop_assert_eq!(parsed_body.body.as_deref(), Some(&b""[..]));
        }

        #[test]
        fn generated_numbered_response_first_lines_ignore_trailing_text(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            suffix in first_line_suffix_strategy(),
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=3),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));
            let first_line = format!("{article_number} {message_id}{suffix}");
            let article_frame = format!("220 {first_line}\r\n{header_block}\r\n{body}.\r\n");
            let head_frame = format!("221 {first_line}\r\n{header_block}.\r\n");
            let body_frame = format!("222 {first_line}\r\n{body}.\r\n");
            let stat_frame = format!("223 {first_line}\r\n");

            let article = Article::parse(article_frame.as_bytes()).unwrap();
            prop_assert_eq!(article.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(article.article_number, Some(ArticleNumber::from(article_number as u64)));

            let head = Article::parse(head_frame.as_bytes()).unwrap();
            prop_assert_eq!(head.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(head.article_number, Some(ArticleNumber::from(article_number as u64)));

            let parsed_body = Article::parse(body_frame.as_bytes()).unwrap();
            prop_assert_eq!(parsed_body.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(parsed_body.article_number, Some(ArticleNumber::from(article_number as u64)));

            let stat = Article::parse(stat_frame.as_bytes()).unwrap();
            prop_assert_eq!(stat.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(stat.article_number, Some(ArticleNumber::from(article_number as u64)));
        }

        #[test]
        fn generated_no_number_response_first_lines_reject_trailing_text(
            message_id in message_id_strategy(),
            suffix in string_regex(" [A-Za-z0-9._-]{1,20}").unwrap(),
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=3),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));
            let first_line = format!("{message_id}{suffix}");

            for frame in [
                format!("220 {first_line}\r\n{header_block}\r\n{body}.\r\n"),
                format!("221 {first_line}\r\n{header_block}.\r\n"),
                format!("222 {first_line}\r\n{body}.\r\n"),
                format!("223 {first_line}\r\n"),
            ] {
                prop_assert_eq!(
                    Article::parse(frame.as_bytes()).unwrap_err(),
                    ArticleParseError::InvalidArticleNumber
                );
            }
        }

        #[test]
        fn stat_accepts_minimal_and_rejects_dot_terminated_body(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
        ) {
            let first_line = format!("{article_number} {message_id}");
            let minimal = format!("223 {first_line}\r\n");
            let dot_terminated = format!("223 {first_line}\r\n.\r\n");

            prop_assert!(Article::parse(minimal.as_bytes()).is_ok());
            prop_assert_eq!(
                Article::parse(dot_terminated.as_bytes()).unwrap_err(),
                ArticleParseError::UnexpectedBody
            );
        }

        #[test]
        fn generated_response_first_lines_reject_overflowing_article_numbers(
            message_id in message_id_strategy(),
            overflowing_number in overflowing_article_number_token_strategy(),
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=3),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));
            let first_line = format!("{overflowing_number} {message_id}");
            let article_frame = format!("220 {first_line}\r\n{header_block}\r\n{body}.\r\n");
            let head_frame = format!("221 {first_line}\r\n{header_block}.\r\n");
            let body_frame = format!("222 {first_line}\r\n{body}.\r\n");
            let stat_frame = format!("223 {first_line}\r\n");

            for frame in [article_frame, head_frame, body_frame, stat_frame] {
                prop_assert_eq!(
                    Article::parse(frame.as_bytes()).unwrap_err(),
                    ArticleParseError::InvalidArticleNumber
                );
            }
        }

        #[test]
        fn generated_response_first_lines_reject_article_numbers_over_rfc_maximum(
            message_id in message_id_strategy(),
            article_number in (MAX_ARTICLE_NUMBER + 1)..=(MAX_ARTICLE_NUMBER + 1_000),
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=3),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));
            let first_line = format!("{article_number} {message_id}");
            let article_frame = format!("220 {first_line}\r\n{header_block}\r\n{body}.\r\n");
            let head_frame = format!("221 {first_line}\r\n{header_block}.\r\n");
            let body_frame = format!("222 {first_line}\r\n{body}.\r\n");
            let stat_frame = format!("223 {first_line}\r\n");

            for frame in [article_frame, head_frame, body_frame, stat_frame] {
                prop_assert_eq!(
                    Article::parse(frame.as_bytes()).unwrap_err(),
                    ArticleParseError::InvalidArticleNumber
                );
            }
        }

        #[test]
        fn generated_invalid_response_shapes_fail_with_expected_kinds(
            invalid_message_id in prop_oneof![
                Just("bad-id".to_string()),
                Just("<bad id>".to_string()),
                Just("<>".to_string()),
            ],
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=3),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));

            for frame in [
                format!("220 1 {invalid_message_id}\r\n{header_block}\r\n{body}.\r\n"),
                format!("221 1 {invalid_message_id}\r\n{header_block}.\r\n"),
                format!("222 1 {invalid_message_id}\r\n{body}.\r\n"),
                format!("223 1 {invalid_message_id}\r\n.\r\n"),
            ] {
                prop_assert_eq!(Article::parse(frame.as_bytes()).unwrap_err(), ArticleParseError::InvalidMessageId);
            }

            let valid_message_id = "<valid@test>";
            prop_assert_eq!(
                Article::parse(format!("220 1 {valid_message_id}\r\n{header_block}{body}.\r\n").as_bytes()).unwrap_err(),
                ArticleParseError::MissingSeparator
            );
            prop_assert_eq!(
                Article::parse(format!("220 1 {valid_message_id}\r\n{header_block}\r\n{body}").as_bytes()).unwrap_err(),
                ArticleParseError::MissingTerminator
            );
            prop_assert_eq!(
                Article::parse(format!("221 1 {valid_message_id}\r\n{header_block}\r\n{body}.\r\n").as_bytes()).unwrap_err(),
                ArticleParseError::UnexpectedBody
            );
            prop_assert_eq!(
                Article::parse(format!("223 1 {valid_message_id}\r\nnot-empty\r\n").as_bytes()).unwrap_err(),
                ArticleParseError::UnexpectedBody
            );
        }

        #[test]
        fn article_try_from_matches_parse_for_generated_article_family_frames(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=3),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));

            for frame in [
                format!("220 {article_number} {message_id}\r\n{header_block}\r\n{body}.\r\n"),
                format!("221 {article_number} {message_id}\r\n{header_block}.\r\n"),
                format!("222 {article_number} {message_id}\r\n{body}.\r\n"),
                format!("223 {article_number} {message_id}\r\n"),
            ] {
                prop_assert_eq!(
                    Article::try_from(frame.as_bytes()),
                    Article::parse(frame.as_bytes())
                );
            }
        }

        #[test]
        fn article_try_from_matches_parse_for_generated_invalid_entrypoints(
            invalid_prefix in invalid_status_prefix_buffer_strategy(),
            unsupported_status in unsupported_status_strategy(),
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
        ) {
            prop_assert_eq!(
                Article::try_from(invalid_prefix.as_slice()),
                Article::parse(invalid_prefix.as_slice())
            );

            let unsupported_frame = format!("{unsupported_status} {article_number} {message_id}\r\n.\r\n");
            prop_assert_eq!(
                Article::try_from(unsupported_frame.as_bytes()),
                Article::parse(unsupported_frame.as_bytes())
            );
        }

        #[test]
        fn parse_first_line_extracts_article_numbers_and_ignores_trailing_text(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            suffix in first_line_suffix_strategy(),
        ) {
            let line = format!("{status} {article_number} {message_id}{suffix}");

            let parsed = parse_first_line(line.as_bytes()).unwrap();
            let line_start = line.as_ptr() as usize;
            let line_end = line_start + line.len();
            prop_assert_eq!(parsed.message_id.as_str(), message_id.as_str());
            prop_assert!((line_start..line_end).contains(&(parsed.message_id.as_str().as_ptr() as usize)));
            prop_assert_eq!(
                parsed.article_number,
                Some(ArticleNumber::from(article_number as u64))
            );
        }

        #[test]
        fn parse_first_line_rejects_missing_article_number(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            message_id in message_id_strategy(),
            suffix in string_regex(" [A-Za-z0-9._-]{1,20}").unwrap(),
        ) {
            let line = format!("{status} {message_id}{suffix}");
            prop_assert_eq!(
                parse_first_line(line.as_bytes()).unwrap_err(),
                ArticleParseError::InvalidArticleNumber
            );
        }

        #[test]
        fn parse_first_line_rejects_non_numeric_article_numbers(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            invalid_number in invalid_article_number_token_strategy(),
            message_id in message_id_strategy(),
            suffix in first_line_suffix_strategy(),
        ) {
            let line = format!("{status} {invalid_number} {message_id}{suffix}");

            prop_assert_eq!(
                parse_first_line(line.as_bytes()).unwrap_err(),
                ArticleParseError::InvalidArticleNumber
            );
        }

        #[test]
        fn parse_first_line_rejects_overflowing_article_numbers(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            overflowing_number in overflowing_article_number_token_strategy(),
            message_id in message_id_strategy(),
            suffix in first_line_suffix_strategy(),
        ) {
            let line = format!("{status} {overflowing_number} {message_id}{suffix}");

            prop_assert_eq!(
                parse_first_line(line.as_bytes()).unwrap_err(),
                ArticleParseError::InvalidArticleNumber
            );
        }

        #[test]
        fn parse_first_line_rejects_invalid_message_id_shapes(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            invalid_message_id in prop_oneof![
                Just("bad-id".to_string()),
                Just("<bad id>".to_string()),
                Just("<>".to_string()),
                Just("<missing".to_string()),
            ],
            article_number in 0_u32..=999_999,
            include_number in any::<bool>(),
        ) {
            let line = if include_number {
                format!("{status} {article_number} {invalid_message_id}")
            } else {
                format!("{status} {invalid_message_id}")
            };

            prop_assert_eq!(
                parse_first_line(line.as_bytes()).unwrap_err(),
                if include_number {
                    ArticleParseError::InvalidMessageId
                } else {
                    ArticleParseError::InvalidArticleNumber
                }
            );
        }

        #[test]
        fn parse_first_line_rejects_invalid_trailing_response_text(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            trailing in prop::sample::select(vec![
                b" bad\0text".to_vec(),
                b" bad caf\xe9".to_vec(),
            ]),
        ) {
            let mut line = format!("{status} {article_number} {message_id}").into_bytes();
            line.extend_from_slice(&trailing);

            prop_assert_eq!(
                parse_first_line(&line).unwrap_err(),
                ArticleParseError::InvalidMessageId
            );
        }

        #[test]
        fn article_parse_rejects_article_family_status_tokens_that_are_not_exactly_three_digits(
            status in prop_oneof![Just("22"), Just("220x"), Just("2200")],
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
        ) {
            // RFC 3977 section 3.1 defines the initial response line as a three-digit
            // status code followed by a space. Article-family parsers must not accept
            // adjacent bytes as part of the status token, even when the first three bytes
            // happen to be a known article status:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let frame = format!("{status} {article_number} {message_id}\r\n");

            prop_assert_eq!(
                Article::parse(frame.as_bytes()).unwrap_err(),
                ArticleParseError::InvalidStatusPrefix
            );
        }

        #[test]
        fn headers_parse_rejects_leading_folds_and_missing_colons(
            name in header_name_strategy(),
            value in header_value_strategy(),
            fold in prop_oneof![Just(" ".to_string()), Just("\t".to_string())],
        ) {
            let leading_fold = format!("{fold}{value}\r\n{name}: {value}\r\n");
            prop_assert!(matches!(
                Headers::parse(leading_fold.as_bytes()),
                Err(ArticleParseError::InvalidHeader(_))
            ));

            let missing_colon = format!("{name} {value}\r\n");
            prop_assert!(matches!(
                Headers::parse(missing_colon.as_bytes()),
                Err(ArticleParseError::InvalidHeader(_))
            ));
        }

        #[test]
        fn article_parse_reports_unsupported_status_codes(
            status in unsupported_status_strategy(),
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=3),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));
            let frame = format!("{status} {article_number} {message_id}\r\n{header_block}\r\n{body}.\r\n");

            prop_assert_eq!(
                Article::parse(frame.as_bytes()).unwrap_err(),
                ArticleParseError::InvalidStatusCode(status)
            );
        }

        #[test]
        fn article_parse_rejects_invalid_status_prefixes_before_other_parsing(
            buf in invalid_status_prefix_buffer_strategy(),
        ) {
            prop_assert_eq!(
                Article::parse(&buf).unwrap_err(),
                ArticleParseError::InvalidStatusPrefix
            );
        }

        #[test]
        fn article_family_responses_without_crlf_first_line_are_buffer_too_short(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
        ) {
            // RFC 3977 section 3.1 requires the response initial line to be terminated
            // by a CRLF pair. Without that pair, the parser must keep treating the frame
            // as incomplete rather than accepting a partial status line:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            let frame = format!("{status} {article_number} {message_id}");
            prop_assert_eq!(
                Article::parse(frame.as_bytes()).unwrap_err(),
                ArticleParseError::BufferTooShort
            );
        }

        #[test]
        fn article_family_responses_reject_bare_lf_or_cr_before_later_crlf(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            before in string_regex("[A-Za-z0-9 ._-]{0,16}").unwrap(),
            after in string_regex("[A-Za-z0-9 ._-]{0,16}").unwrap(),
            bad_separator in prop::sample::select(vec![b"\n".to_vec(), b"\r ".to_vec(), b"\r\r".to_vec()]),
        ) {
            // RFC 3977 section 3.1 says the response initial line ends with CRLF, and
            // section 3.1.1 says multiline block lines also use CRLF and otherwise MUST
            // NOT include bare LF or CR. The article parser must fail at the first invalid
            // line ending instead of resynchronizing on a later CRLF:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let mut frame = format!("{status} {article_number} {message_id}").into_bytes();
            frame.extend_from_slice(before.as_bytes());
            frame.extend_from_slice(&bad_separator);
            frame.extend_from_slice(after.as_bytes());
            frame.extend_from_slice(b"\r\nHeader: value\r\n\r\nbody\r\n.\r\n");

            prop_assert_eq!(Article::parse(&frame).unwrap_err(), ArticleParseError::BufferTooShort);
        }

        #[test]
        fn article_body_terminator_is_first_rfc_dot_line(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            mut body in vec(body_content_bytes(), 0..48),
            trailer in vec(dangerous_wire_bytes(), 0..16),
        ) {
            // RFC 3977 section 3.1.1 terminates a non-empty multiline block with the first
            // exact CRLF "." CRLF sequence. Generated body bytes are scrubbed of that exact
            // sequence before the real terminator is appended, so the parser must expose the
            // whole generated body and ignore trailer bytes after the terminator:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            while let Some(start) = body
                .windows(crate::TERMINATOR.len())
                .position(|window| window == crate::TERMINATOR)
            {
                body[start + 2] = b'x';
            }
            body.insert(0, b'x');
            body.push(b'x');

            let mut frame = format!("222 {article_number} {message_id}\r\n").into_bytes();
            frame.extend_from_slice(&body);
            frame.extend_from_slice(crate::TERMINATOR);
            frame.extend_from_slice(&trailer);

            let parsed = Article::parse(&frame).unwrap();
            let mut expected_body = body;
            expected_body.extend_from_slice(crate::CRLF);
            let expected_body = unstuff_dot_lines(&expected_body);
            prop_assert_eq!(parsed.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(parsed.article_number, Some(ArticleNumber::from(article_number as u64)));
            prop_assert_eq!(parsed.body.as_deref(), Some(expected_body.as_ref()));
        }

        #[test]
        fn article_body_rejects_missing_rfc_dot_line_despite_near_misses(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            prefix in vec(dangerous_wire_bytes(), 0..16),
            suffix in vec(dangerous_wire_bytes(), 0..16),
            near_miss in prop::sample::select(vec![
                b"\n.\r\n".to_vec(),
                b"\r.\r\n".to_vec(),
                b"\r\n.\n".to_vec(),
                b"\r\n.\r".to_vec(),
                b".foo\r\n".to_vec(),
                b"..\r\n".to_vec(),
                b"body.\r\n".to_vec(),
            ]),
        ) {
            // RFC 3977 section 3.1.1 names only CRLF "." CRLF as the non-empty multiline
            // terminator. Near-misses, dot-stuffed lines, and bare LF/CR variants are not a
            // complete terminating line, so BODY parsing must report a missing terminator:
            // https://www.rfc-editor.org/rfc/rfc3977#section-3.1.1
            let mut body = prefix;
            body.extend_from_slice(&near_miss);
            body.extend_from_slice(&suffix);
            while let Some(start) = body
                .windows(crate::TERMINATOR.len())
                .position(|window| window == crate::TERMINATOR)
            {
                body[start + 2] = b'x';
            }
            body.insert(0, b'x');
            body.push(b'x');

            let mut frame = format!("222 {article_number} {message_id}\r\n").into_bytes();
            frame.extend_from_slice(&body);

            prop_assert_eq!(Article::parse(&frame).unwrap_err(), ArticleParseError::MissingTerminator);
        }

        #[test]
        fn headers_parse_accepts_folded_continuations_without_creating_extra_headers(
            first_name in header_name_strategy(),
            first_value in header_value_strategy(),
            folded_values in vec(folded_header_value_strategy(), 1..=3),
            second_name in header_name_strategy(),
            second_value in header_value_strategy(),
            fold in prop_oneof![Just(" ".to_string()), Just("\t".to_string())],
        ) {
            prop_assume!(first_name != second_name);
            let mut data = format!("{first_name}: {first_value}\r\n");
            for value in &folded_values {
                data.push_str(&fold);
                data.push_str(value);
                data.push_str("\r\n");
            }
            data.push_str(&format!("{second_name}: {second_value}\r\n"));

            let headers = Headers::parse(data.as_bytes()).unwrap();
            let items: Vec<_> = headers.iter().collect();
            let mut first_expected = first_value.trim_start_matches([' ', '\t']).to_owned();
            for value in &folded_values {
                if first_expected.is_empty() {
                    first_expected.push_str(value.trim_start_matches([' ', '\t']));
                } else {
                    first_expected.push(' ');
                    first_expected.push_str(value.trim_start_matches([' ', '\t']));
                }
            }
            prop_assert_eq!(items.len(), 2);
            prop_assert_eq!(items[0].0, first_name.as_bytes());
            prop_assert_eq!(items[0].1, first_expected.as_bytes());
            prop_assert_eq!(items[1].0, second_name.as_bytes());
            prop_assert_eq!(
                items[1].1,
                second_value.trim_start_matches([' ', '\t']).as_bytes()
            );
            prop_assert_eq!(headers.get(&first_name), Some(first_expected.as_bytes()));
            prop_assert_eq!(
                headers.get(&second_name),
                Some(second_value.trim_start_matches([' ', '\t']).as_bytes())
            );
        }

        #[test]
        fn generated_article_family_frames_remain_zero_copy(
            message_id in message_id_strategy(),
            article_number in 1_u32..=999_999,
            headers in header_pairs_strategy(),
            body_lines in vec(body_line_strategy(), 1..=4),
        ) {
            let mut header_block = String::new();
            for (name, value) in &headers {
                header_block.push_str(name);
                header_block.push_str(": ");
                header_block.push_str(value);
                header_block.push_str("\r\n");
            }
            let body = format!("{}\r\n", body_lines.join("\r\n"));

            let article_frame =
                format!("220 {article_number} {message_id}\r\n{header_block}\r\n{body}.\r\n");
            let article = Article::parse(article_frame.as_bytes()).unwrap();
            let article_start = article_frame.as_ptr() as usize;
            let article_end = article_start + article_frame.len();
            let article_message_id = article.message_id.as_str();
            let article_headers = article.headers.unwrap();
            let article_headers = article_headers.as_bytes();
            let article_body = article.body.unwrap();
            prop_assert!((article_start..article_end).contains(&(article_message_id.as_ptr() as usize)));
            prop_assert!((article_start..article_end).contains(&(article_headers.as_ptr() as usize)));
            prop_assert!((article_start..article_end).contains(&(article_body.as_ptr() as usize)));

            let head_frame =
                format!("221 {article_number} {message_id}\r\n{header_block}.\r\n");
            let head = Article::parse(head_frame.as_bytes()).unwrap();
            let head_start = head_frame.as_ptr() as usize;
            let head_end = head_start + head_frame.len();
            let head_message_id = head.message_id.as_str();
            let head_headers = head.headers.unwrap();
            let head_headers = head_headers.as_bytes();
            prop_assert!((head_start..head_end).contains(&(head_message_id.as_ptr() as usize)));
            prop_assert!((head_start..head_end).contains(&(head_headers.as_ptr() as usize)));
            prop_assert!(head.body.is_none());

            let body_frame = format!("222 {article_number} {message_id}\r\n{body}.\r\n");
            let parsed_body = Article::parse(body_frame.as_bytes()).unwrap();
            let body_start = body_frame.as_ptr() as usize;
            let body_end = body_start + body_frame.len();
            let body_message_id = parsed_body.message_id.as_str();
            let body_slice = parsed_body.body.unwrap();
            prop_assert!((body_start..body_end).contains(&(body_message_id.as_ptr() as usize)));
            prop_assert!((body_start..body_end).contains(&(body_slice.as_ptr() as usize)));
            prop_assert!(parsed_body.headers.is_none());

            let stat_frame = format!("223 {article_number} {message_id}\r\n");
            let stat = Article::parse(stat_frame.as_bytes()).unwrap();
            let stat_start = stat_frame.as_ptr() as usize;
            let stat_end = stat_start + stat_frame.len();
            let stat_message_id = stat.message_id.as_str();
            prop_assert!((stat_start..stat_end).contains(&(stat_message_id.as_ptr() as usize)));
            prop_assert!(stat.headers.is_none());
            prop_assert!(stat.body.is_none());
        }
    }
}

impl fmt::Display for ArticleParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStatusCode(code) => write!(f, "invalid status code: {code}"),
            Self::InvalidStatusPrefix => write!(f, "invalid status code prefix"),
            Self::MissingSeparator => {
                write!(f, "missing blank line separator between headers and body")
            }
            Self::MissingTerminator => write!(f, "missing multiline terminator"),
            Self::InvalidHeader(reason) => write!(f, "invalid header: {reason}"),
            Self::InvalidBody => write!(f, "invalid body content"),
            Self::UnexpectedBody => write!(f, "response contains unexpected body"),
            Self::BufferTooShort => write!(f, "buffer too short to contain valid response"),
            Self::InvalidArticleNumber => write!(f, "invalid article number"),
            Self::InvalidMessageId => write!(f, "invalid message-id"),
        }
    }
}

impl std::error::Error for ArticleParseError {}

impl fmt::Display for InvalidHeaderReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeadingFold => write!(f, "header cannot start with folding whitespace"),
            Self::EmptyFold => write!(f, "folded header line cannot contain only whitespace"),
            Self::MissingHeader => write!(f, "header block must contain at least one header"),
            Self::MissingColon => write!(f, "header missing colon"),
            Self::MissingSpaceAfterColon => write!(f, "header missing space after colon"),
            Self::EmptyName => write!(f, "empty header name"),
            Self::InvalidName => write!(f, "invalid character in header name"),
            Self::InvalidContent => write!(f, "invalid character in header content"),
        }
    }
}

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

/// Resource-bound response states shared with the proxy's article boundary.
///
/// The storage adapters remain local to each repository. These state names
/// describe the guarantees, not a common allocation type.
pub(crate) mod state {
    use super::{ArticleLayout, RequestKind, StatusCode};

    /// Storage whose bytes remain stable while a validated layout is used.
    /// This crate-private contract prevents validation from accepting an
    /// arbitrary `AsRef<[u8]>` implementation with changing contents.
    pub(crate) trait StableBytes {
        fn as_slice(&self) -> &[u8];
    }

    impl StableBytes for bytes::Bytes {
        fn as_slice(&self) -> &[u8] {
            self.as_ref()
        }
    }

    /// Exclusive end of the request-scoped status line in a framed response.
    /// This coordinate is relative to the same immutable bytes as the frame.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct StatusLineEnd(usize);

    impl StatusLineEnd {
        pub(crate) const fn new(value: usize) -> Self {
            Self(value)
        }

        pub(crate) const fn get(self) -> usize {
            self.0
        }
    }

    /// An article operation in one protocol-owned state.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct Article<State>(State);

    impl<State> Article<State> {
        pub(crate) const fn new(state: State) -> Self {
            Self(state)
        }

        pub(crate) const fn as_inner(&self) -> &State {
            &self.0
        }

        pub(crate) fn as_inner_mut(&mut self) -> &mut State {
            &mut self.0
        }
    }

    impl<B> Article<Framed<B>> {
        pub(crate) const fn kind(&self) -> RequestKind {
            self.0.kind()
        }

        pub(crate) const fn status(&self) -> StatusCode {
            self.0.status()
        }

        pub(crate) const fn bounds(&self) -> Option<crate::terminator::MultilineFrameBounds> {
            self.0.bounds()
        }

        pub(crate) const fn status_line_end(&self) -> StatusLineEnd {
            self.0.status_line_end()
        }

        pub(crate) const fn content_end(&self) -> ContentEnd {
            self.0.content_end()
        }

        pub(crate) fn as_bytes(&self) -> &[u8]
        where
            B: StableBytes,
        {
            self.0.bytes().as_slice()
        }
    }

    impl Article<Framed<bytes::Bytes>> {
        pub(crate) fn clone_bytes(&self) -> bytes::Bytes {
            self.0.bytes().clone()
        }
    }

    /// A complete wire response retained by an adapter owner.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct Framed<B> {
        bytes: B,
        kind: RequestKind,
        status: StatusCode,
        bounds: Option<crate::terminator::MultilineFrameBounds>,
        status_line_end: StatusLineEnd,
        content_end: ContentEnd,
    }

    impl<B> Framed<B> {
        pub(crate) const fn new(
            bytes: B,
            kind: RequestKind,
            status: StatusCode,
            bounds: Option<crate::terminator::MultilineFrameBounds>,
            status_line_end: StatusLineEnd,
            content_end: ContentEnd,
        ) -> Self {
            Self {
                bytes,
                kind,
                status,
                bounds,
                status_line_end,
                content_end,
            }
        }

        pub(crate) fn bytes(&self) -> &B {
            &self.bytes
        }

        pub(crate) const fn kind(&self) -> RequestKind {
            self.kind
        }

        pub(crate) const fn status(&self) -> StatusCode {
            self.status
        }

        pub(crate) const fn bounds(&self) -> Option<crate::terminator::MultilineFrameBounds> {
            self.bounds
        }

        pub(crate) const fn status_line_end(&self) -> StatusLineEnd {
            self.status_line_end
        }

        pub(crate) const fn content_end(&self) -> ContentEnd {
            self.content_end
        }
    }

    /// Semantic validation state for stable bytes and its private layout.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct Validated<B> {
        bytes: B,
        layout: ArticleLayout,
    }

    /// Exclusive end of the semantic response content in frame-relative
    /// coordinates. Multiline terminator bytes and packed suffixes are after
    /// this boundary.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct ContentEnd(usize);

    impl ContentEnd {
        pub(crate) const fn new(value: usize) -> Self {
            Self(value)
        }

        pub(crate) const fn get(self) -> usize {
            self.0
        }
    }

    impl<B> Validated<B> {
        pub(super) const fn new(bytes: B, layout: ArticleLayout) -> Self {
            Self { bytes, layout }
        }

        pub(super) fn bytes(&self) -> &B {
            &self.bytes
        }

        pub(super) fn layout(&self) -> &ArticleLayout {
            &self.layout
        }
    }
}

/// A byte range proven to lie within a validated article frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ArticleFrameRange {
    start: usize,
    end: usize,
}

impl ArticleFrameRange {
    fn new(start: usize, end: usize) -> Result<Self, ArticleParseError> {
        if start <= end {
            Ok(Self { start, end })
        } else {
            Err(ArticleParseError::BufferTooShort)
        }
    }

    fn slice(self, buffer: &[u8]) -> Result<&[u8], ArticleParseError> {
        buffer
            .get(self.start..self.end)
            .ok_or(ArticleParseError::BufferTooShort)
    }

    fn validated_slice(self, buffer: &[u8]) -> &[u8] {
        buffer
            .get(self.start..self.end)
            .expect("validated article layout preserves its byte ranges")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ValidatedFirstLine {
    message_id: ArticleFrameRange,
    article_number: ArticleNumber,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedFirstLine<'a> {
    message_id: MessageId<'a>,
    message_id_range: ArticleFrameRange,
    article_number: Option<ArticleNumber>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeaderTransformation {
    None,
    Unfold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyTransformation {
    None,
    Unstuff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidatedArticleContent {
    Article {
        headers: ArticleFrameRange,
        header_transformation: HeaderTransformation,
        body: ArticleFrameRange,
        body_transformation: BodyTransformation,
    },
    Head {
        headers: ArticleFrameRange,
        header_transformation: HeaderTransformation,
    },
    Body {
        body: ArticleFrameRange,
        body_transformation: BodyTransformation,
    },
    Stat {
        content_start: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ArticleLayout {
    first_line: ValidatedFirstLine,
    content: ValidatedArticleContent,
}

impl ArticleLayout {
    fn materialize<'a>(self, buffer: &'a [u8]) -> Article<'a> {
        let message_id = materialize_validated_message_id(buffer, self.first_line.message_id);
        let article_number = Some(self.first_line.article_number);

        match self.content {
            ValidatedArticleContent::Article {
                headers,
                header_transformation,
                body,
                body_transformation,
            } => Article {
                message_id,
                article_number,
                headers: Some(Headers::from_validated(
                    buffer,
                    headers,
                    header_transformation,
                )),
                body: Some(materialize_validated_body(
                    buffer,
                    body,
                    body_transformation,
                )),
            },
            ValidatedArticleContent::Head {
                headers,
                header_transformation,
            } => Article {
                message_id,
                article_number,
                headers: Some(Headers::from_validated(
                    buffer,
                    headers,
                    header_transformation,
                )),
                body: None,
            },
            ValidatedArticleContent::Body {
                body,
                body_transformation,
            } => Article {
                message_id,
                article_number,
                headers: None,
                body: Some(materialize_validated_body(
                    buffer,
                    body,
                    body_transformation,
                )),
            },
            ValidatedArticleContent::Stat { .. } => Article {
                message_id,
                article_number,
                headers: None,
                body: None,
            },
        }
    }

    fn content_range(self) -> ArticleFrameRange {
        match self.content {
            ValidatedArticleContent::Article { headers, body, .. } => ArticleFrameRange {
                start: headers.start,
                end: body.end,
            },
            ValidatedArticleContent::Head { headers, .. } => headers,
            ValidatedArticleContent::Body { body, .. } => body,
            ValidatedArticleContent::Stat { content_start } => ArticleFrameRange {
                start: content_start,
                end: content_start,
            },
        }
    }
}

/// An article layout bound to the immutable bytes it validates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValidatedArticleView<'a> {
    buffer: &'a [u8],
    layout: ArticleLayout,
}

/// Owned validated article state used by the buffered client.
pub(crate) type ValidatedOwnedArticle = state::Article<state::Validated<Bytes>>;

impl<'a> ValidatedArticleView<'a> {
    pub(crate) fn materialize(self) -> Article<'a> {
        self.layout.materialize(self.buffer)
    }

    pub(crate) fn into_owned(self, bytes: Bytes) -> ValidatedOwnedArticle {
        assert_eq!(self.buffer.as_ptr(), bytes.as_ptr());
        assert_eq!(self.buffer.len(), bytes.len());
        state::Article::new(state::Validated::new(bytes, self.layout))
    }
}

impl state::Validated<Bytes> {
    #[must_use]
    pub(crate) fn content(&self) -> &[u8] {
        self.layout()
            .content_range()
            .slice(self.bytes())
            .expect("owned article preserves its validated content range")
    }

    #[must_use]
    pub(crate) fn materialize(&self) -> Article<'_> {
        self.layout().materialize(self.bytes())
    }
}

impl ValidatedOwnedArticle {
    #[must_use]
    pub(crate) fn bytes(&self) -> &[u8] {
        self.as_inner().bytes()
    }

    #[must_use]
    pub(crate) fn content(&self) -> &[u8] {
        self.as_inner().content()
    }

    #[must_use]
    pub(crate) fn materialize(&self) -> Article<'_> {
        self.as_inner().materialize()
    }
}

/// Bounds supplied by the response framer for an article-family response.
#[derive(Debug, Clone)]
struct FramedArticle<'a> {
    buffer: &'a [u8],
    first_line: ArticleFrameRange,
    content: ArticleFrameRange,
}

impl<'a> FramedArticle<'a> {
    fn from_content_bounds(
        buffer: &'a [u8],
        content_start: usize,
        content_end: usize,
    ) -> Result<Self, ArticleParseError> {
        if content_start > content_end || content_end > buffer.len() {
            return Err(ArticleParseError::BufferTooShort);
        }

        let first_line_end = strict_crlf_line_content_end_from(buffer, 0)
            .ok_or(ArticleParseError::BufferTooShort)?;
        Self::from_known_content_bounds(
            buffer,
            content_start,
            content_end,
            state::StatusLineEnd::new(first_line_end + crate::CRLF.len()),
        )
    }

    fn from_known_content_bounds(
        buffer: &'a [u8],
        content_start: usize,
        content_end: usize,
        status_line_end: state::StatusLineEnd,
    ) -> Result<Self, ArticleParseError> {
        if content_start > content_end || content_end > buffer.len() {
            return Err(ArticleParseError::BufferTooShort);
        }

        let first_line_end = status_line_end
            .get()
            .checked_sub(crate::CRLF.len())
            .ok_or(ArticleParseError::BufferTooShort)?;
        let first_line = ArticleFrameRange::new(0, first_line_end)?;
        if first_line_end + crate::CRLF.len() != content_start
            || buffer.get(first_line_end..content_start) != Some(crate::CRLF)
        {
            return Err(ArticleParseError::BufferTooShort);
        }

        Ok(Self {
            buffer,
            first_line,
            content: ArticleFrameRange::new(content_start, content_end)?,
        })
    }

    fn content_prefix(&self) -> Result<&[u8], ArticleParseError> {
        self.buffer
            .get(..self.content.end)
            .ok_or(ArticleParseError::BufferTooShort)
    }

    fn validate(self) -> Result<ValidatedArticleView<'a>, ArticleParseError> {
        let status = parse_status_code(self.buffer)?;
        self.validate_for_status(status)
    }

    fn validate_for_status(
        self,
        status: u16,
    ) -> Result<ValidatedArticleView<'a>, ArticleParseError> {
        let buffer = self.buffer;
        let first_line = validate_first_line(buffer, self.first_line)?;

        let content = match status {
            220 => self.validate_article_content(),
            221 => self.validate_head_content(),
            222 => self.validate_body_content(),
            223 => self.validate_stat_content(),
            status_code => Err(ArticleParseError::InvalidStatusCode(status_code)),
        }?;

        Ok(ValidatedArticleView {
            buffer,
            layout: ArticleLayout {
                first_line,
                content,
            },
        })
    }

    fn validate_article_content(self) -> Result<ValidatedArticleContent, ArticleParseError> {
        let separator = find_blank_line(self.content_prefix()?, self.content.start)?;
        let headers_end = separator
            .checked_add(crate::CRLF.len())
            .ok_or(ArticleParseError::BufferTooShort)?;
        let headers = ArticleFrameRange::new(self.content.start, headers_end)?;
        let header_transformation = validate_headers(headers.slice(self.buffer)?)?;

        let body_start = headers_end
            .checked_add(crate::CRLF.len())
            .ok_or(ArticleParseError::BufferTooShort)?;
        if body_start > self.content.end {
            return Err(ArticleParseError::BufferTooShort);
        }
        let body = ArticleFrameRange::new(body_start, self.content.end)?;
        let body_transformation = validate_body_content(body.slice(self.buffer)?)?;

        Ok(ValidatedArticleContent::Article {
            headers,
            header_transformation,
            body,
            body_transformation,
        })
    }

    fn validate_head_content(self) -> Result<ValidatedArticleContent, ArticleParseError> {
        if find_blank_line(self.content_prefix()?, self.content.start).is_ok() {
            return Err(ArticleParseError::UnexpectedBody);
        }
        let header_transformation = validate_headers(self.content.slice(self.buffer)?)?;

        Ok(ValidatedArticleContent::Head {
            headers: self.content,
            header_transformation,
        })
    }

    fn validate_body_content(self) -> Result<ValidatedArticleContent, ArticleParseError> {
        let body_transformation = validate_body_content(self.content.slice(self.buffer)?)?;

        Ok(ValidatedArticleContent::Body {
            body: self.content,
            body_transformation,
        })
    }

    fn validate_stat_content(self) -> Result<ValidatedArticleContent, ArticleParseError> {
        if self.content.start != self.content.end {
            return Err(ArticleParseError::UnexpectedBody);
        }

        Ok(ValidatedArticleContent::Stat {
            content_start: self.content.start,
        })
    }
}

/// Validated header block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Headers<'a> {
    data: Cow<'a, [u8]>,
}

impl<'a> Headers<'a> {
    /// Parse and validate a header block.
    pub fn parse(data: &'a [u8]) -> Result<Self, ArticleParseError> {
        let transformation = validate_headers(data)?;
        Ok(Self::from_transformation(data, transformation))
    }

    fn from_transformation(data: &'a [u8], transformation: HeaderTransformation) -> Self {
        Self {
            data: match transformation {
                HeaderTransformation::None => Cow::Borrowed(data),
                HeaderTransformation::Unfold => unfold_header_continuations(data),
            },
        }
    }

    fn from_validated(
        buffer: &'a [u8],
        headers: ArticleFrameRange,
        transformation: HeaderTransformation,
    ) -> Self {
        match transformation {
            HeaderTransformation::None => Self {
                data: Cow::Borrowed(headers.validated_slice(buffer)),
            },
            HeaderTransformation::Unfold => Self {
                data: unfold_header_continuations(headers.validated_slice(buffer)),
            },
        }
    }

    /// Return a header value by case-insensitive name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        let lookup = name.as_bytes();
        let mut pos = 0;
        let data = self.data.as_ref();

        while pos < data.len() {
            let line_end = strict_crlf_line_content_end_from(data, pos)?;
            let line = &data[pos..line_end];
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
            if header_name.eq_ignore_ascii_case(lookup) {
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
    pub fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            data: self.data.as_ref(),
            pos: 0,
        }
    }

    /// Return the raw header bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl<'headers, 'data> IntoIterator for &'headers Headers<'data> {
    type Item = (&'headers [u8], &'headers [u8]);
    type IntoIter = HeaderIter<'headers>;

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
            let line_end = strict_crlf_line_content_end_from(self.data, self.pos)?;
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
    pub body: Option<Cow<'a, [u8]>>,
}

/// The consumer-facing projection of a validated article.
///
/// The name makes the boundary explicit: this value is a reusable view, not
/// the proof-bearing owner returned by the framing/validation pipeline.
pub type ArticleView<'a> = Article<'a>;

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

    /// Parse a response frame whose multiline content boundary was already found.
    ///
    /// This is for callers that already performed RFC 3977 section 3.1.1
    /// dot-terminator framing and can pass the payload range directly.
    pub(crate) fn parse_article_frame(
        buf: &'a [u8],
        content_start: usize,
        content_end: usize,
    ) -> Result<Self, ArticleParseError> {
        let validated = Self::validate_framed_article(buf, content_start, content_end)?;
        Ok(validated.materialize())
    }

    /// Validate a framed article without constructing unfolded or unstuffed data.
    ///
    /// The private framed handle keeps the bytes and their checked ranges
    /// together until validation has produced the reusable article view.
    pub(crate) fn validate_framed_article(
        buf: &'a [u8],
        content_start: usize,
        content_end: usize,
    ) -> Result<ValidatedArticleView<'a>, ArticleParseError> {
        FramedArticle::from_content_bounds(buf, content_start, content_end)?.validate()
    }

    fn parse_article(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end =
            strict_crlf_line_content_end_from(buf, 0).ok_or(ArticleParseError::BufferTooShort)?;
        let parsed = parse_first_line(&buf[..first_line_end])?;
        let message_id = parsed.message_id;
        let article_number = parsed.article_number;
        let content_start = first_line_end + 2;
        let separator_pos = find_blank_line(buf, content_start)?;
        let headers = Some(Headers::parse(&buf[content_start..separator_pos + 2])?);
        let body_start = separator_pos + 4;
        let body_end = find_article_content_end(buf, body_start)
            .ok_or(ArticleParseError::MissingTerminator)?;
        validate_body_content(&buf[body_start..body_end])?;

        Ok(Self {
            message_id,
            article_number,
            headers,
            body: Some(unstuff_dot_lines(&buf[body_start..body_end])),
        })
    }

    fn parse_head(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end =
            strict_crlf_line_content_end_from(buf, 0).ok_or(ArticleParseError::BufferTooShort)?;
        let parsed = parse_first_line(&buf[..first_line_end])?;
        let message_id = parsed.message_id;
        let article_number = parsed.article_number;
        let content_start = first_line_end + 2;
        if find_blank_line(buf, content_start).is_ok() {
            return Err(ArticleParseError::UnexpectedBody);
        }
        let headers_end = find_article_content_end(buf, content_start)
            .ok_or(ArticleParseError::MissingTerminator)?;

        Ok(Self {
            message_id,
            article_number,
            headers: Some(Headers::parse(&buf[content_start..headers_end])?),
            body: None,
        })
    }

    fn parse_body(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end =
            strict_crlf_line_content_end_from(buf, 0).ok_or(ArticleParseError::BufferTooShort)?;
        let parsed = parse_first_line(&buf[..first_line_end])?;
        let message_id = parsed.message_id;
        let article_number = parsed.article_number;
        let body_start = first_line_end + 2;
        let body_end = find_article_content_end(buf, body_start)
            .ok_or(ArticleParseError::MissingTerminator)?;
        validate_body_content(&buf[body_start..body_end])?;

        Ok(Self {
            message_id,
            article_number,
            headers: None,
            body: Some(unstuff_dot_lines(&buf[body_start..body_end])),
        })
    }

    fn parse_stat(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end =
            strict_crlf_line_content_end_from(buf, 0).ok_or(ArticleParseError::BufferTooShort)?;
        let parsed = parse_first_line(&buf[..first_line_end])?;
        let message_id = parsed.message_id;
        let article_number = parsed.article_number;
        let content_start = first_line_end + 2;
        if content_start != buf.len() {
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

fn find_article_content_end(buf: &[u8], start: usize) -> Option<usize> {
    let slice = buf.get(start..)?;
    if slice.starts_with(DOT_TERMINATOR) {
        return Some(start);
    }

    find_terminator_content_end(buf, start)
}

fn validate_body_content(buf: &[u8]) -> Result<BodyTransformation, ArticleParseError> {
    if buf.contains(&b'\0') {
        return Err(ArticleParseError::InvalidBody);
    }

    let mut pos = 0;
    let mut transformation = BodyTransformation::None;
    while pos < buf.len() {
        let line_end =
            strict_crlf_line_content_end_from(buf, pos).ok_or(ArticleParseError::InvalidBody)?;
        let line = &buf[pos..line_end];
        if line.starts_with(b".") {
            if !line.starts_with(b"..") {
                return Err(ArticleParseError::InvalidBody);
            }
            transformation = BodyTransformation::Unstuff;
        }
        pos = line_end + crate::CRLF.len();
    }

    Ok(transformation)
}

fn unfold_header_continuations(buf: &[u8]) -> Cow<'_, [u8]> {
    let mut unfolded = Vec::with_capacity(buf.len());
    let mut pos = 0;
    while pos < buf.len() {
        if pos + 2 < buf.len()
            && buf[pos] == b'\r'
            && buf[pos + 1] == b'\n'
            && matches!(buf[pos + 2], b' ' | b'\t')
        {
            unfolded.push(b' ');
            pos += 3;
            while pos < buf.len() && matches!(buf[pos], b' ' | b'\t') {
                pos += 1;
            }
        } else {
            unfolded.push(buf[pos]);
            pos += 1;
        }
    }

    Cow::Owned(unfolded)
}

fn unstuff_dot_lines(buf: &[u8]) -> Cow<'_, [u8]> {
    if !has_dot_stuffed_line(buf) {
        return Cow::Borrowed(buf);
    }

    unstuff_known_dot_lines(buf)
}

fn unstuff_known_dot_lines(buf: &[u8]) -> Cow<'_, [u8]> {
    let mut unstuffed = Vec::with_capacity(buf.len());
    let mut line_start = true;
    for &byte in buf {
        if line_start && byte == b'.' {
            line_start = false;
            continue;
        }
        unstuffed.push(byte);
        line_start = byte == b'\n';
    }
    Cow::Owned(unstuffed)
}

fn materialize_validated_body(
    buffer: &[u8],
    body: ArticleFrameRange,
    transformation: BodyTransformation,
) -> Cow<'_, [u8]> {
    match transformation {
        BodyTransformation::None => Cow::Borrowed(body.validated_slice(buffer)),
        BodyTransformation::Unstuff => unstuff_known_dot_lines(body.validated_slice(buffer)),
    }
}

fn has_dot_stuffed_line(buf: &[u8]) -> bool {
    buf.first() == Some(&b'.')
        || buf
            .windows(3)
            .any(|window| window[0] == b'\r' && window[1] == b'\n' && window[2] == b'.')
}

fn parse_status_code(buf: &[u8]) -> Result<u16, ArticleParseError> {
    StatusCode::parse(buf)
        .map(StatusCode::as_u16)
        .ok_or(ArticleParseError::InvalidStatusPrefix)
}

fn parse_first_line(line: &[u8]) -> Result<ParsedFirstLine<'_>, ArticleParseError> {
    let first_space = memchr::memchr(b' ', line).ok_or(ArticleParseError::InvalidStatusPrefix)?;
    if first_space != 3 {
        return Err(ArticleParseError::InvalidStatusPrefix);
    }
    let second_space = memchr::memchr(b' ', &line[first_space + 1..])
        .map(|pos| first_space + 1 + pos)
        .ok_or(ArticleParseError::InvalidArticleNumber)?;

    let article_number = parse_response_article_number(&line[first_space + 1..second_space])?;
    let msgid_start = second_space + 1;
    if line.get(msgid_start) != Some(&b'<') {
        return Err(ArticleParseError::InvalidMessageId);
    }

    let msgid_end = memchr::memchr(b'>', &line[msgid_start..])
        .map(|pos| msgid_start + pos + 1)
        .ok_or(ArticleParseError::InvalidMessageId)?;
    if msgid_end < line.len() && line.get(msgid_end) != Some(&b' ') {
        return Err(ArticleParseError::InvalidMessageId);
    }
    if msgid_end < line.len() && !validate_optional_trailing_comment(&line[msgid_end..]) {
        return Err(ArticleParseError::InvalidMessageId);
    }
    let msgid = std::str::from_utf8(&line[msgid_start..msgid_end])
        .map_err(|_| ArticleParseError::InvalidMessageId)?;

    Ok(ParsedFirstLine {
        message_id: MessageId::from_borrowed(msgid)?,
        message_id_range: ArticleFrameRange::new(msgid_start, msgid_end)?,
        article_number: Some(article_number),
    })
}

fn validate_first_line(
    buffer: &[u8],
    first_line: ArticleFrameRange,
) -> Result<ValidatedFirstLine, ArticleParseError> {
    let line = first_line.slice(buffer)?;
    let parsed = parse_first_line(line)?;
    let message_start = first_line
        .start
        .checked_add(parsed.message_id_range.start)
        .ok_or(ArticleParseError::BufferTooShort)?;
    let message_end = first_line
        .start
        .checked_add(parsed.message_id_range.end)
        .ok_or(ArticleParseError::BufferTooShort)?;

    Ok(ValidatedFirstLine {
        message_id: ArticleFrameRange::new(message_start, message_end)?,
        article_number: parsed
            .article_number
            .ok_or(ArticleParseError::InvalidArticleNumber)?,
    })
}

fn materialize_validated_message_id(buffer: &[u8], message_id: ArticleFrameRange) -> MessageId<'_> {
    let value = std::str::from_utf8(message_id.validated_slice(buffer))
        .expect("validated article layout preserves UTF-8 message-id bytes");
    // `validate_first_line` already checked the complete message-id grammar.
    // Re-running it here made every typed accessor pay for a second scan.
    MessageId::from_validated_borrowed(value)
}

fn parse_response_article_number(value: &[u8]) -> Result<ArticleNumber, ArticleParseError> {
    if value.is_empty() || value.len() > 16 || !value.iter().all(u8::is_ascii_digit) {
        return Err(ArticleParseError::InvalidArticleNumber);
    }

    let number = std::str::from_utf8(value)
        .map_err(|_| ArticleParseError::InvalidArticleNumber)?
        .parse::<u64>()
        .map_err(|_| ArticleParseError::InvalidArticleNumber)?;
    if number > MAX_ARTICLE_NUMBER {
        return Err(ArticleParseError::InvalidArticleNumber);
    }
    Ok(ArticleNumber::from(number))
}

fn find_blank_line(buf: &[u8], start: usize) -> Result<usize, ArticleParseError> {
    memchr::memmem::find(&buf[start..], b"\r\n\r\n")
        .map(|pos| start + pos)
        .ok_or(ArticleParseError::MissingSeparator)
}

fn validate_headers(data: &[u8]) -> Result<HeaderTransformation, ArticleParseError> {
    let mut pos = 0;
    let mut has_header = false;
    let mut transformation = HeaderTransformation::None;
    while pos < data.len() {
        let line_end = strict_crlf_line_content_end_from(data, pos)
            .ok_or(ArticleParseError::BufferTooShort)?;
        let line = &data[pos..line_end];

        if line.is_empty() {
            pos = line_end + 2;
            continue;
        }

        if line[0] == b' ' || line[0] == b'\t' {
            if pos == 0 {
                return Err(ArticleParseError::InvalidHeader(
                    InvalidHeaderReason::LeadingFold,
                ));
            }
            if line.iter().all(|byte| matches!(*byte, b' ' | b'\t')) {
                return Err(ArticleParseError::InvalidHeader(
                    InvalidHeaderReason::EmptyFold,
                ));
            }
            if line.contains(&b'\0') {
                return Err(ArticleParseError::InvalidHeader(
                    InvalidHeaderReason::InvalidContent,
                ));
            }
            transformation = HeaderTransformation::Unfold;
            pos = line_end + 2;
            continue;
        }
        has_header = true;

        let colon_pos = memchr::memchr(b':', line).ok_or(ArticleParseError::InvalidHeader(
            InvalidHeaderReason::MissingColon,
        ))?;
        let name = &line[..colon_pos];
        if name.is_empty() {
            return Err(ArticleParseError::InvalidHeader(
                InvalidHeaderReason::EmptyName,
            ));
        }
        for &byte in name {
            if byte == b' ' || byte == b'\t' || !(33..=126).contains(&byte) {
                return Err(ArticleParseError::InvalidHeader(
                    InvalidHeaderReason::InvalidName,
                ));
            }
        }
        if colon_pos + 1 == line.len() {
            if data.get(line_end + crate::CRLF.len()) == Some(&b' ') {
                pos = line_end + crate::CRLF.len();
                continue;
            }
            return Err(ArticleParseError::InvalidHeader(
                InvalidHeaderReason::MissingSpaceAfterColon,
            ));
        }
        if line.get(colon_pos + 1) != Some(&b' ') {
            return Err(ArticleParseError::InvalidHeader(
                InvalidHeaderReason::MissingSpaceAfterColon,
            ));
        }
        if line[colon_pos + 2..].contains(&b'\0') {
            return Err(ArticleParseError::InvalidHeader(
                InvalidHeaderReason::InvalidContent,
            ));
        }

        pos = line_end + 2;
    }

    if has_header {
        Ok(transformation)
    } else {
        Err(ArticleParseError::InvalidHeader(
            InvalidHeaderReason::MissingHeader,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_ARTICLE_TEXT: &[u8] = b"220 12345 <test@example.com>\r\n\
Subject: Test Article\r\n\
From: test@example.com\r\n\
Date: Sat, 30 Nov 2024 12:00:00 +0000\r\n\
Message-ID: <test@example.com>\r\n\
\r\n\
This is the article body.\r\n\
Multiple lines of text.\r\n\
.\r\n";

    const VALID_HEAD: &[u8] = b"221 12345 <test@example.com>\r\n\
Subject: Test Article\r\n\
From: test@example.com\r\n\
Date: Sat, 30 Nov 2024 12:00:00 +0000\r\n\
Message-ID: <test@example.com>\r\n\
.\r\n";

    const VALID_BODY: &[u8] = b"222 12345 <test@example.com>\r\n\
This is the article body.\r\n\
Multiple lines of text.\r\n\
.\r\n";

    const VALID_ARTICLE_YENC: &[u8] = b"220 54321 <binary@example.com>\r\n\
Subject: test.txt (1/1)\r\n\
From: poster@example.com\r\n\
Message-ID: <binary@example.com>\r\n\
\r\n\
=ybegin line=128 size=12 name=test.txt\r\n\
r\x8f\x96\x96\x99VJ\xa3o\x98\x8dK\r\n\
=yend size=12 crc32=0337ab3d\r\n\
.\r\n";

    const VALID_STAT: &[u8] = b"223 12345 <test@example.com>\r\n";

    const ARTICLE_BY_MSGID: &[u8] = b"220 0 <msgid@example.com>\r\n\
Subject: Retrieved by message-ID\r\n\
\r\n\
Body\r\n\
.\r\n";

    const BODY_WITH_HEADERS: &[u8] = b"222 12345 <test@example.com>\r\n\
Subject: Should be ignored\r\n\
\r\n\
Actual body content\r\n\
.\r\n";

    const FOLDED_HEADER: &[u8] = concat!(
        "220 12345 <test@example.com>\r\n",
        "Subject: This is a long subject\r\n",
        " that continues on the next line\r\n",
        "From: test@example.com\r\n",
        "\r\n",
        "Body\r\n",
        ".\r\n"
    )
    .as_bytes();

    #[test]
    fn parse_article_response_220() {
        let buf =
            b"220 100 <test@example.com> article\r\nSubject: Test\r\nFrom: user@example.com\r\n\r\nBody content\r\n.\r\n";

        let article = Article::parse(buf).unwrap();
        assert_eq!(article.message_id.as_str(), "<test@example.com>");
        assert_eq!(article.article_number, Some(ArticleNumber(100)));
        assert_eq!(article.headers.unwrap().get("Subject"), Some(&b"Test"[..]));
        assert_eq!(article.body.as_deref(), Some(&b"Body content\r\n"[..]));
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
        assert_eq!(article.body.as_deref(), Some(&b"Body content\r\n"[..]));
    }

    #[test]
    fn framed_validation_materializes_every_article_response() {
        for frame in [VALID_ARTICLE_TEXT, VALID_HEAD, VALID_BODY, VALID_STAT] {
            let content_start = strict_crlf_line_content_end_from(frame, 0).unwrap() + 2;
            let content_end = if frame.starts_with(b"223") {
                content_start
            } else {
                find_article_content_end(frame, content_start).unwrap()
            };
            let validated =
                Article::validate_framed_article(frame, content_start, content_end).unwrap();
            let reused = validated.materialize();
            assert_eq!(reused, Article::parse(frame).unwrap());
        }
    }

    #[test]
    fn owned_article_validation_preserves_the_bound_bytes_and_layout() {
        let bytes = Bytes::from_static(VALID_BODY);
        let content_start = strict_crlf_line_content_end_from(&bytes, 0).unwrap() + 2;
        let content_end = find_article_content_end(&bytes, content_start).unwrap();
        let validated =
            Article::validate_framed_article(&bytes, content_start, content_end).unwrap();
        let owned = validated.into_owned(bytes.clone());

        assert_eq!(owned.bytes(), VALID_BODY);
        assert_eq!(owned.materialize(), Article::parse(VALID_BODY).unwrap());
    }

    #[test]
    fn parse_stat_response_223() {
        let buf = b"223 100 <test@example.com>\r\n";
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

    #[test]
    fn article_header_parsing_does_not_allocate() {
        let valid = b"Subject: Test\r\nFrom: user@example.com\r\n";
        let empty = b"";
        let missing_colon = b"Invalid Header\r\n";
        let missing_space = b"Subject:Test\r\n";
        let invalid_name = b"Invalid Header: value\r\n";
        let invalid_content = b"Subject: bad\0value\r\n";
        let invalid_folded_content = b"Subject: good\r\n bad\0value\r\n";
        let leading_fold = b" folded\r\nSubject: Test\r\n";
        let empty_fold = b"Subject: Test\r\n \t\r\n";

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        crate::TEST_ALLOCATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));

        let headers = Headers::parse(valid).unwrap();
        assert_eq!(headers.get("subject"), Some(&b"Test"[..]));
        assert_eq!(headers.iter().count(), 2);
        assert_eq!(
            Headers::parse(empty).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingHeader)
        );
        assert_eq!(
            Headers::parse(missing_colon).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingColon)
        );
        assert_eq!(
            Headers::parse(missing_space).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingSpaceAfterColon)
        );
        assert_eq!(
            Headers::parse(invalid_name).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::InvalidName)
        );
        assert_eq!(
            Headers::parse(invalid_content).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::InvalidContent)
        );
        assert_eq!(
            Headers::parse(invalid_folded_content).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::InvalidContent)
        );
        assert_eq!(
            Headers::parse(leading_fold).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::LeadingFold)
        );
        assert_eq!(
            Headers::parse(empty_fold).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::EmptyFold)
        );

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        assert_eq!(
            crate::TEST_ALLOCATIONS.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn parses_rfc_style_article_shapes() {
        for (input, article_number, message_id, has_headers, has_body) in [
            (
                VALID_ARTICLE_TEXT,
                Some(ArticleNumber(12345)),
                "<test@example.com>",
                true,
                true,
            ),
            (
                VALID_HEAD,
                Some(ArticleNumber(12345)),
                "<test@example.com>",
                true,
                false,
            ),
            (
                VALID_BODY,
                Some(ArticleNumber(12345)),
                "<test@example.com>",
                false,
                true,
            ),
            (
                VALID_STAT,
                Some(ArticleNumber(12345)),
                "<test@example.com>",
                false,
                false,
            ),
            (
                ARTICLE_BY_MSGID,
                Some(ArticleNumber(0)),
                "<msgid@example.com>",
                true,
                true,
            ),
        ] {
            let article = Article::parse(input).unwrap();
            assert_eq!(article.article_number, article_number);
            assert_eq!(article.message_id.as_str(), message_id);
            assert_eq!(article.headers.is_some(), has_headers);
            assert_eq!(article.body.is_some(), has_body);
        }
    }

    #[test]
    fn article_content_accessors_match_rfc_examples() {
        let article = Article::parse(VALID_ARTICLE_TEXT).unwrap();
        let headers = article.headers.unwrap();
        assert_eq!(headers.get("Subject"), Some(&b"Test Article"[..]));
        assert_eq!(headers.get("From"), Some(&b"test@example.com"[..]));
        assert_eq!(headers.get("subject"), headers.get("Subject"));
        assert_eq!(headers.get("FROM"), headers.get("From"));
        assert!(
            article
                .body
                .unwrap()
                .starts_with(b"This is the article body.")
        );
    }

    #[test]
    fn borrowed_nntp_proxy_article_fixtures_preserve_binary_and_body_only_shapes() {
        let yenc = Article::parse(VALID_ARTICLE_YENC).unwrap();
        assert_eq!(yenc.article_number, Some(ArticleNumber(54321)));
        assert_eq!(yenc.message_id.as_str(), "<binary@example.com>");
        assert_eq!(
            yenc.headers.unwrap().get("Subject"),
            Some(&b"test.txt (1/1)"[..])
        );
        let yenc_body = yenc.body.unwrap();
        assert!(yenc_body.starts_with(b"=ybegin"));
        assert!(yenc_body.windows(5).any(|window| window == b"=yend"));

        let body_only = Article::parse(BODY_WITH_HEADERS).unwrap();
        assert!(body_only.headers.is_none());
        assert_eq!(
            body_only.body.as_deref(),
            Some(&b"Subject: Should be ignored\r\n\r\nActual body content\r\n"[..])
        );
    }

    #[test]
    fn article_parse_rejects_rfc_error_shapes() {
        for (input, expected) in [
            (
                b"220 12345 <test@example.com>\r\nSubject: Bad\r\nBody without separator\r\n.\r\n"
                    .as_slice(),
                ArticleParseError::MissingSeparator,
            ),
            (
                b"220 12345 <test@example.com>\r\nSubject: Test\r\n\r\nBody without terminator\r\n"
                    .as_slice(),
                ArticleParseError::MissingTerminator,
            ),
            (
                b"220 12345 <test@example.com>\r\nSubject: Valid\r\nInvalidHeaderNoColon\r\n\r\nBody\r\n.\r\n"
                    .as_slice(),
                ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingColon),
            ),
            (
                b"220 12345 <test@example.com>\r\nSubject:No space\r\n\r\nBody\r\n.\r\n"
                    .as_slice(),
                ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingSpaceAfterColon),
            ),
            (
                b"220 12345 <test@example.com>\r\nSubject: Test\r\n \t\r\n\r\nBody\r\n.\r\n"
                    .as_slice(),
                ArticleParseError::InvalidHeader(InvalidHeaderReason::EmptyFold),
            ),
            (
                b"220 12345 <test@example.com>\r\nSubject: bad\0value\r\n\r\nBody\r\n.\r\n"
                    .as_slice(),
                ArticleParseError::InvalidHeader(InvalidHeaderReason::InvalidContent),
            ),
            (
                b"220 12345 <test@example.com>\r\nSubject: Test\r\n\r\nBody\0bad\r\n.\r\n"
                    .as_slice(),
                ArticleParseError::InvalidBody,
            ),
            (
                b"222 12345 <test@example.com>\r\nBody\0bad\r\n.\r\n".as_slice(),
                ArticleParseError::InvalidBody,
            ),
            (
                b"220 12345 <test@example.com>\r\n\r\nBody\r\n.\r\n".as_slice(),
                ArticleParseError::MissingSeparator,
            ),
            (
                b"221 12345 <test@example.com>\r\n.\r\n".as_slice(),
                ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingHeader),
            ),
            (
                b"221 12345 <test@example.com>\r\nSubject: Test\r\n\r\nThis body should not be here\r\n.\r\n"
                    .as_slice(),
                ArticleParseError::UnexpectedBody,
            ),
            (
                b"430 No such article\r\n".as_slice(),
                ArticleParseError::InvalidStatusCode(430),
            ),
            (
                b"bad status line\r\n".as_slice(),
                ArticleParseError::InvalidStatusPrefix,
            ),
        ] {
            let err = Article::parse(input).unwrap_err();
            match (err, expected) {
                (ArticleParseError::InvalidHeader(_), ArticleParseError::InvalidHeader(_)) => {}
                (actual, expected) => assert_eq!(actual, expected),
            }
        }
    }

    #[test]
    fn folded_headers_and_large_headers_parse() {
        let folded = Article::parse(FOLDED_HEADER).unwrap();
        let headers = folded.headers.unwrap();
        // RFC 5322 section 2.2.3: a field value may be folded across CRLF followed by
        // whitespace and is interpreted as one unfolded logical header field.
        assert_eq!(
            headers.get("Subject"),
            Some(&b"This is a long subject that continues on the next line"[..])
        );
        assert_eq!(headers.iter().count(), 2);

        let long_header = format!(
            "220 123 <test@example.com>\r\nSubject: {}\r\n\r\nBody\r\n.\r\n",
            "A".repeat(10000)
        );
        let article = Article::parse(long_header.as_bytes()).unwrap();
        assert_eq!(
            article.headers.unwrap().get("Subject").unwrap().len(),
            10000
        );
    }

    #[test]
    fn article_body_edge_cases_and_zero_copy_hold() {
        let empty_body = Article::parse(b"222 123 <test@example.com>\r\n.\r\n").unwrap();
        assert_eq!(empty_body.body.as_deref(), Some(&b""[..]));

        let mut binary_article = b"222 123 <test@example.com>\r\n".to_vec();
        binary_article.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC, 0xFB]);
        binary_article.extend_from_slice(b"\r\n.\r\n");
        let binary = Article::parse(&binary_article).unwrap();
        assert_eq!(
            binary.body.as_deref().unwrap(),
            &[0xFF, 0xFE, 0xFD, 0xFC, 0xFB, b'\r', b'\n']
        );

        let article = Article::parse(VALID_ARTICLE_TEXT).unwrap();
        let headers_ptr = article.headers.unwrap().as_bytes().as_ptr() as usize;
        let body_ptr = article.body.unwrap().as_ptr() as usize;
        let original_start = VALID_ARTICLE_TEXT.as_ptr() as usize;
        let original_end = original_start + VALID_ARTICLE_TEXT.len();

        assert!((original_start..original_end).contains(&headers_ptr));
        assert!((original_start..original_end).contains(&body_ptr));
    }

    #[test]
    fn compatibility_fixture_matrix_records_strict_article_contract() {
        let fixtures = [
            (VALID_ARTICLE_TEXT, Some(ArticleNumber(12345)), true, true),
            (VALID_HEAD, Some(ArticleNumber(12345)), true, false),
            (VALID_BODY, Some(ArticleNumber(12345)), false, true),
            (VALID_STAT, Some(ArticleNumber(12345)), false, false),
            (
                b"222 0 <empty@example.com>\r\n.\r\n".as_slice(),
                Some(ArticleNumber(0)),
                false,
                true,
            ),
        ];

        for (wire, article_number, has_headers, has_body) in fixtures {
            let article = Article::parse(wire).expect("nntpbench fixture remains accepted");
            assert_eq!(article.article_number, article_number);
            assert_eq!(article.headers.is_some(), has_headers);
            assert_eq!(article.body.is_some(), has_body);
        }
    }

    #[test]
    fn compatibility_fixture_matrix_keeps_strict_article_number_behavior() {
        for number in [b"not-a-number".as_slice(), b"18446744073709551616"] {
            let wire = [
                b"220 ".as_slice(),
                number,
                b" <fixture@example.com>\r\nSubject: fixture\r\n\r\nbody\r\n.\r\n",
            ]
            .concat();

            assert_eq!(
                Article::parse(&wire),
                Err(ArticleParseError::InvalidArticleNumber)
            );
        }
    }

    #[test]
    fn compatibility_fixture_matrix_normalizes_only_strict_article_sections() {
        let folded =
            b"220 0 <folded@example.com>\r\nSubject: first\r\n second\r\n\r\nbody\r\n.\r\n";
        let folded_article = Article::parse(folded).unwrap();
        assert_eq!(
            folded_article.headers.unwrap().get("Subject"),
            Some(&b"first second"[..])
        );

        let stuffed = b"222 0 <stuffed@example.com>\r\n..wire-dot\r\n.\r\n";
        let stuffed_article = Article::parse(stuffed).unwrap();
        assert_eq!(
            stuffed_article.body,
            Some(Cow::Borrowed(&b".wire-dot\r\n"[..]))
        );

        let binary = b"222 0 <binary@example.com>\r\nbinary\0body\r\n.\r\n";
        assert_eq!(Article::parse(binary), Err(ArticleParseError::InvalidBody));

        let bare_lf = b"222 0 <bare@example.com>\r\nbody\nnext\r\n.\r\n";
        assert!(matches!(
            Article::parse(bare_lf),
            Err(ArticleParseError::InvalidBody)
        ));
    }
}
