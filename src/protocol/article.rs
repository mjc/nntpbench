//! Zero-copy NNTP article parsing borrowed and adapted from `nntp-proxy`.

use std::fmt;

use super::{InvalidMessageId, MessageId, StatusCode};
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
    UnexpectedBody,
    BufferTooShort,
    InvalidMessageId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidHeaderReason {
    LeadingFold,
    MissingColon,
    EmptyName,
    InvalidName,
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
                    .find(|(expected_name, _)| expected_name == name)
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
            prop_assert_eq!(article.body, Some(body.as_bytes()));
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
            prop_assert_eq!(parsed_body.body, Some(body.as_bytes()));

            let stat_frame = format!("223 {article_number} {message_id}\r\n");
            let stat = Article::parse(stat_frame.as_bytes()).unwrap();
            prop_assert_eq!(stat.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(stat.article_number, Some(ArticleNumber::from(article_number as u64)));
            prop_assert!(stat.headers.is_none());
            prop_assert!(stat.body.is_none());
        }

        #[test]
        fn generated_response_first_lines_preserve_message_id_and_optional_number(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            include_number in any::<bool>(),
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
            let first_line = if include_number {
                format!("{article_number} {message_id}")
            } else {
                message_id.clone()
            };
            let expected_number = include_number.then_some(ArticleNumber::from(article_number as u64));
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
        fn generated_empty_content_shapes_parse_consistently(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            include_number in any::<bool>(),
        ) {
            let first_line = if include_number {
                format!("{article_number} {message_id}")
            } else {
                message_id.clone()
            };
            let expected_number = include_number.then_some(ArticleNumber::from(article_number as u64));

            let article_frame = format!("220 {first_line}\r\n\r\n\r\n.\r\n");
            let article = Article::parse(article_frame.as_bytes()).unwrap();
            prop_assert_eq!(article.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(article.article_number, expected_number);
            prop_assert_eq!(article.headers.unwrap().iter().count(), 0);
            prop_assert_eq!(article.body, Some(&b""[..]));

            let head_frame = format!("221 {first_line}\r\n.\r\n");
            let head = Article::parse(head_frame.as_bytes()).unwrap();
            prop_assert_eq!(head.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(head.article_number, expected_number);
            prop_assert_eq!(head.headers.unwrap().iter().count(), 0);
            prop_assert!(head.body.is_none());

            let body_frame = format!("222 {first_line}\r\n.\r\n");
            let parsed_body = Article::parse(body_frame.as_bytes()).unwrap();
            prop_assert_eq!(parsed_body.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(parsed_body.article_number, expected_number);
            prop_assert!(parsed_body.headers.is_none());
            prop_assert_eq!(parsed_body.body, Some(&b""[..]));
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
                    ArticleParseError::InvalidMessageId
                );
            }
        }

        #[test]
        fn stat_accepts_minimal_and_rejects_dot_terminated_body(
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            include_number in any::<bool>(),
        ) {
            let first_line = if include_number {
                format!("{article_number} {message_id}")
            } else {
                message_id.clone()
            };
            let minimal = format!("223 {first_line}\r\n");
            let dot_terminated = format!("223 {first_line}\r\n.\r\n");

            prop_assert!(Article::parse(minimal.as_bytes()).is_ok());
            prop_assert_eq!(
                Article::parse(dot_terminated.as_bytes()).unwrap_err(),
                ArticleParseError::UnexpectedBody
            );
        }

        #[test]
        fn generated_response_first_lines_treat_overflowing_article_numbers_as_missing(
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

            let article = Article::parse(article_frame.as_bytes()).unwrap();
            prop_assert_eq!(article.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(article.article_number, None);

            let head = Article::parse(head_frame.as_bytes()).unwrap();
            prop_assert_eq!(head.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(head.article_number, None);

            let parsed_body = Article::parse(body_frame.as_bytes()).unwrap();
            prop_assert_eq!(parsed_body.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(parsed_body.article_number, None);

            let stat = Article::parse(stat_frame.as_bytes()).unwrap();
            prop_assert_eq!(stat.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(stat.article_number, None);
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
        fn parse_first_line_extracts_optional_article_numbers_and_ignores_trailing_text(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            message_id in message_id_strategy(),
            article_number in 0_u32..=999_999,
            include_number in any::<bool>(),
            suffix in first_line_suffix_strategy(),
        ) {
            let line = if include_number {
                format!("{status} {article_number} {message_id}{suffix}")
            } else {
                format!("{status} {message_id}")
            };

            let (parsed_message_id, parsed_number) = parse_first_line(line.as_bytes()).unwrap();
            let line_start = line.as_ptr() as usize;
            let line_end = line_start + line.len();
            prop_assert_eq!(parsed_message_id.as_str(), message_id.as_str());
            prop_assert!((line_start..line_end).contains(&(parsed_message_id.as_str().as_ptr() as usize)));
            prop_assert_eq!(
                parsed_number,
                include_number.then_some(ArticleNumber::from(article_number as u64))
            );
        }

        #[test]
        fn parse_first_line_rejects_trailing_text_without_article_number(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            message_id in message_id_strategy(),
            suffix in string_regex(" [A-Za-z0-9._-]{1,20}").unwrap(),
        ) {
            let line = format!("{status} {message_id}{suffix}");
            prop_assert_eq!(
                parse_first_line(line.as_bytes()).unwrap_err(),
                ArticleParseError::InvalidMessageId
            );
        }

        #[test]
        fn parse_first_line_treats_non_numeric_middle_tokens_as_missing_article_numbers(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            invalid_number in invalid_article_number_token_strategy(),
            message_id in message_id_strategy(),
            suffix in first_line_suffix_strategy(),
        ) {
            let line = format!("{status} {invalid_number} {message_id}{suffix}");

            let (parsed_message_id, parsed_number) = parse_first_line(line.as_bytes()).unwrap();
            let line_start = line.as_ptr() as usize;
            let line_end = line_start + line.len();
            prop_assert_eq!(parsed_message_id.as_str(), message_id.as_str());
            prop_assert!((line_start..line_end).contains(&(parsed_message_id.as_str().as_ptr() as usize)));
            prop_assert_eq!(parsed_number, None);
        }

        #[test]
        fn parse_first_line_treats_overflowing_article_numbers_as_missing(
            status in prop_oneof![Just("220"), Just("221"), Just("222"), Just("223")],
            overflowing_number in overflowing_article_number_token_strategy(),
            message_id in message_id_strategy(),
            suffix in first_line_suffix_strategy(),
        ) {
            let line = format!("{status} {overflowing_number} {message_id}{suffix}");

            let (parsed_message_id, parsed_number) = parse_first_line(line.as_bytes()).unwrap();
            let line_start = line.as_ptr() as usize;
            let line_end = line_start + line.len();
            prop_assert_eq!(parsed_message_id.as_str(), message_id.as_str());
            prop_assert!((line_start..line_end).contains(&(parsed_message_id.as_str().as_ptr() as usize)));
            prop_assert_eq!(parsed_number, None);
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
            mut body in vec(dangerous_wire_bytes(), 0..48),
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
            prop_assert_eq!(parsed.message_id.as_str(), message_id.as_str());
            prop_assert_eq!(parsed.article_number, Some(ArticleNumber::from(article_number as u64)));
            prop_assert_eq!(parsed.body, Some(expected_body.as_slice()));
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
            folded_values in vec(header_value_strategy(), 1..=3),
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
            prop_assert_eq!(items.len(), 2);
            prop_assert_eq!(items[0].0, first_name.as_bytes());
            prop_assert_eq!(
                items[0].1,
                first_value.trim_start_matches([' ', '\t']).as_bytes()
            );
            prop_assert_eq!(items[1].0, second_name.as_bytes());
            prop_assert_eq!(
                items[1].1,
                second_value.trim_start_matches([' ', '\t']).as_bytes()
            );
            prop_assert_eq!(
                headers.get(&first_name),
                Some(first_value.trim_start_matches([' ', '\t']).as_bytes())
            );
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
            let article_headers = article.headers.unwrap().as_bytes();
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
            let head_headers = head.headers.unwrap().as_bytes();
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
            Self::UnexpectedBody => write!(f, "response contains unexpected body"),
            Self::BufferTooShort => write!(f, "buffer too short to contain valid response"),
            Self::InvalidMessageId => write!(f, "invalid message-id"),
        }
    }
}

impl std::error::Error for ArticleParseError {}

impl fmt::Display for InvalidHeaderReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeadingFold => write!(f, "header cannot start with folding whitespace"),
            Self::MissingColon => write!(f, "header missing colon"),
            Self::EmptyName => write!(f, "empty header name"),
            Self::InvalidName => write!(f, "invalid character in header name"),
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
        let lookup = name.as_bytes();
        let mut pos = 0;

        while pos < self.data.len() {
            let line_end = strict_crlf_line_content_end_from(self.data, pos)?;
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

    /// Parse a response frame whose multiline content boundary was already found.
    ///
    /// This is for callers that already performed RFC 3977 section 3.1.1
    /// dot-terminator framing and can pass the payload range directly.
    pub(crate) fn parse_framed(
        buf: &'a [u8],
        content_start: usize,
        content_end: usize,
    ) -> Result<Self, ArticleParseError> {
        if content_start > content_end || content_end > buf.len() {
            return Err(ArticleParseError::BufferTooShort);
        }

        let first_line_end =
            strict_crlf_line_content_end_from(buf, 0).ok_or(ArticleParseError::BufferTooShort)?;
        if first_line_end + 2 != content_start {
            return Err(ArticleParseError::BufferTooShort);
        }

        let status_code = parse_status_code(buf)?;
        match status_code {
            220 => Self::parse_article_framed(buf, first_line_end, content_start, content_end),
            221 => Self::parse_head_framed(buf, first_line_end, content_start, content_end),
            222 => Self::parse_body_framed(buf, first_line_end, content_start, content_end),
            223 => Self::parse_stat_framed(buf, first_line_end, content_start, content_end),
            _ => Err(ArticleParseError::InvalidStatusCode(status_code)),
        }
    }

    fn parse_article(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end =
            strict_crlf_line_content_end_from(buf, 0).ok_or(ArticleParseError::BufferTooShort)?;
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        let content_start = first_line_end + 2;
        let separator_pos = find_blank_line(buf, content_start)?;
        let headers = Some(Headers::parse(&buf[content_start..separator_pos + 2])?);
        let body_start = separator_pos + 4;
        let body_end = find_article_content_end(buf, body_start)
            .ok_or(ArticleParseError::MissingTerminator)?;
        let body_start = unstuff_leading_dot(buf, body_start, body_end);

        Ok(Self {
            message_id,
            article_number,
            headers,
            body: Some(&buf[body_start..body_end]),
        })
    }

    fn parse_head(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end =
            strict_crlf_line_content_end_from(buf, 0).ok_or(ArticleParseError::BufferTooShort)?;
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
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
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        let body_start = first_line_end + 2;
        let body_end = find_article_content_end(buf, body_start)
            .ok_or(ArticleParseError::MissingTerminator)?;
        let body_start = unstuff_leading_dot(buf, body_start, body_end);

        Ok(Self {
            message_id,
            article_number,
            headers: None,
            body: Some(&buf[body_start..body_end]),
        })
    }

    fn parse_stat(buf: &'a [u8]) -> Result<Self, ArticleParseError> {
        let first_line_end =
            strict_crlf_line_content_end_from(buf, 0).ok_or(ArticleParseError::BufferTooShort)?;
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
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

    fn parse_article_framed(
        buf: &'a [u8],
        first_line_end: usize,
        content_start: usize,
        content_end: usize,
    ) -> Result<Self, ArticleParseError> {
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        let separator_pos = find_blank_line(&buf[..content_end], content_start)?;
        let headers = Some(Headers::parse(&buf[content_start..separator_pos + 2])?);
        let body_start = separator_pos + 4;
        if body_start > content_end {
            return Err(ArticleParseError::BufferTooShort);
        }
        let body_start = unstuff_leading_dot(buf, body_start, content_end);

        Ok(Self {
            message_id,
            article_number,
            headers,
            body: Some(&buf[body_start..content_end]),
        })
    }

    fn parse_head_framed(
        buf: &'a [u8],
        first_line_end: usize,
        content_start: usize,
        content_end: usize,
    ) -> Result<Self, ArticleParseError> {
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        if find_blank_line(&buf[..content_end], content_start).is_ok() {
            return Err(ArticleParseError::UnexpectedBody);
        }

        Ok(Self {
            message_id,
            article_number,
            headers: Some(Headers::parse(&buf[content_start..content_end])?),
            body: None,
        })
    }

    fn parse_body_framed(
        buf: &'a [u8],
        first_line_end: usize,
        content_start: usize,
        content_end: usize,
    ) -> Result<Self, ArticleParseError> {
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        let content_start = unstuff_leading_dot(buf, content_start, content_end);

        Ok(Self {
            message_id,
            article_number,
            headers: None,
            body: Some(&buf[content_start..content_end]),
        })
    }

    fn parse_stat_framed(
        buf: &'a [u8],
        first_line_end: usize,
        content_start: usize,
        content_end: usize,
    ) -> Result<Self, ArticleParseError> {
        let (message_id, article_number) = parse_first_line(&buf[..first_line_end])?;
        if content_start != content_end {
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

fn unstuff_leading_dot(buf: &[u8], start: usize, end: usize) -> usize {
    if start < end && buf[start] == b'.' {
        start + 1
    } else {
        start
    }
}

fn parse_status_code(buf: &[u8]) -> Result<u16, ArticleParseError> {
    StatusCode::parse(buf)
        .map(StatusCode::as_u16)
        .ok_or(ArticleParseError::InvalidStatusPrefix)
}

fn parse_first_line(
    line: &[u8],
) -> Result<(MessageId<'_>, Option<ArticleNumber>), ArticleParseError> {
    let first_space = memchr::memchr(b' ', line).ok_or(ArticleParseError::InvalidStatusPrefix)?;
    if first_space != 3 {
        return Err(ArticleParseError::InvalidStatusPrefix);
    }
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

fn find_blank_line(buf: &[u8], start: usize) -> Result<usize, ArticleParseError> {
    memchr::memmem::find(&buf[start..], b"\r\n\r\n")
        .map(|pos| start + pos)
        .ok_or(ArticleParseError::MissingSeparator)
}

fn validate_headers(data: &[u8]) -> Result<(), ArticleParseError> {
    let mut pos = 0;
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
            pos = line_end + 2;
            continue;
        }

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

        pos = line_end + 2;
    }

    Ok(())
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
        let missing_colon = b"Invalid Header\r\n";
        let invalid_name = b"Invalid Header: value\r\n";
        let leading_fold = b" folded\r\nSubject: Test\r\n";

        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(false));
        crate::TEST_ALLOCATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
        crate::COUNT_TEST_ALLOCATIONS.with(|enabled| enabled.set(true));

        let headers = Headers::parse(valid).unwrap();
        assert_eq!(headers.get("subject"), Some(&b"Test"[..]));
        assert_eq!(headers.iter().count(), 2);
        assert_eq!(
            Headers::parse(missing_colon).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::MissingColon)
        );
        assert_eq!(
            Headers::parse(invalid_name).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::InvalidName)
        );
        assert_eq!(
            Headers::parse(leading_fold).unwrap_err(),
            ArticleParseError::InvalidHeader(InvalidHeaderReason::LeadingFold)
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
            body_only.body,
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
        assert_eq!(headers.get("Subject"), Some(&b"This is a long subject"[..]));
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
        assert_eq!(empty_body.body, Some(&b""[..]));

        let mut binary_article = b"222 123 <test@example.com>\r\n".to_vec();
        binary_article.extend_from_slice(&[0xFF, 0xFE, 0xFD, 0xFC, 0xFB]);
        binary_article.extend_from_slice(b"\r\n.\r\n");
        let binary = Article::parse(&binary_article).unwrap();
        assert_eq!(
            binary.body.unwrap(),
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
}
