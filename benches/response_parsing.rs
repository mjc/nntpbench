//! Benchmarks for hot-path response parsing helpers.
//!
//! Borrowed in spirit from `nntp-proxy`'s parser microbenches: keep direct
//! pressure on status-code parsing and complete-response terminator detection so
//! performance-sensitive helper changes stay measurable.

use divan::{Bencher, black_box};
use nntpbench::protocol::ResponseFrame;
use nntpbench::terminator::{MultilineTerminatorDetector, TerminatorStatus};
use nntpbench::{Article, Headers, RequestKind, StatusCode};

fn main() {
    divan::main();
}

mod status_code_parsing {
    use super::{Bencher, StatusCode, black_box};

    const RESPONSES: &[&[u8]] = &[
        b"200 Ready\r\n",
        b"220 0 12345 <msgid@example.com>\r\n",
        b"381 Password required\r\n",
        b"224 Overview information follows\r\n",
    ];
    const LONG_RESPONSE: &[u8] = b"200 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\r\n";

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn parse_prefixes(bencher: Bencher) {
        bencher.bench(|| {
            for response in RESPONSES {
                black_box(StatusCode::parse(black_box(*response)));
            }
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn parse_long_prefix(bencher: Bencher) {
        bencher.bench(|| black_box(StatusCode::parse(black_box(LONG_RESPONSE))));
    }
}

mod terminator_finding {
    use super::{Bencher, MultilineTerminatorDetector, TerminatorStatus, black_box};

    #[inline]
    fn find_terminator(data: &[u8]) -> Option<usize> {
        let detector = MultilineTerminatorDetector::default();
        match detector.detect_terminator(data) {
            TerminatorStatus::FoundAt(pos) => Some(pos),
            TerminatorStatus::NotFound => None,
        }
    }

    const SMALL_RESPONSE: &[u8] =
        b"220 0 12345 <msgid@example.com>\r\nArticle content here\r\nMore lines\r\n.\r\n";

    const MEDIUM_RESPONSE: &[u8] = b"220 0 12345 <msgid@example.com>\r\n\
Header: value\r\n\
Another-Header: another value\r\n\
\r\n\
Article body line 1\r\n\
Article body line 2\r\n\
Article body line 3\r\n\
Article body line 4\r\n\
Article body line 5\r\n\
.\r\n";

    const SPANNING_TAIL: &[u8] = b"prefix\r";
    const SPANNING_CHUNK: &[u8] = b"\n.\r\npayload";

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn complete_small_response(bencher: Bencher) {
        bencher.bench(|| black_box(find_terminator(black_box(SMALL_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn complete_medium_response(bencher: Bencher) {
        bencher.bench(|| black_box(find_terminator(black_box(MEDIUM_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn spanning_boundary_response(bencher: Bencher) {
        let mut detector = MultilineTerminatorDetector::default();
        detector.update(SPANNING_TAIL);
        bencher.bench(|| black_box(detector.detect_terminator(black_box(SPANNING_CHUNK))));
    }
}

mod article_parsing {
    use super::{Article, Bencher, black_box};

    const ARTICLE_RESPONSE: &[u8] = b"220 42 <bench@example.com>\r\n\
Subject: Benchmark\r\n\
From: bench@example.com\r\n\
Message-ID: <bench@example.com>\r\n\
\r\n\
This is the benchmark body.\r\n\
It has multiple lines.\r\n\
.\r\n";

    const HEAD_RESPONSE: &[u8] = b"221 42 <bench@example.com>\r\n\
Subject: Benchmark\r\n\
From: bench@example.com\r\n\
Message-ID: <bench@example.com>\r\n\
.\r\n";

    const BODY_RESPONSE: &[u8] = b"222 42 <bench@example.com>\r\n\
This is the benchmark body.\r\n\
It has multiple lines.\r\n\
.\r\n";

    const STAT_RESPONSE: &[u8] = b"223 42 <bench@example.com>\r\n.\r\n";
    const LONG_ARTICLE_RESPONSE: &[u8] = b"220 42 <aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa@example.com>\r\nSubject: Benchmark\r\nFrom: bench@example.com\r\nMessage-ID: <bench@example.com>\r\n\r\nBody\r\n.\r\n";
    const EMPTY_ARTICLE_RESPONSE: &[u8] = b"220 42 <bench@example.com>\r\n\r\n\r\n.\r\n";
    const EMPTY_HEAD_RESPONSE: &[u8] = b"221 42 <bench@example.com>\r\n.\r\n";
    const EMPTY_BODY_RESPONSE: &[u8] = b"222 42 <bench@example.com>\r\n.\r\n";

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn article_220(bencher: Bencher) {
        bencher.bench(|| black_box(Article::parse(black_box(ARTICLE_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn head_221(bencher: Bencher) {
        bencher.bench(|| black_box(Article::parse(black_box(HEAD_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn body_222(bencher: Bencher) {
        bencher.bench(|| black_box(Article::parse(black_box(BODY_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn stat_223(bencher: Bencher) {
        bencher.bench(|| black_box(Article::parse(black_box(STAT_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn article_long_first_line(bencher: Bencher) {
        bencher.bench(|| black_box(Article::parse(black_box(LONG_ARTICLE_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn article_empty_content(bencher: Bencher) {
        bencher.bench(|| black_box(Article::parse(black_box(EMPTY_ARTICLE_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn head_empty_headers(bencher: Bencher) {
        bencher.bench(|| black_box(Article::parse(black_box(EMPTY_HEAD_RESPONSE))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn body_empty_content(bencher: Bencher) {
        bencher.bench(|| black_box(Article::parse(black_box(EMPTY_BODY_RESPONSE))));
    }
}

mod response_frame_parsing {
    use super::{Bencher, RequestKind, ResponseFrame, black_box};

    const SINGLE_LINE_RESPONSE: &[u8] = b"223 42 <bench@example.com> article retrieved\r\n";
    const ERROR_RESPONSE: &[u8] = b"430 no article with that message-id\r\n";
    const BODY_RESPONSE: &[u8] = b"222 42 <bench@example.com> body follows\r\n\
This is the benchmark body.\r\n\
It has multiple lines.\r\n\
.\r\n";
    const EMPTY_CAPABILITIES_RESPONSE: &[u8] = b"101 capability list follows\r\n.\r\n";

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn single_line_stat(bencher: Bencher) {
        bencher.bench(|| {
            black_box(ResponseFrame::parse(
                black_box(RequestKind::Stat),
                black_box(SINGLE_LINE_RESPONSE),
            ))
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn single_line_error(bencher: Bencher) {
        bencher.bench(|| {
            black_box(ResponseFrame::parse(
                black_box(RequestKind::Article),
                black_box(ERROR_RESPONSE),
            ))
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn multiline_body(bencher: Bencher) {
        bencher.bench(|| {
            black_box(ResponseFrame::parse(
                black_box(RequestKind::Body),
                black_box(BODY_RESPONSE),
            ))
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn empty_multiline_capabilities(bencher: Bencher) {
        bencher.bench(|| {
            black_box(ResponseFrame::parse(
                black_box(RequestKind::Capabilities),
                black_box(EMPTY_CAPABILITIES_RESPONSE),
            ))
        });
    }
}

mod header_parsing {
    use super::{Bencher, Headers, black_box};

    const HEADER_BLOCK: &[u8] = b"Subject: Benchmark subject\r\n\
From: Bench User <bench@example.com>\r\n\
Date: Fri, 16 May 2026 12:00:00 +0000\r\n\
Message-ID: <bench@example.com>\r\n\
X-Trace: abcdef0123456789\r\n";

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn parse_headers(bencher: Bencher) {
        bencher.bench(|| black_box(Headers::parse(black_box(HEADER_BLOCK))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn get_subject_case_insensitive(bencher: Bencher) {
        let headers = Headers::parse(HEADER_BLOCK).unwrap();
        bencher.bench(|| black_box(headers.get(black_box("subject"))));
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn iterate_all_headers(bencher: Bencher) {
        let headers = Headers::parse(HEADER_BLOCK).unwrap();
        bencher.bench(|| black_box(headers.iter().count()));
    }
}
