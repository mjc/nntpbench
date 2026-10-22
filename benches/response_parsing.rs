//! Benchmarks for hot-path response parsing helpers.
//!
//! Borrowed in spirit from `nntp-proxy`'s parser microbenches: keep direct
//! pressure on status-code and header parsing.

use divan::{Bencher, black_box};
use nntpbench::protocol::ResponseFrame;
use nntpbench::{Headers, RequestKind, StatusCode};

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

mod response_frame_parsing {
    use super::{Bencher, RequestKind, ResponseFrame, black_box};

    const SINGLE_LINE_RESPONSE: &[u8] = b"223 42 <bench@example.com> article retrieved\r\n";
    const ERROR_RESPONSE: &[u8] = b"430 no article with that message-id\r\n";
    const EMPTY_CAPABILITIES_RESPONSE: &[u8] = b"101 capability list follows\r\nVERSION 2\r\n.\r\n";

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
