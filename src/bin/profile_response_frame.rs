use std::hint::black_box;

use nntpbench::RequestKind;
use nntpbench::protocol::{ResponseFrame, ResponseFrameParse};

const SINGLE_LINE_RESPONSE: &[u8] = b"223 42 <bench@example.com> article retrieved\r\n";
const ERROR_RESPONSE: &[u8] = b"430 no article with that message-id\r\n";
const BODY_RESPONSE: &[u8] = b"222 42 <bench@example.com> body follows\r\n\
This is the benchmark body.\r\n\
It has multiple lines.\r\n\
.\r\n";
const EMPTY_CAPABILITIES_RESPONSE: &[u8] = b"101 capability list follows\r\n.\r\n";

fn main() {
    let iterations = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100_000_000);
    let mut checksum = 0usize;

    for index in 0..iterations {
        let (kind, frame) = match index & 3 {
            0 => (RequestKind::Stat, SINGLE_LINE_RESPONSE),
            1 => (RequestKind::Article, ERROR_RESPONSE),
            2 => (RequestKind::Body, BODY_RESPONSE),
            _ => (RequestKind::Capabilities, EMPTY_CAPABILITIES_RESPONSE),
        };

        match ResponseFrame::parse(black_box(kind), black_box(frame)) {
            ResponseFrameParse::Complete(response) => {
                checksum = checksum.wrapping_add(response.consumed());
                checksum = checksum.wrapping_add(usize::from(response.status().as_u16()));
                checksum = checksum.wrapping_add(response.content().len());
            }
            ResponseFrameParse::NeedMore | ResponseFrameParse::Invalid => {
                checksum = checksum.wrapping_add(1);
            }
        }
    }

    println!("{checksum}");
}
