use nntpbench::TERMINATOR;

#[derive(Clone, Copy)]
pub enum ArticleVariant {
    PlainBody,
    DotStuffedBody,
    FoldedHeaders,
}

pub fn article_response(target_body_bytes: usize, variant: ArticleVariant) -> Vec<u8> {
    let mut response = Vec::with_capacity(target_body_bytes + 256);
    match variant {
        ArticleVariant::FoldedHeaders => {
            response.extend_from_slice(b"220 42 <bench@example.com> article follows\r\n".as_slice())
        }
        ArticleVariant::PlainBody | ArticleVariant::DotStuffedBody => {
            response.extend_from_slice(b"222 42 <bench@example.com> body follows\r\n")
        }
    }

    if matches!(variant, ArticleVariant::FoldedHeaders) {
        response.extend_from_slice(
            b"Subject: benchmark article\r\n X-Benchmark-Folded: continuation\r\n".as_slice(),
        );
        response.extend_from_slice(
            b"From: bench@example.com\r\nMessage-ID: <bench@example.com>\r\n\r\n",
        );
    }

    let mut body_bytes = 0;
    let mut line_number = 0;
    while body_bytes < target_body_bytes {
        let dot_stuffed =
            matches!(variant, ArticleVariant::DotStuffedBody) && line_number % 17 == 0;
        let line = if dot_stuffed {
            b"..dot-stuffed benchmark payload line\r\n".as_slice()
        } else {
            b"benchmark payload line with deterministic bytes\r\n".as_slice()
        };
        response.extend_from_slice(line);
        body_bytes += line.len();
        line_number += 1;
    }

    response.extend_from_slice(TERMINATOR);
    response
}

pub fn body_response(target_body_bytes: usize, dot_stuffed: bool) -> Vec<u8> {
    article_response(
        target_body_bytes,
        if dot_stuffed {
            ArticleVariant::DotStuffedBody
        } else {
            ArticleVariant::PlainBody
        },
    )
}
