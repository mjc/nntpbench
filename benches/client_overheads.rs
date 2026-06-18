//! Focused benchmarks for client per-request overheads.
//!
//! These isolate costs that are hard to read from the end-to-end client profile:
//! request construction, request wire serialization, streaming response decoding,
//! and Tokio channel/future handoffs.

use divan::{Bencher, black_box};
use nntpbench::client::{bench_streaming_decode_response, bench_write_request_wire_to_sink};
use nntpbench::{
    ClientCommandMix, MessageId, Request, RequestKind, bench_client_request_for_command,
    bench_client_segment_request_for_command,
};
use std::sync::Arc;
use tokio::runtime::Builder;

fn main() {
    divan::main();
}

const BODY_RESPONSE: &[u8] = b"222 42 <bench@example.com> body follows\r\n\
This is the benchmark body.\r\n\
It has multiple lines.\r\n\
.\r\n";

const COMPACT_BODY_RESPONSE: &[u8] = b"222 42 <bench@example.com> body follows\r\n.\r\n";

fn runtime() -> tokio::runtime::Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

mod request_construction {
    use super::{
        Arc, Bencher, ClientCommandMix, MessageId, bench_client_request_for_command,
        bench_client_segment_request_for_command, black_box,
    };

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn numeric_article(bencher: Bencher) {
        bencher.bench_local(|| {
            black_box(bench_client_request_for_command(
                black_box(42),
                black_box(42),
                black_box(ClientCommandMix::Article),
            ))
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn numeric_body(bencher: Bencher) {
        bencher.bench_local(|| {
            black_box(bench_client_request_for_command(
                black_box(42),
                black_box(42),
                black_box(ClientCommandMix::Body),
            ))
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn synthetic_message_id(bencher: Bencher) {
        bencher.bench_local(|| {
            black_box(bench_client_request_for_command(
                black_box(0),
                black_box(0),
                black_box(ClientCommandMix::Article),
            ))
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn shared_segment_article(bencher: Bencher) {
        let segment =
            MessageId::from_shared(Arc::<str>::from("<bench.42@nntpbench.local>")).unwrap();
        bencher.bench_local(|| {
            black_box(bench_client_segment_request_for_command(
                black_box(segment.clone()),
                black_box(ClientCommandMix::Article),
            ))
        });
    }
}

mod request_wire {
    use super::{Bencher, Request, bench_write_request_wire_to_sink, black_box, runtime};
    use std::cell::RefCell;

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn sync_numeric_article_to_vec(bencher: Bencher) {
        let request = Request::article_number(42).unwrap();
        let output = RefCell::new(Vec::with_capacity(64));
        bencher.bench_local(|| {
            let mut output = output.borrow_mut();
            output.clear();
            request.write_wire_to(black_box(&mut *output));
            black_box(output.len())
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn sync_message_id_article_to_vec(bencher: Bencher) {
        let request = Request::article("<bench.42@nntpbench.local>").unwrap();
        let output = RefCell::new(Vec::with_capacity(64));
        bencher.bench_local(|| {
            let mut output = output.borrow_mut();
            output.clear();
            request.write_wire_to(black_box(&mut *output));
            black_box(output.len())
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn async_numeric_article_to_sink(bencher: Bencher) {
        let rt = runtime();
        let request = Request::article_number(42).unwrap();
        bencher.bench_local(|| {
            rt.block_on(async {
                bench_write_request_wire_to_sink(black_box(&request))
                    .await
                    .unwrap();
            });
            black_box(())
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn async_message_id_article_to_sink(bencher: Bencher) {
        let rt = runtime();
        let request = Request::article("<bench.42@nntpbench.local>").unwrap();
        bencher.bench_local(|| {
            rt.block_on(async {
                bench_write_request_wire_to_sink(black_box(&request))
                    .await
                    .unwrap();
            });
            black_box(())
        });
    }
}

mod streaming_decode {
    use super::{
        BODY_RESPONSE, Bencher, COMPACT_BODY_RESPONSE, RequestKind,
        bench_streaming_decode_response, black_box,
    };

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn compact_body_response(bencher: Bencher) {
        bencher.bench(|| {
            black_box(bench_streaming_decode_response(
                black_box(RequestKind::Body),
                black_box(COMPACT_BODY_RESPONSE),
            ))
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn body_response(bencher: Bencher) {
        bencher.bench(|| {
            black_box(bench_streaming_decode_response(
                black_box(RequestKind::Body),
                black_box(BODY_RESPONSE),
            ))
        });
    }
}
