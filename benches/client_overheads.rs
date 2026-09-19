//! Focused benchmarks for client per-request overheads and raw request probes.
//!
//! These isolate costs that are hard to read from the end-to-end client profile:
//! request construction, request wire serialization, streaming response decoding,
//! and Tokio channel/future handoffs.

use divan::{Bencher, black_box};
use nntpbench::client::{
    bench_article_validation_and_materialization, bench_article_validation_and_two_parses,
    bench_owned_article_accessor_parse, bench_owned_response_from_bytes,
    bench_pending_read_capacity, bench_public_response_decode_chunks,
    bench_public_response_decode_chunks_stateless, bench_public_response_receive,
    bench_streaming_decode_response, bench_write_request_wire_to_sink,
};
use nntpbench::{
    ClientCommandMix, MessageId, Request, RequestKind, bench_append_load_workload_request,
    bench_client_request_for_command, bench_client_segment_request_for_command,
    bench_load_read_capacity_in_place, bench_load_response_scan_in_place,
    bench_load_response_verify_in_place,
};
use std::sync::Arc;
use tokio::runtime::Builder;

mod fixtures;

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
    fn message_id_article(bencher: Bencher) {
        bencher.bench_local(|| {
            black_box(bench_client_request_for_command(
                black_box(42),
                black_box(42),
                black_box(ClientCommandMix::Article),
                false,
            ))
        });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn message_id_body(bencher: Bencher) {
        bencher.bench_local(|| {
            black_box(bench_client_request_for_command(
                black_box(42),
                black_box(42),
                black_box(ClientCommandMix::Body),
                false,
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
                false,
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
    use super::{
        Bencher, ClientCommandMix, Request, bench_append_load_workload_request,
        bench_write_request_wire_to_sink, black_box, runtime,
    };
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

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn raw_load_message_id_alternate_to_vec(bencher: Bencher) {
        let output = RefCell::new(Vec::with_capacity(128));
        bencher.bench_local(|| {
            let mut output = output.borrow_mut();
            output.clear();
            bench_append_load_workload_request(
                black_box(&mut output),
                black_box(42),
                black_box(42),
                black_box(ClientCommandMix::Alternate),
            )
            .unwrap();
            black_box(output.len())
        });
    }
}

mod streaming_decode {
    use super::{
        BODY_RESPONSE, Bencher, COMPACT_BODY_RESPONSE, RequestKind,
        bench_load_response_scan_in_place, bench_load_response_verify_in_place,
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

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn raw_load_body_response_scan(bencher: Bencher) {
        bencher
            .with_inputs(|| BODY_RESPONSE.to_vec())
            .bench_local_refs(|buffer| {
                black_box(
                    bench_load_response_scan_in_place(
                        black_box(buffer),
                        black_box(RequestKind::Body),
                    )
                    .unwrap(),
                )
            });
    }

    #[divan::bench(sample_count = 1000, sample_size = 100)]
    fn full_load_body_response_verify(bencher: Bencher) {
        bencher
            .with_inputs(|| BODY_RESPONSE.to_vec())
            .bench_local_refs(|buffer| {
                black_box(
                    bench_load_response_verify_in_place(
                        black_box(buffer),
                        black_box(RequestKind::Body),
                        black_box("<bench@example.com>"),
                    )
                    .unwrap(),
                )
            });
    }
}

mod public_client_experiments {
    use super::{
        Bencher, RequestKind, bench_article_validation_and_materialization,
        bench_article_validation_and_two_parses, bench_load_read_capacity_in_place,
        bench_owned_article_accessor_parse, bench_owned_response_from_bytes,
        bench_pending_read_capacity, bench_public_response_decode_chunks,
        bench_public_response_decode_chunks_stateless, bench_public_response_receive, black_box,
        fixtures, runtime,
    };
    use fixtures::ArticleVariant;

    const BODY_64K: usize = 64 * 1024;
    const BODY_768K: usize = 768 * 1024;

    fn owned_article(size: usize, variant: ArticleVariant) -> nntpbench::OwnedArticle {
        let kind = if matches!(variant, ArticleVariant::FoldedHeaders) {
            RequestKind::Article
        } else {
            RequestKind::Body
        };
        let response = fixtures::article_response(size, variant);
        nntpbench::OwnedArticle::try_from(bench_owned_response_from_bytes(kind, &response).unwrap())
            .unwrap()
    }

    fn bench_article_parse_passes(bencher: Bencher, size: usize, variant: ArticleVariant) {
        let article = owned_article(size, variant);
        bencher.bench(|| black_box(bench_owned_article_accessor_parse(black_box(&article))));
    }

    fn bench_full_article_parse_path(bencher: Bencher, size: usize, variant: ArticleVariant) {
        let kind = if matches!(variant, ArticleVariant::FoldedHeaders) {
            RequestKind::Article
        } else {
            RequestKind::Body
        };
        let response = fixtures::article_response(size, variant);
        bencher.bench(|| {
            black_box(bench_article_validation_and_two_parses(
                black_box(kind),
                black_box(&response),
            ))
        });
    }

    fn bench_single_article_parse_path(bencher: Bencher, size: usize, variant: ArticleVariant) {
        let kind = if matches!(variant, ArticleVariant::FoldedHeaders) {
            RequestKind::Article
        } else {
            RequestKind::Body
        };
        let response = fixtures::article_response(size, variant);
        bencher.bench(|| {
            black_box(bench_article_validation_and_materialization(
                black_box(kind),
                black_box(&response),
            ))
        });
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn full_article_parse_path_plain_64k(bencher: Bencher) {
        bench_full_article_parse_path(bencher, BODY_64K, ArticleVariant::PlainBody);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn full_article_parse_path_dot_stuffed_64k(bencher: Bencher) {
        bench_full_article_parse_path(bencher, BODY_64K, ArticleVariant::DotStuffedBody);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn full_article_parse_path_folded_headers_64k(bencher: Bencher) {
        bench_full_article_parse_path(bencher, BODY_64K, ArticleVariant::FoldedHeaders);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn full_article_parse_path_plain_768k(bencher: Bencher) {
        bench_full_article_parse_path(bencher, BODY_768K, ArticleVariant::PlainBody);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn full_article_parse_path_dot_stuffed_768k(bencher: Bencher) {
        bench_full_article_parse_path(bencher, BODY_768K, ArticleVariant::DotStuffedBody);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn full_article_parse_path_folded_headers_768k(bencher: Bencher) {
        bench_full_article_parse_path(bencher, BODY_768K, ArticleVariant::FoldedHeaders);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn single_article_parse_path_plain_64k(bencher: Bencher) {
        bench_single_article_parse_path(bencher, BODY_64K, ArticleVariant::PlainBody);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn single_article_parse_path_dot_stuffed_64k(bencher: Bencher) {
        bench_single_article_parse_path(bencher, BODY_64K, ArticleVariant::DotStuffedBody);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn single_article_parse_path_folded_headers_64k(bencher: Bencher) {
        bench_single_article_parse_path(bencher, BODY_64K, ArticleVariant::FoldedHeaders);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn single_article_parse_path_plain_768k(bencher: Bencher) {
        bench_single_article_parse_path(bencher, BODY_768K, ArticleVariant::PlainBody);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn single_article_parse_path_dot_stuffed_768k(bencher: Bencher) {
        bench_single_article_parse_path(bencher, BODY_768K, ArticleVariant::DotStuffedBody);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn single_article_parse_path_folded_headers_768k(bencher: Bencher) {
        bench_single_article_parse_path(bencher, BODY_768K, ArticleVariant::FoldedHeaders);
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn repeated_article_parse_plain_64k(bencher: Bencher) {
        bench_article_parse_passes(bencher, BODY_64K, ArticleVariant::PlainBody);
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn repeated_article_parse_dot_stuffed_64k(bencher: Bencher) {
        bench_article_parse_passes(bencher, BODY_64K, ArticleVariant::DotStuffedBody);
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn repeated_article_parse_folded_headers_64k(bencher: Bencher) {
        bench_article_parse_passes(bencher, BODY_64K, ArticleVariant::FoldedHeaders);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn repeated_article_parse_plain_768k(bencher: Bencher) {
        bench_article_parse_passes(bencher, BODY_768K, ArticleVariant::PlainBody);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn repeated_article_parse_dot_stuffed_768k(bencher: Bencher) {
        bench_article_parse_passes(bencher, BODY_768K, ArticleVariant::DotStuffedBody);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn repeated_article_parse_folded_headers_768k(bencher: Bencher) {
        bench_article_parse_passes(bencher, BODY_768K, ArticleVariant::FoldedHeaders);
    }

    fn bench_fragmented_decode_response(bencher: Bencher, response: Vec<u8>, chunk_bytes: usize) {
        bencher.bench(|| {
            black_box(bench_public_response_decode_chunks(
                black_box(RequestKind::Body),
                black_box(&response),
                black_box(chunk_bytes),
            ))
        });
    }

    fn bench_fragmented_decode(bencher: Bencher, size: usize, chunk_bytes: usize) {
        bench_fragmented_decode_response(
            bencher,
            fixtures::body_response(size, false),
            chunk_bytes,
        );
    }

    fn bench_fragmented_decode_whole_read(bencher: Bencher, size: usize) {
        let response = fixtures::body_response(size, false);
        let chunk_bytes = response.len();
        bench_fragmented_decode_response(bencher, response, chunk_bytes);
    }

    fn bench_async_receive_response(bencher: Bencher, response: Vec<u8>, chunk_bytes: usize) {
        let rt = runtime();
        bencher.bench_local(|| {
            black_box(rt.block_on(bench_public_response_receive(
                black_box(RequestKind::Body),
                black_box(&response),
                black_box(chunk_bytes),
            )))
        });
    }

    fn bench_async_receive(bencher: Bencher, size: usize, chunk_bytes: usize) {
        bench_async_receive_response(bencher, fixtures::body_response(size, false), chunk_bytes);
    }

    fn bench_async_receive_whole_read(bencher: Bencher, size: usize) {
        let response = fixtures::body_response(size, false);
        let chunk_bytes = response.len();
        bench_async_receive_response(bencher, response, chunk_bytes);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn async_receive_64k_256_bytes(bencher: Bencher) {
        bench_async_receive(bencher, BODY_64K, 256);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn async_receive_64k_whole_read(bencher: Bencher) {
        bench_async_receive_whole_read(bencher, BODY_64K);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn async_receive_768k_256k(bencher: Bencher) {
        bench_async_receive(bencher, BODY_768K, 256 * 1024);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn async_receive_768k_whole_read(bencher: Bencher) {
        bench_async_receive_whole_read(bencher, BODY_768K);
    }

    fn bench_stateless_decode_response(bencher: Bencher, response: Vec<u8>, chunk_bytes: usize) {
        bencher.bench(|| {
            black_box(bench_public_response_decode_chunks_stateless(
                black_box(RequestKind::Body),
                black_box(&response),
                black_box(chunk_bytes),
            ))
        });
    }

    fn bench_stateless_decode(bencher: Bencher, size: usize, chunk_bytes: usize) {
        bench_stateless_decode_response(bencher, fixtures::body_response(size, false), chunk_bytes);
    }

    fn bench_stateless_decode_whole_read(bencher: Bencher, size: usize) {
        let response = fixtures::body_response(size, false);
        let chunk_bytes = response.len();
        bench_stateless_decode_response(bencher, response, chunk_bytes);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn fragmented_decode_64k_1_byte(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_64K, 1);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn fragmented_decode_64k_2_bytes(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_64K, 2);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn fragmented_decode_64k_4_bytes(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_64K, 4);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn fragmented_decode_64k_31_bytes(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_64K, 31);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn fragmented_decode_64k_256_bytes(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_64K, 256);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn fragmented_decode_64k_whole_read(bencher: Bencher) {
        bench_fragmented_decode_whole_read(bencher, BODY_64K);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    #[ignore]
    fn fragmented_decode_768k_1_byte(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_768K, 1);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn fragmented_decode_768k_2_bytes(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_768K, 2);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn fragmented_decode_768k_4_bytes(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_768K, 4);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn fragmented_decode_768k_31_bytes(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_768K, 31);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn fragmented_decode_768k_256k(bencher: Bencher) {
        bench_fragmented_decode(bencher, BODY_768K, 256 * 1024);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn fragmented_decode_768k_whole_read(bencher: Bencher) {
        bench_fragmented_decode_whole_read(bencher, BODY_768K);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn stateless_decode_control_64k_1_byte(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_64K, 1);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn stateless_decode_control_64k_2_bytes(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_64K, 2);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn stateless_decode_control_64k_4_bytes(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_64K, 4);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn stateless_decode_control_64k_31_bytes(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_64K, 31);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn stateless_decode_control_64k_256_bytes(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_64K, 256);
    }

    #[divan::bench(sample_count = 50, sample_size = 10)]
    fn stateless_decode_control_64k_whole_read(bencher: Bencher) {
        bench_stateless_decode_whole_read(bencher, BODY_64K);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn stateless_decode_control_768k_256k(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_768K, 256 * 1024);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    #[ignore]
    fn stateless_decode_control_768k_1_byte(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_768K, 1);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn stateless_decode_control_768k_2_bytes(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_768K, 2);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn stateless_decode_control_768k_4_bytes(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_768K, 4);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn stateless_decode_control_768k_31_bytes(bencher: Bencher) {
        bench_stateless_decode(bencher, BODY_768K, 31);
    }

    #[divan::bench(sample_count = 20, sample_size = 5)]
    fn stateless_decode_control_768k_whole_read(bencher: Bencher) {
        bench_stateless_decode_whole_read(bencher, BODY_768K);
    }

    fn bench_read_capacity(
        bencher: Bencher,
        source_size: usize,
        initial_len: usize,
        initial_capacity: usize,
        read_chunk_bytes: usize,
    ) {
        let source = vec![0_u8; source_size];
        let rt = runtime();
        bencher.bench(|| {
            black_box(rt.block_on(bench_pending_read_capacity(
                black_box(&source),
                black_box(initial_len),
                black_box(initial_capacity),
                black_box(read_chunk_bytes),
            )))
        });
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn pending_read_64k_with_spare_capacity(bencher: Bencher) {
        bench_read_capacity(bencher, BODY_64K, 32 * 1024, BODY_64K, 768);
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn pending_read_64k_at_capacity_boundary(bencher: Bencher) {
        bench_read_capacity(bencher, BODY_64K, BODY_64K - 256, BODY_64K, 768);
    }

    #[divan::bench(sample_count = 100, sample_size = 20)]
    fn pending_read_768k_reused_capacity(bencher: Bencher) {
        bench_read_capacity(bencher, BODY_768K, BODY_64K - 256, BODY_768K, 768);
    }

    fn bench_load_read(
        bencher: Bencher,
        source_size: usize,
        initial_len: usize,
        initial_capacity: usize,
        read_chunk_bytes: usize,
    ) {
        bencher
            .with_inputs(|| {
                let mut buffer = Vec::with_capacity(initial_capacity);
                buffer.resize(initial_len, 0);
                (runtime(), vec![0_u8; source_size], buffer)
            })
            .bench_local_values(|(rt, source, mut buffer)| {
                black_box(rt.block_on(bench_load_read_capacity_in_place(
                    black_box(&source),
                    black_box(&mut buffer),
                    black_box(read_chunk_bytes),
                )))
            });
    }

    #[divan::bench(sample_count = 100, sample_size = 20, skip_ext_time)]
    fn load_read_64k_with_spare_capacity(bencher: Bencher) {
        bench_load_read(bencher, BODY_64K, 32 * 1024, BODY_64K, 768);
    }

    #[divan::bench(sample_count = 100, sample_size = 20, skip_ext_time)]
    fn load_read_64k_at_capacity_boundary(bencher: Bencher) {
        bench_load_read(bencher, BODY_64K, BODY_64K - 256, BODY_64K, 768);
    }

    #[divan::bench(sample_count = 100, sample_size = 20, skip_ext_time)]
    fn load_read_reused_capacity(bencher: Bencher) {
        // Keep the measured read identical to the spare-capacity case; only
        // the already-grown allocation differs.
        bench_load_read(bencher, BODY_64K, 32 * 1024, BODY_768K, 768);
    }
}
