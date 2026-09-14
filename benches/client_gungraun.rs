//! Gungraun probes for public-client parsing and fragmented decoding.
//!
//! These benchmarks deliberately stop at the deterministic parser boundary:
//! live sockets and Tokio scheduling would obscure the repeated parsing and
//! whole-pending-buffer rescans represented by the control benchmarks.

macro_rules! supported {
    ($($item:item)*) => {
        $(
            #[cfg(all(target_os = "linux", any(target_arch = "x86_64", target_arch = "aarch64")))]
            $item
        )*
    };
}

supported! {
    use gungraun::{
        Callgrind, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main,
    };
    use nntpbench::client::{
        bench_article_validation_and_layout_reuse, bench_article_validation_and_two_parses,
        bench_owned_article_accessor_parse, bench_owned_response_from_bytes,
        bench_public_response_decode_chunks, bench_public_response_decode_chunks_stateless,
    };
    use nntpbench::{OwnedResponse, RequestKind, bench_load_read_capacity_in_place};
    use std::hint::black_box;
    use tokio::runtime::{Builder, Runtime};

    mod fixtures;
    use fixtures::ArticleVariant;

    const BODY_64K: usize = 64 * 1024;
    const BODY_768K: usize = 768 * 1024;

    fn setup_owned_response(size: usize, variant: ArticleVariant) -> OwnedResponse {
        let kind = if matches!(variant, ArticleVariant::FoldedHeaders) {
            RequestKind::Article
        } else {
            RequestKind::Body
        };
        let bytes = fixtures::article_response(size, variant);
        bench_owned_response_from_bytes(kind, &bytes).unwrap()
    }

    fn setup_body_response(size: usize) -> Vec<u8> {
        fixtures::body_response(size, false)
    }

    fn runtime() -> Runtime {
        Builder::new_current_thread().enable_all().build().unwrap()
    }

    #[library_benchmark]
    #[bench::plain_64k(setup = setup_plain_64k)]
    #[bench::dot_stuffed_64k(setup = setup_dot_stuffed_64k)]
    #[bench::folded_headers_64k(setup = setup_folded_headers_64k)]
    #[bench::plain_768k(setup = setup_plain_768k)]
    #[bench::dot_stuffed_768k(setup = setup_dot_stuffed_768k)]
    #[bench::folded_headers_768k(setup = setup_folded_headers_768k)]
    fn repeated_owned_article_parse(response: OwnedResponse) -> usize {
        black_box(bench_owned_article_accessor_parse(black_box(&response)).unwrap())
    }

    #[library_benchmark]
    #[bench::plain_64k(setup = setup_wire_plain_64k)]
    #[bench::dot_stuffed_64k(setup = setup_wire_dot_stuffed_64k)]
    #[bench::folded_headers_64k(setup = setup_wire_folded_headers_64k)]
    #[bench::plain_768k(setup = setup_wire_plain_768k)]
    #[bench::dot_stuffed_768k(setup = setup_wire_dot_stuffed_768k)]
    #[bench::folded_headers_768k(setup = setup_wire_folded_headers_768k)]
    fn full_article_parse_path((kind, response): (RequestKind, Vec<u8>)) -> usize {
        black_box(bench_article_validation_and_two_parses(
            black_box(kind),
            black_box(&response),
        )
        .unwrap())
    }

    #[library_benchmark]
    #[bench::plain_64k(setup = setup_wire_plain_64k)]
    #[bench::dot_stuffed_64k(setup = setup_wire_dot_stuffed_64k)]
    #[bench::folded_headers_64k(setup = setup_wire_folded_headers_64k)]
    #[bench::plain_768k(setup = setup_wire_plain_768k)]
    #[bench::dot_stuffed_768k(setup = setup_wire_dot_stuffed_768k)]
    #[bench::folded_headers_768k(setup = setup_wire_folded_headers_768k)]
    fn single_article_parse_path((kind, response): (RequestKind, Vec<u8>)) -> usize {
        black_box(bench_article_validation_and_layout_reuse(
            black_box(kind),
            black_box(&response),
        )
        .unwrap())
    }

    fn setup_plain_64k() -> OwnedResponse {
        setup_owned_response(BODY_64K, ArticleVariant::PlainBody)
    }

    fn setup_dot_stuffed_64k() -> OwnedResponse {
        setup_owned_response(BODY_64K, ArticleVariant::DotStuffedBody)
    }

    fn setup_folded_headers_64k() -> OwnedResponse {
        setup_owned_response(BODY_64K, ArticleVariant::FoldedHeaders)
    }

    fn setup_plain_768k() -> OwnedResponse {
        setup_owned_response(BODY_768K, ArticleVariant::PlainBody)
    }

    fn setup_dot_stuffed_768k() -> OwnedResponse {
        setup_owned_response(BODY_768K, ArticleVariant::DotStuffedBody)
    }

    fn setup_folded_headers_768k() -> OwnedResponse {
        setup_owned_response(BODY_768K, ArticleVariant::FoldedHeaders)
    }

    fn setup_wire_response(size: usize, variant: ArticleVariant) -> (RequestKind, Vec<u8>) {
        let kind = if matches!(variant, ArticleVariant::FoldedHeaders) {
            RequestKind::Article
        } else {
            RequestKind::Body
        };
        (kind, fixtures::article_response(size, variant))
    }

    fn setup_wire_plain_64k() -> (RequestKind, Vec<u8>) {
        setup_wire_response(BODY_64K, ArticleVariant::PlainBody)
    }

    fn setup_wire_dot_stuffed_64k() -> (RequestKind, Vec<u8>) {
        setup_wire_response(BODY_64K, ArticleVariant::DotStuffedBody)
    }

    fn setup_wire_folded_headers_64k() -> (RequestKind, Vec<u8>) {
        setup_wire_response(BODY_64K, ArticleVariant::FoldedHeaders)
    }

    fn setup_wire_plain_768k() -> (RequestKind, Vec<u8>) {
        setup_wire_response(BODY_768K, ArticleVariant::PlainBody)
    }

    fn setup_wire_dot_stuffed_768k() -> (RequestKind, Vec<u8>) {
        setup_wire_response(BODY_768K, ArticleVariant::DotStuffedBody)
    }

    fn setup_wire_folded_headers_768k() -> (RequestKind, Vec<u8>) {
        setup_wire_response(BODY_768K, ArticleVariant::FoldedHeaders)
    }

    #[library_benchmark]
    #[bench::one_byte_64k(setup = setup_decode_64k_one)]
    #[bench::two_bytes_64k(setup = setup_decode_64k_2)]
    #[bench::four_bytes_64k(setup = setup_decode_64k_4)]
    #[bench::thirty_one_bytes_64k(setup = setup_decode_64k_31)]
    #[bench::chunk_256_64k(setup = setup_decode_64k_256)]
    #[bench::whole_read_64k(setup = setup_decode_64k_whole)]
    #[bench::chunk_1k_768k(setup = setup_decode_768k_1k)]
    #[bench::two_bytes_768k(setup = setup_decode_768k_2)]
    #[bench::four_bytes_768k(setup = setup_decode_768k_4)]
    #[bench::thirty_one_bytes_768k(setup = setup_decode_768k_31)]
    #[bench::chunk_256k_768k(setup = setup_decode_768k_256k)]
    #[bench::whole_read_768k(setup = setup_decode_768k_whole)]
    fn fragmented_public_decode((response, chunk_bytes): (Vec<u8>, usize)) -> usize {
        black_box(
            bench_public_response_decode_chunks(
                RequestKind::Body,
                black_box(&response),
                chunk_bytes,
            )
            .unwrap()
            .1,
        )
    }

    #[library_benchmark]
    #[bench::one_byte_64k(setup = setup_decode_64k_one)]
    #[bench::two_bytes_64k(setup = setup_decode_64k_2)]
    #[bench::four_bytes_64k(setup = setup_decode_64k_4)]
    #[bench::thirty_one_bytes_64k(setup = setup_decode_64k_31)]
    #[bench::chunk_256_64k(setup = setup_decode_64k_256)]
    #[bench::whole_read_64k(setup = setup_decode_64k_whole)]
    #[bench::chunk_1k_768k(setup = setup_decode_768k_1k)]
    #[bench::two_bytes_768k(setup = setup_decode_768k_2)]
    #[bench::four_bytes_768k(setup = setup_decode_768k_4)]
    #[bench::thirty_one_bytes_768k(setup = setup_decode_768k_31)]
    #[bench::chunk_256k_768k(setup = setup_decode_768k_256k)]
    #[bench::whole_read_768k(setup = setup_decode_768k_whole)]
    fn stateless_public_decode_control((response, chunk_bytes): (Vec<u8>, usize)) -> usize {
        black_box(
            bench_public_response_decode_chunks_stateless(
                RequestKind::Body,
                black_box(&response),
                chunk_bytes,
            )
            .unwrap()
            .1,
        )
    }

    fn setup_decode_64k_one() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_64K), 1)
    }

    fn setup_decode_64k_2() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_64K), 2)
    }

    fn setup_decode_64k_4() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_64K), 4)
    }

    fn setup_decode_64k_31() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_64K), 31)
    }

    fn setup_decode_64k_256() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_64K), 256)
    }

    fn setup_decode_64k_whole() -> (Vec<u8>, usize) {
        let response = setup_body_response(BODY_64K);
        let chunk_bytes = response.len();
        (response, chunk_bytes)
    }

    fn setup_decode_768k_1k() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_768K), 1024)
    }

    fn setup_decode_768k_2() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_768K), 2)
    }

    fn setup_decode_768k_4() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_768K), 4)
    }

    fn setup_decode_768k_31() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_768K), 31)
    }

    fn setup_decode_768k_256k() -> (Vec<u8>, usize) {
        (setup_body_response(BODY_768K), 256 * 1024)
    }

    fn setup_decode_768k_whole() -> (Vec<u8>, usize) {
        let response = setup_body_response(BODY_768K);
        let chunk_bytes = response.len();
        (response, chunk_bytes)
    }

    type LoadReadSetup = (Runtime, Vec<u8>, Vec<u8>, usize);

    fn setup_load_read_64k_with_spare() -> LoadReadSetup {
        let mut buffer = Vec::with_capacity(BODY_64K);
        buffer.resize(32 * 1024, 0);
        (runtime(), vec![0_u8; BODY_64K], buffer, 768)
    }

    fn setup_load_read_64k_boundary() -> LoadReadSetup {
        let mut buffer = Vec::with_capacity(BODY_64K);
        buffer.resize(BODY_64K - 256, 0);
        (runtime(), vec![0_u8; BODY_64K], buffer, 768)
    }

    fn setup_load_read_reused() -> LoadReadSetup {
        let mut buffer = Vec::with_capacity(BODY_768K);
        buffer.resize(32 * 1024, 0);
        (runtime(), vec![0_u8; BODY_64K], buffer, 768)
    }

    #[library_benchmark]
    #[bench::with_spare(setup = setup_load_read_64k_with_spare)]
    #[bench::at_boundary(setup = setup_load_read_64k_boundary)]
    #[bench::reused(setup = setup_load_read_reused)]
    fn load_read_capacity(
        (runtime, source, mut buffer, read_chunk_bytes): LoadReadSetup,
    ) -> usize {
        let (read, capacity) = runtime
            .block_on(bench_load_read_capacity_in_place(
                black_box(&source),
                black_box(&mut buffer),
                black_box(read_chunk_bytes),
            ))
            .unwrap();
        black_box(read.saturating_add(capacity))
    }

    library_benchmark_group!(name = public_client; benchmarks =
        repeated_owned_article_parse,
        full_article_parse_path,
        single_article_parse_path,
        fragmented_public_decode,
        stateless_public_decode_control,
        load_read_capacity
    );

    main!(config = LibraryBenchmarkConfig::default()
        .tool(Callgrind::with_args(["--branch-sim=yes", "--cache-sim=yes"]));
        library_benchmark_groups = public_client
    );
}
