# Protocol scope

nntpbench is a benchmark client and a mock server. This page separates the NNTP behavior that code runs from the surfaces that stop at a request builder or a fixed reply.

Response framing and ownership are described in the README section [Response ownership](../README.md#response-ownership).

## In scope

The server is a reader benchmark peer. These commands run as real session behavior:

- The greeting, `QUIT`, `DATE`, `HELP`, and `CAPABILITIES`.
- `GROUP`, `LISTGROUP`, `LAST`, `NEXT`, `ARTICLE`, `HEAD`, `BODY`, and `STAT`.
- `LIST` with `ACTIVE`, `ACTIVE.TIMES`, `DISTRIB.PATS`, `NEWSGROUPS`, `OVERVIEW.FMT`, and `HEADERS`.
- `OVER`, `HDR`, `XOVER`, and `XHDR`.
- `NEWGROUPS` and `NEWNEWS`.
- `MODE READER`, answered with `201 posting not permitted`.
- Pipelined commands. Commands that can change connection state take a send barrier so later commands wait.

`CAPABILITIES` advertises that reader set and nothing else. The fixed body is `CAPABILITIES_RESPONSE` in `src/lib.rs`. It lists `VERSION 2`, `READER`, the `LIST` keywords above, `OVER MSGID`, `HDR`, and `NEWNEWS`. It does not list `STARTTLS`, `AUTHINFO`, `SASL`, `STREAMING`, or `MODE-READER`.

`ClientConnection` sends `CAPABILITIES` before `POST`, `IHAVE`, `CHECK`, `TAKETHIS`, `AUTHINFO USER`, `AUTHINFO PASS`, `STARTTLS`, `OVER`, `XOVER`, `HDR`, `XHDR`, `NEWGROUPS`, and `NEWNEWS` (`request_kind_requires_capabilities` in `src/client.rs`). It stores a boolean after status 101. It does not store the keyword list. `MODE STREAM` is not in that set, so the client can send it without that preflight.

`--article-dir` replaces generated retrieval, list, overview, and discovery bytes with stored response frames when an index is built. Without that directory, those commands use the synthetic responses below.

## Benchmark-only

Each surface below is an intentional benchmark limit. A `Request` constructor or a response-frame parser is the wire shape. It is not the RFC exchange that follows that shape.

### STARTTLS

Benchmark-only. There is no TLS transport upgrade.

`Request::starttls` is the command line `STARTTLS`. `Client::starttls` and `ClientConnection::starttls` send that line and return the response bytes. Neither call wraps the socket, and the crate has no TLS stack.

The server answers `RequestKind::StartTls` with `502 command unavailable` in `handle_command` and in `process_request_to_buffer` (`src/lib.rs`). `CAPABILITIES` and `HELP` do not advertise `STARTTLS`.

The response table in `src/protocol.rs` can parse a 382 frame for `RequestKind::StartTls`. Parsing that frame does not begin a TLS handshake.

### AUTHINFO and SASL

Benchmark-only. There is no authentication exchange.

`AuthInfoKind` in `src/protocol.rs` is `User` and `Pass`. The client can build and send `AUTHINFO USER` and `AUTHINFO PASS`. It has no SASL mechanism, initial response, or challenge step.

The server rejects `AUTHINFO USER` and `AUTHINFO PASS` with `483 command unavailable until TLS has been negotiated`. It rejects other `AUTHINFO` forms, including `AUTHINFO SASL`, with `502 command unavailable` (`RequestKind::AuthInfo` in `src/lib.rs`). `CAPABILITIES` does not advertise `AUTHINFO` or `SASL`.

The same response table can parse 381, 281, 283, and 383. Those parsers are not an `AUTHINFO SASL` conversation.

### POST and IHAVE continuations

Benchmark-only. There is no article submission.

`Request::post` is the line `POST`. `Request::ihave` is `IHAVE` plus a message-id. Neither builder carries an article. The server answers `POST` with `440 posting not permitted` and `IHAVE` with `435 article not wanted`, and it does not read a following article (`src/lib.rs`).

`Request::takethis` can attach a dot-stuffed article. The server reads that body so a pipelined batch stays aligned, then answers `502 command unavailable`. `CHECK` also answers `502`. Reading the `TAKETHIS` body and then rejecting the command is a benchmark shortcut for pipeline alignment, not an accepted transfer. The 340, 240, 335, and 235 response parsers are not posting or transit flows.

### STREAMING and MODE STREAM

Benchmark shortcut. `MODE STREAM` does not enable streaming.

`RequestKind::ModeStream` writes the fixed line `203 streaming enabled` (`src/lib.rs`). The handler does not record a streaming session, and `CAPABILITIES` still omits `STREAMING`. `CHECK` stays `502`. `TAKETHIS` stays `502` after the body scan described above. The 203 line is a benchmark shortcut, not an RFC 4644 streaming mode.

`HELP` lists `CHECK` while that command returns 502. That help line is a benchmark shortcut.

The client sends `MODE STREAM` through `ClientConnection::execute` without the capability preflight and without the send barrier (`request_kind_requires_capabilities` and `request_kind_requires_barrier` in `src/client.rs`).

### Capability state

The client cache is a one-shot success flag, not a capability state machine.

`ClientConnection` keeps `capabilities_negotiated` as a `bool` (`src/client.rs`). `ensure_capabilities_negotiated` sets it after `CAPABILITIES` returns 101. `response_invalidates_capabilities` clears it after `STARTTLS`, after `MODE READER`, and after `AUTHINFO` status 281 or 283. `MODE STREAM` does not clear it. The flag does not record which keywords were advertised.

The server capability body stays `CAPABILITIES_RESPONSE` after those commands. It does not grow or shrink when the client clears its flag.

### Benchmark workload realism

Default article bytes are synthetic. That repeated text is a benchmark shortcut.

`build_article_response_into` in `src/lib.rs` writes a fixed header, `Subject: nntpbench synthetic article`, and repeats `BODY_LINE` until `--article-bytes` or `--body-bytes` is filled. `BODY_LINE` is the sentence `This is synthetic NNTP article payload for throughput and latency benchmarking`. Message-ids look like `<article.N@nntpbench.local>`. The default date is `Fri, 15 May 2026 00:00:00 +0000`.

Without `--article-dir`, group, overview, header, and discovery replies use the fixed tables in `src/lib.rs`. Those fixed rows are a benchmark shortcut. They are not a news spool, a yEnc corpus, or a transit feed.

`--article-dir` points at stored `ARTICLE` response frames keyed by selector. It changes retrieval bytes. It does not add TLS, authentication, posting, or streaming.
