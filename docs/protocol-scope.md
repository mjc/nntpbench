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
- Pipelined commands. `RequestKind::coordination` records whether a later command may be written before the response, whether the command requires the connection alone, whether it changes session or transport state, and whether it enters continuation mode. The client waits until earlier responses finish before writing an exclusive command, and it holds the send barrier until the response when a later command must not follow. After `QUIT` returns 205, further commands are rejected.

`HELP` lists that same reader set: `ARTICLE`, `BODY`, `CAPABILITIES`, `DATE`, `GROUP`, `HDR`, `HEAD`, `HELP`, `LAST`, `LIST`, `LISTGROUP`, `MODE READER`, `NEWGROUPS`, `NEWNEWS`, `NEXT`, `OVER`, `QUIT`, `STAT`, `XHDR`, and `XOVER`. It does not list `CHECK`, `TAKETHIS`, `POST`, `IHAVE`, `AUTHINFO`, `STARTTLS`, or `MODE STREAM`.

`CAPABILITIES` advertises that reader set and nothing else. The fixed body is `CAPABILITIES_RESPONSE` in `src/lib.rs`. It lists `VERSION 2`, `READER`, the `LIST` keywords above, `OVER MSGID`, `HDR`, and `NEWNEWS`. It does not list `STARTTLS`, `AUTHINFO`, `SASL`, `STREAMING`, `POST`, `IHAVE`, `CHECK`, `TAKETHIS`, or `MODE-READER`. An `IMPLEMENTATION` line is not part of this body. If a later line names the program, that line is informational and does not advertise a capability. An unknown keyword argument still returns 101 and that same body. A non-keyword argument, including a one- or two-character token, a numeric token, an underscore, or a second argument, returns `501 command syntax error`.

`ClientConnection` sends `CAPABILITIES` before `POST`, `IHAVE`, `CHECK`, `TAKETHIS`, `AUTHINFO USER`, `AUTHINFO PASS`, `STARTTLS`, `OVER`, `XOVER`, `HDR`, `XHDR`, `NEWGROUPS`, and `NEWNEWS` when it has no stored capability list (`request_kind_requires_capabilities` and `ensure_capabilities_negotiated` in `src/client.rs`). That send is a convenience preflight. The stored list is described under [Capability state](#capability-state). `MODE STREAM` is not in that set, so the client can send it without that preflight.

`--article-dir` replaces generated retrieval, list, overview, and discovery bytes with stored response frames when an index is built. Without that directory, retrieval uses the generated payloads below, and group, overview, header, and discovery replies use fixed rows.

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

Benchmark-only. The server never accepts an article.

`Request::post` is the line `POST`. `Request::ihave` is `IHAVE` plus a message-id. `Client::post_article`, `ClientConnection::post_article`, and the matching `ihave_article` calls keep the article until the interim status arrives. They write the dot-stuffed, CRLF-normalized body, then one `.\r\n`, only after `POST` 340 or `IHAVE` 335. Any other status, including the server's 440 and 435, returns that interim response and sends no article. The send barrier stays held from the command through the final response, so a later command is not written during the pause.

`handle_command` still answers `POST` with `440 posting not permitted` and `IHAVE` with `435 article not wanted`.

`read_command_batch` calls `push_command`. `should_read_transfer_body` in `src/lib.rs` always reads a dot-terminated body after `TAKETHIS`. After `POST` or `IHAVE`, including the lower-case forms recognized by `is_known_transfer_command`, it reads that body only when the next line already in the buffer contains `:`. An empty buffer is not a body: the server answers 440 or 435 without reading another socket byte, so a client waiting for that rejection does not deadlock. `read_dot_terminated_body` discards the bytes. `has_transfer_body` is recorded and never read. The reply stays 440 or 435, so a pipelined `QUIT` after that discarded body still runs. A `POST` or `IHAVE` whose next line has no `:` is left as a single command line.

`for_each_request_line_in_batch` uses that same rule. It always scans a dot-terminated body after `TAKETHIS`. After `POST` or `IHAVE` it scans that body only when the next line contains `:`, and it leaves the command unconsumed until the terminator is complete. A following line with no `:` stays the next command.

`Request::takethis` can attach a dot-stuffed article. Both readers can consume that body, and the server still answers `502 command unavailable`. `CHECK` also answers `502`. Consuming a body and then rejecting the command is a benchmark shortcut for keeping the read aligned, not an accepted transfer. The 340, 240, 335, and 235 response parsers are not posting or transit flows.

### STREAMING and MODE STREAM

The reader server does not offer streaming.

`handle_command` and `process_request_to_buffer` answer `MODE STREAM` with `502 command unavailable`. That reply does not record a streaming session. `CAPABILITIES` omits `STREAMING`, and `HELP` does not list `MODE STREAM`. `CHECK` and `TAKETHIS` stay `502` whether or not `MODE STREAM` came first. A peer that answers 203 can still be parsed; this server does not send that status. `HELP` does not list `CHECK`, `TAKETHIS`, or `MODE STREAM`.

The client sends `MODE STREAM` through `ClientConnection::execute` without the capability preflight and without the send barrier (`request_kind_requires_capabilities` and `request_kind_requires_barrier` in `src/client.rs`). `CHECK` and `TAKETHIS` also allow a following command before their responses. `POST`, `IHAVE`, `AUTHINFO`, `MODE READER`, `STARTTLS`, and `QUIT` do not. The load command mixes (`Article`, `Body`, `Stat`, `Alternate`, `Mixed`) only emit commands whose coordination allows a throughput pipeline.

### Capability state

`ClientConnection` stores the last accepted `CAPABILITIES` 101 body as a `CapabilityList` (`src/protocol.rs`). `Client::capability_state` and `ClientConnection::capability_state` return that list and send no command. `None` means the connection has not stored a list, or a later command cleared it.

`ensure_capabilities_negotiated` sends `CAPABILITIES` when a command in `request_kind_requires_capabilities` runs and the stored list is absent. Status 101 replaces the stored list with the parsed body. Any other status leaves the stored list unchanged, and that command returns `ClientError::CapabilitiesUnavailable`. `capabilities` and `capabilities_exchange` also store a 101 list. A 101 body that fails capability-list validation does not replace the stored list.

`response_invalidates_capabilities` clears the stored list after `STARTTLS`, after `MODE READER`, and after `AUTHINFO` status 281 or 283. `AUTHINFO` status 381 or 383 leaves the list in place, so `AUTHINFO PASS` does not send another `CAPABILITIES` before the password response. `MODE STREAM` does not clear the list.

The server capability body stays `CAPABILITIES_RESPONSE` after those commands. It does not grow or shrink when the client clears its list. `STARTTLS` remains the disabled command described above: the server answers 502, and the client still clears the stored list before continuing on the same plaintext connection.

### Benchmark workload realism

Default ARTICLE and BODY payloads are generated yEnc. That generated encoding is a benchmark shortcut.

`ServerArgs.payload_mode` defaults to `PayloadMode::Yenc`. `build_server_payload` calls `payload::encode_yenc_payload`, and `ServerConfig::from_bound_args` frames that payload once under the fixed header `Subject: nntpbench synthetic article`. `--body-bytes` and `--article-bytes` are the decoded sizes. The file name in the yEnc header is `nntpbench.bin`. Message-ids look like `<article.N@nntpbench.local>`. The default date is `Fri, 15 May 2026 00:00:00 +0000`.

`--payload-mode plain` uses `build_repeated_payload` instead. That path repeats `BODY_LINE`, the sentence `This is synthetic NNTP article payload for throughput and latency benchmarking`. The repeated line is a benchmark shortcut.

Without `--article-dir`, group, overview, header, and discovery replies use the fixed tables in `src/lib.rs`. Those fixed rows are a benchmark shortcut. The default workload is not a captured news spool or a transit feed.

`--article-dir` points at stored `ARTICLE` response frames keyed by selector. It changes retrieval bytes. It does not add TLS, authentication, posting, or streaming.

The one-shot client commands stay single requests. `--capability KEYWORD` adds the keyword argument to `CAPABILITIES`. `--article-file` supplies the bytes for `POST`, `IHAVE`, and `TAKETHIS`; `POST` and `IHAVE` still wait for 340 or 335 before writing that file. `--session FILE` runs the reader commands in that file on one connection, including `GROUP`, article retrieval, `CAPABILITIES`, `POST-ARTICLE`, and `IHAVE-ARTICLE`. A bare `POST` or `IHAVE` line is rejected, because those commands have no article to send after 340 or 335. The script rejects `STARTTLS`, `AUTHINFO SASL`, and `MODE STREAM`. `AUTHINFO USER` and `AUTHINFO PASS` keep the value text after the keyword, including spaces inside it. `--auth-value` remains the `AUTHINFO USER` and `AUTHINFO PASS` builder input.
