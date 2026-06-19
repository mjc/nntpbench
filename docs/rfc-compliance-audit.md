# NNTP RFC Compliance Audit Matrix

This matrix records the current compliance evidence for the benchmark
client/server implementation. It is scoped to the NNTP RFCs currently exercised
by the codebase:

- RFC 3977, Network News Transfer Protocol
- RFC 4642, STARTTLS
- RFC 4643, AUTHINFO
- RFC 4644, STREAMING
- RFC 2980, selected legacy extensions

Status meanings:

- `covered`: current tests or implementation evidence directly cover the item.
- `partial`: current evidence covers part of the item but leaves meaningful gaps.
- `follow-up`: tracked outside this matrix by a Beads issue.
- `not in scope`: the current mock server/client intentionally does not implement
  the capability; tests should prove it is not advertised or is rejected with the
  correct response.

Current test inventory: `rg -n "fn rfc|async fn rfc" src` reports 94 RFC-named
tests, and `rg -n "rfc4642|rfc4643|rfc4644|rfc2980|rfc3977" src` reports 988 RFC
references in test and implementation comments.

## RFC 3977 Core

| Area | RFC sections | Current evidence | Status | Gaps / next action |
| --- | --- | --- | --- | --- |
| Command line limits and CRLF termination | 3.1, 9.2, 9.8 | `src/protocol.rs` request-line parser tests; `src/lib.rs` server syntax tests; `src/terminator.rs` line terminator tests | covered | Rename green `rfc*_red_*` tests under `nntpbench-v4f`. |
| Command keyword grammar and argument counts | 3.1, 9.2 | `rfc3977_red_request_line_grammar_matrix`, server syntax matrices, request builder tests | covered | Keep coverage mapped when renaming tests. |
| Multiline command/data framing | 3.1.1 | `src/terminator.rs` multiline terminator tests; `src/lib.rs` TAKETHIS batch tests; article parser dot-stuffing tests | covered | Benchmark fixtures still need a separate valid-path audit under `nntpbench-hb5`. |
| Response framing and generic errors | 3.2, 3.2.1, 9.4 | `ResponseFraming::for_request_status` tests; `ResponseFrame::parse` status/shape tests | covered | The full server command-state cross-product is tracked by `nntpbench-3k5`. |
| Initial greeting | 5.1 | `read_greeting` status matrix and server greeting assertions | covered | None known. |
| CAPABILITIES | 3.3, 5.2, 9.5 | capability body parser validation; server capability exact-output test | covered | Keep unavailable extension advertisement checks aligned with server support. |
| MODE READER | 3.4.2, 5.3 | request parser/builder coverage and server response tests | covered | No transit-mode server is implemented; capabilities after MODE READER are checked. |
| QUIT | 5.4 | request parser/builder coverage; response text validation | covered | None known. |
| GROUP | 6.1.1 | server selected-group tests; group response initial-line validation; article-store tests | partial | Complete cross-product of missing group, nonexistent group, empty group, and file-backed state under `nntpbench-3k5`. |
| LISTGROUP | 6.1.2 | response parser validation; server LISTGROUP range/current tests | partial | Finish server state matrix for no group/current group and selector boundaries under `nntpbench-3k5`. |
| LAST and NEXT | 6.1.3, 6.1.4 | before-group and movement tests; response-frame argument validation | partial | Finish state matrix for empty group, nonexistent current article, and bounds under `nntpbench-3k5`. |
| ARTICLE, HEAD, BODY, STAT | 6.2.1-6.2.4 | article parser validation; numeric/message-id/current selector tests; response-frame validation | partial | Complete command-state response matrix for every selector/error combination under `nntpbench-3k5`. |
| POST and IHAVE | 6.3.1, 6.3.2 | posting-disabled and IHAVE rejection tests; response-frame continuation-code validation | partial | Transfer acceptance is intentionally unavailable; prove unavailable behavior for pipelined bodies and every state under `nntpbench-3k5`. |
| DATE | 7.1 | current UTC date test; response timestamp validation | covered | None known. |
| HELP | 7.2 | HELP response parser validation and server output checks | covered | None known. |
| NEWGROUPS | 7.3 | date/time parser tests; server result/filter tests; active row validation | partial | Need an explicit server audit for date-time edge cases and wildmat coverage in `nntpbench-3k5`. |
| NEWNEWS | 7.4 | wildmat parser tests; message-id body validation; server result/filter tests | partial | Need an explicit server audit for unsupported distributions and date-time semantics in `nntpbench-3k5`. |
| LIST base and variants | 7.6 | LIST ACTIVE, ACTIVE.TIMES, NEWSGROUPS, OVERVIEW.FMT, HEADERS, DISTRIB.PATS response validation and server tests | covered | Keep generated rows dot-stuffed and benchmark fixtures valid under `nntpbench-hb5`. |
| Overview and header commands | 8.3-8.6 | OVER/HDR/XOVER/XHDR parser, response frame, server selector tests | partial | Server state matrix for selected group/current article and message-id selectors remains in `nntpbench-3k5`. |
| ABNF value classes | 9.4-9.8 | value validation matrix for U-CHAR, B-CHAR, group names, article numbers, message-id, headers, wildmat | covered | None known. |

## RFC 4642 STARTTLS

| Area | RFC sections | Current evidence | Status | Gaps / next action |
| --- | --- | --- | --- | --- |
| STARTTLS command syntax | 2.2 | request parser and request builder tests | covered | None known. |
| STARTTLS capability advertisement | 3.2, 6 | CAPABILITIES exact-output test rejects advertising STARTTLS without TLS support | covered | None known. |
| STARTTLS unavailable behavior | 2.2 | server returns `502 command unavailable`; response-frame validation covers STARTTLS-specific errors | covered | TLS negotiation itself is not implemented. |
| Post-STARTTLS command buffering | 2.2 | buffered-plaintext rejection test exists for unavailable STARTTLS behavior | covered | If TLS support is added, this needs a new negotiated-state matrix. |

## RFC 4643 AUTHINFO

| Area | RFC sections | Current evidence | Status | Gaps / next action |
| --- | --- | --- | --- | --- |
| AUTHINFO USER/PASS syntax | 2.3, 3.1, 3.2 | byte-oriented builder/parser tests and B-CHAR validation | covered | None known. |
| AUTHINFO advertisement | 2.1, 3.4 | CAPABILITIES exact-output test rejects USER/PASS/SASL advertisement without support | covered | None known. |
| AUTHINFO before TLS | 2.3.2 | server rejects plaintext USER/PASS with `483`; PASS sequencing tests | covered | If TLS support is added, add authenticated-state behavior. |
| AUTHINFO SASL syntax | 2.4.1, 3.3-3.5, 7.2 | mechanism and initial-response tests; long SASL line handling | covered | SASL negotiation is not implemented and should remain unadvertised. |
| Protected commands after authentication | 2.1-2.4 | no protected-command mode exists | not in scope | If auth support is added, this becomes a new compliance matrix. |

## RFC 4644 STREAMING

| Area | RFC sections | Current evidence | Status | Gaps / next action |
| --- | --- | --- | --- | --- |
| CHECK syntax and responses | 2.4.1 | request parser/builder tests; response-frame message-id validation | covered | Server rejects unavailable streaming; see state matrix follow-up. |
| TAKETHIS syntax and command continuation | 2.5.1 | request parser/builder tests; multiline body scanner and dot-stuffing tests | covered | Server unavailable behavior and pipelined body handling tracked by `nntpbench-3k5`. |
| STREAMING capability advertisement | 3.4, 6 | CAPABILITIES validation rejects STREAMING arguments; server exact-output omits STREAMING | covered | None known. |
| Accepted/rejected transfer semantics | 2.4-2.5 | response-frame parser validates 238/431/438 and 239/439 shapes | partial | No server-side accepted transfer implementation; unavailable behavior needs final state matrix under `nntpbench-3k5`. |

## RFC 2980 Legacy Extensions

| Area | RFC sections | Current evidence | Status | Gaps / next action |
| --- | --- | --- | --- | --- |
| XOVER request and response shape | 2.8 | request parser/builder tests; response-frame required-field validation; server selector tests | partial | Complete selected-group/current-article server-state coverage under `nntpbench-3k5`. |
| XHDR request and response shape | 2.6 | request parser/builder tests; 221 response validation; server header-value tests | partial | Complete server-state coverage for invalid header, missing selector, and current-article forms under `nntpbench-3k5`. |
| Other RFC 2980 extensions | various | not advertised by the current server | not in scope | Add tests only if the server starts advertising or implementing them. |

## Remaining Beads Work

- `nntpbench-3k5`: finish the server command-state response matrix.
- `nntpbench-v4f`: rename green RFC tests away from the historical `red` prefix.
- `nntpbench-hb5`: audit benchmark client/server workloads after RFC strictness.
- `nntpbench-ikk`: run final Nix validation once the above issues close.

This file satisfies `nntpbench-su3` by making the current RFC evidence explicit
and recording where evidence remains partial or follow-up-tracked.
