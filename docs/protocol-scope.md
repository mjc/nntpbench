# NNTP Benchmark Scope

nntpbench is primarily a benchmark and profiling harness for an async NNTP-like
client/server implementation. It intentionally exercises a broad protocol
surface, but not every RFC feature is guaranteed to be present as a complete
production-grade session model.

## In scope

- command parsing and response framing for the existing client/server surfaces
- selected stateful NNTP behavior when it is needed for benchmark realism
- protocol conformance work that keeps the benchmark outputs honest
- throughput, latency, churn, and profiling runs

## Not automatically in scope

- full TLS transport upgrades unless explicitly implemented and tested
- complete SASL session negotiation unless explicitly implemented and tested
- posting/transit continuation flows beyond the current benchmark persona
- capability advertisement that claims support the implementation does not have
- benchmark claims that do not identify workload shape, connection model, or
  transfer accounting clearly

## Working rule

When a feature affects both protocol correctness and benchmark behavior, prefer
to make the state transition explicit in the code and then document the chosen
persona here. That keeps the hot path honest without pretending the crate is a
general-purpose NNTP server by default.
