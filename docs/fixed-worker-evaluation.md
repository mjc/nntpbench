# Tokio Fixed-Worker Evaluation

This note records the explored alternative behind `nntpbench-sov`.

## Candidate design

One possible Tokio-only worker model would keep the existing async I/O stack but
route accepted sockets to a bounded set of long-lived workers. Each worker would
own the connection loop for a subset of peers instead of spawning a fresh task
per accepted socket.

The intended wins would be:

- less task churn under rapid connect/disconnect workloads
- slightly tighter locality for repeated request handling on the same worker
- an easier place to compare per-worker buffering or queueing strategies later

## Current baseline

The current benchmark surface now includes a churn mode:

```bash
CHURN_TOTAL_CONNECTIONS=256 BUILD=0 RUNS=10 ./scripts/direct-e2e-bench.sh --churn
```

That mode reports `connections/sec` in addition to throughput, elapsed time,
CPU, and RSS. It gives us a direct baseline for the current task-per-connection
server model.

Heaptrack comparison on the server startup profile:

- baseline trace: `target/heaptrack/direct-e2e-server.zst`
- current trace: `target/heaptrack/current-jm8.zst`

Observed numbers:

- baseline: 676 allocation calls, 3.31M peak consumption
- current: 588 allocation calls, 1.83M peak consumption

## Conclusion

The current Tokio task-per-connection model is already lean enough that the
worker-model complexity is not justified yet. Keep it as the default until a
future benchmark shows a clear win from a fixed-worker prototype.
