# Failure 4: dependency slows down

## Question

When the downstream call slows, does the worker stay inside its concurrency
cap, keep every order reserved exactly once, and drain the backlog on a
predictable curve?

## Mechanism

| Control | Where | Value |
|---|---|---|
| Max concurrent workers | SQS event source `ScalingConfig.MaximumConcurrency` | `WorkerMaxConcurrency`, default 2 (AWS minimum) |
| Slow dependency | `dependency_delay()` sleeps at `worker.before_commit` | `DependencyDelayMs`, default 0, max 1500 |
| Batch fits the timeout | 10 records x 1.5 s < 30 s worker timeout; queue visibility 180 s (6x) | template |
| API intake | API Gateway stage throttle | 50 rps, burst 100 |

The delay sits *before* the commit, so a slow dependency holds a worker
slot but cannot change the transaction outcome.

## Model

```
mu     = C * 1000 / (delay_ms + overhead_ms)      drain, msg/s
lambda = arrival_rate * (1 + duplicate_ratio)     offered, msg/s
```

If `mu > lambda`, the backlog drains at `mu - lambda` while arrivals continue,
then at `mu`. Otherwise it grows until arrivals stop. On AWS the model is a
lower bound: SQS polling gaps and cold starts add time. Code: `loadtest/curve.py`.

## Pass criteria (all must hold)

| Check | Fails when |
|---|---|
| `min_run_length` | run shorter than the floor (no vacuous pass on a smoke run) |
| `sample_errors` | any sample failed; failures are recorded, never dropped |
| `backlog_seen` | the queue was never non-empty (nothing was tested) |
| `drained` | backlog never reached a lasting zero |
| `no_pending_orders` | any created order still PENDING |
| `all_orders_reserved` | any order rejected or missing |
| `reserved_exactly_once` | stock consumed != reserved orders x qty, with duplicates injected |
| `dlq_empty` | anything dead-lettered |
| `concurrency_capped` | observed max > cap, **or not measured** |
| `matches_model` | local only: measured/predicted outside +/-50 % |

Each check has a test that makes it fail: `tests/test_04_backlog_recovery.py`.

## Local run (not AWS evidence)

```sh
make loadtest-local
```

Real `OrderService` on in-memory adapters, C threads as C workers.

| Scenario | Model | Measured | Checks |
|---|---|---|---|
| backlog 600, 40/s for 5 s, C=2, delay 20 ms, 10 % duplicates | 8.89 s | 10.36 s (1.17x) | 10/10 pass |

Measured 2026-11-11 on an arm64 Mac, Python 3.12.13. Also in tests:
raising C from 2 to 4 shortens recovery.

## AWS run

Needs: SAM CLI, credentials for a **sandbox** account, permission for
`lambda:UpdateEventSourceMapping` on the load-test stack.

```sh
make loadtest-deploy DELAY_MS=500 CONCURRENCY=2
make loadtest-aws ARGS="--backlog 2000 --arrival-rate 2 --arrival-seconds 300 --duplicate-ratio 0.1"
make loadtest-delete
```

The reconciler is deployed in the load-test stack too. It only re-sends
orders still marked UNSENT, so a preloaded backlog older than 300 s is left
alone and the curve measures the worker, not the reconciler
(`tests/test_06_dispatch_marker.py::test_reconciler_does_not_resend_a_backlog_that_is_merely_waiting`).

Safety rails in `loadtest/aws_run.py`:

- Refuses unless the stack is tagged `purpose=loadtest`.
- Reads delay and cap from the deployed function, not from flags.
- Refuses if the queue or DLQ already holds messages.
- Pauses the event source during preload; re-enables it in `finally`.
- Uses a fresh SKU per run (`LT-<run id>`) so checks see only this run.

Expected (model, 40 ms overhead assumed): C=2, 500 ms -> mu = 3.7 msg/s.
2200 backlog messages, 2.2 msg/s arriving for 300 s -> about 772 s (lower bound). Not yet run.

Output: `results/aws/<time>-<run id>/` with `samples.csv`, `summary.json`
(incl. Lambda invocations, errors, throttles, average duration) and `curve.svg`.

## Cost

Not estimated from list prices. Deploy with `--tags project=replay-safe-orders`
(the Makefile does), activate that cost-allocation tag, then read the real
cost per run in Cost Explorer. Record it in the README with the run id.

## Limits

- Duplicates are injected by the harness, not by SQS itself.
- `--via direct` skips API Gateway to isolate the worker. Use `--via api` for
  the end-to-end path (SigV4-signed).
- CloudWatch `ConcurrentExecutions` has 1-minute resolution.
- One SKU per run: no hot-key contention is tested.
