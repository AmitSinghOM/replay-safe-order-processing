# Replay-Safe Order Processing on AWS

Async order intake that can retry and replay failed work without reserving
inventory twice.

API Gateway -> Python Lambda -> SQS (+ DLQ) -> Lambda worker -> DynamoDB. One IaC tool: AWS SAM.

**Claim:** duplicate-safe effects inside a DynamoDB transaction boundary.
**Not claimed:** exactly-once delivery. SQS delivers at least once.

Design, transaction boundaries and failure matrix: [docs/architecture.md](docs/architecture.md).

## Status

| Area | State |
|---|---|
| Domain, service, handlers | Done |
| Local failure tests (in-memory + moto + template) | 99 passing |
| SAM template | Written, not yet deployed |
| Load test harness (failure 4) | Built; local run passes; AWS run not yet done |
| Measured AWS cost | Not measured |

Synthetic tests do not show production adoption.

## Run locally

Needs [uv](https://docs.astral.sh/uv/).

```sh
make test            # uv run --python 3.12 pytest
make test-fast       # skip the timing-based load runs
make lint
make loadtest-local  # backlog-recovery run on in-memory adapters
```

## Failure evidence

| Failure | Test file |
|---|---|
| Same request twice | `tests/test_01_duplicate_request.py` |
| Crash after commit, before ack | `tests/test_02_crash_after_commit.py` |
| Message keeps failing -> DLQ -> replay | `tests/test_03_dlq_replay.py` |
| Dependency slows down | `tests/test_04_backlog_recovery.py`; AWS curve: [docs/loadtest.md](docs/loadtest.md) |
| Unexpected events | `tests/test_05_state_transitions.py` |
| Who may re-send a message (client retry, reconciler, operator) | `tests/test_06_dispatch_marker.py` |
| Same cases on the DynamoDB adapter | `tests/test_aws_adapter.py` (moto) |
| Template invariants | `tests/test_template.py` |

## Deploy (not yet run)

Needs AWS SAM CLI and credentials for a sandbox account.

```sh
make deploy STACK=replay-safe-orders
```

The API uses `AWS_IAM` auth. Calls must be SigV4-signed by a principal with
`execute-api:Invoke` on the stack's API.

## DLQ replay

```sh
# dry run: audits each message, changes nothing
make replay-dry-run OPERATOR=<you> REASON="<incident / cause fixed>"
# execute
uv run --python 3.12 --extra aws python scripts/replay_dlq.py \
  --stack replay-safe-orders --operator <you> --reason "<why>" --execute
```

Audit rows land under `pk = AUDIT#replay`.

## Teardown

```sh
make delete STACK=replay-safe-orders
make loadtest-delete   # if you deployed the load-test stack
```

Deletes the table (and its data), both queues, functions, API and alarm.

## Out of scope

Storefront UI, real payments, multi-region, AI assistant.
