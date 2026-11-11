# Architecture

## Problem

A small service takes orders asynchronously. Failed work must be replayable
without reserving inventory twice.

Delivery is at-least-once. This design does **not** claim exactly-once
delivery. It claims duplicate-safe *effects* inside one transaction boundary.

## Components

```
 client (SigV4)
     |
     v
 API Gateway (REST, AWS_IAM) --> ApiFunction --------+---------------------+
                                     | TransactWrite  | SendMessage         |
                                     v                v                     |
                               DynamoDB table     OrdersQueue --(5 receives)--> OrdersDLQ
                               (single table)         |                         |
                                     ^                | event source            | scripts/replay_dlq.py
                                     | TransactWrite  | max concurrency 2       | (audited, dry-run first)
                                     +---------- WorkerFunction <---------------+
                                     ^
                                     | Query gsi1 (UNSENT, old)
                               ReconcileFunction (every 5 min) --> OrdersQueue
```

One IaC tool: AWS SAM (`template.yaml`).

## Table layout

| pk | sk | attributes |
|---|---|---|
| `ORDER#<id>` | `META` | status, customer_id, lines, version, created_at, updated_at, reason; gsi1pk=`UNSENT` + gsi1sk=created_at **only while the message may be unsent** |
| `IDEMP#<customer_id>#<key>` | `META` | request_fingerprint, order_id, expires_at (TTL) |
| `SKU#<sku>` | `META` | available |
| `AUDIT#<subject>` | `<at>#<uuid>` | action, detail |

## Transaction boundaries

Each row is one DynamoDB `TransactWriteItems` call. All writes commit or none do.

| Step | Writes | Conditions | Duplicate outcome |
|---|---|---|---|
| Create order | `IDEMP#cust#key` + `ORDER#id` (marked UNSENT) | key absent or expired; order absent | returns the held record; same fingerprint -> same order, different -> 409 |
| Reserve | `ORDER#id` status PENDING->RESERVED + `SKU#*` decrement + `AUDIT` row | `status = PENDING`; `available >= qty` per sku | status check fails -> worker re-reads -> `ALREADY_APPLIED`, acks |
| Reject | `ORDER#id` PENDING->REJECTED | `status = PENDING` | same as above |
| Cancel | `ORDER#id` ->CANCELLED (+ `SKU#*` increment if it was RESERVED) | `status = <status read>` | repeat cancel returns the cancelled order; no second release |
| Confirm | `ORDER#id` RESERVED->CONFIRMED | `status = RESERVED` | repeat confirm is a no-op |

The order's own status is the dedupe record. No separate processed-message
table is needed, because the effect (stock decrement), the marker (status)
and the audit row commit together. A crash anywhere after the commit leaves
a complete record.

**Outside the boundary:** the SQS send after create, and any future external
effect (email, payment, shipping). External effects need downstream
idempotency keys or reconciliation; none exist in this scope.

### The send gap and the "not yet sent" marker

The create transaction cannot include the SQS send. So the order item starts
with a sparse-index key (`gsi1pk = UNSENT`). The API sends the message, then
removes the key (`mark_dispatched`). Three things may re-send a RESERVE
message, and all of them check this marker first:

| Path | Re-sends when |
|---|---|
| Client retry with the same Idempotency-Key | order still marked UNSENT |
| ReconcileFunction (every 5 min) | order marked UNSENT for > 300 s |
| `scripts/replay_dlq.py` | operator decides; audited with operator + reason |

Any status change also removes the key: the worker can only have acted on a
delivered message. If the send succeeds but the mark fails, one extra message
goes out later; the worker's status guard makes it harmless
(`tests/test_06_dispatch_marker.py`).

Why this matters: an earlier version re-sent every PENDING order older than
300 s. That replayed dead-lettered orders every five minutes with no operator,
and injected duplicates into any backlog that took longer than 300 s to drain.

## State machine

```
PENDING --RESERVE--> RESERVED --CONFIRM--> CONFIRMED
   |                    |
   +--REJECT--> REJECTED +--CANCEL--> CANCELLED (stock released)
   +--CANCEL--> CANCELLED
```

Every other `(status, event)` pair raises `InvalidTransition` (API 409). A
RESERVE that arrives after CANCEL is acked and audited as `RESERVE_IGNORED`.

## Failure matrix

| # | Failure | Mechanism | Evidence |
|---|---|---|---|
| 1 | Same request twice | Idempotency record in the create transaction; 24 h window; fingerprint of normalized body | `tests/test_01_duplicate_request.py` (incl. 20 concurrent submits) |
| 2 | Crash after commit, before ack | Conditional reserve on `status = PENDING`; audit row in the same transaction | `tests/test_02_crash_after_commit.py` (incl. a guard-removed store that double-reserves) |
| 3 | Message keeps failing | `maxReceiveCount` 5 -> DLQ -> alarm -> `scripts/replay_dlq.py` (audit row before send; settled orders skipped); reconciler never touches dispatched orders | `tests/test_03_dlq_replay.py`, `tests/test_06_dispatch_marker.py` |
| 4 | Dependency slows down | Event-source `MaximumConcurrency` + API throttling; delay knob before the commit | `tests/test_04_backlog_recovery.py` + local run; AWS curve via `loadtest/aws_run.py`, **not yet run** ([loadtest.md](loadtest.md)) |
| 5 | Unexpected events | Explicit transition table; conditional writes; re-read on lost race | `tests/test_05_state_transitions.py` |

`tests/test_aws_adapter.py` re-runs cases 1, 2 and 5 against moto to check
the DynamoDB condition and cancellation-reason handling, the sparse index and
the single-transaction audit. `tests/test_template.py` pins template invariants
(visibility >= 6x timeout, KEYS_ONLY index, both alarms, IAM auth).

## How this compares with published patterns

Checked 2026-11-11. Content below is paraphrased.

| Source | Their mechanism | Here |
|---|---|---|
| [Classmethod: SQS -> Lambda -> DynamoDB idempotency](https://dev.classmethod.jp/en/articles/sqs-lambda-db-notification-idempotency/) | `processed_events` state row + business row in one `TransactWriteItems`; publish outside; branch on status on redelivery | Same shape. The order's status *is* the state row, since RESERVE is the only message type. |
| [AWS Builder Center: self-healing outbox](https://builder.aws.com/content/3KffagiVsGsGNIiQ6Kvm8S9kIzW) | Outbox row in the business transaction; Streams fast path; scheduled reconciliation over a status GSI; alarm on oldest pending age | Outbox row = the UNSENT key on the order. No Streams (see Decisions). Reconciler + `QueueAgeAlarm` match. |
| [Lambda + SQS event source docs](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html) | Visibility >= 6x function timeout; `maxReceiveCount` >= 5; partial batch responses | 180 s / 30 s, 5, `ReportBatchItemFailures`. |
| [DynamoDB transactions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html) | 2 WCU per transactional item; `ClientRequestToken` for idempotent calls | Two transactions per order on the hot path. No manual token (see Decisions). |
| [Powertools idempotency](https://docs.aws.amazon.com/powertools/python/latest/utilities/idempotency/) | INPROGRESS/COMPLETE record keyed by payload hash | Not used; same idea, visible in code. |

## Decisions

- **SAM over CDK/Terraform.** Python-only repo, smallest template for these
  five services, `sam delete` gives one-command teardown.
- **Standard queue, not FIFO.** Order is not needed; the state machine handles
  reordering. FIFO dedupe is only 5 minutes and would hide, not solve, duplicates.
- **Status as dedupe marker.** One fewer table and no TTL race on a
  processed-message set.
- **Lambda Powertools idempotency not used.** It would hide the mechanism this
  repo exists to show.
- **AWS_IAM auth on the API.** Callers are services. No anonymous access.
- **Retries are uniform.** Every worker error is retried, then dead-lettered.
  Permanent errors (malformed message, unknown order) reach the DLQ after 5
  attempts instead of being dropped silently.
- **No DynamoDB Streams outbox.** It would remove the API's direct send, but
  adds a hop with its own retry, bisect and failure-destination settings, and
  still needs the reconciler. The UNSENT marker closes the gap in plain code.
  Revisit if an AWS run shows send failures the reconciler cannot cover.
- **No `ClientRequestToken` on the reserve transaction.** botocore already
  fills one in for SDK retries. An app-level token keyed by message id would
  fail with `IdempotentParameterMismatch`, because `updated_at` changes on
  every redelivery. The status condition is the idempotency check.
- **Records in a batch run sequentially.** Threads inside the worker would
  multiply load on the slow dependency while the Lambda concurrency cap still
  reads 2. The cap is the control; it must mean what it says.
- **REST API stays, not HTTP API.** HTTP API is cheaper per request, but the
  saving is cents at this scale, cannot be shown by a test, and the handlers
  and tests use the REST v1 event shape. Documented, not done.
- **Idempotency keys are scoped per customer** (`IDEMP#<customer_id>#<key>`),
  as payment APIs scope them per account. Two customers reusing the same key
  string get two orders, not a 409.

## Known limits

- The create transaction and the mark-dispatched update are two writes per
  order; a Streams outbox would save the second at the cost of a component.
- `gsi1pk = UNSENT` is a single partition key, but the index only holds
  orders between create and send (normally near zero).
- Max 10 lines per order keeps the reserve transaction small (12 items with
  the audit row).
- Reconciler caps one run at 1000 orders; a bigger outage takes several runs.
