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
                                     | Query gsi1 (PENDING, old)
                               ReconcileFunction (every 5 min) --> OrdersQueue
```

One IaC tool: AWS SAM (`template.yaml`).

## Table layout

| pk | sk | attributes |
|---|---|---|
| `ORDER#<id>` | `META` | status, customer_id, lines, version, created_at, updated_at, reason, gsi1pk=`STATUS#<s>`, gsi1sk=created_at |
| `IDEMP#<key>` | `META` | request_fingerprint, order_id, expires_at (TTL) |
| `SKU#<sku>` | `META` | available |
| `AUDIT#<subject>` | `<at>#<uuid>` | action, detail |

## Transaction boundaries

Each row is one DynamoDB `TransactWriteItems` call. All writes commit or none do.

| Step | Writes | Conditions | Duplicate outcome |
|---|---|---|---|
| Create order | `IDEMP#key` + `ORDER#id` | key absent or expired; order absent | returns the held record; same fingerprint -> same order, different -> 409 |
| Reserve | `ORDER#id` status PENDING->RESERVED + `SKU#*` decrement | `status = PENDING`; `available >= qty` per sku | status check fails -> worker re-reads -> `ALREADY_APPLIED`, acks |
| Reject | `ORDER#id` PENDING->REJECTED | `status = PENDING` | same as above |
| Cancel | `ORDER#id` ->CANCELLED (+ `SKU#*` increment if it was RESERVED) | `status = <status read>` | repeat cancel returns the cancelled order; no second release |
| Confirm | `ORDER#id` RESERVED->CONFIRMED | `status = RESERVED` | repeat confirm is a no-op |

The order's own status is the dedupe record. No separate processed-message
table is needed, because the effect (stock decrement) and the marker (status)
commit together.

**Outside the boundary:** the SQS send after create, and any future external
effect (email, payment, shipping). The SQS gap is closed by client retry with
the same key plus the reconciler. External effects need downstream
idempotency keys or reconciliation; none exist in this scope.

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
| 2 | Crash after commit, before ack | Conditional reserve on `status = PENDING` | `tests/test_02_crash_after_commit.py` (incl. a guard-removed store that double-reserves) |
| 3 | Message keeps failing | `maxReceiveCount` 5 -> DLQ -> alarm -> `scripts/replay_dlq.py` (audit row before send; settled orders skipped) | `tests/test_03_dlq_replay.py` |
| 4 | Dependency slows down | Event-source `MaximumConcurrency` + API throttling | **Not measured yet.** Needs a deployed load test |
| 5 | Unexpected events | Explicit transition table; conditional writes; re-read on lost race | `tests/test_05_state_transitions.py` |

`tests/test_aws_adapter.py` re-runs cases 1, 2 and 5 against moto to check
the DynamoDB condition and cancellation-reason handling.

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

## Known limits

- Re-enqueue on every idempotent retry of a PENDING order can add duplicate
  messages under a retry storm. Safe, but wasteful.
- Reconciler queries one page of `gsi1` per run (100 orders).
- `gsi1pk = STATUS#PENDING` is a hot partition at high volume.
- Max 10 lines per order keeps the reserve transaction small.
