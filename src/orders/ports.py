"""Ports the service depends on. Adapters: ``adapters.memory`` (tests) and
``adapters.aws`` (DynamoDB + SQS).

Each store method is ONE atomic unit. That is the transaction boundary: either
all of its writes commit or none do. Business effects outside the store (e.g. a
future email or payment call) are NOT covered and need their own idempotency.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Protocol

from .domain import AuditEntry, IdempotencyRecord, Line, Order, Status


class ReserveResult(enum.Enum):
    RESERVED = "RESERVED"  # order moved PENDING -> RESERVED and stock decremented, together
    ORDER_NOT_PENDING = "ORDER_NOT_PENDING"  # nothing written; order already moved on
    INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK"  # nothing written; some sku short or unknown


class CancelResult(enum.Enum):
    CANCELLED = "CANCELLED"  # status changed (and stock released if it was reserved)
    STATE_CHANGED = "STATE_CHANGED"  # status moved under us; caller re-reads


class TransientStoreError(Exception):
    """Retryable: throttling or a transaction conflict. Worker lets SQS redeliver."""


class OrderStore(Protocol):
    def create_order(self, order: Order, record: IdempotencyRecord, now: int) -> IdempotencyRecord | None:
        """Write order + idempotency record atomically. The order starts undispatched.

        Returns None when written. Returns the existing LIVE record when the key
        is already held (nothing written). An expired record may be replaced.
        """

    def get_idempotency(self, key: str) -> IdempotencyRecord | None: ...

    def get_order(self, order_id: str) -> Order | None:
        """Strongly consistent read."""

    def mark_dispatched(self, order_id: str) -> None:
        """Record that the RESERVE message is in the queue. Idempotent."""

    def list_undispatched(self, created_before: int, limit: int) -> list[str]:
        """Order ids whose message may never have been sent. Eventually consistent; reconciler only."""

    def reserve(self, order_id: str, lines: tuple[Line, ...], now: int, audit: AuditEntry) -> ReserveResult:
        """Atomically: status PENDING->RESERVED (condition), decrement each sku
        (condition available>=qty) and write the audit row. All or nothing."""

    def transition(self, order_id: str, expected: Status, to: Status, now: int, reason: str | None = None) -> bool:
        """Conditional status change with no stock effect. False if status != expected."""

    def cancel(self, order: Order, now: int) -> CancelResult:
        """Cancel from ``order.status``; if RESERVED, release its stock in the same transaction."""

    def append_audit(self, entry: AuditEntry) -> None: ...

    def audit_log(self, subject: str) -> list[AuditEntry]: ...

    def available(self, sku: str) -> int | None: ...

    def set_stock(self, sku: str, available: int) -> None:
        """Seeding helper for tests and demos."""


class Queue(Protocol):
    def send(self, body: dict) -> str:
        """Returns the message id."""


@dataclass(frozen=True)
class DlqMessage:
    message_id: str
    receipt: str
    body: dict
    receive_count: int
    attributes: dict = field(default_factory=dict)


class DeadLetterQueue(Protocol):
    def receive(self, max_messages: int) -> list[DlqMessage]: ...

    def delete(self, receipt: str) -> None: ...
