"""Use cases. Handlers stay thin and call these.

Delivery is at-least-once (SQS). Safety comes from the store: the reservation
is a conditional transaction on order status, so a redelivered message finds
the order already RESERVED and writes nothing.
"""

from __future__ import annotations

import enum
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, replace

from .domain import (
    AuditEntry,
    Event,
    IdempotencyConflict,
    IdempotencyRecord,
    InvalidTransition,
    Order,
    OrderNotFound,
    OrderRequest,
    Status,
    next_status,
    scoped_idempotency_key,
    validate_idempotency_key,
)
from .ports import CancelResult, DeadLetterQueue, DlqMessage, OrderStore, Queue, ReserveResult, TransientStoreError

log = logging.getLogger(__name__)

DEFAULT_IDEMPOTENCY_WINDOW_S = 24 * 3600

# Named fault points. Production passes no injector; tests raise at these points.
FAULT_BEFORE_COMMIT = "worker.before_commit"
FAULT_AFTER_COMMIT = "worker.after_commit"  # state committed, message not yet acked
FAULT_AFTER_SEND = "api.after_send"  # message queued, order not yet marked dispatched

FaultInjector = Callable[[str], None]


def _no_faults(_point: str) -> None:
    return None


MAX_DEPENDENCY_DELAY_MS = 1500  # keeps a 10-record batch inside the 30 s worker timeout


def dependency_delay(ms: int, sleep: Callable[[float], None] = time.sleep) -> FaultInjector:
    """Stand-in for a slow downstream call, placed just before the commit.

    Load tests only. Set by stack parameter, never by request data.
    """
    if not 0 <= ms <= MAX_DEPENDENCY_DELAY_MS:
        raise ValueError(f"dependency delay must be 0-{MAX_DEPENDENCY_DELAY_MS} ms")
    if ms == 0:
        return _no_faults

    def inject(point: str) -> None:
        if point == FAULT_BEFORE_COMMIT:
            sleep(ms / 1000)

    return inject


class EnqueueFailed(Exception):
    """Order committed but the message was not sent. Client retry or the reconciler re-sends."""


@dataclass(frozen=True)
class SubmitResult:
    order: Order
    created: bool  # False = idempotent replay of an earlier request


class WorkOutcome(enum.Enum):
    RESERVED = "RESERVED"
    REJECTED = "REJECTED"
    ALREADY_APPLIED = "ALREADY_APPLIED"  # duplicate delivery; no effect repeated
    IGNORED_INVALID = "IGNORED_INVALID"  # event no longer valid (e.g. order cancelled first)


@dataclass
class ReplayReport:
    dry_run: bool
    replayed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)  # deleted from DLQ, order already settled
    inspected: list[str] = field(default_factory=list)


_REPORT_FIELD = {"replay": "replayed", "skip_settled": "skipped", "inspect": "inspected"}


class OrderService:
    def __init__(
        self,
        store: OrderStore,
        queue: Queue,
        *,
        clock: Callable[[], float] = time.time,
        id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
        idempotency_window_s: int = DEFAULT_IDEMPOTENCY_WINDOW_S,
        faults: FaultInjector = _no_faults,
    ) -> None:
        self.store = store
        self.queue = queue
        self.clock = clock
        self.id_factory = id_factory
        self.window = idempotency_window_s
        self.faults = faults

    def _now(self) -> int:
        return int(self.clock())

    # ---- API side ---------------------------------------------------------

    def submit(self, idempotency_key: str, request: OrderRequest) -> SubmitResult:
        key = validate_idempotency_key(idempotency_key)
        fingerprint = request.fingerprint()
        now = self._now()
        order = Order(
            order_id=self.id_factory(),
            customer_id=request.customer_id,
            lines=request.lines,
            status=Status.PENDING,
            version=1,
            created_at=now,
            updated_at=now,
        )
        scoped = scoped_idempotency_key(request.customer_id, key)
        record = IdempotencyRecord(scoped, fingerprint, order.order_id, expires_at=now + self.window)

        held = self.store.create_order(order, record, now)
        if held is None:
            self._audit(order.order_id, "CREATED", now, idempotency_key=key)
            self._dispatch(order.order_id)
            return SubmitResult(order=replace(order, dispatched=True), created=True)

        if held.request_fingerprint != fingerprint:
            raise IdempotencyConflict("Idempotency-Key was already used with a different request")
        existing = self.store.get_order(held.order_id)
        if existing is None:  # should not happen: both items are written in one transaction
            raise OrderNotFound(held.order_id)
        if not existing.dispatched:
            # The first attempt committed but its message may never have been sent.
            # Re-sending is safe because the worker is duplicate-safe.
            self._dispatch(existing.order_id)
            existing = replace(existing, dispatched=True)
        return SubmitResult(order=existing, created=False)

    def get(self, order_id: str) -> Order:
        order = self.store.get_order(order_id)
        if order is None:
            raise OrderNotFound(order_id)
        return order

    def apply_event(self, order_id: str, event: Event, *, max_attempts: int = 3) -> Order:
        """External events: CANCEL or CONFIRM. Repeating a settled event is a no-op."""
        target_by_event = {Event.CANCEL: Status.CANCELLED, Event.CONFIRM: Status.CONFIRMED}
        for _ in range(max_attempts):
            order = self.get(order_id)
            if event not in target_by_event:
                raise InvalidTransition(order.status, event)
            if order.status is target_by_event[event]:
                return order  # idempotent repeat
            now = self._now()
            to = next_status(order.status, event)  # raises InvalidTransition -> 409
            if event is Event.CANCEL:
                changed = self.store.cancel(order, now) is CancelResult.CANCELLED
            else:
                changed = self.store.transition(order_id, order.status, to, now)
            if changed:
                self._audit(order_id, event.value, now, from_status=order.status.value)
                return self.get(order_id)
            # Lost a race with the worker or another caller: re-read and decide again.
        raise TransientStoreError(f"order {order_id} kept changing; retry later")

    # ---- worker side ------------------------------------------------------

    def process(self, message: dict) -> WorkOutcome:
        order_id = message.get("order_id")
        if message.get("type") != Event.RESERVE.value or not isinstance(order_id, str):
            raise ValueError(f"unsupported message: {message!r}")  # -> retries -> DLQ
        order = self.get(order_id)
        now = self._now()

        audit = AuditEntry(order_id, "RESERVED", now, {"message_id": message.get("_message_id")})
        self.faults(FAULT_BEFORE_COMMIT)
        result = self.store.reserve(order_id, order.lines, now, audit)  # effect + audit row, one transaction
        if result is ReserveResult.RESERVED:
            self.faults(FAULT_AFTER_COMMIT)  # nothing of ours runs between commit and ack except this
            return WorkOutcome.RESERVED

        if result is ReserveResult.INSUFFICIENT_STOCK:
            if self.store.transition(order_id, Status.PENDING, Status.REJECTED, now, reason="OUT_OF_STOCK"):
                self._audit(order_id, "REJECTED", now, reason="OUT_OF_STOCK")
                return WorkOutcome.REJECTED

        # The order is no longer PENDING. Decide explicitly why.
        current = self.get(order_id)
        if current.status in (Status.RESERVED, Status.CONFIRMED, Status.REJECTED):
            self._audit(order_id, "DUPLICATE_DELIVERY", now, status=current.status.value)
            return WorkOutcome.ALREADY_APPLIED
        if current.status is Status.CANCELLED:
            self._audit(order_id, "RESERVE_IGNORED", now, status=current.status.value)
            return WorkOutcome.IGNORED_INVALID
        raise RuntimeError(f"order {order_id} still PENDING after failed reserve; retrying")

    # ---- operations -------------------------------------------------------

    def replay_dlq(
        self,
        dlq: DeadLetterQueue,
        *,
        operator: str,
        reason: str,
        max_messages: int = 10,
        dry_run: bool = True,
    ) -> ReplayReport:
        """Move dead-lettered messages back to the main queue with an audit trail.

        The audit row is written BEFORE the send, so a crash mid-replay still
        leaves evidence. A duplicate send is harmless because ``process`` is
        duplicate-safe.
        """
        if not operator or not reason:
            raise ValueError("operator and reason are required for an auditable replay")
        report = ReplayReport(dry_run=dry_run)
        for msg in dlq.receive(max_messages):
            decision = self._replay_decision(msg, dry_run)
            self._audit(
                "replay",
                decision.upper(),
                self._now(),
                operator=operator,
                reason=reason,
                message_id=msg.message_id,
                body_sha256=hashlib.sha256(json.dumps(msg.body, sort_keys=True).encode()).hexdigest(),
                receive_count=msg.receive_count,
                order_id=msg.body.get("order_id"),
                order_status=self._order_status(msg.body.get("order_id")),
            )
            getattr(report, _REPORT_FIELD[decision]).append(msg.message_id)
            if decision == "replay":
                self.queue.send({**msg.body, "replay_of": msg.message_id})
            if decision != "inspect":
                dlq.delete(msg.receipt)
        return report

    def _replay_decision(self, msg: DlqMessage, dry_run: bool) -> str:
        if dry_run:
            return "inspect"
        status = self._order_status(msg.body.get("order_id"))
        settled = status is not None and status != Status.PENDING.value
        return "skip_settled" if settled else "replay"

    def _order_status(self, order_id: object) -> str | None:
        order = self.store.get_order(order_id) if isinstance(order_id, str) else None
        return order.status.value if order else None

    def reconcile_undispatched(self, older_than_s: int = 300, limit: int = 1000) -> list[str]:
        """Backstop for the commit-then-enqueue gap: send orders whose message may never have left.

        Only undispatched orders qualify. Orders waiting in a backlog or sitting in
        the DLQ are dispatched, so this never re-sends them; DLQ replay stays an
        explicit, audited operator action.
        """
        now = self._now()
        resent = []
        for order_id in self.store.list_undispatched(now - older_than_s, limit):
            self._dispatch(order_id)
            self._audit(order_id, "RECONCILE_REENQUEUE", now)
            resent.append(order_id)
        return resent

    # ---- helpers ----------------------------------------------------------

    def _dispatch(self, order_id: str) -> None:
        """Send the RESERVE message, then record that it is in the queue.

        If the send fails the order stays undispatched (client retry or reconciler
        re-sends). If the send succeeds but marking fails, one extra message goes
        out later; the worker's status guard makes that harmless.
        """
        try:
            self.queue.send({"type": Event.RESERVE.value, "order_id": order_id})
        except Exception as exc:  # cqa: ignore=PY-COR-002 reason="wrapped as typed EnqueueFailed"
            log.warning("enqueue failed for %s: %s", order_id, exc)
            raise EnqueueFailed(order_id) from exc
        self.faults(FAULT_AFTER_SEND)
        self.store.mark_dispatched(order_id)

    def _audit(self, subject: str, action: str, at: int, **detail: object) -> None:
        self.store.append_audit(AuditEntry(subject=subject, action=action, at=at, detail=detail))
