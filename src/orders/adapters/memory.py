"""In-memory adapters with the same atomic semantics as the DynamoDB adapter.

One lock stands in for a DynamoDB transaction: every store method checks all
its conditions first and writes only if all pass. The SQS fake models
at-least-once delivery, receive counts and a redrive to a DLQ.
"""

from __future__ import annotations

import copy
import threading
import uuid
from collections import deque
from dataclasses import dataclass, replace

from ..domain import AuditEntry, IdempotencyRecord, Line, Order, Status
from ..ports import CancelResult, DlqMessage, ReserveResult


class InMemoryStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._orders: dict[str, Order] = {}
        self._idem: dict[str, IdempotencyRecord] = {}
        self._stock: dict[str, int] = {}
        self._audit: list[AuditEntry] = []

    def create_order(self, order: Order, record: IdempotencyRecord, now: int) -> IdempotencyRecord | None:
        with self._lock:
            held = self._idem.get(record.key)
            if held is not None and held.is_live(now):
                return held
            if order.order_id in self._orders:
                raise RuntimeError("order id collision")
            self._idem[record.key] = record
            self._orders[order.order_id] = copy.deepcopy(order)
            return None

    def get_idempotency(self, key: str) -> IdempotencyRecord | None:
        with self._lock:
            return self._idem.get(key)

    def get_order(self, order_id: str) -> Order | None:
        with self._lock:
            order = self._orders.get(order_id)
            return copy.deepcopy(order) if order else None

    def reserve(self, order_id: str, lines: tuple[Line, ...], now: int) -> ReserveResult:
        with self._lock:
            order = self._orders.get(order_id)
            if order is None or order.status is not Status.PENDING:
                return ReserveResult.ORDER_NOT_PENDING
            if any(l.sku not in self._stock or self._stock[l.sku] < l.qty for l in lines):
                return ReserveResult.INSUFFICIENT_STOCK
            for l in lines:
                self._stock[l.sku] -= l.qty
            self._orders[order_id] = replace(order, status=Status.RESERVED, version=order.version + 1, updated_at=now)
            return ReserveResult.RESERVED

    def transition(self, order_id: str, expected: Status, to: Status, now: int, reason: str | None = None) -> bool:
        with self._lock:
            order = self._orders.get(order_id)
            if order is None or order.status is not expected:
                return False
            self._orders[order_id] = replace(
                order, status=to, version=order.version + 1, updated_at=now, reason=reason or order.reason
            )
            return True

    def cancel(self, order: Order, now: int) -> CancelResult:
        with self._lock:
            current = self._orders.get(order.order_id)
            cancellable = (Status.PENDING, Status.RESERVED)
            if current is None or current.status is not order.status or order.status not in cancellable:
                return CancelResult.STATE_CHANGED
            if current.status is Status.RESERVED:
                for l in current.lines:
                    self._stock[l.sku] += l.qty
            self._orders[order.order_id] = replace(
                current, status=Status.CANCELLED, version=current.version + 1, updated_at=now
            )
            return CancelResult.CANCELLED

    def list_by_status(self, status: Status, created_before: int, limit: int) -> list[Order]:
        with self._lock:
            hits = [o for o in self._orders.values() if o.status is status and o.created_at < created_before]
            return [copy.deepcopy(o) for o in sorted(hits, key=lambda o: o.created_at)[:limit]]

    def append_audit(self, entry: AuditEntry) -> None:
        with self._lock:
            self._audit.append(entry)

    def audit_log(self, subject: str) -> list[AuditEntry]:
        with self._lock:
            return [e for e in self._audit if e.subject == subject]

    def available(self, sku: str) -> int | None:
        with self._lock:
            return self._stock.get(sku)

    def set_stock(self, sku: str, available: int) -> None:
        with self._lock:
            self._stock[sku] = available

    def order_count(self) -> int:
        with self._lock:
            return len(self._orders)


@dataclass
class _Msg:
    message_id: str
    body: dict
    receive_count: int = 0


class InMemorySqs:
    """At-least-once queue with SQS-style redrive after ``max_receive_count`` receives."""

    def __init__(self, max_receive_count: int = 5) -> None:
        self.max_receive_count = max_receive_count
        self._lock = threading.Lock()
        self._ready: deque[_Msg] = deque()
        self._inflight: dict[str, _Msg] = {}
        self.dlq = InMemoryDlq()
        self.fail_sends = 0  # test hook: make the next N sends raise

    def send(self, body: dict) -> str:
        with self._lock:
            if self.fail_sends > 0:
                self.fail_sends -= 1
                raise ConnectionError("simulated SQS outage")
            msg = _Msg(message_id=str(uuid.uuid4()), body=copy.deepcopy(body))
            self._ready.append(msg)
            return msg.message_id

    def receive(self) -> tuple[str, dict, int] | None:
        """Returns (receipt, body, receive_count) or None. Body carries ``_message_id``."""
        with self._lock:
            if not self._ready:
                return None
            msg = self._ready.popleft()
            msg.receive_count += 1
            receipt = f"{msg.message_id}:{msg.receive_count}"
            self._inflight[receipt] = msg
            return receipt, {**msg.body, "_message_id": msg.message_id}, msg.receive_count

    def ack(self, receipt: str) -> None:
        with self._lock:
            self._inflight.pop(receipt)

    def nack(self, receipt: str) -> None:
        """Visibility timeout expired: redeliver, or redrive to the DLQ."""
        with self._lock:
            msg = self._inflight.pop(receipt)
            if msg.receive_count >= self.max_receive_count:
                self.dlq.put(msg)
            else:
                self._ready.append(msg)

    def depth(self) -> int:
        with self._lock:
            return len(self._ready) + len(self._inflight)


class InMemoryDlq:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._msgs: dict[str, _Msg] = {}

    def put(self, msg: _Msg) -> None:
        with self._lock:
            self._msgs[msg.message_id] = msg

    def receive(self, max_messages: int) -> list[DlqMessage]:
        with self._lock:
            out = []
            for msg in list(self._msgs.values())[:max_messages]:
                out.append(
                    DlqMessage(
                        message_id=msg.message_id,
                        receipt=msg.message_id,
                        body=copy.deepcopy(msg.body),
                        receive_count=msg.receive_count,
                    )
                )
            return out

    def delete(self, receipt: str) -> None:
        with self._lock:
            self._msgs.pop(receipt)

    def depth(self) -> int:
        with self._lock:
            return len(self._msgs)


def drain(queue: InMemorySqs, service, max_deliveries: int = 1000) -> list:
    """Deliver until the queue is empty. Mirrors the Lambda worker: exception -> nack."""
    outcomes = []
    for _ in range(max_deliveries):
        got = queue.receive()
        if got is None:
            return outcomes
        receipt, body, _count = got
        try:
            outcomes.append(service.process(body))
        except Exception as exc:  # noqa: BLE001 - any failure means "not acked"
            outcomes.append(exc)
            queue.nack(receipt)
        else:
            queue.ack(receipt)
    raise RuntimeError("queue did not drain")
