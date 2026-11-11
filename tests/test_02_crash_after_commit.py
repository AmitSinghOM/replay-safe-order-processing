"""Failure 2: worker commits state, then crashes before acking the message."""

from __future__ import annotations

import pytest

from orders.adapters.memory import InMemoryStore, drain
from orders.domain import Line, Status
from orders.ports import ReserveResult
from orders.service import FAULT_AFTER_COMMIT, FAULT_BEFORE_COMMIT, WorkOutcome

from .conftest import request


class WorkerCrash(Exception):
    pass


def test_redelivery_after_commit_does_not_reserve_again(service, store, queue, faults):
    order = service.submit("crash-key-0001", request()).order
    faults.arm(FAULT_AFTER_COMMIT, WorkerCrash)

    outcomes = drain(queue, service)

    assert isinstance(outcomes[0], WorkerCrash)  # first delivery: committed, then crashed
    assert outcomes[1] is WorkOutcome.ALREADY_APPLIED  # redelivery: no second effect
    assert store.available("WIDGET") == 8
    assert store.get_order(order.order_id).status is Status.RESERVED
    assert queue.depth() == 0 and queue.dlq.depth() == 0
    actions = [e.action for e in store.audit_log(order.order_id)]
    assert actions.count("RESERVED") == 1
    assert "DUPLICATE_DELIVERY" in actions


def test_crash_before_commit_leaves_nothing_and_retry_applies_once(service, store, queue, faults):
    order = service.submit("crash-key-0002", request()).order
    faults.arm(FAULT_BEFORE_COMMIT, WorkerCrash)

    outcomes = drain(queue, service)

    assert isinstance(outcomes[0], WorkerCrash)
    assert outcomes[1] is WorkOutcome.RESERVED
    assert store.available("WIDGET") == 8
    assert store.get_order(order.order_id).status is Status.RESERVED


class StoreWithoutStatusGuard(InMemoryStore):
    """The bug this design prevents: decrement stock without checking order status."""

    def reserve(self, order_id: str, lines: tuple[Line, ...], now: int) -> ReserveResult:
        with self._lock:
            for l in lines:
                self._stock[l.sku] -= l.qty
            order = self._orders[order_id]
            order.status = Status.RESERVED
            return ReserveResult.RESERVED


def test_guard_is_what_makes_it_safe(queue, clock, faults):
    """Discriminating check: without the status condition the same crash double-reserves."""
    from orders.service import OrderService

    store = StoreWithoutStatusGuard()
    store.set_stock("WIDGET", 10)
    svc = OrderService(store, queue, clock=clock, id_factory=lambda: "order-x", faults=faults)
    svc.submit("crash-key-0003", request())
    faults.arm(FAULT_AFTER_COMMIT, WorkerCrash)

    drain(queue, svc)

    assert store.available("WIDGET") == 6  # 2 reserved twice


@pytest.mark.parametrize("redeliveries", [1, 3])
def test_many_duplicate_messages_reserve_once(service, store, queue, redeliveries):
    order = service.submit("crash-key-0004", request()).order
    for _ in range(redeliveries):
        queue.send({"type": "RESERVE", "order_id": order.order_id})
    drain(queue, service)
    assert store.available("WIDGET") == 8
