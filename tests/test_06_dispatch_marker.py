"""The "not yet sent" marker: who may re-send a RESERVE message, and when.

Found by a two-agent design review (2026-11-11): the old reconciler re-sent every
PENDING order older than 300 s. That included orders waiting in a backlog and
orders whose message was already in the DLQ, so the DLQ was replayed every five
minutes with no operator and no reason. These tests pin the fix.
"""

from __future__ import annotations

import pytest

from orders.adapters.memory import drain
from orders.domain import Status
from orders.ports import TransientStoreError
from orders.service import FAULT_AFTER_SEND, FAULT_BEFORE_COMMIT, EnqueueFailed

from .conftest import request


class MarkFailed(Exception):
    pass


def test_reconciler_does_not_resend_a_dead_lettered_order(service, store, queue, faults, clock):
    """Fails on the pre-fix code: the DLQ'd order was PENDING and older than 300 s."""
    order = service.submit("mark-key-0001", request()).order
    faults.arm(FAULT_BEFORE_COMMIT, TransientStoreError, times=99)
    drain(queue, service)
    assert queue.dlq.depth() == 1 and store.get_order(order.order_id).status is Status.PENDING

    clock.advance(301)
    assert service.reconcile_undispatched(older_than_s=300) == []
    assert queue.depth() == 0 and queue.dlq.depth() == 1  # replay stays an operator action


def test_reconciler_does_not_resend_a_backlog_that_is_merely_waiting(service, store, queue, clock):
    """Fails on the pre-fix code: a slow drain (> 300 s) got extra copies of every queued order."""
    for n in range(5):
        service.submit(f"mark-key-01{n:02d}", request())
    assert queue.depth() == 5

    clock.advance(301)  # worker has not caught up yet
    assert service.reconcile_undispatched(older_than_s=300) == []
    assert queue.depth() == 5


def test_reconciler_sends_only_orders_whose_send_failed(service, store, queue, clock):
    queue.fail_sends = 1
    with pytest.raises(EnqueueFailed):
        service.submit("mark-key-0201", request())
    service.submit("mark-key-0202", request())  # this one was sent
    clock.advance(301)

    assert service.reconcile_undispatched(older_than_s=300) == ["order-1"]
    assert queue.depth() == 2
    assert store.get_order("order-1").dispatched
    assert service.reconcile_undispatched(older_than_s=300) == []  # marked, so not sent again


def test_idempotent_retry_of_a_sent_pending_order_sends_nothing(service, queue):
    first = service.submit("mark-key-0301", request())
    assert queue.depth() == 1
    again = service.submit("mark-key-0301", request())
    assert again.order.order_id == first.order.order_id
    assert queue.depth() == 1  # old code sent a second copy on every retry


def test_send_ok_but_mark_fails_gives_one_duplicate_and_one_reservation(service, store, queue, faults, clock):
    faults.arm(FAULT_AFTER_SEND, MarkFailed)
    with pytest.raises(MarkFailed):
        service.submit("mark-key-0401", request())
    assert queue.depth() == 1 and not store.get_order("order-1").dispatched

    clock.advance(301)
    assert service.reconcile_undispatched(older_than_s=300) == ["order-1"]  # one extra message
    assert queue.depth() == 2

    drain(queue, service)
    assert store.available("WIDGET") == 8  # stock consumed once


def test_any_status_change_clears_the_marker(service, store, queue, faults):
    faults.arm(FAULT_AFTER_SEND, MarkFailed)
    with pytest.raises(MarkFailed):
        service.submit("mark-key-0501", request())
    assert not store.get_order("order-1").dispatched
    drain(queue, service)  # the message was in the queue after all
    assert store.get_order("order-1").status is Status.RESERVED
    assert store.get_order("order-1").dispatched


def test_reconciler_pages_past_the_first_hundred(service, store, queue, clock):
    queue.fail_sends = 150
    for n in range(150):
        with pytest.raises(EnqueueFailed):
            service.submit(f"mark-key-1{n:03d}", request())
    clock.advance(301)
    assert len(service.reconcile_undispatched(older_than_s=300, limit=1000)) == 150
    assert queue.depth() == 150


def test_same_key_from_two_customers_is_two_orders(service, store):
    a = service.submit("shared-key-001", request(customer_id="cust-A"))
    b = service.submit("shared-key-001", request(customer_id="cust-B"))
    assert a.created and b.created and a.order.order_id != b.order.order_id
