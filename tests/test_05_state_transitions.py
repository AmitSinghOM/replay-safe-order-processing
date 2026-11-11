"""Failure 5: unexpected events are rejected or reconciled explicitly."""

from __future__ import annotations

import itertools

import pytest

from orders.adapters.memory import drain
from orders.domain import TRANSITIONS, Event, InvalidTransition, Status, next_status
from orders.service import WorkOutcome

from .conftest import request


@pytest.mark.parametrize("status,event", list(itertools.product(Status, Event)))
def test_transition_table_is_exhaustive(status, event):
    if (status, event) in TRANSITIONS:
        assert next_status(status, event) is TRANSITIONS[(status, event)]
    else:
        with pytest.raises(InvalidTransition):
            next_status(status, event)


def test_reserve_after_cancel_is_ignored_and_audited(service, store, queue):
    order = service.submit("state-key-0001", request()).order
    service.apply_event(order.order_id, Event.CANCEL)  # cancel overtakes the queued RESERVE

    assert drain(queue, service) == [WorkOutcome.IGNORED_INVALID]
    assert store.available("WIDGET") == 10
    assert "RESERVE_IGNORED" in [e.action for e in store.audit_log(order.order_id)]


def test_cancel_after_reserve_releases_stock_once(service, store, queue):
    order = service.submit("state-key-0002", request()).order
    drain(queue, service)
    assert store.available("WIDGET") == 8

    service.apply_event(order.order_id, Event.CANCEL)
    service.apply_event(order.order_id, Event.CANCEL)  # duplicate cancel is a no-op

    assert store.available("WIDGET") == 10
    assert store.get_order(order.order_id).status is Status.CANCELLED


def test_confirm_before_reserve_is_rejected(service):
    order = service.submit("state-key-0003", request()).order
    with pytest.raises(InvalidTransition):
        service.apply_event(order.order_id, Event.CONFIRM)


def test_cancel_after_confirm_is_rejected(service, queue):
    order = service.submit("state-key-0004", request()).order
    drain(queue, service)
    service.apply_event(order.order_id, Event.CONFIRM)
    with pytest.raises(InvalidTransition):
        service.apply_event(order.order_id, Event.CANCEL)


def test_out_of_stock_rejects_without_partial_reservation(service, store, queue):
    order = service.submit(
        "state-key-0005", request(lines=[{"sku": "WIDGET", "qty": 1}, {"sku": "GADGET", "qty": 4}])
    ).order
    assert drain(queue, service) == [WorkOutcome.REJECTED]
    assert store.get_order(order.order_id).status is Status.REJECTED
    assert store.get_order(order.order_id).reason == "OUT_OF_STOCK"
    assert store.available("WIDGET") == 10  # all-or-nothing
    assert store.available("GADGET") == 3


def test_unknown_sku_is_rejected(service, store, queue):
    order = service.submit("state-key-0006", request(lines=[{"sku": "NOPE", "qty": 1}])).order
    assert drain(queue, service) == [WorkOutcome.REJECTED]
    assert store.get_order(order.order_id).status is Status.REJECTED


def test_malformed_message_goes_to_dlq(service, queue):
    queue.send({"type": "SHIP", "order_id": "x"})
    outcomes = drain(queue, service)
    assert len(outcomes) == queue.max_receive_count
    assert queue.dlq.depth() == 1
