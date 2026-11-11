"""Failure 1: same request submitted twice -> one logical order within the window."""

from __future__ import annotations

import threading

import pytest

from orders.adapters.memory import drain
from orders.domain import IdempotencyConflict, Status, ValidationError
from orders.service import EnqueueFailed

from .conftest import request

KEY = "client-key-0001"


def test_same_key_same_body_returns_the_same_order(service, store, queue):
    first = service.submit(KEY, request())
    second = service.submit(KEY, request())

    assert first.created and not second.created
    assert second.order.order_id == first.order.order_id
    assert store.order_count() == 1

    assert queue.depth() == 1  # the second submit did not send another RESERVE
    drain(queue, service)
    assert store.get_order(first.order.order_id).status is Status.RESERVED
    assert store.available("WIDGET") == 8  # reserved once, not twice


def test_line_order_does_not_change_the_fingerprint(service, store):
    a = request(lines=[{"sku": "WIDGET", "qty": 1}, {"sku": "GADGET", "qty": 1}])
    b = request(lines=[{"sku": "GADGET", "qty": 1}, {"sku": "WIDGET", "qty": 1}])
    assert service.submit(KEY, a).order.order_id == service.submit(KEY, b).order.order_id


def test_same_key_different_body_is_a_conflict(service, store):
    service.submit(KEY, request())
    with pytest.raises(IdempotencyConflict):
        service.submit(KEY, request(lines=[{"sku": "WIDGET", "qty": 3}]))
    assert store.order_count() == 1


def test_key_is_reusable_after_the_window(service, store, clock):
    first = service.submit(KEY, request())
    clock.advance(service.window + 1)
    second = service.submit(KEY, request())
    assert second.created
    assert second.order.order_id != first.order.order_id
    assert store.order_count() == 2


def test_concurrent_duplicates_create_one_order(service, store):
    results, errors = [], []
    barrier = threading.Barrier(20)

    def submit():
        barrier.wait()
        try:
            results.append(service.submit(KEY, request()))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert store.order_count() == 1
    assert sum(r.created for r in results) == 1
    assert len({r.order.order_id for r in results}) == 1


def test_enqueue_failure_then_client_retry_still_one_order(service, store, queue):
    queue.fail_sends = 1
    with pytest.raises(EnqueueFailed):
        service.submit(KEY, request())
    assert store.order_count() == 1  # committed, not yet enqueued

    retry = service.submit(KEY, request())  # client retries with the same key
    assert not retry.created
    drain(queue, service)
    assert store.get_order(retry.order.order_id).status is Status.RESERVED
    assert store.available("WIDGET") == 8


def test_reconciler_recovers_an_order_whose_client_never_retried(service, store, queue, clock):
    queue.fail_sends = 1
    with pytest.raises(EnqueueFailed):
        service.submit(KEY, request())
    clock.advance(301)
    assert service.reconcile_undispatched(older_than_s=300) == ["order-1"]
    drain(queue, service)
    assert store.get_order("order-1").status is Status.RESERVED


@pytest.mark.parametrize("bad", ["", "short", "has space here", "x" * 129])
def test_bad_idempotency_key_is_rejected(service, bad):
    with pytest.raises(ValidationError):
        service.submit(bad, request())
