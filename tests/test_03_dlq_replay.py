"""Failure 3: a message keeps failing -> bounded retries, DLQ, auditable replay."""

from __future__ import annotations

import pytest

from orders.adapters.memory import drain
from orders.domain import Event, Status
from orders.ports import TransientStoreError
from orders.service import FAULT_BEFORE_COMMIT, WorkOutcome

from .conftest import request


def _dead_letter_one(service, queue, faults, key="dlq-key-0001"):
    order = service.submit(key, request()).order
    faults.arm(FAULT_BEFORE_COMMIT, TransientStoreError, times=99)  # dependency is down
    outcomes = drain(queue, service)
    faults.arm(FAULT_BEFORE_COMMIT, TransientStoreError, times=0)  # dependency recovers
    return order, outcomes


def test_retries_are_bounded_then_message_is_dead_lettered(service, store, queue, faults):
    order, outcomes = _dead_letter_one(service, queue, faults)
    assert len(outcomes) == queue.max_receive_count
    assert all(isinstance(o, TransientStoreError) for o in outcomes)
    assert queue.depth() == 0 and queue.dlq.depth() == 1
    assert store.get_order(order.order_id).status is Status.PENDING
    assert store.available("WIDGET") == 10


def test_dry_run_audits_but_changes_nothing(service, store, queue, faults):
    _dead_letter_one(service, queue, faults)
    report = service.replay_dlq(queue.dlq, operator="oncall", reason="INC-1 dependency recovered")
    assert report.dry_run and len(report.inspected) == 1
    assert queue.dlq.depth() == 1 and queue.depth() == 0
    [entry] = store.audit_log("replay")
    assert entry.action == "INSPECT"
    assert entry.detail["operator"] == "oncall"
    assert entry.detail["receive_count"] == queue.max_receive_count
    assert len(entry.detail["body_sha256"]) == 64


def test_execute_replays_and_order_is_reserved_once(service, store, queue, faults):
    order, _ = _dead_letter_one(service, queue, faults)
    report = service.replay_dlq(queue.dlq, operator="oncall", reason="INC-1", dry_run=False)
    assert len(report.replayed) == 1 and queue.dlq.depth() == 0

    outcomes = drain(queue, service)
    assert outcomes == [WorkOutcome.RESERVED]
    assert store.get_order(order.order_id).status is Status.RESERVED
    assert store.available("WIDGET") == 8
    assert [e.action for e in store.audit_log("replay")] == ["REPLAY"]


def test_replay_skips_messages_whose_order_already_settled(service, store, queue, faults):
    order, _ = _dead_letter_one(service, queue, faults)
    service.apply_event(order.order_id, Event.CANCEL)

    report = service.replay_dlq(queue.dlq, operator="oncall", reason="INC-1", dry_run=False)

    assert len(report.skipped) == 1 and report.replayed == []
    assert queue.dlq.depth() == 0 and queue.depth() == 0
    assert store.audit_log("replay")[0].action == "SKIP_SETTLED"


def test_replaying_twice_is_harmless(service, store, queue, faults):
    order, _ = _dead_letter_one(service, queue, faults)
    [msg] = queue.dlq.receive(1)
    queue.send(msg.body)  # operator replays by hand AND with the tool
    service.replay_dlq(queue.dlq, operator="oncall", reason="INC-1", dry_run=False)
    drain(queue, service)
    assert store.available("WIDGET") == 8


def test_replay_requires_operator_and_reason(service, queue):
    with pytest.raises(ValueError):
        service.replay_dlq(queue.dlq, operator="", reason="x")
