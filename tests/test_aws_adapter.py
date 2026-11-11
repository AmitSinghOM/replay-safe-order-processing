"""The DynamoDB adapter must give the same answers as the in-memory one.

Runs the key failure cases against moto (a local AWS fake). moto checks the
transaction and condition logic, not real AWS latency, limits or IAM.
"""

from __future__ import annotations

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from orders.adapters.aws import DynamoStore, SqsQueue  # noqa: E402
from orders.domain import Event, IdempotencyConflict, Status  # noqa: E402
from orders.ports import ReserveResult  # noqa: E402
from orders.service import FAULT_AFTER_COMMIT, OrderService, WorkOutcome  # noqa: E402

from .conftest import request  # noqa: E402

REGION = "us-east-1"


@pytest.fixture
def aws(monkeypatch):
    for k, v in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": REGION,
    }.items():
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        ddb = boto3.client("dynamodb", region_name=REGION)
        ddb.create_table(
            TableName="orders",
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": n, "AttributeType": t}
                for n, t in [("pk", "S"), ("sk", "S"), ("gsi1pk", "S"), ("gsi1sk", "N")]
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [
                        {"AttributeName": "gsi1pk", "KeyType": "HASH"},
                        {"AttributeName": "gsi1sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        sqs = boto3.client("sqs", region_name=REGION)
        url = sqs.create_queue(QueueName="orders")["QueueUrl"]
        store = DynamoStore("orders", ddb)
        store.set_stock("WIDGET", 10)
        store.set_stock("GADGET", 3)
        yield store, SqsQueue(url, sqs)


@pytest.fixture
def svc(aws, clock, faults):
    store, queue = aws
    ids = iter(f"order-{n}" for n in range(1, 100))
    return OrderService(store, queue, clock=clock, id_factory=lambda: next(ids), faults=faults)


def test_idempotent_create(svc):
    a = svc.submit("aws-key-00001", request())
    b = svc.submit("aws-key-00001", request())
    assert a.created and not b.created and a.order.order_id == b.order.order_id
    with pytest.raises(IdempotencyConflict):
        svc.submit("aws-key-00001", request(lines=[{"sku": "WIDGET", "qty": 5}]))


def test_expired_key_can_be_reused(svc, clock):
    first = svc.submit("aws-key-00002", request())
    clock.advance(svc.window + 1)
    second = svc.submit("aws-key-00002", request())
    assert second.created and second.order.order_id != first.order.order_id


def test_crash_after_commit_reserves_once(svc, faults):
    order = svc.submit("aws-key-00003", request()).order
    faults.arm(FAULT_AFTER_COMMIT, RuntimeError)
    msg = {"type": "RESERVE", "order_id": order.order_id}
    with pytest.raises(RuntimeError):
        svc.process(msg)
    assert svc.process(msg) is WorkOutcome.ALREADY_APPLIED
    assert svc.store.available("WIDGET") == 8


def test_insufficient_stock_is_all_or_nothing(svc):
    order = svc.submit("aws-key-00004", request(lines=[{"sku": "WIDGET", "qty": 1}, {"sku": "GADGET", "qty": 4}])).order
    assert svc.store.reserve(order.order_id, order.lines, 0) is ReserveResult.INSUFFICIENT_STOCK
    assert svc.process({"type": "RESERVE", "order_id": order.order_id}) is WorkOutcome.REJECTED
    assert svc.store.available("WIDGET") == 10 and svc.store.available("GADGET") == 3


def test_cancel_reserved_releases_stock_and_late_reserve_is_ignored(svc):
    order = svc.submit("aws-key-00005", request()).order
    svc.process({"type": "RESERVE", "order_id": order.order_id})
    svc.apply_event(order.order_id, Event.CANCEL)
    assert svc.store.available("WIDGET") == 10
    assert svc.store.get_order(order.order_id).status is Status.CANCELLED
    assert svc.process({"type": "RESERVE", "order_id": order.order_id}) is WorkOutcome.IGNORED_INVALID


def test_reconciler_query_and_audit(svc, clock):
    svc.submit("aws-key-00006", request())
    clock.advance(301)
    assert svc.reconcile_stale_pending(300) == ["order-1"]
    assert "RECONCILE_REENQUEUE" in [e.action for e in svc.store.audit_log("order-1")]
