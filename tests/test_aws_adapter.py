"""The DynamoDB adapter must give the same answers as the in-memory one.

Runs the key failure cases against moto (a local AWS fake). moto checks the
transaction and condition logic, not real AWS latency, limits or IAM.
"""

from __future__ import annotations

import itertools

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from orders.adapters.aws import DynamoStore, SqsQueue  # noqa: E402
from orders.domain import AuditEntry, Event, IdempotencyConflict, Status  # noqa: E402
from orders.ports import ReserveResult  # noqa: E402
from orders.service import FAULT_AFTER_COMMIT, EnqueueFailed, OrderService, WorkOutcome  # noqa: E402

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
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
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
    ids = (f"order-{n}" for n in itertools.count(1))
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
    audit = AuditEntry(order.order_id, "RESERVED", 0)
    assert svc.store.reserve(order.order_id, order.lines, 0, audit) is ReserveResult.INSUFFICIENT_STOCK
    assert svc.process({"type": "RESERVE", "order_id": order.order_id}) is WorkOutcome.REJECTED
    assert svc.store.available("WIDGET") == 10 and svc.store.available("GADGET") == 3


def test_cancel_reserved_releases_stock_and_late_reserve_is_ignored(svc):
    order = svc.submit("aws-key-00005", request()).order
    svc.process({"type": "RESERVE", "order_id": order.order_id})
    svc.apply_event(order.order_id, Event.CANCEL)
    assert svc.store.available("WIDGET") == 10
    assert svc.store.get_order(order.order_id).status is Status.CANCELLED
    assert svc.process({"type": "RESERVE", "order_id": order.order_id}) is WorkOutcome.IGNORED_INVALID


def test_reconciler_sees_only_unsent_orders_in_the_sparse_index(svc, aws, clock):
    store, queue = aws
    svc.submit("aws-key-00006", request())  # sent and marked
    queue.sqs.send_message = _boom  # outage
    with pytest.raises(EnqueueFailed):
        svc.submit("aws-key-00007", request())
    del queue.sqs.send_message  # back to the real client method
    clock.advance(301)

    assert store.list_undispatched(clock() - 300, 100) == ["order-2"]
    assert svc.reconcile_undispatched(300) == ["order-2"]
    assert "RECONCILE_REENQUEUE" in [e.action for e in store.audit_log("order-2")]
    assert store.list_undispatched(clock() - 300, 100) == []  # marker removed by mark_dispatched


def test_reconciler_ignores_an_order_already_moved_on(svc, aws, clock):
    """A status change removes the marker even if mark_dispatched never ran."""
    store, _ = aws
    order = svc.submit("aws-key-00008", request()).order
    # Put the marker back as if the mark after the send had failed.
    store.ddb.update_item(
        TableName="orders",
        Key={"pk": {"S": f"ORDER#{order.order_id}"}, "sk": {"S": "META"}},
        UpdateExpression="SET gsi1pk = :u, gsi1sk = :t",
        ExpressionAttributeValues={":u": {"S": "UNSENT"}, ":t": {"N": str(order.created_at)}},
    )
    assert not store.get_order(order.order_id).dispatched
    svc.process({"type": "RESERVE", "order_id": order.order_id})
    assert store.get_order(order.order_id).dispatched
    clock.advance(301)
    assert store.list_undispatched(clock() - 300, 100) == []


def test_reconciler_pages_beyond_one_query_page(svc, aws, clock):
    store, queue = aws
    queue.sqs.send_message = _boom
    for n in range(120):
        with pytest.raises(EnqueueFailed):
            svc.submit(f"aws-key-1{n:04d}", request())
    del queue.sqs.send_message
    clock.advance(301)
    assert len(store.list_undispatched(clock() - 300, 1000)) == 120


def test_reserve_writes_effect_and_audit_in_one_transaction(svc, aws):
    """Discriminating: the old code wrote the audit row with a separate put_item after the commit."""
    store, _ = aws
    order = svc.submit("aws-key-00009", request()).order
    puts, transacts = [], []
    real_put, real_tx = store.ddb.put_item, store.ddb.transact_write_items
    store.ddb.put_item = lambda **kw: puts.append(kw) or real_put(**kw)
    store.ddb.transact_write_items = lambda **kw: transacts.append(kw) or real_tx(**kw)

    assert svc.process({"type": "RESERVE", "order_id": order.order_id}) is WorkOutcome.RESERVED

    assert puts == []  # no audit write outside the transaction
    [tx] = transacts
    kinds = [next(iter(i)) for i in tx["TransactItems"]]
    assert kinds == ["Update", "Update", "Put"]  # status, 1 sku, audit
    assert [e.action for e in store.audit_log(order.order_id) if e.action == "RESERVED"] == ["RESERVED"]


def test_failed_reserve_writes_no_audit_row(svc, aws):
    store, _ = aws
    order = svc.submit("aws-key-00010", request(lines=[{"sku": "GADGET", "qty": 9}])).order
    svc.process({"type": "RESERVE", "order_id": order.order_id})
    assert "RESERVED" not in [e.action for e in store.audit_log(order.order_id)]


def test_same_key_two_customers_two_orders(svc):
    a = svc.submit("aws-shared-key", request(customer_id="A"))
    b = svc.submit("aws-shared-key", request(customer_id="B"))
    assert a.created and b.created and a.order.order_id != b.order.order_id


def _boom(**_kw):
    raise ConnectionError("simulated SQS outage")
