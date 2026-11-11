"""DynamoDB + SQS adapters. Single table, keys ``pk``/``sk``.

Item layout (sk is always ``META`` except audit rows):

    ORDER#<id>   status, customer_id, lines, version, created_at, updated_at, reason,
                 gsi1pk=STATUS#<status>, gsi1sk=<created_at>
    IDEMP#<key>  request_fingerprint, order_id, expires_at   (TTL attribute: expires_at)
    SKU#<sku>    available
    AUDIT#<subj> sk=<at>#<uuid>, action, detail (JSON)

boto3 is imported lazily so unit tests run without it. The Lambda runtime
already ships boto3.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from ..domain import AuditEntry, IdempotencyRecord, Line, Order, Status
from ..ports import CancelResult, DlqMessage, ReserveResult, TransientStoreError

_META = "META"
_RETRYABLE_REASONS = {"TransactionConflict", "ThrottlingError", "ProvisionedThroughputExceeded"}


def _order_key(order_id: str) -> dict:
    return {"pk": {"S": f"ORDER#{order_id}"}, "sk": {"S": _META}}


def _sku_key(sku: str) -> dict:
    return {"pk": {"S": f"SKU#{sku}"}, "sk": {"S": _META}}


def _idem_key(key: str) -> dict:
    return {"pk": {"S": f"IDEMP#{key}"}, "sk": {"S": _META}}


def _n(value: int) -> dict:
    return {"N": str(value)}


def _cancellation_codes(exc: Exception) -> list[str]:
    reasons = getattr(exc, "response", {}).get("CancellationReasons", [])
    return [r.get("Code", "None") for r in reasons]


class DynamoStore:
    def __init__(self, table_name: str, client: Any | None = None) -> None:
        if client is None:
            import boto3

            client = boto3.client("dynamodb")
        self.table = table_name
        self.ddb = client

    # -- helpers -------------------------------------------------------------

    def _transact(self, items: list[dict]) -> list[str] | None:
        """Run a transaction. Returns None on success, or per-item cancellation codes."""
        try:
            self.ddb.transact_write_items(TransactItems=items)
            return None
        except self.ddb.exceptions.TransactionCanceledException as exc:
            codes = _cancellation_codes(exc)
            if any(c in _RETRYABLE_REASONS for c in codes):
                raise TransientStoreError(",".join(codes)) from exc
            return codes

    @staticmethod
    def _to_order(item: dict) -> Order:
        lines = tuple(Line(l["sku"], int(l["qty"])) for l in json.loads(item["lines"]["S"]))
        return Order(
            order_id=item["pk"]["S"].removeprefix("ORDER#"),
            customer_id=item["customer_id"]["S"],
            lines=lines,
            status=Status(item["status"]["S"]),
            version=int(item["version"]["N"]),
            created_at=int(item["created_at"]["N"]),
            updated_at=int(item["updated_at"]["N"]),
            reason=item.get("reason", {}).get("S"),
        )

    # -- OrderStore ------------------------------------------------------------

    def create_order(self, order: Order, record: IdempotencyRecord, now: int) -> IdempotencyRecord | None:
        lines_json = json.dumps([{"sku": l.sku, "qty": l.qty} for l in order.lines])
        codes = self._transact(
            [
                {
                    "Put": {
                        "TableName": self.table,
                        "Item": {
                            **_idem_key(record.key),
                            "request_fingerprint": {"S": record.request_fingerprint},
                            "order_id": {"S": record.order_id},
                            "expires_at": _n(record.expires_at),
                        },
                        # TTL deletion is lazy, so an expired-but-present record may be replaced.
                        "ConditionExpression": "attribute_not_exists(pk) OR expires_at <= :now",
                        "ExpressionAttributeValues": {":now": _n(now)},
                    }
                },
                {
                    "Put": {
                        "TableName": self.table,
                        "Item": {
                            **_order_key(order.order_id),
                            "customer_id": {"S": order.customer_id},
                            "lines": {"S": lines_json},
                            "status": {"S": order.status.value},
                            "version": _n(order.version),
                            "created_at": _n(order.created_at),
                            "updated_at": _n(order.updated_at),
                            "gsi1pk": {"S": f"STATUS#{order.status.value}"},
                            "gsi1sk": _n(order.created_at),
                        },
                        "ConditionExpression": "attribute_not_exists(pk)",
                    }
                },
            ]
        )
        if codes is None:
            return None
        if codes and codes[0] == "ConditionalCheckFailed":
            held = self.get_idempotency(record.key)
            if held is not None and held.is_live(now):
                return held
            raise TransientStoreError("idempotency record changed during write; retry")
        raise RuntimeError(f"create_order cancelled: {codes}")

    def get_idempotency(self, key: str) -> IdempotencyRecord | None:
        item = self.ddb.get_item(TableName=self.table, Key=_idem_key(key), ConsistentRead=True).get("Item")
        if not item:
            return None
        return IdempotencyRecord(
            key=key,
            request_fingerprint=item["request_fingerprint"]["S"],
            order_id=item["order_id"]["S"],
            expires_at=int(item["expires_at"]["N"]),
        )

    def get_order(self, order_id: str) -> Order | None:
        item = self.ddb.get_item(TableName=self.table, Key=_order_key(order_id), ConsistentRead=True).get("Item")
        return self._to_order(item) if item else None

    def _status_update(self, order_id: str, expected: Status, to: Status, now: int, reason: str | None) -> dict:
        names = {"#s": "status"}
        values = {
            ":expected": {"S": expected.value},
            ":to": {"S": to.value},
            ":gsi": {"S": f"STATUS#{to.value}"},
            ":now": _n(now),
            ":one": _n(1),
        }
        update = "SET #s = :to, gsi1pk = :gsi, updated_at = :now, version = version + :one"
        if reason:
            update += ", reason = :reason"
            values[":reason"] = {"S": reason}
        return {
            "Update": {
                "TableName": self.table,
                "Key": _order_key(order_id),
                "UpdateExpression": update,
                "ConditionExpression": "#s = :expected",
                "ExpressionAttributeNames": names,
                "ExpressionAttributeValues": values,
            }
        }

    def _stock_update(self, line: Line, sign: int) -> dict:
        if sign < 0:
            return {
                "Update": {
                    "TableName": self.table,
                    "Key": _sku_key(line.sku),
                    "UpdateExpression": "SET available = available - :q",
                    "ConditionExpression": "available >= :q",  # also fails if the sku is unknown
                    "ExpressionAttributeValues": {":q": _n(line.qty)},
                }
            }
        return {
            "Update": {
                "TableName": self.table,
                "Key": _sku_key(line.sku),
                "UpdateExpression": "SET available = available + :q",
                "ConditionExpression": "attribute_exists(available)",
                "ExpressionAttributeValues": {":q": _n(line.qty)},
            }
        }

    def reserve(self, order_id: str, lines: tuple[Line, ...], now: int) -> ReserveResult:
        items = [self._status_update(order_id, Status.PENDING, Status.RESERVED, now, None)]
        items += [self._stock_update(l, -1) for l in lines]
        codes = self._transact(items)
        if codes is None:
            return ReserveResult.RESERVED
        if codes[0] == "ConditionalCheckFailed":
            return ReserveResult.ORDER_NOT_PENDING
        if any(c == "ConditionalCheckFailed" for c in codes[1:]):
            return ReserveResult.INSUFFICIENT_STOCK
        raise RuntimeError(f"reserve cancelled: {codes}")

    def transition(self, order_id: str, expected: Status, to: Status, now: int, reason: str | None = None) -> bool:
        update = self._status_update(order_id, expected, to, now, reason)["Update"]
        try:
            self.ddb.update_item(**update)
            return True
        except self.ddb.exceptions.ConditionalCheckFailedException:
            return False

    def cancel(self, order: Order, now: int) -> CancelResult:
        if order.status not in (Status.PENDING, Status.RESERVED):
            return CancelResult.STATE_CHANGED
        items = [self._status_update(order.order_id, order.status, Status.CANCELLED, now, None)]
        if order.status is Status.RESERVED:
            items += [self._stock_update(l, +1) for l in order.lines]
        codes = self._transact(items)
        if codes is None:
            return CancelResult.CANCELLED
        if codes[0] == "ConditionalCheckFailed":
            return CancelResult.STATE_CHANGED
        raise RuntimeError(f"cancel cancelled: {codes}")

    def list_by_status(self, status: Status, created_before: int, limit: int) -> list[Order]:
        resp = self.ddb.query(
            TableName=self.table,
            IndexName="gsi1",
            KeyConditionExpression="gsi1pk = :p AND gsi1sk < :t",
            ExpressionAttributeValues={":p": {"S": f"STATUS#{status.value}"}, ":t": _n(created_before)},
            Limit=limit,
        )
        return [self._to_order(i) for i in resp.get("Items", [])]

    def append_audit(self, entry: AuditEntry) -> None:
        self.ddb.put_item(
            TableName=self.table,
            Item={
                "pk": {"S": f"AUDIT#{entry.subject}"},
                "sk": {"S": f"{entry.at:012d}#{uuid.uuid4()}"},
                "action": {"S": entry.action},
                "at": _n(entry.at),
                "detail": {"S": json.dumps(entry.detail, sort_keys=True, default=str)},
            },
        )

    def audit_log(self, subject: str) -> list[AuditEntry]:
        resp = self.ddb.query(
            TableName=self.table,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": {"S": f"AUDIT#{subject}"}},
        )
        return [
            AuditEntry(subject, i["action"]["S"], int(i["at"]["N"]), json.loads(i["detail"]["S"]))
            for i in resp.get("Items", [])
        ]

    def available(self, sku: str) -> int | None:
        item = self.ddb.get_item(TableName=self.table, Key=_sku_key(sku), ConsistentRead=True).get("Item")
        return int(item["available"]["N"]) if item else None

    def set_stock(self, sku: str, available: int) -> None:
        self.ddb.put_item(TableName=self.table, Item={**_sku_key(sku), "available": _n(available)})


class SqsQueue:
    def __init__(self, queue_url: str, client: Any | None = None) -> None:
        if client is None:
            import boto3

            client = boto3.client("sqs")
        self.url = queue_url
        self.sqs = client

    def send(self, body: dict) -> str:
        return self.sqs.send_message(QueueUrl=self.url, MessageBody=json.dumps(body))["MessageId"]


class SqsDlq:
    def __init__(self, queue_url: str, client: Any | None = None, visibility_timeout_s: int = 120) -> None:
        if client is None:
            import boto3

            client = boto3.client("sqs")
        self.url = queue_url
        self.sqs = client
        self.visibility = visibility_timeout_s

    def receive(self, max_messages: int) -> list[DlqMessage]:
        resp = self.sqs.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=min(max_messages, 10),
            VisibilityTimeout=self.visibility,
            WaitTimeSeconds=1,
            MessageSystemAttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
        )
        out = []
        for m in resp.get("Messages", []):
            attrs = m.get("Attributes", {})
            out.append(
                DlqMessage(
                    message_id=m["MessageId"],
                    receipt=m["ReceiptHandle"],
                    body=json.loads(m["Body"]),
                    receive_count=int(attrs.get("ApproximateReceiveCount", 0)),
                    attributes=attrs,
                )
            )
        return out

    def delete(self, receipt: str) -> None:
        self.sqs.delete_message(QueueUrl=self.url, ReceiptHandle=receipt)
