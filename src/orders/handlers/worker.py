"""SQS -> OrderService.process, with partial batch responses.

A record that raises is reported in ``batchItemFailures`` so SQS redelivers
only that record. After ``maxReceiveCount`` receives SQS moves it to the DLQ.
"""

from __future__ import annotations

import json
import logging
import os

from ..service import OrderService, dependency_delay

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_service: OrderService | None = None


def _default_service() -> OrderService:
    global _service
    if _service is None:
        from ..adapters.aws import DynamoStore, SqsQueue

        _service = OrderService(
            DynamoStore(os.environ["TABLE_NAME"]),
            SqsQueue(os.environ["QUEUE_URL"]),
            # Load-test knob for failure 4. Default 0 = no delay.
            faults=dependency_delay(int(os.environ.get("DEPENDENCY_DELAY_MS", "0"))),
        )
    return _service


def lambda_handler(event: dict, context: object, service: OrderService | None = None) -> dict:
    svc = service or _default_service()
    failures = []
    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            outcome = svc.process({**body, "_message_id": message_id})
            log.info(json.dumps({"message_id": message_id, "order_id": body.get("order_id"), "outcome": outcome.value}))
        except Exception as exc:  # cqa: ignore=PY-COR-002 reason="any failure means redeliver"
            log.warning(json.dumps({"message_id": message_id, "error": type(exc).__name__, "detail": str(exc)}))
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
