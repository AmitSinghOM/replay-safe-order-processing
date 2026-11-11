"""Scheduled backstop: re-enqueue PENDING orders older than STALE_AFTER_S.

Covers the gap where the API committed an order but the SQS send failed and
the client never retried.
"""

from __future__ import annotations

import json
import logging
import os

from ..service import OrderService

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def lambda_handler(event: dict, context: object, service: OrderService | None = None) -> dict:
    if service is None:
        from ..adapters.aws import DynamoStore, SqsQueue

        service = OrderService(DynamoStore(os.environ["TABLE_NAME"]), SqsQueue(os.environ["QUEUE_URL"]))
    resent = service.reconcile_stale_pending(older_than_s=int(os.environ.get("STALE_AFTER_S", "300")))
    log.info(json.dumps({"reconciled": len(resent)}))
    return {"reconciled": resent}
