"""API Gateway (REST, AWS_IAM auth) -> OrderService.

Routes:
    POST /orders                     Idempotency-Key header required
    GET  /orders/{order_id}
    POST /orders/{order_id}/cancel
    POST /orders/{order_id}/confirm
"""

from __future__ import annotations

import json
import logging
import os

from ..domain import (
    Event,
    IdempotencyConflict,
    InvalidTransition,
    OrderNotFound,
    OrderRequest,
    ValidationError,
)
from ..ports import TransientStoreError
from ..service import EnqueueFailed, OrderService

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
            idempotency_window_s=int(os.environ.get("IDEMPOTENCY_WINDOW_S", "86400")),
        )
    return _service


def _resp(status: int, body: dict) -> dict:
    return {"statusCode": status, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}


def _header(event: dict, name: str) -> str | None:
    for k, v in (event.get("headers") or {}).items():
        if k.lower() == name.lower():
            return v
    return None


def lambda_handler(event: dict, context: object, service: OrderService | None = None) -> dict:
    svc = service or _default_service()
    resource = event.get("resource", "")
    method = event.get("httpMethod", "")
    order_id = (event.get("pathParameters") or {}).get("order_id")
    try:
        if resource == "/orders" and method == "POST":
            try:
                body = json.loads(event.get("body") or "null")
            except json.JSONDecodeError:
                raise ValidationError("body is not valid JSON") from None
            result = svc.submit(_header(event, "Idempotency-Key") or "", OrderRequest.parse(body))
            return _resp(201 if result.created else 200, result.order.to_dict())
        if resource == "/orders/{order_id}" and method == "GET":
            return _resp(200, svc.get(order_id).to_dict())
        if resource == "/orders/{order_id}/cancel" and method == "POST":
            return _resp(200, svc.apply_event(order_id, Event.CANCEL).to_dict())
        if resource == "/orders/{order_id}/confirm" and method == "POST":
            return _resp(200, svc.apply_event(order_id, Event.CONFIRM).to_dict())
        return _resp(404, {"error": "route not found"})
    except ValidationError as exc:
        return _resp(400, {"error": str(exc)})
    except OrderNotFound:
        return _resp(404, {"error": "order not found"})
    except (IdempotencyConflict, InvalidTransition) as exc:
        return _resp(409, {"error": str(exc)})
    except (EnqueueFailed, TransientStoreError):
        # The order may be committed. Retrying with the same Idempotency-Key is safe.
        return _resp(503, {"error": "temporarily unavailable; retry with the same Idempotency-Key"})
