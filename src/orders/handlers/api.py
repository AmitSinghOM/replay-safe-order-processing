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
from collections.abc import Callable

from ..domain import (
    DomainError,
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


def _create(svc: OrderService, event: dict, _order_id: str | None) -> dict:
    try:
        body = json.loads(event.get("body") or "null")
    except json.JSONDecodeError:
        raise ValidationError("body is not valid JSON") from None
    result = svc.submit(_header(event, "Idempotency-Key") or "", OrderRequest.parse(body))
    return _resp(201 if result.created else 200, result.order.to_dict())


def _get(svc: OrderService, _event: dict, order_id: str | None) -> dict:
    return _resp(200, svc.get(order_id).to_dict())


def _cancel(svc: OrderService, _event: dict, order_id: str | None) -> dict:
    return _resp(200, svc.apply_event(order_id, Event.CANCEL).to_dict())


def _confirm(svc: OrderService, _event: dict, order_id: str | None) -> dict:
    return _resp(200, svc.apply_event(order_id, Event.CONFIRM).to_dict())


ROUTES: dict[tuple[str, str], Callable[[OrderService, dict, str | None], dict]] = {
    ("/orders", "POST"): _create,
    ("/orders/{order_id}", "GET"): _get,
    ("/orders/{order_id}/cancel", "POST"): _cancel,
    ("/orders/{order_id}/confirm", "POST"): _confirm,
}

# Domain error -> HTTP status. 503 means "the order may be committed; retry with the same key".
_STATUS_BY_ERROR: tuple[tuple[type[Exception], int, str | None], ...] = (
    (ValidationError, 400, None),
    (OrderNotFound, 404, "order not found"),
    (IdempotencyConflict, 409, None),
    (InvalidTransition, 409, None),
    (EnqueueFailed, 503, "temporarily unavailable; retry with the same Idempotency-Key"),
    (TransientStoreError, 503, "temporarily unavailable; retry with the same Idempotency-Key"),
)


def lambda_handler(event: dict, context: object, service: OrderService | None = None) -> dict:
    svc = service or _default_service()
    route = ROUTES.get((event.get("resource", ""), event.get("httpMethod", "")))
    if route is None:
        return _resp(404, {"error": "route not found"})
    order_id = (event.get("pathParameters") or {}).get("order_id")
    try:
        return route(svc, event, order_id)
    except (DomainError, EnqueueFailed, TransientStoreError) as exc:
        return _error_response(exc)


def _error_response(exc: Exception) -> dict:
    for kind, status, message in _STATUS_BY_ERROR:
        if isinstance(exc, kind):
            return _resp(status, {"error": message or str(exc)})
    raise exc  # not a mapped error: let Lambda report it
