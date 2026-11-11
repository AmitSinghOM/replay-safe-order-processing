"""Lambda handler wiring: status codes and SQS partial batch responses."""

from __future__ import annotations

import json

from orders.handlers import api, worker


def _post(body, key="handler-key-01"):
    return {
        "resource": "/orders",
        "httpMethod": "POST",
        "headers": {"idempotency-key": key} if key else {},
        "body": json.dumps(body) if not isinstance(body, str) else body,
    }


BODY = {"customer_id": "c1", "lines": [{"sku": "WIDGET", "qty": 1}]}


def test_create_then_replay_status_codes(service):
    assert api.lambda_handler(_post(BODY), None, service)["statusCode"] == 201
    assert api.lambda_handler(_post(BODY), None, service)["statusCode"] == 200
    changed = {**BODY, "lines": [{"sku": "WIDGET", "qty": 2}]}
    assert api.lambda_handler(_post(changed), None, service)["statusCode"] == 409


def test_bad_input_is_400(service):
    assert api.lambda_handler(_post(BODY, key=None), None, service)["statusCode"] == 400
    assert api.lambda_handler(_post("{not json"), None, service)["statusCode"] == 400
    assert api.lambda_handler(_post({"customer_id": "c1", "lines": []}), None, service)["statusCode"] == 400


def test_enqueue_outage_is_503(service, queue):
    queue.fail_sends = 1
    assert api.lambda_handler(_post(BODY), None, service)["statusCode"] == 503


def test_get_missing_order_is_404(service):
    event = {"resource": "/orders/{order_id}", "httpMethod": "GET", "pathParameters": {"order_id": "nope"}}
    assert api.lambda_handler(event, None, service)["statusCode"] == 404


def test_worker_reports_only_failed_records(service):
    created = json.loads(api.lambda_handler(_post(BODY), None, service)["body"])
    event = {
        "Records": [
            {"messageId": "m1", "body": json.dumps({"type": "RESERVE", "order_id": created["order_id"]})},
            {"messageId": "m2", "body": json.dumps({"type": "RESERVE", "order_id": "missing"})},
            {"messageId": "m3", "body": "not json"},
        ]
    }
    assert worker.lambda_handler(event, None, service) == {
        "batchItemFailures": [{"itemIdentifier": "m2"}, {"itemIdentifier": "m3"}]
    }
