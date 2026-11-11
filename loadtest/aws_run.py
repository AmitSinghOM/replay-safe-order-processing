"""Backlog-recovery run against a DEPLOYED, NON-PRODUCTION stack.

    uv run --python 3.12 --extra aws python -m loadtest.aws_run \\
        --stack replay-safe-orders-lt --backlog 2000 --arrival-rate 2 --arrival-seconds 300 \\
        --duplicate-ratio 0.1 --out results/aws

What it does, in order:
 1. Refuses unless the stack is tagged purpose=loadtest (production safety).
 2. Reads the REAL config (worker delay, MaximumConcurrency) from AWS, not from flags.
 3. Refuses unless the queue and DLQ are empty (another run would skew results).
 4. Disables the worker's SQS event source, preloads the backlog, re-enables it at t0.
 5. Adds steady arrivals, samples queue depth every --sample-every seconds.
 6. Stops after 3 empty samples (or --max-seconds), waits for CloudWatch, then checks
    invariants: no PENDING order, stock consumed == reserved orders x qty, DLQ empty,
    Lambda ConcurrentExecutions max <= MaximumConcurrency.
 7. Writes samples.csv, summary.json, curve.svg.

The event source is ALWAYS re-enabled in a finally block.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import json
import pathlib
import platform
import sys
import threading
import time
import urllib.request
import uuid

import boto3

from orders.adapters.aws import DynamoStore, SqsQueue
from orders.domain import Status

from .curve import Observed, Sample, Scenario, evaluate, predict, write_results

ALLOWED_PURPOSE = "loadtest"


class Refused(SystemExit):
    def __init__(self, msg: str) -> None:
        super().__init__(f"refused: {msg}")


def stack_info(cfn, stack: str) -> tuple[dict[str, str], dict[str, str]]:
    st = cfn.describe_stacks(StackName=stack)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in st.get("Outputs", [])}
    tags = {t["Key"]: t["Value"] for t in st.get("Tags", [])}
    return outputs, tags


def worker_config(lam, function_name: str, queue_arn: str) -> tuple[int, int, str]:
    env = lam.get_function_configuration(FunctionName=function_name).get("Environment", {}).get("Variables", {})
    delay_ms = int(env.get("DEPENDENCY_DELAY_MS", "0"))
    mappings = lam.list_event_source_mappings(FunctionName=function_name, EventSourceArn=queue_arn)
    [esm] = mappings["EventSourceMappings"]
    cap = int(esm.get("ScalingConfig", {}).get("MaximumConcurrency", 1000))
    return delay_ms, cap, esm["UUID"]


def queue_counts(sqs, url: str) -> tuple[int, int]:
    a = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    )["Attributes"]
    return int(a["ApproximateNumberOfMessages"]), int(a["ApproximateNumberOfMessagesNotVisible"])


def set_esm(lam, esm_uuid: str, enabled: bool) -> None:
    lam.update_event_source_mapping(UUID=esm_uuid, Enabled=enabled)
    want = "Enabled" if enabled else "Disabled"
    for _ in range(60):
        if lam.get_event_source_mapping(UUID=esm_uuid)["State"] == want:
            return
        time.sleep(2)
    raise RuntimeError(f"event source mapping did not reach {want}")


class ApiClient:
    """SigV4-signed calls to the AWS_IAM-protected API."""

    def __init__(self, base_url: str, session: boto3.Session) -> None:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        self._sigv4, self._req = SigV4Auth, AWSRequest
        self.base, self.session, self.region = base_url.rstrip("/"), session, session.region_name

    def create(self, key: str, body: dict) -> dict:
        data = json.dumps(body)
        req = self._req(
            method="POST",
            url=f"{self.base}/orders",
            data=data,
            headers={"Content-Type": "application/json", "Idempotency-Key": key},
        )
        self._sigv4(self.session.get_credentials().get_frozen_credentials(), "execute-api", self.region).add_auth(req)
        http = urllib.request.Request(req.url, data=data.encode(), headers=dict(req.headers), method="POST")
        with urllib.request.urlopen(http, timeout=10) as resp:  # noqa: S310 - https URL from stack output
            return json.loads(resp.read())


def batch_statuses(ddb, table: str, order_ids: list[str]) -> list[Status | None]:
    found: dict[str, Status] = {}
    for i in range(0, len(order_ids), 100):
        keys = [{"pk": {"S": f"ORDER#{o}"}, "sk": {"S": "META"}} for o in order_ids[i : i + 100]]
        request = {
            table: {
                "Keys": keys,
                "ConsistentRead": True,
                "ProjectionExpression": "pk, #s",
                "ExpressionAttributeNames": {"#s": "status"},
            }
        }
        while request:
            resp = ddb.batch_get_item(RequestItems=request)
            for item in resp["Responses"].get(table, []):
                found[item["pk"]["S"].removeprefix("ORDER#")] = Status(item["status"]["S"])
            request = resp.get("UnprocessedKeys") or None
            if request:
                time.sleep(0.2)
    return [found.get(o) for o in order_ids]


def lambda_max_concurrency(cw, function_name: str, start: dt.datetime, end: dt.datetime) -> int:
    resp = cw.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="ConcurrentExecutions",
        Dimensions=[{"Name": "FunctionName", "Value": function_name}],
        StartTime=start,
        EndTime=end,
        Period=60,
        Statistics=["Maximum"],
    )
    # No datapoints -> -1 so the concurrency check fails instead of passing on missing data.
    return int(max((d["Maximum"] for d in resp["Datapoints"]), default=-1))


def lambda_usage(cw, function_name: str, start: dt.datetime, end: dt.datetime) -> dict:
    out = {}
    for metric, stat in [("Invocations", "Sum"), ("Errors", "Sum"), ("Throttles", "Sum"), ("Duration", "Average")]:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName=metric,
            Dimensions=[{"Name": "FunctionName", "Value": function_name}],
            StartTime=start,
            EndTime=end,
            Period=60,
            Statistics=[stat],
        )
        vals = [d[stat] for d in resp["Datapoints"]]
        out[f"{metric}_{stat}"] = sum(vals) if stat == "Sum" else (sum(vals) / len(vals) if vals else None)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stack", required=True)
    ap.add_argument("--backlog", type=int, default=2000)
    ap.add_argument("--arrival-rate", type=float, default=2.0)
    ap.add_argument("--arrival-seconds", type=float, default=300)
    ap.add_argument("--duplicate-ratio", type=float, default=0.1)
    ap.add_argument(
        "--via",
        choices=["direct", "api"],
        default="direct",
        help="direct: write orders + send messages with SDK (isolates the worker); api: signed POSTs",
    )
    ap.add_argument("--overhead-ms", type=float, default=40.0, help="per-message cost beyond the delay, for the model")
    ap.add_argument("--sample-every", type=float, default=10.0)
    ap.add_argument("--max-seconds", type=float, default=3600)
    ap.add_argument("--min-run-seconds", type=float, default=120)
    ap.add_argument("--metrics-wait", type=float, default=180, help="CloudWatch publishing lag")
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("results/aws"))
    a = ap.parse_args()

    session = boto3.Session()
    cfn, lam, sqs, ddb, cw = (session.client(n) for n in ("cloudformation", "lambda", "sqs", "dynamodb", "cloudwatch"))
    outputs, tags = stack_info(cfn, a.stack)
    if tags.get("purpose") != ALLOWED_PURPOSE:
        raise Refused(f"stack {a.stack} is not tagged purpose={ALLOWED_PURPOSE}; deploy with --tags purpose=loadtest")

    table, q_url, dlq_url, fn = (
        outputs[k] for k in ("TableName", "QueueUrl", "DeadLetterQueueUrl", "WorkerFunctionName")
    )
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    delay_ms, cap, esm_uuid = worker_config(lam, fn, q_arn)
    if sum(queue_counts(sqs, q_url)) or sum(queue_counts(sqs, dlq_url)):
        raise Refused("queue or DLQ is not empty; drain or replay first")

    s = Scenario(a.backlog, a.arrival_rate, a.arrival_seconds, cap, delay_ms, a.duplicate_ratio)
    run_id = uuid.uuid4().hex[:8].upper()
    sku = f"LT-{run_id}"
    store, queue = DynamoStore(table, ddb), SqsQueue(q_url, sqs)
    initial_stock = s.total_orders * s.qty_per_order * 2
    store.set_stock(sku, initial_stock)
    body = {"customer_id": f"lt-{run_id}", "lines": [{"sku": sku, "qty": s.qty_per_order}]}
    api = ApiClient(outputs["ApiUrl"], session) if a.via == "api" else None

    from orders.service import OrderService

    direct = OrderService(store, queue)
    created: list[str] = []
    created_lock = threading.Lock()
    dup_every = round(1 / s.duplicate_ratio) if s.duplicate_ratio else 0

    def submit(n: int) -> None:
        key = f"lt-{run_id}-{n:08d}"
        if api:
            order_id = api.create(key, body)["order_id"]
        else:
            from orders.domain import OrderRequest

            order_id = direct.submit(key, OrderRequest.parse(body)).order.order_id
        if dup_every and n % dup_every == 0:
            queue.send({"type": "RESERVE", "order_id": order_id})
        with created_lock:
            created.append(order_id)

    print(f"run {run_id}: delay={delay_ms}ms cap={cap} via={a.via}; preloading {s.backlog} orders", file=sys.stderr)
    samples: list[Sample] = []
    start_wall = dt.datetime.now(dt.UTC)
    set_esm(lam, esm_uuid, False)
    try:
        with cf.ThreadPoolExecutor(16) as pool:
            list(pool.map(submit, range(s.backlog)))
        set_esm(lam, esm_uuid, True)
        t0 = time.monotonic()
        arrivals_done = threading.Event()

        def arrivals() -> None:
            n = s.backlog
            while n < s.total_orders:
                due = s.backlog + int(s.arrival_rate * (time.monotonic() - t0))
                while n < min(due, s.total_orders):
                    submit(n)
                    n += 1
                time.sleep(0.05)
            arrivals_done.set()

        threading.Thread(target=arrivals, daemon=True).start()
        zero_streak = 0
        while True:
            t = time.monotonic() - t0
            try:
                vis, nv = queue_counts(sqs, q_url)
                dlq_vis, dlq_nv = queue_counts(sqs, dlq_url)
                samples.append(Sample(t, vis, nv, dlq_vis + dlq_nv))
                zero_streak = zero_streak + 1 if (vis + nv == 0 and arrivals_done.is_set()) else 0
            except Exception as exc:  # noqa: BLE001 - recorded and counted as a failing sample
                samples.append(Sample(t, 0, 0, 0, error=f"{type(exc).__name__}: {exc}"))
                zero_streak = 0
            print(f"t={t:7.1f}s backlog={samples[-1].backlog} dlq={samples[-1].dlq}", file=sys.stderr)
            if zero_streak >= 3 or t > a.max_seconds:
                break
            time.sleep(a.sample_every)
    finally:
        set_esm(lam, esm_uuid, True)

    print(f"waiting {a.metrics_wait:.0f}s for CloudWatch", file=sys.stderr)
    time.sleep(a.metrics_wait)
    end_wall = dt.datetime.now(dt.UTC)
    statuses = batch_statuses(ddb, table, created)
    obs = Observed(
        max_concurrency=lambda_max_concurrency(cw, fn, start_wall, end_wall),
        orders_created=len(created),
        orders_pending=sum(st is Status.PENDING for st in statuses),
        orders_reserved=sum(st is Status.RESERVED for st in statuses),
        orders_other=sum(st not in (Status.PENDING, Status.RESERVED) for st in statuses),
        stock_consumed=initial_stock - (store.available(sku) or 0),
        dlq_final=sum(queue_counts(sqs, dlq_url)),
    )
    p = predict(s, a.overhead_ms)
    verdict, t_rec = evaluate(s, p, samples, obs, min_run_seconds=a.min_run_seconds, model_tolerance=None)
    out = write_results(
        a.out / f"{start_wall:%Y%m%dT%H%M%SZ}-{run_id}",
        mode=f"aws-{a.via}",
        s=s,
        p=p,
        samples=samples,
        obs=obs,
        verdict=verdict,
        t_rec=t_rec,
        environment={
            "stack": a.stack,
            "region": session.region_name,
            "run_id": run_id,
            "started": start_wall.isoformat(),
            "ended": end_wall.isoformat(),
            "worker_usage": lambda_usage(cw, fn, start_wall, end_wall),
            "client_python": platform.python_version(),
        },
    )
    for c in verdict.checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name:22} {c.detail}")
    measured = f"{t_rec:.0f}s" if t_rec is not None else "never"
    print(f"model lower bound {p.recovery_seconds:.0f}s, measured {measured} -> {out}")
    return 0 if verdict.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
