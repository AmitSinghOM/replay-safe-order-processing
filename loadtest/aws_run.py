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
from dataclasses import dataclass

import boto3

from orders.adapters.aws import DynamoStore, SqsQueue
from orders.domain import OrderRequest, Status
from orders.service import OrderService

from .curve import (
    Observed,
    RunResult,
    Scenario,
    count_statuses,
    evaluate,
    predict,
    sample_until_drained,
    write_results,
)

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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
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
    return ap.parse_args(argv)


@dataclass
class Target:
    """The deployed stack, as read from AWS (never from flags)."""

    session: boto3.Session
    lam: object
    sqs: object
    ddb: object
    cw: object
    api_url: str
    table: str
    queue_url: str
    dlq_url: str
    worker_fn: str
    esm_uuid: str
    delay_ms: int
    concurrency_cap: int

    @classmethod
    def connect(cls, stack: str) -> Target:
        session = boto3.Session()
        cfn, lam, sqs, ddb, cw = (
            session.client(n) for n in ("cloudformation", "lambda", "sqs", "dynamodb", "cloudwatch")
        )
        outputs, tags = stack_info(cfn, stack)
        if tags.get("purpose") != ALLOWED_PURPOSE:
            raise Refused(f"stack {stack} is not tagged purpose={ALLOWED_PURPOSE}; deploy with --tags purpose=loadtest")
        q_url, dlq_url, fn = outputs["QueueUrl"], outputs["DeadLetterQueueUrl"], outputs["WorkerFunctionName"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        delay_ms, cap, esm_uuid = worker_config(lam, fn, q_arn)
        if sum(queue_counts(sqs, q_url)) or sum(queue_counts(sqs, dlq_url)):
            raise Refused("queue or DLQ is not empty; drain or replay first")
        return cls(
            session=session,
            lam=lam,
            sqs=sqs,
            ddb=ddb,
            cw=cw,
            api_url=outputs["ApiUrl"],
            table=outputs["TableName"],
            queue_url=q_url,
            dlq_url=dlq_url,
            worker_fn=fn,
            esm_uuid=esm_uuid,
            delay_ms=delay_ms,
            concurrency_cap=cap,
        )

    def counts(self) -> tuple[int, int, int]:
        vis, nv = queue_counts(self.sqs, self.queue_url)
        return vis, nv, sum(queue_counts(self.sqs, self.dlq_url))

    def worker_enabled(self, enabled: bool) -> None:
        set_esm(self.lam, self.esm_uuid, enabled)


class Submitter:
    """Creates orders for one run, with a fresh SKU and a deterministic duplicate stream."""

    def __init__(self, t: Target, s: Scenario, via: str) -> None:
        self.t, self.s = t, s
        self.run_id = uuid.uuid4().hex[:8].upper()
        self.sku = f"LT-{self.run_id}"
        self.store, self.queue = DynamoStore(t.table, t.ddb), SqsQueue(t.queue_url, t.sqs)
        self.initial_stock = s.total_orders * s.qty_per_order * 2
        self.store.set_stock(self.sku, self.initial_stock)
        self.body = {"customer_id": f"lt-{self.run_id}", "lines": [{"sku": self.sku, "qty": s.qty_per_order}]}
        self.api = ApiClient(t.api_url, t.session) if via == "api" else None
        self.direct = OrderService(self.store, self.queue)
        self.dup_every = round(1 / s.duplicate_ratio) if s.duplicate_ratio else 0
        self.created: list[str] = []
        self._lock = threading.Lock()
        self.arrivals_done = threading.Event()

    def submit(self, n: int) -> None:
        key = f"lt-{self.run_id}-{n:08d}"
        if self.api:
            order_id = self.api.create(key, self.body)["order_id"]
        else:
            order_id = self.direct.submit(key, OrderRequest.parse(self.body)).order.order_id
        if self.dup_every and n % self.dup_every == 0:
            self.queue.send({"type": "RESERVE", "order_id": order_id})
        with self._lock:
            self.created.append(order_id)

    def preload(self) -> None:
        with cf.ThreadPoolExecutor(16) as pool:
            list(pool.map(self.submit, range(self.s.backlog)))

    def arrivals(self, t0: float) -> None:
        n = self.s.backlog
        while n < self.s.total_orders:
            due = self.s.backlog + int(self.s.arrival_rate * (time.monotonic() - t0))
            while n < min(due, self.s.total_orders):
                self.submit(n)
                n += 1
            time.sleep(0.05)
        self.arrivals_done.set()

    def observe(self, start: dt.datetime, end: dt.datetime) -> Observed:
        statuses = batch_statuses(self.t.ddb, self.t.table, self.created)
        pending, reserved, other = count_statuses(statuses, Status.PENDING, Status.RESERVED)
        return Observed(
            max_concurrency=lambda_max_concurrency(self.t.cw, self.t.worker_fn, start, end),
            orders_created=len(self.created),
            orders_pending=pending,
            orders_reserved=reserved,
            orders_other=other,
            stock_consumed=self.initial_stock - (self.store.available(self.sku) or 0),
            dlq_final=sum(queue_counts(self.t.sqs, self.t.dlq_url)),
        )


def _log(msg: str) -> None:
    print(msg, file=sys.stderr)


def run(t: Target, a: argparse.Namespace) -> tuple[RunResult, str]:
    """One measured run. Returns the result and a directory name for it."""
    s = Scenario(a.backlog, a.arrival_rate, a.arrival_seconds, t.concurrency_cap, t.delay_ms, a.duplicate_ratio)
    sub = Submitter(t, s, a.via)
    _log(f"run {sub.run_id}: delay={t.delay_ms}ms cap={t.concurrency_cap} via={a.via}; preloading {s.backlog} orders")
    start = dt.datetime.now(dt.UTC)
    t.worker_enabled(False)
    try:
        sub.preload()
        t.worker_enabled(True)
        t0 = time.monotonic()
        threading.Thread(target=sub.arrivals, args=(t0,), daemon=True).start()
        samples = sample_until_drained(
            t.counts,
            t0=t0,
            arrivals_done=sub.arrivals_done.is_set,
            sample_every=a.sample_every,
            max_seconds=a.max_seconds,
            on_sample=lambda x: _log(f"t={x.t:7.1f}s backlog={x.backlog} dlq={x.dlq}"),
        )
    finally:
        t.worker_enabled(True)  # never leave the worker paused

    _log(f"waiting {a.metrics_wait:.0f}s for CloudWatch")
    time.sleep(a.metrics_wait)
    end = dt.datetime.now(dt.UTC)
    obs = sub.observe(start, end)
    p = predict(s, a.overhead_ms)
    verdict, t_rec = evaluate(s, p, samples, obs, min_run_seconds=a.min_run_seconds, model_tolerance=None)
    env = {
        "stack": a.stack,
        "region": t.session.region_name,
        "run_id": sub.run_id,
        "started": start.isoformat(),
        "ended": end.isoformat(),
        "worker_usage": lambda_usage(t.cw, t.worker_fn, start, end),
        "client_python": platform.python_version(),
    }
    return RunResult(f"aws-{a.via}", s, p, samples, obs, verdict, t_rec, env), f"{start:%Y%m%dT%H%M%SZ}-{sub.run_id}"


def main() -> int:
    a = parse_args()
    result, run_name = run(Target.connect(a.stack), a)
    out = write_results(a.out / run_name, result)
    print("\n".join(result.summary_lines()), "->", out)
    return 0 if result.verdict.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
