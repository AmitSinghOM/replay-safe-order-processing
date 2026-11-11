"""Local backlog-recovery run: the real OrderService on in-memory adapters.

C worker threads stand in for C Lambda instances. A sleep before the commit
stands in for the slow dependency. This proves the harness and the
invariants. It is NOT AWS evidence: no SQS polling, no cold starts, no
DynamoDB latency.

    uv run --python 3.12 python -m loadtest.local_sim --out results/local
"""

from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import platform
import threading
import time

from orders.adapters.memory import InMemorySqs, InMemoryStore
from orders.domain import OrderRequest, Status
from orders.service import OrderService, dependency_delay

from .curve import Observed, Sample, Scenario, evaluate, predict, write_results

SKU = "LOCAL-LT"


def run_local(
    s: Scenario,
    *,
    sample_every: float = 0.05,
    max_seconds: float = 60.0,
    overhead_ms: float = 0.2,
    model_tolerance: float | None = 0.5,
    min_run_seconds: float = 1.0,
):
    store, queue = InMemoryStore(), InMemorySqs(max_receive_count=5)
    initial_stock = s.total_orders * s.qty_per_order * 2
    store.set_stock(SKU, initial_stock)
    svc = OrderService(store, queue, faults=dependency_delay(s.dependency_delay_ms))
    req = OrderRequest.parse({"customer_id": "lt-local", "lines": [{"sku": SKU, "qty": s.qty_per_order}]})

    created: list[str] = []
    dup_debt = 0.0

    def submit(n: int) -> None:
        nonlocal dup_debt
        order = svc.submit(f"lt-local-{n:08d}", req).order
        created.append(order.order_id)
        dup_debt += s.duplicate_ratio
        while dup_debt >= 1:  # deterministic duplicate stream
            queue.send({"type": "RESERVE", "order_id": order.order_id})
            dup_debt -= 1

    for n in range(s.backlog):
        submit(n)

    stop = threading.Event()
    arrivals_done = threading.Event()
    inflight_lock = threading.Lock()
    inflight = {"now": 0, "max": 0}

    def worker() -> None:
        while not stop.is_set():
            got = queue.receive()
            if got is None:
                time.sleep(0.001)
                continue
            receipt, body, _ = got
            with inflight_lock:
                inflight["now"] += 1
                inflight["max"] = max(inflight["max"], inflight["now"])
            try:
                svc.process(body)
            except Exception:  # noqa: BLE001 - same as the Lambda worker: redeliver
                queue.nack(receipt)
            else:
                queue.ack(receipt)
            finally:
                with inflight_lock:
                    inflight["now"] -= 1

    t0 = time.monotonic()

    def arrivals() -> None:
        n = s.backlog
        total = s.total_orders
        while n < total and not stop.is_set():
            due = s.backlog + int(s.arrival_rate * (time.monotonic() - t0))
            while n < min(due, total):
                submit(n)
                n += 1
            time.sleep(0.005)
        arrivals_done.set()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(s.concurrency)]
    threads.append(threading.Thread(target=arrivals, daemon=True))
    for th in threads:
        th.start()

    samples: list[Sample] = []
    zero_streak = 0
    while True:
        t = time.monotonic() - t0
        visible, in_flight = queue.counts()
        samples.append(Sample(t, visible, in_flight, queue.dlq.depth()))
        zero_streak = zero_streak + 1 if (visible + in_flight == 0 and arrivals_done.is_set()) else 0
        if zero_streak >= 3 or t > max_seconds:
            break
        time.sleep(sample_every)
    stop.set()
    for th in threads:
        th.join(timeout=5)

    statuses = [store.get_order(o).status for o in created]
    obs = Observed(
        max_concurrency=inflight["max"],
        orders_created=len(created),
        orders_pending=sum(st is Status.PENDING for st in statuses),
        orders_reserved=sum(st is Status.RESERVED for st in statuses),
        orders_other=sum(st not in (Status.PENDING, Status.RESERVED) for st in statuses),
        stock_consumed=initial_stock - (store.available(SKU) or 0),
        dlq_final=queue.dlq.depth(),
    )
    p = predict(s, overhead_ms)
    verdict, t_rec = evaluate(s, p, samples, obs, min_run_seconds=min_run_seconds, model_tolerance=model_tolerance)
    return p, samples, obs, verdict, t_rec


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--backlog", type=int, default=600)
    ap.add_argument("--arrival-rate", type=float, default=40)
    ap.add_argument("--arrival-seconds", type=float, default=5)
    ap.add_argument("--concurrency", type=int, default=2)
    ap.add_argument("--delay-ms", type=int, default=20)
    ap.add_argument("--duplicate-ratio", type=float, default=0.1)
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("results/local"))
    a = ap.parse_args()

    s = Scenario(a.backlog, a.arrival_rate, a.arrival_seconds, a.concurrency, a.delay_ms, a.duplicate_ratio)
    p, samples, obs, verdict, t_rec = run_local(s)
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    out = write_results(
        a.out / stamp,
        mode="local-simulation",
        s=s,
        p=p,
        samples=samples,
        obs=obs,
        verdict=verdict,
        t_rec=t_rec,
        environment={"python": platform.python_version(), "machine": platform.machine(), "note": "not AWS evidence"},
    )
    for c in verdict.checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name:22} {c.detail}")
    measured = f"{t_rec:.2f}s" if t_rec is not None else "never"
    print(f"model {p.recovery_seconds:.2f}s, measured {measured} -> {out}")
    return 0 if verdict.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
