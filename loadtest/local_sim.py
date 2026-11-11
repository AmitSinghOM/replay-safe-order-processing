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

SKU = "LOCAL-LT"


class _Harness:
    """Store, queue, service and the counters one local run needs."""

    def __init__(self, s: Scenario) -> None:
        self.s = s
        self.store, self.queue = InMemoryStore(), InMemorySqs(max_receive_count=5)
        self.initial_stock = s.total_orders * s.qty_per_order * 2
        self.store.set_stock(SKU, self.initial_stock)
        self.svc = OrderService(self.store, self.queue, faults=dependency_delay(s.dependency_delay_ms))
        self.req = OrderRequest.parse({"customer_id": "lt-local", "lines": [{"sku": SKU, "qty": s.qty_per_order}]})
        self.created: list[str] = []
        self._dup_debt = 0.0
        self.stop = threading.Event()
        self.arrivals_done = threading.Event()
        self._inflight_lock = threading.Lock()
        self._inflight = 0
        self.max_inflight = 0

    def submit(self, n: int) -> None:
        order = self.svc.submit(f"lt-local-{n:08d}", self.req).order
        self.created.append(order.order_id)
        self._dup_debt += self.s.duplicate_ratio
        while self._dup_debt >= 1:  # deterministic duplicate stream
            self.queue.send({"type": "RESERVE", "order_id": order.order_id})
            self._dup_debt -= 1

    def worker(self) -> None:
        while not self.stop.is_set():
            got = self.queue.receive()
            if got is None:
                time.sleep(0.001)
                continue
            receipt, body, _ = got
            self._enter()
            try:
                self.svc.process(body)
            except Exception:  # cqa: ignore=PY-COR-002 reason="same as the Lambda worker: redeliver"
                self.queue.nack(receipt)
            else:
                self.queue.ack(receipt)
            finally:
                self._leave()

    def arrivals(self, t0: float) -> None:
        n, total = self.s.backlog, self.s.total_orders
        while n < total and not self.stop.is_set():
            due = self.s.backlog + int(self.s.arrival_rate * (time.monotonic() - t0))
            while n < min(due, total):
                self.submit(n)
                n += 1
            time.sleep(0.005)
        self.arrivals_done.set()

    def counts(self) -> tuple[int, int, int]:
        visible, in_flight = self.queue.counts()
        return visible, in_flight, self.queue.dlq.depth()

    def observe(self) -> Observed:
        statuses = [self.store.get_order(o).status for o in self.created]
        pending, reserved, other = count_statuses(statuses, Status.PENDING, Status.RESERVED)
        return Observed(
            max_concurrency=self.max_inflight,
            orders_created=len(self.created),
            orders_pending=pending,
            orders_reserved=reserved,
            orders_other=other,
            stock_consumed=self.initial_stock - (self.store.available(SKU) or 0),
            dlq_final=self.queue.dlq.depth(),
        )

    def _enter(self) -> None:
        with self._inflight_lock:
            self._inflight += 1
            self.max_inflight = max(self.max_inflight, self._inflight)

    def _leave(self) -> None:
        with self._inflight_lock:
            self._inflight -= 1


def run_local(
    s: Scenario,
    *,
    sample_every: float = 0.05,
    max_seconds: float = 60.0,
    overhead_ms: float = 0.2,
    model_tolerance: float | None = 0.5,
    min_run_seconds: float = 1.0,
) -> RunResult:
    h = _Harness(s)
    for n in range(s.backlog):
        h.submit(n)

    t0 = time.monotonic()
    threads = [threading.Thread(target=h.worker, daemon=True) for _ in range(s.concurrency)]
    threads.append(threading.Thread(target=h.arrivals, args=(t0,), daemon=True))
    for th in threads:
        th.start()
    samples = sample_until_drained(
        h.counts, t0=t0, arrivals_done=h.arrivals_done.is_set, sample_every=sample_every, max_seconds=max_seconds
    )
    h.stop.set()
    for th in threads:
        th.join(timeout=5)

    obs = h.observe()
    p = predict(s, overhead_ms)
    verdict, t_rec = evaluate(s, p, samples, obs, min_run_seconds=min_run_seconds, model_tolerance=model_tolerance)
    env = {"python": platform.python_version(), "machine": platform.machine(), "note": "not AWS evidence"}
    return RunResult("local-simulation", s, p, samples, obs, verdict, t_rec, env)


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
    result = run_local(s)
    out = write_results(a.out / dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ"), result)
    print("\n".join(result.summary_lines()), "->", out)
    return 0 if result.verdict.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
