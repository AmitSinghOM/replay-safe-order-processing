"""Backlog-recovery model, verdict and rendering. Pure functions, no I/O except ``write_results``.

Model (one worker = one Lambda instance processing a batch sequentially):

    mu      = concurrency * 1000 / service_ms        drain capacity, msg/s
    lambda  = arrival_rate * (1 + duplicate_ratio)   offered load, msg/s
    B0      = backlog * (1 + duplicate_ratio)        messages at t = 0

If mu > lambda the backlog shrinks at (mu - lambda) while arrivals continue,
then at mu once they stop. If mu <= lambda it grows until arrivals stop.
The model ignores SQS polling gaps and Lambda cold starts, so it is a lower
bound on real recovery time.
"""

from __future__ import annotations

import csv
import json
import math
import pathlib
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class Scenario:
    backlog: int  # orders already queued at t = 0
    arrival_rate: float  # new orders per second during the arrival window
    arrival_seconds: float
    concurrency: int  # max concurrent workers (SQS event-source MaximumConcurrency)
    dependency_delay_ms: int
    duplicate_ratio: float = 0.0  # extra duplicate messages per order (0.1 = 10 %)
    qty_per_order: int = 1

    def __post_init__(self) -> None:
        if self.backlog < 0 or self.arrival_rate < 0 or self.arrival_seconds < 0:
            raise ValueError("backlog, arrival_rate and arrival_seconds must be >= 0")
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if not 0 <= self.duplicate_ratio <= 1:
            raise ValueError("duplicate_ratio must be 0-1")

    @property
    def total_orders(self) -> int:
        return self.backlog + int(self.arrival_rate * self.arrival_seconds)


@dataclass(frozen=True)
class Prediction:
    service_ms: float
    drain_rate: float  # mu, msg/s
    offered_rate: float  # lambda, msg/s
    recovery_seconds: float  # inf if it never drains
    peak_backlog: float


def predict(s: Scenario, overhead_ms: float) -> Prediction:
    service_ms = s.dependency_delay_ms + overhead_ms
    if service_ms <= 0:
        raise ValueError("service time must be > 0")
    mu = s.concurrency * 1000.0 / service_ms
    lam = s.arrival_rate * (1 + s.duplicate_ratio)
    b0 = s.backlog * (1 + s.duplicate_ratio)
    t_arr = s.arrival_seconds
    if mu > lam:
        t_during = b0 / (mu - lam)
        if t_during <= t_arr:
            return Prediction(service_ms, mu, lam, t_during, b0)
        remaining = b0 - (mu - lam) * t_arr
        return Prediction(service_ms, mu, lam, t_arr + remaining / mu, b0)
    remaining = b0 + (lam - mu) * t_arr
    if mu == 0:
        return Prediction(service_ms, mu, lam, math.inf, remaining)
    return Prediction(service_ms, mu, lam, t_arr + remaining / mu, remaining)


@dataclass(frozen=True)
class Sample:
    t: float  # seconds since t0
    visible: int
    in_flight: int
    dlq: int
    error: str | None = None  # a failed sample is kept and counted, never dropped

    @property
    def backlog(self) -> int:
        return self.visible + self.in_flight


def recovered_at(samples: list[Sample], after: float, zero_streak: int = 2) -> float | None:
    """First time from which the backlog is zero in every later good sample (>= zero_streak of them)."""
    good = [s for s in samples if s.error is None and s.t >= after]
    for i, s in enumerate(good):
        tail = good[i:]
        if len(tail) >= zero_streak and all(x.backlog == 0 for x in tail):
            return s.t
    return None


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


@dataclass
class Verdict:
    checks: list[Check] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(c.passed for c in self.checks)

    def add(self, name: str, passed: bool, detail: str) -> None:
        self.checks.append(Check(name, passed, detail))


@dataclass(frozen=True)
class Observed:
    """Facts measured outside the samples."""

    max_concurrency: int  # workers seen in flight at once (local) or Lambda ConcurrentExecutions max (AWS)
    orders_created: int
    orders_pending: int  # must be 0 at the end
    orders_reserved: int
    orders_other: int  # rejected / missing; expected 0 with ample stock
    stock_consumed: int  # initial - final stock of the run's sku
    dlq_final: int


def evaluate(
    s: Scenario,
    p: Prediction,
    samples: list[Sample],
    obs: Observed,
    *,
    min_run_seconds: float,
    model_tolerance: float | None,
) -> tuple[Verdict, float | None]:
    v = Verdict()
    t_rec = recovered_at(samples, after=0.0)
    _check_samples(v, samples, t_rec, min_run_seconds)
    _check_invariants(v, s, obs)
    if model_tolerance is not None:
        _check_model(v, p, t_rec, model_tolerance)
    return v, t_rec


def _check_samples(v: Verdict, samples: list[Sample], t_rec: float | None, min_run_seconds: float) -> None:
    duration = samples[-1].t if samples else 0.0
    v.add("min_run_length", duration >= min_run_seconds, f"{duration:.1f}s >= {min_run_seconds:.1f}s")
    errors = sum(1 for x in samples if x.error)
    v.add("sample_errors", errors == 0, f"{errors} failed samples")
    v.add("backlog_seen", any(x.backlog > 0 for x in samples), "backlog was non-zero at some point")
    v.add("drained", t_rec is not None, f"recovered at {t_rec:.2f}s" if t_rec is not None else "never drained")


def _check_invariants(v: Verdict, s: Scenario, obs: Observed) -> None:
    v.add("no_pending_orders", obs.orders_pending == 0, f"{obs.orders_pending} still PENDING")
    v.add(
        "all_orders_reserved",
        obs.orders_reserved == obs.orders_created and obs.orders_other == 0,
        f"{obs.orders_reserved}/{obs.orders_created} reserved, {obs.orders_other} other",
    )
    expected_stock = obs.orders_reserved * s.qty_per_order
    v.add(
        "reserved_exactly_once",
        obs.stock_consumed == expected_stock,
        f"stock consumed {obs.stock_consumed}, expected {expected_stock} (duplicates sent: {s.duplicate_ratio:.0%})",
    )
    v.add("dlq_empty", obs.dlq_final == 0, f"{obs.dlq_final} in DLQ")
    v.add(
        "concurrency_capped",
        1 <= obs.max_concurrency <= s.concurrency,  # < 1 means not measured: fail, don't pass vacuously
        f"max {obs.max_concurrency} <= cap {s.concurrency}",
    )


def _check_model(v: Verdict, p: Prediction, t_rec: float | None, tolerance: float) -> None:
    if t_rec is None or not math.isfinite(p.recovery_seconds):
        return
    ratio = t_rec / p.recovery_seconds if p.recovery_seconds > 0 else math.inf
    ok = (1 - tolerance) <= ratio <= (1 + tolerance)
    v.add("matches_model", ok, f"measured/predicted = {ratio:.2f} (tolerance +/-{tolerance:.0%})")


def count_statuses(statuses: Iterable[object], pending: object, reserved: object) -> tuple[int, int, int]:
    """(pending, reserved, other) counts; the runners pass Status.PENDING / Status.RESERVED."""
    counts = [0, 0, 0]
    for st in statuses:
        counts[0 if st is pending else 1 if st is reserved else 2] += 1
    return counts[0], counts[1], counts[2]


def sample_until_drained(
    read: Callable[[], tuple[int, int, int]],
    *,
    t0: float,
    arrivals_done: Callable[[], bool],
    sample_every: float,
    max_seconds: float,
    zero_streak_needed: int = 3,
    on_sample: Callable[[Sample], None] | None = None,
) -> list[Sample]:
    """Poll ``read`` -> (visible, in_flight, dlq) until the backlog stays at zero after arrivals end.

    A failed read is recorded as an error sample (and counted by ``evaluate``), never dropped.
    """
    samples: list[Sample] = []
    zero_streak = 0
    while True:
        t = time.monotonic() - t0
        try:
            visible, in_flight, dlq = read()
            sample = Sample(t, visible, in_flight, dlq)
            zero_streak = zero_streak + 1 if (visible + in_flight == 0 and arrivals_done()) else 0
        except Exception as exc:  # cqa: ignore=PY-COR-002 reason="recorded as a failing sample"
            sample = Sample(t, 0, 0, 0, error=f"{type(exc).__name__}: {exc}")
            zero_streak = 0
        samples.append(sample)
        if on_sample:
            on_sample(sample)
        if zero_streak >= zero_streak_needed or t > max_seconds:
            return samples
        time.sleep(sample_every)


# ---- rendering and results ---------------------------------------------------

_W, _H, _PAD = 720, 360, 50


def render_svg(samples: list[Sample], s: Scenario, p: Prediction, t_rec: float | None, title: str) -> str:
    """Dependency-free line chart: measured backlog vs model."""
    good = [x for x in samples if x.error is None]
    t_max = max([x.t for x in good] + [p.recovery_seconds if math.isfinite(p.recovery_seconds) else 0, 1])
    b_max = max([x.backlog for x in good] + [p.peak_backlog, 1])

    def xy(t: float, b: float) -> tuple[float, float]:
        return _PAD + (_W - 2 * _PAD) * t / t_max, _H - _PAD - (_H - 2 * _PAD) * b / b_max

    measured = _polyline(((x.t, x.backlog) for x in good), xy)
    model = _polyline(_model_points(s, p, t_max), xy)
    marker = _recovery_marker(t_rec, xy)
    subtitle = (
        f"C={s.concurrency}, delay={s.dependency_delay_ms}ms, backlog={s.backlog}, "
        f"arrivals={s.arrival_rate}/s for {s.arrival_seconds:.0f}s, dup={s.duplicate_ratio:.0%}"
    )
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{_W}" height="{_H}" font-family="sans-serif" role="img"
 aria-label="{title}: backlog over time, measured versus model">
<rect width="100%" height="100%" fill="#fff"/>
<text x="{_PAD}" y="20" font-size="14">{title}</text>
<text x="{_PAD}" y="36" font-size="11" fill="#555">{subtitle}</text>
<line x1="{_PAD}" y1="{_H - _PAD}" x2="{_W - _PAD}" y2="{_H - _PAD}" stroke="#000"/>
<line x1="{_PAD}" y1="{_PAD}" x2="{_PAD}" y2="{_H - _PAD}" stroke="#000"/>
<text x="{_W / 2:.0f}" y="{_H - 12}" font-size="11" text-anchor="middle">seconds (0 to {t_max:.0f})</text>
<text x="14" y="{_H / 2:.0f}" font-size="11" transform="rotate(-90 14 {_H / 2:.0f})" text-anchor="middle">messages
 (0 to {b_max:.0f})</text>
<polyline points="{model}" fill="none" stroke="#d62728" stroke-width="1.5" stroke-dasharray="6,4"/>
<polyline points="{measured}" fill="none" stroke="#1f77b4" stroke-width="2"/>
{marker}
<text x="{_W - _PAD - 150}" y="{_PAD + 12}" font-size="11" fill="#1f77b4">measured</text>
<text x="{_W - _PAD - 150}" y="{_PAD + 26}" font-size="11" fill="#d62728">model (lower bound)</text>
</svg>
"""


def _polyline(points: Iterable[tuple[float, float]], xy: Callable[[float, float], tuple[float, float]]) -> str:
    return " ".join(f"{a:.1f},{b:.1f}" for a, b in (xy(t, b) for t, b in points))


def _recovery_marker(t_rec: float | None, xy: Callable[[float, float], tuple[float, float]]) -> str:
    if t_rec is None:
        return ""
    rx, _ = xy(t_rec, 0)
    return (
        f'<line x1="{rx:.1f}" y1="{_PAD}" x2="{rx:.1f}" y2="{_H - _PAD}" stroke="#888" stroke-dasharray="2,3"/>'
        f'<text x="{rx + 4:.1f}" y="{_PAD + 12}" font-size="11">recovered {t_rec:.0f}s</text>'
    )


def _model_points(s: Scenario, p: Prediction, t_max: float) -> list[tuple[float, float]]:
    b0 = s.backlog * (1 + s.duplicate_ratio)
    pts, steps = [], 200
    b = b0
    dt = t_max / steps
    for i in range(steps + 1):
        t = i * dt
        pts.append((t, max(b, 0.0)))
        inflow = p.offered_rate if t < s.arrival_seconds else 0.0
        b = max(b + (inflow - p.drain_rate) * dt, 0.0)
    return pts


@dataclass(frozen=True)
class RunResult:
    """Everything one load run produced. ``write_results`` and the CLIs consume it."""

    mode: str
    scenario: Scenario
    prediction: Prediction
    samples: list[Sample]
    observed: Observed
    verdict: Verdict
    recovered_seconds: float | None
    environment: dict = field(default_factory=dict)

    def summary_lines(self) -> list[str]:
        lines = [f"{'PASS' if c.passed else 'FAIL'}  {c.name:22} {c.detail}" for c in self.verdict.checks]
        measured = f"{self.recovered_seconds:.2f}s" if self.recovered_seconds is not None else "never"
        lines.append(f"model {self.prediction.recovery_seconds:.2f}s, measured {measured}")
        return lines


def write_results(out_dir: pathlib.Path, r: RunResult) -> pathlib.Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "samples.csv").open("w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["t_seconds", "visible", "in_flight", "backlog", "dlq", "error"])
        for x in r.samples:
            wr.writerow([f"{x.t:.2f}", x.visible, x.in_flight, x.backlog, x.dlq, x.error or ""])
    prediction = {k: (None if isinstance(v, float) and math.isinf(v) else v) for k, v in asdict(r.prediction).items()}
    summary = {
        "mode": r.mode,
        "passed": r.verdict.passed,
        "scenario": asdict(r.scenario),
        "prediction": prediction,
        "measured_recovery_seconds": r.recovered_seconds,
        "observed": asdict(r.observed),
        "checks": [asdict(c) for c in r.verdict.checks],
        "environment": r.environment,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    svg = render_svg(r.samples, r.scenario, r.prediction, r.recovered_seconds, f"Backlog recovery ({r.mode})")
    (out_dir / "curve.svg").write_text(svg)
    return out_dir
