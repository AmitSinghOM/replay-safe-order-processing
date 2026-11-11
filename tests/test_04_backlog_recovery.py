"""Failure 4: dependency slows down -> controlled concurrency, measured recovery.

Two layers:
 * the verdict oracle, on synthetic samples (so a broken harness cannot pass);
 * a short local run of the real OrderService under a simulated slow dependency.
The AWS curve comes from ``loadtest.aws_run`` against a deployed stack.
"""

from __future__ import annotations

import math

import pytest

from loadtest.curve import Observed, Sample, Scenario, evaluate, predict, recovered_at, render_svg
from loadtest.local_sim import run_local
from orders.service import FAULT_AFTER_COMMIT, FAULT_BEFORE_COMMIT, dependency_delay

S = Scenario(backlog=100, arrival_rate=10, arrival_seconds=5, concurrency=2, dependency_delay_ms=90)


def _good_obs(**kw) -> Observed:
    base = dict(
        max_concurrency=2,
        orders_created=150,
        orders_pending=0,
        orders_reserved=150,
        orders_other=0,
        stock_consumed=150,
        dlq_final=0,
    )
    base.update(kw)
    return Observed(**base)


def _curve(points: list[tuple[float, int]]) -> list[Sample]:
    return [Sample(t, b, 0, 0) for t, b in points]


GOOD = _curve([(0, 100), (2, 60), (4, 20), (6, 0), (7, 0), (8, 0)])


def _verdict(samples=GOOD, obs=None, min_run=5.0, tol=None):
    return evaluate(S, predict(S, 10), samples, obs or _good_obs(), min_run_seconds=min_run, model_tolerance=tol)


# ---- model -------------------------------------------------------------------


def test_model_cases_exact():
    # service 90 + 10 ms -> mu = 2 * 1000 / 100 = 20 msg/s
    assert predict(S, 10).drain_rate == pytest.approx(20)
    # drains before arrivals stop
    p = predict(Scenario(100, 10, 60, 2, 90), 10)
    assert p.recovery_seconds == pytest.approx(10.0)
    # arrivals stop first
    p = predict(Scenario(100, 10, 5, 2, 90), 10)
    assert p.recovery_seconds == pytest.approx(7.5)
    # overloaded while arriving: grows, then drains
    p = predict(Scenario(100, 30, 10, 2, 90), 10)
    assert p.peak_backlog == pytest.approx(200)
    assert p.recovery_seconds == pytest.approx(20.0)


def test_more_concurrency_recovers_faster_in_the_model():
    slow = predict(Scenario(1000, 2, 300, 2, 500), 40).recovery_seconds
    fast = predict(Scenario(1000, 2, 300, 10, 500), 40).recovery_seconds
    assert fast < slow


# ---- oracle ------------------------------------------------------------------


def test_good_run_passes():
    v, t_rec = _verdict()
    assert v.passed, [c for c in v.checks if not c.passed]
    assert t_rec == 6


def test_recovery_needs_a_lasting_zero():
    blip = _curve([(0, 50), (1, 0), (2, 30), (3, 0), (4, 0)])
    assert recovered_at(blip, after=0) == 3


@pytest.mark.parametrize(
    "name,kwargs",
    [
        ("min_run_length", dict(min_run=60.0)),
        ("drained", dict(samples=_curve([(0, 100), (5, 80), (10, 60)]))),
        ("sample_errors", dict(samples=GOOD + [Sample(9, 0, 0, 0, error="Throttling")])),
        ("backlog_seen", dict(samples=_curve([(0, 0), (5, 0), (10, 0)]))),
        ("reserved_exactly_once", dict(obs=_good_obs(stock_consumed=165))),  # duplicates double-reserved
        ("no_pending_orders", dict(obs=_good_obs(orders_pending=3, orders_reserved=147))),
        ("dlq_empty", dict(obs=_good_obs(dlq_final=1))),
        ("concurrency_capped", dict(obs=_good_obs(max_concurrency=5))),
        ("concurrency_capped", dict(obs=_good_obs(max_concurrency=-1))),  # metric missing
        ("matches_model", dict(tol=0.1, samples=_curve([(0, 100), (20, 50), (30, 0), (31, 0), (32, 0)]))),
    ],
)
def test_each_check_can_fail(name, kwargs):
    v, _ = _verdict(**kwargs)
    assert not v.passed
    assert name in [c.name for c in v.checks if not c.passed]


def test_svg_renders_with_and_without_recovery():
    p = predict(S, 10)
    assert math.isfinite(p.recovery_seconds)
    for t_rec in (None, 6.0):
        svg = render_svg(GOOD, S, p, t_rec, "t")
        assert svg.startswith("<svg") and "aria-label" in svg


# ---- slow-dependency seam ----------------------------------------------------


def test_dependency_delay_sleeps_only_before_commit():
    slept = []
    inject = dependency_delay(250, sleep=slept.append)
    inject(FAULT_AFTER_COMMIT)
    inject(FAULT_BEFORE_COMMIT)
    assert slept == [0.25]


@pytest.mark.parametrize("bad", [-1, 1501])
def test_dependency_delay_is_bounded(bad):
    with pytest.raises(ValueError):
        dependency_delay(bad)


# ---- local run of the real service ------------------------------------------


def _local(concurrency: int):
    s = Scenario(
        backlog=200,
        arrival_rate=40,
        arrival_seconds=1.5,
        concurrency=concurrency,
        dependency_delay_ms=10,
        duplicate_ratio=0.1,
    )
    return s, run_local(s, sample_every=0.02, model_tolerance=None, min_run_seconds=0.5)


@pytest.mark.slow
def test_local_run_recovers_with_capped_concurrency_and_no_double_reservation():
    s, (p, samples, obs, verdict, t_rec) = _local(concurrency=2)
    assert verdict.passed, [c for c in verdict.checks if not c.passed]
    assert obs.max_concurrency == 2
    assert obs.stock_consumed == obs.orders_created  # 10 % duplicates, still once each
    assert t_rec >= 0.8 * p.recovery_seconds  # the cap really limits throughput


@pytest.mark.slow
def test_raising_the_cap_shortens_recovery():
    _, (_, _, _, _, t2) = _local(concurrency=2)
    _, (_, _, _, _, t4) = _local(concurrency=4)
    assert t4 < t2
