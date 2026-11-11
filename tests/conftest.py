from __future__ import annotations

import itertools

import pytest

from orders.adapters.memory import InMemorySqs, InMemoryStore
from orders.domain import OrderRequest
from orders.service import OrderService


class Clock:
    def __init__(self, start: float = 1_700_000_000) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Faults:
    """Raise ``exc`` at ``point`` for the next ``times`` hits."""

    def __init__(self) -> None:
        self._plan: dict[str, tuple[type[BaseException], int]] = {}
        self.hits: dict[str, int] = {}

    def arm(self, point: str, exc: type[BaseException], times: int = 1) -> None:
        self._plan[point] = (exc, times)

    def __call__(self, point: str) -> None:
        self.hits[point] = self.hits.get(point, 0) + 1
        exc, times = self._plan.get(point, (None, 0))
        if exc and times > 0:
            self._plan[point] = (exc, times - 1)
            raise exc(f"injected at {point}")


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def faults() -> Faults:
    return Faults()


@pytest.fixture
def store() -> InMemoryStore:
    s = InMemoryStore()
    s.set_stock("WIDGET", 10)
    s.set_stock("GADGET", 3)
    return s


@pytest.fixture
def queue() -> InMemorySqs:
    return InMemorySqs(max_receive_count=5)


@pytest.fixture
def service(store, queue, clock, faults) -> OrderService:
    ids = (f"order-{n}" for n in itertools.count(1))
    return OrderService(store, queue, clock=clock, id_factory=lambda: next(ids), faults=faults)


def request(**overrides) -> OrderRequest:
    body = {"customer_id": "cust-1", "lines": [{"sku": "WIDGET", "qty": 2}]}
    body.update(overrides)
    return OrderRequest.parse(body)
