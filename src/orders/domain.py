"""Order domain: values, the state machine, and request validation.

Nothing here does I/O. Every rule about which status may follow which lives in
``TRANSITIONS`` so the API, the worker and the replay tool cannot disagree.
"""

from __future__ import annotations

import enum
import hashlib
import json
import re
from dataclasses import dataclass, field

MAX_LINES = 10  # keeps one reservation inside a single DynamoDB transaction (limit 100 items)
MAX_QTY = 100
_SKU_RE = re.compile(r"^[A-Z0-9][A-Z0-9-]{0,31}$")
_IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{8,128}$")


class Status(str, enum.Enum):
    PENDING = "PENDING"
    RESERVED = "RESERVED"
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class Event(str, enum.Enum):
    RESERVE = "RESERVE"
    REJECT = "REJECT"
    CONFIRM = "CONFIRM"
    CANCEL = "CANCEL"


# The only legal moves. Anything not listed is an invalid transition.
TRANSITIONS: dict[tuple[Status, Event], Status] = {
    (Status.PENDING, Event.RESERVE): Status.RESERVED,
    (Status.PENDING, Event.REJECT): Status.REJECTED,
    (Status.PENDING, Event.CANCEL): Status.CANCELLED,
    (Status.RESERVED, Event.CONFIRM): Status.CONFIRMED,
    (Status.RESERVED, Event.CANCEL): Status.CANCELLED,  # releases reserved stock
}

TERMINAL: frozenset[Status] = frozenset({Status.CONFIRMED, Status.REJECTED, Status.CANCELLED})


class DomainError(Exception):
    """Base class for errors the API maps to a 4xx response."""


class ValidationError(DomainError):
    pass


class InvalidTransition(DomainError):
    def __init__(self, current: Status, event: Event) -> None:
        super().__init__(f"event {event.value} is not allowed in status {current.value}")
        self.current = current
        self.event = event


class IdempotencyConflict(DomainError):
    """Same idempotency key reused with a different request body."""


class OrderNotFound(DomainError):
    pass


def next_status(current: Status, event: Event) -> Status:
    try:
        return TRANSITIONS[(current, event)]
    except KeyError:
        raise InvalidTransition(current, event) from None


@dataclass(frozen=True)
class Line:
    sku: str
    qty: int


@dataclass(frozen=True)
class OrderRequest:
    customer_id: str
    lines: tuple[Line, ...]

    @classmethod
    def parse(cls, body: object) -> OrderRequest:
        if not isinstance(body, dict):
            raise ValidationError("body must be a JSON object")
        customer_id = body.get("customer_id")
        if not isinstance(customer_id, str) or not 1 <= len(customer_id) <= 64:
            raise ValidationError("customer_id must be a string of 1-64 characters")
        raw_lines = body.get("lines")
        if not isinstance(raw_lines, list) or not 1 <= len(raw_lines) <= MAX_LINES:
            raise ValidationError(f"lines must be a list of 1-{MAX_LINES} items")
        merged: dict[str, int] = {}
        for raw in raw_lines:
            if not isinstance(raw, dict):
                raise ValidationError("each line must be an object")
            sku, qty = raw.get("sku"), raw.get("qty")
            if not isinstance(sku, str) or not _SKU_RE.match(sku):
                raise ValidationError(f"invalid sku: {sku!r}")
            if not isinstance(qty, int) or isinstance(qty, bool) or not 1 <= qty <= MAX_QTY:
                raise ValidationError(f"qty must be an integer 1-{MAX_QTY}")
            # A transaction may touch an item only once, so duplicate SKUs are merged.
            merged[sku] = merged.get(sku, 0) + qty
        if any(q > MAX_QTY for q in merged.values()):
            raise ValidationError(f"total qty per sku must be <= {MAX_QTY}")
        lines = tuple(Line(sku, qty) for sku, qty in sorted(merged.items()))
        return cls(customer_id=customer_id, lines=lines)

    def fingerprint(self) -> str:
        """Stable hash of the normalized request, stored with the idempotency key."""
        canonical = json.dumps(
            {"customer_id": self.customer_id, "lines": [[l.sku, l.qty] for l in self.lines]},
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


def validate_idempotency_key(key: object) -> str:
    if not isinstance(key, str) or not _IDEMPOTENCY_KEY_RE.match(key):
        raise ValidationError("Idempotency-Key header must be 8-128 chars of [A-Za-z0-9_-]")
    return key


@dataclass
class Order:
    order_id: str
    customer_id: str
    lines: tuple[Line, ...]
    status: Status
    version: int
    created_at: int
    updated_at: int
    reason: str | None = None

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "lines": [{"sku": l.sku, "qty": l.qty} for l in self.lines],
            "status": self.status.value,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class IdempotencyRecord:
    key: str
    request_fingerprint: str
    order_id: str
    expires_at: int

    def is_live(self, now: int) -> bool:
        # DynamoDB TTL deletes lazily, so expiry is always checked explicitly.
        return self.expires_at > now


@dataclass(frozen=True)
class AuditEntry:
    subject: str  # order id, or "replay"
    action: str
    at: int
    detail: dict = field(default_factory=dict)
