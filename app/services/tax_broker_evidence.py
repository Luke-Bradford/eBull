"""The account's broker trades as UK tax evidence (#3616 slice 3c).

Spec: docs/specs/metrics/2026-10-13-3616-slice3c-broker-tax-evidence.md.

Part 2a (this module so far) captures the broker's order lookups (Design 3). The broker answered ``orders:lookup``
for recent orders only (spec fact 2), so lookups are captured early and kept as text in ``tax_order_lookups``
(sql/486), one append-only row per attempt. Part 2b adds the pure derivation, the sterling values and the publishing
run (Design 4-6) on the same lock and policy id.

- ``classify_lookup`` is the response table of Design 3; ``lookup_candidates`` and ``plan_lookups`` are its candidate
  table, caps and interleaving. All three are pure.
- ``run_order_lookup_capture`` takes the session lock, checks the ledger scope before asking the broker anything
  (Design 6 steps 1-3), and commits each attempt as it lands.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Protocol

import psycopg
import psycopg.rows

from app.providers.broker import RawOrderLookup

logger = logging.getLogger(__name__)

#: Names the checks, field table, product mapping, normalisation, tolerances, candidate rules and keys (Design 4.11).
#: Changing any of them is a new policy id.
TAX_EVIDENCE_POLICY_ID = "broker_evidence_v1"

#: Session advisory lock held for a whole run (Design 6): int.from_bytes(b"taxevid3", "big").
TAX_EVIDENCE_LOCK_KEY = 8386116358314222643

LookupOutcome = Literal[
    "found", "malformed", "not_found", "rate_limited", "auth_failed", "http_error", "transport_error"
]

#: At most this many attempts per run, split first-attempt / retry, with a queue's unused slots going to the other.
LOOKUP_CAP = 40
FIRST_ATTEMPT_SLOTS = 30
RETRY_SLOTS = 10
#: The age at which a malformed or failing-check body is asked again, and the settling lookup's delay.
RECHECK_AFTER = timedelta(days=7)

_RETRYABLE_ERRORS: frozenset[str] = frozenset({"rate_limited", "transport_error", "http_error", "auth_failed"})
_STOP_OUTCOMES: frozenset[str] = frozenset({"rate_limited", "auth_failed"})


class OrderLookupReader(Protocol):
    def lookup_order_raw(self, order_id: int) -> RawOrderLookup: ...


#: The largest value a BIGINT column holds: an id from a broker body is bound to one, so a larger value is not an id.
BIGINT_MAX = 2**63 - 1


def _is_id(value: Any) -> bool:
    return type(value) is int and 0 < value <= BIGINT_MAX


def classify_lookup(order_id: int, raw: RawOrderLookup) -> tuple[LookupOutcome, int | None]:
    """The outcome of one attempt and, when ``found``, the body's ``accountId`` if it is one valid id (Design 3).

    ``found`` binds the body to the order only: some top-level key equal to ``orderid`` case-insensitively holds
    the asked id. Every other field, including a second order-id spelling that disagrees, is validated at
    derivation. A 200 that is not so bound is ``malformed``: it is evidence of nothing about the order.
    """
    status = raw.http_status
    if status is None:
        return "transport_error", None
    if status == 404:
        return "not_found", None
    if status == 429:
        return "rate_limited", None
    if status in (401, 403):
        return "auth_failed", None
    if status != 200:
        return "http_error", None
    try:
        body = json.loads(raw.body_text or "", parse_float=Decimal)
    except ValueError, RecursionError:
        return "malformed", None
    if not isinstance(body, dict):
        return "malformed", None
    if not any(k.lower() == "orderid" and _is_id(v) and v == order_id for k, v in body.items()):
        return "malformed", None
    accounts = [v for k, v in body.items() if k.lower() == "accountid"]
    account = accounts[0] if accounts and all(_is_id(a) and a == accounts[0] for a in accounts) else None
    return "found", account


@dataclass(frozen=True)
class LedgerOrder:
    """One order of the ledger as the candidate rules see it."""

    order_id: int
    max_event_id: int
    opened_at: datetime
    last_event_at: datetime
    held: bool


@dataclass(frozen=True)
class LookupAttempt:
    lookup_id: int
    order_id: int
    requested_at: datetime
    policy_id: str
    outcome: str
    order_event_max_id: int


def lookup_candidates(
    orders: Iterable[LedgerOrder],
    attempts: Iterable[LookupAttempt],
    now: datetime,
    policy_id: str = TAX_EVIDENCE_POLICY_ID,
) -> tuple[list[LedgerOrder], list[LedgerOrder]]:
    """The first-attempt and retry queues, each in asking order (Design 3, candidate table).

    The first-attempt queue is newest order first, because retention (if any) loses the oldest first; the retry
    queue is oldest latest attempt first, so a failing candidate goes to the back after each attempt.

    ⚠ The row "*F* fails a Design 4 field check" needs the derivation and arrives with it (part 2b).
    ⚠ The settling lookup is asked once ``now`` is 7 days past the order's last event: the spec's row ("no attempt
    was made 7 or more days after its last event") read alone would re-ask daily until then.
    """
    by_order: dict[int, list[LookupAttempt]] = {}
    for attempt in attempts:
        by_order.setdefault(attempt.order_id, []).append(attempt)

    first: list[LedgerOrder] = []
    retry: list[tuple[datetime, int, LedgerOrder]] = []
    for order in orders:
        history = sorted(by_order.get(order.order_id, []), key=lambda a: a.lookup_id)
        if not history:
            first.append(order)
            continue
        latest = history[-1]
        found = [a for a in history if a.outcome == "found"]
        f = found[-1] if found else None
        e = order.max_event_id
        ask = (
            latest.outcome in _RETRYABLE_ERRORS
            or (
                latest.outcome == "malformed"
                and (
                    e > latest.order_event_max_id
                    or now - latest.requested_at >= RECHECK_AFTER
                    or latest.policy_id != policy_id
                )
            )
            or (latest.outcome == "not_found" and e > latest.order_event_max_id)
            or (f is not None and e > f.order_event_max_id)
            or (
                f is not None
                and not order.held
                and now >= order.last_event_at + RECHECK_AFTER
                and not any(a.requested_at >= order.last_event_at + RECHECK_AFTER for a in history)
            )
        )
        if ask:
            retry.append((latest.requested_at, latest.lookup_id, order))

    first.sort(key=lambda o: (o.opened_at, o.order_id), reverse=True)
    retry.sort(key=lambda t: (t[0], t[1]))
    return first, [order for _, _, order in retry]


def plan_lookups(first: Sequence[LedgerOrder], retry: Sequence[LedgerOrder]) -> list[LedgerOrder]:
    """At most 40: up to 30 first attempts and 10 retries, a queue's unused slots going to the other, interleaved
    one retry then three first attempts so a run stopped early has still served retries in proportion."""
    n_first = min(len(first), FIRST_ATTEMPT_SLOTS + max(0, RETRY_SLOTS - len(retry)))
    n_retry = min(len(retry), LOOKUP_CAP - n_first)
    firsts, retries = list(first[:n_first]), list(retry[:n_retry])
    plan: list[LedgerOrder] = []
    while firsts or retries:
        if retries:
            plan.append(retries.pop(0))
        for _ in range(3):
            if firsts:
                plan.append(firsts.pop(0))
    return plan


def _payload_order_ids(payload: dict[str, Any]) -> set[int]:
    return {v for k, v in payload.items() if k.lower() == "orderid" and _is_id(v)}


def load_ledger_orders(conn: psycopg.Connection[Any], environment: str) -> list[LedgerOrder]:
    """Every order the environment's ledger names, from the column and every payload spelling (Design 4.3)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        events = cur.execute(
            """
            SELECT event_id, position_id, event_kind, executed_at, order_id, raw_payload
            FROM trade_events
            WHERE environment = %(env)s
            """,
            {"env": environment},
        ).fetchall()
        held = {
            r["position_id"]
            for r in cur.execute("SELECT position_id FROM broker_positions WHERE position_id >= 0").fetchall()
        }
    positions_by_order: dict[int, set[int]] = {}
    rows_by_order: dict[int, list[dict[str, Any]]] = {}
    for event in events:
        ids = _payload_order_ids(event["raw_payload"])
        if event["order_id"] is not None:
            ids.add(event["order_id"])
        for order_id in ids:
            positions_by_order.setdefault(order_id, set()).add(event["position_id"])
            rows_by_order.setdefault(order_id, []).append(event)
    orders: list[LedgerOrder] = []
    for order_id, rows in rows_by_order.items():
        opens = [r["executed_at"] for r in rows if r["event_kind"] == "open"]
        orders.append(
            LedgerOrder(
                order_id=order_id,
                max_event_id=max(r["event_id"] for r in rows),
                opened_at=min(opens) if opens else min(r["executed_at"] for r in rows),
                last_event_at=max(r["executed_at"] for r in rows),
                held=bool(positions_by_order[order_id] & held),
            )
        )
    return orders


def load_lookup_attempts(conn: psycopg.Connection[Any], environment: str) -> list[LookupAttempt]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        rows = cur.execute(
            """
            SELECT lookup_id, order_id, requested_at, policy_id, outcome, order_event_max_id
            FROM tax_order_lookups
            WHERE environment = %(env)s
            ORDER BY lookup_id
            """,
            {"env": environment},
        ).fetchall()
    return [LookupAttempt(**row) for row in rows]


@dataclass
class CaptureReport:
    state: Literal["captured", "refused", "skipped"]
    reason: str | None = None
    attempted: int = 0
    deferred: int = 0
    stopped_by: str | None = None
    outcomes: dict[str, int] = field(default_factory=dict)


def capture_order_lookups(
    conn: psycopg.Connection[Any],
    broker: OrderLookupReader,
    environment: str,
    now: datetime | None = None,
) -> CaptureReport:
    """Ask the broker about the planned orders, committing each attempt as it lands (Design 3).

    ``conn`` must be in autocommit mode: an attempt is evidence the moment it is made.
    """
    now = now if now is not None else datetime.now(UTC)
    first, retry = lookup_candidates(
        load_ledger_orders(conn, environment), load_lookup_attempts(conn, environment), now
    )
    plan = plan_lookups(first, retry)
    report = CaptureReport(state="captured")
    for order in plan:
        requested_at = datetime.now(UTC)
        try:
            raw = broker.lookup_order_raw(order.order_id)
        except Exception as exc:
            # Not an HTTP outcome: a defect. The attempt is still recorded, then the run fails loudly.
            _insert_attempt(
                conn,
                environment,
                order,
                requested_at,
                RawOrderLookup(http_status=None, body_text=None, error_detail=f"{type(exc).__name__}: {exc}"),
                "transport_error",
                None,
            )
            raise
        outcome, account_id = classify_lookup(order.order_id, raw)
        _insert_attempt(conn, environment, order, requested_at, raw, outcome, account_id)
        report.attempted += 1
        report.outcomes[outcome] = report.outcomes.get(outcome, 0) + 1
        if outcome in _STOP_OUTCOMES:
            report.stopped_by = outcome
            break
    report.deferred = len(first) + len(retry) - report.attempted
    return report


def _insert_attempt(
    conn: psycopg.Connection[Any],
    environment: str,
    order: LedgerOrder,
    requested_at: datetime,
    raw: RawOrderLookup,
    outcome: LookupOutcome,
    account_id: int | None,
) -> None:
    body = raw.body_text
    error = raw.error_detail
    if body is not None and "\x00" in body:
        # Postgres TEXT cannot hold NUL. Such a body is not valid JSON (classified malformed, or by its status), so
        # it is kept as its hash and length rather than lost with the attempt.
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        error = f"body not stored: contains NUL (sha256 {digest}, {len(body)} chars)" + (f"; {error}" if error else "")
        body = None
    conn.execute(
        """
        INSERT INTO tax_order_lookups
            (environment, order_id, requested_at, policy_id, outcome, http_status, error_detail,
             body_text, body_sha256, account_id, order_event_max_id)
        VALUES
            (%(env)s, %(order_id)s, %(requested_at)s, %(policy)s, %(outcome)s, %(status)s, %(error)s,
             %(body)s, %(sha)s, %(account)s, %(e)s)
        """,
        {
            "env": environment,
            "order_id": order.order_id,
            "requested_at": requested_at,
            "policy": TAX_EVIDENCE_POLICY_ID,
            "outcome": outcome,
            "status": raw.http_status,
            "error": error,
            "body": body,
            "sha": hashlib.sha256(body.encode("utf-8")).hexdigest() if body is not None else None,
            "account": account_id,
            "e": order.max_event_id,
        },
    )


def run_order_lookup_capture(
    conn: psycopg.Connection[Any],
    broker: OrderLookupReader,
    environment: str,
) -> CaptureReport:
    """Design 6 steps 1-3 on one autocommit connection: lock, scope pre-check, capture.

    Another run holding the lock: ``skipped``, nothing written. No scope row, or a scope in another environment:
    ``refused``, nothing asked of the broker. The lock is released in every case.
    """
    if not conn.autocommit:
        raise ValueError("run_order_lookup_capture needs an autocommit connection: each attempt commits as it lands")
    row = conn.execute("SELECT pg_try_advisory_lock(%s)", [TAX_EVIDENCE_LOCK_KEY]).fetchone()
    if row is None or not row[0]:
        return CaptureReport(state="skipped", reason="another run holds the lock")
    try:
        scope = conn.execute("SELECT environment FROM trade_ledger_scope").fetchone()
        if scope is None:
            return CaptureReport(state="refused", reason="scope_unestablished")
        if scope[0] != environment:
            return CaptureReport(state="refused", reason="scope_mismatch")
        return capture_order_lookups(conn, broker, environment)
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", [TAX_EVIDENCE_LOCK_KEY])
