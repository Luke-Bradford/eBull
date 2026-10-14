"""The account's broker trades as UK tax evidence (#3616 slice 3c).

Spec: docs/specs/metrics/2026-10-13-3616-slice3c-broker-tax-evidence.md.

Part 2a (this module so far) captures the broker's order lookups (Design 3). The broker answered ``orders:lookup``
for recent orders only (spec fact 2), so lookups are captured early and kept as text in ``tax_order_lookups``
(sql/486), one append-only row per attempt. The pure derivation and sterling values (Design 4-5) are in
``app/services/tax_broker_derivation.py``.

- ``classify_lookup`` is the response table of Design 3; ``lookup_candidates`` and ``plan_lookups`` are its candidate
  table, caps and interleaving. All three are pure.
- ``run_tax_broker_evidence`` is a whole run (Design 6): the session lock, the scope pre-check before anything is
  asked of the broker, the capture (each attempt committed as it lands), one snapshot, the derivation, and one
  publishing transaction (sql/488).
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
from psycopg.types.json import Jsonb

from app.db.snapshot import snapshot_read
from app.providers.broker import RawOrderLookup
from app.services.tax_broker_derivation import (
    TAX_EVIDENCE_POLICY_ID,
    EvidenceLot,
    FoundLookup,
    FxRefusal,
    HeldPosition,
    LedgerEvent,
    RetainedObservation,
    Scope,
    SubjectEvidence,
    derive_all,
    lookup_field_failures,
    parse_lookup_body,
    price_lots,
)
from app.services.tax_fx import load_tax_fx_state, tax_fx_state_is_current
from app.services.trade_events import canonical_decimal, normalise_payload, payload_account_id

logger = logging.getLogger(__name__)

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
    failing_found: frozenset[int] = frozenset(),
) -> tuple[list[LedgerOrder], list[LedgerOrder]]:
    """The first-attempt and retry queues, each in asking order (Design 3, candidate table).

    The first-attempt queue is newest order first, because retention (if any) loses the oldest first; the retry
    queue is oldest latest attempt first, so a failing candidate goes to the back after each attempt.

    ``failing_found`` holds the ``lookup_id`` of every ``found`` body that fails a Design 4 field check
    (``load_failing_found``); such an *F* is asked again once 7 days old or under another policy.
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
            or (
                f is not None
                and f.lookup_id in failing_found
                and (now - f.requested_at >= RECHECK_AFTER or f.policy_id != policy_id)
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


def load_failing_found(conn: psycopg.Connection[Any], environment: str) -> frozenset[int]:
    """The latest ``found`` attempt of each order whose body fails a Design 4 field check on its own."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        rows = cur.execute(
            """
            SELECT DISTINCT ON (order_id) lookup_id, body_text
            FROM tax_order_lookups
            WHERE environment = %(env)s AND outcome = 'found'
            ORDER BY order_id, lookup_id DESC
            """,
            {"env": environment},
        ).fetchall()
    return frozenset(r["lookup_id"] for r in rows if lookup_field_failures(parse_lookup_body(r["body_text"])))


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
        load_ledger_orders(conn, environment),
        load_lookup_attempts(conn, environment),
        now,
        failing_found=load_failing_found(conn, environment),
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


@dataclass
class EvidenceRunReport:
    state: Literal["published", "refused", "skipped"]
    run_id: int | None = None
    reason: str | None = None
    capture: CaptureReport | None = None
    counts: dict[str, int] = field(default_factory=dict)


class _Refused(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class _Snapshot:
    scope: Scope
    events: list[LedgerEvent]
    observations: list[RetainedObservation]
    lookups: dict[int, list[FoundLookup]]
    held: dict[int, HeldPosition]
    latest_fetch: dict[str, Any] | None
    ok_fetch_id: int | None
    watermarks: dict[str, int | None]


def _decimal_json(text: str) -> Any:
    """A stored JSONB value from its text, every number a ``Decimal`` (Design 1, "Precision")."""
    return json.loads(text, parse_float=Decimal)


def _read_snapshot(conn: psycopg.Connection[Any], environment: str) -> _Snapshot:
    """Design 6 step 5: everything the derivation reads, in one REPEATABLE READ snapshot."""
    with snapshot_read(conn), conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        scope_row = cur.execute("SELECT environment, account_id FROM trade_ledger_scope").fetchone()
        if scope_row is None:
            raise _Refused("scope_unestablished")
        if scope_row["environment"] != environment:
            raise _Refused("scope_mismatch")
        events = [
            LedgerEvent(
                event_id=r["event_id"],
                position_id=r["position_id"],
                etoro_instrument_id=r["etoro_instrument_id"],
                instrument_id=r["instrument_id"],
                event_kind=r["event_kind"],
                units=r["units"],
                executed_at=r["executed_at"],
                order_id=r["order_id"],
                source=r["source"],
                raw_payload=_decimal_json(r["payload"]),
            )
            for r in cur.execute(
                """
                SELECT event_id, position_id, etoro_instrument_id, instrument_id, event_kind, units, executed_at,
                       order_id, source, raw_payload::text AS payload
                FROM trade_events WHERE environment = %(env)s ORDER BY event_id
                """,
                {"env": environment},
            ).fetchall()
        ]
        # Observations carry their environment through the fetch that wrote them.
        observations = [
            RetainedObservation(
                observation_id=r["observation_id"],
                event_id=r["event_id"],
                position_id=r["position_id"],
                event_kind=r["event_kind"],
                executed_at=r["executed_at"],
                kind=r["kind"],
                source=r["source"],
                # Re-normalised under the current projection, as ingestion compares (slice 3d-3b).
                observed_normalised=normalise_payload(r["source"], r["event_kind"], _decimal_json(r["raw"])),
                observed_raw=_decimal_json(r["raw"]),
            )
            for r in cur.execute(
                """
                SELECT o.observation_id, o.event_id, o.position_id, o.event_kind, o.executed_at, o.kind, o.source,
                       o.observed_raw::text AS raw
                FROM trade_event_observations o
                JOIN trade_history_fetches f USING (fetch_id)
                WHERE f.environment = %(env)s
                ORDER BY o.observation_id
                """,
                {"env": environment},
            ).fetchall()
        ]
        lookups: dict[int, list[FoundLookup]] = {}
        for r in cur.execute(
            """
            SELECT lookup_id, order_id, requested_at, policy_id, body_text
            FROM tax_order_lookups
            WHERE environment = %(env)s AND outcome = 'found'
            ORDER BY lookup_id
            """,
            {"env": environment},
        ).fetchall():
            lookups.setdefault(r["order_id"], []).append(
                FoundLookup(r["lookup_id"], r["requested_at"], r["policy_id"], parse_lookup_body(r["body_text"]))
            )
        held = {
            r["position_id"]: HeldPosition(r["position_id"], r["units"], r["amount"], _decimal_json(r["payload"]))
            for r in cur.execute(
                "SELECT position_id, units, amount, raw_payload::text AS payload "
                "FROM broker_positions WHERE position_id >= 0"
            ).fetchall()
        }
        latest_fetch = cur.execute(
            "SELECT fetch_id, environment, account_id, outcome, portfolio_real_positions "
            "FROM trade_history_fetches ORDER BY fetch_id DESC LIMIT 1"
        ).fetchone()
        ok_fetch = cur.execute(
            "SELECT max(fetch_id) AS fetch_id FROM trade_history_fetches "
            "WHERE environment = %(env)s AND outcome = 'ok'",
            {"env": environment},
        ).fetchone()
        lookups_max = cur.execute(
            "SELECT max(lookup_id) AS m FROM tax_order_lookups WHERE environment = %(env)s", {"env": environment}
        ).fetchone()
    return _Snapshot(
        scope=Scope(scope_row["environment"], scope_row["account_id"]),
        events=events,
        observations=observations,
        lookups=lookups,
        held=held,
        latest_fetch=latest_fetch,
        ok_fetch_id=ok_fetch["fetch_id"] if ok_fetch else None,
        watermarks={
            "trade_events_max_event_id": max((e.event_id for e in events), default=None),
            "lookups_max_id": lookups_max["m"] if lookups_max else None,
            "observations_max_id": max((o.observation_id for o in observations), default=None),
        },
    )


def _check_portfolio_scope(snapshot: _Snapshot) -> None:
    """Design 6 step 6: the refresh ``broker_positions`` holds is the scope's, and so is every held position."""
    latest, scope = snapshot.latest_fetch, snapshot.scope
    if latest is None:
        raise _Refused("portfolio_unobserved")
    empty_unverified = latest["outcome"] == "scope_unverified" and latest["portfolio_real_positions"] == 0
    if latest["environment"] != scope.environment or (
        not empty_unverified and (latest["account_id"] != scope.account_id or latest["outcome"] not in ("ok", "failed"))
    ):
        raise _Refused("portfolio_out_of_scope")
    if any(payload_account_id(dict(h.payload)) != scope.account_id for h in snapshot.held.values()):
        raise _Refused("positions_out_of_scope")


def _held_json(held: dict[int, HeldPosition | None]) -> dict[str, Any]:
    return {
        str(position_id): None
        if holding is None
        else {
            "units": canonical_decimal(holding.units),
            "amount": canonical_decimal(holding.amount),
            "cid": payload_account_id(dict(holding.payload)),
        }
        for position_id, holding in sorted(held.items())
    }


def _publish(
    conn: psycopg.Connection[Any],
    run_id: int,
    snapshot: _Snapshot,
    subjects: list[SubjectEvidence],
    lots: list[EvidenceLot],
    counts: dict[str, int],
    fx_run_id: int | None,
    fx_policy_id: str | None,
) -> None:
    conn.execute(
        """
        UPDATE tax_evidence_runs SET
            state = 'published', finished_at = clock_timestamp(), account_id = %(account)s,
            trade_events_max_event_id = %(events_max)s, lookups_max_id = %(lookups_max)s,
            observations_max_id = %(observations_max)s, history_fetch_latest_id = %(fetch_latest)s,
            history_fetch_ok_id = %(fetch_ok)s, fx_run_id = %(fx_run)s, fx_policy_id = %(fx_policy)s,
            counts = %(counts)s
        WHERE run_id = %(run_id)s
        """,
        {
            "account": snapshot.scope.account_id,
            "events_max": snapshot.watermarks["trade_events_max_event_id"],
            "lookups_max": snapshot.watermarks["lookups_max_id"],
            "observations_max": snapshot.watermarks["observations_max_id"],
            "fetch_latest": snapshot.latest_fetch["fetch_id"] if snapshot.latest_fetch else None,
            "fetch_ok": snapshot.ok_fetch_id,
            "fx_run": fx_run_id,
            "fx_policy": fx_policy_id,
            "counts": Jsonb(counts),
            "run_id": run_id,
        },
    )
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO tax_evidence_subjects
                (run_id, subject, order_id, etoro_instrument_id, instrument_id, product, outcome, reasons,
                 trade_event_ids, lookup_id, held_positions, first_event_at, last_event_at, execution_evidence,
                 total_fees_unobserved_position_ids, total_fees_nonzero_position_ids)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    run_id,
                    s.subject,
                    s.order_id,
                    s.etoro_instrument_id,
                    s.instrument_id,
                    s.product,
                    s.outcome,
                    list(s.reasons),
                    list(s.trade_event_ids),
                    s.lookup_id,
                    Jsonb(_held_json(dict(s.held_positions))),
                    s.first_event_at,
                    s.last_event_at,
                    s.execution_evidence,
                    list(s.total_fees_unobserved_position_ids),
                    list(s.total_fees_nonzero_position_ids),
                )
                for s in subjects
            ],
        )
        cur.executemany(
            """
            INSERT INTO tax_evidence_lots
                (run_id, lot_key, subject, order_id, instrument_id, etoro_instrument_id, direction, position_ids,
                 trade_event_ids, executed_at, uk_date, tax_year, units, usd_amount, consideration_status,
                 execution_evidence, fx_rate_date, fx_status, fx_observation_date, fx_usd_per_gbp, fx_gbp_per_usd,
                 fx_policy_id, fx_run_id, amount_gbp, fingerprint)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    run_id,
                    lot.draft.lot_key,
                    lot.draft.subject,
                    lot.draft.order_id,
                    lot.draft.instrument_id,
                    lot.draft.etoro_instrument_id,
                    lot.draft.direction,
                    sorted(lot.draft.position_ids),
                    sorted(lot.draft.trade_event_ids),
                    lot.draft.executed_at,
                    lot.draft.uk_date,
                    lot.draft.tax_year,
                    lot.draft.units,
                    lot.draft.usd_amount,
                    lot.draft.consideration_status,
                    lot.draft.execution_evidence,
                    lot.fx_rate_date,
                    lot.fx_status,
                    lot.fx_observation_date,
                    lot.fx_usd_per_gbp,
                    lot.fx_gbp_per_usd,
                    lot.fx_policy_id,
                    lot.fx_run_id,
                    lot.amount_gbp,
                    lot.fingerprint,
                )
                for lot in lots
            ],
        )


def _finish(conn: psycopg.Connection[Any], run_id: int, state: str, reason: str) -> None:
    conn.execute(
        "UPDATE tax_evidence_runs SET state = %s, reason = %s, finished_at = clock_timestamp() "
        "WHERE run_id = %s AND state = 'in_progress'",
        (state, reason, run_id),
    )


def run_tax_broker_evidence(
    conn: psycopg.Connection[Any],
    broker: OrderLookupReader,
    environment: str,
) -> EvidenceRunReport:
    """One run (Design 6) on one autocommit connection.

    Another run holding the lock: ``skipped``, nothing written. A refusal (no scope or another environment before
    the broker is asked; a portfolio or position outside the scope; an FX date 3b cannot answer; the FX state
    changing before publication) rolls back and records the run ``refused`` with its reason. An exception records
    it ``abandoned`` and re-raises. The lock is released in every case, so the connection stays usable.
    """
    if not conn.autocommit:
        raise ValueError("run_tax_broker_evidence needs an autocommit connection: each lookup commits as it lands")
    row = conn.execute("SELECT pg_try_advisory_lock(%s)", [TAX_EVIDENCE_LOCK_KEY]).fetchone()
    if row is None or not row[0]:
        return EvidenceRunReport(state="skipped", reason="another run holds the lock")
    run_id: int | None = None
    report = EvidenceRunReport(state="refused")
    try:
        # Holding the lock, an in-progress row is a run whose process died.
        conn.execute(
            "UPDATE tax_evidence_runs SET state = 'abandoned', finished_at = clock_timestamp(), "
            "reason = 'superseded: in progress when a later run took the lock' WHERE state = 'in_progress'"
        )
        scope = conn.execute("SELECT environment, account_id FROM trade_ledger_scope").fetchone()
        inserted = conn.execute(
            """
            INSERT INTO tax_evidence_runs (environment, account_id, started_at, state, evidence_policy_id)
            VALUES (%s, %s, clock_timestamp(), 'in_progress', %s)
            RETURNING run_id
            """,
            (environment, scope[1] if scope is not None else None, TAX_EVIDENCE_POLICY_ID),
        ).fetchone()
        if inserted is None:
            raise RuntimeError("tax_evidence_runs insert returned no run_id")
        run_id = report.run_id = int(inserted[0])
        try:
            if scope is None:
                raise _Refused("scope_unestablished")
            if scope[0] != environment:
                raise _Refused("scope_mismatch")
            report.capture = capture_order_lookups(conn, broker, environment)
            conn.execute(
                "UPDATE tax_evidence_runs SET lookups_attempted = %s, lookups_deferred = %s, lookups_stopped_by = %s "
                "WHERE run_id = %s",
                (report.capture.attempted, report.capture.deferred, report.capture.stopped_by, run_id),
            )
            fx_state = load_tax_fx_state(conn)
            snapshot = _read_snapshot(conn, environment)
            _check_portfolio_scope(snapshot)
            subjects = derive_all(
                snapshot.scope, snapshot.events, snapshot.observations, snapshot.lookups, snapshot.held
            )
            drafts = [lot for subject in subjects for lot in subject.lots]
            priced = price_lots(drafts, fx_state)
            if isinstance(priced, FxRefusal):
                raise _Refused(f"{priced.reason}: {priced.lot_key}")
            counts = {
                outcome: sum(1 for s in subjects if s.outcome == outcome)
                for outcome in ("admitted", "excluded_non_share_product", "unresolved")
            }
            counts["acquisitions"] = sum(1 for lot in priced if lot.draft.direction == "acquisition")
            counts["disposals"] = sum(1 for lot in priced if lot.draft.direction == "disposal")
            with conn.transaction():
                if not tax_fx_state_is_current(conn, fx_state):
                    raise _Refused("fx_state_changed")
                _publish(
                    conn,
                    run_id,
                    snapshot,
                    subjects,
                    priced,
                    counts,
                    fx_state.latest_applied_run_id,
                    priced[0].fx_policy_id if priced else None,
                )
        except _Refused as refusal:
            _rollback(conn)
            _finish(conn, run_id, "refused", refusal.reason)
            report.reason = refusal.reason
            return report
        report.state = "published"
        report.counts = counts
        return report
    except BaseException as exc:
        _rollback(conn)
        if run_id is not None:
            _finish(conn, run_id, "abandoned", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        _rollback(conn)
        conn.execute("SELECT pg_advisory_unlock(%s)", [TAX_EVIDENCE_LOCK_KEY])


def _rollback(conn: psycopg.Connection[Any]) -> None:
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        conn.rollback()
