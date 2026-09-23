"""Attended, demo-only release of a window-B core authority (#2961).

Spec: ``docs/proposals/execution/2026-09-23-core-window-b-attended-release.md``.

Window B is a core ENTRY whose ``mark_core_submission_entered`` committed
(``submission_phase = 'broker_verb_entered'``) and whose process then died before the
broker response was persisted. No unattended path may resolve it: the marker bounds
only its own commit, and a ``referenceId`` lookup cannot prove non-acceptance (the
2026-09-17 probe, recorded on ``terminalise_unsubmitted_core_entry``). Until this act
runs, the row holds capital and ``core_trade_in_flight`` refuses every core
submission.

The supervisor decision (#2961, 2026-09-23 00:02Z) authorises an ATTENDED, DEMO-ONLY
release and accepts a bounded demo double order as the residual. This module reduces
that residual; it does not eliminate it. A PASS does NOT prove the broker never
received the order. It proves: the sender process is dead, ``WINDOW_B_VISIBILITY_WAIT``
has passed since both its marker and its death, and one account read taken after
that shows no position opened after the authority and no pending order on the
instrument.

⚠ Zero broker MUTATIONS on every path. The one broker call is the informational
``get_account_risk_snapshot``.

⚠ The real environment stays refused. Nothing here reads the kill switch or places
an order; the release only moves a stranded authority to ``rejected`` / ``failed``.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import sys
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, LiteralString, Protocol
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerProvider
from app.security.unattended_guard import is_linked_worktree
from app.services.strategy_core_submission_gate import CORE_SUBMISSION_ADVISORY_LOCK
from app.services.strategy_order_reconciliation import (
    _UNSUBMITTED_CORE_ENTRY_SQL,
    StrategyReconciliationBusy,
    _core_submission_try_lock,
    try_reconciliation_order_lock,
)

logger = logging.getLogger(__name__)

WINDOW_B_RELEASE_RULE_VERSION: Final = "core-window-b-v1"

#: ``T``: by CONSTRUCTION, not measurement. The repo holds one timed lag (order
#: ``382257232``, 3.77 s from authority commit to broker ``openDateTime``) and no
#: arm-L poll, so there is nothing to multiply a safety factor into. 600 s is two
#: orders of magnitude over that lag. The costs are asymmetric: too short risks a
#: double DEMO order, which the decision accepts as bounded; too long costs attended
#: waiting on a wedge that otherwise never recovers. Tightening it requires an arm-L
#: measurement and a bump of ``WINDOW_B_RELEASE_RULE_VERSION``.
WINDOW_B_VISIBILITY_WAIT: Final = timedelta(seconds=600)

#: Set by the loop supervisor on #2961 (2026-09-23): a threshold with no published
#: formulation is fixed by construction and frozen under the rule version, not
#: escalated. ``False`` makes every release refuse ``window_b_visibility_wait_unconfirmed``.
WINDOW_B_VISIBILITY_WAIT_CONFIRMED: Final = True

#: Allowance for skew between the DB clock (``orders.created_at``) and the broker
#: clock (``openDateTime``). It widens the "opened after the authority" window, so by
#: construction it makes the witness refuse MORE, never less.
WINDOW_B_CLOCK_SKEW_ALLOWANCE: Final = timedelta(seconds=60)

WINDOW_B_ATTESTATION_MAX_CHARS: Final = 2000
WINDOW_B_RELEASE_ERROR_CODE: Final = "core_authority_released_attended"
WINDOW_B_AUDIT_STAGE: Final = "core_window_b_release"

#: The window-A predicate with the phase bound to window B, plus the three
#: conditions window B adds (spec step 4). Both statuses are the ones the executor
#: writes at the authority INSERT and changes only once a broker response is
#: persisted, so they are tighter than "non-terminal": ``orders`` is inserted
#: ``'submitted'`` and ``strategy_trades`` ``'planned'`` (``strategy_core_executor``).
#: ⚠ The spec draft said the trade reads ``'submitted'``; the #2949 harness's real
#: crash showed ``'planned'``, because ``'submitted'`` is written only WITH
#: ``broker_order_ref``.
_WINDOW_B_CANDIDATE_SQL: Final[LiteralString] = (
    _UNSUBMITTED_CORE_ENTRY_SQL
    + """
  AND trade.status = 'planned'
  AND o.status = 'submitted'
  AND (SELECT count(*) FROM strategy_trade_orders one_trade WHERE one_trade.order_id = o.order_id) = 1
"""
)

#: Everything the act binds to. Read before the locks and re-read under them; any
#: difference refuses.
_WINDOW_B_DETAIL_SQL: Final[LiteralString] = """
SELECT trade.strategy_trade_id, trade.core_rebalance_intent_id, o.instrument_id,
       o.created_at, o.broker_environment,
       proof.operator_id, proof.api_key_credential_id, proof.user_key_credential_id,
       api_cred.environment, user_cred.environment,
       state.submission_entered_at, state.submission_entered_pid, state.submission_entered_host
FROM strategy_order_reconciliation_state state
JOIN orders o ON o.order_id = state.order_id
JOIN strategy_trade_orders link ON link.order_id = o.order_id
JOIN strategy_trades trade ON trade.strategy_trade_id = link.strategy_trade_id
JOIN strategy_core_eligibility_proofs proof
  ON proof.core_eligibility_proof_id = trade.core_eligibility_proof_id
LEFT JOIN broker_credentials api_cred ON api_cred.id = proof.api_key_credential_id
LEFT JOIN broker_credentials user_cred ON user_cred.id = proof.user_key_credential_id
WHERE state.order_id = %(order_id)s
"""


class WindowBClock(Protocol):
    def monotonic(self) -> float: ...

    def now(self) -> datetime: ...

    def sleep(self, seconds: float) -> None: ...


class SystemWindowBClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def now(self) -> datetime:
        return datetime.now(UTC)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


#: Builds the witness broker from the ORDER's own credential ids. It must raise
#: :class:`WindowBRefused` if they no longer resolve, so the witness can only ever
#: read the account that submitted the order.
BrokerFactory = Callable[[UUID, UUID, UUID], AbstractContextManager[BrokerProvider]]


class WindowBRefused(Exception):
    def __init__(self, slug: str, detail: str = "", evidence: Mapping[str, Any] | None = None) -> None:
        super().__init__(f"{slug}: {detail}" if detail else slug)
        self.slug = slug
        self.detail = detail
        self.evidence: dict[str, Any] = dict(evidence or {})


@dataclass(frozen=True)
class WindowBReleaseResult:
    order_id: int
    passed: bool
    slug: str
    decision_id: int | None
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WindowBWitness:
    refusals: tuple[str, ...]
    #: Every entry on the instrument AS READ, unfiltered, so the timestamp
    #: exemption and the mirror filter can be audited after the fact.
    positions_on_instrument: tuple[Any, ...]
    orders_for_open_on_instrument: tuple[Any, ...]
    orders_on_instrument: tuple[Any, ...]
    total_positions: int | None
    total_pending: int | None

    @property
    def refusal(self) -> str | None:
        return self.refusals[0] if self.refusals else None


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def validate_attestation(attestation: str) -> str:
    text = attestation.strip()
    if not text or len(text) > WINDOW_B_ATTESTATION_MAX_CHARS:
        raise WindowBRefused(
            "window_b_attestation_invalid",
            f"attestation must be 1-{WINDOW_B_ATTESTATION_MAX_CHARS} characters after strip",
        )
    return text


def attendance_refusal(*, linked_worktree: bool, stdin_is_tty: bool) -> str | None:
    """Accident controls, NOT attendance proof (spec step 2).

    Automation in the main checkout with a pseudo-TTY passes both. The hard line is
    the loop's prohibition; these catch a confused run.
    """
    if linked_worktree:
        return "window_b_linked_worktree"
    if not stdin_is_tty:
        return "window_b_no_tty"
    return None


def sender_death_refusal(
    *,
    pid: int | None,
    host: str | None,
    entered_at: datetime | None,
    this_host: str,
    this_pid: int,
    probe: Callable[[int, int], None] = os.kill,
) -> str | None:
    """``None`` only when the recorded sender is PROVABLY gone (spec Correction 3).

    Only ``ProcessLookupError`` (ESRCH) is death. Success, ``PermissionError`` and any
    other ``OSError`` refuse. PID reuse can only make a dead sender look alive, which
    is a refusal. The pid checks run BEFORE the probe: ``kill(0, 0)`` would signal our
    own process group and ``kill(-n, 0)`` another, so neither can be probed.
    """
    if pid is None or host is None or entered_at is None:
        return "window_b_sender_identity_unrecorded"
    if pid <= 0:
        return "window_b_sender_pid_invalid"
    if host != this_host:
        return "window_b_sender_other_host"
    if pid == this_pid:
        return "window_b_sender_is_this_process"
    try:
        probe(pid, 0)
    except ProcessLookupError:
        return None
    except OSError:
        return "window_b_sender_liveness_unknown"
    return "window_b_sender_alive"


def window_b_wait_remaining(
    *,
    dead_monotonic: float,
    now_monotonic: float,
    entered_at: datetime,
    now_wall: datetime,
    wait: timedelta = WINDOW_B_VISIBILITY_WAIT,
) -> float:
    """Seconds still to wait (spec step 8); ``0.0`` once both deadlines have passed.

    The monotonic leg is authoritative for "``T`` since the sender was seen dead" and
    cannot be moved by a wall-clock step. The wall leg adds "``T`` since the marker
    committed", which is the only anchor for a death we did not observe.
    """
    monotonic_left = dead_monotonic + wait.total_seconds() - now_monotonic
    wall_left = (entered_at + wait - now_wall).total_seconds()
    return max(0.0, monotonic_left, wall_left)


def _strict_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _entry_instrument_id(entry: dict[str, Any]) -> int | None:
    documented = entry.get("instrumentId")
    legacy = entry.get("instrumentID")
    if documented is not None and legacy is not None and documented != legacy:
        return None
    return _strict_int(documented if documented is not None else legacy)


def _aware_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def evaluate_window_b_witness(
    raw_payload: Any,
    *,
    instrument_id: int,
    authority_created_at: datetime,
    observed_at: datetime,
    not_before: datetime,
) -> WindowBWitness:
    """Judge one ``/pnl`` account read (spec step 9). Pure.

    Refuses on: a read taken before the wait deadline; a missing envelope or array;
    any entry whose instrument cannot be read, or which is on the instrument and
    fails field validation; a position on the instrument opened at or after
    ``authority_created_at - WINDOW_B_CLOCK_SKEW_ALLOWANCE``; a non-mirror
    ``ordersForOpen`` entry on the instrument; and ANY ``orders`` entry on it (the
    provider parser applies no mirror filter to ``orders``, and neither does this).

    ⚠ A position opened BEFORE the authority passes. That is sound only because an
    open cannot add to an existing position: the v2 order request's one position
    field, ``positionIds``, is documented "Reserved for the close action ...
    supplying any value is currently rejected" (``openapi_v1.375.0.json``), and the
    core body never sends it. Every open therefore creates a new position with its
    own ``openDateTime``.
    """
    refusals: list[str] = []
    if observed_at < not_before:
        refusals.append("window_b_witness_before_deadline")
    portfolio = raw_payload.get("clientPortfolio") if isinstance(raw_payload, dict) else None
    arrays: dict[str, list[Any]] = {}
    if isinstance(portfolio, dict):
        for key in ("positions", "ordersForOpen", "orders"):
            value = portfolio.get(key)
            if isinstance(value, list):
                arrays[key] = value
    if len(arrays) != 3:
        refusals.append("window_b_witness_incomplete")
        return WindowBWitness(tuple(refusals), (), (), (), None, None)

    on_instrument: dict[str, list[Any]] = {key: [] for key in arrays}
    for key, entries in arrays.items():
        for entry in entries:
            if not isinstance(entry, dict):
                refusals.append("window_b_witness_malformed")
                on_instrument[key].append(entry)
                continue
            entry_instrument = _entry_instrument_id(entry)
            if entry_instrument is None:
                refusals.append("window_b_witness_malformed")
                on_instrument[key].append(entry)
                continue
            if entry_instrument != instrument_id:
                continue
            on_instrument[key].append(entry)
            mirror_raw = entry.get("mirrorID", entry.get("mirrorId", 0))
            mirror_id = _strict_int(mirror_raw)
            if mirror_id is None:
                refusals.append("window_b_witness_malformed")
                continue
            if key == "positions":
                position_id = _strict_int(entry.get("positionID", entry.get("positionId")))
                opened = _aware_datetime(entry.get("openDateTime"))
                if position_id is None or opened is None:
                    refusals.append("window_b_witness_malformed")
                elif opened >= authority_created_at - WINDOW_B_CLOCK_SKEW_ALLOWANCE:
                    refusals.append("window_b_witness_position_after_authority")
            elif key == "ordersForOpen":
                if mirror_id == 0:
                    refusals.append("window_b_witness_pending_order")
            else:
                refusals.append("window_b_witness_pending_order")

    return WindowBWitness(
        refusals=tuple(dict.fromkeys(refusals)),
        positions_on_instrument=tuple(on_instrument["positions"]),
        orders_for_open_on_instrument=tuple(on_instrument["ordersForOpen"]),
        orders_on_instrument=tuple(on_instrument["orders"]),
        total_positions=len(arrays["positions"]),
        total_pending=len(arrays["ordersForOpen"]) + len(arrays["orders"]),
    )


# ---------------------------------------------------------------------------
# The act
# ---------------------------------------------------------------------------


def _default_attendance() -> str | None:
    return attendance_refusal(linked_worktree=is_linked_worktree(), stdin_is_tty=sys.stdin.isatty())


def _read_candidate(conn: psycopg.Connection[Any], order_id: int) -> tuple[Any, ...] | None:
    candidate = conn.execute(_WINDOW_B_CANDIDATE_SQL, {"order_id": order_id, "phase": "broker_verb_entered"}).fetchone()
    detail = None
    if candidate is not None:
        detail = conn.execute(_WINDOW_B_DETAIL_SQL, {"order_id": order_id}).fetchone()
    conn.commit()
    if candidate is None or detail is None or int(detail[0]) != int(candidate[0]):
        return None
    return tuple(detail)


def _holds_core_key(conn: psycopg.Connection[Any]) -> bool:
    row = conn.execute(
        """
        SELECT count(*) FROM pg_locks
        WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted
          AND classid::bigint = %s AND objid::bigint = %s AND objsubid = 2
        """,
        CORE_SUBMISSION_ADVISORY_LOCK,
    ).fetchone()
    conn.commit()
    return row is not None and int(row[0]) > 0


def _write_audit(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int | None,
    passed: bool,
    explanation: str,
    evidence: Mapping[str, Any],
) -> int:
    row = conn.execute(
        """
        INSERT INTO decision_audit (instrument_id, stage, model_version, pass_fail, explanation, evidence_json)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        RETURNING decision_id
        """,
        (
            instrument_id,
            WINDOW_B_AUDIT_STAGE,
            WINDOW_B_RELEASE_RULE_VERSION,
            "PASS" if passed else "FAIL",
            explanation,
            json.dumps(evidence, default=str, sort_keys=True),
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError("decision_audit INSERT did not return an id")
    return int(row[0])


def _execute_one(conn: psycopg.Connection[Any], sql: LiteralString, params: tuple[Any, ...], what: str) -> None:
    affected = conn.execute(sql, params).rowcount
    if affected != 1:
        raise WindowBRefused("window_b_terminal_write_mismatch", f"{what} affected {affected} rows")


def release_window_b_core_entry(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    operator_id: str,
    attestation: str,
    broker_factory: BrokerFactory,
    environment: str,
    clock: WindowBClock | None = None,
    attendance: Callable[[], str | None] = _default_attendance,
    this_host: str | None = None,
    this_pid: int | None = None,
    liveness_probe: Callable[[int, int], None] = os.kill,
) -> WindowBReleaseResult:
    """Release one stranded window-B core authority, or refuse by name (spec steps 1-11).

    Refusals before candidacy (input, accident controls, environment, a busy
    connection) are returned without an audit row. Every refusal from candidacy
    onward leaves a committed ``decision_audit`` FAIL row, written after the locks
    are released and in its own transaction.
    """
    clock = clock or SystemWindowBClock()
    evidence: dict[str, Any] = {"order_id": order_id, "operator_id": operator_id}

    def refused(slug: str, detail: str = "") -> WindowBReleaseResult:
        return WindowBReleaseResult(order_id, False, slug, None, {**evidence, "detail": detail})

    # Steps 1-3: nothing below this block touches the database or the broker.
    try:
        evidence["attestation"] = validate_attestation(attestation)
    except WindowBRefused as exc:
        return refused(exc.slug, exc.detail)
    accident = attendance()
    if accident is not None:
        return refused(accident)
    if environment != "demo":
        return refused("window_b_environment_not_demo", f"settings environment is {environment!r}")
    if not WINDOW_B_VISIBILITY_WAIT_CONFIRMED:
        return refused("window_b_visibility_wait_unconfirmed")
    if conn.info.transaction_status != TransactionStatus.IDLE:
        # Never commit on the caller's behalf.
        return refused("window_b_connection_not_idle")

    instrument_id: int | None = None
    try:
        # Step 4: candidacy, read BEFORE any lock.
        detail = _read_candidate(conn, order_id)
        if detail is None:
            raise WindowBRefused("window_b_not_a_candidate")
        instrument_id = int(detail[2])
        _check_detail(detail, evidence)
        # Step 5: locks. The core key is reentrant, so a caller already holding it
        # would pass its own try; refuse that before trying.
        if _holds_core_key(conn):
            raise WindowBRefused("window_b_caller_holds_core_key")
        with _core_submission_try_lock(conn) as core_idle:
            if not core_idle:
                raise WindowBRefused("window_b_core_lock_busy")
            # Per-order lock is ALWAYS last (the reconciliation module's lock order).
            # `_release_locked` raises no `StrategyReconciliationBusy`, so this arm
            # can only be the try-acquire.
            try:
                with try_reconciliation_order_lock(conn, order_id):
                    _release_locked(
                        conn,
                        order_id=order_id,
                        detail=detail,
                        evidence=evidence,
                        broker_factory=broker_factory,
                        clock=clock,
                        this_host=this_host if this_host is not None else socket.gethostname(),
                        this_pid=this_pid if this_pid is not None else os.getpid(),
                        liveness_probe=liveness_probe,
                    )
            except StrategyReconciliationBusy as exc:
                raise WindowBRefused("window_b_order_lock_busy") from exc
    except WindowBRefused as exc:
        evidence.update(exc.evidence)
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        with conn.transaction():
            decision_id = _write_audit(
                conn,
                instrument_id=instrument_id,
                passed=False,
                explanation=f"{exc.slug}: {exc.detail}" if exc.detail else exc.slug,
                evidence={**evidence, "refusal": exc.slug},
            )
        return WindowBReleaseResult(order_id, False, exc.slug, decision_id, evidence)

    decision_id = int(evidence.pop("_decision_id"))
    return WindowBReleaseResult(order_id, True, WINDOW_B_RELEASE_ERROR_CODE, decision_id, evidence)


def _check_detail(detail: tuple[Any, ...], evidence: dict[str, Any]) -> None:
    (
        trade_id,
        intent_id,
        instrument_id,
        created_at,
        order_environment,
        proof_operator_id,
        api_credential_id,
        user_credential_id,
        api_environment,
        user_environment,
        entered_at,
        entered_pid,
        entered_host,
    ) = detail
    evidence.update(
        {
            "strategy_trade_id": trade_id,
            "core_rebalance_intent_id": intent_id,
            "instrument_id": instrument_id,
            "authority_created_at": created_at,
            "sender": {"pid": entered_pid, "host": entered_host, "entered_at": entered_at},
        }
    )
    # Step 3's per-order half. A core order never records `broker_environment` (only
    # `order_client` writes it), so NULL is its normal shape; `'real'` refuses. The
    # credential rows are what bind the account, and both must be demo and resolvable.
    if order_environment not in (None, "demo"):
        raise WindowBRefused("window_b_environment_not_demo", f"order environment is {order_environment!r}")
    if api_environment is None or user_environment is None:
        raise WindowBRefused("window_b_credentials_unresolved")
    if (api_environment, user_environment) != ("demo", "demo"):
        raise WindowBRefused("window_b_environment_not_demo", "credential environment is not demo")
    if proof_operator_id is None or api_credential_id is None or user_credential_id is None:
        raise WindowBRefused("window_b_credentials_unresolved")


def _release_locked(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    detail: tuple[Any, ...],
    evidence: dict[str, Any],
    broker_factory: BrokerFactory,
    clock: WindowBClock,
    this_host: str,
    this_pid: int,
    liveness_probe: Callable[[int, int], None],
) -> None:
    # Step 6: the row must not have moved between the unlocked read and now.
    if _read_candidate(conn, order_id) != detail:
        raise WindowBRefused("window_b_candidate_changed")
    trade_id = int(detail[0])
    instrument_id = int(detail[2])
    created_at: datetime = detail[3]
    entered_at: datetime | None = detail[10]

    # Step 7: the sender is dead.
    sender_refusal = sender_death_refusal(
        pid=detail[11],
        host=detail[12],
        entered_at=entered_at,
        this_host=this_host,
        this_pid=this_pid,
        probe=liveness_probe,
    )
    if sender_refusal is not None:
        raise WindowBRefused(sender_refusal)
    assert entered_at is not None  # sender_death_refusal refuses NULL
    dead_monotonic = clock.monotonic()
    dead_wall = clock.now()
    evidence["sender"]["dead_observed_at"] = dead_wall

    # Step 8: wait, both locks held, so no core submission can start meanwhile.
    while True:
        remaining = window_b_wait_remaining(
            dead_monotonic=dead_monotonic,
            now_monotonic=clock.monotonic(),
            entered_at=entered_at,
            now_wall=clock.now(),
        )
        if remaining <= 0:
            break
        clock.sleep(remaining)
    not_before = max(entered_at, dead_wall) + WINDOW_B_VISIBILITY_WAIT
    evidence["wait"] = {"seconds": WINDOW_B_VISIBILITY_WAIT.total_seconds(), "witness_not_before": not_before}

    # Step 9: one informational account read, from the order's own account.
    try:
        with broker_factory(detail[5], detail[6], detail[7]) as broker:
            snapshot = broker.get_account_risk_snapshot()
    except WindowBRefused:
        raise
    except Exception as exc:
        raise WindowBRefused("window_b_witness_unavailable", type(exc).__name__) from exc
    witness = evaluate_window_b_witness(
        snapshot.raw_payload,
        instrument_id=instrument_id,
        authority_created_at=created_at,
        observed_at=snapshot.observed_at,
        not_before=not_before,
    )
    evidence["witness"] = {
        "observed_at": snapshot.observed_at,
        "positions_on_instrument": list(witness.positions_on_instrument),
        "orders_for_open_on_instrument": list(witness.orders_for_open_on_instrument),
        "orders_on_instrument": list(witness.orders_on_instrument),
        "total_positions": witness.total_positions,
        "total_pending": witness.total_pending,
        "refusals": list(witness.refusals),
    }
    if witness.refusal is not None:
        raise WindowBRefused(witness.refusal)

    # Step 10: one transaction, both locks held, every statement conditional on the
    # candidate shape and asserted to touch exactly one row. Statement order follows
    # the module lock ordering: orders -> reconciliation state -> strategy_trades.
    with conn.transaction():
        _execute_one(
            conn,
            "UPDATE orders SET status='rejected' WHERE order_id=%s AND status='submitted' AND broker_order_ref IS NULL",
            (order_id,),
            "orders",
        )
        _execute_one(
            conn,
            """
            UPDATE strategy_order_reconciliation_state
            SET state='rejected', reconciled_at=now(), last_attempt_at=now(),
                attempt_count=attempt_count+1, last_error_code=%s, updated_at=now()
            WHERE order_id=%s AND submission_phase='broker_verb_entered'
              AND state NOT IN ('resolved', 'rejected')
            """,
            (WINDOW_B_RELEASE_ERROR_CODE, order_id),
            "strategy_order_reconciliation_state",
        )
        _execute_one(
            conn,
            "UPDATE strategy_trades SET status='failed', updated_at=now() "
            "WHERE strategy_trade_id=%s AND status='planned'",
            (trade_id,),
            "strategy_trades",
        )
        evidence["_decision_id"] = _write_audit(
            conn,
            instrument_id=instrument_id,
            passed=True,
            explanation=WINDOW_B_RELEASE_ERROR_CODE,
            evidence=evidence,
        )
