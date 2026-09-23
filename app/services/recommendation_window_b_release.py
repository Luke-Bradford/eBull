"""Attended, demo-only release of a stranded recommendation claim (#2942).

Spec: ``docs/proposals/execution/2026-09-23-recommendation-window-b-attended-release.md``,
a delta against #2961's core act (``strategy_core_window_b_release``), whose helpers
this module reuses.

Two stranded shapes of the recommendation claim (``idx_orders_recommendation_open_attempt``)
have no unattended path, and this act is for them:

- **W** (window B): ``status='submitted'``, phase ``broker_verb_entered``, no broker
  ref, recommendation ``approved``. The sender died after the marker; proved by ESRCH
  on the recorded pid, as for core.
- **U** (uncertain): ``status='uncertain'``, same phase, no ref, recommendation
  ``execution_pending``. The provider call returned by raising and the park
  committed. The sender is normally the long-lived jobs daemon and still alive, so
  sender death is NOT consulted: the recorded park commit is the sends-ended proof.

A PASS does NOT prove the broker never received the order. It proves: the sends had
ended, ``WINDOW_B_VISIBILITY_WAIT`` passed after both the recorded instant and the
act's own observation of it, the operator inspected the stored payload (by digest),
and one account read of the ORDER'S OWN account showed nothing the attempt could
have produced. The residuals are named in the spec.

⚠ Zero broker MUTATIONS on every path. The one broker call is the informational
``get_account_risk_snapshot``. ⚠ The real environment stays refused.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final, LiteralString
from uuid import UUID

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerProvider
from app.services.order_client import (
    RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS,
    _recommendation_submission_try_lock,
)
from app.services.strategy_core_window_b_release import (
    WINDOW_B_RELEASE_RULE_VERSION,
    WINDOW_B_VISIBILITY_WAIT,
    WINDOW_B_VISIBILITY_WAIT_CONFIRMED,
    SystemWindowBClock,
    WindowBClock,
    WindowBRefused,
    WindowBReleaseResult,
    _default_attendance,
    _entry_instrument_id,
    entry_mirror_id,
    entry_position_id,
    evaluate_window_b_witness,
    observed_at_refusal,
    sender_death_refusal,
    validate_attestation,
    window_b_wait_remaining,
    witness_arrays,
)

logger = logging.getLogger(__name__)

#: DERIVED, so a core rule change that bumps its version changes this one too.
RECOMMENDATION_WINDOW_B_RULE_VERSION: Final = f"recommendation-window-b-v1+{WINDOW_B_RELEASE_RULE_VERSION}"
RECOMMENDATION_RELEASE_AUDIT_STAGE: Final = "recommendation_window_b_release"
RECOMMENDATION_RELEASED: Final = "recommendation_released_attended"
ALREADY_RELEASED: Final = "recommendation_release_already_released"
OUTCOME_INDETERMINATE: Final = "recommendation_release_outcome_indeterminate"
INTERNAL_ERROR: Final = "recommendation_release_internal_error"
CREDENTIALS_UNRESOLVED: Final = "recommendation_release_credentials_unresolved"

#: Builds the witness broker from the ORDER's two recorded credential ids. It must
#: raise :class:`WindowBRefused` (``CREDENTIALS_UNRESOLVED``) unless they are still
#: the owning operator's live demo pair.
RecommendationBrokerFactory = Callable[[UUID, UUID], AbstractContextManager[BrokerProvider]]

#: W or U, bound to the recommendation's status (spec, "The two stranded states").
#: Everything the act relies on is read here, before the lock and again under it.
_CANDIDATE_SQL: Final[LiteralString] = """
SELECT o.status, o.recommendation_id, o.recommendation_request_id, o.created_at,
       o.instrument_id, o.action, o.broker_environment, o.raw_payload_json,
       o.recommendation_park_message, o.recommendation_api_key_credential_id,
       o.recommendation_user_key_credential_id, o.recommendation_submission_entered_at,
       o.recommendation_submission_entered_pid, o.recommendation_submission_entered_host,
       o.recommendation_parked_at, o.recommendation_exit_position_id,
       o.recommendation_exit_units, api_cred.created_at, user_cred.created_at
FROM orders o
JOIN trade_recommendations tr ON tr.recommendation_id = o.recommendation_id
LEFT JOIN broker_credentials api_cred ON api_cred.id = o.recommendation_api_key_credential_id
LEFT JOIN broker_credentials user_cred ON user_cred.id = o.recommendation_user_key_credential_id
WHERE o.order_id = %(order_id)s
  AND o.broker_order_ref IS NULL
  AND o.recommendation_submission_phase = 'broker_verb_entered'
  AND o.recommendation_request_id IS NOT NULL
  AND o.action IN ('BUY', 'ADD', 'EXIT')
  AND tr.action = o.action
  AND tr.instrument_id = o.instrument_id
  AND ((o.status = 'submitted' AND tr.status = 'approved')
       OR (o.status = 'uncertain' AND tr.status = 'execution_pending'))
  AND o.recommendation_api_key_credential_id IS NOT NULL
  AND o.recommendation_user_key_credential_id IS NOT NULL
"""

#: The terminal compare-and-set over EVERY field the act relied on (spec step 10).
_TERMINAL_ORDER_SQL: Final[LiteralString] = """
UPDATE orders SET status = 'rejected'
WHERE order_id = %(order_id)s
  AND status = %(status)s
  AND broker_order_ref IS NULL
  AND recommendation_submission_phase = 'broker_verb_entered'
  AND recommendation_id IS NOT DISTINCT FROM %(recommendation_id)s::bigint
  AND created_at IS NOT DISTINCT FROM %(created_at)s::timestamptz
  AND instrument_id IS NOT DISTINCT FROM %(instrument_id)s::bigint
  AND action IS NOT DISTINCT FROM %(action)s::text
  AND broker_environment IS NOT DISTINCT FROM %(broker_environment)s::text
  AND raw_payload_json IS NOT DISTINCT FROM %(raw_payload)s::jsonb
  AND recommendation_park_message IS NOT DISTINCT FROM %(park_message)s::text
  AND recommendation_api_key_credential_id IS NOT DISTINCT FROM %(api_credential_id)s::uuid
  AND recommendation_user_key_credential_id IS NOT DISTINCT FROM %(user_credential_id)s::uuid
  AND recommendation_submission_entered_at IS NOT DISTINCT FROM %(entered_at)s::timestamptz
  AND recommendation_submission_entered_pid IS NOT DISTINCT FROM %(entered_pid)s::integer
  AND recommendation_submission_entered_host IS NOT DISTINCT FROM %(entered_host)s::text
  AND recommendation_parked_at IS NOT DISTINCT FROM %(parked_at)s::timestamptz
  AND recommendation_exit_position_id IS NOT DISTINCT FROM %(exit_position_id)s::bigint
  AND recommendation_exit_units IS NOT DISTINCT FROM %(exit_units)s::numeric
"""


@dataclass(frozen=True, eq=True)
class _Candidate:
    status: str
    recommendation_id: int
    recommendation_request_id: UUID
    created_at: datetime
    instrument_id: int
    action: str
    broker_environment: str | None
    raw_payload: Any
    park_message: str | None
    api_credential_id: UUID
    user_credential_id: UUID
    entered_at: datetime | None
    entered_pid: int | None
    entered_host: str | None
    parked_at: datetime | None
    exit_position_id: int | None
    exit_units: Decimal | None
    api_credential_created_at: datetime | None
    user_credential_created_at: datetime | None

    @property
    def state(self) -> str:
        return "W" if self.status == "submitted" else "U"

    @property
    def recommendation_status(self) -> str:
        return "approved" if self.status == "submitted" else "execution_pending"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

#: Spec Delta 1 "Payload tightening": refused if any view contains one of these.
_REFERENCE_TOKENS: Final = ("orderid", "positionid", "reprtruncated")
_UNICODE_ESCAPE: Final = re.compile(r"\\u([0-9a-fA-F]{4})")
_NON_ALNUM: Final = re.compile(r"[^a-z0-9]")
_MAX_PARSE_DEPTH: Final = 64


def payload_digest(payload: Any, park_message: str | None) -> str:
    """What ``--show`` prints and the release requires back (``--payload-sha256``)."""
    text = json.dumps([payload, park_message], sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def _deep_parse(value: Any, depth: int = 0) -> Any:
    """Replace each string that parses as JSON by its parsed form, recursively."""
    if depth > _MAX_PARSE_DEPTH:
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return value
        if isinstance(parsed, str) and parsed == value:
            return value
        return _deep_parse(parsed, depth + 1)
    if isinstance(value, dict):
        return {key: _deep_parse(item, depth + 1) for key, item in value.items()}
    if isinstance(value, list):
        return [_deep_parse(item, depth + 1) for item in value]
    return value


def payload_reference_refusal(payload: Any, park_message: str | None) -> str | None:
    """``recommendation_release_payload_has_ref`` if a broker reference may be stored.

    ⚠ An ACCIDENT control, not a decoder (spec Delta 1). Three views of each input
    (the raw serialised text, the same with every ``\\uXXXX`` decoded, and the text
    of the recursively parsed form), lower-cased and reduced to ``[a-z0-9]``. A
    reference encoded in a form no view normalises (base64) passes, which is why the
    operator must see the payload first (``--show`` / ``--payload-sha256``).
    """
    for item in (payload, park_message):
        raw = json.dumps(item, ensure_ascii=False, default=str)
        views = (raw, _UNICODE_ESCAPE.sub(lambda m: chr(int(m.group(1), 16)), raw))
        parsed = json.dumps(_deep_parse(item), ensure_ascii=False, default=str)
        for view in (*views, parsed):
            normalised = _NON_ALNUM.sub("", view.lower())
            if any(token in normalised for token in _REFERENCE_TOKENS):
                return "recommendation_release_payload_has_ref"
    return None


def _significant_decimal_places(value: Decimal) -> int:
    exponent = value.normalize().as_tuple().exponent
    return -exponent if isinstance(exponent, int) and exponent < 0 else 0


def _witness_units(raw: Any) -> Decimal | None:
    """Positive, finite, ≤8 significant dp; ``None`` means malformed."""
    if raw is None or isinstance(raw, bool) or not isinstance(raw, int | float | str):
        return None
    try:
        units = Decimal(str(raw))
    except InvalidOperation:
        return None
    if not units.is_finite() or units <= 0 or _significant_decimal_places(units) > 8:
        return None
    return units


@dataclass(frozen=True)
class RecommendationExitWitness:
    refusals: tuple[str, ...]
    #: Every ``positions`` entry carrying the target id, whatever its instrument.
    target_entries: tuple[Any, ...]
    orders_for_open_on_instrument: tuple[Any, ...]
    orders_on_instrument: tuple[Any, ...]
    total_positions: int | None
    total_pending: int | None

    @property
    def refusal(self) -> str | None:
        return self.refusals[0] if self.refusals else None


def evaluate_recommendation_exit_witness(
    raw_payload: Any,
    *,
    instrument_id: int,
    position_id: int,
    recorded_units: Decimal,
    observed_at: Any,
    not_before: datetime,
) -> RecommendationExitWitness:
    """Judge one ``/pnl`` read for an EXIT (spec Delta 2). Pure.

    A close targets ONE broker ``positionID`` and this path always closes it WHOLE,
    so the load-bearing question is whether that lot is still there: an absent lot
    refuses because this attempt's close may have landed. Units equality is
    secondary — it catches something ELSE touching the lot — and a pending order
    on the instrument refuses as it does for an open.
    """
    refusals: list[str] = []
    observed_refusal = observed_at_refusal(observed_at)
    if observed_refusal is not None:
        refusals.append(observed_refusal)
    elif observed_at < not_before:
        refusals.append("window_b_witness_before_deadline")
    arrays = witness_arrays(raw_payload)
    if arrays is None:
        refusals.append("window_b_witness_incomplete")
        return RecommendationExitWitness(tuple(refusals), (), (), (), None, None)

    targets: list[Any] = []
    for entry in arrays["positions"]:
        # The target search looks across EVERY instrument, so an entry whose id or
        # instrument cannot be read could be hiding the target.
        if not isinstance(entry, dict) or entry_position_id(entry) is None or _entry_instrument_id(entry) is None:
            refusals.append("window_b_witness_malformed")
            continue
        if entry_position_id(entry) == position_id:
            targets.append(entry)

    pending: dict[str, list[Any]] = {"ordersForOpen": [], "orders": []}
    for key, on_instrument in pending.items():
        for entry in arrays[key]:
            entry_instrument = _entry_instrument_id(entry) if isinstance(entry, dict) else None
            if entry_instrument is None:
                refusals.append("window_b_witness_malformed")
                on_instrument.append(entry)
                continue
            if entry_instrument != instrument_id:
                continue
            on_instrument.append(entry)
            mirror_id = entry_mirror_id(entry)
            if mirror_id is None:
                refusals.append("window_b_witness_malformed")
            elif key == "orders" or mirror_id == 0:
                refusals.append("window_b_witness_pending_order")

    if len(targets) > 1:
        refusals.append("window_b_witness_malformed")
    elif not targets:
        refusals.append("recommendation_release_exit_lot_gone")
    else:
        target = targets[0]
        mirror_id = entry_mirror_id(target)
        units = _witness_units(target.get("units"))
        if _entry_instrument_id(target) != instrument_id or mirror_id is None or units is None:
            refusals.append("window_b_witness_malformed")
        elif target.get("isBuy") is not True or mirror_id != 0:
            refusals.append("recommendation_release_exit_lot_identity_changed")
        elif units != recorded_units:
            refusals.append("recommendation_release_exit_lot_changed")

    return RecommendationExitWitness(
        refusals=tuple(dict.fromkeys(refusals)),
        target_entries=tuple(targets),
        orders_for_open_on_instrument=tuple(pending["ordersForOpen"]),
        orders_on_instrument=tuple(pending["orders"]),
        total_positions=len(arrays["positions"]),
        total_pending=len(arrays["ordersForOpen"]) + len(arrays["orders"]),
    )


# ---------------------------------------------------------------------------
# The act
# ---------------------------------------------------------------------------


def _read_candidate(conn: psycopg.Connection[Any], order_id: int) -> _Candidate | None:
    row = conn.execute(_CANDIDATE_SQL, {"order_id": order_id}).fetchone()
    conn.commit()
    return None if row is None else _Candidate(*row)


def describe_candidate(conn: psycopg.Connection[Any], order_id: int) -> dict[str, Any] | None:
    """What ``--show`` prints. Takes no lock and writes nothing (the read is committed)."""
    row = conn.execute(
        """
        SELECT o.status, o.recommendation_id, o.instrument_id, o.action, o.broker_environment,
               o.recommendation_submission_phase, o.broker_order_ref, o.created_at,
               o.raw_payload_json, o.recommendation_park_message, o.recommendation_parked_at,
               o.recommendation_submission_entered_at, o.recommendation_submission_entered_pid,
               o.recommendation_submission_entered_host, o.recommendation_exit_position_id,
               o.recommendation_exit_units, tr.status
        FROM orders o LEFT JOIN trade_recommendations tr ON tr.recommendation_id = o.recommendation_id
        WHERE o.order_id = %s
        """,
        (order_id,),
    ).fetchone()
    candidate = _read_candidate(conn, order_id)
    if row is None:
        return None
    names = (
        "status recommendation_id instrument_id action broker_environment submission_phase broker_order_ref "
        "created_at raw_payload_json park_message parked_at entered_at entered_pid entered_host "
        "exit_position_id exit_units recommendation_status"
    ).split()
    described: dict[str, Any] = {str(name): value for name, value in zip(names, row, strict=True)}
    described["order_id"] = order_id
    described["candidate_state"] = None if candidate is None else candidate.state
    described["payload_sha256"] = payload_digest(row[8], row[9])
    return described


def _prior_pass(conn: psycopg.Connection[Any], order_id: int) -> int | None:
    row = conn.execute(
        """
        SELECT decision_id FROM decision_audit
        WHERE stage = %s AND pass_fail = 'PASS' AND evidence_json->>'order_id' = %s
        ORDER BY decision_id DESC LIMIT 1
        """,
        (RECOMMENDATION_RELEASE_AUDIT_STAGE, str(order_id)),
    ).fetchone()
    conn.commit()
    return None if row is None else int(row[0])


def _holds_recommendation_key(conn: psycopg.Connection[Any], recommendation_id: int) -> bool:
    row = conn.execute(
        """
        SELECT count(*) FROM pg_locks
        WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted
          AND classid::bigint = %s AND objid::bigint = %s AND objsubid = 2
        """,
        (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, recommendation_id),
    ).fetchone()
    conn.commit()
    return row is not None and int(row[0]) > 0


def _write_audit(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int | None,
    recommendation_id: int | None,
    passed: bool,
    explanation: str,
    evidence: Mapping[str, Any],
) -> int:
    row = conn.execute(
        """
        INSERT INTO decision_audit
            (instrument_id, recommendation_id, stage, model_version, pass_fail, explanation, evidence_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        RETURNING decision_id
        """,
        (
            instrument_id,
            recommendation_id,
            RECOMMENDATION_RELEASE_AUDIT_STAGE,
            RECOMMENDATION_WINDOW_B_RULE_VERSION,
            "PASS" if passed else "FAIL",
            explanation,
            json.dumps(evidence, default=str, sort_keys=True),
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError("decision_audit INSERT did not return an id")
    return int(row[0])


@dataclass
class _Outcome:
    """Captured BEFORE any cleanup runs, so a cleanup failure cannot overwrite it."""

    commit_attempted: bool = False
    decision_id: int | None = None


def release_recommendation_window_b(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    operator_id: str,
    attestation: str,
    payload_sha256: str,
    broker_factory: RecommendationBrokerFactory,
    environment: str,
    clock: WindowBClock | None = None,
    attendance: Callable[[], str | None] = _default_attendance,
    this_host: str | None = None,
    this_pid: int | None = None,
    liveness_probe: Callable[[int, int], None] = os.kill,
) -> WindowBReleaseResult:
    """Release one stranded W/U recommendation claim, or refuse by name (spec steps 1-11).

    Refusals before candidacy are returned without an audit row. From candidacy
    onward every refusal — and every unexpected exception, as ``INTERNAL_ERROR`` —
    leaves a best-effort committed FAIL row, written after the key is released. An
    exception from the terminal COMMIT itself is ``OUTCOME_INDETERMINATE`` (re-run
    to learn it). ``operator_id`` is the ATTESTOR; the account is selected by the
    order's recorded credential ids.
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
        return refused("recommendation_release_environment_not_demo", f"settings environment is {environment!r}")
    if not WINDOW_B_VISIBILITY_WAIT_CONFIRMED:
        return refused("window_b_visibility_wait_unconfirmed")
    expected_digest = payload_sha256.strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", expected_digest) is None:
        return refused("recommendation_release_payload_not_inspected", "--payload-sha256 is not a SHA-256 hex digest")
    if conn.info.transaction_status != TransactionStatus.IDLE:
        return refused("recommendation_release_connection_not_idle")

    instrument_id: int | None = None
    recommendation_id: int | None = None
    outcome = _Outcome()
    try:
        # Step 4: candidacy, read BEFORE any lock.
        candidate = _read_candidate(conn, order_id)
        if candidate is None:
            prior = _prior_pass(conn, order_id)
            if prior is not None:
                return WindowBReleaseResult(order_id, True, ALREADY_RELEASED, prior, evidence)
            raise WindowBRefused("recommendation_release_not_a_candidate")
        instrument_id = candidate.instrument_id
        recommendation_id = candidate.recommendation_id
        evidence.update(
            {
                "state": candidate.state,
                "action": candidate.action,
                "recommendation_id": candidate.recommendation_id,
                "recommendation_request_id": candidate.recommendation_request_id,
                "instrument_id": candidate.instrument_id,
                "authority_created_at": candidate.created_at,
                "credentials": {
                    "api_key": {"id": candidate.api_credential_id, "created_at": candidate.api_credential_created_at},
                    "user_key": {
                        "id": candidate.user_credential_id,
                        "created_at": candidate.user_credential_created_at,
                    },
                },
            }
        )
        if candidate.broker_environment != "demo":
            raise WindowBRefused(
                "recommendation_release_environment_not_demo",
                f"order environment is {candidate.broker_environment!r}",
            )
        # Step 5: the key is reentrant, so a caller already holding it would pass
        # its own try; refuse that first.
        if _holds_recommendation_key(conn, candidate.recommendation_id):
            raise WindowBRefused("recommendation_release_caller_holds_key")
        with _recommendation_submission_try_lock(conn, candidate.recommendation_id) as idle:
            if not idle:
                raise WindowBRefused("recommendation_release_lock_busy")
            _release_locked(
                conn,
                order_id=order_id,
                candidate=candidate,
                expected_digest=expected_digest,
                evidence=evidence,
                broker_factory=broker_factory,
                clock=clock,
                this_host=this_host if this_host is not None else socket.gethostname(),
                this_pid=this_pid if this_pid is not None else os.getpid(),
                liveness_probe=liveness_probe,
                outcome=outcome,
            )
    except Exception as exc:
        if outcome.decision_id is not None:
            # The terminal COMMIT returned; only the key's cleanup failed, and the
            # helper closed the connection so the key cannot survive.
            logger.exception("recommendation window-B release committed; releasing the key failed afterwards")
            return WindowBReleaseResult(order_id, True, RECOMMENDATION_RELEASED, outcome.decision_id, evidence)
        if outcome.commit_attempted:
            logger.exception("recommendation window-B release: the terminal COMMIT raised; outcome unknown")
            return WindowBReleaseResult(
                order_id, False, OUTCOME_INDETERMINATE, None, {**evidence, "detail": type(exc).__name__}
            )
        if isinstance(exc, WindowBRefused):
            slug, detail = exc.slug, exc.detail
            evidence.update(exc.evidence)
        else:
            logger.exception("recommendation window-B release raised unexpectedly")
            slug, detail = INTERNAL_ERROR, f"{type(exc).__name__}: {exc}"
        return WindowBReleaseResult(
            order_id,
            False,
            slug,
            _best_effort_fail_audit(
                conn,
                instrument_id=instrument_id,
                recommendation_id=recommendation_id,
                slug=slug,
                detail=detail,
                evidence=evidence,
            ),
            {**evidence, "detail": detail},
        )

    assert outcome.decision_id is not None
    return WindowBReleaseResult(order_id, True, RECOMMENDATION_RELEASED, outcome.decision_id, evidence)


def _best_effort_fail_audit(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int | None,
    recommendation_id: int | None,
    slug: str,
    detail: str,
    evidence: dict[str, Any],
) -> int | None:
    """The FAIL row, or ``None`` when the connection cannot write it (spec step 11).

    The caller always prints the slug and evidence; the operator's terminal is the
    record of last resort.
    """
    try:
        if conn.closed:
            raise RuntimeError("connection closed")
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
        with conn.transaction():
            return _write_audit(
                conn,
                instrument_id=instrument_id,
                recommendation_id=recommendation_id,
                passed=False,
                explanation=f"{slug}: {detail}" if detail else slug,
                evidence={**evidence, "refusal": slug},
            )
    except Exception:
        logger.exception("recommendation window-B release: the FAIL audit row could not be written")
        return None


def _release_locked(
    conn: psycopg.Connection[Any],
    *,
    order_id: int,
    candidate: _Candidate,
    expected_digest: str,
    evidence: dict[str, Any],
    broker_factory: RecommendationBrokerFactory,
    clock: WindowBClock,
    this_host: str,
    this_pid: int,
    liveness_probe: Callable[[int, int], None],
    outcome: _Outcome,
) -> None:
    # Step 6: the whole detail must not have moved between the unlocked read and now.
    if _read_candidate(conn, order_id) != candidate:
        raise WindowBRefused("recommendation_release_candidate_changed")

    # Step 7: the sends have ended, per state (spec Delta 1).
    anchor: datetime
    if candidate.state == "W":
        sender_refusal = sender_death_refusal(
            pid=candidate.entered_pid,
            host=candidate.entered_host,
            entered_at=candidate.entered_at,
            this_host=this_host,
            this_pid=this_pid,
            probe=liveness_probe,
        )
        if sender_refusal is not None:
            raise WindowBRefused(sender_refusal)
        assert candidate.entered_at is not None  # sender_death_refusal refuses NULL
        anchor = candidate.entered_at
        evidence["sender"] = {
            "pid": candidate.entered_pid,
            "host": candidate.entered_host,
            "entered_at": candidate.entered_at,
        }
    else:
        # U never consults sender death: the executor is the long-lived daemon.
        if candidate.parked_at is None or candidate.park_message is None:
            raise WindowBRefused("recommendation_release_park_unrecorded")
        anchor = candidate.parked_at
        evidence["parked_at"] = candidate.parked_at
    evidence["raw_payload_json"] = candidate.raw_payload
    evidence["park_message"] = candidate.park_message
    ref_refusal = payload_reference_refusal(candidate.raw_payload, candidate.park_message)
    if ref_refusal is not None:
        raise WindowBRefused(ref_refusal)
    actual_digest = payload_digest(candidate.raw_payload, candidate.park_message)
    if actual_digest != expected_digest:
        raise WindowBRefused(
            "recommendation_release_payload_not_inspected",
            "the payload/park-message digest differs from --payload-sha256; re-run --show",
        )
    evidence["payload_sha256"] = actual_digest
    if candidate.action == "EXIT" and (candidate.exit_position_id is None or candidate.exit_units is None):
        raise WindowBRefused("recommendation_release_exit_lot_unrecorded")
    observed_monotonic = clock.monotonic()
    observed_wall = clock.now()
    evidence["t_obs"] = observed_wall

    # Step 8: wait, key held, NO DB reads (the connection stays idle).
    while True:
        remaining = window_b_wait_remaining(
            dead_monotonic=observed_monotonic,
            now_monotonic=clock.monotonic(),
            entered_at=anchor,
            now_wall=clock.now(),
        )
        if remaining <= 0:
            break
        clock.sleep(remaining)
    not_before = max(anchor, observed_wall) + WINDOW_B_VISIBILITY_WAIT
    evidence["wait"] = {"seconds": WINDOW_B_VISIBILITY_WAIT.total_seconds(), "witness_not_before": not_before}

    # Step 9: one informational read of the ORDER's own account.
    try:
        with broker_factory(candidate.api_credential_id, candidate.user_credential_id) as broker:
            snapshot = broker.get_account_risk_snapshot()
    except WindowBRefused as exc:
        if exc.slug == "window_b_credentials_unresolved":
            raise WindowBRefused(CREDENTIALS_UNRESOLVED, exc.detail, exc.evidence) from exc
        raise
    except Exception as exc:
        raise WindowBRefused("recommendation_release_witness_unavailable", type(exc).__name__) from exc
    if candidate.action == "EXIT":
        assert candidate.exit_position_id is not None and candidate.exit_units is not None
        exit_witness = evaluate_recommendation_exit_witness(
            snapshot.raw_payload,
            instrument_id=candidate.instrument_id,
            position_id=candidate.exit_position_id,
            recorded_units=candidate.exit_units,
            observed_at=snapshot.observed_at,
            not_before=not_before,
        )
        evidence["exit_lot"] = {"position_id": candidate.exit_position_id, "units": candidate.exit_units}
        evidence["witness"] = {"observed_at": snapshot.observed_at, **asdict(exit_witness)}
        witness_refusal = exit_witness.refusal
    else:
        open_witness = evaluate_window_b_witness(
            snapshot.raw_payload,
            instrument_id=candidate.instrument_id,
            authority_created_at=candidate.created_at,
            observed_at=snapshot.observed_at,
            not_before=not_before,
        )
        evidence["witness"] = {"observed_at": snapshot.observed_at, **asdict(open_witness)}
        witness_refusal = open_witness.refusal
    if witness_refusal is not None:
        raise WindowBRefused(witness_refusal)

    # Step 10: one transaction, key held, every statement a compare-and-set asserted
    # to touch exactly one row. The recommendation goes TERMINAL, not back to
    # 'approved': an order may exist, so re-submitting would spend the residual
    # without anyone choosing to.
    orders_updated = conn.execute(
        _TERMINAL_ORDER_SQL,
        {
            "order_id": order_id,
            "status": candidate.status,
            "recommendation_id": candidate.recommendation_id,
            "created_at": candidate.created_at,
            "instrument_id": candidate.instrument_id,
            "action": candidate.action,
            "broker_environment": candidate.broker_environment,
            "raw_payload": Jsonb(candidate.raw_payload),
            "park_message": candidate.park_message,
            "api_credential_id": candidate.api_credential_id,
            "user_credential_id": candidate.user_credential_id,
            "entered_at": candidate.entered_at,
            "entered_pid": candidate.entered_pid,
            "entered_host": candidate.entered_host,
            "parked_at": candidate.parked_at,
            "exit_position_id": candidate.exit_position_id,
            "exit_units": candidate.exit_units,
        },
    ).rowcount
    if orders_updated != 1:
        raise WindowBRefused("recommendation_release_terminal_write_mismatch", f"orders affected {orders_updated} rows")
    recommendations_updated = conn.execute(
        """
        UPDATE trade_recommendations SET status = 'execution_failed'
        WHERE recommendation_id = %s AND status = %s AND instrument_id = %s AND action = %s
        """,
        (candidate.recommendation_id, candidate.recommendation_status, candidate.instrument_id, candidate.action),
    ).rowcount
    if recommendations_updated != 1:
        raise WindowBRefused(
            "recommendation_release_terminal_write_mismatch",
            f"trade_recommendations affected {recommendations_updated} rows",
        )
    # The PASS row comes LAST: written only once both asserted UPDATEs succeeded.
    decision_id = _write_audit(
        conn,
        instrument_id=candidate.instrument_id,
        recommendation_id=candidate.recommendation_id,
        passed=True,
        explanation=RECOMMENDATION_RELEASED,
        evidence=evidence,
    )
    outcome.commit_attempted = True
    conn.commit()
    outcome.decision_id = decision_id
