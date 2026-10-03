"""#3545 slice 1: hourly eToro bid/ask for the cost model's calibration population, inside the NYSE session.

Spec: ``docs/proposals/etl/2026-10-03-3545-session-rate-capture.md``; schema
``sql/465_etoro_session_rate_captures.sql``.

The frozen cost model's limit 1 is that its spreads come from one clock hour of the day, and the daily
perishables recorder (19:07 UTC) repeats that bias. This capture records the same rates endpoint for the
validated universe once an hour while the session is open, so a later recalibration (slice 2, a new
``COST_MODEL_ID``) can see the whole session.

⚠ Deliberately NOT written into the perishables tables: ``ranking_pot_activation`` and ``ai_trial_readout``
read them and are hashed into live declarations (see the migration header).

The request/parse/refusal machinery is the recorder's, imported: ``execute`` classifies one request,
``_Phase`` applies the 401/403, consecutive-error and nothing-succeeded refusals, ``parse_rates`` is the
envelope contract. A capture commits in ONE transaction at the end, failed ones included.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, Protocol

import psycopg

from app.providers.implementations.etoro_perishables import RATES_BATCH_SIZE, RawResponse
from app.services.market_session_support import venue_session_is_open
from app.services.perishables_recorder import (
    MAX_UNIVERSE,
    PHASE_RATES,
    PerishableSnapshotPartial,
    PerishableSnapshotRefused,
    RateRow,
    Request,
    _json,
    _Phase,
    execute,
    parse_rates,
    partial_category,
)
from app.services.strategies.validated_universe import (
    US_EQUITY_ASSET_CLASS,
    VALIDATED_UNIVERSE_RULE_VERSION,
    load_validated_universe,
)
from app.services.sync_orchestrator.layer_types import FailureCategory

logger = logging.getLogger(__name__)

RECORDER_VERSION: Final = "1"

STATUS_COMPLETE: Final = "complete"
STATUS_PARTIAL: Final = "partial"
STATUS_FAILED: Final = "failed"


class RatesSource(Protocol):
    def get_rates(self, instrument_ids: list[int]) -> RawResponse: ...


def session_open(now: datetime) -> bool:
    """The NYSE regular session (09:30 ≤ t < close ET, 13:00 on a half day) is open at ``now``.

    The same predicate the submission paths use (``market_session_support``), not the halt window's wider
    one: an off-session quote is not a session spread.
    """
    return venue_session_is_open(US_EQUITY_ASSET_CLASS, now)


@dataclass(frozen=True)
class SessionRateCaptureResult:
    capture_id: int
    status: str
    universe_size: int
    instruments_served: int
    instruments_quoted: int

    @property
    def row_count(self) -> int:
        return self.instruments_served


@dataclass
class _Capture:
    universe: tuple[int, ...] | None = None
    phase: _Phase | None = None
    rows: list[tuple[RateRow, Request]] = field(default_factory=list)


def _quoted(row: RateRow) -> bool:
    return row.bid is not None and row.ask is not None and row.bid > 0 and row.ask > 0 and row.ask >= row.bid


def _collect(conn: psycopg.Connection[Any], source: RatesSource, cap: _Capture, clock: Callable[[], datetime]) -> None:
    with conn.transaction():
        universe = cap.universe = load_validated_universe(conn)
    if not universe:
        raise PerishableSnapshotRefused(FailureCategory.DATA_GAP, "the validated universe is empty")
    if len(universe) > MAX_UNIVERSE:
        raise PerishableSnapshotRefused(
            FailureCategory.INTERNAL_ERROR, f"universe {len(universe)} exceeds MAX_UNIVERSE {MAX_UNIVERSE}"
        )
    batches = [list(universe[i : i + RATES_BATCH_SIZE]) for i in range(0, len(universe), RATES_BATCH_SIZE)]
    phase = cap.phase = _Phase(len(batches))
    for seq, ids in enumerate(batches):
        request, parsed = execute(
            PHASE_RATES,
            seq,
            ids,
            {"instrumentIds": ids},
            lambda ids=ids: source.get_rates(ids),
            lambda body, ids=ids: parse_rates(ids, body),
            clock,
        )
        if isinstance(parsed, list):
            cap.rows.extend((row, request) for row in parsed)
        phase.add(request)
    phase.finish(PHASE_RATES)


def capture_session_rates(
    conn: psycopg.Connection[Any],
    source: RatesSource,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> SessionRateCaptureResult:
    """Collect and append one capture.

    ⚠ The caller owns the session check (``session_open``): the job re-checks it in its body, not only in
    its prerequisite, because a fire queued behind its lane past the close, or a manual run, must not write
    a row that reads as in-session.

    Any abort commits a ``failed`` header with everything collected so far, then re-raises the original
    exception. A ``partial`` capture commits, then raises ``PerishableSnapshotPartial``.
    """
    started_at = clock()
    cap = _Capture()
    try:
        _collect(conn, source, cap, clock)
        assert cap.phase is not None
        status = STATUS_PARTIAL if cap.phase.errored else STATUS_COMPLETE
        finished_at = clock()
        # `_write` is the LAST statement in the try: once its transaction commits, nothing here can raise
        # and add a `failed` header beside the committed one.
        capture_id = _write(conn, cap, started_at, finished_at, status, None)
    except Exception as exc:
        _record_failure(conn, cap, started_at, clock(), exc)
        raise
    result = SessionRateCaptureResult(
        capture_id=capture_id,
        status=status,
        universe_size=len(cap.universe or ()),
        instruments_served=len(cap.rows),
        instruments_quoted=sum(_quoted(row) for row, _ in cap.rows),
    )
    if status == STATUS_PARTIAL:
        assert cap.phase is not None
        raise PerishableSnapshotPartial(
            partial_category(cap.phase.requests),
            f"session rate capture {capture_id} committed partial: {cap.phase.errored} requests errored",
        )
    return result


_INSERT_CAPTURE_SQL: Final = """
INSERT INTO etoro_session_rate_captures (
    started_at, finished_at, status, recorder_version, universe_rule_version, universe_size, requests_expected,
    requests_ok, requests_errored, instruments_served, instruments_quoted, error
) VALUES (
    %(started_at)s, %(finished_at)s, %(status)s, %(recorder_version)s, %(universe_rule_version)s, %(universe_size)s,
    %(requests_expected)s, %(requests_ok)s, %(requests_errored)s, %(instruments_served)s, %(instruments_quoted)s,
    %(error)s
)
RETURNING capture_id
"""

_COPY_REQUESTS_SQL: Final = (
    "COPY etoro_session_rate_requests (capture_id, seq, instrument_ids, observed_at, outcome, http_status, "
    "error_body) FROM STDIN"
)
_COPY_ROWS_SQL: Final = (
    "COPY etoro_session_rate_observations (capture_id, instrument_id, request_seq, observed_at, quote_at, bid, ask, "
    "last_execution, conversion_rate_bid, conversion_rate_ask) FROM STDIN"
)


def _write(
    conn: psycopg.Connection[Any],
    cap: _Capture,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    error: str | None,
) -> int:
    phase = cap.phase
    requests = phase.requests if phase is not None else []
    with conn.transaction():
        row = conn.execute(
            _INSERT_CAPTURE_SQL,
            {
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "recorder_version": RECORDER_VERSION,
                "universe_rule_version": VALIDATED_UNIVERSE_RULE_VERSION,
                "universe_size": len(cap.universe) if cap.universe is not None else None,
                "requests_expected": phase.expected if phase is not None else None,
                "requests_ok": phase.ok if phase is not None else 0,
                "requests_errored": phase.errored if phase is not None else 0,
                "instruments_served": len(cap.rows),
                "instruments_quoted": sum(_quoted(r) for r, _ in cap.rows),
                "error": error,
            },
        ).fetchone()
        assert row is not None
        capture_id = int(row[0])
        with conn.cursor() as cur:
            if requests:
                with cur.copy(_COPY_REQUESTS_SQL) as copy:
                    for r in requests:
                        copy.write_row(
                            (
                                capture_id,
                                r.seq,
                                list(r.instrument_ids),
                                r.observed_at,
                                r.outcome,
                                r.http_status,
                                _json(r.raw) if r.outcome == "error" else None,
                            )
                        )
            if cap.rows:
                with cur.copy(_COPY_ROWS_SQL) as copy:
                    for q, r in cap.rows:
                        copy.write_row(
                            (
                                capture_id,
                                q.instrument_id,
                                r.seq,
                                r.observed_at,
                                q.quote_at,
                                q.bid,
                                q.ask,
                                q.last_execution,
                                q.conversion_rate_bid,
                                q.conversion_rate_ask,
                            )
                        )
    return capture_id


def _record_failure(
    conn: psycopg.Connection[Any], cap: _Capture, started_at: datetime, finished_at: datetime, exc: BaseException
) -> None:
    """Commit a ``failed`` header with everything collected; failing that, a bare one. Best-effort only —
    never allowed to replace the error the caller re-raises."""
    error = f"{type(exc).__name__}: {exc}"
    for attempt in (cap, _Capture(universe=cap.universe)):
        try:
            capture_id = _write(conn, attempt, started_at, finished_at, STATUS_FAILED, error)
        except Exception:
            logger.exception("session rate capture failed (%s) and its failed row could not be written", error)
            continue
        logger.warning("session rate capture %d failed: %s", capture_id, error)
        return


__all__ = [
    "RECORDER_VERSION",
    "RatesSource",
    "SessionRateCaptureResult",
    "capture_session_rates",
    "session_open",
]
