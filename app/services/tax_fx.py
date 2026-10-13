"""Tax FX source: Bank of England XUDLUSS, verified against the GOV.UK England-and-Wales calendar (#3616 slice 3b).

Spec: ``docs/specs/metrics/2026-10-13-3616-slice3b-tax-fx-boe.md``. HMRC CG78310 requires each foreign-currency
amount to be converted at the rate for its own date by "a reasonable and consistent method" and names no source; the
choices here are this project's local policy, frozen under ``TAX_FX_POLICY_ID``.

Shape:
- ``run_tax_fx_refresh`` is the job body: one run under an exclusive session advisory lock on a dedicated connection.
  It records every download in ``tax_fx_fetches``, validates and applies in one transaction, and raises on a
  ``skipped`` or ``refused`` run and on ``feed_stale``.
- ``plan_run`` is the pure validate-and-apply planner (spec Design 2, steps 4.1-4.11). The job and
  ``replay_from_evidence`` both call it.
- ``load_tax_fx_state`` / ``resolve_tax_fx`` / ``tax_fx_state_is_current`` are the consumer contract (Design 3). No
  consumer exists yet: slice 3c is the first. Nothing here changes ``fx_rates``, ``_load_fx_rate`` or any tax figure.

A value is stored only after two consecutive eligible responses agree on it, and the stored set is always a complete
prefix of working days from ``FX_FLOOR``. Evidence that contradicts the stored state refuses the run and leaves the
source ``unavailable``; this slice has no override (Design 5).

⚠ The repository is public. Never commit a Bank body or rate; tests use synthetic values.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import time
from bisect import bisect_left
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import httpx
import psycopg
from psycopg.pq import TransactionStatus

from app.db.snapshot import snapshot_read
from app.providers.implementations import govuk_calendar
from app.providers.implementations.boe import USER_AGENT, BoeParseError, parse_xudluss_csv, xudluss_url
from app.providers.implementations.govuk_calendar import CalendarParseError, parse_bank_holidays

logger = logging.getLogger(__name__)

#: Names the request's data parameters (``VPD=N``, ``FX_FLOOR``), the parser contracts, validation steps 4.1-4.10
#: with ``BOOTSTRAP_MAX_LAG``, and the resolver rules. Changing any of them is a new ID, so every dependent lot goes
#: stale (Design 3).
TAX_FX_POLICY_ID: Final = "boe_xudluss_ew_calendar_v1"
PARSER_VERSION: Final = "boe_xudluss_csv_v1+govuk_bank_holidays_v1"
#: The calendar seed's first year.
FX_FLOOR: Final = date(2019, 1, 1)
#: Run lock (exclusive, session) and publisher lock (shared, transaction). Above the int4 range, so it cannot equal a
#: ``hashtext(...)`` key. sql/484 states the same literal for the control-change migrations.
TAX_FX_LOCK_KEY: Final = 0x36160003B0E00001
#: sha256 of ``docs/research/3616-slice3b-inputs/bank-holidays_2026-10-13.json``, the sql/484 seed.
CALENDAR_SEED_SHA256: Final = "538b3482c28b85ecd2db606a0d5ae6ad17248900b6498700ce0a48d26a3ecde6"
#: Policy: bootstrap refuses a body whose last observation is more than this many working days before ``run_date``.
BOOTSTRAP_MAX_LAG: Final = 3
#: Operational: the Bank's two-working-day window, plus one day for confirmation, plus one.
STALE_ALERT_LAG: Final = 4
#: Anomaly gates (Design 2, step 4.3). The largest consecutive move since 1975 is 0.0829 (spec fact 4).
MAX_ABS_LN_MOVE: Final = Decimal("0.15")
MAX_REVISIONS_PER_RUN: Final = 5
FETCH_DEADLINE_SECONDS: Final = 60.0
#: A BoE row is at most 27 bytes; 32 per requested day plus the header line.
BOE_BYTES_PER_DAY: Final = 32
BOE_BYTES_BASE: Final = 64
CALENDAR_MAX_BYTES: Final = 1024 * 1024
OVERSIZE_KEEP_BYTES: Final = 64 * 1024
GBP_PER_USD_PLACES: Final = Decimal("1E-10")  # the scale of tax_lots.fx_rate_to_gbp

SOURCE_BOE: Final = "boe_xudluss"
SOURCE_CALENDAR: Final = "govuk_bank_holidays"
_UK_TZ: Final = ZoneInfo("Europe/London")

FetchOutcome = Literal["received", "incomplete", "oversize", "transport_error"]
RateStatus = Literal["unavailable", "out_of_range", "pending", "published", "carried_forward"]
Calendar = Mapping[int, frozenset[date]]


class TaxFxRefreshError(RuntimeError):
    """The run was skipped or refused, or the accepted horizon is stale. The run's rows are already committed."""


class TaxFxInvariantError(RuntimeError):
    """Stored state breaks an invariant the run validation guarantees."""


# --- calendar arithmetic ---------------------------------------------------------------------------------------


def is_working_day(day: date, calendar: Calendar) -> bool:
    """Monday to Friday and not an England-and-Wales bank holiday. A year absent from the calendar raises."""
    holidays = calendar.get(day.year)
    if holidays is None:
        raise TaxFxInvariantError(f"calendar has no year {day.year}")
    return day.weekday() < 5 and day not in holidays


def working_days_after(start: date, end: date, calendar: Calendar) -> int:
    """The number of working days d with start < d <= end."""
    count, day = 0, start + timedelta(days=1)
    while day <= end:
        count += is_working_day(day, calendar)
        day += timedelta(days=1)
    return count


def beyond_move_bound(abs_ln: Decimal) -> bool:
    """The anomaly gate: a move or revision passes at |ln| <= 0.15 inclusive."""
    return abs_ln > MAX_ABS_LN_MOVE


def _exceeds_move_bound(a: Decimal, b: Decimal) -> bool:
    return beyond_move_bound(abs((b / a).ln()))


def feed_stale_origin(
    accepted_through: date | None, first_eligible_run_date: date | None, run_date: date, calendar: Calendar
) -> date | None:
    """The date the alert measures from when it fires (Design 2, step 5), else None. It measures the accepted horizon
    *L*, not the body, so stalled confirmation alerts while fresh bodies keep arriving; while the store is empty it
    measures from the first eligible run, which a retry cannot reset."""
    origin = accepted_through if accepted_through is not None else first_eligible_run_date
    if origin is not None and working_days_after(origin, run_date, calendar) > STALE_ALERT_LAG:
        return origin
    return None


# --- fetching --------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Fetch:
    """One download attempt, as stored in ``tax_fx_fetches``. ``body`` is the exact entity bytes (no decoding)."""

    source: str
    request_url: str
    fetched_at: datetime
    outcome: FetchOutcome
    error: str | None = None
    http_status: int | None = None
    content_length_header: str | None = None
    content_encoding_header: str | None = None
    body: bytes | None = None
    user_agent: str = USER_AGENT


def fetch_capped(
    client: httpx.Client,
    *,
    source: str,
    url: str,
    max_bytes: int,
    now: Callable[[], datetime],
    monotonic: Callable[[], float] = time.monotonic,
    deadline_seconds: float = FETCH_DEADLINE_SECONDS,
) -> Fetch:
    """One GET with no redirect followed, ``Accept-Encoding: identity`` and the raw entity bytes streamed under a limit.

    The deadline is checked after the headers and after every chunk; a single stalled read is bounded by the client's
    per-operation timeout, set to the same value, so the worst case is the deadline plus one read timeout.
    """
    fetched_at = now()
    started = monotonic()
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    try:
        with client.stream("GET", url, headers=headers, follow_redirects=False, timeout=deadline_seconds) as response:
            framing = {
                "http_status": response.status_code,
                "content_length_header": response.headers.get("content-length"),
                "content_encoding_header": response.headers.get("content-encoding"),
            }
            body = bytearray()
            try:
                for chunk in response.iter_raw():
                    body += chunk
                    if len(body) > max_bytes:
                        return Fetch(
                            source,
                            url,
                            fetched_at,
                            "oversize",
                            f"body passed {max_bytes} bytes; first {OVERSIZE_KEEP_BYTES} kept",
                            body=bytes(body[:OVERSIZE_KEEP_BYTES]),
                            **framing,
                        )
                    if monotonic() - started > deadline_seconds:
                        raise httpx.ReadTimeout(f"passed the {deadline_seconds:g} s fetch deadline")
            except httpx.HTTPError as exc:
                return Fetch(
                    source, url, fetched_at, "incomplete", f"{type(exc).__name__}: {exc}", body=bytes(body), **framing
                )
            return Fetch(source, url, fetched_at, "received", body=bytes(body), **framing)
    except httpx.HTTPError as exc:
        return Fetch(source, url, fetched_at, "transport_error", f"{type(exc).__name__}: {exc}")


def boe_max_bytes(date_from: date, date_to: date) -> int:
    return BOE_BYTES_BASE + BOE_BYTES_PER_DAY * ((date_to - date_from).days + 1)


# --- the planner -----------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Confirmer:
    """The previous run with a BoE response: was it eligible, under which policy, and what did its body hold."""

    run_id: int
    eligible: bool
    policy_id: str
    observations: Mapping[date, Decimal]


@dataclass
class Plan:
    state: Literal["applied", "refused"]
    reason: str | None = None
    eligible: bool = False
    calendar_used: Literal["fetched", "stored"] | None = None
    body_last_date: date | None = None
    inserts: list[tuple[date, Decimal]] = field(default_factory=list)
    revisions: list[tuple[date, Decimal, Decimal]] = field(default_factory=list)
    calendar_changes: list[tuple[int, frozenset[date] | None, frozenset[date]]] = field(default_factory=list)
    awaiting_confirmation: int = 0
    accepted_through: date | None = None
    calendar: dict[int, frozenset[date]] = field(default_factory=dict)


def _framing_refusal(label: str, fetch: Fetch) -> str | None:
    if fetch.http_status != 200:
        return f"{label}_http_status: HTTP {fetch.http_status}"
    if fetch.content_encoding_header is not None:
        return f"{label}_content_encoding: {fetch.content_encoding_header!r}"
    declared = fetch.content_length_header
    length = len(fetch.body or b"")
    if declared is not None and (not declared.isdigit() or int(declared) != length):
        return f"{label}_length_mismatch: Content-Length {declared!r}, body {length} bytes"
    return None


def plan_run(
    *,
    run_date: date,
    stored_observations: Mapping[date, Decimal],
    stored_calendar: Calendar,
    boe: Fetch,
    calendar_fetch: Fetch | None,
    confirmer: Confirmer | None,
) -> Plan:
    """Validate a run's evidence against the stored state and plan its changes (spec Design 2, steps 3-4.11).

    ``boe`` must not be ``incomplete`` or a ``transport_error`` (those make the run ``skipped`` before planning).
    The first failed check returns a ``refused`` plan naming the check and the dates involved.
    """
    if boe.outcome not in ("received", "oversize"):
        raise ValueError(f"a {boe.outcome} BoE fetch is skipped, not planned")
    if boe.outcome == "oversize":
        return Plan("refused", f"boe_oversize: {boe.error}")
    if calendar_fetch is not None and calendar_fetch.outcome == "oversize":
        return Plan("refused", f"calendar_oversize: {calendar_fetch.error}")

    # 4.1 BoE response.
    if (refusal := _framing_refusal("boe", boe)) is not None:
        return Plan("refused", refusal)
    try:
        parsed = parse_xudluss_csv(boe.body or b"", FX_FLOOR, run_date)
    except BoeParseError as exc:
        return Plan("refused", f"boe_parse: {exc}")
    if not parsed:
        return Plan("refused", "boe_empty: no observations")
    body = {o.observation_date: o.usd_per_gbp for o in parsed}
    body_last = parsed[-1].observation_date

    # 4.2 Calendar response; 4.5 calendar changes.
    candidate = dict(stored_calendar)
    changes: list[tuple[int, frozenset[date] | None, frozenset[date]]] = []
    calendar_used: Literal["fetched", "stored"] = "stored"
    if calendar_fetch is not None and calendar_fetch.outcome == "received":
        if (refusal := _framing_refusal("calendar", calendar_fetch)) is not None:
            return Plan("refused", refusal, body_last_date=body_last)
        try:
            fetched = parse_bank_holidays(calendar_fetch.body or b"")
        except CalendarParseError as exc:
            return Plan("refused", f"calendar_parse: {exc}", body_last_date=body_last)
        calendar_used = "fetched"
        for year in sorted(fetched):
            if year < FX_FLOOR.year:
                continue
            new, old = fetched[year], stored_calendar.get(year)
            if old == new:
                continue
            if old is not None:
                past = sorted(d for d in old ^ new if d <= run_date)
                if past:
                    return Plan(
                        "refused",
                        f"calendar_history_changed: {', '.join(map(str, past))} on or before {run_date}",
                        calendar_used=calendar_used,
                        body_last_date=body_last,
                    )
            candidate[year] = new
            changes.append((year, old, new))

    def refuse(reason: str, *, eligible: bool = False) -> Plan:
        return Plan("refused", reason, eligible=eligible, calendar_used=calendar_used, body_last_date=body_last)

    # 4.3 Plausibility.
    for a, b in zip(parsed, parsed[1:], strict=False):
        if _exceeds_move_bound(a.usd_per_gbp, b.usd_per_gbp):
            return refuse(f"implausible_move: {a.observation_date} -> {b.observation_date}")
    differing = sorted(d for d, old in stored_observations.items() if d in body and body[d] != old)
    for day in differing:
        if _exceeds_move_bound(stored_observations[day], body[day]):
            return refuse(f"implausible_revision: {day}")
    if len(differing) > MAX_REVISIONS_PER_RUN:
        return refuse(f"too_many_revisions: {len(differing)} stored dates differ ({differing[0]}..{differing[-1]})")

    # 4.4 No regression.
    missing = sorted(d for d in stored_observations if d not in body)
    if missing:
        return refuse(f"regression: {len(missing)} stored dates absent from the body, first {missing[0]}")

    # 4.6 Continuity.
    absent_years = [y for y in range(FX_FLOOR.year, run_date.year + 2) if y not in candidate]
    if absent_years:
        return refuse(f"calendar_discontinuous: no calendar for {absent_years}")

    # 4.7 Exact coverage, both directions.
    unobserved_working: list[date] = []
    observed_non_working: list[date] = []
    day = FX_FLOOR
    while day <= body_last:
        working = is_working_day(day, candidate)
        if working and day not in body:
            unobserved_working.append(day)
        elif not working and day in body:
            observed_non_working.append(day)
        day += timedelta(days=1)
    if unobserved_working or observed_non_working:
        first = (unobserved_working[:3], observed_non_working[:3])
        return refuse(
            f"coverage_mismatch: {len(unobserved_working)} working days unobserved, {len(observed_non_working)} "
            f"observations on non-working days; first {first[0]}, {first[1]}"
        )

    # 4.8 Bootstrap freshness.
    if not stored_observations and working_days_after(body_last, run_date, candidate) > BOOTSTRAP_MAX_LAG:
        return refuse(f"bootstrap_stale: last observation {body_last}, run date {run_date}")

    # 4.9 Eligible. 4.10 Confirmation, the last check.
    confirmed: Mapping[date, Decimal] = {}
    if confirmer is not None and confirmer.eligible and confirmer.policy_id == TAX_FX_POLICY_ID:
        confirmed = confirmer.observations
    unconfirmed = [d for d in differing if confirmed.get(d) != body[d]]
    if unconfirmed:
        return refuse(f"revision_unconfirmed: {', '.join(map(str, unconfirmed))}", eligible=True)
    new_dates = [d for d in body if d not in stored_observations]
    inserts: list[tuple[date, Decimal]] = []
    for new_day in new_dates:
        if confirmed.get(new_day) != body[new_day]:
            break
        inserts.append((new_day, body[new_day]))

    # 4.11 Apply.
    accepted = [*stored_observations, *(d for d, _ in inserts)]
    return Plan(
        "applied",
        eligible=True,
        calendar_used=calendar_used,
        body_last_date=body_last,
        inserts=inserts,
        revisions=[(d, stored_observations[d], body[d]) for d in differing],
        calendar_changes=changes,
        awaiting_confirmation=len(new_dates) - len(inserts),
        accepted_through=max(accepted) if accepted else None,
        calendar=candidate,
    )


# --- the run ---------------------------------------------------------------------------------------------------

_RUN_LOCK_HELD: ContextVar[bool] = ContextVar("tax_fx_run_lock_held", default=False)


@contextmanager
def _run_lock(conn: psycopg.Connection[Any]) -> Iterator[None]:
    """Exclusive session advisory lock, waited for. Non-re-entrant: a nested acquire on one session would succeed and
    its release would release nothing (prevention log, "A NESTED pg_advisory_lock acquire")."""
    if _RUN_LOCK_HELD.get():
        raise RuntimeError("tax FX run lock is already held in this context")
    conn.execute("SELECT pg_advisory_lock(%s)", (TAX_FX_LOCK_KEY,))
    token = _RUN_LOCK_HELD.set(True)
    try:
        yield
    finally:
        _RUN_LOCK_HELD.reset(token)
        released = False
        try:
            if conn.info.transaction_status != TransactionStatus.IDLE:
                conn.rollback()
            row = conn.execute("SELECT pg_advisory_unlock(%s)", (TAX_FX_LOCK_KEY,)).fetchone()
            released = bool(row and row[0])
        except psycopg.Error:
            logger.warning("tax_fx: advisory unlock failed", exc_info=True)
        if not released:
            # Closing ends the session, which releases the lock; never hand a lock-holding session onward.
            conn.close()


@dataclass(frozen=True)
class RunReport:
    run_id: int
    run_date: date
    state: str
    reason: str | None
    inserted: int
    revised: int
    awaiting_confirmation: int
    calendar_years_changed: int
    accepted_through: date | None


def _record_fetch(conn: psycopg.Connection[Any], run_id: int, fetch: Fetch) -> None:
    body = fetch.body
    conn.execute(
        """
        INSERT INTO tax_fx_fetches
            (run_id, source, request_url, user_agent, fetched_at, outcome, error, http_status,
             content_length_header, content_encoding_header, body_gz, body_sha256, body_length)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            fetch.source,
            fetch.request_url,
            fetch.user_agent,
            fetch.fetched_at,
            fetch.outcome,
            fetch.error,
            fetch.http_status,
            fetch.content_length_header,
            fetch.content_encoding_header,
            None if body is None else gzip.compress(body),
            None if body is None else hashlib.sha256(body).hexdigest(),
            None if body is None else len(body),
        ),
    )


def _finish(conn: psycopg.Connection[Any], run_id: int, state: str, reason: str | None, plan: Plan | None) -> None:
    conn.execute(
        """
        UPDATE tax_fx_runs
           SET state = %s, reason = %s, finished_at = clock_timestamp(),
               eligible = %s, calendar_used = %s, body_last_date = %s, accepted_through = %s,
               inserted = %s, revised = %s, awaiting_confirmation = %s, calendar_years_changed = %s
         WHERE run_id = %s AND state = 'in_progress'
        """,
        (
            state,
            reason,
            plan.eligible if plan else False,
            plan.calendar_used if plan else None,
            plan.body_last_date if plan else None,
            plan.accepted_through if plan and state == "applied" else None,
            len(plan.inserts) if plan and state == "applied" else 0,
            len(plan.revisions) if plan and state == "applied" else 0,
            plan.awaiting_confirmation if plan and state == "applied" else 0,
            len(plan.calendar_changes) if plan and state == "applied" else 0,
            run_id,
        ),
    )


def _stored_observations(conn: psycopg.Connection[Any]) -> dict[date, Decimal]:
    rows = conn.execute("SELECT observation_date, usd_per_gbp FROM tax_fx_observations ORDER BY 1").fetchall()
    return {row[0]: row[1] for row in rows}


def _stored_calendar(conn: psycopg.Connection[Any]) -> dict[int, frozenset[date]]:
    rows = conn.execute("SELECT year, holiday_dates FROM tax_fx_calendar_years").fetchall()
    return {int(row[0]): frozenset(row[1]) for row in rows}


def _load_fetch(conn: psycopg.Connection[Any], run_id: int, source: str) -> Fetch | None:
    row = conn.execute(
        """
        SELECT request_url, user_agent, fetched_at, outcome, error, http_status, content_length_header,
               content_encoding_header, body_gz
          FROM tax_fx_fetches WHERE run_id = %s AND source = %s
        """,
        (run_id, source),
    ).fetchone()
    if row is None:
        return None
    body = None if row[8] is None else gzip.decompress(row[8])
    return Fetch(source, row[0], row[2], row[3], row[4], row[5], row[6], row[7], body, user_agent=row[1])


#: A BoE response exists: ``received``, or ``oversize`` (a wrong response, refused). Such a run is the one health and
#: confirmation look at; ``incomplete`` and ``transport_error`` runs add no evidence and are skipped.
_RESPONDED_SQL: Final = (
    "EXISTS (SELECT 1 FROM tax_fx_fetches f WHERE f.run_id = r.run_id AND f.source = 'boe_xudluss' "
    "AND f.outcome IN ('received', 'oversize'))"
)


def _confirmer_from(
    conn: psycopg.Connection[Any], run_id: int, eligible: bool, policy_id: str, run_date: date
) -> Confirmer:
    observations: dict[date, Decimal] = {}
    fetch = _load_fetch(conn, run_id, SOURCE_BOE)
    if eligible and fetch is not None and fetch.outcome == "received":
        try:
            parsed = parse_xudluss_csv(fetch.body or b"", FX_FLOOR, run_date)
        except BoeParseError:
            parsed = []
        observations = {o.observation_date: o.usd_per_gbp for o in parsed}
    return Confirmer(run_id, eligible, policy_id, observations)


def _previous_confirmer(conn: psycopg.Connection[Any], run_id: int) -> Confirmer | None:
    row = conn.execute(
        f"SELECT r.run_id, r.eligible, r.policy_id, r.run_date FROM tax_fx_runs r "
        f"WHERE r.run_id < %s AND {_RESPONDED_SQL} ORDER BY r.run_id DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    return None if row is None else _confirmer_from(conn, row[0], row[1], row[2], row[3])


def _apply(conn: psycopg.Connection[Any], run_id: int, plan: Plan) -> None:
    with conn.cursor() as cur:
        for day, value in plan.inserts:
            cur.execute(
                "INSERT INTO tax_fx_observations (observation_date, usd_per_gbp, set_by_run_id) VALUES (%s, %s, %s)",
                (day, value, run_id),
            )
        for day, old, new in plan.revisions:
            cur.execute(
                "UPDATE tax_fx_observations SET usd_per_gbp = %s, set_by_run_id = %s WHERE observation_date = %s",
                (new, run_id, day),
            )
            cur.execute(
                "INSERT INTO tax_fx_revisions (observation_date, old_usd_per_gbp, new_usd_per_gbp, run_id) "
                "VALUES (%s, %s, %s, %s)",
                (day, old, new, run_id),
            )
            logger.warning("tax_fx run %d: revision %s %s -> %s", run_id, day, old, new)
        for year, old_dates, new_dates in plan.calendar_changes:
            cur.execute(
                """
                INSERT INTO tax_fx_calendar_years (year, holiday_dates, source) VALUES (%s, %s, %s)
                ON CONFLICT (year) DO UPDATE SET holiday_dates = EXCLUDED.holiday_dates, source = EXCLUDED.source
                """,
                (year, sorted(new_dates), f"run:{run_id}"),
            )
            cur.execute(
                "INSERT INTO tax_fx_calendar_revisions (year, old_dates, new_dates, run_id) VALUES (%s, %s, %s, %s)",
                (year, None if old_dates is None else sorted(old_dates), sorted(new_dates), run_id),
            )


def _london_date(moment: datetime) -> date:
    return moment.astimezone(_UK_TZ).date()


def run_tax_fx_refresh(
    conn: psycopg.Connection[Any],
    client: httpx.Client,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
) -> RunReport:
    """One run (spec Design 2). ``conn`` must be a dedicated autocommit connection; it is closed if the lock's release
    cannot be confirmed. Raises ``TaxFxRefreshError`` after committing a ``skipped`` or ``refused`` run, or when the
    accepted horizon is stale (``feed_stale``)."""
    if not conn.autocommit:
        raise ValueError("run_tax_fx_refresh needs an autocommit connection")
    with _run_lock(conn):
        run_date = _london_date(now())
        # 1. Open the run.
        with conn.transaction():
            conn.execute(
                "UPDATE tax_fx_runs SET state = 'abandoned', finished_at = clock_timestamp(), "
                "reason = 'found in_progress by a later run' WHERE state = 'in_progress'"
            )
            row = conn.execute(
                "INSERT INTO tax_fx_runs (started_at, run_date, state, policy_id, parser_version) "
                "VALUES (clock_timestamp(), %s, 'in_progress', %s, %s) RETURNING run_id",
                (run_date, TAX_FX_POLICY_ID, PARSER_VERSION),
            ).fetchone()
        assert row is not None
        run_id = int(row[0])

        def stop(state: str, reason: str, plan: Plan | None = None) -> TaxFxRefreshError:
            with conn.transaction():
                _finish(conn, run_id, state, reason, plan)
            logger.error("tax_fx run %d %s: %s", run_id, state, reason)
            return TaxFxRefreshError(f"tax_fx run {run_id} {state}: {reason}")

        control = conn.execute("SELECT enabled FROM tax_fx_source_control").fetchone()
        if control is None or not control[0]:
            raise stop("skipped", "source_disabled: tax_fx_source_control.enabled is false")

        # 2. Fetch both sources, committing each fetch row at once.
        boe_url = xudluss_url(FX_FLOOR, run_date)
        boe = fetch_capped(
            client,
            source=SOURCE_BOE,
            url=boe_url,
            max_bytes=boe_max_bytes(FX_FLOOR, run_date),
            now=now,
            monotonic=monotonic,
        )
        _record_fetch(conn, run_id, boe)
        cal = fetch_capped(
            client,
            source=SOURCE_CALENDAR,
            url=govuk_calendar.URL,
            max_bytes=CALENDAR_MAX_BYTES,
            now=now,
            monotonic=monotonic,
        )
        _record_fetch(conn, run_id, cal)

        # 3. A missing BoE response adds no evidence.
        if boe.outcome in ("incomplete", "transport_error"):
            raise stop("skipped", f"boe_{boe.outcome}: {boe.error}")

        # 4. Validate and apply in one transaction. The planner is pure, so a refusal has written nothing.
        with conn.transaction():
            stored = _stored_observations(conn)
            plan = plan_run(
                run_date=run_date,
                stored_observations=stored,
                stored_calendar=_stored_calendar(conn),
                boe=boe,
                calendar_fetch=cal,
                confirmer=_previous_confirmer(conn, run_id),
            )
            if plan.state == "applied":
                _apply(conn, run_id, plan)
                _finish(conn, run_id, "applied", None, plan)
        if plan.state == "refused":
            raise stop("refused", plan.reason or "refused", plan)

        report = RunReport(
            run_id,
            run_date,
            "applied",
            None,
            len(plan.inserts),
            len(plan.revisions),
            plan.awaiting_confirmation,
            len(plan.calendar_changes),
            plan.accepted_through,
        )
        logger.info(
            "tax_fx run %d applied: inserted=%d revised=%d awaiting=%d calendar_years=%d accepted_through=%s",
            run_id,
            report.inserted,
            report.revised,
            report.awaiting_confirmation,
            report.calendar_years_changed,
            report.accepted_through,
        )

        # 5. The operational alert, on the accepted horizon.
        first_eligible = None
        if plan.accepted_through is None:
            row = conn.execute("SELECT min(run_date) FROM tax_fx_runs WHERE eligible").fetchone()
            first_eligible = row[0] if row else None
        origin = feed_stale_origin(plan.accepted_through, first_eligible, run_date, plan.calendar)
        if origin is not None:
            basis = "accepted_through" if plan.accepted_through else "first eligible run"
            raise TaxFxRefreshError(
                f"tax_fx run {run_id} applied but feed_stale: {basis} {origin} is more than {STALE_ALERT_LAG} "
                f"working days before {run_date} (inserted={report.inserted}, awaiting={report.awaiting_confirmation})"
            )
        return report


def replay_from_evidence(
    conn: psycopg.Connection[Any], seed_calendar: Calendar
) -> tuple[dict[date, Decimal], dict[int, frozenset[date]]]:
    """Rebuild observations and calendar from the seed, ``tax_fx_runs`` and ``tax_fx_fetches`` alone (Design 2,
    "Replay"): every ``applied`` run is re-planned in ``run_id`` order over its stored bodies and recorded
    ``run_date``, with the previous responded run's stored body as confirmer whatever that run's state."""
    observations: dict[date, Decimal] = {}
    calendar: dict[int, frozenset[date]] = dict(seed_calendar)
    previous: Confirmer | None = None
    runs = conn.execute(
        f"SELECT r.run_id, r.state, r.eligible, r.policy_id, r.run_date, {_RESPONDED_SQL} "
        "FROM tax_fx_runs r ORDER BY r.run_id"
    ).fetchall()
    for run_id, state, eligible, policy_id, run_date, responded in runs:
        if state == "applied":
            boe = _load_fetch(conn, run_id, SOURCE_BOE)
            if boe is None:
                raise TaxFxInvariantError(f"applied run {run_id} has no BoE fetch")
            plan = plan_run(
                run_date=run_date,
                stored_observations=observations,
                stored_calendar=calendar,
                boe=boe,
                calendar_fetch=_load_fetch(conn, run_id, SOURCE_CALENDAR),
                confirmer=previous,
            )
            if plan.state != "applied":
                raise TaxFxInvariantError(f"replay of applied run {run_id} refused: {plan.reason}")
            observations.update(plan.inserts)
            observations.update({d: new for d, _, new in plan.revisions})
            calendar.update({year: new for year, _, new in plan.calendar_changes})
        if responded:
            previous = _confirmer_from(conn, run_id, eligible, policy_id, run_date)
    return dict(sorted(observations.items())), calendar


# --- the consumer contract -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class TaxFxState:
    enabled: bool
    health_run_state: str | None
    health_run_policy_id: str | None
    latest_applied_run_id: int | None
    observations: Mapping[date, Decimal]
    calendar: Calendar
    observation_dates: tuple[date, ...] = ()

    @property
    def healthy(self) -> bool:
        return (
            self.enabled
            and self.health_run_state == "applied"
            and self.health_run_policy_id == TAX_FX_POLICY_ID
            and bool(self.observations)
        )

    @property
    def accepted_through(self) -> date | None:
        return self.observation_dates[-1] if self.observation_dates else None


@dataclass(frozen=True)
class TaxFxRate:
    rate_date: date
    status: RateStatus
    observation_date: date | None
    usd_per_gbp: Decimal | None
    gbp_per_usd: Decimal | None
    run_id: int | None
    policy_id: str


def _read_health(conn: psycopg.Connection[Any]) -> tuple[bool, int | None, str | None, str | None]:
    control = conn.execute("SELECT enabled FROM tax_fx_source_control").fetchone()
    row = conn.execute(
        f"SELECT r.run_id, r.state, r.policy_id FROM tax_fx_runs r WHERE {_RESPONDED_SQL} "
        "ORDER BY r.run_id DESC LIMIT 1"
    ).fetchone()
    enabled = bool(control and control[0])
    return (enabled, *row) if row else (enabled, None, None, None)


def load_tax_fx_state(conn: psycopg.Connection[Any]) -> TaxFxState:
    """Health, observations, calendar and the latest applied run in one REPEATABLE READ snapshot. ``snapshot_read``
    commits a pending transaction on entry, so the connection must be idle: load before any write transaction, or on
    a separate connection."""
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise RuntimeError("load_tax_fx_state needs an idle connection (snapshot_read would commit open work)")
    with snapshot_read(conn):
        enabled, _health_run_id, health_state, health_policy = _read_health(conn)
        applied = conn.execute("SELECT max(run_id) FROM tax_fx_runs WHERE state = 'applied'").fetchone()
        observations = _stored_observations(conn)
        calendar = _stored_calendar(conn)
    return TaxFxState(
        enabled=enabled,
        health_run_state=health_state,
        health_run_policy_id=health_policy,
        latest_applied_run_id=applied[0] if applied else None,
        observations=observations,
        calendar=calendar,
        observation_dates=tuple(sorted(observations)),
    )


def _gbp_per_usd(usd_per_gbp: Decimal) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 40
        return (Decimal(1) / usd_per_gbp).quantize(GBP_PER_USD_PLACES, rounding=ROUND_HALF_EVEN)


def resolve_tax_fx(state: TaxFxState, rate_date: date) -> TaxFxRate:
    """The rate for a Europe/London date, in resolver order: health, ``FX_FLOOR``, *L*, observation, calendar."""

    def answer(status: RateStatus, observation_date: date | None = None) -> TaxFxRate:
        value = None if observation_date is None else state.observations[observation_date]
        return TaxFxRate(
            rate_date,
            status,
            observation_date,
            value,
            None if value is None else _gbp_per_usd(value),
            state.latest_applied_run_id,
            TAX_FX_POLICY_ID,
        )

    if not state.healthy:
        return answer("unavailable")
    if rate_date < FX_FLOOR:
        return answer("out_of_range")
    accepted_through = state.accepted_through
    if accepted_through is None or rate_date > accepted_through:
        return answer("pending")
    if rate_date in state.observations:
        return answer("published", rate_date)
    if is_working_day(rate_date, state.calendar):
        raise TaxFxInvariantError(f"working day {rate_date} on or before {accepted_through} has no observation")
    index = bisect_left(state.observation_dates, rate_date)
    if index == 0:
        return answer("out_of_range")
    return answer("carried_forward", state.observation_dates[index - 1])


def answer_is_stale(previous: TaxFxRate, state: TaxFxState) -> bool:
    """The consumer contract's staleness rule: re-resolving ``previous.rate_date`` on a current, healthy state gives a
    different ``observation_date``, ``usd_per_gbp`` or ``policy_id``. While the state is unhealthy staleness is not
    decided; callers publish nothing FX-dependent then."""
    if not state.healthy:
        raise RuntimeError("staleness is decided only on a healthy state")
    current = resolve_tax_fx(state, previous.rate_date)
    return (current.observation_date, current.usd_per_gbp, current.policy_id) != (
        previous.observation_date,
        previous.usd_per_gbp,
        previous.policy_id,
    )


def tax_fx_state_is_current(conn: psycopg.Connection[Any], state: TaxFxState) -> bool:
    """Call inside the publishing transaction, at READ COMMITTED, before its writes. Takes the shared run lock (held to
    the caller's commit, so no run can change the answer before the publication commits), then checks that the source
    is still enabled and healthy and that the latest responded run is still the one ``state`` was loaded from."""
    if conn.autocommit and conn.info.transaction_status == TransactionStatus.IDLE:
        raise RuntimeError("tax_fx_state_is_current must run inside the publishing transaction")
    conn.execute("SELECT pg_advisory_xact_lock_shared(%s)", (TAX_FX_LOCK_KEY,))
    isolation = conn.execute("SELECT current_setting('transaction_isolation')").fetchone()
    if isolation is None or isolation[0] != "read committed":
        raise RuntimeError(f"tax_fx_state_is_current needs READ COMMITTED, not {isolation[0] if isolation else None}")
    if not state.healthy:
        return False
    enabled, health_run_id, health_state, health_policy = _read_health(conn)
    has_observations = conn.execute("SELECT EXISTS (SELECT 1 FROM tax_fx_observations)").fetchone()
    return (
        enabled
        and health_state == "applied"
        and health_policy == TAX_FX_POLICY_ID
        and bool(has_observations and has_observations[0])
        and health_run_id == state.latest_applied_run_id
    )


__all__ = [
    "BOOTSTRAP_MAX_LAG",
    "CALENDAR_SEED_SHA256",
    "FX_FLOOR",
    "STALE_ALERT_LAG",
    "TAX_FX_LOCK_KEY",
    "TAX_FX_POLICY_ID",
    "Confirmer",
    "Fetch",
    "Plan",
    "RunReport",
    "TaxFxInvariantError",
    "TaxFxRate",
    "TaxFxRefreshError",
    "TaxFxState",
    "answer_is_stale",
    "beyond_move_bound",
    "feed_stale_origin",
    "fetch_capped",
    "is_working_day",
    "load_tax_fx_state",
    "plan_run",
    "replay_from_evidence",
    "resolve_tax_fx",
    "run_tax_fx_refresh",
    "tax_fx_state_is_current",
    "working_days_after",
]
