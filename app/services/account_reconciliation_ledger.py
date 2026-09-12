"""Per-day reconciliation verdicts and the consecutive-green countdown (#2844 clause 3).

*"Reconciliation job green for 5 consecutive days on demo before any live enablement."*

Before this module the official/local verdict was computed on page load by
``account_equity_evidence.load_account_equity_evidence`` and thrown away. Nothing stored a
day, so the countdown had no record to count, no way to fail, and nothing to audit; the
clause could not start.

Spec: ``docs/specs/ops/2026-09-13-account-reconciliation-countdown.md``.

⚠ **Threat model.** This is a control against a broken or drifting pipeline and it is
fail-closed against that. It is not a control against an actor who already holds write
access to the database or this repository -- they can delete the ledger or edit the
constants. Findings of that shape are recorded in the spec as out of model.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

from app.services.account_equity_evidence import (
    RECONCILIATION_RULE_VERSION,
    AccountEquityEvidence,
    load_account_equity_evidence,
)
from app.services.market_calendar import us_market_status

logger = logging.getLogger(__name__)

#: The countdown rule itself, distinct from the TOLERANCE rule it consumes.
#:
#: ⚠ No published rule fixes a broker-reconciliation cadence; searched, none exists, and
#: none is borrowed. That search was already done for the tolerance and recorded at
#: ``RECONCILIATION_RULE_VERSION`` (an earlier draft's citation of SEC Reg NMS Rule 612 was
#: withdrawn as off-point). The same finding holds here: no regulator specifies how a firm
#: counts consecutive days of agreement between two of its own valuations. So the rule is
#: fixed BY CONSTRUCTION, and every constant below carries the measurement it came from.
#:
#: ⚠⚠ Bumping this, or ``RECONCILIATION_RULE_VERSION``, RESETS THE COUNTDOWN TO ZERO. That
#: is the point: it removes "bump the version, then re-verdict only the days that came out
#: red", because a bump invalidates the greens along with them.
COUNTDOWN_RULE_VERSION = "f0-countdown-v1"

#: The clause's own number.
REQUIRED_GREEN_DAYS = 5

#: How long a countdown day has to reach a verdict before a refusal counts as a failure.
#:
#: Measured 2026-09-13 on the full stored population -- ``portfolio_eod_snapshot`` fires at
#: 22:30 UTC but ``MAX(price_daily.price_date)`` only advances at ``orchestrator_full_sync``
#: 03:00 UTC, so it routinely stamps the PREVIOUS session:
#:
#:     select (computed_at at time zone 'UTC')::date - snapshot_date as lag_days, count(*)
#:       from portfolio_eod_snapshots group by 1 order by 1;
#:     -- 0:32  1:6  2:10  3:1  18:1
#:
#: The 18 is the 2026-09-12 recovery burst (the job ran before ``daily_candle_refresh`` and
#: re-stamped an old row). Excluding it the maximum lag is 3; +1 for the countdown job's own
#: next fire gives 4.
MAX_DECISION_LAG_DAYS = 4

#: How stale the newest DUE countdown day may be before the whole streak is void.
#: ``MAX_DECISION_LAG_DAYS`` + a full week of market closure + 1.
MAX_EVIDENCE_AGE_DAYS = 12

#: The widest calendar span ``REQUIRED_GREEN_DAYS`` counted days may cover: 5 countdown days
#: + 2 weekends + a holiday. Without it, five timely greens scattered across five months
#: pass the moment the newest is due, because "consecutive" would mean consecutive in a
#: calendar full of holes.
MAX_STREAK_SPAN_DAYS = 14

#: Window for both the countdown calendar and the job's replay scan.
#: ``MAX_DECISION_LAG_DAYS + MAX_EVIDENCE_AGE_DAYS + MAX_STREAK_SPAN_DAYS``. Nothing older
#: can contribute to a streak, so nothing older is read or re-judged.
CALENDAR_LOOKBACK_DAYS = MAX_DECISION_LAG_DAYS + MAX_EVIDENCE_AGE_DAYS + MAX_STREAK_SPAN_DAYS

#: ⚠ ``portfolio_eod_snapshots`` has NO environment column -- it is the operator's one local
#: book. A ``real`` verdict would therefore silently consume the demo comparand, so the
#: countdown is defined for demo only and refuses anything else rather than returning a
#: number a caller would read as a pass.
COUNTDOWN_ENVIRONMENT: Literal["demo"] = "demo"


class ReconciliationLedgerError(ValueError):
    """The countdown was asked for something it cannot truthfully answer."""


@dataclass(frozen=True)
class ReconciliationDay:
    """One stored verdict, reduced to what the counter reads."""

    snapshot_date: date
    reconciliation_state: Literal["refused", "reconciled", "diverged"]
    comparable: bool
    countdown_rule_version: str
    decided_at: datetime | None

    @property
    def decided_on(self) -> date | None:
        """The UTC date the verdict was reached. ⚠ UTC, not local: ``decided_at`` is
        stored ``timestamptz`` and a naive ``.date()`` on a non-UTC connection timezone
        would shift the lag bound by a day at the boundary."""
        return None if self.decided_at is None else self.decided_at.astimezone(UTC).date()


@dataclass(frozen=True)
class ReconciliationStreak:
    """The countdown, with the evidence a reader needs to explain a zero."""

    green_days: int
    required_days: int
    newest_counted_date: date | None
    #: Why the run stopped where it did -- for the operator panel, never for a decision.
    stop_reason: str | None

    @property
    def green(self) -> bool:
        return self.green_days >= self.required_days


def consecutive_reconciled_days(
    rows: dict[date, ReconciliationDay],
    countdown_days: set[date],
    as_of: date,
) -> ReconciliationStreak:
    """Trailing run of reconciled countdown days. Pure -- reads no database.

    ``countdown_days`` is the calendar, derived independently of ``rows`` (see
    ``load_countdown_calendar``): a countdown day with NO row must break the run, which it
    cannot do if the calendar is read off the ledger.

    The algorithm is the spec's, and every stop closes a named bypass:

    * There is deliberately **no "skip the pending prefix" rule.** An earlier draft skipped
      leading ``comparable = False`` rows as "not yet decided". That is a refusal bypass: a
      PERMANENT refusal -- an undocumented account currency, an unvalued short book, an
      incomplete local valuation -- would sit at the head forever while five old greens kept
      the gate green. A day instead gets exactly ``MAX_DECISION_LAG_DAYS`` to decide, after
      which a refusal is a failure like any other.
    * A day inside the grace window is excluded from COUNTING, but a day already DECIDED
      not-reconciled in there is a known current failure and voids the streak outright.
    * The ``decided_at`` bounds stop replay. Without them a first install could judge thirty
      historical days in one execution and be green immediately. ⚠ The countdown counts days
      of EVIDENCE, not job executions -- but at most ``MAX_DECISION_LAG_DAYS`` days can be
      decided by any one execution, so five green days need at least two.
    * A dead job resets the count with no heartbeat check: ``price_daily`` keeps advancing,
      so new countdown days keep appearing with no row, and the run breaks at the newest.
    """
    ordered = sorted((day for day in countdown_days if day <= as_of), reverse=True)
    due = [day for day in ordered if (as_of - day).days > MAX_DECISION_LAG_DAYS]
    within_grace = [day for day in ordered if (as_of - day).days <= MAX_DECISION_LAG_DAYS]

    for day in within_grace:
        row = rows.get(day)
        if row is not None and row.comparable and row.reconciliation_state != "reconciled":
            return ReconciliationStreak(0, REQUIRED_GREEN_DAYS, None, "divergence_inside_grace_window")

    if not due:
        return ReconciliationStreak(0, REQUIRED_GREEN_DAYS, None, "no_due_countdown_day")
    if (as_of - due[0]).days > MAX_EVIDENCE_AGE_DAYS:
        return ReconciliationStreak(0, REQUIRED_GREEN_DAYS, None, "countdown_calendar_stale")

    counted: list[date] = []
    stop_reason: str | None = None
    for day in due:
        row = rows.get(day)
        if row is None:
            stop_reason = "day_never_recorded"
        elif row.countdown_rule_version != COUNTDOWN_RULE_VERSION:
            stop_reason = "countdown_rule_version_superseded"
        elif not row.comparable:
            stop_reason = "still_refused_past_due"
        elif row.reconciliation_state != "reconciled":
            stop_reason = "diverged"
        elif (decided_on := row.decided_on) is None or decided_on < day:
            # Structurally excluded by the CHECK constraint; kept because the counter must
            # not depend on a constraint it does not itself enforce.
            stop_reason = "verdict_predates_its_day"
        elif (decided_on - day).days > MAX_DECISION_LAG_DAYS:
            stop_reason = "backfilled_not_observed"
        if stop_reason is not None:
            break
        counted.append(day)
        if len(counted) == REQUIRED_GREEN_DAYS:
            break

    if not counted:
        return ReconciliationStreak(0, REQUIRED_GREEN_DAYS, None, stop_reason)
    if (counted[0] - counted[-1]).days > MAX_STREAK_SPAN_DAYS:
        return ReconciliationStreak(0, REQUIRED_GREEN_DAYS, None, "streak_span_too_wide")
    return ReconciliationStreak(len(counted), REQUIRED_GREEN_DAYS, counted[0], stop_reason)


def countdown_calendar(as_of: date) -> set[date]:
    """NYSE sessions in the countdown window. Pure -- reads no database.

    **Source rule: NYSE published holidays and early closings** (nyse.com/markets/
    hours-calendars), already transcribed and cited in ``app.services.market_calendar``.
    Reused rather than re-derived: that module distinguishes NYSE from the US federal
    calendar (Good Friday closed, Columbus/Veterans open) and carries the extraordinary
    closures too.

    ⚠ **Two wrong discriminators were tried first, and the second was only caught by an
    adversarial review reproducing a 0/5 -> 5/5 flip.**

    1. ``EXISTS (SELECT 1 FROM price_daily WHERE price_date = D)``, unrestricted, is true
       for 16 of the 17 stored demo broker days -- every Saturday included -- because ~308
       instruments (crypto) carry weekend bars against ~11k on weekdays.
    2. The same query restricted to CURRENTLY-HELD instruments returns the right 12 of 17,
       but it is a function of today's book. Selling the last crypto position REMOVES past
       weekend sessions from the calendar, and a weekend the job never judged has no ledger
       row to preserve it -- so a sale could delete a missing-day failure and turn 0/5 into
       5/5 with no new evidence. A union with the recorded dates does not save it, because
       the days at risk are precisely the ones with no row.

    A published exchange calendar has neither failure: it is independent of the book, of
    the price corpus, and of what the job has managed to record.

    ⚠ Weekends are therefore never countdown days even when a held crypto marks on them.
    That is conservative in the only direction that matters -- it removes days from the
    count, it cannot add a day that was silently passed.
    """
    floor = as_of - timedelta(days=CALENDAR_LOOKBACK_DAYS)
    return {
        floor + timedelta(days=offset)
        for offset in range((as_of - floor).days + 1)
        if us_market_status(floor + timedelta(days=offset)) != "closed"
    }


def load_reconciliation_days(
    conn: psycopg.Connection[Any], *, environment: str, as_of: date
) -> dict[date, ReconciliationDay]:
    """Stored verdicts for the current tolerance rule, within the countdown window."""
    floor = as_of - timedelta(days=CALENDAR_LOOKBACK_DAYS)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot_date,reconciliation_state,comparable,countdown_rule_version,decided_at
            FROM account_reconciliation_days
            WHERE environment=%(environment)s
              AND reconciliation_rule_version=%(rule_version)s
              AND snapshot_date BETWEEN %(floor)s AND %(as_of)s
            """,
            {
                "environment": environment,
                "rule_version": RECONCILIATION_RULE_VERSION,
                "floor": floor,
                "as_of": as_of,
            },
        )
        return {
            row["snapshot_date"]: ReconciliationDay(
                snapshot_date=row["snapshot_date"],
                reconciliation_state=row["reconciliation_state"],
                comparable=bool(row["comparable"]),
                countdown_rule_version=row["countdown_rule_version"],
                decided_at=row["decided_at"],
            )
            for row in cur.fetchall()
        }


def load_reconciliation_streak(conn: psycopg.Connection[Any], *, as_of: date | None = None) -> ReconciliationStreak:
    """The demo countdown. ⚠ Demo only -- see ``COUNTDOWN_ENVIRONMENT``.

    ⚠ Database errors are NOT caught and turned into a zero. Swallowing an exception inside
    a caller's transaction leaves that transaction aborted, which fails harder and less
    visibly than propagating does.
    """
    today = as_of or datetime.now(UTC).date()
    rows = load_reconciliation_days(conn, environment=COUNTDOWN_ENVIRONMENT, as_of=today)
    return consecutive_reconciled_days(rows, countdown_calendar(today), today)


def pending_reconciliation_dates(conn: psycopg.Connection[Any], *, environment: str, as_of: date) -> tuple[date, ...]:
    """Countdown days inside the window that have no DECIDED verdict yet, oldest first.

    ⚠ ``snapshot_date < as_of`` is load-bearing, not tidiness. A same-UTC-day broker row is
    still MUTABLE: ``record_account_equity_snapshot``'s ``ON CONFLICT`` updates it while
    ``snapshot_date = (now() AT TIME ZONE 'UTC')::date``. Deciding a day whose evidence can
    still change is exactly how a green freezes before the divergent observation lands.

    ⚠⚠ **Filtered through ``countdown_calendar``, and that filter is not cosmetic.** The
    broker snapshot is stamped ``observed_at.date()``, so it writes a row on Saturdays and
    Sundays too -- 5 of the 17 stored demo days. Those can never reconcile: the local
    comparand is a trading session. Recording them would fill the ledger with permanently
    refused non-sessions. Caught by running the job against the real dev corpus, not by a
    test: every weekend row landed ``refused (same_day_local_eod_snapshot_missing)``.

    The filter runs in Python, not SQL, because the NYSE calendar is a published table in
    ``app.services.market_calendar`` and re-expressing it as a predicate over our price
    corpus is exactly the mistake the calendar docstring documents.
    """
    floor = as_of - timedelta(days=CALENDAR_LOOKBACK_DAYS)
    sessions = countdown_calendar(as_of)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.snapshot_date
            FROM (
                SELECT DISTINCT snapshot_date
                FROM broker_account_equity_snapshots
                WHERE environment=%(environment)s
            ) b
            LEFT JOIN account_reconciliation_days d
                   ON d.environment=%(environment)s
                  AND d.reconciliation_rule_version=%(rule_version)s
                  AND d.snapshot_date=b.snapshot_date
            WHERE b.snapshot_date >= %(floor)s
              AND b.snapshot_date < %(as_of)s
              AND coalesce(d.comparable,false) = false
            ORDER BY b.snapshot_date
            """,
            {
                "environment": environment,
                "rule_version": RECONCILIATION_RULE_VERSION,
                "floor": floor,
                "as_of": as_of,
            },
        )
        return tuple(row[0] for row in cur.fetchall() if row[0] in sessions)


def _finite(value: Decimal | None) -> Decimal | None:
    """Drop a non-finite money term rather than let the CHECK constraint reject the row."""
    return value if value is not None and value.is_finite() else None


def record_reconciliation_day(
    conn: psycopg.Connection[Any],
    *,
    environment: str,
    evidence: AccountEquityEvidence,
) -> bool:
    """Store one day's verdict. Returns True if a row was written or upgraded.

    ⚠ A DECIDED row is never touched -- the ``WHERE`` predicate skips it and the table's
    ``BEFORE UPDATE`` trigger would raise if anything else tried. Re-verdicting a decided day
    is a ``RECONCILIATION_RULE_VERSION`` bump, which starts a parallel series and resets the
    countdown; it is not an overwrite.

    ⚠ ``status == "unavailable"`` is never stored: it means no broker day exists, so there is
    no day to record.
    """
    if evidence.status == "unavailable" or evidence.snapshot_date is None:
        return False
    decided = evidence.comparable
    difference = _finite(evidence.difference)
    tolerance = _finite(evidence.tolerance)
    if decided and (difference is None or tolerance is None):
        # The loader's implication says this cannot happen; the CHECK constraint says it
        # must not. Refuse rather than write a row that contradicts one of them.
        raise ReconciliationLedgerError(f"comparable verdict for {evidence.snapshot_date} is missing a money term")
    reasons = list(evidence.incomplete_reasons)
    if not decided and not reasons:
        reasons = ["reconciliation_undecided_without_reason"]
    row = conn.execute(
        """
        INSERT INTO account_reconciliation_days (
            environment,reconciliation_rule_version,snapshot_date,countdown_rule_version,
            reconciliation_state,comparable,difference,tolerance,incomplete_reasons,
            revision_count,decided_at
        ) VALUES (
            %(environment)s,%(rule_version)s,%(snapshot_date)s,%(countdown_version)s,
            %(state)s,%(comparable)s,%(difference)s,%(tolerance)s,%(reasons)s,
            0,CASE WHEN %(comparable)s THEN now() ELSE NULL END
        )
        ON CONFLICT (environment,reconciliation_rule_version,snapshot_date) DO UPDATE SET
            countdown_rule_version=EXCLUDED.countdown_rule_version,
            reconciliation_state=EXCLUDED.reconciliation_state,
            comparable=EXCLUDED.comparable,
            difference=EXCLUDED.difference,
            tolerance=EXCLUDED.tolerance,
            incomplete_reasons=EXCLUDED.incomplete_reasons,
            revision_count=account_reconciliation_days.revision_count + 1,
            decided_at=CASE WHEN EXCLUDED.comparable THEN now() ELSE NULL END
        WHERE account_reconciliation_days.comparable = false
        RETURNING snapshot_date
        """,
        {
            "environment": environment,
            "rule_version": evidence.reconciliation_rule_version,
            "snapshot_date": evidence.snapshot_date,
            "countdown_version": COUNTDOWN_RULE_VERSION,
            "state": evidence.reconciliation_state,
            "comparable": decided,
            "difference": difference,
            "tolerance": tolerance,
            "reasons": reasons,
        },
    ).fetchone()
    return row is not None


def run_reconciliation_check(conn: psycopg.Connection[Any], *, as_of: date | None = None) -> int:
    """Judge every undecided demo countdown day in the window. Returns days recorded.

    ⚠ Oldest-first, **each day in its own transaction with its own ``try``**. A permanent
    refusal -- or an exception -- on the oldest candidate must not starve every newer day
    behind it.

    ⚠⚠ **``conn`` MUST be autocommit.** Under ``autocommit=False`` the
    ``pending_reconciliation_dates`` read above opens an implicit transaction, and every
    ``with conn.transaction()`` below then degrades to a SAVEPOINT rather than a real
    ``BEGIN``/``COMMIT`` -- so a connection loss part-way through discards every verdict
    the run had already "committed". That is a recurring trap in this repo (prevention log:
    "a single pre-loop commit is NOT sufficient when the loop body itself reads on the same
    connection"), and here it is worse than usual: a discarded verdict can miss its own
    ``MAX_DECISION_LAG_DAYS`` window and become permanently uncountable.

    ⚠ **A run in which every candidate raised is a FAILED run, not a healthy no-op.**
    Returning 0 quietly would be indistinguishable from "nothing left to judge", and
    ``_tracked_job`` would record success over a dead pipeline -- the exact class of defect
    the standing retrospective names ("a job that no-ops and reports success is invisible
    to every automated check we have").
    """
    today = as_of or datetime.now(UTC).date()
    recorded = 0
    failed: list[date] = []
    for snapshot_date in pending_reconciliation_dates(conn, environment=COUNTDOWN_ENVIRONMENT, as_of=today):
        try:
            with conn.transaction():
                evidence = load_account_equity_evidence(
                    conn, environment=COUNTDOWN_ENVIRONMENT, snapshot_date=snapshot_date
                )
                if record_reconciliation_day(conn, environment=COUNTDOWN_ENVIRONMENT, evidence=evidence):
                    recorded += 1
        except Exception:
            failed.append(snapshot_date)
            logger.warning(
                "account_reconciliation_check: %s could not be judged; continuing",
                snapshot_date,
                exc_info=True,
            )
    if failed:
        raise ReconciliationLedgerError(
            f"{len(failed)} reconciliation day(s) could not be judged: {', '.join(day.isoformat() for day in failed)}"
        )
    return recorded
