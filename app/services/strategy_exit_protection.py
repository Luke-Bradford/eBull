"""Is every engine-held position's fixed exit actually in place? (#3284 item 4b)

Item 4 of #3284 is *"if the stop cannot be set after N attempts, that is an alert-class
event, not a silent carry-on"*.  Item 4a made a refused repair DURABLE
(``strategy_position_repair_streaks``, ``sql/408``); this module is the alarm that reads
it.  No new channel is owed — ``/system/status`` already aggregates anything exposing
``is_alerting``, and ``strategy_scan_freshness`` is the worked pattern this follows (a
``Literal`` status vocabulary, an ``is_alerting`` property, and a deliberately
NON-alerting status for the expected case, with the reasoning written down).

⛔ **THE SECOND CONJUNCT IN THE HANDOFF DESIGN IS WRONG, AND IT IS WRONG IN THE
SUPPRESSING DIRECTION.**  #3284's 2026-09-21 research comment specified *"alerting when a
position is currently unprotected (``is_no_stop_loss``) AND its streak has reached the
class threshold"*.  Re-falsified before implementing, per working-order 3c, and it does
not survive:

* ``record_repair_visit(..., state="rejected")`` is reached ONLY from inside
  ``if intent.has_gap:`` (``strategy_position_manager.py``), and the resume-path recorder
  re-tests the same ``_exit_intent`` before counting a ``pending`` as a refusal.  **A
  non-zero streak therefore already means "a gap was observed on the last visit and was
  not closed".**  The streak IS the unprotected witness; ``is_no_stop_loss`` adds no
  information to it.
* Worse, it removes some.  On the core arm ``stop_gap = position.is_no_stop_loss or not
  core_exit_level_satisfied(observed, desired)`` — a disjunction.  ``core_exit_levels`` is
  a pure function of the CURRENT weighted entry, and eToro re-weights ``open_price`` on an
  ADD, so a position whose stop is present but computed from the previous entry has
  ``is_no_stop_loss = False`` while its repair refuses on every visit.  The conjunct
  silences the alarm in exactly the case item 2 of this ticket exists for.
* And it would have read the wrong table.  ``is_no_stop_loss`` lives in
  ``broker_positions``, written by the ``portfolio_sync`` job.  Measured on dev
  2026-09-21T23:33Z: every row's ``updated_at`` is ``21:21:33Z`` — 2h12m stale, because
  the jobs child that writes it is mid-drain.  The streak table is written by the paper
  cycle's own fresh ``get_portfolio`` observation, so gating on ``broker_positions``
  would import a second staleness source into a live-safety alarm for no gain.

So the "currently" conjunct here is **ownership liveness**, not a broker flag: the verdict
enumerates ``strategy_position_ownership`` rows with ``status='active'`` and left-joins
the streak.  That is what stops a released position's frozen streak alerting forever —
which is the real hazard the handoff design was reaching for.

**N, split by refusal class.**  No published formulation exists for a retry budget on a
broker edit, so per "source-rule before design" it is fixed BY CONSTRUCTION, and the rule
is stated as a rule rather than as a table of guesses:

    N = 2 consecutive visits, EXCEPT where retrying is proven incapable of changing the
    answer, which gets N = 1.

Two is the smallest count that distinguishes a transient from a condition: one refusal is
a single event, a second CONSECUTIVE one means the explanation was re-tested and failed
again.  The exception is granted per reason code, on evidence from the refusing branch:

* ``broker_fixed_exit_edit_not_allowed`` → **N = 1.**  The branch tests
  ``arms[0].allow_edit_stop_loss is not True`` — a broker capability verdict about the
  instrument, not a market condition.  Retrying cannot change it, so a budget would only
  delay the alert.
* ``fixed_exit_quote_unsafe`` → **N = 2.**  Transient by construction: a quote older than
  ``CORE_EXIT_MAX_QUOTE_AGE_SECONDS``, or a desired stop at/above the current bid.
* ``broker_edit_pending`` → **N = 2.**  This is the class item 4a introduced and the
  design comment predates (a submitted edit observed NOT in effect).  Transient in shape —
  the next visit either confirms it landed or counts it again — so it takes the rule, not
  the exception.
* **Anything else → N = 2**, the rule.  ⚠ Deliberately NOT "unknown reasons never alert":
  a refusal code nobody has classified, on a position observed unprotected, is precisely
  the silent carry-on this ticket forbids.  It is also not N = 1 — we have no evidence
  that retrying is futile for a class we have not seen, and N = 1 on an unseen transient
  would ship a false alarm.  The reason code is carried verbatim in the response so the
  operator sees what it actually was.

⚠ Deliberately a count of VISITS, not wall-clock — ``_OWNED_BATCH_SQL`` rotates the owned
batch by five-minute slot, so visit rate is a function of sleeve size and a time bound
would silently mean different things at different sizes.  ``sql/408`` carries that
derivation in full; it is not re-decided here.

⚠ There is NO "and the cycle is still running" conjunct, for the same structural reason
``strategy_scan_freshness`` has no "while prices are fresh" one.  A dead paper cycle
freezes every streak where it stands, which ``check_job_health`` already reports; adding a
staleness test here would build a second, weaker detector for a condition that is already
covered, and it would fire on the healthy case where a position is simply not due a visit
this slot.  ``last_checked_at`` is carried on every entry instead, so the operator can see
the age of the evidence without a second alarm claiming to interpret it.

Refs #2437.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg

logger = logging.getLogger(__name__)

ExitProtectionStatus = Literal[
    #: The position was observed carrying both levels on its last visit.
    "ok",
    #: An active ownership with no streak row at all — the fixed-exit arm has not
    #: evaluated it yet.  ⚠⚠ Non-alerting, and that is deliberate: a position opened
    #: seconds ago legitimately has no visit, and `_derive_overall_status`' own docstring
    #: already settled this shape for `check_job_health` (*"jobs with last_status is None
    #: are deliberately NOT treated as degraded on their own — a fresh deploy would
    #: otherwise always report degraded"*).  `never_scanned` in `strategy_scan_freshness`
    #: is the same call.  The state is REPORTED, it just does not claim a fault.
    "never_checked",
    #: Refusals are accruing but the class budget is not spent.  ⚠⚠ Non-alerting on
    #: purpose — this is the expected transient (one stale quote, one edit in flight),
    #: and an alarm that fires on it would ship with a documented "ignore this" attached,
    #: which is strictly worse than no alarm because it still costs attention and
    #: occupies the slot a working detector would have (the `retired` precedent).
    "repairing",
    #: The budget is spent: the position is unprotected and the repair is not fixing it.
    #: This is the whole point of item 4.
    "unrepairable",
    #: The read failed.  Alerting — an unreadable safety net is not a healthy one.
    "error",
]

_ALERTING_STATUSES: frozenset[str] = frozenset({"unrepairable", "error"})

#: The rule.  See the module docstring: the smallest count that distinguishes a transient
#: from a condition.  Applies to every reason code without a proven-futile exception,
#: INCLUDING ones this module has never seen.
DEFAULT_REFUSAL_BUDGET = 2

#: The exception, granted only where retrying is proven incapable of changing the answer.
#: ⚠ Keyed on `PositionManagerResult.reason_code` strings; pinned against the manager's
#: own literals by a test rather than by an import, because `strategy_position_manager`
#: imports the 4a recorder and a second edge would close a cycle.
REFUSAL_BUDGET_BY_REASON: dict[str, int] = {
    # `arms[0].allow_edit_stop_loss is not True` — a capability verdict about the
    # instrument, re-read from the same eligibility payload on every visit.
    "broker_fixed_exit_edit_not_allowed": 1,
}


def refusal_budget_for(reason: str | None) -> int:
    """Consecutive refusals this class is allowed before the entry alerts."""
    if reason is None:
        return DEFAULT_REFUSAL_BUDGET
    return REFUSAL_BUDGET_BY_REASON.get(reason, DEFAULT_REFUSAL_BUDGET)


@dataclass(frozen=True)
class OwnedRepairStreak:
    """One active ownership left-joined to its streak row — the reader's whole output.

    ``consecutive_refusals`` is ``None`` when no streak row exists, which is a different
    fact from zero: zero means "visited and found protected", ``None`` means "not visited".
    Collapsing them would make a never-visited position report ``ok``.
    """

    ownership_id: int
    strategy_trade_id: int
    broker_position_id: int
    consecutive_refusals: int | None
    first_refused_at: datetime | None
    last_refusal_reason: str | None
    last_checked_at: datetime | None


@dataclass(frozen=True)
class ExitProtection:
    """One position's verdict, carrying the arithmetic that produced it.

    The budget travels with the count for the same reason ``StrategyScanFreshness``
    carries ``max_lag_bars``: an operator seeing ``unrepairable`` needs to know whether 1
    of 1 or 3 of 2 produced it, without opening a psql session.
    """

    ownership_id: int
    strategy_trade_id: int
    broker_position_id: int
    status: ExitProtectionStatus
    consecutive_refusals: int
    refusal_budget: int
    last_refusal_reason: str | None
    first_refused_at: datetime | None
    last_checked_at: datetime | None
    detail: str | None = None

    @property
    def is_alerting(self) -> bool:
        return self.status in _ALERTING_STATUSES


def assess_exit_protection(streaks: Iterable[OwnedRepairStreak]) -> list[ExitProtection]:
    """Pure verdict for every ACTIVE ownership handed in.

    Returns one entry per ownership, always — a position with no verdict is a position
    nothing reports on, which is the condition item 4a was built to end.
    """
    results: list[ExitProtection] = []
    for streak in sorted(streaks, key=lambda s: s.ownership_id):
        budget = refusal_budget_for(streak.last_refusal_reason)
        refusals = streak.consecutive_refusals

        def _verdict(
            status: ExitProtectionStatus,
            *,
            count: int,
            detail: str | None = None,
            _streak: OwnedRepairStreak = streak,
            _budget: int = budget,
        ) -> ExitProtection:
            return ExitProtection(
                ownership_id=_streak.ownership_id,
                strategy_trade_id=_streak.strategy_trade_id,
                broker_position_id=_streak.broker_position_id,
                status=status,
                consecutive_refusals=count,
                refusal_budget=_budget,
                last_refusal_reason=_streak.last_refusal_reason,
                first_refused_at=_streak.first_refused_at,
                last_checked_at=_streak.last_checked_at,
                detail=detail,
            )

        if refusals is None:
            results.append(_verdict("never_checked", count=0, detail="no fixed-exit repair visit recorded yet"))
        elif refusals <= 0:
            results.append(_verdict("ok", count=refusals))
        elif refusals < budget:
            results.append(_verdict("repairing", count=refusals))
        else:
            results.append(_verdict("unrepairable", count=refusals))
    return results


# Bounded by OPEN POSITIONS, not by time — one row per active ownership, and
# `strategy_position_repair_streaks` is keyed on `ownership_id`.  `/system/status` is
# polled, so a scan whose cost grew with history would be a real problem here; this one
# cannot, because released ownerships and closed positions are both outside the
# predicate.  `idx_strategy_position_one_active_trade` is the partial index on
# `status='active'`.
#
# ⚠ LEFT JOIN, not INNER: an active ownership with no streak row is the `never_checked`
# state, and an inner join would silently drop exactly the position nothing has looked at.
_OWNED_STREAKS_SQL = """
SELECT o.ownership_id,
       o.strategy_trade_id,
       o.broker_position_id,
       s.consecutive_refusals,
       s.first_refused_at,
       s.last_refusal_reason,
       s.last_checked_at
FROM strategy_position_ownership o
LEFT JOIN strategy_position_repair_streaks s ON s.ownership_id = o.ownership_id
WHERE o.status = 'active'
ORDER BY o.ownership_id
"""


def read_exit_protection_inputs(conn: psycopg.Connection[Any]) -> list[OwnedRepairStreak]:
    """The measured inputs, so the verdict itself stays pure and table-testable."""
    rows = conn.execute(_OWNED_STREAKS_SQL).fetchall()
    return [
        OwnedRepairStreak(
            ownership_id=row[0],
            strategy_trade_id=row[1],
            broker_position_id=row[2],
            consecutive_refusals=row[3],
            first_refused_at=row[4],
            last_refusal_reason=row[5],
            last_checked_at=row[6],
        )
        for row in rows
    ]


def check_exit_protection(conn: psycopg.Connection[Any]) -> list[ExitProtection]:
    """Read the inputs and return one verdict per active ownership.

    Never raises: a reader failure yields one ``error`` row, so a probe failure cannot
    503 the whole status endpoint.

    ⚠ ``conn.transaction()`` is the containment, and a bare ``try/except`` is NOT.
    ``get_conn`` hands out a NON-autocommit pooled connection, so a failed query leaves
    Postgres' transaction ABORTED; catching the exception does not clear that, and the
    next statement on the same connection raises ``InFailedSqlTransaction``.  Verbatim
    the reasoning in ``check_scan_freshness``, which learned it from Codex at checkpoint
    2 — in ``get_system_status`` the next statement sits OUTSIDE the handler's try, so
    the "contained" error row becomes an HTTP 500.

    ⚠ ``detail`` says "see server logs" and NEVER carries ``exc``.  The prevention log's
    2026 entry on ``/system/status`` is exactly this: driver and SQL error text echoed
    verbatim into a 200 response body reachable by any bearer-token holder.
    """
    try:
        with conn.transaction():
            streaks = read_exit_protection_inputs(conn)
    except Exception:
        logger.exception("check_exit_protection: failed to read owned repair streaks")
        return [
            ExitProtection(
                ownership_id=0,
                strategy_trade_id=0,
                broker_position_id=0,
                status="error",
                consecutive_refusals=0,
                refusal_budget=DEFAULT_REFUSAL_BUDGET,
                last_refusal_reason=None,
                first_refused_at=None,
                last_checked_at=None,
                detail="exit protection query failed (see server logs)",
            )
        ]
    return assess_exit_protection(streaks)


def alerting_exit_protection(entries: Sequence[ExitProtection]) -> list[ExitProtection]:
    """The entries an operator must act on. Exists so callers cannot re-derive the rule."""
    return [entry for entry in entries if entry.is_alerting]


__all__ = [
    "DEFAULT_REFUSAL_BUDGET",
    "REFUSAL_BUDGET_BY_REASON",
    "ExitProtection",
    "ExitProtectionStatus",
    "OwnedRepairStreak",
    "alerting_exit_protection",
    "assess_exit_protection",
    "check_exit_protection",
    "read_exit_protection_inputs",
    "refusal_budget_for",
]
