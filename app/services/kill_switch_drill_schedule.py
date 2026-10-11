"""When the scheduled kill-switch drill is due, and which occurrences went undrilled (#3614 slice 3).

Spec: ``docs/specs/ops/2026-10-06-3614-kill-drill-time-to-flat.md`` § Running it and § Paging.
Stdlib only: the operator alert watch imports it from a launchd one-shot, and the
scheduler registers the job's cadence from the same constants.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Final

#: The monthly slot, UTC. A choice, not a rule: outside the US session, before
#: ``execute_approved_orders`` (06:30) and off the ``:15``/``:30`` dev connection peaks.
DRILL_DAY: Final = 3
DRILL_HOUR: Final = 6
DRILL_MINUTE: Final = 12

#: Slice 3's deploy. Overdue notices count occurrences from here, and drill notices
#: cover events started from here; the slice-2 acceptance runs before it were reviewed
#: on #3614 by the session that ran them.
DRILL_WATCH_FROM: Final = datetime(2026, 10, 12, tzinfo=UTC)

#: An occurrence is satisfied by any drill, scheduled or manual, started within this
#: long of it. Constructed: one day lets a catch-up fire after an overnight outage.
DRILL_OCCURRENCE_GRACE: Final = timedelta(hours=24)


def _slot(year: int, month: int) -> datetime:
    return datetime(year, month, DRILL_DAY, DRILL_HOUR, DRILL_MINUTE, tzinfo=UTC)


def occurrence_at_or_before(t: datetime) -> datetime:
    """The latest slot at or before ``t`` (timezone-aware)."""
    t = t.astimezone(UTC)
    slot = _slot(t.year, t.month)
    if slot <= t:
        return slot
    return _slot(t.year - 1, 12) if t.month == 1 else _slot(t.year, t.month - 1)


def occurrences_between(start: datetime, end: datetime) -> list[datetime]:
    """Every slot ``o`` with ``start <= o <= end``, oldest first."""
    out: list[datetime] = []
    o = occurrence_at_or_before(end)
    while o >= start:
        out.append(o)
        o = occurrence_at_or_before(o - timedelta(microseconds=1))
    return out[::-1]


def overdue_occurrences(started_at: Iterable[datetime], *, now: datetime) -> list[datetime]:
    """Slots since ``DRILL_WATCH_FROM`` whose grace has ended with no drill started in ``[o, o + grace)``."""
    starts = list(started_at)
    return [
        o
        for o in occurrences_between(DRILL_WATCH_FROM, now - DRILL_OCCURRENCE_GRACE)
        if not any(o <= s < o + DRILL_OCCURRENCE_GRACE for s in starts)
    ]
