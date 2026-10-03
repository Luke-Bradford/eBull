"""#3545: how many NYSE sessions the session rates capture has FULLY covered — slice 2's wake condition.

A session is full when a ``complete`` capture started inside every one of its scheduled ET clock hours:
09-15 on a full day, 09-12 on a half day (the job fires at :37, so 09:37 ... 15:37 / 12:37). Slice 2 (the
recalibration under a new ``COST_MODEL_ID``) may start at >= 20 full sessions. Spec:
``docs/proposals/etl/2026-10-03-3545-session-rate-capture.md`` §Wake condition.

    PYTHONPATH=. uv run python -m scripts.report_3545_session_rate_coverage

Read-only. The denominator comes from ``market_calendar``, never from the captures themselves, so a session
with no capture at all shows as a gap rather than disappearing.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import psycopg

from app.config import settings
from app.services.market_calendar import us_market_status

NY = ZoneInfo("America/New_York")
WAKE_FULL_SESSIONS = 20
FULL_DAY_HOURS = frozenset(range(9, 16))
HALF_DAY_HOURS = frozenset(range(9, 13))

_SQL = "SELECT started_at FROM etoro_session_rate_captures WHERE status = 'complete' ORDER BY started_at"


def required_hours(day: date) -> frozenset[int]:
    status = us_market_status(day)
    if status == "closed":
        return frozenset()
    return HALF_DAY_HOURS if status == "half_day" else FULL_DAY_HOURS


def full_sessions(starts: list[datetime], today: date) -> list[tuple[date, frozenset[int], frozenset[int]]]:
    """Every session from the first capture's day to ``today``: (day, required hours, covered hours)."""
    if not starts:
        return []
    covered: dict[date, set[int]] = defaultdict(set)
    for started_at in starts:
        local = started_at.astimezone(NY)
        covered[local.date()].add(local.hour)
    out = []
    day = min(covered)
    while day <= today:
        required = required_hours(day)
        if required:
            out.append((day, required, frozenset(covered.get(day, set())) & required))
        day += timedelta(days=1)
    return out


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        starts = [row[0] for row in conn.execute(_SQL).fetchall()]
    sessions = full_sessions(starts, datetime.now(UTC).astimezone(NY).date())
    full = 0
    for day, required, covered in sessions:
        missing = sorted(required - covered)
        full += not missing
        print(f"  {day}  {len(covered)}/{len(required)} hours" + (f"   missing ET {missing}" if missing else ""))
    print(f"\ncomplete captures {len(starts):,}   sessions {len(sessions)}   full sessions {full}")
    verdict = "MET" if full >= WAKE_FULL_SESSIONS else "not met"
    print(f"wake (slice 2 recalibration): {verdict} — {full}/{WAKE_FULL_SESSIONS}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
