"""#3545 slice 1: when the session rates capture may run (pure — no database)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from app.services.session_rate_capture import session_open
from app.workers.scheduler import JOB_ETORO_SESSION_RATES_CAPTURE, SCHEDULED_JOBS
from scripts.report_3545_session_rate_coverage import full_sessions


def _utc(y: int, m: int, d: int, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(y, m, d, hh, mm, ss, tzinfo=UTC)


@pytest.mark.parametrize(
    ("instant", "expected"),
    [
        # EDT (UTC-4): Monday 2026-10-05.
        (_utc(2026, 10, 5, 13, 29, 59), False),  # 09:29:59 ET
        (_utc(2026, 10, 5, 13, 30), True),  # 09:30 ET
        (_utc(2026, 10, 5, 13, 37), True),  # the first :37 fire
        (_utc(2026, 10, 5, 19, 59, 59), True),  # 15:59:59 ET
        (_utc(2026, 10, 5, 20, 0), False),  # 16:00 ET
        (_utc(2026, 10, 5, 20, 37), False),  # the :37 after the close
        # EST (UTC-5): Monday 2026-12-07.
        (_utc(2026, 12, 7, 14, 29, 59), False),  # 09:29:59 ET
        (_utc(2026, 12, 7, 14, 30), True),  # 09:30 ET
        (_utc(2026, 12, 7, 13, 37), False),  # 08:37 ET, the pre-open :37
        (_utc(2026, 12, 7, 20, 59, 59), True),  # 15:59:59 ET
        (_utc(2026, 12, 7, 21, 0), False),  # 16:00 ET
        # Half day: Friday 2026-11-27 (day after Thanksgiving), 13:00 ET close, EST.
        (_utc(2026, 11, 27, 17, 59, 59), True),  # 12:59:59 ET
        (_utc(2026, 11, 27, 18, 0), False),  # 13:00 ET
        # Weekend and a full holiday (Thanksgiving 2026-11-26).
        (_utc(2026, 10, 3, 15, 37), False),
        (_utc(2026, 11, 26, 15, 37), False),
    ],
)
def test_the_session_is_open_only_inside_the_regular_session(instant: datetime, expected: bool) -> None:
    assert session_open(instant) is expected


def test_the_job_fires_hourly_at_37_with_no_catch_up() -> None:
    job = next(j for j in SCHEDULED_JOBS if j.name == JOB_ETORO_SESSION_RATES_CAPTURE)
    assert (job.cadence.kind, job.cadence.minute) == ("hourly", 37)
    assert job.source == "etoro_crowd"
    assert not job.catch_up_on_boot
    assert not job.rearm_on_lost_fire


def test_coverage_counts_a_session_only_when_every_scheduled_hour_has_a_capture() -> None:
    # Monday 2026-10-05 (EDT): all seven :37 fires. Tuesday: the 15:37 fire lost. Wednesday: no capture.
    starts = [_utc(2026, 10, 5, h, 37) for h in range(13, 20)] + [_utc(2026, 10, 6, h, 37) for h in range(13, 19)]
    rows = full_sessions(starts, date(2026, 10, 7))
    assert [(d.isoformat(), len(req), len(cov)) for d, req, cov in rows] == [
        ("2026-10-05", 7, 7),
        ("2026-10-06", 7, 6),
        ("2026-10-07", 7, 0),
    ]
    # Half day 2026-11-27: four fires (09:37-12:37 ET, EST) make it full.
    half = full_sessions([_utc(2026, 11, 27, h, 37) for h in range(14, 18)], date(2026, 11, 27))
    assert [(len(req), len(cov)) for _d, req, cov in half] == [(4, 4)]
