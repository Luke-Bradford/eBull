"""Unit tests for the N-CSR retention helpers (#1233 §4.12 / PR8).

Pure-Python helpers — no DB. Covered:

- ``n_csr_retention_cutoff`` returns ``now - 730d`` (UTC).
- Helper normalises non-UTC ``now`` to UTC.
- Helper raises on tz-naive ``now``.
- ``n_csr_within_retention`` boundary inclusive (== cutoff → True;
  cutoff - 1µs → False).
- ``None`` filed_at → False.
- tz-naive filed_at raises.

PR7 N-PORT lesson: tz-naive ``now`` (or local ``datetime.now()``) would
honour the caller's TZ and drift the cutoff by ±1 day on non-UTC dev
hosts; both helpers fail-closed via ValueError.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.services.manifest_parsers.sec_n_csr import (
    N_CSR_RETENTION_DAYS,
    n_csr_retention_cutoff,
    n_csr_within_retention,
)


class TestRetentionCutoff:
    def test_default_now_returns_utc(self) -> None:
        cutoff = n_csr_retention_cutoff()
        assert cutoff.tzinfo is not None
        assert cutoff.tzinfo.utcoffset(cutoff) == timedelta(0)

    def test_exact_730d_offset(self) -> None:
        now = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
        cutoff = n_csr_retention_cutoff(now)
        # 730 days back from 2026-05-20: 2024-05-20 (2024 is leap, but
        # Feb 29 lies before the window so subtracting 365*2 lands on
        # the same MM-DD).
        assert cutoff == datetime(2024, 5, 20, 12, 0, 0, tzinfo=UTC)

    def test_normalises_non_utc_to_utc(self) -> None:
        ny = timezone(timedelta(hours=-5))
        now_ny = datetime(2026, 5, 20, 7, 0, 0, tzinfo=ny)  # = 12:00 UTC
        cutoff = n_csr_retention_cutoff(now_ny)
        assert cutoff == datetime(2024, 5, 20, 12, 0, 0, tzinfo=UTC)

    def test_raises_on_naive_now(self) -> None:
        with pytest.raises(ValueError, match="tz-aware"):
            n_csr_retention_cutoff(datetime(2026, 5, 20, 12, 0, 0))

    def test_constant_equals_730(self) -> None:
        # Drift sentinel — anybody bumping this without updating the
        # spec § + plan + lint guard expectations would trip this.
        assert N_CSR_RETENTION_DAYS == 730


class TestWithinRetention:
    NOW = datetime(2026, 5, 20, 12, 0, 0, tzinfo=UTC)
    CUTOFF = datetime(2024, 5, 20, 12, 0, 0, tzinfo=UTC)

    def test_boundary_inclusive_at_cutoff(self) -> None:
        assert n_csr_within_retention(self.CUTOFF, self.NOW) is True

    def test_just_before_cutoff_rejected(self) -> None:
        just_before = self.CUTOFF - timedelta(microseconds=1)
        assert n_csr_within_retention(just_before, self.NOW) is False

    def test_just_after_cutoff_admitted(self) -> None:
        just_after = self.CUTOFF + timedelta(microseconds=1)
        assert n_csr_within_retention(just_after, self.NOW) is True

    def test_recent_filed_at_admitted(self) -> None:
        recent = self.NOW - timedelta(days=10)
        assert n_csr_within_retention(recent, self.NOW) is True

    def test_far_past_filed_at_rejected(self) -> None:
        far = self.NOW - timedelta(days=10_000)
        assert n_csr_within_retention(far, self.NOW) is False

    def test_none_filed_at_returns_false(self) -> None:
        assert n_csr_within_retention(None, self.NOW) is False

    def test_naive_filed_at_raises(self) -> None:
        naive = datetime(2026, 5, 20, 12, 0, 0)
        with pytest.raises(ValueError, match="tz-aware"):
            n_csr_within_retention(naive, self.NOW)

    def test_non_utc_filed_at_normalised(self) -> None:
        # Filed at 2026-05-20 07:00 NY = 2026-05-20 12:00 UTC — well
        # within the 730d window from 2026-05-20 12:00 UTC.
        ny = timezone(timedelta(hours=-5))
        non_utc = datetime(2026, 5, 20, 7, 0, 0, tzinfo=ny)
        assert n_csr_within_retention(non_utc, self.NOW) is True
