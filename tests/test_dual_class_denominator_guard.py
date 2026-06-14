"""Pure-logic tests for the stale shares-outstanding denominator guard (#1581).

No DB: exercises the pure staleness policy + the ``no_data`` reason
threading directly. The end-to-end (view -> guard -> no_data) path is
covered by an integration test in ``test_ownership_rollup.py``.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.ownership_rollup import (
    _STALE_DENOMINATOR_MAX_AGE_DAYS,
    OwnershipRollup,
    _denominator_too_stale,
    _stale_denominator_banner,
)

_TODAY = date(2026, 6, 14)


class TestDenominatorTooStale:
    @pytest.mark.parametrize(
        ("as_of", "expected"),
        [
            (None, False),  # absence handled by the caller's None/<=0 check, not here
            (date(2026, 3, 31), False),  # last quarter — fresh
            (date(2011, 4, 29), True),  # BRK.B dual-class dimension-only trap
            (_TODAY - timedelta(days=_STALE_DENOMINATOR_MAX_AGE_DAYS), False),  # boundary: exactly 548d
            (_TODAY - timedelta(days=_STALE_DENOMINATOR_MAX_AGE_DAYS + 1), True),  # 549d
            (_TODAY + timedelta(days=30), False),  # future period_end -> not stale (F5)
        ],
    )
    def test_policy(self, as_of: date | None, expected: bool) -> None:
        assert _denominator_too_stale(as_of, _TODAY) is expected


class TestStaleDenominatorBanner:
    def test_banner_names_date_keeps_no_data_state_and_is_cause_agnostic(self) -> None:
        banner = _stale_denominator_banner(date(2011, 4, 29))
        assert banner.state == "no_data"
        assert banner.variant == "error"
        assert "29 Apr 2011" in banner.body  # en-GB short month
        assert "too stale" in banner.body
        # Cause-agnostic: most stale rows are ingest-coverage gaps, NOT the
        # dual-class trap — copy must not claim "multi-class".
        assert "multi-class" not in banner.body.lower()
        # And must NOT tell the operator to trigger a sync (futile for the
        # dual-class case — it re-fetches the same ancient row).
        assert "sync" not in banner.body.lower()


class TestNoDataStaleReason:
    def test_stale_reason_retains_as_of_and_honest_banner(self) -> None:
        rollup = OwnershipRollup.no_data(
            symbol="BRK.B",
            instrument_id=1118,
            reason="stale_denominator",
            stale_as_of=date(2011, 4, 29),
        )
        assert rollup.banner.state == "no_data"
        assert rollup.shares_outstanding is None
        # as_of RETAINED as the FE discriminator (absent nulls it).
        assert rollup.shares_outstanding_as_of == date(2011, 4, 29)
        assert "29 Apr 2011" in rollup.banner.body

    def test_absent_default_nulls_as_of_and_uses_generic_banner(self) -> None:
        rollup = OwnershipRollup.no_data(symbol="X", instrument_id=1)
        assert rollup.banner.state == "no_data"
        assert rollup.shares_outstanding_as_of is None
        assert "not on file" in rollup.banner.body.lower()

    def test_stale_reason_requires_as_of(self) -> None:
        with pytest.raises(ValueError, match="requires stale_as_of"):
            OwnershipRollup.no_data(symbol="X", instrument_id=1, reason="stale_denominator")
