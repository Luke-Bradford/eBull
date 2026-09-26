"""Pure tests for the Reg SHO short-volume day builder (#3390)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from app.api.instruments import (
    REGSHO_CAVEATS,
    REGSHO_IDENTITY_COLLISION,
    build_short_volume_days,
)
from app.providers.implementations.finra_regsho import CONSOLIDATED_PREFIX, PREFIXES


def _row(d: date, short: str, total: str, market: str = "B,Q,N") -> dict[str, Any]:
    return {
        "trade_date": d,
        "market": market,
        "short_volume": Decimal(short),
        "short_exempt_volume": Decimal("0"),
        "total_volume": Decimal(total),
    }


def test_days_are_newest_first_with_share() -> None:
    rows = [
        _row(date(2026, 9, 24), "3047092.251603", "5048085.931547"),
        _row(date(2026, 9, 25), "3701066.408585", "6533946.477449"),
    ]
    days, withheld = build_short_volume_days(rows, collision_in_history=False)
    assert [d.trade_date for d in days] == [date(2026, 9, 25), date(2026, 9, 24)]
    assert days[0].short_volume_share is not None
    # GME, CNMSshvol20260925.txt: 3701066.408585 / 6533946.477449.
    assert round(days[0].short_volume_share, 4) == Decimal("0.5664")
    assert withheld is None


def test_zero_total_volume_gives_no_share() -> None:
    days, _ = build_short_volume_days([_row(date(2026, 9, 25), "0", "0")], collision_in_history=False)
    assert days[0].short_volume_share is None


def test_facility_union_is_passed_through_not_used_to_select() -> None:
    days, _ = build_short_volume_days([_row(date(2026, 9, 25), "1", "4", market="Q,N")], collision_in_history=False)
    assert days[0].facilities == "Q,N"
    assert days[0].short_volume_share == Decimal("0.25")


def test_two_rows_on_one_date_withhold_every_day() -> None:
    # TpC and TPC in one file both resolve to TPC (#3437).
    rows = [
        _row(date(2026, 9, 24), "90085", "331859"),
        _row(date(2026, 9, 24), "11873", "44640", market="Q"),
        _row(date(2026, 9, 23), "100", "200"),
    ]
    days, withheld = build_short_volume_days(rows, collision_in_history=False)
    assert days == []
    assert withheld == REGSHO_IDENTITY_COLLISION


def test_collision_outside_the_window_withholds_a_clean_looking_window() -> None:
    # Same-market collisions collapse to one row, so the window alone cannot show them.
    days, withheld = build_short_volume_days([_row(date(2026, 9, 25), "1", "2")], collision_in_history=True)
    assert days == []
    assert withheld == REGSHO_IDENTITY_COLLISION


def test_consolidated_prefix_is_a_fetched_prefix() -> None:
    assert CONSOLIDATED_PREFIX in PREFIXES


def test_caveats_state_it_is_not_short_interest() -> None:
    assert any("short interest" in c for c in REGSHO_CAVEATS)
