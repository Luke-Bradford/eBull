"""Pure tests for the fair-value-band payload builder (#3390 slice 4)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from app.api.instruments import build_fair_value_band
from app.services.fair_value_band import METHOD_VERSION, PRICE_STALE_DAYS

TODAY = date(2026, 9, 26)


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "bear_value": Decimal("9.084547"),
        "base_value": Decimal("15.555652"),
        "bull_value": Decimal("21.730000"),
        "quality_status": "medium",
        "reason": "ok",
        "as_of_date": date(2026, 9, 25),
        "ttm_end": date(2026, 8, 1),
        "price_as_of": date(2026, 9, 25),
        "computed_at": datetime(2026, 9, 26, 5, 15, tzinfo=UTC),
        "target_basis": "not_multiclass",
        # GME's stored fvb_v5 basis, trimmed.
        "basis_json": {
            "selected": ["pe", "ps"],
            "multiples": {
                "pe": {"cohort_n": 5, "sic_level": 0, "own_points": 14, "earnings_nonrep": "G3_spiked"},
                "ps": {
                    "cohort_n": 4,
                    "sic_level": 0,
                    "own_points": 17,
                    "base_value": 15.555651713356415,
                    "cohort_screened": False,
                },
            },
            "cross_leg_base_ratio": 1.2,
        },
    }
    row.update(overrides)
    return row


def test_available_band_carries_version_label_and_legs() -> None:
    out = build_fair_value_band("GME", "USD", _row(), today=TODAY)
    assert out.available is True
    assert out.method_version == METHOD_VERSION
    assert "not a validated fair value" in out.label
    assert (out.bear_value, out.base_value, out.bull_value) == (
        Decimal("9.084547"),
        Decimal("15.555652"),
        Decimal("21.730000"),
    )
    assert out.stale is False
    assert out.cross_leg_base_ratio == 1.2
    legs = {leg.multiple: leg for leg in out.legs}
    # pe is in the profile's candidate set but the v5 gate removed it.
    assert legs["ps"].contributed is True and legs["ps"].cohort_screened is False
    assert legs["pe"].contributed is False and legs["pe"].earnings_nonrep == "G3_spiked"
    assert legs["pe"].base_value is None and legs["pe"].cohort_screened is None


def test_dropped_ev_leg_does_not_contribute() -> None:
    basis = {
        "selected": ["pe", "ps", "ev_ebitda"],
        "multiples": {"ev_ebitda": {"cohort_n": 20, "dropped_nonpositive": True}},
    }
    leg = build_fair_value_band("X", "USD", _row(basis_json=basis), today=TODAY).legs[0]
    assert leg.contributed is False and leg.dropped_nonpositive is True


def test_no_row_is_no_band() -> None:
    out = build_fair_value_band("VOD.L", "GBP", None, today=TODAY)
    assert out.available is False
    assert out.reason == "no_band"
    assert out.legs == []


def test_statused_absent_row_keeps_its_reason() -> None:
    out = build_fair_value_band(
        "HD",
        "USD",
        _row(bear_value=None, base_value=None, bull_value=None, quality_status=None, reason="stale_price"),
        today=TODAY,
    )
    assert out.available is False
    assert out.reason == "stale_price"


def test_partial_triple_fails_closed() -> None:
    out = build_fair_value_band("X", "USD", _row(bull_value=None), today=TODAY)
    assert out.available is False
    assert out.reason == "no_band"
    assert out.bear_value is None and out.base_value is None and out.quality_status is None


def test_stale_after_the_bands_own_price_rule() -> None:
    fresh = _row(as_of_date=date.fromordinal(TODAY.toordinal() - PRICE_STALE_DAYS))
    old = _row(as_of_date=date.fromordinal(TODAY.toordinal() - PRICE_STALE_DAYS - 1))
    assert build_fair_value_band("X", "USD", fresh, today=TODAY).stale is False
    assert build_fair_value_band("X", "USD", old, today=TODAY).stale is True
