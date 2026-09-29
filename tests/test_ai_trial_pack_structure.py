"""#3471 v6-2 — the pack's §16.2 structure levels, setups and §16.5 library (pure)."""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import pytest

from app.services import ai_trial_pack_reader as r
from app.services.ai_trial_decision import exact, measure_atr
from app.services.ai_trial_levels import LEVEL_IDS, SETUP_TYPES, compute_levels
from app.services.ai_trial_pack import canonical_json, indicators
from app.services.ai_trial_plan import qs
from app.services.indicator_series import BarSeries, OHLCVRow


def _series(bars: list[tuple[str, str, str]]) -> BarSeries:
    """(high, low, close) per bar; open = close."""
    start = date(2026, 1, 1)
    rows = tuple(
        OHLCVRow(open=Decimal(c), high=Decimal(h), low=Decimal(lo), close=Decimal(c), volume=1000) for h, lo, c in bars
    )
    return BarSeries(dates=tuple(start + timedelta(days=i) for i in range(len(bars))), rows=rows)


def _breakout() -> BarSeries:
    return _series([("101", "99", "100")] * 25 + [("110", "104", "109")])


def _pack_indicators(series: BarSeries) -> dict[str, float | None]:
    return dict(indicators(series))


def test_library_is_bound_by_sha_and_carried_verbatim() -> None:
    lib = r.load_setup_library()
    raw = r.SETUP_LIBRARY_PATH.read_bytes()
    assert lib["sha256"] == hashlib.sha256(raw).hexdigest() == r.SETUP_LIBRARY_SHA256
    doc = json.loads(raw)
    assert lib["rows"] == doc["rows"] and lib["caveat"] == doc["caveat"]
    assert {(row["setup_type"], row["horizon_days"]) for row in lib["rows"]} == {
        (s, h) for s in SETUP_TYPES for h in (5, 10, 20)
    }


def test_a_different_or_missing_library_refuses(tmp_path: Path) -> None:
    other = tmp_path / "lib.json"
    other.write_bytes(r.SETUP_LIBRARY_PATH.read_bytes() + b"\n")
    with pytest.raises(r.SetupLibraryMismatch, match="sha256"):
        r.load_setup_library(other)
    with pytest.raises(r.SetupLibraryMismatch, match="unreadable"):
        r.load_setup_library(tmp_path / "absent.json")


@pytest.mark.parametrize(
    ("value", "expected"),
    [(Fraction(5491, 100), Decimal("54.91")), (Fraction(1, 8), Decimal("0.125")), (Fraction(1, 3), None)],
)
def test_exact_decimal_never_rounds(value: Fraction, expected: Decimal | None) -> None:
    assert r.exact_decimal(value) == expected


def test_structure_entry_carries_every_id_and_setup_and_rederives() -> None:
    series = _breakout()
    ind = _pack_indicators(series)
    entry = r.structure_entry(series, ind)
    assert entry["indicator_bars"] == len(series)
    assert tuple(entry["levels"]) == LEVEL_IDS
    assert tuple(entry["setups"]) == SETUP_TYPES
    assert entry["setups"]["breakout_donchian20"] == {"detected": True, "inputs_missing": False}

    # O-v6-1 (pack half): the stored JSON alone re-derives every level and its ATR distance.
    stored = json.loads(canonical_json(entry))
    levels = compute_levels(series, indicators=ind)
    atr = measure_atr(ind["atr14"], series.rows[-1]["close"])
    assert atr is not None
    for level_id in LEVEL_IDS:
        level, got = levels[level_id], stored["levels"][level_id]
        if level is None:
            assert got is None
            continue
        assert Fraction(Decimal(got["price"])) == level.price
        assert got["origin_bar"] == level.origin_bar
        assert Decimal(got["atr_distance"]) == qs((level.price - exact(atr.close)) / exact(atr.atr14))
    assert stored["levels"]["donchian20_high"] == {
        "price": "101",
        "atr_distance": str(qs((Fraction(101) - Fraction(109)) / exact(atr.atr14))),
        "origin_bar": 24,
    }


def test_invalid_atr_nulls_every_level() -> None:
    series = _series([("100", "100", "100")] * 30)  # flat: ATR14 = 0, the §6 measurement is invalid
    entry = r.structure_entry(series, _pack_indicators(series))
    assert all(v is None for v in entry["levels"].values())
