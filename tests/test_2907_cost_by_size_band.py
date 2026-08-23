from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from scripts.verify_2907_cost_by_size_band import (
    PopulationRow,
    build_evidence,
    classify_cost,
    classify_market_cap,
    derive_verdict,
    nearest_rank,
    spread_losses,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "unknown_market_cap"),
        (Decimal("0"), "unknown_market_cap"),
        (Decimal("299999999.99"), "micro"),
        (Decimal("300000000"), "small"),
        (Decimal("1999999999.99"), "small"),
        (Decimal("2000000000"), "mid"),
        (Decimal("9999999999.99"), "mid"),
        (Decimal("10000000000"), "large"),
    ],
)
def test_market_cap_boundaries_are_half_open(value: Decimal | None, expected: str) -> None:
    assert classify_market_cap(value) == expected


def test_nearest_rank_is_discrete_without_interpolation() -> None:
    values = [Decimal(value) for value in ("1", "2", "3", "4")]
    assert nearest_rank(values, Decimal("0.50")) == 2
    assert nearest_rank(values, Decimal("0.75")) == 3
    assert nearest_rank(values, Decimal("0.95")) == 4


def test_spread_loss_charges_both_sides_and_compounds_three_round_trips() -> None:
    one_loss, three_loss = spread_losses(Decimal("1.450"))
    half = Decimal("1.450") / 200
    retention = (1 - half) / (1 + half)
    assert one_loss == 1 - retention
    assert three_loss == 1 - retention**3


def test_cost_classification_uses_multiplicative_break_even_boundaries() -> None:
    robust_return = Decimal("0.203") * Decimal("0.42")
    lenient_return = Decimal("0.203") * Decimal("0.85")
    robust_break_even = robust_return / (1 + robust_return)
    lenient_break_even = lenient_return / (1 + lenient_return)
    epsilon = Decimal("0.000000000001")
    assert classify_cost(robust_break_even - epsilon) == "COST-SURVIVES-ROBUST"
    assert classify_cost(robust_break_even) == "CONTINGENT"
    assert classify_cost(lenient_break_even - epsilon) == "CONTINGENT"
    assert classify_cost(lenient_break_even) == "COST-KILLED"


def test_tail_warning_means_p95_has_a_worse_classification() -> None:
    p75, p95, verdict = derive_verdict(Decimal("7"), Decimal("8"))
    assert p75 == "COST-SURVIVES-ROBUST"
    assert p95 == "CONTINGENT"
    assert verdict == "COST-SURVIVES-ROBUST — TAIL-WARNING"


def _row(instrument_id: int, cap: str | None, price: str | None) -> PopulationRow:
    return PopulationRow(
        instrument_id=instrument_id,
        market_cap_live=Decimal(cap) if cap is not None else None,
        last=Decimal(price) if price is not None else None,
        quoted_at=datetime(2026, 8, 23, tzinfo=UTC) if price is not None else None,
    )


def test_population_is_conserved_across_cartesian_census() -> None:
    rows = (
        _row(1, "100000000", "4"),
        _row(2, "500000000", None),
        _row(3, None, "50"),
        _row(4, "12000000000", "150"),
    )
    evidence = build_evidence(
        rows,
        measured_at=datetime(2026, 8, 24, tzinfo=UTC),
        execution_commit="a" * 40,
        source_sha256={"test": "b" * 64},
    )
    assert sum(evidence.census_cells.values()) == evidence.universe_size == evidence.distinct_ids == 4
    assert evidence.census_cells["unknown_market_cap|priced"] == 1
    assert evidence.census_cells["small|unpriced"] == 1
    assert evidence.bands["micro"].cost_band_counts["<$5"] == 1
    assert evidence.bands["large"].cost_band_counts[">=$100"] == 1
    assert sum(evidence.bands["micro"].cost_band_counts.values()) == 1
    assert sum(evidence.bands["large"].cost_band_counts.values()) == 1


def test_no_priced_microcap_is_data_fail() -> None:
    evidence = build_evidence(
        (_row(1, "100000000", None), _row(2, "12000000000", "150")),
        measured_at=datetime(2026, 8, 24, tzinfo=UTC),
        execution_commit="a" * 40,
        source_sha256={"test": "b" * 64},
    )
    assert evidence.verdict == "DATA-FAIL"
    assert evidence.p75_classification is None
