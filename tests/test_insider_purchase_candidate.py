from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from app.services.insider_purchase_candidate import (
    PurchaseObservation,
    build_research_resolution,
    classify_purchases,
    resolve_research_instrument,
    validate_archive_sequence,
)


def _purchase(year: int, month: int, *, filer: str = "0000000001") -> PurchaseObservation:
    day = date(year, month, 2)
    return PurchaseObservation(
        issuer_cik="0000000010",
        issuer_symbol="ABC",
        accession_number=f"{filer}-{year}-{month}",
        filer_cik=filer,
        transaction_date=day,
        filed_date=day,
        disclosed_value=Decimal("1000"),
    )


def test_classifier_uses_only_three_complete_prior_years() -> None:
    purchases = [_purchase(2022, 2), _purchase(2023, 3), _purchase(2024, 4), _purchase(2025, 5)]
    classified, counts = classify_purchases(purchases)
    assert [item.insider_class for item in classified] == ["opportunistic"]
    assert classified[0].observation.transaction_date.year == 2025
    assert counts["unclassified_missing_prior_purchase_year"] == 3


def test_same_month_intersection_makes_all_current_year_purchases_routine() -> None:
    purchases = [
        _purchase(2021, 6),
        _purchase(2022, 2),
        _purchase(2022, 6),
        _purchase(2023, 6),
        _purchase(2024, 1),
        _purchase(2024, 11),
    ]
    classified, _ = classify_purchases(purchases)
    assert [(item.observation.transaction_date.month, item.insider_class) for item in classified] == [
        (1, "routine"),
        (11, "routine"),
    ]


def test_history_is_at_insider_level_across_issuers() -> None:
    history = [_purchase(2021, 1), _purchase(2022, 2), _purchase(2023, 3)]
    current = _purchase(2024, 4)
    current = PurchaseObservation(**{**current.__dict__, "issuer_cik": "0000000099"})
    classified, _ = classify_purchases([*history, current])
    assert len(classified) == 1
    assert classified[0].insider_class == "opportunistic"


def test_sparse_insider_is_not_mislabelled_opportunistic() -> None:
    classified, counts = classify_purchases([_purchase(2023, 1), _purchase(2025, 2)])
    assert classified == ()
    assert counts["unclassified_missing_prior_purchase_year"] == 2


def test_archive_sequence_requires_pinned_contiguous_history() -> None:
    paths = [
        Path("2019q1_form345.zip"),
        Path("2019q2_form345.zip"),
    ]
    validate_archive_sequence(paths, expected_last_quarter="2019q2")
    with pytest.raises(ValueError, match="contiguous"):
        validate_archive_sequence(paths[:1], expected_last_quarter="2019q2")


def test_exact_cik_symbol_resolves_a_multi_series_issuer_before_unique_fallback() -> None:
    exact = {("0000000001", "AAA"): 1, ("0000000001", "AAB"): 2}
    assert resolve_research_instrument("0000000001", "AAB", exact, {}) == (2, False)
    assert resolve_research_instrument("0000000002", "OLD", exact, {"0000000002": 3}) == (3, True)


def test_duplicate_exact_cik_symbol_mapping_is_refused() -> None:
    exact, unique = build_research_resolution(
        [
            ("0000000001", "AAA", 1),
            ("0000000001", "AAA", 2),
        ]
    )
    assert exact == {}
    assert unique == {}
    assert resolve_research_instrument("0000000001", "AAA", exact, unique) == (None, False)
