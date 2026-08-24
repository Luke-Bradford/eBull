from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from scripts.census_2900_sec_cover_identity import Submission
from scripts.census_2900_sec_share_counts import _share_pair, share_census


def test_same_accession_share_pair_uses_period_and_nearest_prior_year() -> None:
    submission = Submission(
        accession="0000000001-23-000001",
        cik="0000000001",
        form="10-K",
        accepted=datetime(2023, 2, 1, 12),
        period="20221231",
        instance="test.xml",
    )
    outcome, pair = _share_pair(
        submission,
        {
            datetime(2022, 12, 31).date(): frozenset({Decimal("120")}),
            datetime(2021, 12, 31).date(): frozenset({Decimal("100")}),
            datetime(2020, 12, 31).date(): frozenset({Decimal("90")}),
        },
    )

    assert outcome == "complete_pair"
    assert pair is not None
    assert pair["current_shares"] == "120"
    assert pair["prior_shares"] == "100"


def test_same_accession_share_pair_refuses_conflicting_current_values() -> None:
    submission = Submission(
        accession="0000000001-23-000001",
        cik="0000000001",
        form="10-K",
        accepted=datetime(2023, 2, 1, 12),
        period="20221231",
        instance="test.xml",
    )

    outcome, pair = _share_pair(
        submission,
        {datetime(2022, 12, 31).date(): frozenset({Decimal("120"), Decimal("121")})},
    )

    assert outcome == "conflicting_current_fiscal_year"
    assert pair is None


def test_census_falls_back_when_latest_amendment_omits_unchanged_facts() -> None:
    original = Submission(
        accession="0000000001-23-000001",
        cik="0000000001",
        form="10-K",
        accepted=datetime(2023, 2, 1, 12),
        period="20221231",
        instance="original.xml",
    )
    amendment = Submission(
        accession="0000000001-23-000002",
        cik="0000000001",
        form="10-K/A",
        accepted=datetime(2023, 3, 1, 12),
        period="20221231",
        instance="amendment.xml",
    )
    cutoff = datetime(2023, 6, 30, 16)
    report = share_census(
        {cutoff: (amendment,)},
        {original.accession: original, amendment.accession: amendment},
        {
            original.accession: {
                datetime(2022, 12, 31).date(): frozenset({Decimal("120")}),
                datetime(2021, 12, 31).date(): frozenset({Decimal("100")}),
            }
        },
    )

    assert report["formations"][cutoff.isoformat()]["counts"] == {
        "fallback_complete_pair": 1,
        "latest_missing_current_fiscal_year": 1,
        "population_ciks": 1,
    }
