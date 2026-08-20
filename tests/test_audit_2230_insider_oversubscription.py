"""Pure-logic tests for the #2230 insider-oversubscription partition.

The partition is what the ticket's verdict rests on — it is the arithmetic that prices
#2230's own deemed-attribution mechanism against the staleness alternative on one cohort —
so it is table-tested rather than left to the audit run's output.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.ownership_rollup import Holder
from scripts.audit_2230_insider_oversubscription import _years_before, main, partition_insiders


def _holder(
    shares: str,
    *,
    cik: str | None = None,
    name: str = "Somebody",
    as_of: date | None = date(2026, 1, 1),
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source="form4",
        winning_accession="0000000000-00-000000",
        winning_edgar_url=None,
        as_of_date=as_of,
        filer_type=None,
        dropped_sources=(),
    )


def test_single_holder_over_is_flagged_when_one_holder_exceeds_outstanding() -> None:
    holders = [_holder("150"), _holder("10")]
    out = partition_insiders(holders, Decimal(100), date(2026, 1, 1))
    assert out["single_holder_over"] is True
    assert out["top_holder_shares"] == "150"


def test_single_holder_over_is_false_when_only_the_sum_exceeds_outstanding() -> None:
    """The whole point of the split: a wedge over 100% made of small holders is an
    additivity question; one made of a single 150%-of-float row is not."""
    holders = [_holder("60", cik="A"), _holder("60", cik="B")]
    out = partition_insiders(holders, Decimal(100), date(2026, 1, 1))
    assert out["single_holder_over"] is False


def test_equal_value_cluster_excess_counts_identities_minus_one() -> None:
    holders = [_holder("40", cik=c) for c in ("A", "B", "C")]
    out = partition_insiders(holders, Decimal(100), date(2026, 1, 1))
    assert out["equal_value_clusters"] == 1
    assert out["max_cluster_members"] == 3
    assert out["cluster_excess"] == "80"
    # 120 total - 80 duplicated = 40 <= 100
    assert out["clears_if_clusters_folded"] is True


def test_two_rows_from_the_SAME_identity_are_not_a_cluster() -> None:
    """A cluster is ≥2 distinct IDENTITIES at one value. One holder's two lots at the same
    value are not a deemed double-count, and folding them would delete a real lot."""
    holders = [_holder("60", cik="A"), _holder("60", cik="A")]
    out = partition_insiders(holders, Decimal(100), date(2026, 1, 1))
    assert out["equal_value_clusters"] == 0
    assert out["cluster_excess"] == "0"
    assert out["clears_if_clusters_folded"] is False


def test_cluster_identity_falls_back_to_name_when_cik_is_null() -> None:
    holders = [_holder("40", cik=None, name="Alice"), _holder("40", cik=None, name="Bob")]
    out = partition_insiders(holders, Decimal(100), date(2026, 1, 1))
    assert out["equal_value_clusters"] == 1
    assert out["max_cluster_members"] == 2


def test_zero_share_rows_never_seed_a_cluster() -> None:
    holders = [_holder("0", cik="A"), _holder("0", cik="B"), _holder("150", cik="C")]
    out = partition_insiders(holders, Decimal(100), date(2026, 1, 1))
    assert out["equal_value_clusters"] == 0


def test_staleness_arm_drops_holders_older_than_the_anchor_by_more_than_the_bound() -> None:
    anchor = date(2026, 1, 1)
    holders = [
        _holder("30", cik="A", as_of=date(2025, 6, 1)),  # inside 2y
        _holder("90", cik="B", as_of=date(2018, 6, 1)),  # outside 2y, inside nothing
    ]
    out = partition_insiders(holders, Decimal(100), anchor)
    assert out["total_within_2y"] == "30"
    assert out["clears_if_bounded_2y"] is True
    assert out["total_within_5y"] == "30"


def test_staleness_arm_keeps_a_holder_with_no_as_of_date() -> None:
    """Fail toward the status quo: a NULL as-of is not evidence of staleness, so the arm
    must not silently remove it — that would overstate the axis it exists to size."""
    anchor = date(2026, 1, 1)
    holders = [_holder("150", cik="A", as_of=None)]
    out = partition_insiders(holders, Decimal(100), anchor)
    assert out["total_within_2y"] == "150"
    assert out["clears_if_bounded_2y"] is False
    assert out["holders_without_as_of"] == 1


def test_no_anchor_leaves_every_staleness_arm_at_the_full_total() -> None:
    holders = [_holder("150", cik="A", as_of=date(1999, 1, 1))]
    out = partition_insiders(holders, Decimal(100), None)
    assert out["total_within_2y"] == "150"
    assert out["total_within_5y"] == "150"


def test_no_denominator_yields_no_ratio_and_no_clearance_claims() -> None:
    holders = [_holder("150", cik="A")]
    for outstanding in (None, Decimal(0)):
        out = partition_insiders(holders, outstanding, date(2026, 1, 1))
        assert out["insiders_ratio"] is None
        assert out["single_holder_over"] is False
        assert out["clears_if_clusters_folded"] is False
        assert out["clears_if_bounded_2y"] is False


def test_the_cutoff_is_calendar_years_not_365_day_blocks() -> None:
    """`timedelta(days=365 * years)` drifts one day per leap day in the span, moving the
    boundary the whole classification is scored against (Codex checkpoint 2)."""
    assert _years_before(date(2026, 3, 1), 2) == date(2024, 3, 1)
    assert _years_before(date(2026, 3, 1), 5) == date(2021, 3, 1)


def test_a_29_february_anchor_clamps_to_the_28th() -> None:
    assert _years_before(date(2024, 2, 29), 2) == date(2022, 2, 28)
    assert _years_before(date(2024, 2, 29), 4) == date(2020, 2, 29)


def test_a_holding_exactly_on_the_calendar_boundary_is_kept() -> None:
    anchor = date(2026, 3, 1)
    holders = [_holder("150", cik="A", as_of=date(2024, 3, 1))]
    out = partition_insiders(holders, Decimal(100), anchor)
    assert out["total_within_2y"] == "150"


def test_denominator_under_1m_is_flagged_independently_of_the_wedge() -> None:
    out = partition_insiders([_holder("10")], Decimal(999_999), date(2026, 1, 1))
    assert out["denominator_under_1m"] is True
    assert out["single_holder_over"] is False


@pytest.mark.parametrize(
    "argv",
    [
        ["--shard", "5", "--shards", "2", "--out", "/tmp/never-written.jsonl"],
        ["--shard", "-1", "--shards", "2", "--out", "/tmp/never-written.jsonl"],
        ["--shard", "0", "--shards", "0", "--out", "/tmp/never-written.jsonl"],
    ],
)
def test_an_out_of_range_shard_is_an_error_not_an_empty_run(argv: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    """An out-of-range shard selects nothing, so the census would exit 0 with an empty
    output file — indistinguishable from a clean population. This script exists to avoid
    making exactly that mistake, so the guard is pinned. Validation runs BEFORE any
    connection, which is why this test needs no DB."""
    monkeypatch.setattr("sys.argv", ["audit", *argv])
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 2
