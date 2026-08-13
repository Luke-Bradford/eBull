"""Pure unit tests for the shared 13F normaliser (#1567 / #1566).

No DB. Covers the three corrections — PRN/bad-quantity filter, pre-2023
VALUE x1000 cutover, SUM-aggregation of multi-row sub-manager positions —
plus the post-resolution instrument merge. The golden case cross-sources
the real Vanguard Group Q4-2025 13F-HR (SEC EDGAR accession
0000102909-26-000031) trimmed to its 7 AAPL rows.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from app.providers.implementations.sec_13f import (
    ThirteenFHolding,
    dominant_voting_authority,
    parse_infotable,
)
from app.services.thirteen_f_normalise import (
    VALUE_DOLLARS_CUTOVER,
    merge_resolved_by_instrument,
    normalise_13f_holdings,
)

_AAPL = "037833100"
_POST_CUTOVER = datetime(2026, 1, 15, tzinfo=UTC)
_PRE_CUTOVER = datetime(2022, 11, 30, tzinfo=UTC)


def _h(
    *,
    cusip: str = _AAPL,
    value: int = 100,
    shares: int = 1000,
    type_: str = "SH",
    put_call: str | None = None,
    discretion: str | None = "SOLE",
    sole: int = 0,
    shared: int = 0,
    none: int = 0,
) -> ThirteenFHolding:
    return ThirteenFHolding(
        cusip=cusip,
        name_of_issuer="ISSUER",
        title_of_class="COM",
        value_usd=Decimal(value),
        shares_or_principal=Decimal(shares),
        shares_or_principal_type=type_,
        put_call=put_call,  # type: ignore[arg-type]
        investment_discretion=discretion,
        voting_sole=Decimal(sole),
        voting_shared=Decimal(shared),
        voting_none=Decimal(none),
    )


class TestPrnAndQuantityFilter:
    def test_prn_rows_dropped(self) -> None:
        out = normalise_13f_holdings([_h(type_="PRN", shares=500), _h(shares=10)], filed_at=_POST_CUTOVER)
        assert len(out) == 1
        assert out[0].shares_or_principal == Decimal(10)

    def test_prn_case_and_whitespace_insensitive(self) -> None:
        out = normalise_13f_holdings([_h(type_=" prn ")], filed_at=_POST_CUTOVER)
        assert out == []

    def test_sh_blank_default_kept(self) -> None:
        # parse_infotable already defaults blank/unknown Type to 'SH'; a
        # holding that arrives as 'SH' is kept.
        out = normalise_13f_holdings([_h(type_="SH")], filed_at=_POST_CUTOVER)
        assert len(out) == 1

    def test_nonpositive_shares_dropped(self) -> None:
        out = normalise_13f_holdings([_h(shares=0), _h(shares=-5), _h(shares=7)], filed_at=_POST_CUTOVER)
        assert len(out) == 1
        assert out[0].shares_or_principal == Decimal(7)


class TestValueCutover:
    def test_pre_cutover_scaled_x1000(self) -> None:
        out = normalise_13f_holdings([_h(value=42)], filed_at=_PRE_CUTOVER)
        assert out[0].value_usd == Decimal(42_000)

    def test_post_cutover_unscaled(self) -> None:
        out = normalise_13f_holdings([_h(value=42)], filed_at=_POST_CUTOVER)
        assert out[0].value_usd == Decimal(42)

    def test_on_cutover_date_unscaled(self) -> None:
        # cutover is "< date(2023,1,3)"; the cutover day itself is post-regime.
        on = datetime(VALUE_DOLLARS_CUTOVER.year, VALUE_DOLLARS_CUTOVER.month, VALUE_DOLLARS_CUTOVER.day, tzinfo=UTC)
        out = normalise_13f_holdings([_h(value=42)], filed_at=on)
        assert out[0].value_usd == Decimal(42)

    def test_filed_at_none_not_scaled(self) -> None:
        out = normalise_13f_holdings([_h(value=42)], filed_at=None)
        assert out[0].value_usd == Decimal(42)


class TestSumAggregation:
    def test_multi_row_position_summed(self) -> None:
        rows = [_h(shares=100, value=10), _h(shares=250, value=25), _h(shares=3, value=1)]
        out = normalise_13f_holdings(rows, filed_at=_POST_CUTOVER)
        assert len(out) == 1
        assert out[0].shares_or_principal == Decimal(353)
        assert out[0].value_usd == Decimal(36)

    def test_put_call_equity_kept_separate(self) -> None:
        rows = [
            _h(shares=10, put_call=None),
            _h(shares=20, put_call="PUT"),
            _h(shares=30, put_call="CALL"),
            _h(shares=5, put_call="PUT"),
        ]
        out = normalise_13f_holdings(rows, filed_at=_POST_CUTOVER)
        by_exposure = {(o.put_call or "EQUITY"): o.shares_or_principal for o in out}
        assert by_exposure == {"EQUITY": Decimal(10), "PUT": Decimal(25), "CALL": Decimal(30)}

    def test_different_cusips_not_merged(self) -> None:
        out = normalise_13f_holdings(
            [_h(cusip="111111111", shares=1), _h(cusip="222222222", shares=2)], filed_at=_POST_CUTOVER
        )
        assert len(out) == 2

    def test_empty_input(self) -> None:
        assert normalise_13f_holdings([], filed_at=_POST_CUTOVER) == []

    def test_single_row_passthrough_identity(self) -> None:
        only = _h(shares=10)
        out = normalise_13f_holdings([only], filed_at=_POST_CUTOVER)
        assert out[0] is only  # _sum_group short-circuits len==1

    def test_first_seen_order_preserved(self) -> None:
        out = normalise_13f_holdings(
            [_h(cusip="333333333"), _h(cusip="111111111"), _h(cusip="333333333")],
            filed_at=_POST_CUTOVER,
        )
        assert [o.cusip for o in out] == ["333333333", "111111111"]


class TestVotingMerge:
    def test_voting_components_summed_then_dominant_derived(self) -> None:
        # Sum: sole=5, shared=30, none=10 -> dominant SHARED.
        rows = [_h(sole=5, shared=10, none=0), _h(sole=0, shared=20, none=10)]
        out = normalise_13f_holdings(rows, filed_at=_POST_CUTOVER)
        assert (out[0].voting_sole, out[0].voting_shared, out[0].voting_none) == (
            Decimal(5),
            Decimal(30),
            Decimal(10),
        )
        assert dominant_voting_authority(out[0]) == "SHARED"

    def test_discretion_unanimous_kept(self) -> None:
        out = normalise_13f_holdings([_h(discretion="DFND"), _h(discretion="DFND")], filed_at=_POST_CUTOVER)
        assert out[0].investment_discretion == "DFND"

    def test_discretion_mixed_nulled(self) -> None:
        out = normalise_13f_holdings([_h(discretion="SOLE"), _h(discretion="DFND")], filed_at=_POST_CUTOVER)
        assert out[0].investment_discretion is None


class TestMergeResolvedByInstrument:
    def test_same_instrument_two_cusips_summed(self) -> None:
        merged = merge_resolved_by_instrument(
            [(7, _h(cusip="111111111", shares=10)), (7, _h(cusip="222222222", shares=20))]
        )
        assert len(merged) == 1
        assert merged[0][0] == 7
        assert merged[0][1].shares_or_principal == Decimal(30)

    def test_distinct_instruments_kept(self) -> None:
        merged = merge_resolved_by_instrument([(7, _h(shares=10)), (8, _h(shares=20))])
        assert {iid for iid, _ in merged} == {7, 8}

    def test_put_call_exposure_kept_separate_per_instrument(self) -> None:
        merged = merge_resolved_by_instrument([(7, _h(shares=10, put_call=None)), (7, _h(shares=20, put_call="PUT"))])
        assert len(merged) == 2


class TestGoldenVanguard:
    """Cross-source: real Vanguard Group Q4-2025 13F-HR (accession
    0000102909-26-000031), trimmed to its 7 AAPL rows. SEC EDGAR direct
    sum = 1,426,283,914; the single SOLE row the old keep-first kept was
    1,279,051,701 (a 10.3% undercount)."""

    def test_aapl_rows_sum_via_parse_then_normalise(self) -> None:
        xml = (Path(__file__).parent / "fixtures" / "vanguard_13f_aapl_golden.xml").read_text()
        holdings = parse_infotable(xml)
        aapl = [h for h in holdings if h.cusip == _AAPL]
        assert len(aapl) == 7  # EdgarTools surfaces every sub-manager row

        out = normalise_13f_holdings(holdings, filed_at=_POST_CUTOVER)
        aapl_equity = [o for o in out if o.cusip == _AAPL and o.put_call is None]
        assert len(aapl_equity) == 1
        assert aapl_equity[0].shares_or_principal == Decimal(1_426_283_914)
        # Summed voting: none (1.33B) dominates — NOT the kept SOLE row's
        # label. Bulk SQL CASE derives the same from the same summed sums.
        assert dominant_voting_authority(aapl_equity[0]) == "NONE"


@pytest.mark.parametrize(
    ("sole", "shared", "none", "expected"),
    [
        (0, 0, 0, None),
        (10, 5, 5, "SOLE"),
        (5, 5, 0, "SOLE"),  # SOLE wins ties
        (0, 10, 5, "SHARED"),
        (0, 5, 5, "SHARED"),  # SHARED wins tie vs NONE
        (0, 0, 7, "NONE"),
    ],
)
def test_dominant_voting_matches_bulk_sql_case(sole: int, shared: int, none: int, expected: str | None) -> None:
    """Pins the per-filing derivation against the bulk drain's CASE in
    `sec_13f_dataset_ingest._INSERT_FROM_STG_SQL`. Both consume the SUMMED
    components; any drift between this rule and the SQL CASE silently
    splits voting between the two paths."""
    h = _h(sole=sole, shared=shared, none=none)
    assert dominant_voting_authority(h) == expected
