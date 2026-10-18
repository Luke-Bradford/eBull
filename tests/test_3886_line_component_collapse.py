"""Pure-logic tests for the control-group collapse at Table I line grain (#3886).

A #3227 summed Form 4 observation hides the one line that equals another holder's block, so
#1652's exact-``shares`` bucketing stops counting the block once. ``_reconcile_insider_line_components``
buckets by each summed observation's line balances instead. No DB — hand-built ``Holder`` objects.
See docs/specs/ownership/2026-10-18-3886-control-group-line-components.md.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    DroppedSource,
    Holder,
    SourceTag,
    _line_components,
    _reconcile_insider_line_components,
)

_P = date(2026, 6, 12)
_DUGGAN = "0001055919"
_ZANGANEH = "0001507650"


def _h(
    cik: str | None,
    name: str,
    shares: int,
    *,
    nature: str | None = "direct",
    lines: tuple[int, ...] = (),
    source: SourceTag = "form4",
    accession: str | None = None,
) -> Holder:
    acc = accession or f"acc-{cik}-{source}"
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=acc,
        winning_edgar_url=None,
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
        ownership_nature=nature,
        nature_from_table_i=source in ("form4", "form3"),
        line_components=tuple(Decimal(v) for v in lines),
    )


def _total(holders: list[Holder]) -> Decimal:
    return sum((h.shares for h in holders), Decimal(0))


def _smmt() -> list[Holder]:
    """SMMT 2026-06-12, as stored after #3887: each spouse reports the household's lines."""
    return [
        _h(_DUGGAN, "DUGGAN ROBERT W", 573_883_879, accession="0001055919-26-000007"),
        _h(
            _DUGGAN,
            "DUGGAN ROBERT W",
            36_131_930,
            nature="indirect",
            lines=(25_824_474, 10_199_776, 76_680, 31_000),
            accession="0001055919-26-000007",
        ),
        _h(
            _ZANGANEH,
            "Zanganeh Mahkam",
            609_984_809,
            nature="indirect",
            lines=(573_883_879, 25_824_474, 10_199_776, 76_680),
            accession="0001507650-26-000007",
        ),
        _h(_ZANGANEH, "Zanganeh Mahkam", 31_000, accession="0001507650-26-000007"),
    ]


def test_spouses_reporting_the_same_lines_count_each_block_once() -> None:
    """SMMT: the 573,883,879 block and both trusts count once; the sub-1M lines stay doubled."""
    survivors, blockholders, corrections = _reconcile_insider_line_components(_smmt(), [])
    assert blockholders == []
    assert [c.shares_removed for c in corrections] == [
        Decimal(573_883_879),
        Decimal(25_824_474),
        Decimal(10_199_776),
    ]
    assert all(c.kind == "insider_control_group_collapse" for c in corrections)
    # Household lines once (610,015,809) plus the two sub-floor lines both spouses report.
    assert _total(survivors) == Decimal(610_015_809) + Decimal(76_680) + Decimal(31_000)
    # Every row survives: a line member keeps its row with the line subtracted.
    assert len(survivors) == 4


def test_the_attested_direct_holder_keeps_the_block() -> None:
    """Duggan's ``direct`` 573,883,879 is the holder of record (#2385 clause 1); Zanganeh's
    "By Spouse" line is what is subtracted, so her other lines still count."""
    survivors, _, corrections = _reconcile_insider_line_components(_smmt(), [])
    assert corrections[0].filer_cik == _DUGGAN
    by_key = {(h.filer_cik, h.ownership_nature): h.shares for h in survivors}
    assert by_key[(_DUGGAN, "direct")] == Decimal(573_883_879)
    assert by_key[(_ZANGANEH, "indirect")] == Decimal(36_100_930)


def test_a_whole_13d_member_is_consumed_and_the_line_member_kept_intact() -> None:
    """TNYA: The Column Group's 13D 49,313,559 is one line of Goeddel's summed ``indirect``."""
    goeddel = _h("0001", "GOEDDEL DAVID V", 58_713_849, nature="indirect", lines=(49_313_559, 4_985_570, 4_414_720))
    fund = _h("0002", "The Column Group Opportunity III, LP", 49_313_559, nature=None, source="13d")
    survivors, blockholders, corrections = _reconcile_insider_line_components([goeddel], [fund])
    assert blockholders == []
    assert [h.shares for h in survivors] == [Decimal(58_713_849)]
    assert survivors[0].dropped_sources[-1].filer_cik == "0002"
    assert corrections[0].shares_removed == Decimal(49_313_559)


def test_a_line_member_can_reach_zero_and_keeps_its_row() -> None:
    """Every line of one spouse matches the other's: the row stays, at zero, so the identity
    stays in the owner-once grouping."""
    a = _h("0001", "Spouse A", 3_333_337, nature="indirect", lines=(2_222_223, 1_111_114))
    b = _h("0002", "Spouse B", 3_333_337 + 5, nature="indirect", lines=(2_222_223, 1_111_114, 5))
    survivors, _, corrections = _reconcile_insider_line_components([a, b], [])
    assert len(survivors) == 2
    assert len(corrections) == 2
    # Neither attests ``direct``, so the incumbent tie-break keeps the higher CIK's row whole.
    assert {h.filer_cik: h.shares for h in survivors} == {"0001": Decimal(0), "0002": Decimal(3_333_342)}


def test_no_bucket_returns_inputs_unchanged() -> None:
    """No line passes the value proxies (round, below the floor, or one CIK only): the pass is
    the identity, which is what lets the A/B skip its control arm there."""
    survivors = [
        _h("0001", "Round", 3_000_000, nature="indirect", lines=(2_000_000, 1_000_000)),
        _h("0002", "Round twin", 2_000_000),
        _h("0003", "Small", 900_001, nature="indirect", lines=(800_001, 100_000)),
        _h("0004", "Small twin", 800_001),
        _h("0005", "Self", 5_555_559, nature="indirect", lines=(4_444_447, 1_111_112)),
        _h("0005", "Self", 4_444_447),
    ]
    blockholders = [_h("0009", "13D", 7_777_771, nature=None, source="13d")]
    out_s, out_b, corrections = _reconcile_insider_line_components(list(survivors), list(blockholders))
    assert corrections == []
    assert out_s == survivors
    assert out_b == blockholders


def test_line_components_require_two_lines_that_add_up() -> None:
    t3, h4 = ("T", Decimal(3), True, True), ("H", Decimal(4), True, True)
    assert _line_components([t3, h4], Decimal(7)) == (Decimal(4), Decimal(3))
    assert _line_components([t3, h4], Decimal(4)) == ()  # the latest line, not a sum
    assert _line_components([("T", Decimal(7), True, True)], Decimal(7)) == ()
    assert _line_components(None, Decimal(7)) == ()


def test_line_components_find_a_vehicle_sum_kept_beside_a_refused_fold() -> None:
    """A mixed fold that refuses keeps the ``I`` vehicle sum, which excludes the holding."""
    lines = [
        ("T", Decimal(5_000_000), True, True),
        ("T", Decimal(802_468), True, True),
        ("H", Decimal(100), True, True),
    ]
    assert _line_components(lines, Decimal(5_802_468)) == (Decimal(5_000_000), Decimal(802_468))


def test_line_components_skip_a_scope_no_writer_sums() -> None:
    """Codex ckpt-2: a two-title unit refuses the mixed fold, but its transactions alone still
    form the ``I`` vehicle sum the writer kept."""
    lines = [
        ("T", Decimal(5_000_000), True, False),
        ("T", Decimal(802_468), True, False),
        ("H", Decimal(9), True, False),
    ]
    assert _line_components(lines, Decimal(5_802_477)) == ()
    assert _line_components(lines, Decimal(5_802_468)) == (Decimal(5_000_000), Decimal(802_468))
    unsummable = [("T", Decimal(5_000_000), False, False), ("T", Decimal(802_468), False, False)]
    assert _line_components(unsummable, Decimal(5_802_468)) == ()


def test_line_components_refuse_a_repeated_balance() -> None:
    assert (
        _line_components(
            [("H", Decimal(2_222_223), True, True), ("H", Decimal(2_222_223), True, True)], Decimal(4_444_446)
        )
        == ()
    )


def test_a_repeat_outside_the_chosen_sum_does_not_refuse_it() -> None:
    """Codex ckpt-1 r2: a holding repeating a transaction final must not block the vehicle sum."""
    lines = [
        ("T", Decimal(2_345_679), True, True),
        ("T", Decimal(654_322), True, True),
        ("H", Decimal(2_345_679), True, True),
    ]
    assert _line_components(lines, Decimal(3_000_001)) == (Decimal(2_345_679), Decimal(654_322))


def test_a_consumed_whole_member_hands_its_provenance_to_the_keeper() -> None:
    goeddel = _h("0001", "GOEDDEL DAVID V", 58_713_849, nature="indirect", lines=(49_313_559, 4_985_570, 4_414_720))
    fund = replace(
        _h("0002", "Fund B", 49_313_559, nature=None, source="13d"),
        dropped_sources=(
            DroppedSource(
                source="13d",
                accession_number="acc-0003-13d",
                shares=Decimal(49_313_559),
                as_of_date=_P,
                edgar_url=None,
                filer_cik="0003",
                filer_name="Fund C",
            ),
        ),
    )
    survivors, _, _ = _reconcile_insider_line_components([goeddel], [fund])
    assert {d.filer_cik for d in survivors[0].dropped_sources} == {"0002", "0003"}


def test_a_subtracted_rows_components_still_sum_to_its_shares() -> None:
    survivors, _, _ = _reconcile_insider_line_components(_smmt(), [])
    for h in survivors:
        if h.line_components:
            assert sum(h.line_components, Decimal(0)) == h.shares


def test_a_whole_member_whose_removal_releases_another_channel_refuses_the_bucket() -> None:
    """#2230's hazard: removing B's only Section 16 row would release its 13F into institutions."""
    a = _h("0009", "Principal", 3_000_001, nature="indirect", lines=(2_345_679, 654_322))
    # ``indirect``, so no Table I direct holder outranks the incumbent: A keeps, B would be removed.
    b = _h("0002", "Fund", 2_345_679, nature="indirect")
    b_13f = _h("0002", "Fund", 90_000_000, nature=None, source="13f")
    survivors, _, corrections = _reconcile_insider_line_components([a, b, b_13f], [])
    assert corrections == []
    assert survivors == [a, b, b_13f]


def test_a_line_member_with_a_competing_13d_stays_the_keeper() -> None:
    """Demoting A's lot would be undone by owner-once's MAX with A's 13D restatement, so the
    selector's #2785 fence must still see that 13D (Codex ckpt-1 finding 6)."""
    a = _h("0009", "A", 3_000_001, nature="indirect", lines=(2_345_679, 654_322), accession="acc-a")
    b = _h("0001", "B", 2_345_679 + 123_456, lines=(2_345_679, 123_456), accession="acc-b")
    a_13d = _h("0009", "A", 3_000_001, nature=None, source="13d", accession="acc-a13d")
    survivors, blockholders, corrections = _reconcile_insider_line_components([a, b], [a_13d])
    assert corrections[0].filer_cik == "0009"
    assert {h.filer_cik: h.shares for h in survivors} == {"0009": Decimal(3_000_001), "0001": Decimal(123_456)}
    assert blockholders == [a_13d]
