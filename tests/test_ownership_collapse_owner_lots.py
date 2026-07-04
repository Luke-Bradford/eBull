"""#1942 — an owner's additive Section-16 lots collapse to ONE display line.

When one person holds a non-zero ``direct`` and a non-zero ``indirect`` lot,
``_source_rows_and_total`` (correctly) keeps both rows so the total SUMs them
(#905, Form 4 General Instruction 4(b) reports the natures on separate lines).
They then render as two lines for one name in the L1 holder list — a
duplicate-looking row even though the total is right. Item 403 (17 CFR 229.403)
shows one beneficial owner on one line at total beneficial ownership, so
``_build_slice`` collapses the lots to one line at the summed shares and preserves
the split in ``lots`` for the drilldown.

Figure-neutral on ``total_shares`` / ``pct`` / ``dominant_source``; corrects
``filer_count`` (one person = one filer, not one per lot).

Pure-logic — no DB (see test-tiering guidance).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    Holder,
    HolderLot,
    SourceTag,
    _build_slice,
    _collapse_owner_lots,
    _slice_coherence,
)


def _h(
    cik: str | None,
    name: str,
    source: SourceTag,
    shares: str,
    *,
    nature: str | None = None,
    accession: str = "0000000000-00-000000",
    as_of: date = date(2026, 1, 1),
    dropped: tuple = (),
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=None,
        as_of_date=as_of,
        filer_type=None,
        dropped_sources=dropped,
        ownership_nature=nature,
    )


OUT = Decimal("1000000")


def test_two_additive_lots_collapse_to_one_line() -> None:
    rows = _collapse_owner_lots(
        [
            _h("0000000001", "Jane Director", "form4", "300", nature="direct"),
            _h("0000000001", "Jane Director", "form3", "50", nature="indirect"),
        ]
    )
    assert len(rows) == 1
    h = rows[0]
    assert h.shares == Decimal("350")
    # Representative = the max-shares lot (the direct 300 on form4).
    assert h.winning_source == "form4"
    assert {(lot.ownership_nature, lot.shares, lot.source) for lot in h.lots} == {
        ("direct", Decimal("300"), "form4"),
        ("indirect", Decimal("50"), "form3"),
    }


def test_single_lot_owner_passes_through_without_lots() -> None:
    rows = _collapse_owner_lots([_h("1", "Solo", "form4", "500", nature="direct")])
    assert len(rows) == 1
    assert rows[0].lots == ()
    assert rows[0].shares == Decimal("500")


def test_distinct_owners_do_not_merge() -> None:
    # Different CIKs — never merged.
    rows = _collapse_owner_lots(
        [
            _h("1", "Alice", "form4", "300", nature="direct"),
            _h("2", "Bob", "form4", "200", nature="direct"),
        ]
    )
    assert {r.filer_name for r in rows} == {"Alice", "Bob"}
    assert all(r.lots == () for r in rows)
    # Two NULL-CIK owners with distinct names fall back to the name key — no merge.
    rows2 = _collapse_owner_lots(
        [
            _h(None, "Ghost One", "form4", "10", nature="direct"),
            _h(None, "Ghost Two", "form4", "20", nature="direct"),
        ]
    )
    assert {r.filer_name for r in rows2} == {"Ghost One", "Ghost Two"}
    assert all(r.lots == () for r in rows2)


def test_representative_keeps_max_row_provenance() -> None:
    from app.services.ownership_rollup import DroppedSource

    dropped = (
        DroppedSource(source="13f", accession_number="acc-13f", shares=Decimal("9"), as_of_date=None, edgar_url=None),
    )
    rows = _collapse_owner_lots(
        [
            _h("1", "Owner", "form4", "300", nature="direct", accession="big", dropped=dropped),
            _h("1", "Owner", "form3", "50", nature="indirect", accession="small"),
        ]
    )
    assert len(rows) == 1
    assert rows[0].winning_accession == "big"
    assert rows[0].dropped_sources == dropped


def test_non_primary_lot_dropped_sources_are_merged() -> None:
    """A non-primary lot's provenance (a superseded amendment folded within its
    own nature) must survive the collapse, not be discarded with the row."""
    from app.services.ownership_rollup import DroppedSource

    primary_drop = (
        DroppedSource(source="13f", accession_number="p", shares=Decimal("9"), as_of_date=None, edgar_url=None),
    )
    secondary_drop = (
        DroppedSource(source="form4", accession_number="s", shares=Decimal("2"), as_of_date=None, edgar_url=None),
    )
    rows = _collapse_owner_lots(
        [
            _h("1", "Owner", "form4", "300", nature="direct", dropped=primary_drop),
            _h("1", "Owner", "form3", "50", nature="indirect", dropped=secondary_drop),
        ]
    )
    assert len(rows) == 1
    accessions = {d.accession_number for d in rows[0].dropped_sources}
    assert accessions == {"p", "s"}


def test_build_slice_does_not_collapse_funds_slice() -> None:
    """The funds slice is one row per fund_series but distinct series share a
    fund_filer_cik; a CIK-keyed collapse there would wrongly merge them and
    undercount filer_count (Codex ckpt-2 HIGH). Only insiders collapse."""
    s = _build_slice(
        "funds",
        [
            _h("0000102909", "Vanguard 500 Index Fund", "nport", "300"),
            _h("0000102909", "Vanguard Total Stock Market Fund", "nport", "200"),
        ],
        OUT,
    )
    assert s.filer_count == 2  # NOT merged despite the shared CIK
    assert len(s.holders) == 2
    assert all(not h.lots for h in s.holders)


def test_build_slice_collapses_and_counts_owner_once() -> None:
    s = _build_slice(
        "insiders",
        [
            _h("1", "Jane Director", "form4", "300", nature="direct"),
            _h("1", "Jane Director", "form3", "50", nature="indirect"),
            _h("2", "Solo", "form4", "650", nature="direct"),
        ],
        OUT,
    )
    assert s.total_shares == Decimal("1000")  # figure-neutral
    assert s.filer_count == 2  # Jane's two lots count once
    assert [h.filer_name for h in s.holders] == ["Solo", "Jane Director"]
    jane = next(h for h in s.holders if h.filer_name == "Jane Director")
    assert jane.shares == Decimal("350")
    assert len(jane.lots) == 2


def test_collapse_is_figure_neutral_vs_dominant_source() -> None:
    # A big form4 direct lot + a small form3 indirect lot: the form3 shares must
    # NOT be reattributed to form4 for the dominant-source computation.
    holders = [
        _h("1", "Jane", "form4", "300", nature="direct"),
        _h("1", "Jane", "form3", "50", nature="indirect"),
        _h("2", "Other", "form3", "400", nature="direct"),
    ]
    s = _build_slice("insiders", holders, OUT)
    # form3 shares = 50 + 400 = 450 > form4 300 → form3 dominates, unchanged by collapse.
    assert s.dominant_source == "form3"
    assert s.total_shares == Decimal("750")


def test_slice_coherence_includes_lot_as_of_dates() -> None:
    # Direct lot in Q1, indirect lot in Q3 of one owner → the envelope spans both
    # quarters even though the representative row carries only one date.
    h = Holder(
        filer_cik="1",
        filer_name="Jane",
        shares=Decimal("350"),
        pct_outstanding=Decimal(0),
        winning_source="form4",
        winning_accession="a",
        winning_edgar_url=None,
        as_of_date=date(2026, 1, 15),
        filer_type=None,
        dropped_sources=(),
        lots=(
            HolderLot(
                ownership_nature="direct",
                shares=Decimal("300"),
                source="form4",
                accession_number="a",
                edgar_url=None,
                as_of_date=date(2026, 1, 15),
            ),
            HolderLot(
                ownership_nature="indirect",
                shares=Decimal("50"),
                source="form3",
                accession_number="b",
                edgar_url=None,
                as_of_date=date(2026, 8, 20),
            ),
        ),
    )
    as_of_min, as_of_max, distinct_quarters, mixed_period = _slice_coherence([h])
    assert as_of_min == date(2026, 1, 15)
    assert as_of_max == date(2026, 8, 20)
    assert distinct_quarters == 2
    assert mixed_period is True


def test_lots_survive_api_serialization() -> None:
    """The API response builder must carry ``lots`` through (it lists holder
    kwargs explicitly and would otherwise drop the field)."""
    from datetime import UTC, datetime

    from app.api.instruments import _rollup_to_response
    from app.services.ownership_rollup import (
        BannerCopy,
        CategoryCoverage,
        ConcentrationInfo,
        CoverageReport,
        OwnershipRollup,
        ResidualBlock,
        SharesOutstandingSource,
    )

    collapsed = _build_slice(
        "insiders",
        [
            _h("1", "Jane Director", "form4", "300", nature="direct"),
            _h("1", "Jane Director", "form3", "50", nature="indirect"),
        ],
        OUT,
    )
    rollup = OwnershipRollup(
        symbol="TEST",
        instrument_id=1,
        shares_outstanding=OUT,
        shares_outstanding_as_of=date(2026, 3, 31),
        shares_outstanding_source=SharesOutstandingSource(
            accession_number="a", concept="EntityCommonStockSharesOutstanding", form_type="10-Q", edgar_url=None
        ),
        treasury_shares=None,
        treasury_as_of=None,
        slices=(collapsed,),
        residual=ResidualBlock(
            shares=Decimal(0), pct_outstanding=Decimal(0), label="", tooltip="", oversubscribed=False
        ),
        concentration=ConcentrationInfo(pct_outstanding_known=Decimal(0), info_chip=""),
        coverage=CoverageReport(state="green", categories={"insiders": CategoryCoverage(0, 0, None, "green")}),
        banner=BannerCopy(state="green", variant="success", headline="", body=""),
        historical_symbols=(),
        computed_at=datetime(2026, 5, 3, tzinfo=UTC),
    )
    resp = _rollup_to_response(rollup)
    jane = next(h for h in resp.slices[0].holders if h.filer_name == "Jane Director")
    assert jane.shares == Decimal("350")
    assert {(lot.ownership_nature, lot.shares, lot.source) for lot in jane.lots} == {
        ("direct", Decimal("300"), "form4"),
        ("indirect", Decimal("50"), "form3"),
    }
