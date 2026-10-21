"""Pure-logic tests for the #3227 13F-duplicate fold (``_fold_13f_duplicates``).

A manager's 13F figure is folded onto a co-filer when the manager's own joint Schedule 13D/G reports
the same block for both. No DB — hand-built ``Holder`` objects, cover rows and a fake connection.
See docs/specs/ownership/2026-10-21-3227-13f-position-scope.md (Acceptance, Tests).
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from app.api.instruments import _rollup_to_response
from app.services.ownership_rollup import (
    BannerCopy,
    CategoryCoverage,
    ConcentrationInfo,
    CoverageReport,
    DroppedSource,
    Holder,
    OwnershipRollup,
    ResidualBlock,
    SharesOutstandingSource,
    SliceCategory,
    SourceTag,
    _CoverRow,
    _fold_13f_duplicates,
    _read_13f_covers,
    _reconcile_cofiler_groups,
    build_rollup_csv,
)

_P = date(2026, 6, 30)
_T = 71_201_875
_FILED = datetime(2026, 2, 24, tzinfo=UTC)
_K = "0001257324"
_M = "0000921669"
_NEW_YORK = timezone(timedelta(hours=-4))


def _h(
    cik: str | None,
    name: str,
    shares: int,
    *,
    source: SourceTag = "form4",
    accession: str = "",
    as_of: date | None = _P,
    dropped: tuple[DroppedSource, ...] = (),
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession or f"acc-{cik}-{source}",
        winning_edgar_url=None,
        as_of_date=as_of,
        filer_type=None,
        dropped_sources=dropped,
    )


def _cover(
    cik: str | None,
    accession: str = "C",
    *,
    filed: datetime = _FILED,
    sole: int | None = 0,
    shared: int | None = _T,
    aggregate: int | None = _T,
) -> _CoverRow:
    return _CoverRow(
        reporter_cik=cik,
        accession_number=accession,
        filed_at=filed,
        sole_dispositive=None if sole is None else Decimal(sole),
        shared_dispositive=None if shared is None else Decimal(shared),
        aggregate=None if aggregate is None else Decimal(aggregate),
    )


def _cats(
    insiders: list[Holder] = (),  # type: ignore[assignment]
    blockholders: list[Holder] = (),  # type: ignore[assignment]
    institutions: list[Holder] = (),  # type: ignore[assignment]
) -> dict[SliceCategory, list[Holder]]:
    return {
        "insiders": list(insiders),
        "blockholders": list(blockholders),
        "institutions": list(institutions),
        "etfs": [],
    }


def _pie(by_category: dict[SliceCategory, list[Holder]]) -> Decimal:
    return sum((h.shares for hs in by_category.values() for h in hs), Decimal(0))


def _k(shares: int = _T, **kw: Any) -> Holder:
    return _h(_K, "Icahn Enterprises G.P. Inc.", shares, accession=kw.pop("accession", "F4K"), **kw)


def _m(shares: int = _T, **kw: Any) -> Holder:
    return _h(_M, "Carl C. Icahn", shares, source=kw.pop("source", "13f"), accession=kw.pop("accession", "13F-M"), **kw)


def _cvi() -> dict[SliceCategory, list[Holder]]:
    """CVI.US on dev, 2026-10-21: Icahn's 13F and Icahn Enterprises G.P.'s Form 4, one block on 13D/A C."""
    return _cats(insiders=[_k()], institutions=[_m()])


def _cvi_covers() -> list[_CoverRow]:
    return [_cover(_M), _cover(_K), _cover("0001034563")]


def _folded(cats: dict[SliceCategory, list[Holder]], covers: list[_CoverRow]) -> bool:
    out, corrections = _fold_13f_duplicates(cats, covers)
    assert (_pie(cats) - _pie(out)) == sum((c.shares_removed for c in corrections), Decimal(0))
    return bool(corrections)


def test_the_cvi_shape_folds_the_13f_onto_the_twin() -> None:
    cats = _cvi()
    out, corrections = _fold_13f_duplicates(cats, _cvi_covers())
    assert out["institutions"] == []
    (k,) = out["insiders"]
    assert k.shares == Decimal(_T)
    assert [(d.filer_cik, d.source, d.accession_number, d.shares) for d in k.dropped_sources] == [
        (_M, "13f", "13F-M", Decimal(_T))
    ]
    (c,) = corrections
    assert (c.kind, c.filer_cik, c.shares_removed, c.source_channel) == (
        "cofiler_group_collapse",
        _K,
        Decimal(_T),
        "13f",
    )
    assert (c.winning_source, c.winning_accession) == ("form4", "F4K")
    assert c.detail == (
        f"13F duplicate counted once (#3227): Carl C. Icahn ({_M}) 13F {_T} for 2026-06-30; on joint Schedule "
        f"13D/G C (filed 2026-02-24) Carl C. Icahn and Icahn Enterprises G.P. Inc. each report dispositive power "
        f"and aggregate amount {_T}. Counted once by policy: overlap on that statement, across dates and in class "
        "scope is not verified."
    )


def test_a_blockholders_twin_and_sole_dispositive_power_fold() -> None:
    cats = _cats(blockholders=[_k(source="13d", accession="C")], institutions=[_m()])
    covers = [_cover(_M, sole=_T, shared=0), _cover(_K, sole=_T, shared=0)]
    assert _folded(cats, covers)


def _with_cofiler_pass(
    cats: dict[SliceCategory, list[Holder]], covers: list[_CoverRow], cofilers: dict[str, frozenset[str]]
) -> tuple[dict[SliceCategory, list[Holder]], list[Any], dict[SliceCategory, list[Holder]]]:
    folded, f13 = _fold_13f_duplicates(cats, covers)
    out, cof = _reconcile_cofiler_groups(folded, cofilers)
    base, _ = _reconcile_cofiler_groups(cats, cofilers)
    return out, [*f13, *cof], base


def test_twin_keeps_its_cofiler_clique() -> None:
    cats = _cvi()
    cats["insiders"].append(_h("0000000777", "Small Co-filer", 1_000, accession="J4"))
    cofilers = {"J4": frozenset({"1257324", "777"})}
    out, corrections, base = _with_cofiler_pass(cats, _cvi_covers(), cofilers)
    assert _pie(out) == _pie(base) - _T
    (k,) = out["insiders"]
    assert k.filer_cik == _K
    assert [(d.filer_cik, d.source) for d in k.dropped_sources] == [(_M, "13f"), ("0000000777", "form4")]
    assert [(c.kind, c.filer_cik) for c in corrections] == [("cofiler_group_collapse", _K)] * 2


def test_twin_losing_its_cofiler_clique_carries_the_13f_to_the_keeper_once() -> None:
    cats = _cvi()
    cats["insiders"].append(_h("0000000777", "Large Co-filer", 100_000_000, accession="J4"))
    cofilers = {"J4": frozenset({"1257324", "777"})}
    out, corrections, base = _with_cofiler_pass(cats, _cvi_covers(), cofilers)
    assert _pie(out) == _pie(base) - _T
    (keeper,) = out["insiders"]
    assert keeper.filer_cik == "0000000777"
    assert [(d.filer_cik, d.source) for d in keeper.dropped_sources] == [(_K, "form4"), (_M, "13f")]
    assert [c.filer_cik for c in corrections] == [_K, "0000000777"]


@pytest.mark.parametrize(
    "covers",
    [
        pytest.param([_cover(_M, shared=_T - 1), _cover(_K)], id="C dispositive sum != T"),
        pytest.param([_cover(_M, aggregate=_T - 1), _cover(_K)], id="C aggregate != T"),
        pytest.param([_cover(_M, sole=None), _cover(_K)], id="C NULL sole dispositive"),
        pytest.param([_cover(_M, shared=None, sole=_T), _cover(_K)], id="C NULL shared dispositive"),
        pytest.param([_cover(_M, aggregate=None), _cover(_K)], id="C NULL aggregate"),
        pytest.param([_cover(_M), _cover(_K, shared=_T - 1)], id="k on C dispositive sum != T"),
        pytest.param([_cover(_M), _cover(_K, aggregate=_T - 1)], id="k on C aggregate != T"),
        pytest.param([_cover(_M), _cover(_K, sole=None)], id="k on C NULL dispositive"),
        pytest.param([_cover(_M), _cover(_K, aggregate=None)], id="k on C NULL aggregate"),
        pytest.param([_cover(_K)], id="no cover row for M"),
        pytest.param([_cover(_M)], id="k not on C"),
        pytest.param([_cover(_M), _cover(_K, "OTHER")], id="k on another accession"),
        pytest.param(
            [_cover(_M, filed=datetime(2026, 7, 1, tzinfo=UTC)), _cover(_K, filed=datetime(2026, 7, 1, tzinfo=UTC))],
            id="C filed at P + 1 day 00:00 UTC",
        ),
        pytest.param(
            [_cover(_M, filed=datetime(2026, 6, 30, 20, tzinfo=_NEW_YORK)), _cover(_K)],
            id="C filed at P + 1 day 00:00 UTC, stamped in another offset",
        ),
        pytest.param(
            [_cover(_M, "A1"), _cover(_K, "A1"), _cover(_M, "A2", shared=_T - 1), _cover(_K, "A2")],
            id="same timestamp, the later accession does not match",
        ),
        pytest.param(
            [
                _cover(_M, "OLD"),
                _cover(_K, "OLD"),
                _cover(_M, "NEW", filed=datetime(2026, 5, 1, tzinfo=UTC), aggregate=_T + 1),
            ],
            id="a newer non-matching cover wins over an older matching one",
        ),
    ],
)
def test_each_refusal_alone(covers: list[_CoverRow]) -> None:
    cats = _cvi()
    out, corrections = _fold_13f_duplicates(cats, covers)
    assert corrections == [] and out == cats


@pytest.mark.parametrize(
    "last",
    [datetime(2026, 6, 30, 23, 59, 59, 999_000, tzinfo=UTC), datetime(2026, 6, 30, 19, 59, 59, tzinfo=_NEW_YORK)],
)
def test_the_last_instant_of_p_is_included(last: datetime) -> None:
    assert _folded(_cvi(), [_cover(_M, filed=last), _cover(_K, filed=last)])


def test_same_timestamp_covers_resolve_to_the_later_accession_when_it_matches() -> None:
    covers = [_cover(_M, "A1", shared=_T - 1), _cover(_K, "A1"), _cover(_M, "A2"), _cover(_K, "A2")]
    assert _folded(_cvi(), covers)


def test_k_total_other_than_t_refuses() -> None:
    cats = _cats(insiders=[_k(), _h(_K, "Icahn Enterprises G.P. Inc.", 5, accession="F4K2")], institutions=[_m()])
    assert not _folded(cats, _cvi_covers())


def test_two_twins_refuse() -> None:
    cats = _cvi()
    cats["blockholders"].append(_h("0001412093", "Icahn Partners", _T, source="13d", accession="C"))
    assert not _folded(cats, [*_cvi_covers(), _cover("0001412093")])


def test_one_twin_for_two_cover_matching_candidates_refuses_both() -> None:
    cats = _cvi()
    cats["institutions"].append(_h("0000000555", "Other Manager", _T, source="13f", accession="13F-O"))
    assert not _folded(cats, [*_cvi_covers(), _cover("0000000555")])


@pytest.mark.parametrize(
    "rival",
    [
        pytest.param(
            [
                _h("0000000555", "Other", _T - 10, source="13f", accession="O1"),
                _h("0000000555", "Other", 10, source="13f", accession="O2", as_of=date(2026, 3, 31)),
            ],
            id="two period ends",
        ),
        pytest.param(
            [
                _h("0000000555", "Other", _T - 10, source="13f", accession="O1"),
                _h("0000000555", "Other", 10, source="13f", accession="O2", as_of=None),
            ],
            id="P and NULL",
        ),
        pytest.param(
            [
                _h("0000000555", "Other", _T - 10, source="13f", accession="O1"),
                _h("0000000555", "Other", 10, source="13d", accession="O2"),
            ],
            id="mixed 13F and 13D",
        ),
    ],
)
def test_an_invalid_candidate_sharing_the_twin_does_not_block(rival: list[Holder]) -> None:
    cats = _cvi()
    cats["institutions"].extend(rival)
    out, corrections = _fold_13f_duplicates(cats, [*_cvi_covers(), _cover("0000000555")])
    assert [c.shares_removed for c in corrections] == [Decimal(_T)]
    assert [h.filer_cik for h in out["institutions"]] == ["0000000555", "0000000555"]


def test_a_candidate_whose_own_cover_fails_does_not_compete() -> None:
    cats = _cvi()
    cats["institutions"].append(_h("0000000555", "Other", _T, source="13f", accession="O1"))
    newer = datetime(2026, 5, 1, tzinfo=UTC)
    covers = [*_cvi_covers(), _cover("0000000555"), _cover("0000000555", "C2", filed=newer, aggregate=1)]
    out, corrections = _fold_13f_duplicates(cats, covers)
    assert [c.shares_removed for c in corrections] == [Decimal(_T)]
    assert [h.filer_cik for h in out["institutions"]] == ["0000000555"]


def test_another_representation_of_the_manager_is_never_a_twin() -> None:
    # "921669" and "0000921669" are two identity keys with one canonical CIK.
    cats = _cats(insiders=[_h("921669", "Carl C. Icahn", _T, accession="F4M")], institutions=[_m()])
    assert not _folded(cats, [_cover(_M), _cover("921669")])


def test_two_candidates_with_different_period_ends() -> None:
    p1 = date(2026, 3, 31)
    cats = _cats(
        insiders=[_k(), _h("0000000888", "Twin Two", 4_000, accession="F4T")],
        institutions=[_m(), _h("0000000555", "Other", 4_000, source="13f", accession="O1", as_of=p1)],
    )
    covers = [
        *_cvi_covers(),
        _cover("0000000555", "C2", filed=datetime(2026, 2, 1, tzinfo=UTC), shared=4_000, aggregate=4_000),
        _cover("0000000888", "C2", filed=datetime(2026, 2, 1, tzinfo=UTC), shared=4_000, aggregate=4_000),
        # After the 2026-03-31 candidate's P: it is not that candidate's cover, though the batched read returns it.
        _cover("0000000555", "C3", filed=datetime(2026, 5, 1, tzinfo=UTC), shared=1, aggregate=1),
    ]
    out, corrections = _fold_13f_duplicates(cats, covers)
    assert sorted(c.shares_removed for c in corrections) == [Decimal(4_000), Decimal(_T)]
    assert out["institutions"] == []

    conn = _FakeConn([])
    _read_13f_covers(conn, 42, cats)  # type: ignore[arg-type]
    assert conn.params == [{"iid": 42, "bound": datetime(2026, 7, 1, tzinfo=UTC)}]


@pytest.mark.parametrize("reporter", ["921669", " 0000921669 ", "\t921669\n", "00921669"])
def test_padded_reporter_ciks_match(reporter: str) -> None:
    assert _folded(_cvi(), [_cover(reporter), _cover(_K)])


@pytest.mark.parametrize("reporter", ["", "   ", "0000000000", None, "92166X", "９２１６６９"])
def test_blank_zero_and_non_numeric_reporter_ciks_never_match(reporter: str | None) -> None:
    assert not _folded(_cvi(), [_cover(reporter), _cover(_K)])


def test_a_non_numeric_cik_never_matches_an_equal_non_numeric_cik() -> None:
    m = _h("ABC", "Odd Manager", _T, source="13f", accession="13F-M")
    k = _h("XYZ", "Odd Twin", _T, accession="F4K")
    assert not _folded(_cats(insiders=[_k()], institutions=[m]), [_cover("ABC"), _cover(_K)])
    assert not _folded(_cats(insiders=[k], institutions=[_m()]), [_cover(_M), _cover("XYZ")])


_EVEN = 2_000_000


def _even_covers() -> list[_CoverRow]:
    return [_cover(_M, shared=_EVEN, aggregate=_EVEN), _cover(_K, shared=_EVEN, aggregate=_EVEN)]


def test_largest_row_tie_break_and_nested_dropped_order() -> None:
    # Equal shares: the lowest accession takes M's rows, whatever the input order.
    shared_entry = DroppedSource("13d", "D1", Decimal(9), None, None, "0000000999", "Nested")
    k_rows = [
        _h(_K, "Icahn Enterprises G.P. Inc.", _EVEN // 2, accession="B"),
        _h(_K, "Icahn Enterprises G.P. Inc.", _EVEN // 2, accession="A", dropped=(shared_entry,)),
    ]
    nested = DroppedSource("13g", "G1", Decimal(7), None, None, "0000000444", "Nested Two")
    m = _m(_EVEN, dropped=(shared_entry, nested))
    out, _ = _fold_13f_duplicates(_cats(insiders=k_rows, institutions=[m]), _even_covers())
    by_acc = {h.winning_accession: h for h in out["insiders"]}
    assert by_acc["B"].dropped_sources == ()
    # k's own entry first, then M's row, then M's nested entries with the duplicate D1 dropped.
    assert [d.accession_number for d in by_acc["A"].dropped_sources] == ["D1", "13F-M", "G1"]


def test_equal_rows_on_one_accession_fold_onto_the_first() -> None:
    k_rows = [
        _h(_K, "Icahn Enterprises G.P. Inc.", _EVEN // 2, accession="A"),
        _h(_K, "Icahn Enterprises G.P. Inc.", _EVEN // 2, accession="A"),
    ]
    out, _ = _fold_13f_duplicates(_cats(insiders=k_rows, institutions=[_m(_EVEN)]), _even_covers())
    assert [len(h.dropped_sources) for h in out["insiders"]] == [1, 0]


def test_the_category_collision_check_still_raises() -> None:
    cats = _cvi()
    cats["blockholders"].append(_m(source="13d"))
    with pytest.raises(ValueError, match="two categories"):
        _fold_13f_duplicates(cats, _cvi_covers())


class _FakeCursor:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        self.conn.params.append(params)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.conn.rows


class _FakeConn:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.params: list[dict[str, Any]] = []

    def cursor(self, **_: Any) -> _FakeCursor:
        return _FakeCursor(self)


def test_no_candidate_means_no_cover_query() -> None:
    conn = _FakeConn([])
    assert _read_13f_covers(conn, 1, _cats(insiders=[_k()], institutions=[_m(source="13g")])) == []  # type: ignore[arg-type]
    assert conn.params == []


def test_the_read_maps_rows_in_column_order() -> None:
    row = (" 0000921669", "C", _FILED, Decimal(0), Decimal(_T), Decimal(_T))
    conn = _FakeConn([row])
    assert _read_13f_covers(conn, 7, _cvi()) == [_CoverRow(*row)]  # type: ignore[arg-type]
    assert conn.params == [{"iid": 7, "bound": datetime(2026, 7, 1, tzinfo=UTC)}]


def _rollup(corrections: tuple[Any, ...]) -> OwnershipRollup:
    return OwnershipRollup(
        symbol="CVI",
        instrument_id=1,
        shares_outstanding=Decimal(100_530_599),
        shares_outstanding_as_of=_P,
        shares_outstanding_source=SharesOutstandingSource(
            accession_number=None, concept=None, form_type=None, edgar_url=None
        ),
        treasury_shares=None,
        treasury_as_of=None,
        slices=(),
        residual=ResidualBlock(
            shares=Decimal(0),
            pct_outstanding=Decimal(0),
            label="Public / unattributed",
            tooltip="",
            oversubscribed=False,
        ),
        concentration=ConcentrationInfo(pct_outstanding_known=Decimal(0), info_chip=""),
        coverage=CoverageReport(state="green", categories={"insiders": CategoryCoverage(0, 0, None, "green")}),
        banner=BannerCopy(state="green", variant="success", headline="", body=""),
        historical_symbols=(),
        computed_at=datetime(2026, 10, 21, tzinfo=UTC),
        corrections_applied=corrections,
    )


def test_two_corrections_on_one_cik_survive_serialisation_and_csv() -> None:
    cats = _cvi()
    cats["insiders"].append(_h("0000000777", "Small Co-filer", 1_000, accession="J4"))
    _, corrections, _ = _with_cofiler_pass(cats, _cvi_covers(), {"J4": frozenset({"1257324", "777"})})
    rollup = _rollup(tuple(corrections))
    response = _rollup_to_response(rollup)
    assert [(c.kind, c.filer_cik, c.shares_removed) for c in response.corrections_applied] == [
        ("cofiler_group_collapse", _K, Decimal(_T)),
        ("cofiler_group_collapse", _K, Decimal(1_000)),
    ]
    rows = list(csv.reader(io.StringIO(build_rollup_csv(rollup))))
    assert [r[3] for r in rows if r[2] == f"__cofiler_group_collapse:{_K}__"] == [str(_T), "1000"]
