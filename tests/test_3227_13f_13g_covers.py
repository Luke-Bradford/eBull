"""Pure-logic tests for the 13G extension of the #3227 13F-duplicate fold (``_fold_13f_duplicates``).

A Schedule 13G block carries no reporting-person CIK, so a manager's 13F figure is folded onto its
co-filer through the 13G's filer of record plus one name match. No DB — hand-built ``Holder``
objects, cover rows, filer observations and a fake connection.
See docs/specs/ownership/2026-10-22-3227-13f-13g-covers.md (Acceptance, Tests).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.services.ownership_rollup import (
    CorrectionApplied,
    Holder,
    SliceCategory,
    SourceTag,
    _CoverRow,
    _fold_13f_duplicates,
    _read_13g_filers,
)

_P = date(2026, 6, 30)
_T = 84_587_035
_FILED = datetime(2026, 5, 15, tzinfo=UTC)
# SARO on dev, 2026-10-22: Carlyle Group Inc. files the 13G/A and the 13F; one of the ten blocks is
# Carlyle Partners VII S1 Holdings II, L.P., which has a Form 4 of T.
_CARLYLE = "0001527166"
_CARLYLE_NAME = "Carlyle Group Inc."
_FUND = "0002035691"
_FUND_NAME = "Carlyle Partners VII S1 Holdings II, L.P."
# WAY on dev: Derby LuxCo files the 13G; EQT Fund Management is a block on it and files the 13F.
_DERBY = "0001998443"
_DERBY_NAME = "Derby LuxCo S.a r.l."
_EQT = "0001834239"
_EQT_NAME = "EQT Fund Management S.a r.l."
_OTHER = "0000000777"


def _h(cik: str | None, name: str, shares: int = _T, *, source: SourceTag = "form4", accession: str = "") -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession or f"acc-{cik}-{source}-{name}",
        winning_edgar_url=None,
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
    )


def _13f(cik: str, name: str, shares: int = _T) -> Holder:
    return _h(cik, name, shares, source="13f", accession=f"13F-{cik}")


def _block(
    name: str,
    accession: str = "G",
    *,
    filed: datetime = _FILED,
    t: int = _T,
    aggregate: int | None = None,
    cik: str | None = None,
) -> _CoverRow:
    """A 13G block reporting sole 0, shared ``t`` and aggregate ``t`` (or ``aggregate``)."""
    return _CoverRow(
        reporter_cik=cik,
        accession_number=accession,
        filed_at=filed,
        sole_dispositive=Decimal(0),
        shared_dispositive=Decimal(t),
        aggregate=Decimal(t if aggregate is None else aggregate),
        reporter_name=name,
        submission_type="SCHEDULE 13G/A",
    )


def _13d(cik: str, accession: str = "D", *, filed: datetime = _FILED, t: int = _T) -> _CoverRow:
    return _CoverRow(cik, accession, filed, Decimal(0), Decimal(t), Decimal(t), "x", "SCHEDULE 13D/A")


def _cats(
    insiders: Sequence[Holder] = (), blockholders: Sequence[Holder] = (), institutions: Sequence[Holder] = ()
) -> dict[SliceCategory, list[Holder]]:
    return {
        "insiders": list(insiders),
        "blockholders": list(blockholders),
        "institutions": list(institutions),
        "etfs": [],
    }


def _pie(by_category: Mapping[SliceCategory, Sequence[Holder]]) -> Decimal:
    return sum((h.shares for hs in by_category.values() for h in hs), Decimal(0))


def _run(
    cats: dict[SliceCategory, list[Holder]], covers: list[_CoverRow], filers: Mapping[str, Sequence[str | None]]
) -> list[CorrectionApplied]:
    out, corrections = _fold_13f_duplicates(cats, covers, filers)
    assert _pie(cats) - _pie(out) == sum((c.shares_removed for c in corrections), Decimal(0))
    return corrections


def _saro() -> dict[SliceCategory, list[Holder]]:
    return _cats(insiders=[_h(_FUND, _FUND_NAME)], institutions=[_13f(_CARLYLE, _CARLYLE_NAME)])


def _saro_covers() -> list[_CoverRow]:
    return [_block("Carlyle Group Inc."), _block("CARLYLE PARTNERS VII S1 HOLDINGS II L.P."), _block("Carlyle Fund X")]


_SARO_FILERS: Mapping[str, Sequence[str | None]] = {"G": (_CARLYLE,)}


def _way() -> dict[SliceCategory, list[Holder]]:
    return _cats(insiders=[_h(_DERBY, _DERBY_NAME)], institutions=[_13f(_EQT, _EQT_NAME)])


def _way_covers() -> list[_CoverRow]:
    return [_block("Derby LuxCo S.a r.l."), _block("EQT FUND MANAGEMENT S.A R.L.")]


_WAY_FILERS: Mapping[str, Sequence[str | None]] = {"G": (_DERBY,)}


# --- Folds ---------------------------------------------------------------------------------------


def test_the_saro_shape_folds_m_as_filer_and_k_by_form4_name() -> None:
    cats = _saro()
    out, corrections = _fold_13f_duplicates(cats, _saro_covers(), _SARO_FILERS)
    assert out["institutions"] == []
    (k,) = out["insiders"]
    assert [(d.filer_cik, d.source, d.shares) for d in k.dropped_sources] == [(_CARLYLE, "13f", Decimal(_T))]
    (c,) = corrections
    assert (c.kind, c.filer_cik, c.shares_removed, c.source_channel, c.winning_source) == (
        "cofiler_group_collapse",
        _FUND,
        Decimal(_T),
        "13f",
        "form4",
    )
    assert c.detail == (
        f"13F duplicate counted once (#3227): {_CARLYLE_NAME} ({_CARLYLE}) 13F {_T} for 2026-06-30; on joint "
        f"Schedule 13G G (filed 2026-05-15, filer of record 1527166) every stored reporting-person block reports "
        f"dispositive power and aggregate amount {_T}; {_CARLYLE_NAME} is taken to be on it as filer of record "
        f"and {_FUND_NAME} is matched by name. Counted once by policy: the filer's presence on the statement, "
        "identity by name, completeness of the stored blocks, overlap on that statement, across dates and in "
        "class scope are not verified."
    )


def test_the_way_shape_folds_k_as_filer_and_m_by_13f_name() -> None:
    (c,) = _run(_way(), _way_covers(), _WAY_FILERS)
    assert c.filer_cik == _DERBY
    assert f"{_DERBY_NAME} is taken to be on it as filer of record and {_EQT_NAME} is matched by name" in c.detail


def test_a_blockholders_twin_whose_row_is_c_itself_folds() -> None:
    cats = _cats(
        blockholders=[_h(_DERBY, _DERBY_NAME, source="13g", accession="G")], institutions=[_13f(_EQT, _EQT_NAME)]
    )
    assert _run(cats, _way_covers(), _WAY_FILERS)


# --- Identity routes ------------------------------------------------------------------------------


def test_m_not_filer_with_k_named_but_not_filer_refuses() -> None:
    # Two name identifications (M and k both by name only) are never a case.
    cats = _cats(insiders=[_h(_FUND, _FUND_NAME)], institutions=[_13f(_CARLYLE, _CARLYLE_NAME)])
    assert not _run(cats, _saro_covers(), {"G": (_OTHER,)})


def test_k_as_filer_with_a_matching_name_is_found_by_the_filer_route() -> None:
    cats = _cats(insiders=[_h(_DERBY, _DERBY_NAME)], institutions=[_13f(_EQT, _EQT_NAME)])
    (c,) = _run(cats, _way_covers(), _WAY_FILERS)
    assert f"{_DERBY_NAME} is taken to be on it as filer of record" in c.detail


def test_k_with_two_names_matching_two_blocks_is_one_twin() -> None:
    k_rows = [_h(_FUND, _FUND_NAME, _T - 5, accession="F1"), _h(_FUND, "Carlyle Fund X", 5, accession="F2")]
    cats = _cats(insiders=k_rows, institutions=[_13f(_CARLYLE, _CARLYLE_NAME)])
    (c,) = _run(cats, _saro_covers(), _SARO_FILERS)
    assert c.filer_cik == _FUND


@pytest.mark.parametrize("source", ["13g", "13d"])
def test_a_name_from_a_13dg_row_is_not_a_name_source(source: SourceTag) -> None:
    cats = _cats(blockholders=[_h(_FUND, _FUND_NAME, source=source)], institutions=[_13f(_CARLYLE, _CARLYLE_NAME)])
    assert not _run(cats, _saro_covers(), _SARO_FILERS)


def test_a_form3_name_is_a_name_source() -> None:
    cats = _cats(insiders=[_h(_FUND, _FUND_NAME, source="form3")], institutions=[_13f(_CARLYLE, _CARLYLE_NAME)])
    assert _run(cats, _saro_covers(), _SARO_FILERS)


# --- Each refusal alone ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "covers",
    [
        pytest.param([*_saro_covers()[:2], _block("Carlyle Fund X", aggregate=_T - 1)], id="a block off T"),
        pytest.param(
            [*_saro_covers()[:2], replace(_block("Carlyle Fund X"), shared_dispositive=Decimal(_T - 1))],
            id="a block's dispositive sum off T",
        ),
        pytest.param([*_saro_covers()[:2], replace(_block("Carlyle Fund X"), sole_dispositive=None)], id="NULL sole"),
        pytest.param(
            [
                *_saro_covers()[:2],
                replace(_block("Carlyle Fund X"), shared_dispositive=None, sole_dispositive=Decimal(_T)),
            ],
            id="NULL shared",
        ),
        pytest.param([*_saro_covers()[:2], replace(_block("Carlyle Fund X"), aggregate=None)], id="NULL aggregate"),
        pytest.param(
            [replace(c, filed_at=datetime(2026, 7, 1, tzinfo=UTC)) for c in _saro_covers()],
            id="C filed at P + 1 day 00:00 UTC",
        ),
        pytest.param([_block("Carlyle Group Inc."), _block("Another")], id="k not named on C"),
    ],
)
def test_each_refusal_alone(covers: list[_CoverRow]) -> None:
    cats = _saro()
    out, corrections = _fold_13f_duplicates(cats, covers, _SARO_FILERS)
    assert corrections == [] and out == cats


def test_the_last_instant_of_p_is_included() -> None:
    last = datetime(2026, 6, 30, 23, 59, 59, 999_000, tzinfo=UTC)
    covers = [replace(c, filed_at=last) for c in _saro_covers()]
    assert _run(_saro(), covers, _SARO_FILERS)


def test_k_total_other_than_t_refuses() -> None:
    cats = _saro()
    cats["insiders"].append(_h(_FUND, _FUND_NAME, 5, accession="F2"))
    assert not _run(cats, _saro_covers(), _SARO_FILERS)


def test_k_with_the_managers_canonical_cik_is_never_a_twin() -> None:
    cats = _cats(insiders=[_h("1527166", _FUND_NAME)], institutions=[_13f(_CARLYLE, _CARLYLE_NAME)])
    assert not _run(cats, _saro_covers(), _SARO_FILERS)


def test_no_13g_naming_or_filed_by_m_refuses() -> None:
    # The filer is someone else and no block carries M's 13F name.
    covers = [_block("Derby LuxCo S.a r.l."), _block("Someone Else")]
    assert not _run(_way(), covers, _WAY_FILERS)


# --- Filer of record ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "observed",
    [
        pytest.param((), id="zero current observations"),
        pytest.param((_CARLYLE, _OTHER), id="two distinct valid values"),
        pytest.param((_CARLYLE, None), id="a valid value plus NULL"),
        pytest.param((_CARLYLE, "15271X6"), id="a valid value plus a non-digit value"),
        pytest.param(("15271X6",), id="a lone non-digit value"),
    ],
)
@pytest.mark.parametrize("shape", ["saro", "way"])
def test_an_undefined_filer_refuses_on_either_route(observed: tuple[str | None, ...], shape: str) -> None:
    if shape == "saro":
        cats, covers, filer = _saro(), _saro_covers(), _CARLYLE
    else:
        cats, covers, filer = _way(), _way_covers(), _DERBY
    swapped = tuple(filer if o == _CARLYLE else o for o in observed)
    assert not _run(cats, covers, {"G": swapped})


@pytest.mark.parametrize("shape", ["saro", "way"])
def test_a_block_with_a_cik_makes_the_filer_undefined(shape: str) -> None:
    if shape == "saro":
        cats, covers, filers = _saro(), _saro_covers(), _SARO_FILERS
    else:
        cats, covers, filers = _way(), _way_covers(), _WAY_FILERS
    covers = [*covers, _block("Extra", cik="0000000999")]
    assert not _run(cats, covers, filers)


def test_padded_forms_of_one_cik_are_one_value() -> None:
    assert _run(_saro(), _saro_covers(), {"G": ("0001527166", " 1527166 ", "1527166")})


# --- C selection ----------------------------------------------------------------------------------


def test_a_newer_13d_row_by_cik_pre_empts_an_older_uniform_13g() -> None:
    newer = datetime(2026, 6, 1, tzinfo=UTC)
    covers = [*_saro_covers(), _13d(_CARLYLE, filed=newer, t=_T - 1)]
    assert not _run(_saro(), covers, _SARO_FILERS)


def test_a_newer_non_uniform_13g_naming_m_pre_empts_an_older_uniform_one() -> None:
    newer = datetime(2026, 6, 1, tzinfo=UTC)
    covers = [*_way_covers(), _block("EQT Fund Management S.a r.l.", "G2", filed=newer, aggregate=_T - 1)]
    assert not _run(_way(), covers, {**_WAY_FILERS, "G2": (_DERBY,)})


@pytest.mark.parametrize(("d_acc", "g_acc", "folds"), [("A1", "A2", True), ("A2", "A1", False)])
def test_a_13d_and_a_13g_filed_together_order_by_accession(d_acc: str, g_acc: str, folds: bool) -> None:
    covers = [_13d(_CARLYLE, d_acc, t=_T - 1), *(replace(c, accession_number=g_acc) for c in _saro_covers())]
    assert bool(_run(_saro(), covers, {g_acc: (_CARLYLE,)})) is folds


def test_two_candidates_with_different_periods_each_take_their_own_c() -> None:
    p1 = date(2026, 3, 31)
    early = replace(_13f(_EQT, _EQT_NAME, 4_000), as_of_date=p1)
    cats = _cats(
        insiders=[_h(_FUND, _FUND_NAME), _h(_DERBY, _DERBY_NAME, 4_000)],
        institutions=[_13f(_CARLYLE, _CARLYLE_NAME), early],
    )
    covers = [
        *_saro_covers(),
        _block("Derby LuxCo S.a r.l.", "W", filed=datetime(2026, 2, 1, tzinfo=UTC), t=4_000),
        _block(_EQT_NAME, "W", filed=datetime(2026, 2, 1, tzinfo=UTC), t=4_000),
        # After the 2026-03-31 candidate's P: not that candidate's C, though the batched read returns it.
        _block(_EQT_NAME, "W2", filed=datetime(2026, 5, 1, tzinfo=UTC), t=1),
    ]
    corrections = _run(cats, covers, {**_SARO_FILERS, "W": (_DERBY,), "W2": (_DERBY,)})
    assert sorted(c.shares_removed for c in corrections) == [Decimal(4_000), Decimal(_T)]


def test_a_missed_newer_statement_leaves_the_older_uniform_13g_as_c() -> None:
    # Risk 7, declared: a newer 13G filed by M with no readable filer and no matching spelling of M
    # is not M's statement, so the older uniform 13G decides.
    newer = datetime(2026, 6, 1, tzinfo=UTC)
    covers = [*_way_covers(), _block("EQT Fund Mgmt", "G2", filed=newer, aggregate=_T - 1)]
    assert _run(_way(), covers, _WAY_FILERS)


# --- Order and uniqueness against the 13D arm -----------------------------------------------------

_K13D = "0000000555"


def _with_13d_twin() -> dict[SliceCategory, list[Holder]]:
    cats = _saro()
    cats["insiders"].append(_h(_K13D, "13D Twin"))
    return cats


def test_an_older_13d_cover_with_a_twin_still_decides_m() -> None:
    older = datetime(2026, 1, 1, tzinfo=UTC)
    covers = [_13d(_CARLYLE, filed=older), _13d(_K13D, filed=older), *_saro_covers()]
    cats = _with_13d_twin()
    (c,) = _run(cats, covers, _SARO_FILERS)
    assert c.filer_cik == _K13D and "Schedule 13D/G D" in c.detail
    # The 13D arm's decision is the same with or without the 13G.
    assert _fold_13f_duplicates(cats, covers, _SARO_FILERS) == _fold_13f_duplicates(cats, covers[:2], {})


def test_an_older_13d_cover_with_no_twin_leaves_m_to_the_extension() -> None:
    older = datetime(2026, 1, 1, tzinfo=UTC)
    (c,) = _run(_saro(), [_13d(_CARLYLE, filed=older), *_saro_covers()], _SARO_FILERS)
    assert c.filer_cik == _FUND and "Schedule 13G G" in c.detail


def test_a_13d_match_refused_for_two_twins_reserves_both() -> None:
    # M1 (13D) has twins FUND and K13D, so it refuses; the 13G candidate's twin FUND is reserved.
    cats = _with_13d_twin()
    cats["institutions"].append(_13f("0000000111", "Manager One"))
    covers = [_13d("0000000111"), _13d(_FUND), _13d(_K13D), *_saro_covers()]
    assert not _run(cats, covers, _SARO_FILERS)


def test_two_competing_13d_matches_reserve_their_shared_twin() -> None:
    cats = _saro()
    cats["institutions"] += [_13f("0000000111", "Manager One"), _13f("0000000222", "Manager Two")]
    covers = [_13d("0000000111"), _13d("0000000222"), _13d(_FUND), *_saro_covers()]
    assert not _run(cats, covers, _SARO_FILERS)


def test_two_13g_candidates_sharing_a_twin_both_refuse() -> None:
    cats = _saro()
    cats["institutions"].append(_13f("0000000222", "Manager Two"))
    covers = [*_saro_covers(), *(_block(n, "G2") for n in ("Manager Two", _FUND_NAME))]
    assert not _run(cats, covers, {**_SARO_FILERS, "G2": ("0000000222",)})


def test_a_13g_candidate_with_two_twins_refuses() -> None:
    cats = _saro()
    cats["insiders"].append(_h("0000000333", "Carlyle Fund X"))
    assert not _run(cats, _saro_covers(), _SARO_FILERS)


@pytest.mark.parametrize(("a_uniform", "b_folds"), [(True, False), (False, True)])
def test_a_refused_two_twin_candidate_still_reserves_its_twins(a_uniform: bool, b_folds: bool) -> None:
    # A (Carlyle, filer of G) has twins {FUND, X} when G is uniform; B (Manager Two) only {FUND}.
    cats = _saro()
    cats["insiders"].append(_h("0000000333", "Carlyle Fund X"))
    cats["institutions"].append(_13f("0000000222", "Manager Two"))
    a_covers = _saro_covers() if a_uniform else [*_saro_covers(), _block("Off", aggregate=1)]
    covers = [*a_covers, *(_block(n, "G2") for n in ("Manager Two", _FUND_NAME))]
    corrections = _run(cats, covers, {**_SARO_FILERS, "G2": ("0000000222",)})
    assert [c.filer_cik for c in corrections] == ([_FUND] if b_folds else [])


# --- Names ----------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("m_name", "block_name", "folds"),
    [
        ("SoftBank Group Corp.", "SOFTBANK GROUP CORP.", True),
        ("A & B LLC", "A AND B LLC", True),
        ("ACME LP", "ACME LLC", False),
        ("", "", False),
        ("...", "--", False),
    ],
)
def test_name_equality(m_name: str, block_name: str, folds: bool) -> None:
    cats = _cats(insiders=[_h(_DERBY, _DERBY_NAME)], institutions=[_13f(_EQT, m_name)])
    covers = [_block("Derby LuxCo S.a r.l."), _block(block_name)]
    assert bool(_run(cats, covers, _WAY_FILERS)) is folds


# --- Read -----------------------------------------------------------------------------------------


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


def test_the_filer_read_queries_13g_accessions_once() -> None:
    conn = _FakeConn([("G", _CARLYLE), ("G", " 1527166"), ("G2", _OTHER)])
    covers = [*_saro_covers(), _block("x", "G2"), _13d(_CARLYLE)]
    assert _read_13g_filers(conn, 9, covers) == {"G": (_CARLYLE, " 1527166"), "G2": (_OTHER,)}  # type: ignore[arg-type]
    day = _FILED.date()
    assert conn.params == [
        {"iid": 9, "lo": day - timedelta(days=1), "hi": day + timedelta(days=1), "accs": ["G", "G2"]}
    ]


def test_no_13g_accession_means_no_filer_query() -> None:
    conn = _FakeConn([])
    assert _read_13g_filers(conn, 9, [_13d(_CARLYLE)]) == {}  # type: ignore[arg-type]
    assert conn.params == []
