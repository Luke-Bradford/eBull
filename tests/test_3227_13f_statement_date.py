"""Pure-logic tests for the #3227 13F-duplicate fold's statement-date selection.

Every tier selects M's statement C by its statement date (``date_of_event``, falling back to the UTC
filing date), not by filing date. No DB — hand-built ``Holder`` objects, cover rows and observations.
See docs/specs/ownership/2026-10-22-3227-13f-statement-as-of.md (Acceptance, Pure tests).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from app.services.ownership_rollup import (
    CorrectionApplied,
    Holder,
    SliceCategory,
    SourceTag,
    _CoverRow,
    _fold_13f_duplicates,
    _Observed13G,
)

_P = date(2026, 6, 30)
_T = 12_128_756
_M, _M_NAME = "0001449434", "Apollo Management Holdings, L.P."
_K, _K_NAME = "0001449433", "Apollo Management Holdings GP, LLC"
_M2, _M2_NAME = "0000000555", "Other Manager"
_13D, _13G = "SCHEDULE 13D/A", "SCHEDULE 13G/A"
_O_MARK = "counted here as"
_U_MARK = "every stored reporting-person block reports"


def _at(y: int, m: int, d: int, hh: int = 0) -> datetime:
    return datetime(y, m, d, hh, tzinfo=UTC)


def _h(cik: str, name: str, shares: int = _T, *, source: SourceTag = "form4", accession: str = "") -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession or f"acc-{cik}-{source}",
        winning_edgar_url=None,
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
    )


def _13f(cik: str = _M, name: str = _M_NAME, shares: int = _T, *, period: date = _P) -> Holder:
    return replace(_h(cik, name, shares, source="13f", accession=f"13F-{cik}"), as_of_date=period)


def _row(
    accession: str,
    filed: datetime,
    event: date | None,
    *,
    cik: str | None = None,
    name: str = "x",
    t: int = _T,
    aggregate: int | None = None,
    form: str = _13D,
) -> _CoverRow:
    """A block reporting sole 0, shared ``t`` and aggregate ``t`` (or ``aggregate``)."""
    return _CoverRow(
        cik,
        accession,
        filed,
        Decimal(0),
        Decimal(t),
        Decimal(t if aggregate is None else aggregate),
        name,
        form,
        event,
    )


def _cats(
    insiders: Sequence[Holder] = (), blockholders: Sequence[Holder] = (), institutions: Sequence[Holder] = ()
) -> dict[SliceCategory, list[Holder]]:
    return {"insiders": list(insiders), "blockholders": list(blockholders), "institutions": list(institutions)}


def _pie(by_category: Mapping[SliceCategory, Sequence[Holder]]) -> Decimal:
    return sum((h.shares for hs in by_category.values() for h in hs), Decimal(0))


def _run(
    cats: dict[SliceCategory, list[Holder]],
    covers: Sequence[_CoverRow],
    filers: Mapping[str, Sequence[_Observed13G]] | None = None,
) -> list[CorrectionApplied]:
    out, corrections = _fold_13f_duplicates(cats, covers, filers or {})
    assert _pie(cats) - _pie(out) == sum((c.shares_removed for c in corrections), Decimal(0))
    return corrections


# --- The 13D arm ----------------------------------------------------------------------------------


def _arm() -> dict[SliceCategory, list[Holder]]:
    return _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f()])


def _pair(accession: str, filed: datetime, event: date | None, *, m_t: int = _T) -> list[_CoverRow]:
    """M's and k's blocks on one 13D; M's reports ``m_t``."""
    return [_row(accession, filed, event, cik=_M, t=m_t), _row(accession, filed, event, cik=_K)]


def test_a_13d_filed_after_p_with_a_date_of_event_on_or_before_p_is_the_arms_cover() -> None:
    (c,) = _run(_arm(), _pair("D", _at(2026, 7, 2), date(2026, 6, 29)))
    assert c.filer_cik == _K
    assert "(filed 2026-07-02, date of event 2026-06-29)" in c.detail


def test_a_13d_with_a_date_of_event_after_p_is_not_selected() -> None:
    assert _run(_arm(), _pair("D", _at(2026, 7, 2), date(2026, 7, 1))) == []


@pytest.mark.parametrize(
    ("filed", "folds"),
    [
        pytest.param(datetime(2026, 6, 30, 23, 59, 59, 999_000, tzinfo=UTC), True, id="P 23:59:59.999 UTC"),
        pytest.param(_at(2026, 7, 1), False, id="P + 1 day 00:00 UTC"),
    ],
)
def test_a_null_date_of_event_selects_by_filing_date_as_built(filed: datetime, folds: bool) -> None:
    corrections = _run(_arm(), _pair("D", filed, None))
    assert bool(corrections) is folds
    assert all("date of event" not in c.detail for c in corrections)


def test_a_date_of_event_after_the_filing_date_falls_back_and_changes_the_fold() -> None:
    # P 07-31. A: filed 07-01, date of event 08-05 (after filing) → statement date 07-01, eligible.
    # B: filed 07-02, date of event 06-30, does not report T. A is the later statement; read raw, A's
    # date would be after P and B would decide (and refuse).
    p = date(2026, 7, 31)
    cats = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(period=p)])
    a = _pair("A", _at(2026, 7, 1), date(2026, 8, 5))
    b = _pair("B", _at(2026, 7, 2), date(2026, 6, 30), m_t=_T - 1)
    assert a[0].statement_date == date(2026, 7, 1)
    (c,) = _run(cats, [*a, *b])
    assert "Schedule 13D/G A (filed 2026-07-01, date of event 2026-08-05)" in c.detail


def test_mixed_null_and_dated_statements_rank_by_statement_date() -> None:
    # The spec's § Rule example: P 07-31; A filed 07-01 with no date; B filed 07-02 with date of event
    # 06-30. The built rule took B; this rule takes A.
    p = date(2026, 7, 31)
    cats = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(period=p)])
    a = _pair("A", _at(2026, 7, 1), None)
    b = _pair("B", _at(2026, 7, 2), date(2026, 6, 30), m_t=_T - 1)
    (c,) = _run(cats, [*a, *b])
    assert "Schedule 13D/G A (filed 2026-07-01)" in c.detail
    a_bad = _pair("A", _at(2026, 7, 1), None, m_t=_T - 1)
    b_good = _pair("B", _at(2026, 7, 2), date(2026, 6, 30))
    assert _run(cats, [*a_bad, *b_good]) == []


@pytest.mark.parametrize("later_matches", [True, False])
def test_same_statement_date_the_later_filing_wins(later_matches: bool) -> None:
    early = _pair("A", _at(2026, 7, 20), _P, m_t=_T - 1 if later_matches else _T)
    late = _pair("B", _at(2026, 8, 1), _P, m_t=_T if later_matches else _T - 1)
    assert bool(_run(_arm(), [*early, *late])) is later_matches


@pytest.mark.parametrize("higher_matches", [True, False])
def test_same_statement_date_and_filing_instant_the_higher_accession_wins(higher_matches: bool) -> None:
    lo = _pair("A1", _at(2026, 8, 1), _P, m_t=_T - 1 if higher_matches else _T)
    hi = _pair("A2", _at(2026, 8, 1), _P, m_t=_T if higher_matches else _T - 1)
    assert bool(_run(_arm(), [*lo, *hi])) is higher_matches


def test_a_statement_for_a_later_date_beats_one_filed_later_for_an_earlier_date() -> None:
    later_date = _pair("A", _at(2026, 8, 10), _P)
    filed_later = _pair("B", _at(2026, 8, 12), date(2026, 3, 31), m_t=_T - 1)
    (c,) = _run(_arm(), [*later_date, *filed_later])
    assert "13D/G A " in c.detail


def test_a_newly_selected_statement_not_reporting_t_refuses_and_the_older_is_not_used() -> None:
    older = _pair("OLD", _at(2026, 5, 1), date(2026, 3, 31))
    newer = _pair("NEW", _at(2026, 8, 10), _P, m_t=_T - 1)
    assert _run(_arm(), older)
    assert _run(_arm(), [*older, *newer]) == []


def test_two_candidates_with_different_periods_each_select_by_their_own_p() -> None:
    p2 = date(2026, 3, 31)
    cats = _cats(
        insiders=[_h(_K, _K_NAME), _h("0000000888", "Twin Two", 4_000)],
        institutions=[_13f(), _13f(_M2, _M2_NAME, 4_000, period=p2)],
    )
    covers = [
        *_pair("D", _at(2026, 8, 10), _P),
        # M2: its 03-31 statement (filed after its P) reports 4,000; its 06-30 statement does not.
        _row("S1", _at(2026, 5, 10), p2, cik=_M2, t=4_000),
        _row("S1", _at(2026, 5, 10), p2, cik="0000000888", t=4_000),
        _row("S2", _at(2026, 8, 10), _P, cik=_M2, t=1),
    ]
    corrections = _run(cats, covers)
    assert sorted(c.shares_removed for c in corrections) == [Decimal(4_000), Decimal(_T)]


# --- The 13G extension (U) ------------------------------------------------------------------------

_DERBY, _DERBY_NAME = "0001998443", "Derby LuxCo S.a r.l."
_EQT, _EQT_NAME = "0001834239", "EQT Fund Management S.a r.l."


def _way() -> dict[SliceCategory, list[Holder]]:
    return _cats(insiders=[_h(_DERBY, _DERBY_NAME)], institutions=[_13f(_EQT, _EQT_NAME)])


def _way_covers(filed: datetime, event: date | None, accession: str = "G") -> list[_CoverRow]:
    return [
        _row(accession, filed, event, name=_DERBY_NAME, form=_13G),
        _row(accession, filed, event, name=_EQT_NAME.upper(), form=_13G),
    ]


def _filer(cik: str, *accessions: str) -> dict[str, tuple[_Observed13G, ...]]:
    return {a: ((cik, None, None),) for a in accessions}


@pytest.mark.parametrize(("event", "folds"), [(_P, True), (date(2026, 7, 1), False)])
def test_a_13g_filed_after_p_is_selected_by_its_date_of_event(event: date, folds: bool) -> None:
    corrections = _run(_way(), _way_covers(_at(2026, 8, 14), event), _filer(_DERBY, "G"))
    assert bool(corrections) is folds
    if folds:
        assert "Schedule 13G G (filed 2026-08-14, date of event 2026-06-30, filer of record 1998443)" in (
            corrections[0].detail
        )


def test_the_u_template_omits_a_null_date_of_event() -> None:
    (c,) = _run(_way(), _way_covers(_at(2026, 5, 15), None), _filer(_DERBY, "G"))
    assert "Schedule 13G G (filed 2026-05-15, filer of record 1998443)" in c.detail


def test_a_re_dated_13d_arm_match_reserves_its_twin_from_u() -> None:
    # M (13D arm) and EQT ((U), filer of record k) both reach k. Re-dated, M's 13D filed after P is in
    # force, so k is reserved and EQT refuses; with no date of event, M has no cover and EQT folds.
    cats = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(), _13f(_EQT, _EQT_NAME)])
    g = _way_covers(_at(2026, 5, 15), None)
    corrections = _run(cats, [*_pair("D", _at(2026, 8, 10), _P), *g], _filer(_K, "G"))
    assert [(c.filer_cik, _U_MARK in c.detail) for c in corrections] == [(_K, False)]
    undated = _run(cats, [*_pair("D", _at(2026, 8, 10), None), *g], _filer(_K, "G"))
    assert [(c.filer_cik, _U_MARK in c.detail) for c in undated] == [(_K, True)]


# --- Tier (O) -------------------------------------------------------------------------------------


def _o_covers(accession: str, filed: datetime, event: date | None, m_name: str = _M_NAME) -> list[_CoverRow]:
    """A 13G not uniform at T: the filer's block and M's report T, another reports less."""
    return [
        _row(accession, filed, event, name=_K_NAME, form=_13G),
        _row(accession, filed, event, name=m_name, form=_13G),
        _row(accession, filed, event, name="Apollo Fund", t=1_000, form=_13G),
    ]


def _o_filers(*accessions: str) -> dict[str, tuple[_Observed13G, ...]]:
    return {a: ((_K, _K_NAME, Decimal(_T)),) for a in accessions}


def test_the_tday_shape_folds_under_o() -> None:
    # k's only kept row is the 2026-08-11 13G/A's observation (date of event 06-30). The 2026-05-14
    # 13G/A (date of event 03-31) has the same blocks; filing-date selection took it, and (O) refused.
    cats = _cats(blockholders=[_h(_K, _K_NAME, source="13g", accession="NEW")], institutions=[_13f()])
    covers = [
        *_o_covers("OLD", _at(2026, 5, 14), date(2026, 3, 31)),
        *_o_covers("NEW", _at(2026, 8, 11), _P),
    ]
    (c,) = _run(cats, covers, _o_filers("OLD", "NEW"))
    assert _O_MARK in c.detail and c.filer_cik == _K
    assert "Schedule 13G NEW (filed 2026-08-11, date of event 2026-06-30, filer of record 1449433)" in c.detail
    undated = [replace(b, date_of_event=None) for b in covers]
    assert _run(cats, undated, _o_filers("OLD", "NEW")) == []


def test_the_calm_shape_folds_under_o() -> None:
    # M has no statement filed before P + 1 day; k files a non-uniform 13G after P with date of event P.
    cats = _cats(blockholders=[_h(_K, _K_NAME, source="13g", accession="G")], institutions=[_13f()])
    (c,) = _run(cats, _o_covers("G", _at(2026, 8, 14), _P), _o_filers("G"))
    assert _O_MARK in c.detail


def test_a_re_dated_u_match_reserves_its_twin_from_o() -> None:
    # EQT ((U), G1 filed after P, date of event P) and M ((O), G2) both reach k on the filer's CIK.
    cats = _cats(
        blockholders=[_h(_K, _K_NAME, source="13g", accession="G2")],
        institutions=[_13f(), _13f(_EQT, _EQT_NAME)],
    )
    g1 = [_row("G1", _at(2026, 8, 14), _P, name=_EQT_NAME, form=_13G)]
    g2 = _o_covers("G2", _at(2026, 5, 14), date(2026, 3, 31))
    filers = {**_filer(_K, "G1"), **_o_filers("G2")}
    corrections = _run(cats, [*g1, *g2], filers)
    assert [_U_MARK in c.detail for c in corrections] == [True]
    g1_undated = [_row("G1", _at(2026, 8, 14), None, name=_EQT_NAME, form=_13G)]
    corrections = _run(cats, [*g1_undated, *g2], filers)
    assert [_O_MARK in c.detail for c in corrections] == [True]


def test_the_o_template_omits_a_null_date_of_event() -> None:
    cats = _cats(blockholders=[_h(_K, _K_NAME, source="13g", accession="G")], institutions=[_13f()])
    (c,) = _run(cats, _o_covers("G", _at(2026, 5, 14), None), _o_filers("G"))
    assert "Schedule 13G G (filed 2026-05-14, filer of record 1449433)" in c.detail


# --- The statement date itself --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("filed", "event", "expected"),
    [
        pytest.param(_at(2026, 8, 11), _P, _P, id="date of event before filing"),
        pytest.param(_at(2026, 8, 11), None, date(2026, 8, 11), id="NULL"),
        pytest.param(_at(2026, 7, 1), date(2026, 8, 5), date(2026, 7, 1), id="after filing: fallback"),
        pytest.param(_at(2026, 7, 1, 2), None, date(2026, 7, 1), id="UTC date of the filing instant"),
    ],
)
def test_statement_date(filed: datetime, event: date | None, expected: date) -> None:
    assert _row("A", filed, event).statement_date == expected
