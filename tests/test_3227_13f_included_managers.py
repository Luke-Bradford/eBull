"""Pure-logic tests for tier (IM) of the #3227 13F-duplicate fold (``_fold_13f_duplicates``).

(IM) folds a manager's 13F figure onto the included Manager its own 13F names, in Column 7, on every line of the
position (Form 13F Special Instructions 7.b, 11.b.vi, 11.b.vii). No DB — hand-built ``Holder`` objects and
``_IMEvidence``.
See docs/specs/ownership/2026-10-22-3227-13f-included-managers.md (Acceptance, Pure tests).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
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
    _im_keys,
    _im_named_ciks,
    _IMEvidence,
    _Observed13G,
    _reconcile_cofiler_groups,
)

_P = date(2026, 6, 30)
_T = 1_979_173
# CTNM on dev, 2026-10-22: Johnson & Johnson's 13F names Johnson & Johnson Innovation - JJDC, Inc. as included
# manager 1 on its one DFND line; JJDC's Form 3 reports the same figure.
_M, _M_NAME = "0000200406", "JOHNSON & JOHNSON"
_K, _K_NAME = "0000924020", "Johnson & Johnson Innovation - JJDC, Inc."
_A = "0000904454-26-000435"
_CAPTURE = "6f1c7c1e-0000-4000-8000-000000000001"
_U = "0000000444"


def _h(cik: str, name: str, shares: int = _T, *, source: SourceTag = "form3", accession: str = "F3") -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=None,
        as_of_date=_P if source == "13f" else date(2024, 4, 9),
        filer_type=None,
        dropped_sources=(),
    )


def _13f(cik: str = _M, name: str = _M_NAME, shares: int = _T, accession: str = _A) -> Holder:
    return _h(cik, name, shares, source="13f", accession=accession)


def _cats(
    insiders: Sequence[Holder] = (), institutions: Sequence[Holder] = (), blockholders: Sequence[Holder] = ()
) -> dict[SliceCategory, list[Holder]]:
    return {"insiders": list(insiders), "blockholders": list(blockholders), "institutions": list(institutions)}


def _ctnm() -> dict[SliceCategory, list[Holder]]:
    return _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f()])


def _line(shares: int = _T, discretion: str = "DFND", managers: list[int] | None = None) -> dict[str, Any]:
    return {"discretion": discretion, "other_managers": [1] if managers is None else managers, "shares": str(shares)}


def _ev(
    lines: list[dict[str, Any]] | None = None,
    *,
    shares: int = _T,
    accession: str = _A,
    status: str = "complete",
    capture: str = _CAPTURE,
    segments_capture: str = _CAPTURE,
    entries: Sequence[tuple[int, str | None]] = ((1, _K),),
    observations: int = 1,
    segments: bool = True,
    record: bool = True,
) -> _IMEvidence:
    payload = {"capture_id": segments_capture, "cusips": ["15234Q207"], "lines": [_line()] if lines is None else lines}
    return _IMEvidence(
        accession=accession,
        observations=tuple((Decimal(shares), payload if segments else None) for _ in range(observations)),
        capture=(capture, status) if record else None,
        entries=tuple(entries),
    )


def _key(cik: str = _M) -> str:
    return f"CIK:{cik}"


def _pie(by_category: Mapping[SliceCategory, Sequence[Holder]]) -> Decimal:
    return sum((h.shares for hs in by_category.values() for h in hs), Decimal(0))


def _run(
    cats: dict[SliceCategory, list[Holder]],
    evidence: Mapping[str, _IMEvidence],
    covers: Sequence[_CoverRow] = (),
    filers: Mapping[str, Sequence[_Observed13G]] | None = None,
) -> list[CorrectionApplied]:
    out, corrections = _fold_13f_duplicates(cats, covers, filers or {}, evidence)
    assert _pie(cats) - _pie(out) == sum((c.shares_removed for c in corrections), Decimal(0))
    return corrections


def _im(corrections: Sequence[CorrectionApplied]) -> list[CorrectionApplied]:
    return [c for c in corrections if "included manager" in c.detail]


def _refuses(cats: dict[SliceCategory, list[Holder]] | None = None, evidence: _IMEvidence | None = None) -> None:
    cats = _ctnm() if cats is None else cats
    out, corrections = _fold_13f_duplicates(cats, [], {}, {_key(): _ev() if evidence is None else evidence})
    assert corrections == [] and out == cats


# --- Folds ----------------------------------------------------------------------------------------


def test_the_ctnm_shape_folds_onto_the_included_manager() -> None:
    cats = _ctnm()
    out, corrections = _fold_13f_duplicates(cats, [], {}, {_key(): _ev()})
    assert out["institutions"] == []
    (k,) = out["insiders"]
    assert [(d.filer_cik, d.source, d.accession_number, d.shares) for d in k.dropped_sources] == [
        (_M, "13f", _A, Decimal(_T))
    ]
    (c,) = corrections
    assert (c.kind, c.filer_cik, c.shares_removed, c.source_channel, c.winning_source, c.winning_accession) == (
        "cofiler_group_collapse",
        _K,
        Decimal(_T),
        "13f",
        "form3",
        "F3",
    )
    assert c.detail == (
        f"13F duplicate counted once (#3227): {_M_NAME} ({_M}) 13F {_T} for 2026-06-30; its 13F {_A} reports every "
        f"line of the position as shared discretion with included manager {_K_NAME} ({_K}), Column 7. Counted once "
        "by policy: that the manager's own figure counts the same shares is not verified."
    )


def test_several_segments_of_different_column_7_sets_sharing_k_fold() -> None:
    # HHH: one DFND line names 1-4, another 1-4 and 6; manager 1 is k. OTR counts as shared too.
    lines = [_line(_T - 7, managers=[1, 2, 3, 4]), _line(5, managers=[1, 2, 3, 4, 6]), _line(2, "OTR", [1])]
    entries = ((1, _K), (2, "0000000002"), (3, None), (4, "0000000004"), (6, "0000000006"))
    assert len(_im(_run(_ctnm(), {_key(): _ev(lines, entries=entries)}))) == 1


def test_a_twin_whose_total_comes_from_two_kept_rows_folds() -> None:
    cats = _cats(
        insiders=[_h(_K, _K_NAME, _T - 100, accession="F3"), _h(_K, _K_NAME, 100, source="form4", accession="F4")],
        institutions=[_13f()],
    )
    (c,) = _run(cats, {_key(): _ev()})
    assert c.winning_accession == "F3"


def test_a_same_size_position_of_k_from_years_before_p_folds_declared_not_detected() -> None:
    # Risk 15: k's kept row predates P by years; a same-size replacement is not detected.
    old = replace(_h(_K, _K_NAME), as_of_date=date(2019, 1, 2))
    assert len(_run(_cats(insiders=[old], institutions=[_13f()]), {_key(): _ev()})) == 1


def test_a_list_cik_with_whitespace_and_without_padding_maps_canonically() -> None:
    assert len(_run(_ctnm(), {_key(): _ev(entries=((1, " 924020 "),))})) == 1


# --- Refusals -------------------------------------------------------------------------------------


def test_a_segment_lacking_k_refuses() -> None:
    _refuses(evidence=_ev([_line(_T - 5), _line(5, managers=[2])], entries=((1, _K), (2, _U))))


def test_a_segment_with_an_invalid_column_7_refuses() -> None:
    line = _line(5)
    line["other_managers"] = None
    _refuses(evidence=_ev([_line(_T - 5), line]))


@pytest.mark.parametrize("lines", [[_line(discretion="SOLE")], [_line(_T - 5), _line(5, "SOLE")]])
def test_a_sole_segment_refuses_even_naming_k(lines: list[dict[str, Any]]) -> None:
    _refuses(evidence=_ev(lines))


@pytest.mark.parametrize(
    "entries",
    [
        pytest.param((), id="no list entry"),
        pytest.param(((2, _K),), id="k under another number"),
        pytest.param(((1, None),), id="NULL CIK"),
        pytest.param(((1, "  "),), id="blank CIK"),
        pytest.param(((1, "0000"),), id="all-zero CIK"),
        pytest.param(((1, "92402O"),), id="non-digit CIK"),
        pytest.param(((1, "٩٢٤٠٢٠"),), id="non-ASCII digits"),
    ],
)
def test_ks_number_without_a_usable_list_entry_refuses(entries: tuple[tuple[int, str | None], ...]) -> None:
    _refuses(evidence=_ev(entries=entries))


@pytest.mark.parametrize("status", ["incomplete", "invalid"])
def test_a_capture_record_not_complete_refuses(status: str) -> None:
    _refuses(evidence=_ev(status=status))


def test_no_capture_record_refuses() -> None:
    _refuses(evidence=_ev(record=False))


def test_segments_of_another_capture_refuse() -> None:
    _refuses(evidence=_ev(segments_capture="6f1c7c1e-0000-4000-8000-000000000002"))


def test_null_segments_refuse() -> None:
    _refuses(evidence=_ev(segments=False))


@pytest.mark.parametrize("lines", [[_line(_T - 1)], [_line(_T), _line(1)], []])
def test_segments_not_summing_to_the_observations_shares_refuse(lines: list[dict[str, Any]]) -> None:
    _refuses(evidence=_ev(lines))


def test_ms_rows_spanning_two_accessions_refuse() -> None:
    cats = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(shares=_T - 5), _13f(shares=5, accession="OTHER")])
    _refuses(cats=cats)


def test_evidence_for_another_accession_refuses() -> None:
    _refuses(evidence=_ev(accession="0000904454-26-000999"))


def test_an_observation_with_shares_other_than_t_refuses() -> None:
    _refuses(evidence=_ev([_line(_T + 1)], shares=_T + 1))


@pytest.mark.parametrize("observations", [0, 2])
def test_zero_or_two_matching_observations_refuse(observations: int) -> None:
    _refuses(evidence=_ev(observations=observations))


def test_no_evidence_refuses() -> None:
    cats = _ctnm()
    assert _fold_13f_duplicates(cats, [], {}, {}) == (cats, [])
    assert _fold_13f_duplicates(cats, [], {}) == (cats, [])


def test_a_list_entry_carrying_ms_own_cik_is_not_a_twin() -> None:
    # M's own 13F identity is no node (13F rows are excluded), so give M a Form 4 node under a padded spelling.
    cats = _cats(insiders=[_h(_M.lstrip("0"), "J&J as insider")], institutions=[_13f()])
    _refuses(cats=cats, evidence=_ev(entries=((1, _M),)))


def test_a_twin_with_another_total_refuses() -> None:
    _refuses(cats=_cats(insiders=[_h(_K, _K_NAME, _T + 1)], institutions=[_13f()]))


# --- Uniqueness and reservation -------------------------------------------------------------------

_M2, _M2_NAME = "0000000222", "Manager Two"
_K2, _K2_NAME = "0000000333", "Included Two"


def test_two_im_candidates_sharing_a_twin_both_refuse() -> None:
    cats = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(), _13f(_M2, _M2_NAME, accession="A2")])
    assert _run(cats, {_key(): _ev(), _key(_M2): _ev(accession="A2")}) == []


def test_an_im_candidate_with_two_twins_refuses() -> None:
    cats = _cats(insiders=[_h(_K, _K_NAME), _h(_K2, _K2_NAME)], institutions=[_13f()])
    assert _run(cats, {_key(): _ev(entries=((1, _K), (2, _K2)), lines=[_line(managers=[1, 2])])}) == []


def test_a_multi_twin_candidate_sharing_one_twin_makes_an_otherwise_unique_one_refuse() -> None:
    cats = _cats(
        insiders=[_h(_K, _K_NAME), _h(_K2, _K2_NAME)], institutions=[_13f(), _13f(_M2, _M2_NAME, accession="A2")]
    )
    evidence = {
        _key(): _ev(),
        _key(_M2): _ev(accession="A2", entries=((1, _K), (2, _K2)), lines=[_line(managers=[1, 2])]),
    }
    assert _run(cats, evidence) == []


def _d(name: str, cik: str, accession: str = "D") -> _CoverRow:
    return _CoverRow(
        reporter_cik=cik,
        accession_number=accession,
        filed_at=datetime(2026, 3, 2, tzinfo=UTC),
        sole_dispositive=Decimal(0),
        shared_dispositive=Decimal(_T),
        aggregate=Decimal(_T),
        reporter_name=name,
        submission_type="SCHEDULE 13D",
    )


@pytest.mark.parametrize("earlier_refused", [False, True])
def test_a_twin_matched_by_the_13d_arm_is_reserved_even_when_that_match_refused(earlier_refused: bool) -> None:
    # M2's joint 13D with k matches k; M's (IM) evidence names k too.
    cats = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(), _13f(_M2, _M2_NAME, accession="A2")])
    covers = [_d(_M2_NAME, _M2), _d(_K_NAME, _K)]
    if earlier_refused:
        # A third 13F manager on the same 13D makes the 13D arm's match non-unique.
        cats["institutions"].append(_13f("0000000555", "Manager Five", accession="A5"))
        covers.append(_d("Manager Five", "0000000555"))
    sets: dict[str, dict[str, list[str]]] = {}
    out, corrections = _fold_13f_duplicates(cats, covers, {}, {_key(): _ev()}, matched_sets=sets)
    assert not _im(corrections)
    assert sets["13d"][_key(_M2)] == [_key(_K)]
    # (IM) matches k before reservation, and reservation refuses it.
    assert sets["im"] == {_key(): [_key(_K)]}
    assert [c.filer_cik for c in corrections] == ([] if earlier_refused else [_K])


def _g13(name: str, accession: str = "G", t: int = _T) -> _CoverRow:
    return _CoverRow(
        reporter_cik=None,
        accession_number=accession,
        filed_at=datetime(2026, 2, 2, tzinfo=UTC),
        sole_dispositive=Decimal(0),
        shared_dispositive=Decimal(t),
        aggregate=Decimal(t),
        reporter_name=name,
        submission_type="SCHEDULE 13G",
    )


def test_a_twin_matched_by_u_is_reserved() -> None:
    # M2 is named on a uniform 13G whose filer of record is k: (U) matches k.
    cats = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(), _13f(_M2, _M2_NAME, accession="A2")])
    sets: dict[str, dict[str, list[str]]] = {}
    out, corrections = _fold_13f_duplicates(
        cats, [_g13(_M2_NAME), _g13("Fund")], {"G": ((_K, "Fund", Decimal(_T)),)}, {_key(): _ev()}, matched_sets=sets
    )
    assert sets["u"] == {_key(_M2): [_key(_K)]} and sets["im"] == {_key(): [_key(_K)]}
    (c,) = corrections
    assert "every stored reporting-person block reports" in c.detail


def test_a_twin_matched_by_o_is_reserved() -> None:
    # M2 is named on a non-uniform 13G whose filer of record is k, and k's only row is C's observation: (O) matches k.
    k = _h(_K, _K_NAME, source="13g", accession="G")
    cats = _cats(blockholders=[k], institutions=[_13f(), _13f(_M2, _M2_NAME, accession="A2")])
    covers = [_g13(_K_NAME), _g13(_M2_NAME), _g13("Fund", t=5)]
    sets: dict[str, dict[str, list[str]]] = {}
    out, corrections = _fold_13f_duplicates(
        cats, covers, {"G": ((_K, _K_NAME, Decimal(_T)),)}, {_key(): _ev()}, matched_sets=sets
    )
    assert sets["o"] == {_key(_M2): [_key(_K)]} and sets["im"] == {_key(): [_key(_K)]}
    (c,) = corrections
    assert "counted here as" in c.detail


def test_a_candidate_in_an_earlier_matched_set_is_not_considered_by_im() -> None:
    # M itself matches the 13D arm with a non-unique twin set; (IM) does not get a second try at M.
    cats = _cats(insiders=[_h(_K, _K_NAME), _h(_K2, _K2_NAME)], institutions=[_13f()])
    covers = [_d(_M_NAME, _M), _d(_K_NAME, _K), _d(_K2_NAME, _K2)]
    sets: dict[str, dict[str, list[str]]] = {}
    out, corrections = _fold_13f_duplicates(cats, covers, {}, {_key(): _ev()}, matched_sets=sets)
    assert corrections == [] and sets["13d"] == {_key(): [_key(_K2), _key(_K)]} and sets["im"] == {}


def test_matched_sets_report_im_before_uniqueness() -> None:
    cats = _cats(insiders=[_h(_K, _K_NAME), _h(_K2, _K2_NAME)], institutions=[_13f()])
    sets: dict[str, dict[str, list[str]]] = {}
    _fold_13f_duplicates(
        cats, [], {}, {_key(): _ev(entries=((1, _K), (2, _K2)), lines=[_line(managers=[1, 2])])}, matched_sets=sets
    )
    assert sets == {"13d": {}, "u": {}, "o": {}, "im": {_key(): [_key(_K2), _key(_K)]}}


def test_matched_sets_are_empty_without_candidates() -> None:
    sets: dict[str, dict[str, list[str]]] = {}
    _fold_13f_duplicates(_cats(insiders=[_h(_K, _K_NAME)]), [], {}, {}, matched_sets=sets)
    assert sets == {"13d": {}, "u": {}, "o": {}, "im": {}}


# --- The read's key selection and N ---------------------------------------------------------------


def test_im_keys_select_single_accession_candidates_with_a_possible_twin() -> None:
    assert _im_keys(_ctnm()) == {_key(): ("200406", _P, _A)}
    # No node of total T other than M: no read.
    assert _im_keys(_cats(insiders=[_h(_K, _K_NAME, _T + 1)], institutions=[_13f()])) == {}
    # Two accessions: no read.
    two = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(shares=_T - 5), _13f(shares=5, accession="X")])
    assert _im_keys(two) == {}
    # M's own CIK (another spelling, so another identity) is not a possible twin.
    own = _cats(insiders=[_h(_M.lstrip("0"), "J&J as insider")], institutions=[_13f()])
    assert _im_keys(own) == {}


def test_im_named_ciks_intersects_before_mapping() -> None:
    lines = [_line(_T - 5, managers=[1, 2]), _line(5, "OTR", [2, 3])]
    entries = ((1, _K), (2, " 0000000002"), (3, "3"))
    assert _im_named_ciks(_ev(lines, entries=entries), Decimal(_T)) == frozenset({"2"})


def test_a_cik_above_ten_digits_is_not_read() -> None:
    long_cik = "12345678901"
    cats = _cats(insiders=[_h(_K, _K_NAME)], institutions=[_13f(long_cik)])
    assert _im_keys(cats) == {}


# --- k inside a #3898 co-filer clique -------------------------------------------------------------

_J = "0000924020-24-000001"


@pytest.mark.parametrize("k_survives", [True, False])
def test_k_inside_a_cofiler_clique_keeps_the_13f_trail(k_survives: bool) -> None:
    # k's Form 3 and k2's Form 4 are on one joint accession J, so the co-filer pass collapses them at the larger.
    k = _h(_K, _K_NAME, accession=_J)
    k2 = _h(_K2, _K2_NAME, _T - 1 if k_survives else _T + 1, source="form4", accession=_J)
    cats = _cats(insiders=[k, k2], institutions=[_13f()])
    folded, f13 = _fold_13f_duplicates(cats, [], {}, {_key(): _ev()})
    out, cof = _reconcile_cofiler_groups(folded, {_J: frozenset({"924020", "333"})})
    (im,) = f13
    (collapse,) = cof
    assert out["institutions"] == []
    (keeper,) = out["insiders"]
    trail = [(d.filer_cik, d.source, d.shares) for d in keeper.dropped_sources]
    if k_survives:
        assert keeper.filer_cik == _K and collapse.filer_cik == _K
        assert trail == [(_M, "13f", Decimal(_T)), (_K2, "form4", Decimal(_T - 1))]
    else:
        assert keeper.filer_cik == _K2 and collapse.filer_cik == _K2
        assert trail == [(_K, "form3", Decimal(_T)), (_M, "13f", Decimal(_T))]
    assert im.filer_cik == _K and im.shares_removed == Decimal(_T)
    assert _pie(cats) - _pie(out) == im.shares_removed + collapse.shares_removed


@pytest.mark.parametrize(
    "lines",
    [
        pytest.param([{"discretion": "DFND", "other_managers": [1]}], id="no shares"),
        pytest.param([{"discretion": "DFND", "other_managers": [1], "shares": "1,979,173"}], id="shares not a number"),
        pytest.param([{"discretion": "DFND", "other_managers": [1], "shares": []}], id="shares a list"),
        pytest.param([{"discretion": "DFND", "other_managers": [1], "shares": None}], id="shares null"),
        pytest.param([{"discretion": "DFND", "shares": str(_T)}], id="no other_managers"),
        pytest.param([{"other_managers": [1], "shares": str(_T)}], id="no discretion"),
        pytest.param(["DFND"], id="a line not an object"),
        pytest.param([{"discretion": "DFND", "other_managers": 1, "shares": str(_T)}], id="other_managers not a list"),
    ],
)
def test_malformed_segments_refuse_without_raising(lines: list[Any]) -> None:
    _refuses(evidence=_ev(lines))


def test_segments_not_an_object_or_lines_not_a_list_refuse() -> None:
    base = _ev()
    for payload in ([1], {"capture_id": _CAPTURE, "lines": "x"}):
        _refuses(evidence=replace(base, observations=((Decimal(_T), payload),)))  # type: ignore[arg-type]
