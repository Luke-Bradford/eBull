"""Pure-logic tests for tier (O) of the #3227 13F-duplicate fold (``_fold_13f_duplicates``).

(O) folds a manager's 13F figure onto the filer of record of a joint Schedule 13G that is not uniform
at T, by locating the block C's observation was copied from. No DB — hand-built ``Holder`` objects,
cover rows and observations.
See docs/specs/ownership/2026-10-22-3227-13f-13g-observation-block.md (Acceptance, Tests).
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
    _o_cover_blocks,
    _Observed13G,
)

_P = date(2026, 6, 30)
_T = 16_562_498
_FILED = datetime(2025, 11, 12, tzinfo=UTC)
# PRME on dev, 2026-10-22: Alphabet Inc. files the 13F; Alphabet Holdings LLC is the 13G's filer of
# record, and its observation of C is its only kept row. Two GV fund blocks report less than T.
_ALPHA, _ALPHA_NAME = "0001652044", "Alphabet Inc."
_HOLD, _HOLD_NAME = "0001845041", "Alphabet Holdings LLC"
_XXVI = "XXVI Holdings Inc."
_U = "0000000444"


def _h(cik: str | None, name: str, shares: int = _T, *, source: SourceTag = "13g", accession: str = "G") -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=None,
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
    )


def _13f(cik: str, name: str, shares: int = _T) -> Holder:
    return _h(cik, name, shares, source="13f", accession=f"13F-{cik}-{name}")


def _block(
    name: str, accession: str = "G", *, t: int = _T, filed: datetime = _FILED, aggregate: int | None = None
) -> _CoverRow:
    """A 13G block reporting sole 0, shared ``t`` and aggregate ``t`` (or ``aggregate``)."""
    return _CoverRow(
        reporter_cik=None,
        accession_number=accession,
        filed_at=filed,
        sole_dispositive=Decimal(0),
        shared_dispositive=Decimal(t),
        aggregate=Decimal(t if aggregate is None else aggregate),
        reporter_name=name,
        submission_type="SCHEDULE 13G",
    )


def _cats(
    blockholders: Sequence[Holder] = (), institutions: Sequence[Holder] = (), insiders: Sequence[Holder] = ()
) -> dict[SliceCategory, list[Holder]]:
    return {"insiders": list(insiders), "blockholders": list(blockholders), "institutions": list(institutions)}


def _prme() -> dict[SliceCategory, list[Holder]]:
    return _cats(blockholders=[_h(_HOLD, _HOLD_NAME)], institutions=[_13f(_ALPHA, _ALPHA_NAME)])


def _prme_covers(accession: str = "G", filed: datetime = _FILED) -> list[_CoverRow]:
    return [
        _block(_HOLD_NAME, accession, filed=filed),
        _block(_XXVI, accession, filed=filed),
        _block(_ALPHA_NAME, accession, filed=filed),
        _block("GV 2019, L.P.", accession, t=10_100_058, filed=filed),
        _block("GV 2021, L.P.", accession, t=_T - 10_100_058, filed=filed),
    ]


def _obs(cik: str = _HOLD, name: str | None = _HOLD_NAME, aggregate: int | None = _T) -> _Observed13G:
    return (cik, name, None if aggregate is None else Decimal(aggregate))


_PRME_FILERS: Mapping[str, Sequence[_Observed13G]] = {"G": (_obs(),)}


def _pie(by_category: Mapping[SliceCategory, Sequence[Holder]]) -> Decimal:
    return sum((h.shares for hs in by_category.values() for h in hs), Decimal(0))


def _run(
    cats: dict[SliceCategory, list[Holder]], covers: list[_CoverRow], filers: Mapping[str, Sequence[_Observed13G]]
) -> list[CorrectionApplied]:
    out, corrections = _fold_13f_duplicates(cats, covers, filers)
    assert _pie(cats) - _pie(out) == sum((c.shares_removed for c in corrections), Decimal(0))
    return corrections


def _o(corrections: Sequence[CorrectionApplied]) -> list[CorrectionApplied]:
    return [c for c in corrections if "counted here as" in c.detail]


def _without_o(filers: Mapping[str, Sequence[_Observed13G]]) -> dict[str, tuple[_Observed13G, ...]]:
    """The same filers of record with no observation name, so (O) cannot fire and (U) is untouched."""
    return {a: tuple((c, None, agg) for c, _, agg in rows) for a, rows in filers.items()}


def _detail(m_blocks: str, same: str = "", *, obs: str = _HOLD_NAME, k: str = _HOLD_NAME, m: str = _ALPHA_NAME) -> str:
    return (
        f"13F duplicate counted once (#3227): {m} ({_ALPHA}) 13F {_T} for 2026-06-30; on joint Schedule 13G G "
        f"(filed 2025-11-12, filer of record 1845041) the block of {obs}, counted here as {k}'s figure, reports "
        f"dispositive power and aggregate amount {_T}; the block(s) named {m_blocks} each report the same{same}; "
        f"no stored block has a non-NULL aggregate above {_T}; {m} is matched by name. Counted once by policy: "
        "identity by name, the observation's source block, completeness of the stored blocks, overlap on that "
        "statement, across dates and in class scope are not verified."
    )


# --- Folds by (O) ---------------------------------------------------------------------------------


def test_the_prme_shape_folds_onto_the_filer() -> None:
    cats = _prme()
    out, corrections = _fold_13f_duplicates(cats, _prme_covers(), _PRME_FILERS)
    assert out["institutions"] == []
    (k,) = out["blockholders"]
    assert [(d.filer_cik, d.source, d.shares) for d in k.dropped_sources] == [(_ALPHA, "13f", Decimal(_T))]
    (c,) = corrections
    assert (c.kind, c.filer_cik, c.shares_removed, c.source_channel, c.winning_source, c.winning_accession) == (
        "cofiler_group_collapse",
        _HOLD,
        Decimal(_T),
        "13f",
        "13g",
        "G",
    )
    assert c.detail == _detail(_ALPHA_NAME)


def test_the_bbot_shape_lists_the_observation_block_with_the_marker() -> None:
    # The observation's block is the block named as M (M's 13F name differs in case only).
    cats = _cats(blockholders=[_h(_HOLD, _HOLD_NAME)], institutions=[_13f(_ALPHA, _HOLD_NAME.upper())])
    covers = [_block(_HOLD_NAME), _block("Fund", t=_T - 5)]
    (c,) = _run(cats, covers, _PRME_FILERS)
    assert c.detail == _detail(_HOLD_NAME, " (one of them is that block)", m=_HOLD_NAME.upper())


def test_two_distinct_13f_names_each_naming_a_block_fold_and_are_listed() -> None:
    cats = _prme()
    cats["institutions"] = [_13f(_ALPHA, _ALPHA_NAME, _T - 7), _13f(_ALPHA, _XXVI, 7)]
    (c,) = _run(cats, _prme_covers(), _PRME_FILERS)
    assert f"the block(s) named {_ALPHA_NAME}; {_XXVI} each report the same;" in c.detail


def test_one_13f_name_matching_two_equally_normalised_blocks_folds_and_lists_both() -> None:
    cats = _cats(blockholders=[_h(_HOLD, _HOLD_NAME)], institutions=[_13f(_ALPHA, "Acme LLC")])
    covers = [_block(_HOLD_NAME), _block("ACME, LLC"), _block("Acme LLC"), _block("Fund", t=1)]
    (c,) = _run(cats, covers, _PRME_FILERS)
    assert "the block(s) named ACME, LLC; Acme LLC each report the same;" in c.detail


def test_the_same_block_shape_with_a_second_block_named_as_m_lists_both_with_the_marker() -> None:
    cats = _cats(
        blockholders=[_h(_HOLD, _HOLD_NAME)],
        institutions=[_13f(_ALPHA, _HOLD_NAME, _T - 7), _13f(_ALPHA, _XXVI, 7)],
    )
    covers = [_block(_HOLD_NAME), _block(_XXVI), _block("Fund", t=1)]
    (c,) = _run(cats, covers, _PRME_FILERS)
    assert f"the block(s) named {_HOLD_NAME}; {_XXVI} each report the same (one of them is that block);" in c.detail


@pytest.mark.parametrize(
    "unrelated",
    [
        pytest.param(_block("GV 2021, L.P.", aggregate=1), id="aggregate off T"),
        pytest.param(replace(_block("GV 2021, L.P."), sole_dispositive=None), id="NULL sole"),
        pytest.param(replace(_block("GV 2021, L.P."), shared_dispositive=None), id="NULL shared"),
        pytest.param(replace(_block("GV 2021, L.P."), aggregate=None), id="NULL aggregate"),
    ],
)
def test_an_unrelated_block_off_t_or_with_a_null_field_still_folds(unrelated: _CoverRow) -> None:
    # The inherited (U) refusals' paired (O) positives: the same unrelated block, and the case folds.
    covers = [*_prme_covers()[:3], unrelated]
    assert len(_o(_run(_prme(), covers, _PRME_FILERS))) == 1


# --- Each refusal alone ---------------------------------------------------------------------------


def _refuses(
    cats: dict[SliceCategory, list[Holder]] | None = None,
    covers: list[_CoverRow] | None = None,
    filers: Mapping[str, Sequence[_Observed13G]] | None = None,
) -> None:
    cats = _prme() if cats is None else cats
    out, corrections = _fold_13f_duplicates(
        cats, _prme_covers() if covers is None else covers, _PRME_FILERS if filers is None else filers
    )
    assert corrections == [] and out == cats


def test_m_as_filer_of_record_is_not_an_o_route() -> None:
    _refuses(filers={"G": (_obs(cik=_ALPHA),)})


@pytest.mark.parametrize(
    "filers",
    [
        pytest.param({}, id="zero current observations"),
        pytest.param({"G": (_obs(), _obs(cik=_U))}, id="two filer CIKs"),
        pytest.param({"G": (_obs(cik="18450X1"),)}, id="a non-digit CIK"),
    ],
)
def test_an_undefined_filer_refuses(filers: Mapping[str, Sequence[_Observed13G]]) -> None:
    _refuses(filers=filers)


def test_a_block_with_a_cik_makes_the_filer_undefined() -> None:
    _refuses(covers=[*_prme_covers(), replace(_block("Extra", t=1), reporter_cik="0000000999")])


def test_two_current_observations_of_c_refuse() -> None:
    _refuses(filers={"G": (_obs(), _obs(name=_XXVI))})


def test_an_observation_aggregate_other_than_t_refuses() -> None:
    _refuses(filers={"G": (_obs(aggregate=_T - 1),)})


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param(_h(_HOLD, _HOLD_NAME, 5, accession="G-OTHER"), id="a 13g row of another accession"),
        pytest.param(_h(_HOLD, _HOLD_NAME, 5, source="form4", accession="F4"), id="a Form 4 row"),
    ],
)
def test_a_kept_row_of_k_other_than_cs_observation_refuses(extra: Holder) -> None:
    cats = _cats(blockholders=[_h(_HOLD, _HOLD_NAME, _T - 5), extra], institutions=[_13f(_ALPHA, _ALPHA_NAME)])
    _refuses(cats=cats)


@pytest.mark.parametrize("name", ["ALPHABET HOLDINGS LLC", "Alphabet Holdings, LLC", "Someone Else"])
def test_no_stored_block_with_the_observations_exact_name_refuses(name: str) -> None:
    _refuses(filers={"G": (_obs(name=name),)})


def test_a_null_observation_name_refuses() -> None:
    _refuses(filers={"G": (_obs(name=None),)})


def test_two_stored_blocks_with_the_observations_exact_name_refuse() -> None:
    # Defensive: the upsert key allows one stored block per name on a 13G accession today.
    _refuses(covers=[*_prme_covers(), _block(_HOLD_NAME)])


@pytest.mark.parametrize(
    "located",
    [
        pytest.param(replace(_block(_HOLD_NAME), sole_dispositive=None), id="NULL dispositive"),
        pytest.param(replace(_block(_HOLD_NAME), shared_dispositive=Decimal(_T - 1)), id="dispositive off T"),
        pytest.param(_block(_HOLD_NAME, aggregate=_T - 1), id="aggregate off T"),
    ],
)
def test_the_observations_block_failing_reports_t_refuses(located: _CoverRow) -> None:
    _refuses(covers=[located, *_prme_covers()[1:]])


def test_a_block_with_an_aggregate_above_t_refuses() -> None:
    _refuses(covers=[*_prme_covers(), _block("GV Other", aggregate=_T + 1)])


def test_two_13f_names_one_naming_a_block_off_t_refuses() -> None:
    cats = _prme()
    cats["institutions"] = [_13f(_ALPHA, _ALPHA_NAME, _T - 7), _13f(_ALPHA, "GV 2019, L.P.", 7)]
    _refuses(cats=cats)


def test_one_13f_name_matching_two_equal_blocks_one_off_t_refuses() -> None:
    cats = _cats(blockholders=[_h(_HOLD, _HOLD_NAME)], institutions=[_13f(_ALPHA, "Acme LLC")])
    _refuses(cats=cats, covers=[_block(_HOLD_NAME), _block("ACME, LLC"), _block("Acme LLC", aggregate=1)])


def test_no_block_named_as_m_is_refused_by_the_predicate() -> None:
    # Defensive: through the fold, C is only ever selected by a block named as M.
    blocks = [_block(_HOLD_NAME), _block("Fund", t=1)]
    assert _o_cover_blocks(blocks, (_obs(),), Decimal(_T), {"ALPHABET INC"}) is None
    assert _o_cover_blocks(blocks, (_obs(),), Decimal(_T), {"ALPHABET HOLDINGS LLC"}) == (_HOLD_NAME, [_HOLD_NAME])


# --- Order and uniqueness -------------------------------------------------------------------------

_M2, _M2_NAME = "0000000222", "Manager Two"


def _with_u_candidate(k_cik: str = _HOLD) -> tuple[dict[SliceCategory, list[Holder]], list[CorrectionApplied]]:
    """PRME plus a (U) candidate M2 named on a uniform 13G "U1" whose filer of record is ``k_cik``."""
    cats = _prme()
    cats["institutions"].append(_13f(_M2, _M2_NAME))
    covers = [*_prme_covers(), _block(_M2_NAME, "U1"), _block("Fund U", "U1")]
    filers = {**_PRME_FILERS, "U1": (_obs(cik=k_cik, name="Fund U"),)}
    corrections = _run(cats, covers, filers)
    assert [c for c in corrections if c not in _o(corrections)] == _run(cats, covers, _without_o(filers))
    return cats, corrections


def test_a_u_fold_on_k_stands_and_the_o_candidate_refuses() -> None:
    _, corrections = _with_u_candidate()
    (c,) = corrections
    assert "every stored reporting-person block reports" in c.detail and c.filer_cik == _HOLD
    assert "Manager Two (0000000222)" in c.detail


def test_a_u_candidate_refused_for_uniqueness_still_reserves_its_twins() -> None:
    cats = _prme()
    cats["institutions"] += [_13f(_M2, _M2_NAME), _13f("0000000333", "Manager Three")]
    covers = [*_prme_covers(), _block(_M2_NAME, "U1"), _block("Manager Three", "U1")]
    filers = {**_PRME_FILERS, "U1": (_obs(name="Fund U"),)}
    assert _run(cats, covers, filers) == []


@pytest.mark.parametrize("second_13d", [False, True])
def test_a_13d_arm_match_folded_or_refused_reserves_its_twins(second_13d: bool) -> None:
    cats = _prme()
    cats["institutions"].append(_13f(_M2, _M2_NAME))
    d = [
        replace(_block(n, "D"), reporter_cik=c, submission_type="SCHEDULE 13D")
        for n, c in ((_M2_NAME, _M2), ("H", _HOLD))
    ]
    if second_13d:
        cats["institutions"].append(_13f("0000000333", "Manager Three"))
        d.append(replace(d[0], reporter_cik="0000000333"))
    corrections = _run(cats, [*d, *_prme_covers()], _PRME_FILERS)
    assert not _o(corrections)
    assert [c.filer_cik for c in corrections] == ([] if second_13d else [_HOLD])


def test_two_o_candidates_sharing_a_twin_both_refuse() -> None:
    cats = _prme()
    cats["institutions"].append(_13f(_M2, _XXVI))
    assert _run(cats, _prme_covers(), _PRME_FILERS) == []


def test_an_o_candidate_with_two_twins_on_the_filers_cik_refuses() -> None:
    cats = _prme()
    cats["blockholders"] = [_h(_HOLD, _HOLD_NAME), _h(_HOLD.lstrip("0"), _HOLD_NAME)]
    assert _run(cats, _prme_covers(), _PRME_FILERS) == []


def test_a_u_candidate_with_two_twins_reserves_the_o_candidates_twin() -> None:
    # A (M2, uniform U1, filer HOLD) matches {k1, k2} and refuses; B (Alphabet, (O) on G) has only k1,
    # whose rows are G's observation, and refuses because k1 is reserved.
    cats = _prme()
    cats["blockholders"].append(_h(_HOLD.lstrip("0"), "Alphabet Holdings LLC (U1)", accession="U1"))
    cats["institutions"].append(_13f(_M2, _M2_NAME))
    covers = [*_prme_covers(), _block(_M2_NAME, "U1"), _block("Fund U", "U1")]
    filers = {**_PRME_FILERS, "U1": (_obs(name="Fund U"),)}
    assert _run(cats, covers, filers) == []


def test_o_matches_never_change_a_u_decision() -> None:
    # The spec's "(U) B {k1} folds, (O) A {k1, k2} refuses" is unreachable: (O)'s twins on a filer CIK
    # are a subset of (U)'s there, and (U)'s name route needs a Form 3/4 row that (O) refuses. Its
    # property, that an (O) candidate cannot reserve against (U), is asserted here and in
    # ``_with_u_candidate``: (U)'s corrections are the same with (O) disabled.
    _, corrections = _with_u_candidate()
    assert len(corrections) == 1 and not _o(corrections)


def test_an_older_statement_is_not_used_when_the_newest_fails_o() -> None:
    older = datetime(2025, 5, 1, tzinfo=UTC)
    covers = [*_prme_covers("G0", older), *_prme_covers()]
    filers = {"G0": (_obs(),), "G": (_obs(aggregate=_T - 1),)}
    cats = _cats(blockholders=[_h(_HOLD, _HOLD_NAME, accession="G0")], institutions=[_13f(_ALPHA, _ALPHA_NAME)])
    assert _run(cats, covers, filers) == []
    # The older statement alone folds.
    assert len(_o(_run(cats, _prme_covers("G0", older), {"G0": (_obs(),)}))) == 1


@pytest.mark.parametrize("o_holds", [True, False])
def test_a_newer_non_uniform_13g_naming_m_is_evaluated_under_o(o_holds: bool) -> None:
    older = datetime(2025, 5, 1, tzinfo=UTC)
    uniform = [_block(_HOLD_NAME, "G0", filed=older), _block(_ALPHA_NAME, "G0", filed=older)]
    filers = {"G0": (_obs(),), "G": (_obs(aggregate=_T if o_holds else _T - 1),)}
    corrections = _run(_prme(), [*uniform, *_prme_covers()], filers)
    assert [c.detail for c in corrections] == ([_detail(_ALPHA_NAME)] if o_holds else [])


# --- (U) unchanged --------------------------------------------------------------------------------


def test_an_o_shape_on_a_uniform_c_is_decided_by_u() -> None:
    covers = _prme_covers()[:3]
    (c,) = _run(_prme(), covers, _PRME_FILERS)
    assert "every stored reporting-person block reports" in c.detail and "counted here as" not in c.detail
