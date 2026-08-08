"""Pure-logic tests for the #2230 deemed-chain tier of the insider control-group pass.

#1652 infers group membership from an improbably precise shared figure, and needs a ≥1M
magnitude floor plus a non-round guard to keep that inference honest. Those guards are
proxies for evidence, not truths about deemed ownership — so where the Form 3/4 filing
states the relationship structurally, they can be retired for that bucket.

The structural signature (:func:`_is_deemed_chain`) is all three of: ≥3 distinct CIKs,
every member flagged "10% Owner" on the relationship box, and the Rule 16a-1(a)(2)
ownership-form shape — at most one member DIRECT, at least two INDIRECT.

Cases below are the real full-population clusters the measurement turned up, so a
regression names something an operator can look up. No DB — hand-built ``Holder`` objects.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    Holder,
    SourceTag,
    _Candidate,
    _collapse_insider_control_group,
    _control_group_rep_key,
    _dedup_by_priority,
    _is_deemed_chain,
    _reconcile_insider_control_groups,
    _rows_by_identity,
    _select_control_group_rep,
)

_P = date(2026, 3, 31)

# LGN (Legence) — 16 Blackstone entities each restating 958,692. Non-round, but BELOW the
# 1M floor, so #1652's value proxies reject it outright.
_SUB_FLOOR = "958692"
# TRTX (TPG RE Finance) — 9 Starwood entities each at 12,000,000. Above the floor, but an
# exact multiple of 100,000, so the roundness guard rejects it.
_ROUND = "12000000"
# AMTM Lindsay Goldberg — non-round and ≥1M, so the ORIGINAL #1652 route still owns it.
_PROXY_OK = "45026743"


def _h(
    cik: str,
    name: str,
    shares: str,
    *,
    nature: str | None = "indirect",
    ten_pct: bool = True,
    source: SourceTag = "form4",
    table_i: bool = True,
) -> Holder:
    acc = f"acc-{cik}"
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=acc,
        winning_edgar_url=f"https://sec.gov/{acc}",
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
        ownership_nature=nature,
        is_ten_percent_owner=ten_pct,
        nature_from_table_i=table_i,
    )


def _chain(shares: str, n: int = 3, *, ten_pct: bool = True) -> list[Holder]:
    """A well-formed deemed chain: one DIRECT holder + ``n-1`` INDIRECT deemed owners."""
    out = [_h("000000001", "Sponsor Fund L.P.", shares, nature="direct", ten_pct=ten_pct)]
    out += [
        _h(f"00000000{i + 2}", f"Sponsor GP {i} L.L.C.", shares, nature="indirect", ten_pct=ten_pct)
        for i in range(n - 1)
    ]
    return out


def _kinds(corrs: list) -> list[str]:
    return [c.kind for c in corrs]


# ---------------------------------------------------------------------------
# The two classes #1652's value proxies reject, and the deemed chain admits
# ---------------------------------------------------------------------------


def test_sub_floor_chain_collapses() -> None:
    """LGN: 958,692 is non-round but under the 1M floor. The structural signature
    carries it anyway — 3 holders in, 1 out, and the block counted once."""
    out_s, out_b, corrs = _reconcile_insider_control_groups(_chain(_SUB_FLOOR), [])
    assert out_b == []
    assert len(out_s) == 1
    assert out_s[0].shares == Decimal(_SUB_FLOOR)
    assert _kinds(corrs) == ["insider_control_group_collapse"]
    assert corrs[0].shares_removed == Decimal(_SUB_FLOOR) * 2


def test_round_value_chain_collapses() -> None:
    """TRTX: 12,000,000 is ≥1M but an exact multiple of 100,000, so the roundness guard
    rejects it. A round figure is only *coincidence-prone*; it is not evidence AGAINST a
    chain that the relationship box already attests."""
    out_s, _out_b, corrs = _reconcile_insider_control_groups(_chain(_ROUND, n=4), [])
    assert len(out_s) == 1
    assert _kinds(corrs) == ["insider_control_group_collapse"]
    assert corrs[0].shares_removed == Decimal(_ROUND) * 3


def test_all_indirect_chain_collapses() -> None:
    """EXE/HCC shape: every member reports INDIRECT and the direct holder is absent from
    the bucket (it holds a different lot). ``n_direct=0`` satisfies "at most one"."""
    holders = [
        _h("000000001", "Blackstone Group L.P.", _SUB_FLOOR),
        _h("000000002", "GSO Special Situations Fund LP", _SUB_FLOOR),
        _h("000000003", "GSO Special Situations Overseas Master Fund Ltd.", _SUB_FLOOR),
    ]
    out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert len(out_s) == 1
    assert _kinds(corrs) == ["insider_control_group_collapse"]


# ---------------------------------------------------------------------------
# The false-positive classes the tier must NOT admit
# ---------------------------------------------------------------------------


def test_equal_director_grants_are_not_collapsed() -> None:
    """FLG/BBT: eleven directors each holding an identical 11,220 indirectly (deferred
    comp). None is a 10% owner, so the relationship box refuses the cluster — this is
    the #1659 equal-grant class and it must stay additive."""
    holders = [_h(f"00000000{i}", f"Director {i}", "11220", ten_pct=False) for i in range(1, 6)]
    out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert len(out_s) == 5
    assert corrs == []


def test_equal_family_trusts_holding_directly_are_not_collapsed() -> None:
    """GSHD: four Jones family trusts, each *fbo* a different beneficiary, each holding
    9,787 DIRECTLY. Equal distribution, not deemed attribution — three DIRECT members
    breach "at most one", so the shape test rejects it."""
    holders = [
        _h("000000001", "Brendan Scot Jones Trust", "9787", nature="direct"),
        _h("000000002", "Emily Marie Jones Trust", "9787", nature="direct"),
        _h("000000003", "Joshua Thomas Jones Trust", "9787", nature="direct"),
        _h("000000004", "Jones Serena", "9787", nature="indirect"),
    ]
    out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert len(out_s) == 4
    assert corrs == []


def test_two_cik_natural_person_pair_is_not_collapsed() -> None:
    """TEAM: Atlassian's two co-founders hold near-identical stakes independently and BY
    DESIGN. A 2-CIK cluster is not separable from source data, so it stays double-counted
    — deliberately, because merging them would erase half the reported insider ownership.
    """
    holders = [
        _h("000000001", "Cannon-Brookes Michael", "275940"),
        _h("000000002", "Farquhar Scott", "275940"),
    ]
    out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert len(out_s) == 2
    assert corrs == []


def test_beneficial_nature_cannot_satisfy_the_indirect_floor() -> None:
    """Fail-closed on the nature axis: legacy ``beneficial`` rows count toward neither
    DIRECT nor INDIRECT, so a bucket of them cannot reach ``_DEEMED_CHAIN_MIN_INDIRECT``.
    """
    holders = [_h(f"00000000{i}", f"Entity {i}", _SUB_FLOOR, nature="beneficial") for i in range(1, 4)]
    out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert len(out_s) == 3
    assert corrs == []


def test_unjoined_relationship_row_fails_closed() -> None:
    """A holder whose accession/CIK does not join ``insider_filers`` carries
    ``is_ten_percent_owner=False`` and must veto the cluster rather than be ignored."""
    holders = _chain(_SUB_FLOOR, n=3)
    holders[1] = _h(holders[1].filer_cik or "", holders[1].filer_name, _SUB_FLOOR, ten_pct=False)
    out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert len(out_s) == 3
    assert corrs == []


def test_fold_that_would_strand_another_channel_row_is_refused() -> None:
    """CQP: folding an identity out of ``survivors`` removes it from the owner-once
    identity grouping, which RELEASES that identity's 13F row into the institutions
    wedge — measured at +101,420,487 shares on the real instrument. Claiming the row is
    not available (#1652 settled consumption as exact-value-only so a member's larger
    genuine 13D block survives), so the new tier refuses the whole cluster instead."""
    stranded = _h("000000002", "Blackstone Group L.P.", "101420487", nature=None, source="13f")
    holders = _chain(_SUB_FLOOR, n=3) + [stranded]
    out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert corrs == []
    assert len(out_s) == 4
    assert stranded in out_s


def test_fold_is_allowed_when_the_other_row_belongs_to_the_REP() -> None:
    """The rep stays in ``survivors``, so its own other-channel rows are never stranded
    — only a NON-rep member's are. Guards against the check being written too broadly
    and refusing every cluster whose rep has a 13F footprint."""
    holders = _chain(_SUB_FLOOR, n=3)
    # ⚠ Ask the module for the rep. This line used to re-spell the key inline as
    # ``(True, shares, cik, accession)``. #2385 then ADDED a selector on top of that key,
    # and both the inline copy and a bare ``max(..., key=_control_group_rep_key)`` now
    # name the INCUMBENT — i.e. they would attach the 13F row to a holder that is no
    # longer the rep, setting up the OPPOSITE scenario from the one this test's name
    # describes while still asserting on the same output.
    rep = _select_control_group_rep(holders, _rows_by_identity(holders, []))
    holders.append(_h(rep.filer_cik or "", rep.filer_name, "999999999", nature=None, source="13f"))
    _out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert _kinds(corrs) == ["insider_control_group_collapse"]


# ---------------------------------------------------------------------------
# The original #1652 route is untouched
# ---------------------------------------------------------------------------


def test_value_proxy_route_still_fires_without_the_new_signals() -> None:
    """A non-round ≥1M block collapses on the ORIGINAL route with only 2 CIKs, no 10%
    flag and no nature — the new tier is additive, not a replacement."""
    holders = [
        _h("000000001", "LG GP Holding IV LLC", _PROXY_OK, nature=None, ten_pct=False),
        _h("000000002", "Lindsay Goldberg IV L.P.", _PROXY_OK, nature=None, ten_pct=False),
    ]
    out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert len(out_s) == 1
    assert _kinds(corrs) == ["insider_control_group_collapse"]


def test_widened_bucketing_does_not_disturb_ineligible_holders() -> None:
    """Bucketing now admits every positive-share insider row, not only ≥1M non-round
    ones. A row that satisfies neither tier must come back out of its origin channel
    unchanged — same object identity, same channel."""
    survivors = [_h("000000001", "Lone Director", "726198", ten_pct=False)]
    blockholders = [_h("000000009", "Some 13G Filer", "500000", source="13g", ten_pct=False)]
    out_s, out_b, corrs = _reconcile_insider_control_groups(survivors, blockholders)
    assert out_s == survivors
    assert out_b == blockholders
    assert corrs == []


# ---------------------------------------------------------------------------
# The predicate itself
# ---------------------------------------------------------------------------


def test_is_deemed_chain_requires_three_distinct_ciks() -> None:
    assert _is_deemed_chain(_chain(_SUB_FLOOR, n=3)) is True
    assert _is_deemed_chain(_chain(_SUB_FLOOR, n=2)) is False


def test_is_deemed_chain_rejects_two_direct_members() -> None:
    chain = _chain(_SUB_FLOOR, n=4)
    assert _is_deemed_chain(chain) is True
    # Flip a second member to DIRECT — two direct holders is an equal-holding
    # coincidence, not a control chain.
    chain[1] = _h(chain[1].filer_cik or "", chain[1].filer_name, _SUB_FLOOR, nature="direct")
    assert _is_deemed_chain(chain) is False


def test_is_deemed_chain_requires_two_indirect_members() -> None:
    holders = [
        _h("000000001", "Fund L.P.", _SUB_FLOOR, nature="direct"),
        _h("000000002", "GP L.L.C.", _SUB_FLOOR, nature="indirect"),
        _h("000000003", "Odd One", _SUB_FLOOR, nature="beneficial"),
    ]
    assert _is_deemed_chain(holders) is False


# ---------------------------------------------------------------------------
# ``ownership_nature`` provenance (#2386)
#
# The column has four writers and only three mean Form 4/3 Table I column 5 by it.
# ``sec_insider_dataset_ingest._map_relationship`` maps the DERA insider dataset's
# RELATIONSHIP flags onto the same column — officer/director → ``direct``,
# ten-percent-owner → ``beneficial``. The shape test counts Table I-attested rows only.
# ---------------------------------------------------------------------------


def test_dedup_carries_nature_provenance_with_any_semantics() -> None:
    """The flag has to survive the merge or the gate reads ``False`` on every holder and
    silently refuses everything. ``any``, not ``all``: candidates are already grouped by
    ``ownership_nature``, so one XML-attested row proves the string is a real Table I
    value — and over-claiming here is the direction that KEEPS a direct member counted,
    which is fail-closed for :func:`_is_deemed_chain`."""

    def _c(nature: str, table_i: bool, row_id: int) -> _Candidate:
        return _Candidate(
            source="form4",
            priority_rank=1,
            filer_cik="000000001",
            filer_name="Sponsor Fund L.P.",
            filer_type=None,
            shares=Decimal(_SUB_FLOOR),
            as_of_date=_P,
            accession_number=f"acc-{row_id}",
            source_row_id=row_id,
            ownership_nature=nature,
            nature_from_table_i=table_i,
        )

    mixed = _dedup_by_priority([_c("direct", False, 1), _c("direct", True, 2)])
    assert [h.nature_from_table_i for h in mixed] == [True]
    role_only = _dedup_by_priority([_c("direct", False, 1), _c("direct", False, 2)])
    assert [h.nature_from_table_i for h in role_only] == [False]


def test_role_derived_direct_does_not_count_against_the_direct_cap() -> None:
    """A DERA officer row is not a Rule 16a-1(a)(2) direct holder. Counting its
    ``direct`` string pushes a genuine chain past ``_DEEMED_CHAIN_MAX_DIRECT`` and
    refuses it — the coverage defect #2386 records."""
    chain = _chain(_SUB_FLOOR, n=3)  # 1 Table I direct + 2 Table I indirect
    chain.append(_h("000000009", "Officer Person", _SUB_FLOOR, nature="direct", table_i=False))
    assert _is_deemed_chain(chain) is True


def test_role_derived_row_cannot_satisfy_the_indirect_floor() -> None:
    """Fail-closed in the other direction too: provenance gating is not a blanket
    loosening. Two role-derived ``indirect`` rows do not make a chain, so the floor
    still has to be met by Table I-attested members."""
    holders = [
        _h("000000001", "Fund L.P.", _SUB_FLOOR, nature="direct"),
        _h("000000002", "GP L.L.C.", _SUB_FLOOR, nature="indirect", table_i=False),
        _h("000000003", "GP II L.L.C.", _SUB_FLOOR, nature="indirect", table_i=False),
    ]
    assert _is_deemed_chain(holders) is False


def test_role_derived_member_still_faces_the_ten_percent_gate() -> None:
    """Provenance gates the NATURE counters only. The relationship-box requirement is
    joined from ``insider_filers``, a different source, and still binds every member."""
    chain = _chain(_SUB_FLOOR, n=3)
    chain.append(_h("000000009", "Officer Person", _SUB_FLOOR, nature="direct", ten_pct=False, table_i=False))
    assert _is_deemed_chain(chain) is False


def test_role_derived_member_folds_end_to_end() -> None:
    """Through the real pass, not just the predicate: the cluster that the overloaded
    column was refusing now collapses to one holder at the block value."""
    chain = _chain(_SUB_FLOOR, n=3)
    chain.append(_h("000000009", "Officer Person", _SUB_FLOOR, nature="direct", table_i=False))
    out_s, out_b, corrs = _reconcile_insider_control_groups(chain, [])
    assert len(out_s) == 1
    assert out_s[0].shares == Decimal(_SUB_FLOOR)
    assert out_b == []
    assert _kinds(corrs) == ["insider_control_group_collapse"]


def test_duplicate_cik_does_not_inflate_the_cluster_size() -> None:
    """Distinctness is on CIK, so one filer's several rows cannot manufacture a
    three-member chain out of two reporters."""
    holders = [
        _h("000000001", "Fund L.P.", _SUB_FLOOR, nature="direct"),
        _h("000000002", "GP L.L.C.", _SUB_FLOOR, nature="indirect"),
        _h("000000002", "GP L.L.C. (second lot)", _SUB_FLOOR, nature="indirect"),
    ]
    assert _is_deemed_chain(holders) is False


def test_rep_preview_and_fold_pick_the_same_holder() -> None:
    """The release-hazard preview and the fold must agree on WHO the rep is, or the
    check protects one identity and strands another. They now share
    :func:`_select_control_group_rep`; this pins that they still agree."""
    cluster = [
        _h("000000003", "Tie B", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "Sponsor Fund L.P.", _SUB_FLOOR, nature="direct"),
        _h("000000002", "Tie A", _SUB_FLOOR, nature="indirect"),
        _h("000000009", "Blockholder Co", _SUB_FLOOR, nature=None, source="13d"),
    ]
    index = _rows_by_identity(cluster, [])
    folded, _corr = _collapse_insider_control_group(cluster, index)
    preview = _select_control_group_rep(cluster, index)
    assert (folded.filer_cik, folded.filer_name) == (preview.filer_cik, preview.filer_name)


# ---------------------------------------------------------------------------
# #2385 — WHICH member the fold keeps. Rule 16a-1(a)(2): the chain's one DIRECT
# holder is the record holder, and it is what the block should be labelled with.
# ---------------------------------------------------------------------------


def _rep(cluster: list[Holder], blockholders: list[Holder] | None = None) -> Holder:
    return _select_control_group_rep(cluster, _rows_by_identity(cluster, blockholders or []))


def test_rep_is_the_table_i_direct_holder_not_the_highest_cik() -> None:
    """XFOR/TRVI shape: six deemed owners restating one NEA fund's block. The incumbent
    key has no nature component, so past the insider-source preference it selects by CIK
    order and labels the block with whichever deemed owner happens to sort highest."""
    cluster = _chain(_SUB_FLOOR, n=4)
    assert max(cluster, key=_control_group_rep_key).filer_name == "Sponsor GP 2 L.L.C."
    assert _rep(cluster).filer_name == "Sponsor Fund L.P."


def test_role_derived_direct_is_not_a_record_holder() -> None:
    """``sec_insider_dataset_ingest._map_relationship`` maps officer/director → ``direct``
    without ever reading Table I column 5, so a DERA row's ``direct`` means "is an
    officer". Promoting one puts an individual's name on a fund's block — measured at 59
    of the 224 folds the ungated key moved (#2385)."""
    cluster = [
        _h("000000009", "Deemed Owner A", _SUB_FLOOR, nature="indirect"),
        _h("000000008", "Deemed Owner B", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "Officer Person", _SUB_FLOOR, nature="direct", table_i=False),
    ]
    assert _rep(cluster).filer_name == "Deemed Owner A"


def test_direct_holder_co_filing_the_incumbents_accession_is_not_promoted() -> None:
    """``TACO`` (Form 3 ``0001829126-25-003075``): the ownership XML puts
    ``<nonDerivativeHolding>`` beside ``<reportingOwner>``, not inside it, so a joint
    filing does not say which co-filer holds the D line — and
    ``insider_transactions._extract_holdings`` attributes every row to ``filers[0]``.
    Within one accession the attestation therefore ranks by XML listing order. Measured:
    6 of 931 same-accession folds carry >=2 attested members, against 378 of 503
    cross-accession."""
    acc = "0001829126-25-003075"
    first_listed = _h("000000001", "You Harry L.", _SUB_FLOOR, nature="direct")
    co_filer = _h("000000009", "Sponsor LLC", _SUB_FLOOR, nature="direct", table_i=False)
    cluster = [replace(first_listed, winning_accession=acc), replace(co_filer, winning_accession=acc)]
    assert _rep(cluster).filer_name == "Sponsor LLC"  # incumbent kept — no discriminant


def test_two_attested_direct_holders_is_not_a_chain() -> None:
    """A Rule 16a-1(a)(2) chain has ONE holder of record — the same shape
    ``_DEEMED_CHAIN_MAX_DIRECT`` already encodes. Two members each attesting D on their
    OWN filings is the #1659 equal-value coincidence instead, and choosing between them
    falls through to arbitrary CIK order (``ABTC`` swapped one person's revocable trust
    for another's; ``HYMC`` promoted the advisor over the fund)."""
    cluster = [
        _h("000000005", "Deemed Owner", _SUB_FLOOR, nature="indirect"),
        _h("000000002", "Trust A", _SUB_FLOOR, nature="direct"),
        _h("000000001", "Trust B", _SUB_FLOOR, nature="direct"),
    ]
    assert _rep(cluster).filer_name == "Deemed Owner"


def test_swap_is_declined_when_the_incumbent_holds_other_channel_rows() -> None:
    """``ACDC``: demoting ``THRC Management, LLC`` releases its 13D rows into the
    blockholders wedge and the insiders wedge ROSE 225,951,558 → 298,774,575. The rep is
    the identity that survives into owner-once, so the swap is arithmetic; it is taken
    only when it cannot add shares back."""
    incumbent = _h("000000009", "THRC Management, LLC", _SUB_FLOOR, nature="indirect")
    cluster = [incumbent, _h("000000001", "THRC Holdings, LP", _SUB_FLOOR, nature="direct")]
    elsewhere = [_h("000000009", "THRC Management, LLC", "72822917", nature=None, source="13d")]
    assert _rep(cluster).filer_name == "THRC Holdings, LP"  # release-free: swap taken
    assert _rep(cluster, elsewhere).filer_name == "THRC Management, LLC"  # exposed: declined


def test_institutional_row_carrying_table_i_provenance_cannot_become_the_rep() -> None:
    """``nature_from_table_i`` survives the cross-source merge via
    ``any(c.nature_from_table_i for c in cands)``, so a 13F-winning holder can carry it.
    The rep must stay insider-sourced or it does not route to the insiders slice."""
    cluster = [
        _h("000000009", "Deemed Owner A", _SUB_FLOOR, nature="indirect"),
        _h("000000008", "Deemed Owner B", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "Institution Co", _SUB_FLOOR, nature="direct", source="13f"),
    ]
    assert _rep(cluster).filer_name == "Deemed Owner A"
