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
    _releases_into_another_wedge,
    _releases_other_rows,
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


# ---------------------------------------------------------------------------
# #2408 — the SAME-accession half. Table I cannot separate co-filers of one
# accession, so the discriminant is the filing's own indirect-ownership text.
# ---------------------------------------------------------------------------

_ACC = "0001829126-25-003075"  # TACO Form 3, the case the ticket reasons from


def _joint(*holders: Holder) -> list[Holder]:
    """A joint filing: every member on ONE accession, which is what defeats Table I."""
    return [replace(h, winning_accession=_ACC) for h in holders]


def _evidence(*texts: str, shares: str = _SUB_FLOOR, accession: str = _ACC) -> dict:
    """Record-holder evidence keyed the way the read path keys it: the Table I line's own
    reported amount, NOT the accession alone. One filing's footnote set covers many
    holdings naming different record holders, so the value is the link."""
    return {(accession, Decimal(shares)): texts}


def _rep_with(cluster: list[Holder], evidence: dict, blockholders: list[Holder] | None = None) -> Holder:
    return _select_control_group_rep(cluster, _rows_by_identity(cluster, blockholders or []), evidence)


def test_record_holder_text_promotes_the_member_it_names() -> None:
    """``TACO`` ``0001829126-25-003075`` footnote F3, verbatim from the dev corpus. Two
    reporting owners, two Table I lines, nothing tying either line to either owner — but
    the footnote states the record holder outright."""
    cluster = _joint(
        _h("000000001", "You Harry L.", _SUB_FLOOR, nature="direct"),
        _h("000000009", "Berto Acquisition Sponsor LLC", _SUB_FLOOR, nature="indirect", table_i=False),
    )
    text = (
        'Berto Acquisition Sponsor, LLC (the "Sponsor") is the record holder of the securities '
        "reported herein. Harry L. You is the sole managing member of the Sponsor."
    )
    assert _rep(cluster).filer_name == "Berto Acquisition Sponsor LLC"  # incumbent, by CIK order
    assert _rep_with(cluster, _evidence(text)).filer_name == "Berto Acquisition Sponsor LLC"
    # …and the same text moves the rep when the incumbent is NOT the named holder.
    inverted = _joint(
        _h("000000009", "You Harry L.", _SUB_FLOOR, nature="direct"),
        _h("000000001", "Berto Acquisition Sponsor LLC", _SUB_FLOOR, nature="indirect", table_i=False),
    )
    assert _rep(inverted).filer_name == "You Harry L."
    assert _rep_with(inverted, _evidence(text)).filer_name == "Berto Acquisition Sponsor LLC"


def test_text_naming_two_members_fails_closed() -> None:
    """A control-chain footnote routinely names every tier — ``GEI Capital VI, LLC is the
    general partner of GEI VI`` (``0000950170-24-116635`` F8). Uniqueness is what stops a
    manager being read as the holder, and there is no tie-break to fall back on.

    ⚠ Member order is load-bearing and the first draft of this test got it wrong: the
    LP is listed first so that "take the first named member" would swap. With the
    incumbent listed first, dropping the uniqueness check leaves the same answer and the
    test passes against the defect it exists to catch (revert-probe B, NOT CAUGHT)."""
    cluster = _joint(
        _h("000000001", "Green Equity Investors VI, L.P.", _SUB_FLOOR, nature="indirect"),
        _h("000000009", "GEI Capital VI, LLC", _SUB_FLOOR, nature="indirect"),
    )
    text = "GEI Capital VI, LLC is the general partner of Green Equity Investors VI, L.P."
    assert _rep_with(cluster, _evidence(text)).filer_name == "GEI Capital VI, LLC"  # incumbent kept


def test_text_naming_no_member_keeps_the_incumbent() -> None:
    """Berkshire's Liberty Form 4 names GEICO and National Fire & Marine as the holders —
    subsidiaries that are not reporting owners at all. "Exactly one member named" is
    unsatisfiable there, and the pass must not invent an answer."""
    cluster = _joint(
        _h("000000009", "BUFFETT WARREN E", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "BERKSHIRE HATHAWAY INC", _SUB_FLOOR, nature="indirect"),
    )
    text = "owned by the following subsidiaries: Government Employees Insurance Company"
    assert _rep_with(cluster, _evidence(text)).filer_name == "BUFFETT WARREN E"


def test_evidence_for_a_different_block_value_is_not_consulted() -> None:
    """The value key is load-bearing, not decoration. One Battery Ventures accession names
    BV IX, BIP IX, BP IX, The Lee Family Trust and "Roger H. Lee jointly with his spouse"
    across five footnotes — pooling them per ACCESSION would let another row's record
    holder decide this row's rep."""
    cluster = _joint(
        _h("000000009", "Battery Investment Partners IX, LLC", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "Battery Ventures IX, L.P.", _SUB_FLOOR, nature="indirect"),
    )
    other_row = _evidence("Securities are held by Battery Ventures IX, L.P.", shares=_ROUND)
    assert _rep_with(cluster, other_row).filer_name == "Battery Investment Partners IX, LLC"


def test_table_i_attestation_still_outranks_the_text() -> None:
    """Clause order. Where the Table I ``D`` line IS admissible — a different accession —
    it is the source rule and the free text must not override it. This is also the
    configuration the rule was VALIDATED on, so an inversion here would invalidate the
    measurement it was shipped against."""
    cluster = [
        _h("000000009", "Deemed Owner A", _SUB_FLOOR, nature="indirect"),
        _h("000000008", "Deemed Owner B", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "Sponsor Fund L.P.", _SUB_FLOOR, nature="direct"),
    ]
    evidence = {("acc-000000008", Decimal(_SUB_FLOOR)): ("Securities are held by Deemed Owner B",)}
    assert _rep_with(cluster, evidence).filer_name == "Sponsor Fund L.P."


def test_named_blockholder_row_cannot_become_the_rep() -> None:
    """The cross-channel 13D/G rows a same-accession fold pulls in are named in the very
    same footnotes. The rep must stay insider-sourced or it does not route to the insiders
    slice — the same constraint ``_attested_direct_holders`` carries."""
    cluster = _joint(
        _h("000000009", "Deemed Owner A", _SUB_FLOOR, nature="indirect"),
        _h("000000008", "Deemed Owner B", _SUB_FLOOR, nature="indirect"),
    )
    cluster.append(_h("000000001", "Blockholder Co", _SUB_FLOOR, nature=None, source="13d"))
    evidence = _evidence("Securities are held by Blockholder Co")
    assert _rep_with(cluster, evidence).filer_name == "Deemed Owner A"


def test_text_tier_swap_is_declined_when_the_incumbent_holds_other_channel_rows() -> None:
    """Clause 4 gates BOTH routes. The text tier changes which identity survives into
    owner-once exactly as the Table I tier does, so it inherits the same fail-closed
    posture — #2385 measured 108 instruments' pie totals moving on a rep change."""
    incumbent = _h("000000009", "THRC Management, LLC", _SUB_FLOOR, nature="indirect")
    cluster = _joint(incumbent, _h("000000001", "THRC Holdings, LP", _SUB_FLOOR, nature="indirect"))
    evidence = _evidence("Securities are held by THRC Holdings, LP")
    elsewhere = [_h("000000009", "THRC Management, LLC", "72822917", nature=None, source="13d")]
    assert _rep_with(cluster, evidence).filer_name == "THRC Holdings, LP"
    assert _rep_with(cluster, evidence, elsewhere).filer_name == "THRC Management, LLC"


def test_person_names_are_matched_verbatim_not_rotated() -> None:
    """EDGAR conformed names are ``LAST FIRST``; prose writes ``FIRST LAST``. Adding the
    rotated form was built and PRICED — on the labelled cross-accession set it drops the
    rule from 47 correct swaps of 51 to 106 correct of 124 overall, because it makes the
    deemed owner matchable alongside the holder and collapses uniqueness. Verbatim
    matching is a measured decision, so a future "improvement" must re-run that arm."""
    cluster = _joint(
        _h("000000009", "Sponsor Fund L.P.", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "You Harry L.", _SUB_FLOOR, nature="indirect"),
    )
    evidence = _evidence("The shares are held of record by Harry L. You.")
    assert _rep_with(cluster, evidence).filer_name == "Sponsor Fund L.P."  # incumbent kept


def test_an_abbreviated_conformed_name_still_matches_its_own_footnote() -> None:
    """``LCID`` ``0001104659-24-113592``: EDGAR's conformed name is ``Ayar Third Investment
    Co``; the filing's own footnote says "By Ayar Third Investment Company". Matching on
    token boundaries makes the subsidiary that HOLDS unmatchable and leaves its parent
    ``PUBLIC INVESTMENT FUND`` as the only named member — promoting the parent over the
    record holder, and moving the pie by 280,992,324 shares.

    ⚠ Word-boundary anchoring was implemented and reverted for exactly this. It scored an
    IDENTICAL 47/4 on the labelled set and passed every test then in this file; only the
    paired full-population A/B separated the two spellings."""
    cluster = _joint(
        _h("000000009", "Ayar Third Investment Co", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "PUBLIC INVESTMENT FUND", _SUB_FLOOR, nature="indirect"),
    )
    evidence = _evidence(
        "By Ayar Third Investment Company",
        'Ayar is a wholly-owned subsidiary of Public Investment Fund of Saudi Arabia ("PIF").',
    )
    # Both are named, so the tier declines and the incumbent stands — which is the
    # subsidiary here. Anchoring names only the parent and promotes it.
    assert _rep_with(cluster, evidence).filer_name == "Ayar Third Investment Co"


def test_evidence_is_pooled_across_a_clusters_accessions() -> None:
    """Cross-accession clusters reach this tier too, whenever clause 1 yields no candidate
    — 17 of the 76 folds the A/B moves. There the members file SEPARATELY, so the union of
    their own filings is the evidence about the one block they all restate, and a deemed
    owner's filing naming the holder is exactly the statement the rule wants.

    Pooling is therefore deliberate, and it is also the configuration the labelled-set
    score was measured on (that set is entirely cross-accession), so restricting it would
    ship an unvalidated rule. Pinned because no other test exercises it — every other case
    uses ``_joint`` (review WARNING on PR #2422)."""
    holder = _h("000000001", "Sponsor Fund L.P.", _SUB_FLOOR, nature="indirect")
    deemed = _h("000000009", "Sponsor GP, L.L.C.", _SUB_FLOOR, nature="indirect")
    cluster = [deemed, holder]
    assert deemed.winning_accession != holder.winning_accession
    # The DEEMED owner's own filing names the holder; the holder's filing says nothing.
    evidence = {(deemed.winning_accession, Decimal(_SUB_FLOOR)): ("Securities are held by Sponsor Fund L.P.",)}
    assert _rep(cluster).filer_name == "Sponsor GP, L.L.C."  # incumbent, by CIK order
    assert _rep_with(cluster, evidence).filer_name == "Sponsor Fund L.P."


def test_two_accessions_naming_two_members_fails_closed() -> None:
    """The other half of pooling: co-filers who disagree. Each filing names a different
    member, the union names two, and the uniqueness guard declines — the pooled blob is
    not a licence to pick whichever was read first.

    ⚠ The low-CIK member is listed FIRST, and that is what makes the assertion mean
    anything. With the incumbent listed first, "take the first named member" returns the
    incumbent too, so the test passes against the very defect it names — the third
    appearance of that shape on this ticket (revert probe B, and the sibling
    ``test_text_naming_two_members_fails_closed``), and the second time review had to
    point it out. Probe B now runs this case as well."""
    a = _h("000000001", "Sponsor Fund L.P.", _SUB_FLOOR, nature="indirect")
    b = _h("000000009", "Sponsor GP, L.L.C.", _SUB_FLOOR, nature="indirect")
    evidence = {
        (a.winning_accession, Decimal(_SUB_FLOOR)): ("Securities are held by Sponsor GP, L.L.C.",),
        (b.winning_accession, Decimal(_SUB_FLOOR)): ("Securities are held by Sponsor Fund L.P.",),
    }
    assert _rep_with([a, b], evidence).filer_name == "Sponsor GP, L.L.C."  # incumbent kept


def test_a_sibling_fund_substring_declines_rather_than_guessing() -> None:
    """``LFCR`` / ``NNBR``: one cluster carries ``Legion Partners, L.P. I`` AND
    ``Legion Partners, L.P. II``, and the first normalises to a prefix of the second, so a
    footnote naming fund II also "names" fund I. 6 folds of 1,434 carry a member pair with
    this shape.

    The cost is a LOST promotion, and that is the direction this pass fails in. Pinned so
    the loss stays visible: a future boundary-anchored matcher would turn this into a
    promotion and the ``LCID`` case above into a wrong one, and the trade has to be made
    knowingly."""
    cluster = _joint(
        _h("000000009", "Legion Partners, L.P. I", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "Legion Partners, L.P. II", _SUB_FLOOR, nature="indirect"),
    )
    evidence = _evidence("Securities are held of record by Legion Partners, L.P. II.")
    assert _rep_with(cluster, evidence).filer_name == "Legion Partners, L.P. I"  # incumbent


# ---------------------------------------------------------------------------
# Release hazard: a stranded row is not a release (#2230 residual)
# ---------------------------------------------------------------------------
#
# The fold gate asks whether demoting a member would move its OTHER rows into a
# DIFFERENT wedge, not whether any row is stranded. ``_reconcile_owner_once`` branches
# on ``present & _INSIDER_SOURCES`` — a Section-16 person is classified ``insiders`` and
# its 13F stays a ``dropped_source`` (#1640) — so an identity retaining ANY form4 /
# form3 / def14a row outside the cluster is unchanged by the fold.
#
# Cases are the real refused clusters from
# ``PYTHONPATH=. uv run python -m scripts.audit_2230_release_hazard``.


def _with_stranded(*stranded: Holder) -> list:
    """A 3-member chain plus rows belonging to its non-rep members. ⚠ Values differ from
    the block so the stranded rows do not join the cluster's own value bucket."""
    holders = _chain(_SUB_FLOOR, n=3)
    return holders + list(stranded)


def test_stranded_form4_row_does_not_block_the_fold() -> None:
    """``ESTC`` / ``RYTM`` / ``COUR``: the dominant shape. NEA's managing members restate
    the fund's block INDIRECTLY on Form 3 and each also holds a personal DIRECT stake on
    Form 4. Folding the restatement leaves the personal row, so the identity is still a
    Section-16 person to owner-once — same wedge, 13F still suppressed, nothing released.
    18 of the 21 refused clusters are this shape."""
    personal = _h("000000002", "Sponsor GP 0 L.L.C.", "4155995", nature="direct")
    _out_s, _b, corrs = _reconcile_insider_control_groups(_with_stranded(personal), [])
    assert _kinds(corrs) == ["insider_control_group_collapse"]


def test_stranded_form3_row_does_not_block_the_fold() -> None:
    """``ALTO`` / ``GTES``: same argument, other Section-16 form. ``form3`` is in
    ``_INSIDER_SOURCES``, and ``_merge_section16_forms`` pools it with ``form4``."""
    other = _h("000000002", "Sponsor GP 0 L.L.C.", "64708", nature="direct", source="form3")
    _out_s, _b, corrs = _reconcile_insider_control_groups(_with_stranded(other), [])
    assert _kinds(corrs) == ["insider_control_group_collapse"]


def test_stranded_def14a_row_does_not_block_the_fold() -> None:
    """``ROLR``: ``OEH Invest AB`` strands a proxy row. ``def14a`` is in
    ``_INSIDER_SOURCES`` (though NOT in ``_INSIDER_GROUP_SOURCES``), so it too keeps the
    identity classified ``insiders`` — the two sets are not interchangeable here."""
    proxy = _h("000000002", "Sponsor GP 0 L.L.C.", "2010631", nature=None, source="def14a")
    _out_s, _b, corrs = _reconcile_insider_control_groups(_with_stranded(proxy), [])
    assert _kinds(corrs) == ["insider_control_group_collapse"]


def test_zero_share_stranded_row_still_counts_as_insider_evidence() -> None:
    """``EXE`` / ``ALOY`` / ``UPWK``: the stranded Form 4 reports 0 shares. It is below
    ``_is_eligible``'s positivity bar so it never joins a bucket, but it is still a
    Section-16 row for owner-once's classification, which is what the gate turns on."""
    zeroed = _h("000000002", "Sponsor GP 0 L.L.C.", "0", nature="direct")
    _out_s, _b, corrs = _reconcile_insider_control_groups(_with_stranded(zeroed), [])
    assert _kinds(corrs) == ["insider_control_group_collapse"]


def test_a_13f_row_beside_an_insider_row_does_not_block_the_fold() -> None:
    """``TG`` / ``KG`` shape, taken to its limit: the member strands BOTH a Form 4 row and
    a 13F row. The Form 4 keeps the identity Section-16, so owner-once still routes the
    13F to ``dropped_sources`` rather than the institutions wedge — the release cannot
    happen, and refusing here would be refusing on a hazard that is already fenced."""
    member = ("000000002", "Sponsor GP 0 L.L.C.")
    holders = _with_stranded(
        _h(*member, "524624", nature="direct"),
        _h(*member, "5997453", nature=None, source="13f"),
    )
    _out_s, _b, corrs = _reconcile_insider_control_groups(holders, [])
    assert _kinds(corrs) == ["insider_control_group_collapse"]


def test_stranded_13d_row_still_blocks_the_fold() -> None:
    """``HSIC``: the two KKR partnerships strand ONLY 13D rows. Nothing keeps the identity
    Section-16, so folding moves it to the blockholders wedge at its 13D figure — a real
    release, and one of the 3 refusals that survive."""
    kkr = _h("000000002", "Sponsor GP 0 L.L.C.", "17583918", nature=None, source="13d")
    _out_s, _b, corrs = _reconcile_insider_control_groups(_with_stranded(kkr), [])
    assert corrs == []


def test_stranded_13g_row_still_blocks_the_fold() -> None:
    """``EXE``: ``Blackstone Holdings III L.P.`` strands a 13G row and nothing else."""
    bx = _h("000000002", "Sponsor GP 0 L.L.C.", "10320090", nature=None, source="13g")
    _out_s, _b, corrs = _reconcile_insider_control_groups(_with_stranded(bx), [])
    assert corrs == []


def test_the_two_release_predicates_disagree_exactly_on_insider_rows() -> None:
    """Pins the narrowing itself. ``_releases_other_rows`` stays WIDE — it is what
    ``_select_control_group_rep`` clause 4 was measured under (#2385) and is not changed
    here (#2789) — so the two must diverge on an insider-source row and agree elsewhere."""
    cluster = _chain(_SUB_FLOOR, n=3)
    member = cluster[1]
    idx = _rows_by_identity
    for source, expect_release in (("form4", False), ("form3", False), ("def14a", False), ("13f", True), ("13d", True)):
        elsewhere = [_h(member.filer_cik or "", member.filer_name, "12345", nature=None, source=source)]
        rows = idx(cluster + elsewhere, [])
        assert _releases_other_rows(member, cluster, rows) is True, source
        assert _releases_into_another_wedge(member, cluster, rows) is expect_release, source
    # No rows outside the cluster at all: both predicates agree there is nothing to release.
    rows = idx(cluster, [])
    assert _releases_other_rows(member, cluster, rows) is False
    assert _releases_into_another_wedge(member, cluster, rows) is False
