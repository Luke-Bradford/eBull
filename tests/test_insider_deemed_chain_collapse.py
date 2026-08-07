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

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    Holder,
    SourceTag,
    _collapse_insider_control_group,
    _control_group_rep_key,
    _is_deemed_chain,
    _reconcile_insider_control_groups,
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
    # ``(True, shares, cik, accession)``. #2385 prototyped an extra component and the
    # inline copy would have attached the 13F row to a holder that is no longer the rep
    # — i.e. set up the OPPOSITE scenario from the one this test's name describes, while
    # still asserting on the same output.
    rep = max(holders, key=_control_group_rep_key)
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
    ``_control_group_rep_key``; this pins that they still agree, including on the
    ``sorted(reverse=True)[0]`` vs ``max`` equivalence (review WARNING, PR #2384)."""
    cluster = [
        _h("000000003", "Tie B", _SUB_FLOOR, nature="indirect"),
        _h("000000001", "Sponsor Fund L.P.", _SUB_FLOOR, nature="direct"),
        _h("000000002", "Tie A", _SUB_FLOOR, nature="indirect"),
        _h("000000009", "Blockholder Co", _SUB_FLOOR, nature=None, source="13d"),
    ]
    folded, _corr = _collapse_insider_control_group(cluster)
    preview = max(cluster, key=_control_group_rep_key)
    assert (folded.filer_cik, folded.filer_name) == (preview.filer_cik, preview.filer_name)
    assert sorted(cluster, key=_control_group_rep_key, reverse=True)[0] is preview
