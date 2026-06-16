"""Pure-logic tests for 13D/G group collapse (#1645).

A Rule 13d-5 group's members each report the identical aggregate group stake on
separate accessions/CIKs; without this pass they sum N× in the blockholders pie
wedge. No DB — operates on hand-built ``Holder`` objects. See
docs/specs/etl/2026-06-16-blockholder-13d-group-collapse.md.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    DroppedSource,
    Holder,
    SourceTag,
    _is_group_block,
    _reconcile_13d_groups,
)

_P = date(2025, 2, 18)  # a shared period_end


def _h(
    cik: str | None,
    name: str,
    shares: str,
    *,
    source: SourceTag = "13d",
    as_of: date | None = _P,
    accession: str | None = None,
    dropped: tuple = (),
) -> Holder:
    acc = accession or f"acc-{cik}"
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=acc,
        winning_edgar_url=f"https://sec.gov/{acc}",
        as_of_date=as_of,
        filer_type=None,
        dropped_sources=dropped,
    )


# ---------------------------------------------------------------------------
# _is_group_block — round-lot coincidence guard
# ---------------------------------------------------------------------------


def test_is_group_block_truth_table() -> None:
    # Precise figures (group evidence) → True (collapsible).
    for precise in ("7720340", "6016847", "2724075", "94021358", "2486052"):
        assert _is_group_block(Decimal(precise)) is True, precise
    # Whole multiples of 100,000 (round-lot coincidence) → False (keep separate).
    for round_lot in ("700000", "1000000", "100000", "4000000", "4100000"):
        assert _is_group_block(Decimal(round_lot)) is False, round_lot


# ---------------------------------------------------------------------------
# Collapse — the in-scope wins
# ---------------------------------------------------------------------------


def test_ske_two_member_group_collapses_to_max() -> None:
    """SKE: Orion 7,720,340 + Selwyn 7,720,339 (1-share sliver) → one holder at
    MAX, the other folded, one correction."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0001714217", "Orion Resource Partners", "7720340", source="13g"),
            _h("0001404077", "Selwyn Lower Holdings", "7720339", source="13g"),
        ],
        frozenset(),
    )
    assert len(survivors) == 1
    rep = survivors[0]
    assert rep.shares == Decimal("7720340")  # MAX
    assert rep.filer_cik == "0001714217"  # Orion = max-share rep
    assert [d.shares for d in rep.dropped_sources] == [Decimal("7720339")]
    assert len(corrections) == 1
    corr = corrections[0]
    assert corr.kind == "blockholder_group_collapse"
    assert corr.shares_removed == Decimal("7720339")  # the eliminated double-count
    assert corr.filer_cik == "0001714217"
    assert corr.winning_source == "13g"
    assert "Rule 13d-5 group" in corr.detail


def test_rime_exact_match_collapses() -> None:
    """RIME: L1 + S.H.N. both exactly 6,016,847 (non-round) → collapse."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0001702202", "L1 Capital", "6016847", source="13g"),
            _h("0001890802", "S.H.N. Financial", "6016847", source="13g"),
        ],
        frozenset(),
    )
    assert len(survivors) == 1
    assert survivors[0].shares == Decimal("6016847")
    assert corrections[0].shares_removed == Decimal("6016847")


def test_three_member_group_collapses_once() -> None:
    """A 3-member group (TKO shape) folds to one MAX holder with two dropped
    sources and a single correction; shares_removed = sum of the two losers."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0001320234", "Ariel Emanuel", "94081732"),
            _h("0001766363", "Endeavor", "94021358"),
            _h("0001868088", "Silver Lake", "94021358"),
        ],
        frozenset(),
    )
    assert len(survivors) == 1
    rep = survivors[0]
    assert rep.shares == Decimal("94081732")  # MAX (Ariel, the sliver)
    assert rep.filer_cik == "0001320234"
    assert len(rep.dropped_sources) == 2
    assert len(corrections) == 1
    assert corrections[0].shares_removed == Decimal("94021358") + Decimal("94021358")


# ---------------------------------------------------------------------------
# Keep separate — the conservative guards
# ---------------------------------------------------------------------------


def test_round_lot_exact_match_kept_separate() -> None:
    """Two independents at exactly 700,000 (round) → NOT collapsed."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0001910592", "Harraden Circle", "700000", source="13g"),
            _h("0001569241", "Newtyn Management", "700000", source="13g"),
        ],
        frozenset(),
    )
    assert len(survivors) == 2
    assert corrections == []


def test_loose_tolerance_distinct_values_kept_separate() -> None:
    """Two non-round holders 0.9% apart (> 0.1% tolerance) → NOT collapsed."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0000000001", "A", "7720340", source="13g"),
            _h("0000000002", "B", "7650007", source="13g"),  # 0.9% below, non-round
        ],
        frozenset(),
    )
    assert len(survivors) == 2
    assert corrections == []


def test_lone_filer_untouched() -> None:
    survivors, corrections = _reconcile_13d_groups([_h("0000000001", "Solo Activist", "5123456")], frozenset())
    assert len(survivors) == 1
    assert survivors[0].shares == Decimal("5123456")
    assert corrections == []


def test_survivor_overlap_member_excluded_from_clustering() -> None:
    """A member whose CIK is in survivor_keys (also files Form 4 / 13F) is
    excluded from clustering and passes through — owner-once reconciles it
    cross-channel. An all-overlap near-equal pair yields no collapse."""
    a = _h("0001320234", "Insider Co", "6016847", source="13g")
    b = _h("0001890802", "Insider Co 2", "6016847", source="13g")
    keys = frozenset({"CIK:0001320234", "CIK:0001890802"})
    survivors, corrections = _reconcile_13d_groups([a, b], keys)
    assert len(survivors) == 2
    assert corrections == []


def test_partial_overlap_only_non_overlap_pair_collapses() -> None:
    """3 near-equal members, one in survivor_keys: it is excluded; the other two
    (non-overlap) collapse."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0000000001", "Fund A", "6016847", source="13g"),
            _h("0000000002", "Fund B", "6016847", source="13g"),
            _h("0001320234", "Insider C", "6016847", source="13g"),  # excluded
        ],
        frozenset({"CIK:0001320234"}),
    )
    # Insider C passes through; A+B collapse to one.
    assert len(survivors) == 2  # collapsed{A,B} + passthrough C
    assert len(corrections) == 1
    assert corrections[0].shares_removed == Decimal("6016847")


def test_different_period_end_not_collapsed() -> None:
    """Same near-equal aggregate but different period_end → NOT one group."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0000000001", "A", "6016847", source="13g", as_of=date(2025, 1, 13)),
            _h("0000000002", "B", "6016847", source="13g", as_of=date(2025, 2, 18)),
        ],
        frozenset(),
    )
    assert len(survivors) == 2
    assert corrections == []


def test_null_period_and_nonpositive_shares_passthrough() -> None:
    """A null-period row and a shares<=0 row are never clustered."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0000000001", "A", "6016847", source="13g", as_of=None),
            _h("0000000002", "B", "6016847", source="13g", as_of=None),
            _h("0000000003", "Zero", "0", source="13g"),
        ],
        frozenset(),
    )
    assert len(survivors) == 3
    assert corrections == []


# ---------------------------------------------------------------------------
# Max-anchored clustering — exact partition, no transitive chaining
# ---------------------------------------------------------------------------


def test_max_anchored_ladder_exact_partition() -> None:
    """Ladder a=10,000,007 / b=9,995,000 / c=9,991,000 / d=9,980,000 at 0.1%: a, b,
    c are all within the loose band of the max a; d (0.2% below a) is not — even
    though d is within 0.11% of c, which a transitive/nearest-neighbour chainer
    would chain in. Max-anchored (seed=max) partitions as the 3-member {a,b,c}
    collapsed + {d} untouched. Assert the precise partition, not merely 'rejects
    chain'."""
    a = _h("0000000001", "A", "10000007", source="13g")
    b = _h("0000000002", "B", "9995000", source="13g")
    c = _h("0000000003", "C", "9991000", source="13g")
    d = _h("0000000004", "D", "9980000", source="13g")
    survivors, corrections = _reconcile_13d_groups([d, b, a, c], frozenset())  # unsorted input
    assert len(survivors) == 2  # collapsed {a,b,c} + standalone d
    assert len(corrections) == 1
    rep = next(s for s in survivors if s.dropped_sources)
    assert rep.shares == Decimal("10000007")  # a = max
    assert sorted(dr.shares for dr in rep.dropped_sources) == [Decimal("9991000"), Decimal("9995000")]
    standalone = next(s for s in survivors if not s.dropped_sources)
    assert standalone.shares == Decimal("9980000")  # d left separate (max-anchored)


def test_round_seed_does_not_dissolve_nonround_group() -> None:
    """Codex ckpt-2 HIGH: a round seed (1,000,000) must NOT anchor a cluster and
    swallow a genuine non-round sub-group only to dissolve it on the roundness
    check. Input [1,000,000 round, 999,999, 999,998]: the round value passes through
    standalone; the non-round near-exact pair {999,999, 999,998} still collapses."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0000000001", "Round Independent", "1000000", source="13g"),
            _h("0000000002", "Group A", "999999", source="13g"),
            _h("0000000003", "Group B", "999998", source="13g"),
        ],
        frozenset(),
    )
    assert len(survivors) == 2  # round standalone + collapsed pair
    assert len(corrections) == 1
    rep = next(s for s in survivors if s.dropped_sources)
    assert rep.shares == Decimal("999999")
    standalone = next(s for s in survivors if not s.dropped_sources)
    assert standalone.shares == Decimal("1000000")


def test_two_member_loose_pair_not_collapsed() -> None:
    """Codex ckpt-2 MED: two independent non-round holders 0.015% apart (within the
    loose 0.1% band but outside the tight 2-member gate) must NOT collapse — a
    2-member near-equal is the coincidence-prone case."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0000000001", "Independent A", "5001999", source="13g"),
            _h("0000000002", "Independent B", "5001234", source="13g"),
        ],
        frozenset(),
    )
    assert len(survivors) == 2
    assert corrections == []


def test_null_cik_excluded_from_clustering() -> None:
    """Codex ckpt-2 MED: a group inference needs distinct CIK evidence. Two
    null-CIK rows (natural persons / unresolved filers) are excluded from
    clustering and pass through, even with identical non-round aggregates."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h(None, "Legacy A", "6016847", source="13g"),
            _h(None, "Legacy B", "6016847", source="13g"),
        ],
        frozenset(),
    )
    assert len(survivors) == 2
    assert corrections == []


def test_existing_dropped_sources_preserved() -> None:
    """The rep's pre-existing dropped_sources (amendment chain from
    _dedup_within_source) are preserved — appended to, not replaced."""
    rep = _h(
        "0001714217",
        "Orion",
        "7720340",
        source="13g",
        dropped=(
            # a prior 13D/A amendment collapsed upstream by _dedup_within_source
            DroppedSource(
                source="13g",
                accession_number="prior-amendment",
                shares=Decimal("7000000"),
                as_of_date=date(2024, 6, 30),
                edgar_url="https://sec.gov/prior",
            ),
        ),
    )
    other = _h("0001404077", "Selwyn", "7720339", source="13g")
    survivors, _ = _reconcile_13d_groups([rep, other], frozenset())
    collapsed = survivors[0]
    accs = [d.accession_number for d in collapsed.dropped_sources]
    assert "prior-amendment" in accs  # preserved
    assert "acc-0001404077" in accs  # folded member appended


def test_round_member_does_not_inflate_cluster_count() -> None:
    """Codex confirm-pass MED: a round lower member must not be pulled into a
    non-round seed's cluster and inflate the count to ≥3, bypassing the tight
    2-member gate. Input [5,001,999, 5,001,234, 5,000,000(round)]: the round member
    passes through; the remaining {5,001,999, 5,001,234} is a 2-distinct-CIK cluster
    0.0153% apart → fails the tight gate → no collapse."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0000000001", "A", "5001999", source="13g"),
            _h("0000000002", "B", "5001234", source="13g"),
            _h("0000000003", "C round", "5000000", source="13g"),
        ],
        frozenset(),
    )
    assert len(survivors) == 3
    assert corrections == []


def test_duplicate_cik_not_collapsed() -> None:
    """Codex confirm-pass LOW: two rows sharing one reporter CIK are not distinct-CIK
    evidence of a Rule 13d-5 group (and `_dedup_within_source` collapses them
    upstream anyway). The tier is keyed on distinct CIKs, so this does not collapse."""
    survivors, corrections = _reconcile_13d_groups(
        [
            _h("0001", "Same Filer", "6016847", source="13g", accession="acc-a"),
            _h("0001", "Same Filer", "6016847", source="13g", accession="acc-b"),
        ],
        frozenset(),
    )
    assert corrections == []
