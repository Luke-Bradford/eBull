"""Pure-logic tests for insider control-group collapse (#1652).

A sponsor's GP/LP chain Form-4s the SAME deemed block under many related CIKs (and
restates it on 13D/G), so the insiders pie wedge explodes past 100% of float. This
pass collapses the cross-channel union by EXACT non-round value (≥1M) to ONE insiders
holder, removing consumed rows from BOTH the survivors and blockholders lists so a
consumed CIK's exact-block 13D cannot resurface. No DB — hand-built ``Holder`` objects.
See docs/specs/etl/2026-06-16-insider-control-group-collapse.md.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    DroppedSource,
    Holder,
    SourceTag,
    _reconcile_insider_control_groups,
)

_P = date(2025, 5, 8)
_BLOCK = "45026743"  # AMTM Lindsay Goldberg deemed block — exact, non-round, ≥1M


def _h(
    cik: str | None,
    name: str,
    shares: str,
    *,
    source: SourceTag = "form4",
    as_of: date | None = _P,
    accession: str | None = None,
    dropped: tuple = (),
) -> Holder:
    acc = accession or f"acc-{cik}-{source}"
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


def _kinds(corrs: list) -> list[str]:
    return [c.kind for c in corrs]


# ---------------------------------------------------------------------------
# Core collapse — the in-scope wins
# ---------------------------------------------------------------------------


def test_form4_chain_collapses_to_one_insider() -> None:
    """~3 GP/LP entities each Form-4 the same 45,026,743 block → one insiders holder."""
    survivors = [
        _h("0000000001", "LG GP Holding IV LLC", _BLOCK),
        _h("0000000002", "Lindsay Goldberg IV L.P.", _BLOCK),
        _h("0000000003", "Lindsay Goldberg V L.P.", _BLOCK),
        _h("0000000099", "Real Director", "726198"),  # genuine, below floor, untouched
    ]
    out_s, out_b, corrs = _reconcile_insider_control_groups(survivors, [])
    assert out_b == []
    assert _kinds(corrs) == ["insider_control_group_collapse"]
    block_holders = [h for h in out_s if h.shares == Decimal(_BLOCK)]
    assert len(block_holders) == 1  # counted once
    assert any(h.filer_name == "Real Director" for h in out_s)  # genuine survives
    (corr,) = corrs
    assert corr.shares_removed == Decimal(_BLOCK) * 2  # two folded members
    assert len(block_holders[0].dropped_sources) == 2


def test_within_holder_direct_indirect_both_fold() -> None:
    """A control person reporting the block as both direct and indirect (two survivor
    rows, same CIK) folds both — no #905 additive double (gotcha b)."""
    survivors = [
        _h("0001077430", "GOLDBERG ALAN E", _BLOCK, accession="acc-direct"),
        _h("0001077430", "GOLDBERG ALAN E", _BLOCK, accession="acc-indirect"),
        _h("0000000002", "Lindsay Goldberg IV L.P.", _BLOCK),
    ]
    out_s, _out_b, corrs = _reconcile_insider_control_groups(survivors, [])
    block_holders = [h for h in out_s if h.shares == Decimal(_BLOCK)]
    assert len(block_holders) == 1
    # 3 members (2 Alan rows + 1 LP), rep is one of them, two folded.
    (corr,) = corrs
    assert corr.shares_removed == Decimal(_BLOCK) * 2


def test_cross_channel_no_resurrection() -> None:
    """Codex ckpt-1 TEST GAP: a control group with overlapping CIKs on Form 4 AND 13D
    at the block value → collapses to ONE insiders rep; the exact-block 13D rows are
    REMOVED from blockholders (consumed, not orphaned), so #1645/owner-once cannot
    re-count the block."""
    survivors = [
        _h("0000000001", "Apax IX GP", _BLOCK),
        _h("0000000002", "Triton LuxTopHolding", _BLOCK),
    ]
    blockholders = [
        _h("0000000001", "Apax IX GP", _BLOCK, source="13d", as_of=date(2025, 5, 10)),
        _h("0000000002", "Triton LuxTopHolding", _BLOCK, source="13d", as_of=date(2025, 5, 10)),
    ]
    out_s, out_b, corrs = _reconcile_insider_control_groups(survivors, blockholders)
    assert out_b == []  # 13D rows consumed, not resurrected
    block_holders = [h for h in out_s if h.shares == Decimal(_BLOCK)]
    assert len(block_holders) == 1
    assert block_holders[0].winning_source in ("form4", "form3")  # classified insiders
    (corr,) = corrs
    # 4 members total (2 form4 + 2 13d), rep is one form4 → 3 folded.
    assert corr.shares_removed == Decimal(_BLOCK) * 3


def test_rep_prefers_insider_source() -> None:
    """A mixed bucket's rep is a Form-4 member (so it routes to the insiders slice),
    even when a 13D member sorts higher on CIK."""
    survivors = [_h("0000000001", "Fund GP", _BLOCK, source="form4")]
    blockholders = [_h("0000000009", "Fund 13D", _BLOCK, source="13d", as_of=date(2025, 5, 9))]
    out_s, out_b, corrs = _reconcile_insider_control_groups(survivors, blockholders)
    (rep,) = [h for h in out_s if h.shares == Decimal(_BLOCK)]
    assert rep.winning_source == "form4"
    assert rep.filer_cik == "0000000001"
    assert out_b == []
    assert len(corrs) == 1


def test_non_exact_13d_residual_stays_for_1645() -> None:
    """Exact-value consumption only: a consumed CIK's 13D at a DIFFERENT (larger) value
    is left in blockholders for #1645/owner-once (folding it would delete the genuine
    larger block — Codex ckpt-1 MED)."""
    survivors = [
        _h("0000000001", "Silver Lake form4", _BLOCK),
        _h("0000000002", "Endeavor form4", _BLOCK),
    ]
    residual = _h("0000000001", "Silver Lake 13D", "94021358", source="13d", as_of=date(2025, 5, 1))
    out_s, out_b, corrs = _reconcile_insider_control_groups(survivors, [residual])
    assert len(corrs) == 1
    assert out_b == [residual]  # the larger, different-valued 13D survives untouched


def test_rep_residual_split_documented() -> None:
    """Codex ckpt-2 characterization: both members of a collapsed V-bucket ALSO share a
    larger residual W 13D. This pass collapses V to one rep and leaves BOTH W rows in
    blockholders (exact-value consumption). The W rows are NOT consumed here — the
    fragment-vs-block split they later cause (W in insiders via the rep + W in blockholders
    via the folded member) is the deferred cross-channel residual, pre-existing and not
    worsened (pre-fix both founders already counted 2×W). Only dev instance is GDRX, which
    renders no_data. This test pins the unit boundary so a future fix has an anchor."""
    v, w = "2632721", "5391994"  # GDRX Hirsch/Bezdek shape (both ≥1M, non-round)
    survivors = [_h("0001822522", "Hirsch", v), _h("0001822104", "Bezdek", v)]
    blockholders = [
        _h("0001822522", "Hirsch 13D", w, source="13d", as_of=date(2025, 5, 1)),
        _h("0001822104", "Bezdek 13D", w, source="13d", as_of=date(2025, 5, 1)),
    ]
    out_s, out_b, corrs = _reconcile_insider_control_groups(survivors, blockholders)
    assert len(corrs) == 1  # V collapsed once
    assert {h.shares for h in out_s} == {Decimal(v)}  # one rep at V in survivors
    assert [h.shares for h in out_b] == [Decimal(w), Decimal(w)]  # both W left for #1645
    assert out_b == blockholders  # W rows untouched by this pass


# ---------------------------------------------------------------------------
# Negatives — conservative guards
# ---------------------------------------------------------------------------


def test_below_magnitude_floor_not_collapsed() -> None:
    """Coincidental equal small grants (non-round but <1M) are NOT a deemed block."""
    survivors = [
        _h("0000000001", "Director A", "101107"),
        _h("0000000002", "Director B", "101107"),
        _h("0000000003", "Director C", "101107"),
    ]
    out_s, _out_b, corrs = _reconcile_insider_control_groups(survivors, [])
    assert corrs == []
    assert len(out_s) == 3  # all pass through


def test_round_value_not_collapsed() -> None:
    """A round shared figure (whole multiple of 100,000) is a plausible independent
    coincidence → kept separate."""
    survivors = [
        _h("0000000001", "Holder A", "5000000"),
        _h("0000000002", "Holder B", "5000000"),
    ]
    _out_s, _out_b, corrs = _reconcile_insider_control_groups(survivors, [])
    assert corrs == []


def test_lone_cik_not_collapsed() -> None:
    """A single CIK at the block value (even two same-value rows) is not a group."""
    survivors = [
        _h("0000000001", "Solo", _BLOCK, accession="a"),
        _h("0000000001", "Solo", _BLOCK, accession="b"),
    ]
    _out_s, _out_b, corrs = _reconcile_insider_control_groups(survivors, [])
    assert corrs == []


def test_pure_13d_bucket_left_for_1645() -> None:
    """A purely-13D/G bucket (no Form-4 footprint) is untouched here — handed to
    _reconcile_13d_groups (#1645)."""
    blockholders = [
        _h("0000000001", "Orion", "7720340", source="13d"),
        _h("0000000002", "Selwyn", "7720340", source="13d"),
    ]
    out_s, out_b, corrs = _reconcile_insider_control_groups([], blockholders)
    assert corrs == []
    assert out_s == []
    assert out_b == blockholders  # untouched, flows to #1645


def test_distinct_exact_values_stay_separate() -> None:
    """Two different exact blocks in one instrument → two buckets, two collapses."""
    survivors = [
        _h("0000000001", "Lindsay A", "45026743"),
        _h("0000000002", "Lindsay B", "45026743"),
        _h("0000000003", "American A", "43893904", source="form3"),
        _h("0000000004", "American B", "43893904", source="form3"),
    ]
    _out_s, _out_b, corrs = _reconcile_insider_control_groups(survivors, [])
    assert len(corrs) == 2
    removed = sorted(c.shares_removed for c in corrs)
    assert removed == [Decimal("43893904"), Decimal("45026743")]


def test_null_cik_and_nonpositive_never_clustered() -> None:
    survivors = [
        _h(None, "No CIK 1", _BLOCK),
        _h(None, "No CIK 2", _BLOCK),
        _h("0000000003", "Zero", "0"),
    ]
    out_s, _out_b, corrs = _reconcile_insider_control_groups(survivors, [])
    assert corrs == []
    assert len(out_s) == 3


def test_13f_survivor_not_pulled_in() -> None:
    """A 13F survivor at the block value is NOT clustered (source scope: form4/form3 +
    13d/13g only)."""
    survivors = [
        _h("0000000001", "Fund GP form4", _BLOCK),
        _h("0000000002", "Manager 13F", _BLOCK, source="13f"),
    ]
    out_s, _out_b, corrs = _reconcile_insider_control_groups(survivors, [])
    assert corrs == []  # only one eligible (insider) member → no ≥2-CIK cluster
    assert any(h.winning_source == "13f" for h in out_s)


def test_existing_dropped_sources_preserved() -> None:
    """The rep's existing dropped_sources (amendment chain) are appended to, never
    replaced."""
    prior = (
        DroppedSource(
            source="form4",
            accession_number="old",
            shares=Decimal(_BLOCK),
            as_of_date=_P,
            edgar_url=None,
        ),
    )
    survivors = [
        _h("0000000009", "Rep", _BLOCK, accession="zzz", dropped=prior),  # high CIK → rep
        _h("0000000001", "Member", _BLOCK),
    ]
    out_s, _out_b, _corrs = _reconcile_insider_control_groups(survivors, [])
    (rep,) = [h for h in out_s if h.shares == Decimal(_BLOCK)]
    assert prior[0] in rep.dropped_sources  # preserved
    assert len(rep.dropped_sources) == 2  # prior + one folded member
