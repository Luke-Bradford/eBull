"""Pure-logic tests for same-accession control-group collapse (#1764).

A joint Form 4/3 (or 13D/G) accession reports the SAME deemed block under ≥2 distinct
reporting owners (the controlling person + the entity, or a fund GP/LP chain) — Rule
16a-1(a)(2) deemed beneficial ownership. A SUM rollup counts the one block N×. This pass
collapses the PRECISE same-(accession, shares) signal to ONE holder with NO magnitude floor
(the shared accession IS the group-membership evidence the fuzzy #1652/#1645 passes must
infer). Restricted to {form4,form3} survivors + {13d,13g} blockholders; never def14a (the
#1659 false-positive class) or 13f. No DB — hand-built ``Holder`` objects.
See docs/specs/etl/2026-06-28-insider-same-accession-control-group-collapse.md.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    Holder,
    SourceTag,
    _reconcile_same_accession_groups,
)

_P = date(2024, 8, 5)
_ACC = "0001104659-24-090956"  # BKTI joint Form 4 — Horowitz (indirect) + Palm Global (direct)


def _h(
    cik: str | None,
    name: str,
    shares: str,
    *,
    source: SourceTag = "form4",
    accession: str = _ACC,
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=f"https://sec.gov/{accession}",
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
    )


def _kinds(corrs: list) -> list[str]:
    return [c.kind for c in corrs]


# ---------------------------------------------------------------------------
# Core collapse — the in-scope win (BKTI)
# ---------------------------------------------------------------------------


def test_insider_same_accession_pair_collapses_to_one() -> None:
    """Horowitz (indirect 90k) + Palm Global (direct 90k) on ONE accession → counted once."""
    survivors = [
        _h("0001104659", "Horowitz Joshua", "90000"),
        _h("0001428336", "Palm Global Small Cap Master Fund LP", "90000"),
        _h("0009999999", "Unrelated Director", "39896", accession="other-acc"),
    ]
    out_s, out_b, corrs = _reconcile_same_accession_groups(survivors, [])
    assert out_b == []
    assert _kinds(corrs) == ["insider_control_group_collapse"]
    block = [h for h in out_s if h.shares == Decimal("90000")]
    assert len(block) == 1  # the 90k block counted ONCE
    assert any(h.filer_name == "Unrelated Director" for h in out_s)  # different accession untouched
    (corr,) = corrs
    assert corr.shares_removed == Decimal("90000")  # one member folded
    assert len(block[0].dropped_sources) == 1  # folded member preserved in dropped_sources


def test_collapse_has_no_magnitude_floor() -> None:
    """A sub-1M AND a round-lot same-accession pair both collapse (the #1652 floor is gone)."""
    survivors = [
        _h("0000000001", "GP LLC", "200000"),  # round multiple of 100k — #1652 would skip
        _h("0000000002", "Managed Fund LP", "200000"),
    ]
    out_s, _out_b, corrs = _reconcile_same_accession_groups(survivors, [])
    assert _kinds(corrs) == ["insider_control_group_collapse"]
    assert len([h for h in out_s if h.shares == Decimal("200000")]) == 1


# ---------------------------------------------------------------------------
# Guards — what must NOT collapse
# ---------------------------------------------------------------------------


def test_same_holder_direct_indirect_not_collapsed() -> None:
    """One person's direct+indirect on one accession (distinct identity = 1) is NOT touched —
    it flows to owner-once's additive SUM unchanged."""
    survivors = [
        _h("0001077430", "GOLDBERG ALAN E", "90000"),
        _h("0001077430", "GOLDBERG ALAN E", "90000"),  # same CIK, same accession
    ]
    out_s, _out_b, corrs = _reconcile_same_accession_groups(survivors, [])
    assert corrs == []
    assert len(out_s) == 2  # both pass through


def test_different_shares_same_accession_not_collapsed() -> None:
    """Two genuinely different positions on one accession (different share counts) never merge."""
    survivors = [
        _h("0000000001", "Holder A", "90000"),
        _h("0000000002", "Holder B", "45000"),
    ]
    out_s, _out_b, corrs = _reconcile_same_accession_groups(survivors, [])
    assert corrs == []
    assert len(out_s) == 2


def test_def14a_same_accession_not_collapsed() -> None:
    """The #1659 FP guard: independent equal-grant execs share ONE proxy accession, so a
    def14a-source pair on one accession must NOT collapse (def14a is non-additive memo, and a
    matched proxy row can reach survivors)."""
    survivors = [
        _h("0000000010", "Koop Bryan", "1875000", source="def14a"),
        _h("0000000011", "LaBelle Douglas", "1875000", source="def14a"),
    ]
    out_s, _out_b, corrs = _reconcile_same_accession_groups(survivors, [])
    assert corrs == []
    assert len(out_s) == 2


def test_empty_accession_does_not_bucket() -> None:
    """Codex ckpt-2 #1: a NULL/'' accession coerces to '' on read; two UNRELATED holders with
    the same exact shares and no accession must NOT bucket as ('', shares) and collapse."""
    survivors = [
        _h("0000000001", "Holder A", "90000", accession=""),
        _h("0000000002", "Holder B", "90000", accession=""),
    ]
    out_s, _out_b, corrs = _reconcile_same_accession_groups(survivors, [])
    assert corrs == []
    assert len(out_s) == 2


def test_cross_channel_blockholder_restatement_consumed() -> None:
    """Codex ckpt-2 #2: a folded insider member that ALSO restates the SAME block on a 13D/G
    must have that blockholder row consumed (folded into the insider rep), not orphaned into the
    blockholders wedge where owner-once would re-count it."""
    survivors = [
        _h("0001104659", "Horowitz Joshua", "90000"),
        _h("0001428336", "Palm Global LP", "90000"),
    ]
    # Palm Global also files a 13D for the same 90k block (different accession).
    blockholders = [_h("0001428336", "Palm Global LP", "90000", source="13d", accession="0001-13d")]
    out_s, out_b, corrs = _reconcile_same_accession_groups(survivors, blockholders)
    assert _kinds(corrs) == ["insider_control_group_collapse"]
    assert out_b == []  # the cross-channel 13D restatement was consumed, not orphaned
    block = [h for h in out_s if h.shares == Decimal("90000")]
    assert len(block) == 1
    (corr,) = corrs
    # two folded members (the other insider + the cross-channel 13D), both at 90k
    assert corr.shares_removed == Decimal("180000")
    assert len(block[0].dropped_sources) == 2


def test_blockholder_at_different_value_not_consumed() -> None:
    """A folded insider member's 13D at a DIFFERENT (larger full-group) value is a separate
    position — it must stay in blockholders, not be consumed by the same-accession insider pass."""
    survivors = [
        _h("0000000001", "GP LLC", "90000"),
        _h("0000000002", "Managed Fund LP", "90000"),
    ]
    blockholders = [_h("0000000002", "Managed Fund LP", "250000", source="13d", accession="0001-13d")]
    _out_s, out_b, corrs = _reconcile_same_accession_groups(survivors, blockholders)
    assert _kinds(corrs) == ["insider_control_group_collapse"]
    assert len(out_b) == 1  # the 250k block is a different position, untouched
    assert out_b[0].shares == Decimal("250000")


def test_13f_same_accession_not_collapsed() -> None:
    """13F survivors are excluded (one filer per accession by construction)."""
    survivors = [
        _h("0000000020", "Manager One", "500000", source="13f"),
        _h("0000000021", "Manager Two", "500000", source="13f"),
    ]
    out_s, _out_b, corrs = _reconcile_same_accession_groups(survivors, [])
    assert corrs == []
    assert len(out_s) == 2


# ---------------------------------------------------------------------------
# Blockholder channel
# ---------------------------------------------------------------------------


def test_blockholder_same_accession_pair_collapses() -> None:
    """A 13D/G joint accession listing 2 reporters at the same stake → counted once at MAX."""
    blockholders = [
        _h("0000000030", "Activist GP", "154000", source="13d"),
        _h("0000000031", "Activist Fund LP", "154000", source="13d"),
    ]
    out_s, out_b, corrs = _reconcile_same_accession_groups([], blockholders)
    assert out_s == []
    assert _kinds(corrs) == ["blockholder_group_collapse"]
    assert len([h for h in out_b if h.shares == Decimal("154000")]) == 1
    (corr,) = corrs
    assert corr.shares_removed == Decimal("154000")
