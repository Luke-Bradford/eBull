"""Pure-logic tests for owner-identity reconciliation (#1640).

One beneficial owner is counted ONCE across filing channels, classified by
most-specific role, at their total beneficial ownership. No DB — operates on
hand-built ``Holder`` objects. See
docs/specs/etl/2026-06-15-ownership-owner-once-dedup.md.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.ownership_rollup import (
    Holder,
    SourceTag,
    _reconcile_owner_once,
    _source_rows_and_total,
)


def _h(
    cik: str | None,
    name: str,
    source: SourceTag,
    shares: str,
    *,
    filer_type: str | None = None,
    accession: str = "0000000000-00-000000",
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=f"https://sec.gov/{accession}",
        as_of_date=date(2026, 1, 1),
        filer_type=filer_type,
        dropped_sources=(),
    )


def _only(holders: list[Holder]) -> Holder:
    assert len(holders) == 1, f"expected exactly one holder, got {len(holders)}"
    return holders[0]


def test_insider_plus_13d_same_cik_counted_once_in_insiders() -> None:
    """Cohen-on-GME: Form 4 (38.35M) + 13D (36.85M), same CIK. Counted once
    in insiders at MAX (38.35M, from Form 4); 13D demoted to dropped_source;
    NO blockholders row."""
    out = _reconcile_owner_once(
        [
            _h("0001767470", "Cohen Ryan", "form4", "38347842", accession="a-form4"),
            _h("0001767470", "Cohen Ryan", "13d", "36847842", accession="a-13d"),
        ]
    )
    cohen = _only(out["insiders"])
    assert cohen.shares == Decimal("38347842")
    assert cohen.winning_source == "form4"
    assert [d.source for d in cohen.dropped_sources] == ["13d"]
    assert out["blockholders"] == []


def test_max_keeps_larger_beneficial_even_when_from_13d() -> None:
    """Form 4 30M, 13D 40M, same CIK insider. Role → insiders, figure → MAX
    (40M, from the 13D); Form 4 becomes the dropped_source. The larger
    beneficial number is never discarded (#837 intent)."""
    out = _reconcile_owner_once(
        [
            _h("0000000001", "Director Jane", "form4", "30000000", accession="a-f4"),
            _h("0000000001", "Director Jane", "13d", "40000000", accession="a-13d"),
        ]
    )
    jane = _only(out["insiders"])
    assert jane.shares == Decimal("40000000")
    assert jane.winning_source == "13d"
    assert [d.source for d in jane.dropped_sources] == ["form4"]


def test_form4_def14a_13d_three_way_counted_once() -> None:
    """Same CIK in three beneficial channels: form4 Σ(direct+indirect)=38M,
    def14a=39M, 13d=37M → one insiders holder @ 39M (def14a max); form4 (both
    nature rows) + 13d as dropped_sources."""
    out = _reconcile_owner_once(
        [
            _h("0000000002", "Owner Bob", "form4", "30000000", accession="f4"),  # direct
            _h("0000000002", "Owner Bob", "form4", "8000000", accession="f4"),  # indirect
            _h("0000000002", "Owner Bob", "def14a", "39000000", accession="d14"),
            _h("0000000002", "Owner Bob", "13d", "37000000", accession="13d"),
        ]
    )
    bob = _only(out["insiders"])
    assert bob.shares == Decimal("39000000")
    assert bob.winning_source == "def14a"
    dropped = {d.source for d in bob.dropped_sources}
    assert dropped == {"form4", "13d"}


def test_form4_direct_plus_indirect_preserved_single_source() -> None:
    """One CIK, form4 direct + indirect, no other channel → both rows survive
    (separate Section-16 lines, #905), slice total = sum, no dropped sources."""
    out = _reconcile_owner_once(
        [
            _h("0000000003", "Insider Sam", "form4", "30000000", accession="f4"),
            _h("0000000003", "Insider Sam", "form4", "8000000", accession="f4"),
        ]
    )
    rows = out["insiders"]
    assert len(rows) == 2
    assert sum((h.shares for h in rows), Decimal(0)) == Decimal("38000000")
    assert all(h.dropped_sources == () for h in rows)


def test_13f_managed_assets_never_added_to_insider() -> None:
    """Pathological same-CIK form4 (5M personal) + 13f (900M managed). The
    insider is counted at their BENEFICIAL 5M; the 900M managed book is a
    dropped_source, NOT added — managed assets never inflate an insider."""
    out = _reconcile_owner_once(
        [
            _h("0000000004", "Manager Mo", "form4", "5000000", accession="f4"),
            _h("0000000004", "Manager Mo", "13f", "900000000", accession="13f"),
        ]
    )
    mo = _only(out["insiders"])
    assert mo.shares == Decimal("5000000")
    assert mo.winning_source == "form4"
    assert [d.source for d in mo.dropped_sources] == ["13f"]
    assert out["institutions"] == []
    assert out["etfs"] == []


def test_13g_and_13f_same_cik_counted_once_institution() -> None:
    """Vanguard-shape: 13G (22M beneficial) + 13F (21.8M, ETF filer_type),
    same CIK. Not an insider; has 13F → bucket by 13F filer_type (etfs).
    Counted once at MAX (22M, the 13G); 13F → dropped_source."""
    out = _reconcile_owner_once(
        [
            _h("0000102909", "VANGUARD GROUP INC", "13g", "22000000", accession="13g"),
            _h(
                "0000102909",
                "VANGUARD GROUP INC",
                "13f",
                "21800000",
                filer_type="ETF",
                accession="13f",
            ),
        ]
    )
    vg = _only(out["etfs"])
    assert vg.shares == Decimal("22000000")
    assert vg.winning_source == "13g"
    assert [d.source for d in vg.dropped_sources] == ["13f"]
    assert out["blockholders"] == []


def test_13f_etf_and_non_etf_same_cik_bucket_by_largest() -> None:
    """Same CIK with an ETF 13F row and a larger non-ETF 13F row → lands in
    institutions (largest 13F row drives the bucket). 13F natures overlap (one
    economic position), so the owner is counted once at MAX, not summed."""
    out = _reconcile_owner_once(
        [
            _h("0000000005", "Mixed Mgr", "13f", "10000000", filer_type="ETF", accession="13f"),
            _h("0000000005", "Mixed Mgr", "13f", "90000000", filer_type="INV", accession="13f"),
        ]
    )
    assert out["etfs"] == []
    mgr = _only(out["institutions"])
    assert mgr.shares == Decimal("90000000")  # MAX, not 100M summed


def test_def14a_beneficial_and_voting_not_summed() -> None:
    """Codex #1640 ckpt-2 F3: DEF 14A ``beneficial`` and ``voting`` rows for
    the same holder are the SAME shares through two lenses — overlapping, not
    additive. The owner is counted ONCE at MAX (5M), not 5M + 4M summed."""
    out = _reconcile_owner_once(
        [
            _h("0000000007", "Proxy Holder", "def14a", "5000000", accession="d14"),  # beneficial
            _h("0000000007", "Proxy Holder", "def14a", "4000000", accession="d14"),  # voting
        ]
    )
    holder = _only(out["insiders"])
    assert holder.shares == Decimal("5000000")  # MAX, not 9,000,000
    assert holder.dropped_sources == ()  # same source, no cross-channel drop


def test_losing_form4_dropped_as_one_source_subtotal() -> None:
    """Codex #1640 ckpt-2 F1: when a Form 4 (direct 30M + indirect 8M) LOSES
    to a larger 13D, it becomes ONE dropped_source carrying the Section-16
    subtotal (38M), not two per-row entries that drop the indirect 8M."""
    out = _reconcile_owner_once(
        [
            _h("0000000008", "Owner Kay", "form4", "30000000", accession="f4"),  # direct
            _h("0000000008", "Owner Kay", "form4", "8000000", accession="f4"),  # indirect
            _h("0000000008", "Owner Kay", "13d", "45000000", accession="13d"),
        ]
    )
    kay = _only(out["insiders"])
    assert kay.shares == Decimal("45000000")  # 13D wins (MAX)
    assert kay.winning_source == "13d"
    f4_dropped = [d for d in kay.dropped_sources if d.source == "form4"]
    assert len(f4_dropped) == 1
    assert f4_dropped[0].shares == Decimal("38000000")  # direct + indirect, not just 30M


def test_source_rows_and_total_helper() -> None:
    """Direct table test of the additive-vs-overlapping split."""
    f4 = [_h("c", "n", "form4", "30000000"), _h("c", "n", "form4", "8000000")]
    rows, total = _source_rows_and_total("form4", f4)
    assert len(rows) == 2 and total == Decimal("38000000")  # additive

    d14 = [_h("c", "n", "def14a", "5000000"), _h("c", "n", "def14a", "4000000")]
    rows, total = _source_rows_and_total("def14a", d14)
    assert len(rows) == 1 and total == Decimal("5000000")  # overlapping → MAX rep only


def test_pure_blockholder_unchanged() -> None:
    """A 13D/G filer with no Form 4 / 13F stays in blockholders."""
    out = _reconcile_owner_once([_h("0000000006", "Activist Fund LP", "13d", "12000000", accession="13d")])
    bh = _only(out["blockholders"])
    assert bh.shares == Decimal("12000000")
    assert bh.winning_source == "13d"


def test_single_source_institution_unchanged() -> None:
    """A plain 13F manager (no other channel) passes through to institutions
    untouched — the common case, identical to pre-#1640 behavior."""
    out = _reconcile_owner_once([_h("0000093751", "STATE STREET CORP", "13f", "12642257", filer_type="INV")])
    ss = _only(out["institutions"])
    assert ss.shares == Decimal("12642257")
    assert ss.dropped_sources == ()


def test_distinct_ciks_not_merged() -> None:
    """Vanguard sub-entities with DIFFERENT CIKs (the #1639 quarter-mix case)
    are NOT merged — reconciliation keys on identity, so distinct CIKs stay
    distinct institution rows."""
    out = _reconcile_owner_once(
        [
            _h("0000102909", "VANGUARD GROUP INC", "13f", "38195010", filer_type="INV"),
            _h("0002100119", "VANGUARD CAPITAL MANAGEMENT LLC", "13f", "18217575", filer_type="INV"),
        ]
    )
    assert len(out["institutions"]) == 2


def test_name_fallback_merges_null_cik_same_name() -> None:
    """Two NULL-CIK rows with the same name collapse (name-key identity);
    distinct names do not."""
    same = _reconcile_owner_once(
        [
            _h(None, "Legacy Officer", "form4", "42000", accession="f4"),
            _h(None, "Legacy Officer", "def14a", "42000", accession="d14"),
        ]
    )
    officer = _only(same["insiders"])
    assert officer.shares == Decimal("42000")  # MAX, not 84000

    distinct = _reconcile_owner_once(
        [
            _h(None, "Officer A", "form4", "1000", accession="f4a"),
            _h(None, "Officer B", "form4", "2000", accession="f4b"),
        ]
    )
    assert len(distinct["insiders"]) == 2
