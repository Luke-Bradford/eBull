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
    _PRIORITY_RANK,
    Holder,
    SourceTag,
    _Candidate,
    _dedup_by_priority,
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
    nature: str | None = None,
    as_of: date = date(2026, 1, 1),
) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(shares),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=f"https://sec.gov/{accession}",
        as_of_date=as_of,
        filer_type=filer_type,
        dropped_sources=(),
        ownership_nature=nature,
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


def test_form3_indirect_plus_form4_direct_summed_not_maxed() -> None:
    """#1941: an owner's ``direct`` lot on Form 4 and ``indirect`` lot on Form 3
    are distinct Section-16 holdings → SUM across the two forms, not MAX-drop the
    smaller form. Mirrors AAPL / Khan Sabih (direct 1,073,895 F4 + indirect 31,632
    F3 = 1,105,527)."""
    out = _reconcile_owner_once(
        [
            _h("0002078476", "Khan Sabih", "form4", "1073895", accession="f4", nature="direct"),
            _h("0002078476", "Khan Sabih", "form3", "31632", accession="f3", nature="indirect"),
        ]
    )
    rows = out["insiders"]
    assert len(rows) == 2
    assert {h.winning_source for h in rows} == {"form4", "form3"}
    assert sum((h.shares for h in rows), Decimal(0)) == Decimal("1105527")
    # Neither lot is dropped — both are genuine additive holdings.
    assert all(h.dropped_sources == () for h in rows)


def test_section16_pooled_forms_then_max_against_def14a() -> None:
    """Cross-form additive channel first pools (form4 direct 100 + form3 indirect
    50 = 150), THEN MAXes against an overlapping def14a beneficial restatement.
    When the pooled additive (150) beats def14a (120), the additive lots win and
    def14a is folded to dropped_sources (not summed on top)."""
    out = _reconcile_owner_once(
        [
            _h("c1", "Owner Ada", "form4", "100", accession="f4", nature="direct"),
            _h("c1", "Owner Ada", "form3", "50", accession="f3", nature="indirect"),
            _h("c1", "Owner Ada", "def14a", "120", accession="dA", nature="beneficial"),
        ]
    )
    rows = out["insiders"]
    assert sum((h.shares for h in rows), Decimal(0)) == Decimal("150")
    dropped = {d.source for h in rows for d in h.dropped_sources}
    assert dropped == {"def14a"}


def test_section16_pooled_loses_to_def14a_provenance_labelled_by_form() -> None:
    """When an overlapping def14a restatement (300) EXCEEDS the pooled Section-16
    additive channel (form4 direct 100 + form3 indirect 50 = 150), def14a is the
    figure and the Section-16 channel is dropped. The dropped entry must be
    stamped with the rep row's OWN form (form4, its true accession), not the
    ``form4`` merge-bucket label smeared onto a form3 filing (#1941 / Codex
    ckpt-2)."""
    out = _reconcile_owner_once(
        [
            _h("c3", "Owner Cai", "form4", "100", accession="f4acc", nature="direct"),
            _h("c3", "Owner Cai", "form3", "50", accession="f3acc", nature="indirect"),
            _h("c3", "Owner Cai", "def14a", "300", accession="dAcc", nature="beneficial"),
        ]
    )
    holder = _only(out["insiders"])
    assert holder.shares == Decimal("300") and holder.winning_source == "def14a"
    dropped = {(d.source, d.accession_number, d.shares) for d in holder.dropped_sources}
    # One Section-16 channel entry at the pooled subtotal (150), labelled by the
    # largest lot's real form (form4 / f4acc) — never a form4 tag on f3acc.
    assert ("form4", "f4acc", Decimal("150")) in dropped
    assert not any(src == "form4" and acc == "f3acc" for src, acc, _ in dropped)


def test_section16_same_nature_across_forms_latest_wins_not_summed() -> None:
    """Defensive: if a degenerate identity merge lands the SAME nature on both
    forms, the later observation supersedes (Form 4 transaction over the Form 3
    snapshot) — never SUM the same nature twice."""
    out = _reconcile_owner_once(
        [
            _h("c2", "Owner Ben", "form3", "500", accession="f3", nature="direct", as_of=date(2025, 1, 1)),
            _h("c2", "Owner Ben", "form4", "800", accession="f4", nature="direct", as_of=date(2026, 1, 1)),
        ]
    )
    rows = out["insiders"]
    assert sum((h.shares for h in rows), Decimal(0)) == Decimal("800")
    assert {h.winning_source for h in rows} == {"form4"}


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


# --- #788: within-source additive (direct/indirect) vs overlapping (beneficial)
#     nature regime. ``beneficial`` is a Rule-13d-3 restatement (MAX), never a
#     third additive Section-16 lot (prevention-log 1835 / #905). ----------------


def test_nature_direct_plus_indirect_sums() -> None:
    """#905 preserved: direct + indirect are distinct lots → SUM."""
    rows, total = _source_rows_and_total(
        "form4", [_h("c", "n", "form4", "100", nature="direct"), _h("c", "n", "form4", "50", nature="indirect")]
    )
    assert total == Decimal("150")
    assert sum((h.shares for h in rows), Decimal(0)) == Decimal("150")


def test_nature_beneficial_equal_to_direct_not_doubled() -> None:
    """The bug: dataset ``beneficial`` == XML ``direct`` for one stake → counted
    ONCE (MAX), the beneficial restatement folded as provenance, not summed."""
    rows, total = _source_rows_and_total(
        "form4", [_h("c", "n", "form4", "100", nature="direct"), _h("c", "n", "form4", "100", nature="beneficial")]
    )
    assert total == Decimal("100")
    assert len(rows) == 1 and rows[0].ownership_nature == "direct"
    # beneficial restatement preserved for audit, not dropped silently.
    assert any(d.source == "form4" for d in rows[0].dropped_sources)


def test_nature_beneficial_only_is_the_figure() -> None:
    """Dataset-only owner (no XML rows): the sole ``beneficial`` row stands."""
    rows, total = _source_rows_and_total("form4", [_h("c", "n", "form4", "200", nature="beneficial")])
    assert total == Decimal("200") and len(rows) == 1


def test_nature_beneficial_exceeds_additive_wins_and_folds() -> None:
    """``beneficial`` (250) > direct (100): XML under-captured → take the
    Rule-13d-3 total, fold the additive lot as provenance."""
    rows, total = _source_rows_and_total(
        "form4", [_h("c", "n", "form4", "100", nature="direct"), _h("c", "n", "form4", "250", nature="beneficial")]
    )
    assert total == Decimal("250")
    assert len(rows) == 1 and rows[0].shares == Decimal("250")
    assert any(d.shares == Decimal("100") for d in rows[0].dropped_sources)


def test_nature_additive_sum_beats_smaller_beneficial() -> None:
    """direct(100) + indirect(50) = 150 dominates a 120 beneficial restatement."""
    rows, total = _source_rows_and_total(
        "form4",
        [
            _h("c", "n", "form4", "100", nature="direct"),
            _h("c", "n", "form4", "50", nature="indirect"),
            _h("c", "n", "form4", "120", nature="beneficial"),
        ],
    )
    assert total == Decimal("150")


def _cand(cik: str, source: SourceTag, shares: str, *, nature: str, accession: str, row_id: int) -> _Candidate:
    return _Candidate(
        source=source,
        priority_rank=_PRIORITY_RANK[source],
        filer_cik=cik,
        filer_name="Insider Same",
        filer_type=None,
        shares=Decimal(shares),
        as_of_date=date(2026, 1, 1),
        accession_number=accession,
        source_row_id=row_id,
        ownership_nature=nature,
    )


def test_readpath_candidate_to_owner_once_collapses_cross_nature() -> None:
    """Read-path test through the lossy candidate→Holder boundary (Codex ckpt-1):
    a dual-pipeline collision (one ``direct`` + one ``beneficial`` row, same cik /
    accession / shares — the RNTX signature) must resolve to ONE insiders
    contribution at the single value, not a summed 2×. Exercises
    ``ownership_nature`` propagation through ``_dedup_by_priority``."""
    cands = [
        _cand("0001019231", "form4", "1746549", nature="direct", accession="acc-1", row_id=1),
        _cand("0001019231", "form4", "1746549", nature="beneficial", accession="acc-1", row_id=2),
    ]
    holders = _dedup_by_priority(cands)
    # _dedup_by_priority keeps both natures (key includes nature) and carries it.
    assert {h.ownership_nature for h in holders} == {"direct", "beneficial"}
    out = _reconcile_owner_once(holders)
    insiders = out["insiders"]
    assert len(insiders) == 1
    assert insiders[0].shares == Decimal("1746549")


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
