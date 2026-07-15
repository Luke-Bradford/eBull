"""Deterministic fair-value valuation-evidence band (#2009).

Pure policy: cohort ladder, percentile synthesis, blend+envelope, per-share
conversion, dual-class routing, quality scoring — all over rows-as-args, no DB.
The IO wrapper (bottom of file) resolves MarketCapResolution + oracle
membership into plain values BEFORE calling the pure functions.

Spec: docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md
"""

from __future__ import annotations

import datetime as _dt
import math
from dataclasses import dataclass
from statistics import median
from typing import Any, LiteralString

METHOD_VERSION = "fvb_v3"
MIN_PEERS = 8
PEER_LIMIT = 8
MIN_OWN_POINTS = 6
PRICE_STALE_DAYS = 7
PEER_STALE_DAYS = 7
DIVERGENCE_THRESHOLD = 0.30  # consumed in PR-B (thesis divergence flag)
# Upper sanity bound on a materialized multiple. mult_value is numeric(18,6)
# (max ~1e12); a garbage tiny-denominator TTM row (e.g. revenue_ttm=$1) can
# produce a value that overflows the column -> the whole INSERT..SELECT fails ->
# the materialize tx aborts the ENTIRE batch, and it also inflates peer p75 into
# absurd bulls. Cap in the pass-1 WHERE so only sane multiples materialize. A9
# full-pop validation may tighten this.
_MAX_SANE_MULTIPLE = 1_000_000.0

_FINANCIAL_SIC_LO, _FINANCIAL_SIC_HI = 60, 67

# v2 (#2022) per-leg envelope-ratio cap — the ONLY deterministic bound on the
# peer-only tail (names with no own-history to discipline peer.p75). Clamps
# high_mult <= base_mult * _R_UP[m] and low_mult >= base_mult / _R_DN[m] in
# multiple-space, before per-share conversion. base-neutral (wings only).
# Frozen, source-ruled from the full-pop p95 of the v1 leg envelope ratio
# (docs/proposals/valuation/2026-07-13-fair-value-band-v2-robustness.md §3/§6.2).
# Re-validate on the fvb_v2 distribution post-backfill (DoD clause 11).
# ev_ebitda (#2021, fvb_v3): peer-only leg (no own-history EBITDA exists), so
# this cap is its ONLY wing bound. Provisional — calibrated from the SIC-ladder
# cohort approximation (no nearest-8 refinement; pre-backfill there are no
# stored ev legs to calibrate from): p95(p75/p50)=2.77, p95(p50/p25)=1.87
# (docs/proposals/valuation/2026-07-15-fair-value-band-ev-ebitda.md §2/§3.5).
# Re-validate via the acceptance gate on the post-backfill fvb_v3 population.
_R_UP: dict[str, float] = {"pe": 2.6, "ps": 3.1, "pb": 4.1, "ev_ebitda": 2.8}
_R_DN: dict[str, float] = {"pe": 2.7, "ps": 4.3, "pb": 2.4, "ev_ebitda": 1.9}


@dataclass(frozen=True)
class TargetInputs:
    eps_diluted_ttm: float | None
    revenue_ttm: float | None
    shareholders_equity: float | None
    net_income_ttm: float | None
    shares_outstanding: float | None
    sic: str | None
    reported_currency: str | None
    instrument_currency: str | None
    target_basis: str  # resolve_market_cap_basis result; "not_multiclass" when single-class
    # ev_ebitda leg inputs (#2021, fvb_v3) — defaulted None so pre-#2021
    # constructors are untouched; a None EBITDA input simply fails the
    # _computable gate and the name keeps its pe/ps legs.
    operating_income_ttm: float | None = None
    depreciation_amort_ttm: float | None = None
    long_term_debt: float | None = None
    short_term_debt: float | None = None
    cash: float | None = None
    interest_expense_ttm: float | None = None


def _pos(x: float | None) -> bool:
    return x is not None and x > 0


def _is_financial(sic: str | None) -> bool:
    if not sic or len(sic) < 2 or not sic[:2].isdigit():
        return False
    return _FINANCIAL_SIC_LO <= int(sic[:2]) <= _FINANCIAL_SIC_HI


def ebitda_ttm(op: float | None, da: float | None) -> float | None:
    """Strict EBITDA = OpInc + D&A (formula shape: sql/201:128/154). None if
    EITHER input is None — sql/220 makes each *_ttm NULL unless all 4 quarters
    are present, so strictness composes; no COALESCE(d&a,0) (a cohort median
    over mixed true-EBITDA / OpInc-only members is silent median poisoning —
    spec #2021 §1)."""
    if op is None or da is None:
        return None
    return op + da


def net_debt(
    long_term_debt: float | None,
    short_term_debt: float | None,
    cash: float | None,
) -> float | None:
    """EV debt/cash back-out per sql/201:92-97, with the #2021 §1 cash gate:
    None iff cash is None (a going concern with zero cash is implausible —
    NULL cash is a data gap that would overstate EV). Debt COALESCE-0
    (full-pop falsified via interest expense: 90/103 debt-null names show no
    positive interest; the incoherent 13 are gated in _computable)."""
    if cash is None:
        return None
    return (long_term_debt or 0.0) + (short_term_debt or 0.0) - cash


def _computable(t: TargetInputs, m: str) -> bool:
    """§4.1 denominator gate for a single multiple."""
    if m == "pe":
        return _pos(t.eps_diluted_ttm)
    if m == "ps":
        return _pos(t.revenue_ttm) and _pos(t.shares_outstanding)
    if m == "pb":
        return _pos(t.shareholders_equity) and _pos(t.shares_outstanding)
    if m == "ev_ebitda":
        # Strict EBITDA>0 + shares + cash present + debt/interest coherence
        # (debt-both-NULL with positive interest = unrecorded debt; spec §3.1).
        if not _pos(ebitda_ttm(t.operating_income_ttm, t.depreciation_amort_ttm)):
            return False
        if not _pos(t.shares_outstanding) or t.cash is None:
            return False
        return not (t.long_term_debt is None and t.short_term_debt is None and _pos(t.interest_expense_ttm))
    raise ValueError(f"unknown multiple {m!r}")


def select_multiples(t: TargetInputs) -> list[str]:
    """§4.2 deterministic profile selection, first match wins, §4.1-gated.

    Dual-class target (basis != not_multiclass) intersects the set with {pe}
    because sql/201 suppresses cap-/share-based multiples for dual-class.
    """
    if _is_financial(t.sic):
        selected = ["pb", "pe"]
    elif _pos(t.net_income_ttm):
        # #2021 (fvb_v3): profitable non-financials gain the EV/EBITDA leg —
        # add, not replace (v1 spec §11 item 2, "is P/S right for profitable?",
        # stays open for #2032). Median-of-3 bases is more outlier-robust than
        # mean-of-2. Financials never get EV (deposit-funded balance sheets).
        selected = ["pe", "ps", "ev_ebitda"]
    elif _pos(t.revenue_ttm):
        selected = ["ps"]
    else:
        selected = []

    if t.target_basis != "not_multiclass":
        selected = [m for m in selected if m == "pe"]

    return [m for m in selected if _computable(t, m)]


def percentiles(values: list[float], ps: tuple[float, ...]) -> list[float]:
    """Continuous percentiles matching Postgres percentile_cont.

    For sorted v[0..n-1] and fraction p: rank = p*(n-1); interpolate linearly
    between v[floor(rank)] and v[ceil(rank)].
    """
    if not values:
        raise ValueError("percentiles requires a non-empty list")
    s = sorted(values)
    n = len(s)
    out: list[float] = []
    for p in ps:
        if n == 1:
            out.append(s[0])
            continue
        rank = p * (n - 1)
        lo = int(rank)
        hi = min(lo + 1, n - 1)
        frac = rank - lo
        out.append(s[lo] + (s[hi] - s[lo]) * frac)
    return out


def currency_coherent(reported: str | None, instrument: str | None) -> bool:
    """Fail-closed: require reported_currency == instrument currency (§4.1)."""
    return reported is not None and instrument is not None and reported == instrument


@dataclass(frozen=True)
class PeerPct:
    p25: float | None
    p50: float | None
    p75: float | None


@dataclass(frozen=True)
class OwnPct:
    # v2 (#2022): INTERIOR quantiles p25/p75 (was p20/p80). See own_range.
    p25: float | None
    p50: float | None
    p75: float | None


def synth_multiple(peer: PeerPct, own: OwnPct) -> tuple[float, float, float] | None:
    """§4.5 blend base, outer-envelope low/high; degrade to the surviving one.

    -O-safe: enforces the all-or-none partial-triple invariant with explicit
    ``is not None`` guards (NOT bare ``assert``, which is stripped under
    ``python -O``). A partial some-None triple falls through to the degraded
    single-comparator / None path rather than crashing, and the local tuple
    bindings narrow the members to ``float`` for pyright without asserts.
    """
    peer_full = (
        (peer.p25, peer.p50, peer.p75)
        if peer.p25 is not None and peer.p50 is not None and peer.p75 is not None
        else None
    )
    own_full = (
        (own.p25, own.p50, own.p75) if own.p25 is not None and own.p50 is not None and own.p75 is not None else None
    )
    if peer_full is not None and own_full is not None:
        p25, p50, p75 = peer_full
        o25, o50, o75 = own_full
        base = (p50 + o50) / 2
        low = min(p25, o25)
        high = max(p75, o75)
        return (low, base, high)
    if peer_full is not None:
        return peer_full
    if own_full is not None:
        return own_full
    return None


def cap_envelope(m: str, low: float, base: float, high: float) -> tuple[float, float, bool, bool]:
    """v2 (#2022) §6.2: clamp the wing multiples to base*_R_UP[m] / base/_R_DN[m].

    Returns (low, high, capped_low, capped_high). base > 0 always (positive-denominator
    §4.1 gate + median of positive multiples), so base*R (R>=1) straddles base and the
    low <= base <= high invariant is preserved. Peer-only / own-only / two-sided all pass
    through the same clamp — the peer-only tail (no own-history to discipline peer.p75) is
    the target. base-neutral: base is not touched. Unknown multiple or non-positive base =>
    no-op (defensive; the batch handler would otherwise statuse the row)."""
    r_up = _R_UP.get(m)
    r_dn = _R_DN.get(m)
    if r_up is None or r_dn is None or base <= 0:
        return (low, high, False, False)
    cap_lo = base / r_dn
    cap_hi = base * r_up
    return (max(low, cap_lo), min(high, cap_hi), low < cap_lo, high > cap_hi)


def to_per_share(
    m: str,
    low_mult: float,
    base_mult: float,
    high_mult: float,
    *,
    eps: float | None,
    revenue: float | None,
    shareholders_equity: float | None,
    shares: float | None,
    ebitda: float | None = None,
    net_debt_value: float | None = None,
) -> tuple[float, float, float]:
    """Convert a (low, base, high) multiple triple to per-share values.

    ev_ebitda (#2021) is an AFFINE transform, not a scalar product:
    implied = (mult * EBITDA - net_debt) / shares. Monotonic increasing in
    mult (EBITDA>0, shares>0 by §4.1) => low<=base<=high survives conversion.
    Negative net debt (net cash) raises implied value — correct, no special
    case. The result CAN be <= 0 (mult*EBITDA < net_debt); compute_band's
    leg-drop guard handles that (spec §3.4) — this fn stays a pure transform.
    """
    if m == "ev_ebitda":
        if ebitda is None or net_debt_value is None or not shares:
            raise ValueError(f"per-share metric unavailable for {m!r}")
        return (
            (low_mult * ebitda - net_debt_value) / shares,
            (base_mult * ebitda - net_debt_value) / shares,
            (high_mult * ebitda - net_debt_value) / shares,
        )
    if m == "pe":
        per = eps
    elif m == "ps":
        per = None if not shares or revenue is None else revenue / shares
    elif m == "pb":
        per = None if not shares or shareholders_equity is None else shareholders_equity / shares
    else:
        raise ValueError(f"unknown multiple {m!r}")
    if per is None:
        raise ValueError(f"per-share metric unavailable for {m!r}")
    return (low_mult * per, base_mult * per, high_mult * per)


def combine_across(triples: list[tuple[float, float, float]]) -> tuple[float, float, float]:
    """§4.5 median-of-bases + outer envelope.

    Fail-closed on a band-order violation via an explicit ``raise ValueError``
    (NOT ``assert`` — stripped under ``python -O``, and AssertionError is outside
    the batch's per-row except clause). §4.5: an order violation must status to an
    absence, not crash the run; the batch handler catches this ValueError.
    """
    if not triples:
        raise ValueError("combine_across requires at least one triple")
    bear = min(t[0] for t in triples)
    base = median([t[1] for t in triples])
    bull = max(t[2] for t in triples)
    if not (bear <= base <= bull):
        raise ValueError(f"band order violated: bear={bear} base={base} bull={bull}")
    return (bear, base, bull)


def own_range(multiple_values: list[float]) -> OwnPct:
    """§4.4 own trailing range. Positive values only; MIN_OWN_POINTS floor.

    v2 (#2022): INTERIOR quantiles p25/p75, not p20/p80. own.p80 over the ~6-quarter
    floor is a near-max order statistic of a tiny sample (76% of own-legs have <=6
    points; Hyndman & Fan 1996) — it injected small-sample noise into the outer-envelope
    wing and was the dominant reproducibility risk. p25/p75 match the peer side and are
    markedly stabler across recomputes. base uses p50 only, so this is base-neutral.
    See docs/proposals/valuation/2026-07-13-fair-value-band-v2-robustness.md §6.1."""
    pos = [v for v in multiple_values if v > 0]
    if len(pos) < MIN_OWN_POINTS:
        return OwnPct(None, None, None)
    p25, p50, p75 = percentiles(pos, (0.25, 0.50, 0.75))
    return OwnPct(p25=p25, p50=p50, p75=p75)


def filter_dual_class(rows: list[tuple[int, float]], dual_class_ids: set[int]) -> list[float]:
    """Pure twin of the §4.3 curated-oracle anti-join. Drops dual-class members."""
    return [mult for iid, mult in rows if iid not in dual_class_ids]


@dataclass(frozen=True)
class QualityInputs:
    n_selected: int
    n_comparator_sides: int
    own_points: int
    cohort_n: int
    excluded_stale_n: int
    sic_level: int
    cross_multiple_spread: float


def band_quality_status(q: QualityInputs) -> str:
    """§4.7 deterministic quality tier. Points-based; conservative floors."""
    score = 0
    score += 2 if q.n_comparator_sides == 2 else 0
    score += 2 if q.own_points >= 2 * MIN_OWN_POINTS else (1 if q.own_points >= MIN_OWN_POINTS else 0)
    stale_frac = (q.excluded_stale_n / q.cohort_n) if q.cohort_n else 1.0
    score += 2 if stale_frac == 0 else (1 if stale_frac < 0.25 else 0)
    score += {4: 2, 3: 1}.get(q.sic_level, 0)
    score += 1 if q.n_selected >= 2 else 0
    score -= 1 if q.cross_multiple_spread > 0.5 else 0
    if score >= 7:
        return "high"
    if score >= 4:
        return "medium"
    return "low"


def compute_divergence(
    llm_base: float | None,
    band_base: float | None,
    threshold: float,
) -> tuple[float | None, bool | None]:
    """NULL-safe (#1632). Non-finite/absent operand or band_base<=0 => (None, None).

    Both operands checked with math.isfinite BEFORE the positivity test: nan<=0
    is False, so a NaN band_base would otherwise slip through to (nan, False)
    (Codex ckpt-1 PR-B MED). Never raises — divergence is measure-only and must
    not gate the atomic thesis insert.
    """
    if band_base is None or llm_base is None:
        return (None, None)
    if not (math.isfinite(band_base) and math.isfinite(llm_base)):
        return (None, None)
    if band_base <= 0:
        return (None, None)
    pct = abs(llm_base - band_base) / band_base
    return (pct, pct > threshold)


def _shape_fair_value_band(
    row: tuple[
        float | None,
        float | None,
        float | None,
        str,
        str | None,
        _dt.date | None,
        _dt.date | None,
        _dt.date | None,
        dict[str, Any],
    ]
    | None,
) -> dict[str, object]:
    """Passive thesis context block. Absent => {available:false, reason}.

    row cols (B3 SELECT order):
      (bear, base, bull, quality_status, reason, as_of_date, ttm_end, price_as_of, basis_json)
    available:true ONLY when bear+base+bull all non-null — the storage CHECK
    permits a partial triple, so a partial row fails closed rather than crashing
    float(None) (Codex ckpt-1 PR-B LOW).
    """
    if row is None:
        return {"available": False, "reason": "no_band"}
    bear, base, bull, quality, reason, as_of_date, ttm_end, price_as_of, basis = row
    if bear is None or base is None or bull is None:
        return {"available": False, "reason": reason or "no_band"}
    return {
        "available": True,
        "reason": reason,
        "quality_status": quality,
        "bear": float(bear),
        "base": float(base),
        "bull": float(bull),
        "as_of_date": as_of_date.isoformat() if as_of_date else None,
        "ttm_end": ttm_end.isoformat() if ttm_end else None,
        "price_as_of": price_as_of.isoformat() if price_as_of else None,
        "basis": basis,
    }


@dataclass(frozen=True)
class BandResult:
    bear: float | None
    base: float | None
    bull: float | None
    quality_status: str | None
    reason: str
    target_basis: str
    n_selected: int
    basis: dict


def _absent(t: TargetInputs, reason: str, n_selected: int = 0, basis: dict | None = None) -> BandResult:
    return BandResult(None, None, None, None, reason, t.target_basis, n_selected, basis or {})


def compute_band(
    t: TargetInputs,
    *,
    peer_by_multiple: dict[str, PeerPct],
    own_by_multiple: dict[str, OwnPct],
    own_points_by_multiple: dict[str, int],
    cohort_meta: dict[str, dict],
    sic_level: int,
) -> BandResult:
    """Pure orchestration. Returns statused-absent BandResult, never raises for
    the normal no-band paths. Currency + basis gates are applied by the caller
    (IO wrapper) — this fn assumes t already passed currency_coherent.

    Quality uses the TRUE own_points (per-multiple distinct-quarter counts the IO
    computed) and the TRUE excluded_stale_n from cohort_meta — no proxy (Codex
    ckpt-1 HIGH #4). band_quality_status is the single source of the tiering rule."""
    selected = select_multiples(t)
    if not selected:
        return _absent(t, "no_multiple")

    ev_ebitda_value = ebitda_ttm(t.operating_income_ttm, t.depreciation_amort_ttm)
    ev_net_debt = net_debt(t.long_term_debt, t.short_term_debt, t.cash)

    per_share_triples: list[tuple[float, float, float]] = []
    basis: dict = {"selected": selected, "multiples": {}}
    max_sides = 0
    max_excluded_stale_n = 0
    max_cohort_n = 0
    contributing_own_points = 0
    for m in selected:
        peer = peer_by_multiple.get(m, PeerPct(None, None, None))
        own = own_by_multiple.get(m, OwnPct(None, None, None))
        synth = synth_multiple(peer, own)
        if synth is None:
            continue
        low_mult, base_mult, high_mult = synth
        precap_low, precap_high = low_mult, high_mult
        low_mult, high_mult, capped_low, capped_high = cap_envelope(m, low_mult, base_mult, high_mult)
        triple = to_per_share(
            m,
            low_mult,
            base_mult,
            high_mult,
            eps=t.eps_diluted_ttm,
            revenue=t.revenue_ttm,
            shareholders_equity=t.shareholders_equity,
            shares=t.shares_outstanding,
            ebitda=ev_ebitda_value,
            net_debt_value=ev_net_debt,
        )
        meta = cohort_meta.get(m, {})
        cn, esn = meta.get("cohort_n", 0), meta.get("excluded_stale_n", 0)
        # Basis entry built BEFORE the leg-drop decision (spec #2021 §3.4) so a
        # dropped leg's peer stats stay auditable on the stored row.
        entry: dict = {
            "peer": {"p25": peer.p25, "p50": peer.p50, "p75": peer.p75},
            "own": {"p25": own.p25, "p50": own.p50, "p75": own.p75},
            "cohort_n": cn,
            "excluded_stale_n": esn,
            "own_points": own_points_by_multiple.get(m, 0),
            # v2 (#2022) cap audit: the pre-cap wing multiples + whether each was
            # clamped, so the base*_R_UP/_R_DN clamp is reconstructable from the row.
            "capped_low": capped_low,
            "capped_high": capped_high,
            "precap_low_mult": precap_low,
            "precap_high_mult": precap_high,
        }
        if m == "ev_ebitda":
            # Conversion inputs — the affine transform is reconstructable from
            # the stored row (mult * ebitda_ttm - net_debt) / shares.
            entry["ebitda_ttm"] = ev_ebitda_value
            entry["net_debt"] = ev_net_debt
        if any(v <= 0 for v in triple):
            # Leg-drop guard (spec #2021 §3.4, fail-closed): a converted value
            # <= 0 (mult*EBITDA < net debt — equity is an option, not a price
            # target) must never enter combine_across, where a <=0 bear would
            # poison the combined min(). Impossible for pe/ps/pb (positive
            # mult x positive per-share metric); real for ~6% of ev legs.
            # n_selected stays len(selected) — the shipped synth-None precedent
            # for non-contributing legs; contribution is visible right here.
            entry["dropped_nonpositive"] = True
            basis["multiples"][m] = entry
            continue
        entry["base_value"] = triple[1]
        basis["multiples"][m] = entry
        per_share_triples.append(triple)
        sides = (1 if peer.p50 is not None else 0) + (1 if own.p50 is not None else 0)
        max_sides = max(max_sides, sides)
        max_cohort_n = max(max_cohort_n, cn)
        max_excluded_stale_n = max(max_excluded_stale_n, esn)
        if own.p50 is not None:
            contributing_own_points = max(contributing_own_points, own_points_by_multiple.get(m, 0))

    if not per_share_triples:
        return _absent(t, "thin_cohort", n_selected=len(selected), basis=basis)

    bear, base, bull = combine_across(per_share_triples)
    bases = [tr[1] for tr in per_share_triples]
    spread = ((max(bases) - min(bases)) / base) if base and len(bases) > 1 else 0.0
    quality = band_quality_status(
        QualityInputs(
            n_selected=len(selected),
            n_comparator_sides=max_sides,
            own_points=contributing_own_points,
            cohort_n=max_cohort_n,
            excluded_stale_n=max_excluded_stale_n,
            sic_level=sic_level,
            cross_multiple_spread=spread,
        )
    )
    return BandResult(bear, base, bull, quality, "ok", t.target_basis, len(selected), basis)


# ---------------------------------------------------------------------------
# --- IO wrapper (DB) ---
# ---------------------------------------------------------------------------
#
# Resolves MarketCapResolution + curated-oracle membership + strict-TTM rows
# into plain values, runs the two-pass compute (pass-1 materialize cohort
# members universe-wide at ONE as-of date; pass-2 per-name pure synthesis),
# and write-throughs into the two-layer observations/current pair.
#
# SQL correctness anchors (grepped 2026-07-12, not from memory):
#   * resolve_market_cap_basis  — app/services/xbrl_derived_stats.py:538
#   * _apply_market_cap_basis pattern — app/services/scoring.py:353
#   * peer_comparison._rank_peers — app/services/peer_comparison.py:178
#   * dual-class oracle CTE — sql/201_instrument_valuation_dual_class_suppress.sql:46-56
#   * financial_periods_ttm VIEW — sql/220_ttm_strict_flow_sums.sql
#   * fundamentals_snapshot columns — sql/001_init.sql:29-44
#
# #2008 (bb03f62e) column semantics — VERIFIED at code time against the
# write-through in app/services/fundamentals/__init__.py:159-182 AND spec §4.4:
#   * fundamentals_snapshot.eps       = SUM(eps_diluted) over 4 adjacent quarters
#                                       => TTM diluted EPS, PER SHARE.
#   * fundamentals_snapshot.book_value = shareholders_equity / shares_outstanding
#                                       => book value PER SHARE.
# Hence own P/E = close / eps ; own P/B = close / book_value (book_value already
# per-share) ; own P/S = close * shares_outstanding / revenue_ttm. These mirror
# sql/201's legacy CTE (pe_ratio = price/eps, pb_ratio = price/book_value) and
# the cohort-member P/S = price*shares/revenue so target and cohort share one
# multiple definition.

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

from app.services.peer_comparison import _rank_peers  # noqa: E402
from app.services.xbrl_derived_stats import resolve_market_cap_basis  # noqa: E402


def _f(x: object) -> float | None:
    """Decimal/None -> float/None (psycopg returns numeric as Decimal)."""
    if x is None:
        return None
    try:
        return float(x)  # type: ignore[arg-type]
    except TypeError, ValueError:
        return None


def resolve_batch_as_of_date(conn: psycopg.Connection[Any]) -> _dt.date | None:
    """Newest closed market session — the single data-anchored as-of DATE for the
    whole batch (§4.6: NOT now()::date). Every price read below is relative to it.
    None when price_daily is empty (nothing to compute)."""
    with conn.cursor() as cur:
        cur.execute("SELECT max(price_date) FROM price_daily WHERE close IS NOT NULL AND close > 0")
        row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def resolve_cohort_members_as_of_date(conn: psycopg.Connection[Any]) -> _dt.date | None:
    """Latest as_of_date actually present in fair_value_cohort_members — the anchor
    for a SINGLE-NAME cascade run (I1). A single-name recompute (A8 cascade) must
    read the freshest MATERIALIZED cohort set, NOT the price max: if new price bars
    advanced max(price_date) since the last full materialize, the price-anchored
    date carries ZERO cohort rows -> every peer_pct_for returns all-None ->
    a peerless thin_cohort band silently overwrites a good one. None when nothing
    has been materialized yet."""
    with conn.cursor() as cur:
        cur.execute("SELECT max(as_of_date) FROM fair_value_cohort_members")
        row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


# Pass-1: materialize the cohort-member working set for ONE as-of date. ONE
# parameterized statement (no f-string/format of identifiers or values — global
# CLAUDE.md non-negotiable). The dual_class CTE is copied verbatim from
# sql/201:46-56 (Codex ckpt-1 HIGH #2): the provider/identifier_type/is_primary
# predicate lives on external_identifiers, joined to
# instrument_class_shares_outstanding on source_cik = lpad(cik,10,'0').
_MATERIALIZE_SQL = """
INSERT INTO fair_value_cohort_members
    (as_of_date, instrument_id, multiple, mult_value, sic, sic3, sic2,
     total_assets, close_date, dual_class_suppressed)
WITH asof_price AS (
    SELECT DISTINCT ON (pd.instrument_id)
           pd.instrument_id,
           pd.close      AS close,
           pd.price_date AS close_date
    FROM price_daily pd
    WHERE pd.price_date <= %(as_of)s
      AND NULLIF(GREATEST(pd.close, 0), 0) IS NOT NULL
    ORDER BY pd.instrument_id, pd.price_date DESC
),
dual_class AS (
    -- verbatim from sql/201:46-56
    SELECT DISTINCT ei.instrument_id
    FROM external_identifiers ei
    JOIN instrument_class_shares_outstanding c
      ON c.source_cik = lpad(ei.identifier_value, 10, '0')
    WHERE ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
      AND ei.is_primary = TRUE
),
base AS (
    SELECT
        ap.instrument_id,
        ap.close,
        ap.close_date,
        sp.sic,
        sp.sic3,
        sp.sic2,
        ttm.total_assets,
        ttm.eps_diluted_ttm,
        ttm.revenue_ttm,
        ttm.shareholders_equity,
        ttm.shares_outstanding,
        ttm.operating_income_ttm,
        ttm.depreciation_amort_ttm,
        ttm.long_term_debt,
        ttm.short_term_debt,
        ttm.cash,
        ttm.interest_expense_ttm,
        (dc.instrument_id IS NOT NULL) AS dual_class_suppressed
    FROM asof_price ap
    JOIN financial_periods_ttm ttm
      ON ttm.instrument_id = ap.instrument_id AND ttm.is_complete_ttm = TRUE
    JOIN instrument_sec_profile sp
      ON sp.instrument_id = ap.instrument_id
    LEFT JOIN dual_class dc
      ON dc.instrument_id = ap.instrument_id
),
per_multiple AS (
    SELECT instrument_id, 'pe'::text AS multiple,
           (close / eps_diluted_ttm) AS mult_value,
           sic, sic3, sic2, total_assets, close_date, dual_class_suppressed
    FROM base WHERE eps_diluted_ttm > 0
    UNION ALL
    -- P/S = price*shares/revenue (mirrors sql/201 price_sales); target conversion
    -- mult*(revenue/shares) recovers price => one shared multiple definition.
    SELECT instrument_id, 'ps'::text,
           ((close * shares_outstanding) / revenue_ttm),
           sic, sic3, sic2, total_assets, close_date, dual_class_suppressed
    FROM base WHERE revenue_ttm > 0 AND shares_outstanding > 0
    UNION ALL
    -- P/B = price*shares/equity (mirrors sql/201 pb_ratio = price/(equity/shares)).
    SELECT instrument_id, 'pb'::text,
           ((close * shares_outstanding) / shareholders_equity),
           sic, sic3, sic2, total_assets, close_date, dual_class_suppressed
    FROM base WHERE shareholders_equity > 0 AND shares_outstanding > 0
    UNION ALL
    -- EV/EBITDA (#2021) = (close*shares + debt - cash) / (OpInc + D&A) —
    -- mirrors sql/201:128-135. Strict EBITDA: a NULL op/d&a propagates and the
    -- row drops (sql/220 already NULLs any *_ttm without 4 quarters). cash must
    -- be PRESENT (NULL cash = data gap; COALESCE-0 would overstate EV);
    -- debt-both-NULL with positive interest = unrecorded debt, incoherent ->
    -- dropped (spec 2026-07-15 §3.1). The outer mult_value > 0 also excludes
    -- negative-EV members (net cash > cap: meaningless multiple, 2/532 full-pop).
    SELECT instrument_id, 'ev_ebitda'::text,
           ((close * shares_outstanding)
            + COALESCE(long_term_debt, 0) + COALESCE(short_term_debt, 0) - cash)
           / (operating_income_ttm + depreciation_amort_ttm),
           sic, sic3, sic2, total_assets, close_date, dual_class_suppressed
    FROM base
    WHERE (operating_income_ttm + depreciation_amort_ttm) > 0
      AND shares_outstanding > 0
      AND cash IS NOT NULL
      AND NOT (long_term_debt IS NULL AND short_term_debt IS NULL
               AND COALESCE(interest_expense_ttm, 0) > 0)
)
SELECT %(as_of)s, instrument_id, multiple, mult_value, sic, sic3, sic2,
       total_assets, close_date, dual_class_suppressed
FROM per_multiple
-- mult_value > 0 gates junk; < %(max_mult)s prevents a tiny-denominator garbage
-- multiple from overflowing numeric(18,6) and aborting the whole batch INSERT
-- (and from inflating peer p75). Parameterized cap — see _MAX_SANE_MULTIPLE.
WHERE mult_value > 0
  AND mult_value < %(max_mult)s
"""


def materialize_cohort_members(conn: psycopg.Connection[Any], as_of_date: _dt.date) -> None:
    """Pass-1 (§4.3): one row per (instrument_id, multiple) over the eligible
    universe, each name's multiple from its close AS OF the batch date (nearest
    at-or-before, NOT its own latest close). DELETE-then-INSERT for the as_of_date
    (idempotent re-run). Does the expensive price-as-of join ONCE for every name."""
    conn.execute(
        "DELETE FROM fair_value_cohort_members WHERE as_of_date = %(as_of)s",
        {"as_of": as_of_date},
    )
    conn.execute(_MATERIALIZE_SQL, {"as_of": as_of_date, "max_mult": _MAX_SANE_MULTIPLE})


# Pass-2 member read — three fully-static SQL strings keyed by SIC ladder level
# (the column name is from a frozen 3-element whitelist, never interpolated from
# input). keep_dual = TRUE for P/E (dual-class members keep their P/E, #1662);
# FALSE for every cap-/share-based multiple (P/S, P/B, EV/EBITDA #2021 —
# curated-oracle members drop out of those medians, mirroring sql/201:254).
_MEMBER_SQL: dict[int, LiteralString] = {
    4: """
        SELECT instrument_id, mult_value, total_assets, close_date, dual_class_suppressed
        FROM fair_value_cohort_members
        WHERE as_of_date = %(as_of)s AND multiple = %(m)s AND sic = %(prefix)s
          AND instrument_id <> %(target)s
          AND (%(keep_dual)s OR NOT dual_class_suppressed)
        """,
    3: """
        SELECT instrument_id, mult_value, total_assets, close_date, dual_class_suppressed
        FROM fair_value_cohort_members
        WHERE as_of_date = %(as_of)s AND multiple = %(m)s AND sic3 = %(prefix)s
          AND instrument_id <> %(target)s
          AND (%(keep_dual)s OR NOT dual_class_suppressed)
        """,
    2: """
        SELECT instrument_id, mult_value, total_assets, close_date, dual_class_suppressed
        FROM fair_value_cohort_members
        WHERE as_of_date = %(as_of)s AND multiple = %(m)s AND sic2 = %(prefix)s
          AND instrument_id <> %(target)s
          AND (%(keep_dual)s OR NOT dual_class_suppressed)
        """,
}


def _sic_prefix(sic: str | None, level: int) -> str | None:
    if not sic:
        return None
    return sic[:level]


def peer_pct_for(
    conn: psycopg.Connection[Any],
    target_id: int,
    target_sic: str | None,
    target_total_assets: float | None,
    multiple: str,
    as_of_date: _dt.date,
) -> tuple[PeerPct, dict]:
    """Pass-2 comparator (a) (§4.3): read the member set for ``multiple`` at
    ``as_of_date``; walk SIC-4->3->2 to the first prefix with >= MIN_PEERS FRESH
    eligible members; size-refine to nearest PEER_LIMIT by
    |ln(total_assets) - ln(target_total_assets)| (reusing peer_comparison._rank_peers);
    percentiles(mults, (0.25,0.5,0.75)) in PURE Python (the SAME percentiles() as
    own-history -> guaranteed agreement).

    INVARIANT (A4 review): never returns a partial some-None triple — percentiles()
    yields all three, and the MIN_PEERS-unmet path returns PeerPct(None,None,None).
    Returns (PeerPct, {"cohort_n","excluded_stale_n","sic_level"})."""
    keep_dual = multiple == "pe"
    cutoff = as_of_date - _dt.timedelta(days=PEER_STALE_DAYS)
    fallback_meta = {"cohort_n": 0, "excluded_stale_n": 0, "sic_level": 0}

    for level in (4, 3, 2):
        prefix = _sic_prefix(target_sic, level)
        if prefix is None:
            continue
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                _MEMBER_SQL[level],
                {
                    "as_of": as_of_date,
                    "m": multiple,
                    "prefix": prefix,
                    "target": target_id,
                    "keep_dual": keep_dual,
                },
            )
            rows = cur.fetchall()
        if not rows:
            continue
        fresh = [r for r in rows if r["close_date"] >= cutoff]
        excluded_stale_n = len(rows) - len(fresh)
        # Widest cohort we saw, in case no level clears MIN_PEERS.
        fallback_meta = {
            "cohort_n": len(rows),
            "excluded_stale_n": excluded_stale_n,
            "sic_level": 0,
        }
        if len(fresh) < MIN_PEERS:
            continue

        mult_by_id: dict[int, float] = {}
        for r in fresh:
            v = _f(r["mult_value"])
            if v is not None and v > 0:
                mult_by_id[int(r["instrument_id"])] = v

        chosen: list[float]
        if target_total_assets and target_total_assets > 0 and len(mult_by_id) > PEER_LIMIT:
            rank_rows = [
                {
                    "instrument_id": int(r["instrument_id"]),
                    "total_assets": _f(r["total_assets"]),
                    "symbol": "",
                    "company_name": None,
                }
                for r in fresh
                if int(r["instrument_id"]) in mult_by_id
            ]
            ranked = _rank_peers(
                rank_rows,
                self_id=target_id,
                self_total_assets=float(target_total_assets),
                limit=PEER_LIMIT,
            )
            chosen = [mult_by_id[pr.instrument_id] for pr in ranked if pr.instrument_id in mult_by_id]
            if not chosen:  # every fresh member lacked a positive total_assets
                chosen = list(mult_by_id.values())
        else:
            chosen = list(mult_by_id.values())

        # F1 (High): _rank_peers DROPS members with missing/non-positive
        # total_assets, so a level with >= MIN_PEERS FRESH members can still
        # size-refine to < MIN_PEERS asset-usable peers. Re-check the MIN_PEERS
        # invariant AFTER size-ranking; if it fails, widen the SIC ladder (same
        # as the pre-rank fresh check) and go comparator-absent (all-None,
        # fallback_meta) if no wider level clears it. This also subsumes the
        # empty-chosen case. Preserves the all-None-or-all-three partial-triple
        # invariant (percentiles() below always yields all three).
        if len(chosen) < MIN_PEERS:
            continue

        p25, p50, p75 = percentiles(chosen, (0.25, 0.5, 0.75))
        return PeerPct(p25=p25, p50=p50, p75=p75), {
            "cohort_n": len(rows),
            "excluded_stale_n": excluded_stale_n,
            "sic_level": level,
        }

    return PeerPct(None, None, None), fallback_meta


# Target inputs: strict-TTM denominators + SIC + reporting/instrument currency.
_TARGET_SQL = """
    SELECT
        ttm.eps_diluted_ttm,
        ttm.revenue_ttm,
        ttm.shareholders_equity,
        ttm.net_income_ttm,
        ttm.shares_outstanding,
        ttm.operating_income_ttm,
        ttm.depreciation_amort_ttm,
        ttm.long_term_debt,
        ttm.short_term_debt,
        ttm.cash,
        ttm.interest_expense_ttm,
        ttm.reported_currency,
        ttm.total_assets,
        ttm.ttm_end,
        sp.sic          AS sic,
        i.currency      AS instrument_currency
    FROM instruments i
    LEFT JOIN financial_periods_ttm ttm
      ON ttm.instrument_id = i.instrument_id AND ttm.is_complete_ttm = TRUE
    LEFT JOIN instrument_sec_profile sp
      ON sp.instrument_id = i.instrument_id
    WHERE i.instrument_id = %(iid)s
"""

# Own trailing history (§4.4): one fundamentals_snapshot row per historical
# quarter (as_of_date = period_end) x price_daily.close NEAREST-AT-OR-BEFORE that
# quarter (price_date <= as_of_date — a post-quarter price is lookahead bias),
# windowed to <= the batch as-of date.
_OWN_HISTORY_SQL = """
    SELECT fs.as_of_date, fs.eps, fs.book_value, fs.revenue_ttm, fs.shares_outstanding,
           pj.close AS close
    FROM fundamentals_snapshot fs
    LEFT JOIN LATERAL (
        SELECT pd.close
        FROM price_daily pd
        WHERE pd.instrument_id = fs.instrument_id
          AND pd.price_date <= fs.as_of_date
          AND NULLIF(GREATEST(pd.close, 0), 0) IS NOT NULL
        ORDER BY pd.price_date DESC
        LIMIT 1
    ) pj ON TRUE
    WHERE fs.instrument_id = %(iid)s
      AND fs.as_of_date <= %(as_of)s
    ORDER BY fs.as_of_date
"""

# Target's latest close at-or-before the batch as-of date (freshness gate + the
# per-share own-history anchor share this one date policy).
_TARGET_PRICE_SQL = """
    SELECT pd.close, pd.price_date
    FROM price_daily pd
    WHERE pd.instrument_id = %(iid)s
      AND pd.price_date <= %(as_of)s
      AND NULLIF(GREATEST(pd.close, 0), 0) IS NOT NULL
    ORDER BY pd.price_date DESC
    LIMIT 1
"""


def _own_series(
    conn: psycopg.Connection[Any], instrument_id: int, as_of_date: _dt.date
) -> tuple[dict[str, list[float]], int]:
    """§4.4 own trailing multiples per quarter. book_value is PER SHARE and eps is
    TTM diluted (#2008 write-through, verified above), so P/E=close/eps,
    P/B=close/book_value, P/S=close*shares/revenue.

    Returns (multiples_by_bucket, own_capped_total) — the second element is the
    count of quarters dropped by the sanity cap, surfaced into basis so a name
    whose own_points fell below MIN_OWN_POINTS via capping is auditable (review
    NITPICK)."""
    # ev_ebitda stays an EMPTY series by construction: fundamentals_snapshot has
    # no historical EBITDA/debt/cash (sql/201 legacy CTE NULLs ev_ebitda), and a
    # 4th strict-TTM copy is banned (#2008). own_range([]) -> all-None -> the
    # ev leg is peer-only; the fixed cap is its sole wing bound (#2021).
    out: dict[str, list[float]] = {"pe": [], "ps": [], "pb": [], "ev_ebitda": []}
    capped = 0

    def _push(bucket: str, value: float) -> None:
        # F2(a): cap symmetric to the pass-1 materialize WHERE
        # (0 < mult_value < _MAX_SANE_MULTIPLE). A tiny-positive eps/book_value
        # quarter must not push own p80 to a huge/inf multiple -> garbage band +
        # numeric(18,6) overflow on write-through. Callers only pass value > 0,
        # so a drop always means the cap fired -> count it for the audit trail.
        nonlocal capped
        if 0.0 < value < _MAX_SANE_MULTIPLE:
            out[bucket].append(value)
        elif value >= _MAX_SANE_MULTIPLE:
            capped += 1

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(_OWN_HISTORY_SQL, {"iid": instrument_id, "as_of": as_of_date})
        rows = cur.fetchall()
    for r in rows:
        close = _f(r["close"])
        if close is None or close <= 0:
            continue
        eps = _f(r["eps"])
        if eps is not None and eps > 0:
            _push("pe", close / eps)
        rev = _f(r["revenue_ttm"])
        sh = _f(r["shares_outstanding"])
        if rev is not None and rev > 0 and sh is not None and sh > 0:
            _push("ps", close * sh / rev)
        bv = _f(r["book_value"])
        if bv is not None and bv > 0:
            _push("pb", close / bv)
    return out, capped


def _stamp(result: BandResult, ttm_end: _dt.date | None, price_as_of: _dt.date | None) -> BandResult:
    """Carry ttm_end/price_as_of on the result's basis dict so the batch
    orchestrator can hand them to write_band (basis is a mutable dict on a frozen
    dataclass — mutating its contents is allowed)."""
    result.basis["ttm_end"] = ttm_end.isoformat() if ttm_end else None
    result.basis["price_as_of"] = price_as_of.isoformat() if price_as_of else None
    return result


def compute_band_for_instrument(conn: psycopg.Connection[Any], instrument_id: int, as_of_date: _dt.date) -> BandResult:
    """Pass-2 orchestration (§4): resolve basis, gather target inputs, apply the
    currency + freshness gates, build own-history + peer comparators for the
    selected multiples, call the pure compute_band. Does NOT write. ttm_end and
    price_as_of are stamped onto the returned BandResult.basis for write_band."""
    resolution = resolve_market_cap_basis(conn, instrument_id=instrument_id)
    target_basis = resolution.basis

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(_TARGET_SQL, {"iid": instrument_id})
        trow = cur.fetchone()
    if trow is None:
        # Unknown instrument_id — statused absence with the resolved basis.
        return _stamp(BandResult(None, None, None, None, "no_multiple", target_basis, 0, {}), None, None)

    ttm_end = trow["ttm_end"]
    reported_currency = trow["reported_currency"]
    instrument_currency = trow["instrument_currency"]
    target_sic = trow["sic"]
    target_total_assets = _f(trow["total_assets"])

    t = TargetInputs(
        eps_diluted_ttm=_f(trow["eps_diluted_ttm"]),
        revenue_ttm=_f(trow["revenue_ttm"]),
        shareholders_equity=_f(trow["shareholders_equity"]),
        net_income_ttm=_f(trow["net_income_ttm"]),
        shares_outstanding=_f(trow["shares_outstanding"]),
        sic=target_sic,
        reported_currency=reported_currency,
        instrument_currency=instrument_currency,
        target_basis=target_basis,
        operating_income_ttm=_f(trow["operating_income_ttm"]),
        depreciation_amort_ttm=_f(trow["depreciation_amort_ttm"]),
        long_term_debt=_f(trow["long_term_debt"]),
        short_term_debt=_f(trow["short_term_debt"]),
        cash=_f(trow["cash"]),
        interest_expense_ttm=_f(trow["interest_expense_ttm"]),
    )

    # Currency gate (§4.1) — only when the name actually has a reporting currency
    # (a no-fundamentals name has reported_currency NULL; that is a no_multiple /
    # stale_price case, not a currency mismatch).
    if reported_currency is not None and not currency_coherent(reported_currency, instrument_currency):
        return _stamp(_absent(t, "currency_mismatch"), ttm_end, None)

    # Freshness gate (§4.6) — do not publish a band anchored on a stale target.
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(_TARGET_PRICE_SQL, {"iid": instrument_id, "as_of": as_of_date})
        prow = cur.fetchone()
    price_as_of = prow["price_date"] if prow else None
    if price_as_of is None or (as_of_date - price_as_of).days > PRICE_STALE_DAYS:
        return _stamp(_absent(t, "stale_price"), ttm_end, price_as_of)

    selected = select_multiples(t)
    if not selected:
        reason = "multiclass_unavailable" if target_basis == "multiclass_unavailable" else "no_multiple"
        return _stamp(_absent(t, reason), ttm_end, price_as_of)

    own_all, own_capped_total = _own_series(conn, instrument_id, as_of_date)
    own_by_multiple: dict[str, OwnPct] = {}
    own_points_by_multiple: dict[str, int] = {}
    peer_by_multiple: dict[str, PeerPct] = {}
    cohort_meta: dict[str, dict] = {}
    sic_level = 0
    for m in selected:
        own_by_multiple[m] = own_range(own_all[m])
        own_points_by_multiple[m] = len(own_all[m])
        peer, meta = peer_pct_for(conn, instrument_id, target_sic, target_total_assets, m, as_of_date)
        peer_by_multiple[m] = peer
        cohort_meta[m] = meta
        sic_level = max(sic_level, int(meta.get("sic_level", 0)))

    result = compute_band(
        t,
        peer_by_multiple=peer_by_multiple,
        own_by_multiple=own_by_multiple,
        own_points_by_multiple=own_points_by_multiple,
        cohort_meta=cohort_meta,
        sic_level=sic_level,
    )
    # Surface own-history quarters dropped by the sanity cap so a name whose
    # own_points fell below MIN_OWN_POINTS via capping is auditable (review NITPICK).
    result.basis["own_capped_total"] = own_capped_total
    return _stamp(result, ttm_end, price_as_of)


_OBS_INSERT_SQL = """
    INSERT INTO fair_value_band_observations
        (instrument_id, method_version, computed_at, as_of_date, ttm_end, price_as_of,
         bear_value, base_value, bull_value, quality_status, reason, target_basis,
         n_selected, basis_json)
    VALUES
        (%(iid)s, %(mv)s, %(now)s, %(as_of)s, %(ttm_end)s, %(price_as_of)s,
         %(bear)s, %(base)s, %(bull)s, %(quality)s, %(reason)s, %(basis)s,
         %(n)s, %(basis_json)s)
"""

_CURRENT_UPSERT_SQL = """
    INSERT INTO fair_value_band_current
        (instrument_id, method_version, computed_at, as_of_date, ttm_end, price_as_of,
         bear_value, base_value, bull_value, quality_status, reason, target_basis,
         n_selected, basis_json)
    VALUES
        (%(iid)s, %(mv)s, %(now)s, %(as_of)s, %(ttm_end)s, %(price_as_of)s,
         %(bear)s, %(base)s, %(bull)s, %(quality)s, %(reason)s, %(basis)s,
         %(n)s, %(basis_json)s)
    ON CONFLICT (instrument_id, method_version) DO UPDATE SET
        computed_at    = EXCLUDED.computed_at,
        as_of_date     = EXCLUDED.as_of_date,
        ttm_end        = EXCLUDED.ttm_end,
        price_as_of    = EXCLUDED.price_as_of,
        bear_value     = EXCLUDED.bear_value,
        base_value     = EXCLUDED.base_value,
        bull_value     = EXCLUDED.bull_value,
        quality_status = EXCLUDED.quality_status,
        reason         = EXCLUDED.reason,
        target_basis   = EXCLUDED.target_basis,
        n_selected     = EXCLUDED.n_selected,
        basis_json     = EXCLUDED.basis_json
"""


def write_band(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    band: BandResult,
    as_of_date: _dt.date,
    ttm_end: _dt.date | None,
    price_as_of: _dt.date | None,
) -> None:
    """Write-through (§5, #1632): APPEND an observation (the audit record — past
    bands are not reconstructable from mutable price_daily) then upsert the
    current row. Absence writes NULL band + reason + the resolved target_basis."""
    params = {
        "iid": instrument_id,
        "mv": METHOD_VERSION,
        "now": _dt.datetime.now(tz=_dt.UTC),
        "as_of": as_of_date,
        "ttm_end": ttm_end,
        "price_as_of": price_as_of,
        "bear": band.bear,
        "base": band.base,
        "bull": band.bull,
        "quality": band.quality_status,
        "reason": band.reason,
        "basis": band.target_basis,
        "n": band.n_selected,
        "basis_json": Jsonb(band.basis),
    }
    conn.execute(_OBS_INSERT_SQL, params)
    conn.execute(_CURRENT_UPSERT_SQL, params)


def _universe_ids(conn: psycopg.Connection[Any]) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT instrument_id FROM instruments WHERE is_tradable = TRUE ORDER BY instrument_id")
        return [int(r[0]) for r in cur.fetchall()]


def refresh_fair_value_band_batch(conn: psycopg.Connection[Any], instrument_ids: list[int] | None) -> dict:
    """Orchestration: resolve the single as-of date; on a full/bootstrap run
    (instrument_ids is None) run pass-1 materialize once, then per-instrument
    pass-2 + write-through under a SAVEPOINT (one bad row never aborts the batch —
    catches missing-table/column during a mid-migration window). A single-name
    cascade run (instrument_ids given) reads the existing member set.

    Returns {"written","statused","failed"}: written = real band (reason 'ok'),
    statused = persisted absence row, failed = per-row exception (rolled back)."""
    if instrument_ids is None:
        # Full/bootstrap run: price-anchored as-of, materialize the cohort at it.
        as_of = resolve_batch_as_of_date(conn)
        if as_of is None:
            return {"written": 0, "statused": 0, "failed": 0}
        with conn.transaction():
            materialize_cohort_members(conn, as_of)
        ids = _universe_ids(conn)
    else:
        # Single-name cascade (A8, I1): anchor to the latest MATERIALIZED cohort
        # as_of, never the price max — else a post-materialize price advance leaves
        # the price-anchored date with zero cohort rows, every peer read comes back
        # all-None, and a good band is clobbered by a peerless thin_cohort band.
        # Fall back to the price-anchored date only when nothing was ever
        # materialized (empty table): bands then compute peer-absent (own-only /
        # thin_cohort) cleanly rather than crash.
        as_of = resolve_cohort_members_as_of_date(conn) or resolve_batch_as_of_date(conn)
        if as_of is None:
            return {"written": 0, "statused": 0, "failed": 0}
        ids = instrument_ids

    written = statused = failed = 0
    for iid in ids:
        try:
            with conn.transaction():
                band = compute_band_for_instrument(conn, iid, as_of)
                ttm_end = _parse_iso(band.basis.get("ttm_end"))
                price_as_of = _parse_iso(band.basis.get("price_as_of"))
                write_band(conn, iid, band, as_of, ttm_end, price_as_of)
            if band.reason == "ok":
                written += 1
            else:
                statused += 1
        except (
            psycopg.errors.UndefinedTable,
            psycopg.errors.UndefinedColumn,
            psycopg.errors.DataError,
            ValueError,
        ):
            # F2(b): UndefinedTable/UndefinedColumn guard a mid-migration window.
            # DataError (NumericValueOutOfRange is a subclass) is the targeted
            # backstop so one tiny-denominator multiple that overflows
            # numeric(18,6) skips that instrument instead of aborting the whole
            # universe run. ValueError isolates the pure compute path's
            # fail-closed signals (combine_across band-order violation,
            # to_per_share unavailable metric, percentiles empty) to this ONE row.
            # The `with conn.transaction()` above already rolled back this name's
            # savepoint. NOT bare Exception — a genuine programming bug must still
            # surface.
            failed += 1
            continue
    return {"written": written, "statused": statused, "failed": failed}


def _parse_iso(v: object) -> _dt.date | None:
    if not isinstance(v, str):
        return None
    return _dt.date.fromisoformat(v)
