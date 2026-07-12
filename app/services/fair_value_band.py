"""Deterministic fair-value valuation-evidence band (#2009).

Pure policy: cohort ladder, percentile synthesis, blend+envelope, per-share
conversion, dual-class routing, quality scoring — all over rows-as-args, no DB.
The IO wrapper (bottom of file) resolves MarketCapResolution + oracle
membership into plain values BEFORE calling the pure functions.

Spec: docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md
"""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median

METHOD_VERSION = "fvb_v1"
MIN_PEERS = 8
PEER_LIMIT = 8
MIN_OWN_POINTS = 6
PRICE_STALE_DAYS = 7
PEER_STALE_DAYS = 7
DIVERGENCE_THRESHOLD = 0.30  # consumed in PR-B (thesis divergence flag)

_FINANCIAL_SIC_LO, _FINANCIAL_SIC_HI = 60, 67


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


def _pos(x: float | None) -> bool:
    return x is not None and x > 0


def _is_financial(sic: str | None) -> bool:
    if not sic or len(sic) < 2 or not sic[:2].isdigit():
        return False
    return _FINANCIAL_SIC_LO <= int(sic[:2]) <= _FINANCIAL_SIC_HI


def _computable(t: TargetInputs, m: str) -> bool:
    """§4.1 denominator gate for a single multiple."""
    if m == "pe":
        return _pos(t.eps_diluted_ttm)
    if m == "ps":
        return _pos(t.revenue_ttm) and _pos(t.shares_outstanding)
    if m == "pb":
        return _pos(t.shareholders_equity) and _pos(t.shares_outstanding)
    raise ValueError(f"unknown multiple {m!r}")


def select_multiples(t: TargetInputs) -> list[str]:
    """§4.2 deterministic profile selection, first match wins, §4.1-gated.

    Dual-class target (basis != not_multiclass) intersects the set with {pe}
    because sql/201 suppresses cap-/share-based multiples for dual-class.
    """
    if _is_financial(t.sic):
        selected = ["pb", "pe"]
    elif _pos(t.net_income_ttm):
        selected = ["pe", "ps"]
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
    p20: float | None
    p50: float | None
    p80: float | None


def synth_multiple(peer: PeerPct, own: OwnPct) -> tuple[float, float, float] | None:
    """§4.5 blend base, outer-envelope low/high; degrade to the surviving one."""
    peer_ok = peer.p25 is not None and peer.p50 is not None and peer.p75 is not None
    own_ok = own.p20 is not None and own.p50 is not None and own.p80 is not None
    if peer_ok and own_ok:
        assert peer.p25 is not None and peer.p50 is not None and peer.p75 is not None
        assert own.p20 is not None and own.p50 is not None and own.p80 is not None
        base = (peer.p50 + own.p50) / 2
        low = min(peer.p25, own.p20)
        high = max(peer.p75, own.p80)
        return (low, base, high)
    if peer_ok:
        assert peer.p25 is not None and peer.p50 is not None and peer.p75 is not None
        return (peer.p25, peer.p50, peer.p75)
    if own_ok:
        assert own.p20 is not None and own.p50 is not None and own.p80 is not None
        return (own.p20, own.p50, own.p80)
    return None


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
) -> tuple[float, float, float]:
    """Convert a (low, base, high) multiple triple to per-share values."""
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
    """§4.5 median-of-bases + outer envelope. Asserts bear <= base <= bull."""
    if not triples:
        raise ValueError("combine_across requires at least one triple")
    bear = min(t[0] for t in triples)
    base = median([t[1] for t in triples])
    bull = max(t[2] for t in triples)
    assert bear <= base <= bull, f"band order violated: {bear} {base} {bull}"
    return (bear, base, bull)


def own_range(multiple_values: list[float]) -> OwnPct:
    """§4.4 own trailing range. Positive values only; MIN_OWN_POINTS floor."""
    pos = [v for v in multiple_values if v > 0]
    if len(pos) < MIN_OWN_POINTS:
        return OwnPct(None, None, None)
    p20, p50, p80 = percentiles(pos, (0.20, 0.50, 0.80))
    return OwnPct(p20=p20, p50=p50, p80=p80)


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
        triple = to_per_share(
            m,
            low_mult,
            base_mult,
            high_mult,
            eps=t.eps_diluted_ttm,
            revenue=t.revenue_ttm,
            shareholders_equity=t.shareholders_equity,
            shares=t.shares_outstanding,
        )
        per_share_triples.append(triple)
        sides = (1 if peer.p50 is not None else 0) + (1 if own.p50 is not None else 0)
        max_sides = max(max_sides, sides)
        meta = cohort_meta.get(m, {})
        cn, esn = meta.get("cohort_n", 0), meta.get("excluded_stale_n", 0)
        max_cohort_n = max(max_cohort_n, cn)
        max_excluded_stale_n = max(max_excluded_stale_n, esn)
        if own.p50 is not None:
            contributing_own_points = max(contributing_own_points, own_points_by_multiple.get(m, 0))
        basis["multiples"][m] = {
            "peer": {"p25": peer.p25, "p50": peer.p50, "p75": peer.p75},
            "own": {"p20": own.p20, "p50": own.p50, "p80": own.p80},
            "base_value": triple[1],
            "cohort_n": cn,
            "excluded_stale_n": esn,
            "own_points": own_points_by_multiple.get(m, 0),
        }

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
