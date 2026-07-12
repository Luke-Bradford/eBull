"""Deterministic fair-value valuation-evidence band (#2009).

Pure policy: cohort ladder, percentile synthesis, blend+envelope, per-share
conversion, dual-class routing, quality scoring — all over rows-as-args, no DB.
The IO wrapper (bottom of file) resolves MarketCapResolution + oracle
membership into plain values BEFORE calling the pure functions.

Spec: docs/proposals/valuation/2026-07-12-deterministic-fair-value-band.md
"""

from __future__ import annotations

from dataclasses import dataclass

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
