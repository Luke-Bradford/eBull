"""Instrument Analytical Record (IAR) evidence signals — #1823 (P2 of #1815).

EVIDENCE-ONLY. None of these signals enter the headline scoring composite (they
ride at weight 0 in the IAR until the #1822/P5 backtest + operator sign-off);
``scoring.compute_score`` persists the assembled block on ``scores.analytics_json``
without ever feeding ``raw_total`` / ``total_score``. See
``docs/specs/ranking/2026-06-29-1823-iar-evidence-signals.md``.

Design split (mirrors ``tests/test_scoring.py``):
  * pure signal math here — table-tested, no DB;
  * the DB-facing assembler (``assemble_instrument_analytics``) loads the inputs,
    reusing the de-duped read paths (``get_insider_summary``,
    ``get_ownership_category_totals``) and the new latest-2-FY concept reader,
    then calls the pure functions.

Source rules: Piotroski (J. Accounting Research 38, 2000); Altman Z" non-
manufacturer recalibration (Altman 2000); SEC Item 403 / FINRA short-interest for
positioning. Missing inputs are reported as missing — NEVER imputed.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from app.services.thesis_break import FRESHNESS_BOUNDS

logger = logging.getLogger(__name__)

# Freshness bound for the FINRA bimonthly snapshot, imported rather than
# restated: `thesis_break.FRESHNESS_BOUNDS` already carries this table's bound
# for its other consumer (`thesis_break_scan._short_interest_observations`), and
# a second hand-written copy is the #1955 sibling-drift class. FINRA designates
# two settlement dates a month and publishes each ~12 calendar days after it
# (finra.org/filing-reporting/short-interest schedule; skill
# `data-sources/finra.md` §1 and §4.1), so the newest disseminated figure is up
# to ~27 days old in normal operation — 45d is that plus a missed cycle.
#
# Keyed on `short_interest_pct_shares_out` because that is the metric this
# reader computes (`short_pct` = short interest / shares outstanding); the bound
# belongs to the `finra_settlement` INPUT, so every metric fed by this table
# carries the same value and the bridge test in
# tests/test_instrument_analytics.py pins them equal.
_FINRA_SETTLEMENT_MAX_AGE = FRESHNESS_BOUNDS["short_interest_pct_shares_out"]["finra_settlement"]

# The OTHER input to the same ratio, imported for the same reason (#2411). A ratio is
# only as fresh as its stalest input, and `thesis_break_scan._short_interest_observations`
# has bounded BOTH since #2010 while this reader bounded neither until #2336 and then only
# the numerator. dei shares outstanding is stated on the cover of every 10-K/10-Q, so 183d
# is a quarterly cadence plus one missed filing (`thesis_break.FRESHNESS_BOUNDS` header).
#
# ⚠ Scoped to the short-interest ratio deliberately. `insider_signal` divides by the same
# share count and is NOT bounded here: FRESHNESS_BOUNDS assigns `share_count_filed` to
# `short_interest_pct_shares_out` and to nothing else, and inventing a bound for a metric
# the vocabulary does not name is the thing this import exists to avoid.
_SHARE_COUNT_FILED_MAX_AGE = FRESHNESS_BOUNDS["short_interest_pct_shares_out"]["share_count_filed"]

# ---------------------------------------------------------------------------
# us-gaap concept resolution (financial_facts_raw holds only the non-dimensional
# default member — companyfacts strips dimensional facts, prevention-log 1879).
# Revenue is ASC-606-fragmented, so it carries a fallback chain (full-population
# verified 2026-06-29). LiabilitiesNoncurrent is absent from our data (0 rows) →
# leverage uses LongTermDebt with a LongTermDebtNoncurrent fallback.
# ---------------------------------------------------------------------------
SCHEMA_VERSION = "iar_v1"

# Single-concept inputs.
_NET_INCOME = ("NetIncomeLoss",)
_ASSETS = ("Assets",)
_ASSETS_CURRENT = ("AssetsCurrent",)
_LIABILITIES = ("Liabilities",)
_LIABILITIES_CURRENT = ("LiabilitiesCurrent",)
_RETAINED_EARNINGS = ("RetainedEarningsAccumulatedDeficit",)
_OPERATING_INCOME = ("OperatingIncomeLoss",)
_EQUITY = ("StockholdersEquity",)
_CFO = ("NetCashProvidedByUsedInOperatingActivities",)
_GROSS_PROFIT = ("GrossProfit",)
_COST_OF_REVENUE = ("CostOfRevenue", "CostOfGoodsAndServicesSold")
# Ordered fallback chains (most-preferred first).
_REVENUE = (
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
)
_LONG_TERM_DEBT = ("LongTermDebt", "LongTermDebtNoncurrent")
_SHARES = ("WeightedAverageNumberOfDilutedSharesOutstanding", "CommonStockSharesOutstanding")

#: Every concept the F/Z reader needs (one query, both FYs).
PIOTROSKI_ALTMAN_CONCEPTS: tuple[str, ...] = (
    *_NET_INCOME,
    *_ASSETS,
    *_ASSETS_CURRENT,
    *_LIABILITIES,
    *_LIABILITIES_CURRENT,
    *_RETAINED_EARNINGS,
    *_OPERATING_INCOME,
    *_EQUITY,
    *_CFO,
    *_GROSS_PROFIT,
    *_COST_OF_REVENUE,
    *_REVENUE,
    *_LONG_TERM_DEBT,
    *_SHARES,
)


def _pick(facts: dict[str, float], chain: tuple[str, ...]) -> float | None:
    """First present, non-None concept value in the fallback chain."""
    for concept in chain:
        v = facts.get(concept)
        if v is not None:
            return v
    return None


def _revenue(facts: dict[str, float]) -> float | None:
    return _pick(facts, _REVENUE)


def _gross_profit(facts: dict[str, float]) -> float | None:
    """GrossProfit direct, else Revenue − CostOfRevenue when both present."""
    gp = _pick(facts, _GROSS_PROFIT)
    if gp is not None:
        return gp
    rev = _revenue(facts)
    cor = _pick(facts, _COST_OF_REVENUE)
    if rev is not None and cor is not None:
        return rev - cor
    return None


# ---------------------------------------------------------------------------
# Piotroski F-score (0-9) — Piotroski (2000).
# 7 of 9 points need a prior FY. A component whose inputs are absent is NOT
# awarded AND NOT counted toward components_available — never imputed.
#
# Documented variant (evidence-only): ROA / asset-turnover use END-of-period
# total assets, not Piotroski's beginning-of-year assets — the canonical
# beginning-asset basis for ΔROA needs THREE consecutive FYs (TA_{t-2}); we read
# two. `roa_positive` is denominator-sign-invariant (Assets>0 ⇒ sign(ROA)=sign(NI)),
# so only the ΔROA / Δasset-turnover trend points use the end-asset basis — applied
# consistently to both years. A common provider variant (Gray & Carlisle), not the
# strict original; the sign rarely flips and this is non-headline evidence.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PiotroskiResult:
    score: int | None
    components_available: int
    band: str | None
    components: dict[str, bool]
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "components_available": self.components_available,
            "band": self.band,
            "components": self.components,
            "reason": self.reason,
        }


def _ratio(numer: float | None, denom: float | None) -> float | None:
    if numer is None or denom is None or denom == 0:
        return None
    return numer / denom


def _band_piotroski(score: int) -> str:
    if score >= 7:
        return "strong"
    if score >= 4:
        return "neutral"
    return "weak"


def piotroski_f(curr: dict[str, float], prior: dict[str, float] | None) -> PiotroskiResult:
    """Compute the Piotroski F-score from one or two FY fact dicts.

    ``components`` maps each evaluated signal to its boolean; only evaluated
    signals count toward ``components_available`` and ``score``. ``band`` is read
    off the raw score (a partial score is a lower bound). Returns an all-empty
    result with ``reason`` when nothing can be evaluated.
    """
    components: dict[str, bool] = {}

    ni = _pick(curr, _NET_INCOME)
    assets = _pick(curr, _ASSETS)
    cfo = _pick(curr, _CFO)
    roa = _ratio(ni, assets)

    # Profitability (4)
    if roa is not None:
        components["roa_positive"] = roa > 0
    if cfo is not None:
        components["cfo_positive"] = cfo > 0
    if cfo is not None and ni is not None:
        components["accrual_cfo_gt_ni"] = cfo > ni

    # Prior-year-dependent signals
    if prior is not None:
        ni_p = _pick(prior, _NET_INCOME)
        assets_p = _pick(prior, _ASSETS)
        roa_p = _ratio(ni_p, assets_p)
        if roa is not None and roa_p is not None:
            components["droa_positive"] = roa > roa_p

        # Leverage: long-term debt / total assets (lower is better)
        ltd = _pick(curr, _LONG_TERM_DEBT)
        ltd_p = _pick(prior, _LONG_TERM_DEBT)
        lev = _ratio(ltd, assets)
        lev_p = _ratio(ltd_p, assets_p)
        if lev is not None and lev_p is not None:
            components["dleverage_down"] = lev < lev_p

        # Current ratio
        cr = _ratio(_pick(curr, _ASSETS_CURRENT), _pick(curr, _LIABILITIES_CURRENT))
        cr_p = _ratio(_pick(prior, _ASSETS_CURRENT), _pick(prior, _LIABILITIES_CURRENT))
        if cr is not None and cr_p is not None:
            components["dcurrent_ratio_up"] = cr > cr_p

        # No new shares (dilution): shares_curr <= shares_prior
        sh = _pick(curr, _SHARES)
        sh_p = _pick(prior, _SHARES)
        if sh is not None and sh_p is not None:
            components["no_new_shares"] = sh <= sh_p

        # Gross margin
        gm = _ratio(_gross_profit(curr), _revenue(curr))
        gm_p = _ratio(_gross_profit(prior), _revenue(prior))
        if gm is not None and gm_p is not None:
            components["dgross_margin_up"] = gm > gm_p

        # Asset turnover
        at = _ratio(_revenue(curr), assets)
        at_p = _ratio(_revenue(prior), assets_p)
        if at is not None and at_p is not None:
            components["dasset_turnover_up"] = at > at_p

    components_available = len(components)
    if components_available == 0:
        return PiotroskiResult(None, 0, None, {}, reason="no_inputs")
    score = sum(1 for v in components.values() if v)
    return PiotroskiResult(score, components_available, _band_piotroski(score), components)


# ---------------------------------------------------------------------------
# Altman Z" (non-manufacturer recalibration) — Altman (2000). Single-period.
#   Z" = 6.56*X1 + 3.26*X2 + 6.72*X3 + 1.05*X4
#   X1=(CA-CL)/TA  X2=RE/TA  X3=EBIT/TA  X4=Equity/TL
# Every input is required; any absent (or TA<=0 / TL<=0) -> null + reason.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AltmanResult:
    z: float | None
    band: str | None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"z": self.z, "band": self.band, "reason": self.reason}


def _band_altman(z: float) -> str:
    if z > 2.60:
        return "safe"
    if z >= 1.10:
        return "grey"
    return "distress"


def altman_z2(facts: dict[str, float]) -> AltmanResult:
    ta = _pick(facts, _ASSETS)
    tl = _pick(facts, _LIABILITIES)
    ca = _pick(facts, _ASSETS_CURRENT)
    cl = _pick(facts, _LIABILITIES_CURRENT)
    re = _pick(facts, _RETAINED_EARNINGS)
    # X3 EBIT proxy = OperatingIncomeLoss. Operating income is the standard
    # XBRL-available EBIT proxy (Damodaran / common screeners); it omits non-
    # operating items, so it is a proxy, not exact EBIT. Acceptable for non-
    # headline evidence — the result carries no claim of being exact EBIT.
    ebit = _pick(facts, _OPERATING_INCOME)
    equity = _pick(facts, _EQUITY)

    if ta is None or ta <= 0:
        return AltmanResult(None, None, reason="no_total_assets")
    if tl is None or tl <= 0:
        return AltmanResult(None, None, reason="no_total_liabilities")
    if any(v is None for v in (ca, cl, re, ebit, equity)):
        return AltmanResult(None, None, reason="missing_input")

    # mypy/pyright: the None-guard above narrows these.
    assert ca is not None and cl is not None and re is not None and ebit is not None and equity is not None
    x1 = (ca - cl) / ta
    x2 = re / ta
    x3 = ebit / ta
    x4 = equity / tl
    z = 6.56 * x1 + 3.26 * x2 + 6.72 * x3 + 1.05 * x4
    return AltmanResult(round(z, 4), _band_altman(z))


# ---------------------------------------------------------------------------
# Positioning signals — normalized to [0,1], 0.5 neutral (#1815 §5).
# ---------------------------------------------------------------------------
def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def insider_signal(net_shares: float | None, shares_outstanding: float | None) -> dict[str, Any]:
    """0.5 + 0.5*tanh((net_shares/shares_out)/0.001); sells floored ~0.40.

    ``net_$ / mktcap == net_shares / shares_outstanding`` (price cancels), so the
    fraction of the company traded is computed directly from open-market net
    shares. Buys signal, sells are noise -> a net-sell can dip to but not below
    ~0.40.
    """
    if net_shares is None or shares_outstanding is None or shares_outstanding <= 0:
        return {"signal": None, "net_shares": net_shares, "reason": "no_insider_or_shares"}
    frac = net_shares / shares_outstanding
    raw = 0.5 + 0.5 * math.tanh(frac / 0.001)
    if net_shares < 0:
        raw = max(raw, 0.40)
    return {
        "signal": round(_clip(raw, 0.0, 1.0), 4),
        "net_shares": net_shares,
        "shares_outstanding": shares_outstanding,
        "caveat": None,
        "source": "insider_transactions",
    }


def inst_13f_signal(delta_shares_pct: float | None) -> dict[str, Any]:
    """0.5 + 0.5*clip(delta_shares_pct/0.10, -1, 1).

    ``delta_shares_pct`` is the QoQ change in de-duped aggregate institutional
    SHARES (not holder count — prevention-log 1866/1873: raw filer-CIK counts are
    corrupted by manager sub-book fanout).
    """
    if delta_shares_pct is None:
        return {"signal": None, "reason": "insufficient_periods"}
    raw = 0.5 + 0.5 * _clip(delta_shares_pct / 0.10, -1.0, 1.0)
    return {
        "signal": round(raw, 4),
        "delta_shares_pct": round(delta_shares_pct, 4),
        "caveat": "<=135d stale",
        "source": "ownership_institutions_observations",
    }


def short_interest_signal(short_pct: float | None, falling: bool | None) -> dict[str, Any]:
    """1 - clip((short_pct - 0.05)/0.25, 0, 1); +0.1 if falling 2 periods.

    ``short_pct`` = current_short_interest / shares_outstanding (public float is
    not ingested, so the denominator is shares outstanding — caveat carried).
    """
    if short_pct is None:
        return {"signal": None, "reason": "no_short_interest_or_shares"}
    raw = 1.0 - _clip((short_pct - 0.05) / 0.25, 0.0, 1.0)
    if falling:
        raw += 0.1
    return {
        "signal": round(_clip(raw, 0.0, 1.0), 4),
        "short_pct": round(short_pct, 4),
        "falling": bool(falling),
        "caveat": "% shares outstanding (public float not ingested); bi-monthly",
        "source": "finra_short_interest_current",
    }


# ---------------------------------------------------------------------------
# Hybrid peer grade — 0.70*absolute + 0.30*sector_percentile (#1815 §6).
# Evidence-only; the headline family score stays absolute (pure percentile would
# reverse scoring.py's banned cohort-relative normalization).
# ---------------------------------------------------------------------------
def percentile_rank(value: float, population: list[float]) -> float:
    """Empirical percentile (fraction of the population strictly below ``value``,
    plus half the ties — the standard mid-rank definition). Empty -> 0.5."""
    n = len(population)
    if n == 0:
        return 0.5
    below = sum(1 for p in population if p < value)
    equal = sum(1 for p in population if p == value)
    return (below + 0.5 * equal) / n


def hybrid_grade(absolute: float, percentile: float) -> float:
    return round(0.70 * absolute + 0.30 * percentile, 4)


#: The six headline families graded relative to peers.
PEER_GRADE_FAMILIES: tuple[str, ...] = (
    "quality",
    "value",
    "turnaround",
    "momentum",
    "sentiment",
    "confidence",
)
_MIN_SECTOR_PEERS = 8
_MIN_UNIVERSE_PEERS = 5


def compute_peer_grades(
    run_items: list[tuple[int, str | None, dict[str, float]]],
) -> dict[int, dict[str, Any]]:
    """Cross-sectional hybrid peer grade for every instrument in a scoring run.

    ``run_items`` = ``[(instrument_id, sector_key, {family: absolute_score})]``
    over the RUN-ELIGIBLE population (NOT the full universe — ``basis`` records
    this). Per family the percentile cohort is, in order of preference:
      * the instrument's eToro sector (n>=8) -> ``run_eligible_sector``;
      * the whole run-eligible universe (5<=n<8) -> ``run_eligible_universe``;
      * else absolute-only -> ``peer_set_thin``.
    Evidence-only: ``hybrid = 0.70*absolute + 0.30*percentile`` never replaces the
    headline absolute family score.
    """
    # Per-family universe + per-sector populations.
    universe: dict[str, list[float]] = {f: [] for f in PEER_GRADE_FAMILIES}
    by_sector: dict[str | None, dict[str, list[float]]] = {}
    for _iid, sector, fam in run_items:
        sec_pop = by_sector.setdefault(sector, {f: [] for f in PEER_GRADE_FAMILIES})
        for f in PEER_GRADE_FAMILIES:
            v = fam.get(f)
            if v is not None:
                universe[f].append(v)
                sec_pop[f].append(v)

    universe_n = max((len(universe[f]) for f in PEER_GRADE_FAMILIES), default=0)

    out: dict[int, dict[str, Any]] = {}
    for iid, sector, fam in run_items:
        sec_pop = by_sector.get(sector, {})
        sector_n = max((len(sec_pop.get(f, [])) for f in PEER_GRADE_FAMILIES), default=0)
        if sector_n >= _MIN_SECTOR_PEERS:
            basis, pop_map, peer_n = "run_eligible_sector", sec_pop, sector_n
        elif universe_n >= _MIN_UNIVERSE_PEERS:
            basis, pop_map, peer_n = "run_eligible_universe", universe, universe_n
        else:
            basis, pop_map, peer_n = "peer_set_thin", None, sector_n

        families: dict[str, Any] = {}
        for f in PEER_GRADE_FAMILIES:
            absolute = fam.get(f)
            if absolute is None:
                continue
            if pop_map is None:
                families[f] = {"absolute": round(absolute, 4), "percentile": None, "hybrid": round(absolute, 4)}
            else:
                pct = percentile_rank(absolute, pop_map.get(f, []))
                families[f] = {
                    "absolute": round(absolute, 4),
                    "percentile": round(pct, 4),
                    "hybrid": hybrid_grade(absolute, pct),
                }
        out[iid] = {"peer_key": sector, "peer_n": peer_n, "basis": basis, "families": families}
    return out


# ---------------------------------------------------------------------------
# DB-facing assembler
# ---------------------------------------------------------------------------
def _read_latest_two_fy_facts(
    conn: psycopg.Connection[Any], instrument_id: int
) -> tuple[dict[str, float] | None, dict[str, float] | None, date | None]:
    """Latest two fiscal years of annual (10-K, fiscal_period='FY') us-gaap facts
    for the F/Z concepts, one ``{concept: float}`` dict per FY (current, prior),
    plus the current FY's canonical period_end (the as-of for freshness bounds,
    #2012 — never the scoring timestamp, which would make stale facts look fresh).

    DISTINCT ON (concept, fiscal_year) collapses to ONE value per concept per FY,
    preferring the canonical FY-end (``period_end DESC``), then the full-year row
    (``(period_end - period_start) DESC``), then the latest filing
    (``filed_date DESC``), with ``fact_id DESC`` as a unique final key. The period_end
    tie-break guards against a comparative prior-year line carried in a later 10-K
    being mistaken for the FY value; the period-span tie-break (#2127 Phase 2) picks
    the annual (≈365-day) row over a Q4 stub the source co-tags ``fiscal_period='FY'``
    with the same period_end — without it the pick was non-deterministic between the
    annual and quarterly value (495 instruments carry ≥1 such tie). The unique
    ``fact_id`` final key makes the pick fully reproducible run-to-run and identical to
    the bulk reader (:func:`_bulk_read_latest_two_fy_facts`). Returns (None, None,
    None) when no annual facts are on file.
    """
    rows: list[tuple[int, str, float, date]]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fiscal_year, concept, val, period_end FROM (
                SELECT DISTINCT ON (concept, fiscal_year)
                    fiscal_year, concept, val, period_end
                FROM financial_facts_raw
                WHERE instrument_id = %(iid)s
                  AND taxonomy = 'us-gaap'
                  AND fiscal_period = 'FY'
                  AND form_type LIKE '10-K%%'
                  AND concept = ANY(%(concepts)s)
                  AND fiscal_year IS NOT NULL
                  AND val IS NOT NULL
                ORDER BY concept, fiscal_year, period_end DESC,
                         (period_end - period_start) DESC NULLS LAST,
                         filed_date DESC, accession_number DESC, fact_id DESC
            ) latest
            WHERE fiscal_year IN (
                SELECT DISTINCT fiscal_year
                FROM financial_facts_raw
                WHERE instrument_id = %(iid)s
                  AND taxonomy = 'us-gaap'
                  AND fiscal_period = 'FY'
                  AND form_type LIKE '10-K%%'
                  AND fiscal_year IS NOT NULL
                ORDER BY fiscal_year DESC
                LIMIT 2
            )
            """,
            {"iid": instrument_id, "concepts": list(PIOTROSKI_ALTMAN_CONCEPTS)},
        )
        rows = [(int(r[0]), str(r[1]), float(r[2]), r[3]) for r in cur.fetchall()]

    if not rows:
        return None, None, None
    years = sorted({fy for fy, _, _, _ in rows}, reverse=True)
    curr_year = years[0]
    prior_year = years[1] if len(years) > 1 else None
    curr = {c: v for fy, c, v, _ in rows if fy == curr_year}
    prior = {c: v for fy, c, v, _ in rows if fy == prior_year} if prior_year is not None else None
    curr_pes = [pe for fy, _, _, pe in rows if fy == curr_year and pe is not None]
    return curr, prior, (max(curr_pes) if curr_pes else None)


def read_latest_fy_altman(conn: psycopg.Connection[Any], instrument_id: int) -> tuple[AltmanResult, date | None]:
    """Latest-FY Altman Z″ plus the FY period_end it was computed from.

    The as-of for #2012's freshness bound is the FACT period end — a
    filing-derived date — never a scoring/scan timestamp. Sector gating
    (financial firms are outside the Z″ model) is the CALLER's job:
    this reader computes for whatever instrument it is given."""
    curr, _prior, period_end = _read_latest_two_fy_facts(conn, instrument_id)
    if curr is None:
        return AltmanResult(None, None, reason="no_annual_facts"), None
    return altman_z2(curr), period_end


def _read_13f_delta(conn: psycopg.Connection[Any], instrument_id: int) -> tuple[float | None, date | None]:
    """QoQ % change in de-duped aggregate 13F shares over the two most recent
    quarters. Reuses the #922 dedup-before-sum series. (None, None) if <2 periods
    or the prior aggregate is zero."""
    from app.services.ownership_history import get_ownership_category_totals

    points = get_ownership_category_totals(conn, instrument_id=instrument_id, category="institutions")
    usable = [p for p in points if p.shares is not None]
    if len(usable) < 2:
        return None, None
    latest, prior = usable[-1], usable[-2]
    prior_sh = float(prior.shares) if prior.shares is not None else 0.0
    latest_sh = float(latest.shares) if latest.shares is not None else 0.0
    if prior_sh <= 0:
        return None, latest.period_end
    return (latest_sh - prior_sh) / prior_sh, latest.period_end


def _short_interest_from_row(
    row: tuple[Any, ...] | None,
    shares_outstanding: float | None,
    *,
    today: date,
    shares_outstanding_filed: date | None,
) -> dict[str, Any]:
    """Pure row→signal for short interest, shared by the per-instrument reader and
    the bulk reader (#2127 Phase 2). ``row`` = (current_short_interest,
    previous_short_interest, days_to_cover, settlement_date) or None.

    BOTH inputs are freshness-gated, because a ratio is only as fresh as its stalest
    input and this one divides two independently-sourced figures.

    Numerator — ``settlement_date`` (#2336). ``finra_short_interest_current``
    is latest-WINS, not current-CYCLE (sql/152) — its INSERT arm is unconditional, so
    an instrument absent from recent FINRA files keeps whatever settlement date last
    named it, and a backfill of old files seeds rows years out of date. Beyond
    ``_FINRA_SETTLEMENT_MAX_AGE`` the signal is suppressed with reason
    ``stale_settlement`` (``asof`` retained so the age stays visible) rather than
    read as live positioning. A NULL settlement date fails closed for the same
    reason ``thesis_break.observe`` does, though the column is NOT NULL.

    Denominator — ``shares_outstanding_filed`` (#2411), the filing date of the share
    count. Beyond ``_SHARE_COUNT_FILED_MAX_AGE`` → ``stale_share_count``. A NULL filed
    date fails closed, same posture. Checked AFTER the settlement gate so a row stale
    on both reports the numerator's age, which is the one an operator can act on
    (FINRA republishes fortnightly; a delinquent filer's share count does not move).
    """
    if row is None or row[0] is None or shares_outstanding is None or shares_outstanding <= 0:
        return short_interest_signal(None, None)
    settlement = row[3]
    if settlement is None or (today - settlement) > _FINRA_SETTLEMENT_MAX_AGE:
        stale = short_interest_signal(None, None)
        stale["reason"] = "stale_settlement"
        stale["max_age_days"] = _FINRA_SETTLEMENT_MAX_AGE.days
        if settlement is not None:
            stale["asof"] = settlement.isoformat()
        return stale
    if shares_outstanding_filed is None or (today - shares_outstanding_filed) > _SHARE_COUNT_FILED_MAX_AGE:
        stale = short_interest_signal(None, None)
        stale["reason"] = "stale_share_count"
        stale["max_age_days"] = _SHARE_COUNT_FILED_MAX_AGE.days
        if shares_outstanding_filed is not None:
            stale["shares_outstanding_asof"] = shares_outstanding_filed.isoformat()
        return stale
    current_si = float(row[0])
    prev_si = float(row[1]) if row[1] is not None else None
    short_pct = current_si / shares_outstanding
    falling = prev_si is not None and current_si < prev_si
    out = short_interest_signal(short_pct, falling)
    if row[2] is not None:
        out["days_to_cover"] = float(row[2])
    if row[3] is not None:
        out["asof"] = row[3].isoformat()
    return out


def _read_short_interest(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    shares_outstanding: float | None,
    *,
    today: date,
    shares_outstanding_filed: date | None,
) -> dict[str, Any]:
    """short_pct + falling + days_to_cover from finra_short_interest_current."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT current_short_interest, previous_short_interest, days_to_cover, settlement_date
            FROM finra_short_interest_current
            WHERE instrument_id = %(iid)s
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    return _short_interest_from_row(
        row, shares_outstanding, today=today, shares_outstanding_filed=shares_outstanding_filed
    )


def _build_analytics_block(
    *,
    gics_sector: str | None,
    shares_outstanding: float | None,
    curr: dict[str, float] | None,
    prior: dict[str, float] | None,
    insider_net: float | None,
    insider_asof: date | None,
    delta_pct: float | None,
    inst_asof: date | None,
    short_interest: dict[str, Any],
) -> dict[str, Any]:
    """Pure IAR-block assembly shared by the per-instrument and bulk paths (#2127
    Phase 2). No DB access — callers own the reads (per-instrument savepoint reads
    or the bulk readers) and pass the resolved signal inputs. Sharing this builder
    guarantees the two paths emit a byte-identical block.

    Degradation contract (must match the readers): ``curr is None`` →
    ``no_annual_facts``; ``insider_net is None`` → insider signal ``None``
    (missing-schema); ``delta_pct is None`` → ``insufficient_periods``;
    ``short_interest`` is a fully-formed signal dict (never None).
    """
    suppress_fz = gics_sector == "Financials"
    block: dict[str, Any] = {"schema": SCHEMA_VERSION}

    # Piotroski + Altman
    if suppress_fz:
        block["piotroski"] = {"score": None, "suppressed": True, "reason": "quality_signal_na_financials"}
        block["altman_z"] = {"z": None, "suppressed": True, "reason": "quality_signal_na_financials"}
    elif curr is None:
        block["piotroski"] = {"score": None, "suppressed": False, "reason": "no_annual_facts"}
        block["altman_z"] = {"z": None, "suppressed": False, "reason": "no_annual_facts"}
    else:
        p = piotroski_f(curr, prior).to_dict()
        p["suppressed"] = False
        z = altman_z2(curr).to_dict()
        z["suppressed"] = False
        block["piotroski"] = p
        block["altman_z"] = z

    # Positioning
    positioning: dict[str, Any] = {}

    ins = insider_signal(insider_net, shares_outstanding)
    if insider_asof is not None:
        ins["asof"] = insider_asof.isoformat()
    positioning["insider_net_90d"] = ins

    inst = inst_13f_signal(delta_pct)
    if inst_asof is not None:
        inst["asof"] = inst_asof.isoformat()
    positioning["inst_13f_qoq"] = inst

    positioning["short_interest"] = short_interest

    block["positioning"] = positioning
    # Default peer_grade so the persisted shape is consistent even when this
    # assembler runs OUTSIDE compute_rankings (a standalone compute_score has no
    # run cohort). compute_rankings overwrites this with the real cross-sectional
    # grade for the batch path.
    block["peer_grade"] = {"basis": "absolute_only", "reason": "no_run_context", "families": {}}
    return block


def assemble_instrument_analytics(
    instrument_id: int,
    conn: psycopg.Connection[Any],
    *,
    gics_sector: str | None,
    shares_outstanding: float | None,
    shares_outstanding_filed: date | None,
    today: date,
) -> dict[str, Any]:
    """Per-instrument IAR evidence block (everything except the cross-sectional
    ``peer_grade``, which ``compute_rankings`` injects from the run population).

    Every DB read is savepoint-guarded (catch UndefinedTable/UndefinedColumn,
    prevention-log 1941) so a partial schema degrades the signal to null rather
    than failing the score. The block itself is assembled by the shared pure
    :func:`_build_analytics_block` (#2127 Phase 2) so this path and the bulk path
    are byte-identical.
    """
    suppress_fz = gics_sector == "Financials"

    # Piotroski/Altman FY facts (skipped for Financials — the block suppresses them).
    curr: dict[str, float] | None = None
    prior: dict[str, float] | None = None
    if not suppress_fz:
        try:
            with conn.transaction():
                curr, prior, _fy_end = _read_latest_two_fy_facts(conn, instrument_id)
        except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
            curr = prior = None

    # insider — reuse the open-market (P/S) de-duped summary
    insider_net: float | None = None
    insider_asof: date | None = None
    try:
        with conn.transaction():
            # Lazy import keeps scoring importable without the insider
            # stack. The historical partial-init import cycle
            # (insider_transactions -> manifest_parsers package init ->
            # insider_345 -> insider_form3_ingest) is dead: #2110 moved
            # the shared `_classify` leaf to app/services/upsert_classify,
            # so importing insider modules no longer fires the parser
            # registry. Guarded by tests/test_import_order_regression.py.
            from app.services.insider_transactions import get_insider_summary

            summary = get_insider_summary(conn, instrument_id=instrument_id)
            # open_market_net_shares_90d is COALESCE'd to 0 by the query (never
            # None today), but guard the cast: a bare float(None) would escape the
            # psycopg-only except and crash the whole score.
            net = summary.open_market_net_shares_90d
            insider_net = float(net) if net is not None else None
            insider_asof = summary.latest_txn_date
    except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
        insider_net = None

    # 13F QoQ aggregate-shares delta
    delta_pct: float | None = None
    inst_asof: date | None = None
    try:
        with conn.transaction():
            delta_pct, inst_asof = _read_13f_delta(conn, instrument_id)
    except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
        delta_pct = None

    # short interest
    try:
        with conn.transaction():
            short_interest = _read_short_interest(
                conn,
                instrument_id,
                shares_outstanding,
                today=today,
                shares_outstanding_filed=shares_outstanding_filed,
            )
    except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
        short_interest = short_interest_signal(None, None)

    return _build_analytics_block(
        gics_sector=gics_sector,
        shares_outstanding=shares_outstanding,
        curr=curr,
        prior=prior,
        insider_net=insider_net,
        insider_asof=insider_asof,
        delta_pct=delta_pct,
        inst_asof=inst_asof,
        short_interest=short_interest,
    )


# ---------------------------------------------------------------------------
# Bulk readers (#2127 Phase 2) — one set-based query per source for a whole
# scoring batch, replacing the per-instrument round-trip storm. Each returns a
# per-id map; the caller (assemble_instrument_analytics_bulk) savepoint-guards the
# read and seeds defaults so a missing id degrades EXACTLY as the per-instrument
# reader. Array params carry the column type (prevention-log #1961).
# ---------------------------------------------------------------------------
def _bulk_read_latest_two_fy_facts(
    conn: psycopg.Connection[Any], instrument_ids: list[int]
) -> dict[int, tuple[dict[str, float] | None, dict[str, float] | None, date | None]]:
    """Bulk form of :func:`_read_latest_two_fy_facts`. Two-step to match the
    per-instrument reader exactly: (1) ``top2`` = each instrument's two latest
    fiscal years over the BROAD 10-K/FY set (``SELECT DISTINCT instrument_id,
    fiscal_year`` then ROW_NUMBER — the same year set the per-instrument
    ``fiscal_year IN (... LIMIT 2)`` subquery uses, NOT a dense-rank over the
    concept-filtered facts, which would drop a prior year that exists broadly but
    has no in-scope concept row); (2) ``deduped`` = one value per (instrument,
    concept, fiscal_year) from the concept+val-filtered set, JOINed to ``top2``.
    Per instrument the curr/prior dicts + period_end are derived exactly as the
    per-instrument path (years present in the result rows). Every requested id is a
    key; ids with no annual facts map to (None, None, None)."""
    out: dict[int, tuple[dict[str, float] | None, dict[str, float] | None, date | None]] = {
        iid: (None, None, None) for iid in instrument_ids
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH top2 AS (
                SELECT instrument_id, fiscal_year
                FROM (
                    SELECT instrument_id, fiscal_year,
                           ROW_NUMBER() OVER (
                               PARTITION BY instrument_id ORDER BY fiscal_year DESC
                           ) AS rn
                    FROM (
                        SELECT DISTINCT instrument_id, fiscal_year
                        FROM financial_facts_raw
                        WHERE instrument_id = ANY(%(ids)s::bigint[])
                          AND taxonomy = 'us-gaap'
                          AND fiscal_period = 'FY'
                          AND form_type LIKE '10-K%%'
                          AND fiscal_year IS NOT NULL
                    ) d
                ) r
                WHERE r.rn <= 2
            ),
            deduped AS (
                SELECT DISTINCT ON (instrument_id, concept, fiscal_year)
                    instrument_id, fiscal_year, concept, val, period_end
                FROM financial_facts_raw
                WHERE instrument_id = ANY(%(ids)s::bigint[])
                  AND taxonomy = 'us-gaap'
                  AND fiscal_period = 'FY'
                  AND form_type LIKE '10-K%%'
                  AND concept = ANY(%(concepts)s)
                  AND fiscal_year IS NOT NULL
                  AND val IS NOT NULL
                ORDER BY instrument_id, concept, fiscal_year, period_end DESC,
                         (period_end - period_start) DESC NULLS LAST,
                         filed_date DESC, accession_number DESC, fact_id DESC
            )
            SELECT d.instrument_id, d.fiscal_year, d.concept, d.val, d.period_end
            FROM deduped d
            JOIN top2 t ON t.instrument_id = d.instrument_id AND t.fiscal_year = d.fiscal_year
            """,
            {"ids": instrument_ids, "concepts": list(PIOTROSKI_ALTMAN_CONCEPTS)},
        )
        rows = [(int(r[0]), int(r[1]), str(r[2]), float(r[3]), r[4]) for r in cur.fetchall()]

    by_iid: dict[int, list[tuple[int, str, float, date]]] = {}
    for iid, fy, concept, val, pe in rows:
        by_iid.setdefault(iid, []).append((fy, concept, val, pe))
    for iid, irows in by_iid.items():
        years = sorted({fy for fy, _, _, _ in irows}, reverse=True)
        curr_year = years[0]
        prior_year = years[1] if len(years) > 1 else None
        curr = {c: v for fy, c, v, _ in irows if fy == curr_year}
        prior = {c: v for fy, c, v, _ in irows if fy == prior_year} if prior_year is not None else None
        curr_pes = [pe for fy, _, _, pe in irows if fy == curr_year and pe is not None]
        out[iid] = (curr, prior, max(curr_pes) if curr_pes else None)
    return out


def _bulk_insider_net_90d(
    conn: psycopg.Connection[Any], instrument_ids: list[int]
) -> dict[int, tuple[float, date | None]]:
    """Bulk open-market net (P−S) shares + latest txn date over the 90-day window,
    per instrument. Mirrors the aggregate in
    :func:`app.services.insider_transactions.get_insider_summary` (same tombstone /
    derivative / valid-date / 90-day filters). ``MAX(txn_date)`` is over ALL
    qualifying rows, not only P/S. Only returns instruments with ≥1 qualifying row —
    the caller seeds no-row ids to (0.0, None) to match get_insider_summary's
    COALESCE-0 (its no-GROUP-BY aggregate always returns net=0 for empty), and
    degrades to net=None only when the whole read fails on a missing schema."""
    out: dict[int, tuple[float, date | None]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT it.instrument_id,
                   COALESCE(SUM(CASE WHEN it.txn_code = 'P' THEN it.shares
                                     WHEN it.txn_code = 'S' THEN -it.shares
                                     ELSE 0 END), 0) AS open_market_net,
                   MAX(it.txn_date) AS latest
            FROM insider_transactions it
            INNER JOIN insider_filings f
                ON f.accession_number = it.accession_number
               AND f.is_tombstone = FALSE
            WHERE it.instrument_id = ANY(%(ids)s::bigint[])
              AND it.is_derivative = FALSE
              AND NOT it.txn_date_invalid
              AND it.txn_date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY it.instrument_id
            """,
            {"ids": instrument_ids},
        )
        for r in cur.fetchall():
            net = r[1]
            out[int(r[0])] = (float(net) if net is not None else 0.0, r[2])
    return out


def _bulk_read_13f_delta(
    conn: psycopg.Connection[Any], instrument_ids: list[int]
) -> dict[int, tuple[float | None, date | None]]:
    """Bulk form of :func:`_read_13f_delta` — QoQ % change in de-duped aggregate 13F
    shares over the two most recent quarters, per instrument. Replicates
    :func:`app.services.ownership_history._institutions_aggregate_history`'s
    dedup-before-sum winner rule (DISTINCT ON (period_end, filer_cik) ORDER BY
    filed_at DESC, source_document_id ASC) and its filters, then keeps the two latest
    quarters per instrument. <2 quarters → (None, None); prior≤0 → (None, latest_pe);
    else ((latest−prior)/prior, latest_pe)."""
    out: dict[int, tuple[float | None, date | None]] = {iid: (None, None) for iid in instrument_ids}
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH winners AS (
                SELECT DISTINCT ON (instrument_id, period_end, filer_cik)
                    instrument_id, period_end, filer_cik, shares
                FROM ownership_institutions_observations
                WHERE instrument_id = ANY(%(ids)s::bigint[])
                  AND known_to IS NULL
                  AND shares IS NOT NULL
                  AND exposure_kind = 'EQUITY'
                  AND ownership_nature = 'economic'
                ORDER BY instrument_id, period_end, filer_cik, filed_at DESC, source_document_id ASC
            ),
            per_quarter AS (
                SELECT instrument_id, period_end, SUM(shares) AS shares,
                       ROW_NUMBER() OVER (
                           PARTITION BY instrument_id ORDER BY period_end DESC
                       ) AS rn
                FROM winners
                GROUP BY instrument_id, period_end
            )
            SELECT instrument_id, period_end, shares, rn
            FROM per_quarter
            WHERE rn <= 2
            ORDER BY instrument_id, rn
            """,
            {"ids": instrument_ids},
        )
        rows = cur.fetchall()
    by_iid: dict[int, list[tuple[date, Any]]] = {}
    for iid, pe, shares, _rn in rows:
        by_iid.setdefault(int(iid), []).append((pe, shares))  # rn ASC → latest first
    for iid, q in by_iid.items():
        if len(q) < 2:
            continue  # <2 quarters → stays (None, None)
        latest_pe, latest_sh_raw = q[0]
        _prior_pe, prior_sh_raw = q[1]
        latest_sh = float(latest_sh_raw) if latest_sh_raw is not None else 0.0
        prior_sh = float(prior_sh_raw) if prior_sh_raw is not None else 0.0
        if prior_sh <= 0:
            out[iid] = (None, latest_pe)
        else:
            out[iid] = ((latest_sh - prior_sh) / prior_sh, latest_pe)
    return out


def _bulk_read_short_interest(
    conn: psycopg.Connection[Any],
    instrument_ids: list[int],
    shares_outstanding_by_id: Mapping[int, float | None],
    *,
    today: date,
    shares_outstanding_filed_by_id: Mapping[int, date | None],
) -> dict[int, dict[str, Any]]:
    """Bulk form of :func:`_read_short_interest` (one row per instrument;
    ``finra_short_interest_current`` PK is instrument_id). Each id's signal via the
    shared :func:`_short_interest_from_row`; ids with no row →
    short_interest_signal(None, None)."""
    out: dict[int, dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, current_short_interest, previous_short_interest,
                   days_to_cover, settlement_date
            FROM finra_short_interest_current
            WHERE instrument_id = ANY(%(ids)s::bigint[])
            """,
            {"ids": instrument_ids},
        )
        for r in cur.fetchall():
            iid = int(r[0])
            out[iid] = _short_interest_from_row(
                (r[1], r[2], r[3], r[4]),
                shares_outstanding_by_id.get(iid),
                today=today,
                shares_outstanding_filed=shares_outstanding_filed_by_id.get(iid),
            )
    for iid in instrument_ids:
        out.setdefault(iid, short_interest_signal(None, None))
    return out


def assemble_instrument_analytics_bulk(
    conn: psycopg.Connection[Any],
    instrument_ids: list[int],
    *,
    gics_sector_by_id: Mapping[int, str | None],
    shares_outstanding_by_id: Mapping[int, float | None],
    shares_outstanding_filed_by_id: Mapping[int, date | None],
    today: date,
) -> dict[int, dict[str, Any]]:
    """Bulk IAR evidence blocks for a whole scoring batch (#2127 Phase 2). One
    set-based read per source (each savepoint-wrapped and fail-open on ANY exception
    — a reader failure degrades that one signal batch-wide, never aborts the run),
    then the shared pure :func:`_build_analytics_block` per id. Returns a block for
    EVERY requested id.

    Per-id error isolation (spec §Phase 2): if building a single id's block raises,
    that id degrades to the empty-analytics default (never re-raises) so one bad id
    can never fail the batch. Both isolations together preserve the pre-Phase-2
    invariant (an analytics failure never kills the run); the granularity differs
    (a reader failure degrades a signal batch-wide vs the per-instrument path's
    per-id skip) but this is unreachable on real data — the pure readers/builder do
    not raise on well-formed inputs; the full-population A/B confirms 3916/3916.

    Insider carries a degraded flag: get_insider_summary's COALESCE makes a no-txn
    instrument net=0.0, but a missing table must degrade to net=None (Codex ckpt-1).
    FY facts / 13F / short interest degrade identically whether the read raised or
    returned no rows, so their default map entries suffice."""
    # Each bulk read is savepoint-wrapped and catches ANY exception, not just missing
    # schema (Codex ckpt-2): a non-schema reader failure (a bad value cast on one
    # fetched row, a query error) must degrade that ONE evidence signal batch-wide,
    # never abort the whole rankings run — the pre-Phase-2 per-instrument path caught
    # every analytics failure in compute_rankings' per-id try and skipped only that
    # instrument. Fail-open (evidence-only, weight 0), logged with the traceback so a
    # real breakage stays visible. Coarser than per-instrument degradation, but a
    # reader raising is unreachable on real data (full-pop A/B 3916/3916).
    try:
        with conn.transaction():
            fy_map = _bulk_read_latest_two_fy_facts(conn, instrument_ids)
    except Exception:
        logger.warning("assemble_instrument_analytics_bulk: FY-facts bulk read failed; degrading", exc_info=True)
        fy_map = {}

    insider_degraded = False
    insider_map: dict[int, tuple[float, date | None]] = {}
    try:
        with conn.transaction():
            insider_map = _bulk_insider_net_90d(conn, instrument_ids)
    except Exception:
        logger.warning("assemble_instrument_analytics_bulk: insider bulk read failed; degrading", exc_info=True)
        insider_degraded = True

    try:
        with conn.transaction():
            delta_map = _bulk_read_13f_delta(conn, instrument_ids)
    except Exception:
        logger.warning("assemble_instrument_analytics_bulk: 13F-delta bulk read failed; degrading", exc_info=True)
        delta_map = {}

    try:
        with conn.transaction():
            si_map = _bulk_read_short_interest(
                conn,
                instrument_ids,
                shares_outstanding_by_id,
                today=today,
                shares_outstanding_filed_by_id=shares_outstanding_filed_by_id,
            )
    except Exception:
        logger.warning("assemble_instrument_analytics_bulk: short-interest bulk read failed; degrading", exc_info=True)
        si_map = {}

    out: dict[int, dict[str, Any]] = {}
    for iid in instrument_ids:
        gics = gics_sector_by_id.get(iid)
        shares = shares_outstanding_by_id.get(iid)
        try:
            curr, prior, _pe = fy_map.get(iid, (None, None, None))
            if insider_degraded:
                insider_net: float | None = None
                insider_asof: date | None = None
            else:
                insider_net, insider_asof = insider_map.get(iid, (0.0, None))
            delta_pct, inst_asof = delta_map.get(iid, (None, None))
            short_interest = si_map.get(iid) or short_interest_signal(None, None)
            out[iid] = _build_analytics_block(
                gics_sector=gics,
                shares_outstanding=shares,
                curr=curr,
                prior=prior,
                insider_net=insider_net,
                insider_asof=insider_asof,
                delta_pct=delta_pct,
                inst_asof=inst_asof,
                short_interest=short_interest,
            )
        except Exception:
            logger.warning(
                "assemble_instrument_analytics_bulk: block build failed for instrument_id=%d; degrading",
                iid,
                exc_info=True,
            )
            out[iid] = _build_analytics_block(
                gics_sector=gics,
                shares_outstanding=shares,
                curr=None,
                prior=None,
                insider_net=None,
                insider_asof=None,
                delta_pct=None,
                inst_asof=None,
                short_interest=short_interest_signal(None, None),
            )
    return out
