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

import math
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

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
) -> tuple[dict[str, float] | None, dict[str, float] | None]:
    """Latest two fiscal years of annual (10-K, fiscal_period='FY') us-gaap facts
    for the F/Z concepts, one ``{concept: float}`` dict per FY (current, prior).

    DISTINCT ON (concept, fiscal_year) collapses to ONE value per concept per FY,
    preferring the canonical FY-end (``period_end DESC``) then the latest filing
    (``filed_date DESC``). The period_end tie-break guards against a comparative
    prior-year line carried in a later 10-K being mistaken for the FY value.
    Returns (None, None) when no annual facts are on file.
    """
    rows: list[tuple[int, str, float]]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fiscal_year, concept, val FROM (
                SELECT DISTINCT ON (concept, fiscal_year)
                    fiscal_year, concept, val
                FROM financial_facts_raw
                WHERE instrument_id = %(iid)s
                  AND taxonomy = 'us-gaap'
                  AND fiscal_period = 'FY'
                  AND form_type LIKE '10-K%%'
                  AND concept = ANY(%(concepts)s)
                  AND fiscal_year IS NOT NULL
                  AND val IS NOT NULL
                ORDER BY concept, fiscal_year, period_end DESC, filed_date DESC, accession_number DESC
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
        rows = [(int(r[0]), str(r[1]), float(r[2])) for r in cur.fetchall()]

    if not rows:
        return None, None
    years = sorted({fy for fy, _, _ in rows}, reverse=True)
    curr_year = years[0]
    prior_year = years[1] if len(years) > 1 else None
    curr = {c: v for fy, c, v in rows if fy == curr_year}
    prior = {c: v for fy, c, v in rows if fy == prior_year} if prior_year is not None else None
    return curr, prior


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


def _read_short_interest(
    conn: psycopg.Connection[Any], instrument_id: int, shares_outstanding: float | None
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
    if row is None or row[0] is None or shares_outstanding is None or shares_outstanding <= 0:
        return short_interest_signal(None, None)
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


def assemble_instrument_analytics(
    instrument_id: int,
    conn: psycopg.Connection[Any],
    *,
    gics_sector: str | None,
    shares_outstanding: float | None,
) -> dict[str, Any]:
    """Per-instrument IAR evidence block (everything except the cross-sectional
    ``peer_grade``, which ``compute_rankings`` injects from the run population).

    Every DB read is savepoint-guarded (catch UndefinedTable/UndefinedColumn,
    prevention-log 1941) so a partial schema degrades the signal to null rather
    than failing the score.
    """
    suppress_fz = gics_sector == "Financials"
    block: dict[str, Any] = {"schema": SCHEMA_VERSION}

    # Piotroski + Altman
    if suppress_fz:
        block["piotroski"] = {"score": None, "suppressed": True, "reason": "quality_signal_na_financials"}
        block["altman_z"] = {"z": None, "suppressed": True, "reason": "quality_signal_na_financials"}
    else:
        curr = prior = None
        try:
            with conn.transaction():
                curr, prior = _read_latest_two_fy_facts(conn, instrument_id)
        except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
            curr = prior = None
        if curr is None:
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

    # insider — reuse the open-market (P/S) de-duped summary
    insider_net: float | None = None
    insider_asof: date | None = None
    try:
        with conn.transaction():
            # Prime manifest_parsers BEFORE insider_transactions to avoid the
            # documented partial-init import cycle (review-prevention-log:
            # insider_transactions -> manifest_parsers._classify -> insider_345 ->
            # insider_form3_ingest -> insider_transactions). The app/test
            # entrypoints import manifest_parsers first by side effect; a fresh
            # import order (standalone scoring run) would otherwise re-enter.
            import app.services.manifest_parsers  # noqa: F401
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
    ins = insider_signal(insider_net, shares_outstanding)
    if insider_asof is not None:
        ins["asof"] = insider_asof.isoformat()
    positioning["insider_net_90d"] = ins

    # 13F QoQ aggregate-shares delta
    delta_pct: float | None = None
    inst_asof: date | None = None
    try:
        with conn.transaction():
            delta_pct, inst_asof = _read_13f_delta(conn, instrument_id)
    except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
        delta_pct = None
    inst = inst_13f_signal(delta_pct)
    if inst_asof is not None:
        inst["asof"] = inst_asof.isoformat()
    positioning["inst_13f_qoq"] = inst

    # short interest
    try:
        with conn.transaction():
            positioning["short_interest"] = _read_short_interest(conn, instrument_id, shares_outstanding)
    except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
        positioning["short_interest"] = short_interest_signal(None, None)

    block["positioning"] = positioning
    # Default peer_grade so the persisted shape is consistent even when this
    # assembler runs OUTSIDE compute_rankings (a standalone compute_score has no
    # run cohort). compute_rankings overwrites this with the real cross-sectional
    # grade for the batch path.
    block["peer_grade"] = {"basis": "absolute_only", "reason": "no_run_context", "families": {}}
    return block
