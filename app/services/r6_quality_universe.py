"""#2901 quality arm: construction rules 2-7 as pure functions.

Spec: ``docs/proposals/ta/2026-09-24-2901-quality-arm.md`` ("Construction"). Every constant
the rules use is in :data:`POLICY` and is fixed by construction (the spec's source rules say
which ones HXZ quotes and which are ours). Nothing here reads a file, a price or a return: the
fundamentals come through the #3360 reader (``pit_fundamentals``), the linkage through a
#3361 ``LinkResult``, and prices as rows the caller has already validated.

Order of use at formation D (rule numbers are the spec's):

- rule 2: :func:`is_candidate` on each series' ``link_as_of(S, D)``; :func:`choose_series`
  picks one series per CIK BEFORE executability; :func:`is_executable` on its X(D) row;
- rules 3-6: :func:`classify` walks the ladder up to ``equity_negative`` (the pre-X(D)
  ladder); the caller appends ``not_executable``;
- rule 7: :func:`gpa` and :func:`top_decile` over the eligible set E(D).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any, Final

from app.services import pit_fundamentals as pf
from app.services import security_linkage as sl

REVENUE: Final = (  # declared order: broadest first (REVT is total revenue)
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
)
COGS: Final = ("CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold")
ASSETS: Final = ("Assets",)
EQUITY: Final = ("StockholdersEquity",)  # parent equity only (D3)
GROSS_PROFIT: Final = ("GrossProfit",)  # read as a field status only; never an operand
UNIT: Final = "USD"
ANNUAL_DAYS: Final = (350, 380)  # inclusive, (end - start) + 1; covers 52/53-week years (D7)
FPI_FORMS: Final = frozenset({"20-F", "40-F"})
ANNUAL_FORMS: Final = frozenset({"10-K", *FPI_FORMS})  # form families (``pf.form_family``)
SIC_RANGE: Final = (100, 9999)
SIC_FINANCIAL: Final = (6000, 6999)  # HXZ "Stock Sample"
CANDIDATE_GRAMMARS: Final = frozenset({"plain", "class"})
TOP_DECILE: Final = 9
MIN_ELIGIBLE: Final = 100  # a formation with fewer eligible issuers refuses the build

NOT_EXECUTABLE: Final = "not_executable"
ELIGIBLE: Final = "eligible"
RUNGS: Final = (
    "fundamentals_integrity_excluded",
    "no_companyfacts_entry",
    "sic_missing",
    "sic_financial",
    "no_public_annual_period",
    "several_annual_starts",
    "foreign_private_issuer",
    "gross_profit_unavailable",
    "component_sign_invalid",
    "assets_unavailable",
    "assets_nonpositive",
    "equity_unavailable",
    "equity_negative",
    NOT_EXECUTABLE,
    ELIGIBLE,
)
NOT_EVALUATED: Final = "not_evaluated"

POLICY: Final[dict[str, Any]] = {
    "aliases": {
        "revenue": list(REVENUE),
        "cogs": list(COGS),
        "assets": list(ASSETS),
        "equity": list(EQUITY),
        "gross_profit_status_only": list(GROSS_PROFIT),
    },
    "unit": UNIT,
    "annual_days_inclusive": list(ANNUAL_DAYS),
    "annual_form_families": sorted(ANNUAL_FORMS),
    "fpi_form_families": sorted(FPI_FORMS),
    "sic_range": list(SIC_RANGE),
    "sic_financial": list(SIC_FINANCIAL),
    "candidate_grammars": sorted(CANDIDATE_GRAMMARS),
    "top_decile": TOP_DECILE,
    "min_eligible": MIN_ELIGIBLE,
    "rungs": list(RUNGS),
}


class QualityUniverseError(RuntimeError):
    """A configuration read or a formation that the construction refuses."""


# --------------------------------------------------------------------------- rule 2


def is_candidate(result: sl.LinkResult) -> bool:
    """``linked`` with ``plain`` or ``class:*`` grammar. ``q_alias`` is allowed (dropping
    bankrupt ``...Q`` names favours survivors); ``link_as_of`` has already applied the bar
    range and the inventory-wide collision guard."""
    kind = (result.grammar or "").split(":")[0]
    return result.reason is sl.Reason.LINKED and kind in CANDIDATE_GRAMMARS and result.cik is not None


def choose_series(liquidity: Mapping[int, Decimal | None]) -> int:
    """One series per CIK, chosen before executability: highest liquidity, then lowest
    ``series_id``; an unavailable liquidity ranks below any value."""
    if not liquidity:
        raise QualityUniverseError("no candidate series")
    return min(liquidity, key=lambda sid: (liquidity[sid] is None, -(liquidity[sid] or 0), sid))


def is_executable(
    raw_open: Decimal | None, raw_close: Decimal | None, adjusted_close: Decimal | None, volume: int | None
) -> bool:
    """The chosen series' X(D) row: positive finite raw open, raw close and adjusted close,
    volume > 0. ``None`` is a missing or unparseable field (or no row at all)."""
    prices = (raw_open, raw_close, adjusted_close)
    if any(p is None or not p.is_finite() or p <= 0 for p in prices):
        return False
    return volume is not None and volume > 0


# --------------------------------------------------------------------------- rules 3-6


def parse_sic(raw: object) -> int | None:
    """Rule 3: a 1-4 digit ASCII string or an integer, inside ``SIC_RANGE``; else missing."""
    if isinstance(raw, str) and raw.isascii() and raw.isdigit() and len(raw) <= 4:
        value = int(raw)
    elif isinstance(raw, int) and not isinstance(raw, bool):
        value = raw
    else:
        return None
    return value if SIC_RANGE[0] <= value <= SIC_RANGE[1] else None


@dataclass(frozen=True)
class Component:
    """Rule 5 for one item. ``status`` is ``absent``, ``value`` or the winning alias's
    non-value reader status; ``value`` is its canonical decimal string."""

    status: str
    value: str | None = None
    alias: str | None = None
    accns: tuple[str, ...] = ()
    acceptance: str | None = None
    aliases_disagree: bool = False

    @property
    def number(self) -> Decimal | None:
        return None if self.value is None else Decimal(self.value)


def read_component(
    bundle: pf.PitFundamentalsBundle, cik10: str, concepts: tuple[str, ...], start: str | None, end: str, d: date
) -> Component:
    """Among aliases whose read is not ABSENT, the latest public acceptance decides (the
    reader returns an acceptance for value, ambiguous and blocked reads alike); at equal
    acceptance the declared alias order decides. The winner's status is the component's:
    ambiguous or blocked is unavailable, with no fallback to another alias."""
    reads: list[tuple[str, pf.ValueRead]] = []
    for concept in concepts:
        read = bundle.value_as_of(cik10, pf.FactKey("us-gaap", concept, UNIT, start, end), d)
        if read.status in (pf.ReadStatus.AFTER_CAPTURE, pf.ReadStatus.CONCEPT_NOT_IN_POLICY):
            raise QualityUniverseError(f"configuration read {read.status} for {cik10} {concept} at {d}")
        if read.status is not pf.ReadStatus.ABSENT:
            if read.acceptance is None:
                raise QualityUniverseError(f"non-absent read without acceptance: {cik10} {concept} {d}")
            reads.append((concept, read))
    if not reads:
        return Component("absent")
    latest = max(read.acceptance or "" for _, read in reads)
    top = [(concept, read) for concept, read in reads if read.acceptance == latest]  # declared order
    disagree = len({read.values for _, read in top}) > 1 or any(r.status is not pf.ReadStatus.VALUE for _, r in top)
    alias, winner = top[0]
    if winner.status is not pf.ReadStatus.VALUE:
        # An ambiguous winner still names its accessions, and rule 6 reads them.
        return Component(str(winner.status), None, alias, winner.accns, latest, disagree)
    return Component("value", winner.values[0], alias, winner.accns, latest, disagree)


def _public(bundle: pf.PitFundamentalsBundle, cik10: str, concept: str, d: date) -> pf.PrefixRead:
    """The prefix reader, refusing any gated read: ``classify`` screens integrity and bundle
    membership first, so a gate here is configuration (after capture, concept outside
    policy) and aborts rather than reading as "no period"."""
    read = bundle.public_events(cik10, "us-gaap", concept, d)
    if read.status is not pf.ReadStatus.OK:
        raise QualityUniverseError(f"configuration read {read.status} for {cik10} {concept} at {d}")
    return read


def accession_index(bundle: pf.PitFundamentalsBundle, cik10: str, d: date) -> tuple[Mapping[str, Any], ...]:
    """The CIK's whole public accession index at D (#3360 rule 6). ``public_events`` returns
    the shard-wide index whichever concept is named; ``REVENUE[0]`` only passes its gate."""
    return _public(bundle, cik10, REVENUE[0], d).accessions


def annual_period(bundle: pf.PitFundamentalsBundle, cik10: str, d: date) -> tuple[str, str] | str:
    """Rule 4: ``(start, end)``, or the rung that stops it. Candidates are public events AND
    public blocking rejections of the revenue and COGS aliases (so a later rejection-only
    period is never skipped past), from a 10-K/20-F/40-F-family accession, with an annual
    duration ending in calendar year ``D.year - 1``. No fallback to an older year."""
    forms = {a["accn"]: pf.form_family(a["form"]) for a in accession_index(bundle, cik10, d)}
    starts: dict[str, set[str]] = defaultdict(set)
    for concept in (*REVENUE, *COGS):
        read = _public(bundle, cik10, concept, d)
        for row in (*read.events, *read.rejections):
            if row["unit"] != UNIT or forms.get(row["accn"]) not in ANNUAL_FORMS:
                continue
            start, end = _iso_date(row["start"]), _iso_date(row["end"])
            if start is None or end is None:  # a rejection may keep a raw, unparseable period key
                continue
            if end.year == d.year - 1 and ANNUAL_DAYS[0] <= (end - start).days + 1 <= ANNUAL_DAYS[1]:
                starts[row["end"]].add(row["start"])
    if not starts:
        return "no_public_annual_period"
    end = max(starts)
    if len(starts[end]) > 1:
        return "several_annual_starts"
    return next(iter(starts[end])), end


def is_foreign_private_issuer(bundle: pf.PitFundamentalsBundle, cik10: str, d: date, winners: Iterable[str]) -> bool:
    """Rule 6 (D4): the latest public ORIGINAL (non-``/A``) annual-report accession is a
    20-F/40-F (any such form at that acceptance counts), or any winning component accession is."""
    accessions = accession_index(bundle, cik10, d)
    forms = {a["accn"]: pf.form_family(a["form"]) for a in accessions}
    originals = [
        (a["acceptance"], pf.form_family(a["form"]))
        for a in accessions
        if not a["form"].endswith("/A") and pf.form_family(a["form"]) in ANNUAL_FORMS
    ]
    latest = max((acc for acc, _ in originals), default=None)
    latest_fpi = any(form in FPI_FORMS for acc, form in originals if acc == latest)
    return latest_fpi or any(forms.get(accn) in FPI_FORMS for accn in winners)


@dataclass(frozen=True)
class Classification:
    """The first failing pre-X(D) rung (or ``eligible``), every field status counted
    independently of the ladder, and the inputs rule 7 and the artefact need."""

    rung: str
    fields: dict[str, str]
    sic: int | None = None
    period: tuple[str, str] | None = None
    components: dict[str, Component] = field(default_factory=dict)


def classify(bundle: pf.PitFundamentalsBundle, cik10: str, sic: int | None, d: date) -> Classification:
    """Rules 3-6. ``sic`` is :func:`parse_sic` of the submissions ``sic`` (a CURRENT value, R3).
    Stops before executability: an ``eligible`` result is eligible up to the X(D) row."""
    fields: dict[str, str] = dict.fromkeys(
        ("period", "revenue", "cogs", "assets", "equity", "gp_tag", "fpi"), NOT_EVALUATED
    )
    if cik10 in bundle.integrity_excluded:
        return Classification("fundamentals_integrity_excluded", fields)
    if cik10 not in bundle.ciks:
        return Classification("no_companyfacts_entry", fields)
    ladder: list[str] = []
    fields["sic"] = "missing" if sic is None else "financial" if _financial(sic) else "other"
    if sic is None:
        ladder.append("sic_missing")
    elif _financial(sic):
        ladder.append("sic_financial")
    period = annual_period(bundle, cik10, d)
    if isinstance(period, str):
        fields["period"] = period
        return Classification((ladder + [period])[0], fields, sic)
    fields["period"] = "found"
    start, end = period
    components = {
        "revenue": read_component(bundle, cik10, REVENUE, start, end, d),
        "cogs": read_component(bundle, cik10, COGS, start, end, d),
        "assets": read_component(bundle, cik10, ASSETS, None, end, d),
        "equity": read_component(bundle, cik10, EQUITY, None, end, d),
        "gp_tag": read_component(bundle, cik10, GROSS_PROFIT, start, end, d),
    }
    for name, component in components.items():
        fields[name] = component.status
        if component.aliases_disagree:
            fields[f"{name}_aliases_disagree_at_latest"] = "true"
    rev, cogs, assets, equity = (components[n].number for n in ("revenue", "cogs", "assets", "equity"))
    winners = {accn for n in ("revenue", "cogs", "assets", "equity") for accn in components[n].accns}
    fpi = is_foreign_private_issuer(bundle, cik10, d, winners)
    fields["fpi"] = str(fpi).lower()
    if fpi:
        ladder.append("foreign_private_issuer")
    if rev is None or cogs is None:
        ladder.append("gross_profit_unavailable")
    elif rev < 0 or cogs < 0:
        ladder.append("component_sign_invalid")
    if assets is None:
        ladder.append("assets_unavailable")
    elif assets <= 0:
        ladder.append("assets_nonpositive")
    if equity is None:
        ladder.append("equity_unavailable")
    elif equity < 0:  # the quoted rule excludes NEGATIVE book equity: zero is kept
        ladder.append("equity_negative")
    if rev is not None and cogs is not None and assets is not None and equity is not None:
        fields["winning_accessions"] = "one" if len(winners) == 1 else "several"
        checks = (("revenue", rev >= 0), ("cogs", cogs >= 0), ("assets", assets > 0), ("equity", equity >= 0))
        bad = [name for name, ok in checks if not ok]
        fields["signs"] = "ok" if not bad else "+".join(bad)
    else:
        fields["winning_accessions"] = fields["signs"] = "components_unavailable"
    return Classification(ladder[0] if ladder else ELIGIBLE, fields, sic, period, components)


def _iso_date(raw: object) -> date | None:
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _financial(sic: int) -> bool:
    return SIC_FINANCIAL[0] <= sic <= SIC_FINANCIAL[1]


# --------------------------------------------------------------------------- rule 7


def gpa(classification: Classification) -> Fraction:
    """(revenue - COGS) / assets, exact: each operand's canonical decimal string becomes a
    ``Fraction`` before any arithmetic."""
    if classification.rung != ELIGIBLE:
        raise QualityUniverseError(f"GP/A of a non-eligible issuer ({classification.rung})")
    operands = {name: classification.components[name].value for name in ("revenue", "cogs", "assets")}
    if any(value is None for value in operands.values()):
        raise QualityUniverseError("eligible issuer without all GP/A operands")
    revenue, cogs, assets = (Fraction(str(operands[n])) for n in ("revenue", "cogs", "assets"))
    return (revenue - cogs) / assets


@dataclass(frozen=True)
class Deciles:
    """Rule 7 over E(D). ``arm`` = decile 9 (exactly ``n // 10`` names) plus every issuer
    whose GP/A equals the lowest decile-9 value; ``boundary_ties`` counts those additions."""

    decile: dict[str, int]
    arm: frozenset[str]
    boundary_ties: int


def top_decile(signal: Mapping[str, Fraction]) -> Deciles:
    """Order by (GP/A, CIK) ascending, zero-based rank r, decile ``10 * r // n``. Ties at the
    decile-9 boundary are never split by CIK. ``n < MIN_ELIGIBLE`` refuses."""
    n = len(signal)
    if n < MIN_ELIGIBLE:
        raise QualityUniverseError(f"formation has {n} eligible issuers, below {MIN_ELIGIBLE}")
    ranked = sorted(signal, key=lambda cik: (signal[cik], cik))
    decile = {cik: 10 * rank // n for rank, cik in enumerate(ranked)}
    top = [cik for cik in ranked if decile[cik] == TOP_DECILE]
    floor = signal[top[0]]
    ties = frozenset(cik for cik in ranked if decile[cik] < TOP_DECILE and signal[cik] == floor)
    return Deciles(decile, frozenset(top) | ties, len(ties))
