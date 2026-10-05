"""#3609 step 1: the factor panel's accounting and market-equity core.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` (§"Dates and stages", §"Reading the bundle",
§"Market equity", §"Accounting", §"Fidelity"). Everything here is a pure function of a #3360 bundle read at
one decision session s(M) plus the caller's prices and split stamps; the DB-backed builder
(``scripts/build_3609_factor_panel.py``) supplies those.

Reading rules that are ours, by construction, where the spec leaves a choice:

- **Synonym concepts vs formula branches.** A concept list (``sale*``: ``Revenues``, then the ASC 606 tag, then
  ``SalesRevenueNet``) is one Compustat item under several tags, so it falls back per quarter. A formula branch
  (``gp*`` = GP, else ``sale*`` − COGS) is a different item, so it falls back per period.
- **Key ranking.** Where several keys could serve one interval (two annual durations ending at E), the key with
  the latest public item wins; a tie between different keys at that acceptance is ``ambiguous``.
- **Period anchors** come from stored ``Assets`` events only. A rejected row carries no usable date.
- **Units.** Monetary concepts are read in ``USD`` only; a filer reporting in another currency reads ``absent``.
"""

from __future__ import annotations

import calendar
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any, Final, Protocol

from app.services.pit_fundamentals import FactKey, PrefixRead, ReadStatus, ValueRead, acceptance_ny_date, form_family

# --------------------------------------------------------------------------- constants (spec, by construction)

#: Stage A formations (§"Dates and stages"); holding months 2014-10 .. 2021-05.
STAGE_A_FIRST_FORMATION: Final = date(2014, 9, 30)
STAGE_A_LAST_FORMATION: Final = date(2021, 4, 30)
#: JKP §6.2: data are assumed public 4 months after the period end.
LAG_MONTHS: Final = 4
#: Accounting value older than this at M is missing (ours; JKP states no limit).
MAX_ACCOUNTING_AGE_MONTHS: Final = 18
#: A filer has a 10-K/10-Q-family accession accepted within this window before s(M).
FILER_WINDOW_MONTHS: Final = 18
#: A share count's context date must be within this many months of s(M).
MAX_SHARES_AGE_MONTHS: Final = 15
#: Amendment 2: XBRL US DQC_0095 (v30.0.4) -- two share counts may differ by at most 100 times. Checks 1, 2 and 4.
SCALE_TOLERANCE: Final = Decimal(100)
#: Amendment 2 check 3: trailing dollar volume / ME, calibrated on the 2026-10-05 proof artefact and frozen.
DOLLAR_VOLUME_ME_CEILING: Final = 10.0
ANNUAL_DAYS: Final = (350, 380)
QUARTER_DAYS: Final = (80, 100)
#: TTM chain: each quarter starts within this many days after the prior one's end.
QUARTER_GAP_DAYS: Final = 7
#: ``at_gr1``: the prior period ends 12 months ± this many days before E.
YOY_TOLERANCE_DAYS: Final = 14
#: SEC SIC list: "Real Estate Investment Trusts".
REIT_SIC: Final = 6798
MIN_LEG_NAMES: Final = 5

ANNUAL_FAMILY: Final = "10-K"
QUARTERLY_FAMILY: Final = "10-Q"
_ROLE_FAMILIES: Final = frozenset({ANNUAL_FAMILY, QUARTERLY_FAMILY})
USD: Final = "USD"
SHARES: Final = "shares"

SALE: Final = ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet")
GP: Final = ("GrossProfit",)
#: Amendment 2c's parts of ``ope*`` and ``gp*`` (spec §"Accounting"). COGS*: Compustat COGS excludes depreciation,
#: so each excluding-DDA tag is read before its inclusive counterpart; else goods + services.
COGS_TOTAL: Final = (
    "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization",
    "CostOfGoodsAndServicesSold",
    "CostOfRevenue",
)
COGS_GOODS: Final = ("CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization", "CostOfGoodsSold")
COGS_SERVICES: Final = ("CostOfServicesExcludingDepreciationDepletionAndAmortization", "CostOfServices")
#: XSGA* (Compustat scope: SG&A plus R&D net of in-process R&D) = SG&A + RD*, else G&A + selling + RD*.
XSGA: Final = ("SellingGeneralAndAdministrativeExpense",)
GA: Final = ("GeneralAndAdministrativeExpense",)
SELL: Final = ("SellingAndMarketingExpense", "SellingExpense")
SELL_WITNESSES: Final = (*SELL, "MarketingExpense", "MarketingAndAdvertisingExpense")
#: RD*: the excluding-IPR&D tag excludes software R&D, which has its own concept, so software is added to it; the
#: inclusive tag is the fallback (a proxy: it may hold expensed acquired IPR&D).
RD_EXCL_IPR: Final = "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost"
RD_SOFTWARE: Final = "ResearchAndDevelopmentExpenseSoftwareExcludingAcquiredInProcessCost"
RD_INCL_IPR: Final = "ResearchAndDevelopmentExpense"
RD_WITNESSES: Final = (RD_EXCL_IPR, RD_INCL_IPR, RD_SOFTWARE)
#: XINT*: both fallbacks are proxies; absent, zero under the witness veto (adaptation d).
XINT_TAGS: Final = ("InterestExpense", "InterestAndDebtExpense", "InterestExpenseDebt")
XINT_WITNESSES: Final = (
    *XINT_TAGS,
    "InterestExpenseBorrowings",
    "InterestExpenseLongTermDebt",
    "InterestExpenseRelatedParty",
    "InterestExpenseOther",
    "InterestExpenseDeposits",
    "InterestCostsIncurred",
    "InterestPaidNet",
    "InterestPaid",
)
#: The one-sided bound on a summed XSGA*: above ``OperatingExpenses`` by more than this share of it (fixed by
#: construction).
OPEX: Final = ("OperatingExpenses",)
OPEX_TOLERANCE: Final = Decimal("0.005")
IB: Final = ("IncomeLossFromContinuingOperations",)
OANCF: Final = ("NetCashProvidedByUsedInOperatingActivities",)
#: Amendment 2b fallbacks: ``ni*`` = NI − XI − DO when IB is absent; ``ocf*`` = continuing + discontinued when
#: OANCF is absent.
NI: Final = "NetIncomeLoss"
XI: Final = "ExtraordinaryItemNetOfTax"
XIDO_PARENT: Final = "IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity"
XIDO_CONSOLIDATED: Final = "IncomeLossFromDiscontinuedOperationsNetOfTax"
DO_NCI: Final = "IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToNoncontrollingInterest"
OCF_CONTINUING: Final = "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
OCF_DISCONTINUED: Final = "CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations"
#: Witness-only concepts.
DO_DETAIL: Final = (
    "DiscontinuedOperationIncomeLossFromDiscontinuedOperationBeforeIncomeTax",
    "DiscontinuedOperationGainLossOnDisposalOfDiscontinuedOperationNetOfTax",
    "DiscontinuedOperationIncomeLossFromDiscontinuedOperationDuringPhaseOutPeriodNetOfTax",
)
DISCONTINUED_CASH: Final = "NetCashProvidedByUsedInDiscontinuedOperations"
#: A public non-zero fact on one of these overlapping the interval vetoes an imputed zero.
XI_WITNESSES: Final = (XI,)
DO_WITNESSES: Final = (XIDO_PARENT, XIDO_CONSOLIDATED, DO_NCI, *DO_DETAIL)
#: Conservative: discontinued operations in the interval leave a zero discontinued operating cash flow unsupported.
OCF_DO_WITNESSES: Final = (OCF_DISCONTINUED, DISCONTINUED_CASH, *DO_WITNESSES)
AT: Final = ("Assets",)
LT: Final = ("Liabilities",)
SEQ: Final = ("StockholdersEquity",)
TXDITC: Final = ("DeferredTaxLiabilitiesNoncurrent", "DeferredIncomeTaxLiabilitiesNet")
PSTK: Final = ("PreferredStockRedemptionAmount", "PreferredStockLiquidationPreferenceValue", "PreferredStockValue")
DEI_SHARES: Final = ("dei", "EntityCommonStockSharesOutstanding")
BALANCE_SHEET_SHARES: Final = ("us-gaap", "CommonStockSharesOutstanding")

ACCOUNTING_CHARACTERISTICS: Final = ("gp_at", "be_me", "ope_be", "ni_me", "ocf_me", "at_gr1")


class PanelError(RuntimeError):
    """A state the spec says refuses the run (configuration error, capture overrun, bad stamp)."""


class Kind(StrEnum):
    ANNUAL = "annual"
    QUARTERLY = "quarterly"


class TermStatus(StrEnum):
    """Value-read outcomes (§"Reading the bundle"). The first three block: no later branch, no zero."""

    AMBIGUOUS = "ambiguous"
    BLOCKED_BY_REJECTION = "blocked_by_rejection"
    FORM_MISMATCH = "form_mismatch"
    ABSENT = "absent"
    VALUE = "value"


_BLOCKING: Final = (TermStatus.AMBIGUOUS, TermStatus.BLOCKED_BY_REJECTION, TermStatus.FORM_MISMATCH)


class Exclusion(StrEnum):
    """Bundle-gate exclusions at M (prefix-read state machine)."""

    NO_FUNDAMENTALS = "no_fundamentals"
    INTEGRITY_EXCLUDED = "integrity_excluded"


class Missing(StrEnum):
    NO_PERIOD = "no_period"
    AGED_OUT = "aged_out"
    AMBIGUOUS = "ambiguous"
    BLOCKED_BY_REJECTION = "blocked_by_rejection"
    FORM_MISMATCH = "form_mismatch"
    NONPOSITIVE_DENOMINATOR = "nonpositive_denominator"
    NO_ME = "no_me"


class MeMissing(StrEnum):
    NO_SHARES = "no_shares"
    SHARES_AMBIGUOUS = "shares_ambiguous"
    SHARES_BLOCKED = "shares_blocked_by_rejection"
    SHARES_FORM_MISMATCH = "shares_form_mismatch"
    NONPOSITIVE_SHARES = "nonpositive_shares"
    NONPOSITIVE_PRICE = "nonpositive_price"
    # Amendment 2 share checks, in evaluation order.
    SHARES_BASIS_AMBIGUOUS = "shares_basis_ambiguous"
    SHARES_SCALE_CONFLICT = "shares_scale_conflict"
    SHARES_TURNOVER_IMPLAUSIBLE = "shares_turnover_implausible"
    SHARES_DISCONTINUITY = "shares_discontinuity"


class Check(StrEnum):
    """One Amendment 2 check's own outcome, stored on the row whatever the first failure was."""

    PASS = "pass"
    FAIL = "fail"
    UNTESTED = "untested"
    RECOVERED = "recovered"  # check 2 only: the side consistent with the reference was used


# --------------------------------------------------------------------------- calendar


def add_months(day: date, months: int) -> date:
    """Calendar months, clamping the day: 2016-12-31 + 4 = 2017-04-30."""
    index = day.year * 12 + day.month - 1 + months
    year, month = divmod(index, 12)
    return date(year, month + 1, min(day.day, calendar.monthrange(year, month + 1)[1]))


def month_end(day: date) -> date:
    return date(day.year, day.month, calendar.monthrange(day.year, day.month)[1])


def formation_months(first: date = STAGE_A_FIRST_FORMATION, last: date = STAGE_A_LAST_FORMATION) -> tuple[date, ...]:
    months: list[date] = []
    current = month_end(first)
    while current <= last:
        months.append(current)
        current = month_end(add_months(date(current.year, current.month, 1), 1))
    return tuple(months)


def decision_session(formation: date, sessions: Sequence[date]) -> date:
    """s(M): the last SPY session on or before M. ``sessions`` ascending."""
    eligible = [s for s in sessions if s <= formation]
    if not eligible:
        raise PanelError(f"no SPY session on or before {formation}")
    return eligible[-1]


def lag_eligible(period_end: date, formation: date) -> bool:
    return add_months(period_end, LAG_MONTHS) <= formation


def aged_out(period_end: date, formation: date) -> bool:
    return period_end < add_months(formation, -MAX_ACCOUNTING_AGE_MONTHS)


def inclusive_days(start: date, end: date) -> int:
    return (end - start).days + 1


# --------------------------------------------------------------------------- terms


@dataclass(frozen=True)
class FactUse:
    """One contributing read, for the panel row's provenance."""

    key: FactKey
    coefficient: int
    status: TermStatus
    accns: tuple[str, ...] = ()
    acceptance: str | None = None
    branch: str = ""
    value: str | None = None  # as filed (canonical decimal), before the coefficient

    def to_json(self) -> dict[str, Any]:
        return {
            "taxonomy": self.key.taxonomy,
            "concept": self.key.concept,
            "unit": self.key.unit,
            "start": self.key.start,
            "end": self.key.end,
            "coefficient": self.coefficient,
            "status": self.status.value,
            "accns": list(self.accns),
            "acceptance": self.acceptance,
            "branch": self.branch,
            "value": self.value,
        }


@dataclass(frozen=True)
class Term:
    status: TermStatus
    value: Decimal | None = None
    facts: tuple[FactUse, ...] = ()
    #: Quarter flows only: the quarter's first day, which chains the TTM walk.
    start: date | None = None
    branches: tuple[str, ...] = ()
    #: Reads a guard tested the term against, kept apart from the arithmetic ``facts`` (Amendment 2c's
    #: ``OperatingExpenses`` bound).
    guards: tuple[FactUse, ...] = ()


ABSENT: Final = Term(TermStatus.ABSENT)
ZERO: Final = Term(TermStatus.VALUE, Decimal(0), branches=("zero_if_missing",))


def combine(*parts: tuple[int, Term]) -> Term:
    """Σ coefficient × term. A blocking part wins over an absent one: a blocked newer key is never skipped."""
    facts = tuple(f for _, term in parts for f in term.facts)
    branches = tuple(b for _, term in parts for b in term.branches)
    guards = tuple(g for _, term in parts for g in term.guards)
    for status in _BLOCKING:
        if any(term.status is status for _, term in parts):
            return Term(status, facts=facts, branches=branches, guards=guards)
    if any(term.status is TermStatus.ABSENT for _, term in parts):
        return Term(TermStatus.ABSENT, facts=facts, branches=branches, guards=guards)
    total = sum((coefficient * term.value for coefficient, term in parts if term.value is not None), Decimal(0))
    return Term(TermStatus.VALUE, total, facts, branches=branches, guards=guards)


def first_available(branches: Iterable[tuple[str, Callable[[], Term]]]) -> Term:
    """Hierarchy walk: ``absent`` tries the next branch; anything else stops it."""
    for name, read in branches:
        term = read()
        if term.status is not TermStatus.ABSENT:
            return replace(term, branches=(name, *term.branches))
    return ABSENT


# --------------------------------------------------------------------------- the bundle at s(M)


class BundleReader(Protocol):
    def value_as_of(self, cik10: str, key: FactKey, decision: date) -> ValueRead: ...

    def public_events(self, cik10: str, taxonomy: str, concept: str, decision: date) -> PrefixRead: ...


class PrefixCache:
    """One CIK's prefix reads taken once at its latest decision, re-filtered per earlier decision.

    Exact, not an approximation: ``public_events`` keeps the rows whose acceptance NY date is strictly before the
    decision, so the rows public at an earlier decision are the rows public at the latest one passing the same
    test. It exists because ``public_events`` scans the whole shard on every call, which made 80 formations × 24
    concepts per CIK cost hours on the full population.
    """

    def __init__(self, bundle: BundleReader, cik10: str, last_decision: date) -> None:
        self.bundle = bundle
        self.cik10 = cik10
        self.last_decision = last_decision
        self._reads: dict[tuple[str, str], PrefixRead] = {}
        self._ny_dates: dict[str, date] = {}

    def at(self, taxonomy: str, concept: str, decision: date) -> PrefixRead:
        if decision > self.last_decision:
            raise PanelError(f"{self.cik10}: decision {decision} after the cached {self.last_decision}")
        full = self._reads.get((taxonomy, concept))
        if full is None:
            full = self._reads[(taxonomy, concept)] = self.bundle.public_events(
                self.cik10, taxonomy, concept, self.last_decision
            )
        if full.status is not ReadStatus.OK:
            return full

        def public(row: Mapping[str, Any]) -> bool:
            acceptance = row["acceptance"]
            ny = self._ny_dates.get(acceptance)
            if ny is None:
                ny = self._ny_dates[acceptance] = acceptance_ny_date(acceptance)
            return ny < decision

        return PrefixRead(
            ReadStatus.OK,
            tuple(filter(public, full.events)),
            tuple(filter(public, full.rejections)),
            tuple(filter(public, full.accessions)),
        )


class CikView:
    """One CIK's bundle at decision session s(M). Every read uses acceptance NY date strictly before s(M)."""

    def __init__(
        self, bundle: BundleReader, cik10: str, decision: date, *, prefixes: PrefixCache | None = None
    ) -> None:
        self.bundle = bundle
        self.cik10 = cik10
        self.decision = decision
        self._prefixes = prefixes
        self._prefix: dict[tuple[str, str], PrefixRead] = {}
        self._keys: dict[tuple[str, str, str], dict[tuple[str | None, str], str]] = {}
        assets = self.prefix("us-gaap", "Assets")
        self.exclusion = prefix_exclusion(assets.status)
        #: accn -> (acceptance, form) for every public admitted accession.
        self.filings: dict[str, tuple[str, str]] = {a["accn"]: (a["acceptance"], a["form"]) for a in assets.accessions}
        self.anchors = period_anchors(assets.events, {accn: form for accn, (_, form) in self.filings.items()})
        self.unanchored = sorted(
            accn
            for accn, (_, form) in self.filings.items()
            if form_family(form) in _ROLE_FAMILIES and accn not in self.anchors
        )
        annual = {end for accn, end in self.anchors.items() if form_family(self.filings[accn][1]) == ANNUAL_FAMILY}
        self.periods: dict[Kind, tuple[date, ...]] = {
            Kind.ANNUAL: tuple(sorted(annual, reverse=True)),
            Kind.QUARTERLY: tuple(sorted(set(self.anchors.values()), reverse=True)),
        }

    def prefix(self, taxonomy: str, concept: str) -> PrefixRead:
        cached = self._prefix.get((taxonomy, concept))
        if cached is None:
            if self._prefixes is None:
                cached = self.bundle.public_events(self.cik10, taxonomy, concept, self.decision)
            else:
                cached = self._prefixes.at(taxonomy, concept, self.decision)
            self._prefix[(taxonomy, concept)] = cached
            prefix_exclusion(cached.status)  # refuses AFTER_CAPTURE / CONCEPT_NOT_IN_POLICY on every concept
        return cached

    def keys(self, concept: str, *, taxonomy: str = "us-gaap", unit: str = USD) -> dict[tuple[str | None, str], str]:
        """Every public key of the concept -> its latest public acceptance (events and rejections alike)."""
        cached = self._keys.get((taxonomy, concept, unit))
        if cached is not None:
            return cached
        read = self.prefix(taxonomy, concept)
        latest: dict[tuple[str | None, str], str] = {}
        for row in (*read.events, *read.rejections):
            if row["unit"] == unit:
                key = (row["start"], row["end"])
                latest[key] = max(latest.get(key, ""), row["acceptance"])
        self._keys[(taxonomy, concept, unit)] = latest
        return latest

    def read(self, key: FactKey, *, coefficient: int = 1, branch: str = "") -> Term:
        """Value-read state machine (§"Reading the bundle")."""
        result = self.bundle.value_as_of(self.cik10, key, self.decision)
        if result.status is ReadStatus.ABSENT:
            return ABSENT
        if result.status not in (ReadStatus.VALUE, ReadStatus.AMBIGUOUS, ReadStatus.BLOCKED_BY_REJECTION):
            raise PanelError(f"{self.cik10} {key}: value read {result.status.value} refuses the run")
        status = TermStatus(result.status.value)
        if status is TermStatus.VALUE and not all(self.role_accession(accn) for accn in result.accns):
            status = TermStatus.FORM_MISMATCH
        filed = result.values[0] if status is TermStatus.VALUE else None
        use = FactUse(key, coefficient, status, result.accns, result.acceptance, branch, filed)
        value = None if filed is None else Decimal(filed)
        return Term(status, value, (use,))

    def role_accession(self, accn: str) -> bool:
        """A 10-K/10-Q-family accession with a period anchor (unanchored accessions' facts are not used)."""
        filing = self.filings.get(accn)
        return filing is not None and form_family(filing[1]) in _ROLE_FAMILIES and accn in self.anchors


def prefix_exclusion(status: ReadStatus) -> Exclusion | None:
    """Prefix-read state machine (§"Reading the bundle")."""
    match status:
        case ReadStatus.OK:
            return None
        case ReadStatus.CIK_NOT_IN_BUNDLE:
            return Exclusion.NO_FUNDAMENTALS
        case ReadStatus.CIK_INTEGRITY_EXCLUDED:
            return Exclusion.INTEGRITY_EXCLUDED
        case _:
            raise PanelError(f"prefix read {status.value} refuses the run")


def period_anchors(assets_events: Iterable[Mapping[str, Any]], forms: Mapping[str, str]) -> dict[str, date]:
    """accn -> its period end: the latest ``Assets`` instant it reports (10-K/10-Q families only)."""
    anchors: dict[str, date] = {}
    for row in assets_events:
        form = forms.get(row["accn"])
        if row["start"] is not None or form is None or form_family(form) not in _ROLE_FAMILIES:
            continue
        end = date.fromisoformat(row["end"])
        if row["accn"] not in anchors or end > anchors[row["accn"]]:
            anchors[row["accn"]] = end
    return anchors


# --------------------------------------------------------------------------- items


def instant(view: CikView, concepts: Sequence[str], end: date, *, coefficient: int = 1) -> Term:
    iso = end.isoformat()
    return first_available(
        (c, lambda c=c: view.read(FactKey("us-gaap", c, USD, None, iso), coefficient=coefficient, branch=c))
        for c in concepts
    )


def _ranked_read(view: CikView, concept: str, candidates: Mapping[tuple[str | None, str], str], branch: str) -> Term:
    """Read the candidate key with the latest public item; different keys tied at that acceptance are ambiguous."""
    if not candidates:
        return ABSENT
    top = max(candidates.values())
    best = [key for key, acceptance in candidates.items() if acceptance == top]
    if len(best) > 1:
        uses = tuple(
            FactUse(FactKey("us-gaap", concept, USD, s, e), 1, TermStatus.AMBIGUOUS, acceptance=top, branch=branch)
            for s, e in sorted(best, key=lambda k: (k[0] or "", k[1]))
        )
        return Term(TermStatus.AMBIGUOUS, facts=uses)
    start, end = best[0]
    return view.read(FactKey("us-gaap", concept, USD, start, end), branch=branch)


def annual_flow(view: CikView, concept: str, end: date) -> Term:
    """350–380-day facts ending at an annual anchor."""
    iso = end.isoformat()
    candidates = {
        key: acc
        for key, acc in view.keys(concept).items()
        if key[1] == iso
        and key[0] is not None
        and _within(inclusive_days(date.fromisoformat(key[0]), end), ANNUAL_DAYS)
    }
    return _ranked_read(view, concept, candidates, "annual")


def _within(days: int, bounds: tuple[int, int]) -> bool:
    return bounds[0] <= days <= bounds[1]


def quarter_flow(view: CikView, concept: str, end: date, *, residual: bool = True) -> Term:
    """One quarter of one concept ending at ``end``: direct, else YTD difference, else Q4 residual."""
    keys = view.keys(concept)
    iso = end.isoformat()
    ending = {k: a for k, a in keys.items() if k[1] == iso and k[0] is not None}

    def direct() -> Term:
        candidates = {
            k: a for k, a in ending.items() if _within(inclusive_days(date.fromisoformat(str(k[0])), end), QUARTER_DAYS)
        }
        term = _ranked_read(view, concept, candidates, "direct")
        if term.status is not TermStatus.VALUE:
            return term
        start = date.fromisoformat(str(term.facts[0].key.start))
        return Term(term.status, term.value, term.facts, start, term.branches)

    def ytd() -> Term:
        # (long key, short key) with the same start; the short one ends one quarter before ``end``.
        pairs: dict[tuple[str, str], str] = {}
        for (start, _), acc in ending.items():
            if inclusive_days(date.fromisoformat(str(start)), end) <= QUARTER_DAYS[1]:
                continue
            for (s2, e2), acc2 in keys.items():
                if s2 == start and e2 < iso and _within((end - date.fromisoformat(e2)).days, QUARTER_DAYS):
                    pairs[(str(start), e2)] = max(acc, acc2)
        if not pairs:
            return ABSENT
        top = max(pairs.values())
        best = [p for p, a in pairs.items() if a == top]
        if len(best) > 1:
            return Term(TermStatus.AMBIGUOUS)
        start, short_end = best[0]
        term = combine(
            (1, view.read(FactKey("us-gaap", concept, USD, start, iso), branch="ytd_long")),
            (-1, view.read(FactKey("us-gaap", concept, USD, start, short_end), coefficient=-1, branch="ytd_short")),
        )
        return Term(term.status, term.value, term.facts, date.fromisoformat(short_end) + timedelta(days=1))

    def q4_residual() -> Term:
        if end not in view.periods[Kind.ANNUAL]:
            return ABSENT
        annual = annual_flow(view, concept, end)
        if annual.status is not TermStatus.VALUE:
            return annual
        fiscal_start = date.fromisoformat(str(annual.facts[0].key.start))
        quarters = _chain(view, lambda e: quarter_flow(view, concept, e, residual=False), end, 3, skip_last=True)
        if quarters.status is not TermStatus.VALUE:
            return quarters
        if quarters.start != fiscal_start:
            return ABSENT
        term = combine((1, annual), (-1, _negated(quarters)))
        q3_end = max(date.fromisoformat(f.key.end) for f in quarters.facts)
        return Term(term.status, term.value, term.facts, q3_end + timedelta(days=1))

    branches: list[tuple[str, Callable[[], Term]]] = [("direct", direct), ("ytd_difference", ytd)]
    if residual:
        branches.append(("q4_residual", q4_residual))
    return first_available(branches)


def _negated(term: Term) -> Term:
    facts = tuple(
        FactUse(f.key, -f.coefficient, f.status, f.accns, f.acceptance, f.branch, f.value) for f in term.facts
    )
    return replace(term, facts=facts)


def _chain(view: CikView, quarter: Callable[[date], Term], end: date, count: int, *, skip_last: bool = False) -> Term:
    """``count`` consecutive quarters ending at ``end`` (or at the quarter before it when ``skip_last``).

    Each quarter's start is within ``QUARTER_GAP_DAYS`` after the prior quarter's end, and every end is an
    anchored quarterly period. The returned ``start`` is the first quarter's start.
    """
    quarterly = view.periods[Kind.QUARTERLY]
    current: date | None = end
    if skip_last:
        current = _prior_quarter_end(
            quarterly, end - timedelta(days=QUARTER_DAYS[0]), end - timedelta(days=QUARTER_DAYS[1])
        )
    terms: list[Term] = []
    while len(terms) < count:
        if current is None:
            return ABSENT
        term = quarter(current)
        if term.status is not TermStatus.VALUE:
            return term
        terms.append(term)
        assert term.start is not None
        current = _prior_quarter_end(
            quarterly, term.start - timedelta(days=1), term.start - timedelta(days=QUARTER_GAP_DAYS)
        )
    total = combine(*((1, t) for t in terms))
    return replace(total, start=terms[-1].start)


def _prior_quarter_end(quarterly: Sequence[date], latest: date, earliest: date) -> date | None:
    return next((e for e in quarterly if earliest <= e <= latest), None)


def flow(view: CikView, concepts: Sequence[str], end: date, kind: Kind) -> Term:
    """A flow item over the period: annual fact, or TTM of four chained quarters. Concepts fall back per quarter."""
    if kind is Kind.ANNUAL:
        return first_available((c, lambda c=c: annual_flow(view, c, end)) for c in concepts)

    def quarter(e: date) -> Term:
        return first_available((c, lambda c=c: quarter_flow(view, c, e)) for c in concepts)

    return _chain(view, quarter, end, 4)


# --------------------------------------------------------------------------- JKP Table 5 items


def be(view: CikView, end: date) -> Term:
    """``seq*`` + ``txditc*`` − ``pstk*``; the last two are zero if missing (CEQ + PSTK branch dropped)."""
    seq = first_available(
        (
            ("SEQ", lambda: instant(view, SEQ, end)),
            ("AT-LT", lambda: combine((1, instant(view, AT, end)), (-1, instant(view, LT, end, coefficient=-1)))),
        )
    )
    txditc = first_available((("TXDITC", lambda: instant(view, TXDITC, end)), ("zero", lambda: ZERO)))
    pstk = first_available((("PSTK", lambda: instant(view, PSTK, end, coefficient=-1)), ("zero", lambda: ZERO)))
    return combine((1, seq), (1, txditc), (-1, pstk))


# --------------------------------------------------------------------------- Amendment 2b: ni* and ocf* fallbacks

Reader = Callable[[CikView, str, date], Term]

#: Per fallback characteristic: the primary branch label, the fallback branch label and its companions' names.
FALLBACK_LABELS: Final = {
    "ni_me": ("ib", "ni_minus_xido", ("xi", "do")),
    "ocf_me": ("oancf", "continuing_plus_discontinued", ("ocf_disc",)),
}
#: Prefix of the label an absent term carries when a witness refused its companion's zero. The census reads it.
VETO: Final = "veto_"
#: ``do_read``'s branch labels: the parent tag, consolidated minus noncontrolling, the consolidated proxy.
DO_BRANCHES: Final = ("do_parent", "do_consolidated_minus_nci", "do_consolidated_proxy")


def _interval_start(term: Term, end: date, kind: Kind) -> date | None:
    """The interval start of a VALUE term ending at ``end``."""
    if term.status is not TermStatus.VALUE:
        return None
    if kind is Kind.QUARTERLY:
        return term.start
    starts = {f.key.start for f in term.facts if f.key.start is not None and f.key.end == end.isoformat()}
    return date.fromisoformat(min(starts)) if len(starts) == 1 else None


def witnessed(view: CikView, concepts: Sequence[str], start: date, end: date) -> list[str]:
    """Concepts with a public fact overlapping [start, end] whose CURRENT value is non-zero.

    ``view.prefix`` holds only rows accepted before s(M), so a later filing cannot veto an earlier formation. Per
    key, only the rows at the key's latest public acceptance count, so a non-zero fact later corrected to zero does
    not veto. A fact filed without a start on these duration concepts is dated at its end, and overlaps when that
    date is inside the interval.
    """
    hits: list[str] = []
    for concept in concepts:
        latest: dict[tuple[str, str], tuple[str, set[str]]] = {}
        for row in view.prefix("us-gaap", concept).events:
            if row["unit"] != USD:
                continue
            key = (row["start"] or row["end"], row["end"])
            held = latest.get(key)
            if held is None or row["acceptance"] > held[0]:
                latest[key] = (row["acceptance"], {row["value"]})
            elif row["acceptance"] == held[0]:
                held[1].add(row["value"])
        for (s, e), (_, values) in latest.items():
            if date.fromisoformat(s) <= end and date.fromisoformat(e) >= start and any(Decimal(v) != 0 for v in values):
                hits.append(concept)
                break
    return hits


def _labelled(term: Term, *labels: str) -> Term:
    return replace(term, branches=(*labels, *term.branches))


def companion(
    view: CikView,
    read: Callable[[], Term],
    end: date,
    kind: Kind,
    start: date | None,
    *,
    name: str,
    witnesses: Sequence[str],
) -> Term:
    """A deducted or added term. Only ``absent`` may become zero; blocking states are returned for ``combine``."""
    term = read()
    if term.status is TermStatus.VALUE:
        if start is not None and _interval_start(term, end, kind) != start:
            return ABSENT
        return _labelled(term, f"{name}_filed_zero" if term.value == 0 else f"{name}_filed")
    if term.status is not TermStatus.ABSENT:
        return term
    # No base interval to test a zero against: the base is blocked or absent (``combine`` returns its status either
    # way), or an annual base with no single start, which stays missing.
    if start is None:
        return ABSENT
    if hits := witnessed(view, witnesses, start, end):
        return Term(TermStatus.ABSENT, branches=(f"{VETO}{name}:{start}:{end}:{'+'.join(hits)}",))
    return Term(TermStatus.VALUE, Decimal(0), branches=(f"zero_{name}:{start}:{end}",))


def do_read(view: CikView, reader: Reader, end: date) -> Term:
    """DO: the parent tag; else consolidated minus the noncontrolling share; else consolidated (proxy)."""
    parent = reader(view, XIDO_PARENT, end)
    if parent.status is not TermStatus.ABSENT:
        return _labelled(parent, DO_BRANCHES[0])
    consolidated = reader(view, XIDO_CONSOLIDATED, end)
    if consolidated.status is TermStatus.ABSENT:
        return ABSENT
    nci = reader(view, DO_NCI, end)
    if nci.status is TermStatus.ABSENT:
        return _labelled(consolidated, DO_BRANCHES[2])
    # Quarter terms carry their start, so a different one is a different interval (annual terms carry none; their
    # fact keys are checked by ``companion``). Absent, either non-zero term vetoes the zero as a witness.
    both = consolidated.status is TermStatus.VALUE and nci.status is TermStatus.VALUE
    if both and consolidated.start != nci.start:
        return ABSENT
    term = combine((1, consolidated), (-1, _negated(nci)))
    return Term(term.status, term.value, term.facts, consolidated.start, (DO_BRANCHES[1], *term.branches))


def unit_term(view: CikView, name: str, end: date, kind: Kind) -> Term:
    """One period of ``ni*`` or ``ocf*``: an annual period, or one quarter before the TTM chain."""
    reader: Reader = annual_flow if kind is Kind.ANNUAL else quarter_flow
    primary_label, label, _ = FALLBACK_LABELS[name]
    if name == "ni_me":
        primary = reader(view, IB[0], end)
        if primary.status is not TermStatus.ABSENT:
            return _labelled(primary, primary_label)
        base = reader(view, NI, end)
        start = _interval_start(base, end, kind)
        xi = companion(view, lambda: reader(view, XI, end), end, kind, start, name="xi", witnesses=XI_WITNESSES)
        do = companion(view, lambda: do_read(view, reader, end), end, kind, start, name="do", witnesses=DO_WITNESSES)
        term = combine((1, base), (-1, _negated(xi)), (-1, _negated(do)))
    else:
        primary = reader(view, OANCF[0], end)
        if primary.status is not TermStatus.ABSENT:
            return _labelled(primary, primary_label)
        base = reader(view, OCF_CONTINUING, end)
        start = _interval_start(base, end, kind)
        disc = companion(
            view,
            lambda: reader(view, OCF_DISCONTINUED, end),
            end,
            kind,
            start,
            name="ocf_disc",
            witnesses=OCF_DO_WITNESSES,
        )
        term = combine((1, base), (1, disc))
    out_start = base.start if kind is Kind.QUARTERLY else None
    return Term(term.status, term.value, term.facts, out_start, (label, *term.branches))


def fallback_flow(view: CikView, name: str, end: date, kind: Kind) -> Term:
    """``ni*`` or ``ocf*`` over the period. The branch is chosen per period: per quarter before the TTM chain."""
    if kind is Kind.ANNUAL:
        return unit_term(view, name, end, kind)
    return _chain(view, lambda e: unit_term(view, name, e, Kind.QUARTERLY), end, 4)


# --------------------------------------------------------------------------- Amendment 2c: ope* and gp*


class Period:
    """One annual period or one quarter of one CIK, with the start of its base interval (the base term of the
    ``ebitda*`` or ``gp*`` branch); ``None`` while the base is not a value."""

    def __init__(self, view: CikView, end: date, kind: Kind, start: date | None) -> None:
        self.view, self.end, self.kind, self.start = view, end, kind, start

    def read(self, concepts: Sequence[str]) -> Term:
        reader: Reader = annual_flow if self.kind is Kind.ANNUAL else quarter_flow
        return first_available((c, lambda c=c: reader(self.view, c, self.end)) for c in concepts)

    def aligned(self, term: Term) -> Term:
        """A value over another interval than the base's is ``absent``, as ``companion`` treats its terms."""
        if term.status is not TermStatus.VALUE or self.start is None:
            return term
        if _interval_start(term, self.end, self.kind) != self.start:
            return Term(TermStatus.ABSENT, branches=("interval_mismatch",))
        return term

    def companion(self, read: Callable[[], Term], name: str, witnesses: Sequence[str]) -> Term:
        return companion(self.view, read, self.end, self.kind, self.start, name=name, witnesses=witnesses)


def rd_read(p: Period) -> Term:
    """RD*: excluding-IPR&D + software (zero if absent, under its own witness); else the inclusive tag (proxy)."""
    excl = p.read((RD_EXCL_IPR,))
    if excl.status is TermStatus.ABSENT:
        return _labelled(p.read((RD_INCL_IPR,)), "rd_incl_ipr")
    if excl.status is not TermStatus.VALUE:
        return excl
    software = p.companion(lambda: p.read((RD_SOFTWARE,)), "rd_software", (RD_SOFTWARE,))
    total = combine((1, excl), (1, software))
    return replace(total, start=excl.start, branches=("rd_excl_ipr", *total.branches))


def bounded(p: Period, total: Term, *, refuse: Term) -> Term:
    """A sum the rule makes, tested against the filer's ``OperatingExpenses`` over the same interval.

    The ``OperatingExpenses`` reads go to ``guards``, never to the arithmetic ``facts``. A blocked one blocks the
    sum (a guard in doubt is not a pass); an absent or other-interval one leaves it unchecked. More than
    ``OPEX_TOLERANCE`` above it, ``refuse`` is returned. The bound is one-sided: a passing sum is not proved right.
    """
    if total.status is not TermStatus.VALUE:
        return total
    opex = p.aligned(p.read(OPEX))
    if opex.status in _BLOCKING:
        return Term(opex.status, facts=total.facts, branches=("opex_bound_blocked", *total.branches), guards=opex.facts)
    if opex.status is not TermStatus.VALUE:
        return _labelled(total, "opex_unchecked")
    assert total.value is not None and opex.value is not None
    if total.value > opex.value + abs(opex.value) * OPEX_TOLERANCE:
        return replace(refuse, guards=(*refuse.guards, *opex.facts))
    return replace(total, branches=("opex_checked", *total.branches), guards=(*total.guards, *opex.facts))


def xsga_term(p: Period) -> Term:
    """XSGA* = SG&A + RD*; else, with G&A filed, G&A + selling + RD*. Only a sum with a non-zero RD* added to SG&A,
    and every sum of parts, is bounded: a filed SG&A is taken as filed."""
    sga = p.aligned(p.read(XSGA))
    rd = p.companion(lambda: rd_read(p), "rd", RD_WITNESSES)
    if sga.status is not TermStatus.ABSENT:
        total = combine((1, sga), (1, rd))
        if rd.status is not TermStatus.VALUE or not rd.value:
            return total
        return bounded(p, total, refuse=_labelled(sga, "rd_excluded_opex_bound"))
    ga = p.aligned(p.read(GA))
    if ga.status is TermStatus.ABSENT:
        return combine((1, ga), (1, rd))  # a blocked RD* still blocks
    sell = p.companion(lambda: p.read(SELL), "sell", SELL_WITNESSES)
    total = _labelled(combine((1, ga), (1, sell), (1, rd)), "xsga_parts")
    refusal = Term(TermStatus.ABSENT, branches=(f"{VETO}opex_bound:{p.start}:{p.end}:{OPEX[0]}",))
    return bounded(p, total, refuse=refusal)


def cogs_term(p: Period) -> Term:
    """COGS*: a total tag, else goods + services with one of the two filed and the other a witnessed zero
    (adaptation e). With no cost-of-sales tag at all, COGS* is ``absent`` (a zero was measured and rejected)."""
    total = p.aligned(p.read(COGS_TOTAL))
    if total.status is not TermStatus.ABSENT:
        return total
    goods, services = p.aligned(p.read(COGS_GOODS)), p.aligned(p.read(COGS_SERVICES))
    if goods.status is TermStatus.ABSENT and services.status is TermStatus.ABSENT:
        return combine((1, goods), (1, services))
    parts = (
        p.companion(lambda: goods, "cogs_goods", COGS_GOODS),
        p.companion(lambda: services, "cogs_services", COGS_SERVICES),
    )
    return _labelled(combine(*((1, t) for t in parts)), "cogs_goods_plus_services")


def xint_term(p: Period) -> Term:
    return p.companion(lambda: p.read(XINT_TAGS), "xint", XINT_WITNESSES)


def ope_unit(view: CikView, end: date, kind: Kind) -> Term:
    """One annual period or one quarter of ``ope*`` = ``ebitda*`` − XINT*. ``ebitda*`` = ``sale*`` − COGS* − XSGA*,
    else GP − XSGA*; every part is aligned to the chosen branch's base interval. When both branches are ``absent``,
    the refusals' labels and guard reads are kept, so a vetoed period is recorded as Amendment 2b's are.

    The parts are read even when the base is not a value, because a blocked part blocks an absent base (``combine``).
    A refusal or bound there tested no base interval, so its label is dropped, and so are its guards unless the term
    is blocked; ``companion`` makes no label without an interval either."""

    def deducted(base: Term, label: str, costs: Callable[[Period], Term]) -> Term:
        p = Period(view, end, kind, _interval_start(base, end, kind))
        term = combine((1, base), (-1, _negated(costs(p))), (-1, _negated(xint_term(p))))
        if p.start is None:  # a blocked term keeps its guards: a blocked OperatingExpenses read may be why
            term = replace(
                term,
                branches=tuple(b for b in term.branches if not b.startswith(VETO)),
                guards=() if term.status is TermStatus.ABSENT else term.guards,
            )
        return replace(term, start=base.start, branches=(label, *term.branches))

    whole = Period(view, end, kind, None)
    sale = deducted(
        whole.read(SALE),
        "sale_minus_opex",
        lambda p: _labelled(combine((1, cogs_term(p)), (1, xsga_term(p))), "cogs_plus_xsga"),
    )
    if sale.status is not TermStatus.ABSENT:
        return sale
    gp_branch = deducted(whole.read(GP), "gp_minus_xsga", xsga_term)
    if gp_branch.status is not TermStatus.ABSENT:
        return gp_branch
    return Term(
        TermStatus.ABSENT,
        branches=tuple(b for t in (sale, gp_branch) for b in t.branches if b.startswith(VETO)),
        guards=(*sale.guards, *gp_branch.guards),
    )


def gp_unit(view: CikView, end: date, kind: Kind) -> Term:
    """One annual period or one quarter of ``gp*`` = GP, else ``sale*`` − COGS*."""
    whole = Period(view, end, kind, None)
    gp = whole.read(GP)
    if gp.status is not TermStatus.ABSENT:
        return _labelled(gp, "GP")
    sale = whole.read(SALE)
    p = Period(view, end, kind, _interval_start(sale, end, kind))
    term = combine((1, sale), (-1, _negated(cogs_term(p))))
    return replace(term, start=sale.start, branches=("sale-cogs", *term.branches))


def period_flow(unit: Callable[[CikView, date, Kind], Term], view: CikView, end: date, kind: Kind) -> Term:
    """A formula read per period: the annual period, or each quarter before the TTM chain."""
    if kind is Kind.ANNUAL:
        return unit(view, end, kind)
    return _chain(view, lambda e: unit(view, e, Kind.QUARTERLY), end, 4)


@dataclass(frozen=True)
class Ratio:
    """A characteristic's numerator and denominator at one period, before eligibility."""

    numerator: Term
    denominator: Term | None  # None: the denominator is ME


def compute_ratio(name: str, view: CikView, end: date, kind: Kind) -> Ratio:
    match name:
        case "gp_at":
            return Ratio(period_flow(gp_unit, view, end, kind), instant(view, AT, end))
        case "be_me":
            return Ratio(be(view, end), None)
        case "ope_be":
            return Ratio(period_flow(ope_unit, view, end, kind), be(view, end))
        case "ni_me" | "ocf_me":
            return Ratio(fallback_flow(view, name, end, kind), None)
        case "at_gr1":
            prior = _prior_year_period(view.periods[kind], end)
            prior_at = ABSENT if prior is None else instant(view, AT, prior)
            return Ratio(instant(view, AT, end), prior_at)
        case _:
            raise PanelError(f"unknown characteristic {name!r}")


def _prior_year_period(periods: Sequence[date], end: date) -> date | None:
    target = add_months(end, -12)
    window = [p for p in periods if abs((p - target).days) <= YOY_TOLERANCE_DAYS]
    return min(window, key=lambda p: (abs((p - target).days), -p.toordinal()), default=None)


# --------------------------------------------------------------------------- choosing the period


@dataclass(frozen=True)
class Characteristic:
    name: str
    value: float | None
    missing: Missing | None
    period_end: date | None = None
    kind: Kind | None = None
    facts: tuple[FactUse, ...] = ()
    branches: tuple[str, ...] = ()
    #: Lag-eligible periods evaluated before one qualified (> 1: a later period's components were not public).
    candidates_tested: int = 0
    #: Every companion zero a witness refused, in evaluation order over all tested periods (a vetoed period is
    #: ``absent``, so the walk moves past it). A quarter shared by two tested TTM periods is refused once in each.
    vetoes: tuple[str, ...] = ()
    #: Every guard read (``Term.guards``) over all tested periods, in evaluation order, as ``vetoes``: "with every
    #: tested sum" (Amendment 2c), so a refused or losing period's ``OperatingExpenses`` read stays on the row.
    guards: tuple[FactUse, ...] = ()


def _status_missing(status: TermStatus) -> Missing:
    return {
        TermStatus.AMBIGUOUS: Missing.AMBIGUOUS,
        TermStatus.BLOCKED_BY_REJECTION: Missing.BLOCKED_BY_REJECTION,
        TermStatus.FORM_MISMATCH: Missing.FORM_MISMATCH,
    }[status]


def characteristic(name: str, view: CikView, formation: date, me: Decimal | None) -> Characteristic:
    """§"Choosing the period": per kind, the latest lag-eligible period whose components are public; the later
    of the two (annual on a tie); then the maximum age; then the read statuses; then eligibility."""
    chosen: list[tuple[date, Kind, Ratio]] = []
    tested = 0
    vetoes: list[str] = []
    guards: list[FactUse] = []
    for kind in (Kind.ANNUAL, Kind.QUARTERLY):
        for end in view.periods[kind]:
            if not lag_eligible(end, formation):
                continue
            tested += 1
            ratio = compute_ratio(name, view, end, kind)
            parts = [ratio.numerator] + ([] if ratio.denominator is None else [ratio.denominator])
            vetoes += (b for p in parts for b in p.branches if b.startswith(VETO))
            guards += (g for p in parts for g in p.guards)
            if combine(*((1, p) for p in parts)).status is not TermStatus.ABSENT:
                chosen.append((end, kind, ratio))
                break
    if not chosen:
        return Characteristic(
            name, None, Missing.NO_PERIOD, candidates_tested=tested, vetoes=tuple(vetoes), guards=tuple(guards)
        )
    end, kind, ratio = max(chosen, key=lambda c: (c[0], c[1] is Kind.ANNUAL))
    parts = [ratio.numerator] + ([] if ratio.denominator is None else [ratio.denominator])
    facts = tuple(f for p in parts for f in p.facts)
    branches = tuple(b for p in parts for b in p.branches)

    def missing(reason: Missing) -> Characteristic:
        return Characteristic(name, None, reason, end, kind, facts, branches, tested, tuple(vetoes), tuple(guards))

    if aged_out(end, formation):
        return missing(Missing.AGED_OUT)
    status = combine(*((1, p) for p in parts)).status
    if status is not TermStatus.VALUE:
        return missing(_status_missing(status))
    numerator = ratio.numerator.value
    denominator = me if ratio.denominator is None else ratio.denominator.value
    assert numerator is not None
    if denominator is None:
        return missing(Missing.NO_ME)
    if denominator <= 0:
        return missing(Missing.NONPOSITIVE_DENOMINATOR)
    value = numerator / denominator - (1 if name == "at_gr1" else 0)
    return Characteristic(name, float(value), None, end, kind, facts, branches, tested, tuple(vetoes), tuple(guards))


# --------------------------------------------------------------------------- market equity


@dataclass(frozen=True)
class SplitStamp:
    day: date
    factor: Decimal


@dataclass(frozen=True)
class MarketEquity:
    value: Decimal | None
    missing: MeMissing | None
    shares_scope: str | None = None  # "cover" | "balance_sheet" | "dqc_recovered:<side>"
    shares: Decimal | None = None
    basis: date | None = None
    split_product: Decimal | None = None
    facts: tuple[FactUse, ...] = ()
    #: Amendment 2: ME before the checks (contaminated when a check removed it), each check's own outcome, and
    #: whether this count may become a check-4 reference: check 3 passed and either check 2 passed or, with check 2
    #: untested, the count agrees with the run's previous admitted count from another accession (Amendment 2.1).
    raw_value: Decimal | None = None
    checks: Mapping[str, Check] = field(default_factory=dict)
    verified: bool = False


@dataclass(frozen=True)
class ShareReference:
    """A split-adjusted share count admitted at an earlier formation of the series: check 4's reference when
    verified, or Amendment 2.1's consensus partner. ``accns`` and ``counted`` (the fact's own date) say which
    filing's count it is."""

    formation: date
    session: date
    shares: Decimal
    cik10: str
    accns: tuple[str, ...] = ()
    counted: str | None = None


def _share_candidates(
    view: CikView, taxonomy: str, concept: str, decision: date, *, cover: bool
) -> list[tuple[date, str, str]]:
    """(context date, acceptance, accn) per public item; cover counts on or after the filing's period end,
    balance-sheet counts at exactly it. A context after s(M) is a count as of no knowable date: a rejected
    row dated in the far future would otherwise outrank every later valid count for good."""
    read = view.prefix(taxonomy, concept)
    floor = add_months(decision, -MAX_SHARES_AGE_MONTHS)
    out: list[tuple[date, str, str]] = []
    for row in (*read.events, *read.rejections):
        if row["unit"] != SHARES or row["start"] is not None or not view.role_accession(row["accn"]):
            continue
        context, anchor = date.fromisoformat(row["end"]), view.anchors[row["accn"]]
        if (context >= anchor if cover else context == anchor) and floor <= context <= decision:
            out.append((context, row["acceptance"], row["accn"]))
    return out


def _within_tolerance(ratio: Decimal) -> bool:
    return 1 / SCALE_TOLERANCE <= ratio <= SCALE_TOLERANCE


def _accession_balance_sheet(view: CikView, accn: str) -> FactUse | None:
    """The accession's own ``CommonStockSharesOutstanding`` at its period anchor, if it reported one positive value.
    Read from that accession's rows, not by key: a later filing's comparative for the same date is not this filing.
    A public rejection of that key blocks the comparator, as it blocks ``CikView.read``."""
    anchor = view.anchors.get(accn)
    if anchor is None:
        return None
    iso = anchor.isoformat()
    read = view.prefix(*BALANCE_SHEET_SHARES)

    def at_anchor(row: Mapping[str, Any]) -> bool:
        return row["unit"] == SHARES and row["start"] is None and row["end"] == iso

    if any(at_anchor(row) for row in read.rejections):
        return None
    values = {row["value"] for row in read.events if at_anchor(row) and row["accn"] == accn}
    if len(values) != 1:
        return None
    (value,) = values
    if Decimal(value) <= 0:
        return None
    key = FactKey(*BALANCE_SHEET_SHARES, SHARES, None, iso)
    return FactUse(key, 1, TermStatus.VALUE, (accn,), view.filings[accn][0], "dqc_comparator", value)


def usable_reference(reference: ShareReference | None, formation: date, cik10: str) -> ShareReference | None:
    """Check 4's reference, or the previous admitted count, applies within the share-age bound on the same CIK."""
    if reference is None or reference.cik10 != cik10:
        return None
    return None if add_months(reference.formation, MAX_SHARES_AGE_MONTHS) < formation else reference


def _consistent(shares: Decimal, reference: ShareReference, splits: Sequence[SplitStamp], decision: date) -> bool:
    expected = reference.shares * split_product(splits, reference.session, decision)
    return expected > 0 and _within_tolerance(shares / expected)


def market_equity(
    view: CikView,
    close: Decimal | None,
    splits: Sequence[SplitStamp],
    *,
    dollar_volume: float | None = None,
    reference: ShareReference | None = None,
    partner: ShareReference | None = None,
) -> MarketEquity:
    """§"Market equity": the filing's own cover count, else the balance-sheet count; never a fallback when blocked.
    Then Amendment 2's four checks, in order; the first failure is the missing reason. ``reference`` (the last
    verified count) and ``partner`` (Amendment 2.1: the last admitted count that passed check 3 and was not
    recovered) must already be ``usable_reference``-filtered by the caller."""
    decision = view.decision
    for taxonomy, concept, scope in (
        ("dei", DEI_SHARES[1], "cover"),
        ("us-gaap", BALANCE_SHEET_SHARES[1], "balance_sheet"),
    ):
        candidates = _share_candidates(view, taxonomy, concept, decision, cover=scope == "cover")
        if not candidates:
            continue
        context = max(candidates)[0]
        term = view.read(FactKey(taxonomy, concept, SHARES, None, context.isoformat()), branch=scope)
        if term.status is TermStatus.ABSENT:  # cannot happen: the candidate is a public item of this key
            raise PanelError(f"{view.cik10}: share key {context} listed public but read absent")
        if term.status is not TermStatus.VALUE:
            reason = {
                TermStatus.AMBIGUOUS: MeMissing.SHARES_AMBIGUOUS,
                TermStatus.BLOCKED_BY_REJECTION: MeMissing.SHARES_BLOCKED,
                TermStatus.FORM_MISMATCH: MeMissing.SHARES_FORM_MISMATCH,
            }[term.status]
            return MarketEquity(None, reason, scope, facts=term.facts)
        assert term.value is not None
        use = term.facts[0]
        acceptance = acceptance_ny_date(str(use.acceptance))
        basis = context if scope == "cover" else acceptance
        product = split_product(splits, basis, decision)
        shares = term.value * product
        common = {
            "shares_scope": scope,
            "shares": shares,
            "basis": basis,
            "split_product": product,
            "facts": term.facts,
        }
        if shares <= 0:
            return MarketEquity(None, MeMissing.NONPOSITIVE_SHARES, **common)
        if close is None or not close.is_finite() or close <= 0:
            return MarketEquity(None, MeMissing.NONPOSITIVE_PRICE, **common)
        return _checked(
            view, close, splits, common, term.value, use.accns, acceptance, dollar_volume, reference, partner
        )
    return MarketEquity(None, MeMissing.NO_SHARES)


def _checked(
    view: CikView,
    close: Decimal,
    splits: Sequence[SplitStamp],
    common: dict[str, Any],
    filed: Decimal,
    accns: Sequence[str],
    acceptance: date,
    dollar_volume: float | None,
    reference: ShareReference | None,
    partner: ShareReference | None,
) -> MarketEquity:
    """Amendment 2 checks 1-4 on a computable ME. Every check is evaluated; the first failure is the reason."""
    decision = view.decision
    scope: str = common["shares_scope"]
    basis: date = common["basis"]
    shares: Decimal = common["shares"]
    raw = shares * close
    checks: dict[str, Check] = {}

    # 1. A split between the cover count's date and its filing, or an applied product beyond tolerance.
    straddles = scope == "cover" and any(basis < s.day <= acceptance for s in splits)
    checks["basis"] = Check.FAIL if straddles or not _within_tolerance(common["split_product"]) else Check.PASS

    # 2. DQC_0095: the cover count against each returned accession's own anchor balance-sheet count. SAB Topic 4C
    # restates a balance-sheet count for splits effective before the statements are issued; the acceptance date
    # stands in for issuance, as for the fallback (Amendment 2.2). A split after the cover date and before filing
    # already fails check 1.
    checks["scale"] = Check.UNTESTED
    if scope == "cover":
        conflicts: list[FactUse] = []
        agreeing = False
        for accn in accns:
            sheet = _accession_balance_sheet(view, accn)
            if sheet is None:
                continue
            count, filed_on = Decimal(str(sheet.value)), acceptance_ny_date(str(sheet.acceptance))
            if _within_tolerance(filed / (count * split_product(splits, filed_on, basis))):
                agreeing = True
            else:
                conflicts.append(sheet)
        if conflicts:
            checks["scale"] = Check.FAIL
        elif agreeing:
            checks["scale"] = Check.PASS
        # Recovery needs every comparator to conflict and to carry one count. The co-filed accessions share an
        # acceptance (``value_as_of``), so the highest accession number is taken, for a deterministic provenance.
        recoverable = not agreeing and len({c.value for c in conflicts}) == 1
        if checks["scale"] is Check.FAIL and reference is not None and recoverable:
            if len({c.acceptance for c in conflicts}) != 1:
                cofiled = sorted((c.accns, str(c.acceptance)) for c in conflicts)
                raise PanelError(f"{view.cik10}: co-filed comparators with different acceptances: {cofiled}")
            sheet = max(conflicts, key=lambda c: c.accns)
            filed_on = acceptance_ny_date(str(sheet.acceptance))
            sheet_shares = Decimal(str(sheet.value)) * split_product(splits, filed_on, decision)
            cover_ok = _consistent(shares, reference, splits, decision)
            sheet_ok = _consistent(sheet_shares, reference, splits, decision)
            if cover_ok != sheet_ok:
                checks["scale"] = Check.RECOVERED
                if sheet_ok:
                    common = {**common, "shares": sheet_shares, "basis": filed_on}
                    common["split_product"] = split_product(splits, filed_on, decision)
                    # The count used comes first; the rejected cover fact stays for the audit.
                    common["facts"] = (sheet, *common["facts"])
                    shares = sheet_shares
                common["shares_scope"] = f"dqc_recovered:{'balance_sheet' if sheet_ok else 'cover'}"

    # 3. Trailing dollar volume against ME.
    value = shares * close
    if dollar_volume is None:
        checks["turnover"] = Check.UNTESTED
    else:
        checks["turnover"] = Check.FAIL if dollar_volume / float(value) > DOLLAR_VOLUME_ME_CEILING else Check.PASS

    # 4. Consistency with the last verified count of the series.
    if reference is None:
        checks["discontinuity"] = Check.UNTESTED
    else:
        checks["discontinuity"] = Check.PASS if _consistent(shares, reference, splits, decision) else Check.FAIL

    # Reference eligibility (Amendment 2.1): an untested check 2 needs a two-filing consensus. A recovered count
    # never qualifies. Disjoint accessions and a different count date stop one filing's fact, read in consecutive
    # months or repeated by an amendment, agreeing with itself.
    consensus = (
        checks["scale"] is Check.UNTESTED
        and partner is not None
        and not set(partner.accns) & set(accns)
        and partner.counted != common["facts"][0].key.end
        and _consistent(shares, partner, splits, decision)
    )
    verified = checks["turnover"] is Check.PASS and (checks["scale"] is Check.PASS or consensus)
    for name, reason in (
        ("basis", MeMissing.SHARES_BASIS_AMBIGUOUS),
        ("scale", MeMissing.SHARES_SCALE_CONFLICT),
        ("turnover", MeMissing.SHARES_TURNOVER_IMPLAUSIBLE),
        ("discontinuity", MeMissing.SHARES_DISCONTINUITY),
    ):
        if checks[name] is Check.FAIL:
            return MarketEquity(None, reason, **common, raw_value=raw, checks=checks)
    return MarketEquity(value, None, **common, raw_value=raw, checks=checks, verified=verified)


def split_product(splits: Sequence[SplitStamp], basis: date, decision: date) -> Decimal:
    """Π split_factor over Intrader stamps dated after ``basis`` and on or before s(M).

    Intrader's convention (pinned by tests): the stamp sits on the ex-date as new shares per old share, so
    AAPL's 2020-08-31 4-for-1 is 4 and a 1-for-8 reverse split is 0.125.
    """
    product = Decimal(1)
    for stamp in splits:
        if not stamp.factor.is_finite() or stamp.factor <= 0:
            raise PanelError(f"split stamp {stamp.day} factor {stamp.factor} refuses the run")
        if basis < stamp.day <= decision:
            product *= stamp.factor
    return product


# --------------------------------------------------------------------------- universe helpers


def is_filer(filings: Mapping[str, tuple[str, str]], decision: date) -> bool:
    """A 10-K- or 10-Q-family accession accepted before s(M) and within the filer window.
    ``filings``: accn -> (acceptance, form), as ``CikView.filings``."""
    floor = add_months(decision, -FILER_WINDOW_MONTHS)
    return any(
        form_family(form) in _ROLE_FAMILIES and floor <= acceptance_ny_date(acceptance) < decision
        for acceptance, form in filings.values()
    )


class SicStatus(StrEnum):
    SIC = "sic"
    SIC_NULL = "sic_null"
    SIC_UNLOADED = "sic_unloaded"
    NO_ACCESSION = "no_accession"


@dataclass(frozen=True)
class SicRead:
    status: SicStatus
    sic: int | None = None
    accn: str | None = None


def sic_as_of(filings: Mapping[str, tuple[str, str]], sub_sic: Mapping[str, int | None], decision: date) -> SicRead:
    """SUB ``sic`` of the CIK's latest 10-K/10-Q-family accession accepted before s(M), by exact accession."""
    public = [
        (acceptance, accn)
        for accn, (acceptance, form) in filings.items()
        if form_family(form) in _ROLE_FAMILIES and acceptance_ny_date(acceptance) < decision
    ]
    if not public:
        return SicRead(SicStatus.NO_ACCESSION)
    _, accn = max(public)
    if accn not in sub_sic:
        return SicRead(SicStatus.SIC_UNLOADED, accn=accn)
    sic = sub_sic[accn]
    return SicRead(SicStatus.SIC_NULL if sic is None else SicStatus.SIC, sic, accn)


# --------------------------------------------------------------------------- terciles


class Group(StrEnum):
    LOW = "low"
    MIDDLE = "middle"
    HIGH = "high"


@dataclass(frozen=True)
class SortInput:
    name_key: int
    value: float
    me: float


@dataclass(frozen=True)
class TercileSort:
    groups: Mapping[int, Group] = field(default_factory=dict)
    low_breakpoint: float | None = None
    high_breakpoint: float | None = None


def tercile_groups(names: Sequence[SortInput], micro_cutoff_usd: float) -> TercileSort:
    """JKP terciles: non-micro (ME > NYSE p20) names split into equal-count thirds, micro names placed on the
    same breakpoints. Ties go to the group of their run's first member (our convention)."""
    if any(not math.isfinite(n.value) for n in names):
        raise PanelError("non-finite characteristic value reached the sort")
    big = sorted((n for n in names if n.me > micro_cutoff_usd), key=lambda n: (n.value, n.name_key))
    third = len(big) // 3
    groups: dict[int, Group] = {}
    run_group: Group | None = None
    for i, name in enumerate(big):
        base = Group.LOW if i < third else Group.HIGH if i >= len(big) - third else Group.MIDDLE
        if i == 0 or name.value != big[i - 1].value:
            run_group = base
        assert run_group is not None
        groups[name.name_key] = run_group
    low = [n.value for n in big if groups[n.name_key] is Group.LOW]
    high = [n.value for n in big if groups[n.name_key] is Group.HIGH]
    low_bp, high_bp = (max(low) if low else None), (min(high) if high else None)
    for name in names:
        if name.me > micro_cutoff_usd:
            continue
        if low_bp is not None and name.value <= low_bp:
            groups[name.name_key] = Group.LOW
        elif high_bp is not None and name.value >= high_bp:
            groups[name.name_key] = Group.HIGH
        else:
            groups[name.name_key] = Group.MIDDLE
    return TercileSort(groups, low_bp, high_bp)
