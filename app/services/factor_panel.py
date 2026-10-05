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
from dataclasses import dataclass, field
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
COGS: Final = ("CostOfGoodsAndServicesSold", "CostOfRevenue", "CostOfGoodsSold")
#: Flagged ``cogs_scope=goods_only`` when it supplies COGS.
COGS_GOODS_ONLY: Final = "CostOfGoodsSold"
GP: Final = ("GrossProfit",)
XSGA: Final = ("SellingGeneralAndAdministrativeExpense",)
XINT: Final = ("InterestExpense",)
IB: Final = ("IncomeLossFromContinuingOperations",)
OANCF: Final = ("NetCashProvidedByUsedInOperatingActivities",)
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


ABSENT: Final = Term(TermStatus.ABSENT)
ZERO: Final = Term(TermStatus.VALUE, Decimal(0), branches=("zero_if_missing",))


def combine(*parts: tuple[int, Term]) -> Term:
    """Σ coefficient × term. A blocking part wins over an absent one: a blocked newer key is never skipped."""
    facts = tuple(f for _, term in parts for f in term.facts)
    branches = tuple(b for _, term in parts for b in term.branches)
    for status in _BLOCKING:
        if any(term.status is status for _, term in parts):
            return Term(status, facts=facts, branches=branches)
    if any(term.status is TermStatus.ABSENT for _, term in parts):
        return Term(TermStatus.ABSENT, facts=facts, branches=branches)
    total = sum((coefficient * term.value for coefficient, term in parts if term.value is not None), Decimal(0))
    return Term(TermStatus.VALUE, total, facts, branches=branches)


def first_available(branches: Iterable[tuple[str, Callable[[], Term]]]) -> Term:
    """Hierarchy walk: ``absent`` tries the next branch; anything else stops it."""
    for name, read in branches:
        term = read()
        if term.status is not TermStatus.ABSENT:
            return Term(term.status, term.value, term.facts, term.start, (name, *term.branches))
    return ABSENT


# --------------------------------------------------------------------------- the bundle at s(M)


class BundleReader(Protocol):
    def value_as_of(self, cik10: str, key: FactKey, decision: date) -> ValueRead: ...

    def public_events(self, cik10: str, taxonomy: str, concept: str, decision: date) -> PrefixRead: ...


class CikView:
    """One CIK's bundle at decision session s(M). Every read uses acceptance NY date strictly before s(M)."""

    def __init__(self, bundle: BundleReader, cik10: str, decision: date) -> None:
        self.bundle = bundle
        self.cik10 = cik10
        self.decision = decision
        self._prefix: dict[tuple[str, str], PrefixRead] = {}
        assets = self.prefix("us-gaap", "Assets")
        self.exclusion = prefix_exclusion(assets.status)
        self.forms: dict[str, str] = {a["accn"]: a["form"] for a in assets.accessions}
        self.acceptances: dict[str, str] = {a["accn"]: a["acceptance"] for a in assets.accessions}
        self.anchors = period_anchors(assets.events, self.forms)
        self.unanchored = sorted(
            accn
            for accn, form in self.forms.items()
            if form_family(form) in _ROLE_FAMILIES and accn not in self.anchors
        )
        annual = {end for accn, end in self.anchors.items() if form_family(self.forms[accn]) == ANNUAL_FAMILY}
        self.periods: dict[Kind, tuple[date, ...]] = {
            Kind.ANNUAL: tuple(sorted(annual, reverse=True)),
            Kind.QUARTERLY: tuple(sorted(set(self.anchors.values()), reverse=True)),
        }

    def prefix(self, taxonomy: str, concept: str) -> PrefixRead:
        cached = self._prefix.get((taxonomy, concept))
        if cached is None:
            cached = self._prefix[(taxonomy, concept)] = self.bundle.public_events(
                self.cik10, taxonomy, concept, self.decision
            )
            prefix_exclusion(cached.status)  # refuses AFTER_CAPTURE / CONCEPT_NOT_IN_POLICY on every concept
        return cached

    def keys(self, concept: str, *, taxonomy: str = "us-gaap", unit: str = USD) -> dict[tuple[str | None, str], str]:
        """Every public key of the concept -> its latest public acceptance (events and rejections alike)."""
        read = self.prefix(taxonomy, concept)
        latest: dict[tuple[str | None, str], str] = {}
        for row in (*read.events, *read.rejections):
            if row["unit"] == unit:
                key = (row["start"], row["end"])
                latest[key] = max(latest.get(key, ""), row["acceptance"])
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
        form = self.forms.get(accn)
        return form is not None and form_family(form) in _ROLE_FAMILIES and accn in self.anchors


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
    return Term(term.status, term.value, facts, term.start, term.branches)


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
    return Term(total.status, total.value, total.facts, terms[-1].start, total.branches)


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


def gp(view: CikView, end: date, kind: Kind) -> Term:
    return first_available(
        (
            ("GP", lambda: flow(view, GP, end, kind)),
            (
                "sale-cogs",
                lambda: combine((1, flow(view, SALE, end, kind)), (-1, _negated(flow(view, COGS, end, kind)))),
            ),
        )
    )


def ope(view: CikView, end: date, kind: Kind) -> Term:
    """Proxy: ``sale*`` − COGS − XSGA − XINT (FF's OP numerator, which JKP's ``ope*`` targets)."""
    return combine(
        (1, flow(view, SALE, end, kind)),
        (-1, _negated(flow(view, COGS, end, kind))),
        (-1, _negated(flow(view, XSGA, end, kind))),
        (-1, _negated(flow(view, XINT, end, kind))),
    )


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


@dataclass(frozen=True)
class Ratio:
    """A characteristic's numerator and denominator at one period, before eligibility."""

    numerator: Term
    denominator: Term | None  # None: the denominator is ME


def compute_ratio(name: str, view: CikView, end: date, kind: Kind) -> Ratio:
    match name:
        case "gp_at":
            return Ratio(gp(view, end, kind), instant(view, AT, end))
        case "be_me":
            return Ratio(be(view, end), None)
        case "ope_be":
            return Ratio(ope(view, end, kind), be(view, end))
        case "ni_me":
            return Ratio(flow(view, IB, end, kind), None)
        case "ocf_me":
            return Ratio(flow(view, OANCF, end, kind), None)
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
    for kind in (Kind.ANNUAL, Kind.QUARTERLY):
        for end in view.periods[kind]:
            if not lag_eligible(end, formation):
                continue
            tested += 1
            ratio = compute_ratio(name, view, end, kind)
            parts = [ratio.numerator] + ([] if ratio.denominator is None else [ratio.denominator])
            if combine(*((1, p) for p in parts)).status is not TermStatus.ABSENT:
                chosen.append((end, kind, ratio))
                break
    if not chosen:
        return Characteristic(name, None, Missing.NO_PERIOD, candidates_tested=tested)
    end, kind, ratio = max(chosen, key=lambda c: (c[0], c[1] is Kind.ANNUAL))
    parts = [ratio.numerator] + ([] if ratio.denominator is None else [ratio.denominator])
    facts = tuple(f for p in parts for f in p.facts)
    branches = tuple(b for p in parts for b in p.branches)

    def missing(reason: Missing) -> Characteristic:
        return Characteristic(name, None, reason, end, kind, facts, branches, tested)

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
    return Characteristic(name, float(value), None, end, kind, facts, branches, tested)


# --------------------------------------------------------------------------- market equity


@dataclass(frozen=True)
class SplitStamp:
    day: date
    factor: Decimal


@dataclass(frozen=True)
class MarketEquity:
    value: Decimal | None
    missing: MeMissing | None
    shares_scope: str | None = None  # "cover" | "balance_sheet"
    shares: Decimal | None = None
    basis: date | None = None
    split_product: Decimal | None = None
    facts: tuple[FactUse, ...] = ()


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


def market_equity(view: CikView, close: Decimal | None, splits: Sequence[SplitStamp]) -> MarketEquity:
    """§"Market equity": the filing's own cover count, else the balance-sheet count; never a fallback when blocked."""
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
        basis = context if scope == "cover" else acceptance_ny_date(str(use.acceptance))
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
        return MarketEquity(shares * close, None, **common)
    return MarketEquity(None, MeMissing.NO_SHARES)


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


def is_filer(accessions: Mapping[str, str], forms: Mapping[str, str], decision: date) -> bool:
    """A 10-K- or 10-Q-family accession accepted before s(M) and within the filer window. ``accessions`` are
    already public (accn -> acceptance)."""
    floor = add_months(decision, -FILER_WINDOW_MONTHS)
    return any(
        form_family(forms[accn]) in _ROLE_FAMILIES and floor <= acceptance_ny_date(acceptance) < decision
        for accn, acceptance in accessions.items()
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


def sic_as_of(
    accessions: Mapping[str, str], forms: Mapping[str, str], sub_sic: Mapping[str, int | None], decision: date
) -> SicRead:
    """SUB ``sic`` of the CIK's latest 10-K/10-Q-family accession accepted before s(M), by exact accession."""
    public = [
        (acceptance, accn)
        for accn, acceptance in accessions.items()
        if form_family(forms[accn]) in _ROLE_FAMILIES and acceptance_ny_date(acceptance) < decision
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
