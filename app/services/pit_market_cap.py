"""#3841: point-in-time market cap on a measured price basis -- the pure rules.

Spec: ``docs/specs/fundamentals/2026-10-16-3841-pit-market-cap.md`` (v3.1). This module holds every decision the
spec freezes: the 13F anchor status per quarter, the window factor for one share count (rules 1-4), count selection
and steps, the ported #3609 Amendment 2 checks (no recovery), and Gates 1 and 2. It does no I/O; the rebuild job
and the acceptance script supply the inputs and persist the output.

Why a measured basis: eToro ``price_daily`` is back-adjusted for splits to its last refetch while XBRL counts are as
reported, so ``count x close`` is wrong by every later split. A 13F value / shares is a reported raw quarter-end
close (Form 13F Special Instruction 8); its median over filers, / the stored close on the same session, measures how
far eToro has re-based that session since (F-hat). No split register is read or inferred.

The output is an estimate that refuses an OBSERVED discrepancy inside a count's window. It does not prove the window
was split-free: an offsetting split pair, a re-basing under 10% snapped to 1, and incomplete back-adjustment are
invisible to the anchors (spec "Checkpoint record", accepted limits).
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from bisect import bisect_right
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Any, Final

from app.services.factor_panel import (
    DOLLAR_VOLUME_ME_CEILING,
    MAX_SHARES_AGE_MONTHS,
    SCALE_TOLERANCE,
    Check,
    add_months,
)
from app.services.pit_fundamentals import NEW_YORK, form_family

# Chosen by construction (spec "Source rule": no governing rule exists). Every one is in ``rule_version``.
FIRST_QUARTER: Final = date(2024, 6, 30)
MIN_FILERS: Final = 20
COHERENCE_BAND: Final = 1.02
COHERENCE_SHARE: Final = 0.75
SNAP_BAND: Final = 1.10
SPLIT_BAND: Final = 1.25
AGREEMENT_RATIO: Final = 1.05
TRAILING_QUARTERS: Final = 2
GATE1_BAND: Final = (0.9, 1.1)
GATE1_MIN_TRADES: Final = 3
GATE1_MIN_DATES: Final = 2
GATE2_FLOAT_RATIO: Final = 0.9
# Rule 13f-1(a)(1): a 13F-HR is due within 45 days after the calendar quarter end.
FILING_DUE_DAYS: Final = 45
# Spec Amendment 2 (#3908): the table the anchors' 13F unit values are read from.
ANCHOR_SOURCE: Final = "ownership_institutions_observations"
# 10-K, 10-Q, their amendments and transition forms (``form_family``); FPI forms are out of scope.
IN_SCOPE_FAMILIES: Final = frozenset({"10-K", "10-Q"})


class AnchorStatus(StrEnum):
    OK = "ok"
    NO_CLOSE = "no_close"
    THIN = "thin"
    INCOHERENT = "incoherent"
    AMBIGUOUS = "anchor_ambiguous"


class Refusal(StrEnum):
    NO_LEFT_ANCHOR = "no_left_anchor"
    BASIS_UNRESOLVED = "basis_unresolved"
    SHARES_AMBIGUOUS = "shares_ambiguous"
    SHARES_SCALE_CONFLICT = "shares_scale_conflict"
    SHARES_TURNOVER_IMPLAUSIBLE = "shares_turnover_implausible"
    SHARES_DISCONTINUITY = "shares_discontinuity"
    MULTICLASS_UNAVAILABLE = "multiclass_unavailable"
    INVALID_INPUT = "invalid_input"
    ANCHOR_CONTRADICTED = "anchor_contradicted"
    FLOAT_BOUND = "float_bound"


class CikRefusal(StrEnum):
    """Why an instrument has no steps at all (counted in the build summary, never stored as a step)."""

    UNTIMED_ACCESSION = "untimed_accession"  # a fact whose accession the submissions cannot time (Amendment 1 rule 4)
    FPI_ADR = "fpi_adr"  # an FPI or ADS listing: its quote unit is not the counted share (Amendment 1 rule 6)


class Concept(StrEnum):
    COVER = "cover"  # dei:EntityCommonStockSharesOutstanding
    BALANCE_SHEET = "balance_sheet"  # us-gaap:CommonStockSharesOutstanding


class GateOutcome(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    UNTESTED = "untested"
    UNCOMPARABLE = "uncomparable"


def rule_version() -> str:
    """sha256 of the frozen constants and the refusal set (``market_cap_pit_builds.rule_version``)."""
    payload = {
        "first_quarter": FIRST_QUARTER.isoformat(),
        "min_filers": MIN_FILERS,
        "coherence": [COHERENCE_BAND, COHERENCE_SHARE],
        "bands": [SNAP_BAND, SPLIT_BAND],
        "agreement": AGREEMENT_RATIO,
        "trailing_quarters": TRAILING_QUARTERS,
        "gate1": [*GATE1_BAND, GATE1_MIN_TRADES, GATE1_MIN_DATES],
        "gate2": GATE2_FLOAT_RATIO,
        "filing_due_days": FILING_DUE_DAYS,
        "anchor_source": ANCHOR_SOURCE,
        "families": sorted(IN_SCOPE_FAMILIES),
        "max_shares_age_months": MAX_SHARES_AGE_MONTHS,
        "scale_tolerance": str(SCALE_TOLERANCE),
        "dollar_volume_me_ceiling": DOLLAR_VOLUME_ME_CEILING,
        "scale_fallback": {
            "comparator": "weighted_average_basic_eps_identity",
            "version": SCALE_FALLBACK_VERSION,
            "units": IDENTITY_UNITS,
            "numerator": [INCOME_AVAILABLE, NET_INCOME],
            "periods": "latest weighted-average end, on or before acceptance; one per start",
            "focus": "one distinct non-blank fiscal_period, else unknown",
            "short_period_days": DQC_SHORT_PERIOD_DAYS,
            "per_share_tolerance": f"exact |N - E*W| <= {PER_SHARE_TOLERANCE}*W",
            "statuses": sorted(PeriodStatus),
        },
        "refusals": sorted(Refusal),
        "cik_refusals": sorted(CikRefusal),
        "anchor_statuses": sorted(AnchorStatus),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


# --------------------------------------------------------------------------- calendar


def magnitude(ratio: float) -> float:
    """m(x) = max(x, 1/x): a ratio's distance from 1, symmetric for a split and its reverse."""
    return max(ratio, 1 / ratio)


def quarter_ends(first: date, through: date) -> tuple[date, ...]:
    """Calendar quarter ends in [first, through]."""
    out: list[date] = []
    year = first.year
    while True:
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            end = date(year, month, day)
            if end > through:
                return tuple(out)
            if end >= first:
                out.append(end)
        year += 1


def last_session_on_or_before(sessions: Sequence[date], day: date) -> date | None:
    """s(q): the last NYSE session on or before ``day``; ``sessions`` ascending (``nyse-market-calendar-v1``)."""
    index = bisect_right(sessions, day)
    return sessions[index - 1] if index else None


# --------------------------------------------------------------------------- anchors


@dataclass(frozen=True)
class Anchor:
    """One quarter's observation status for one instrument (spec "Anchors")."""

    quarter: date
    session: date
    status: AnchorStatus
    filers: int
    coherence: float | None = None
    f_hat: float | None = None
    spread: float | None = None  # m(F-hat), each side one division
    provisional: bool = False
    accessions: tuple[str, ...] = ()

    @property
    def phi(self) -> float | None:
        """1 inside the snap band, F-hat at or beyond the split band; None unless ``ok``."""
        if self.status is not AnchorStatus.OK:
            return None
        assert self.f_hat is not None and self.spread is not None
        return 1.0 if self.spread <= SNAP_BAND else self.f_hat

    def evidence(self) -> dict[str, Any]:
        """The quarter's status for a step's evidence. The 13F accession list is carried as its count and the sha256
        of the sorted list: a large issuer's list runs to thousands of accessions per quarter, repeated in every step
        whose window holds the quarter, and the build row's snapshot re-derives the list to compare against."""
        accessions = sorted(self.accessions)
        return {
            "q": self.quarter.isoformat(),
            "s": self.session.isoformat(),
            "status": str(self.status),
            "filers": self.filers,
            "coherence": self.coherence,
            "f_hat": self.f_hat,
            "spread": self.spread,
            "provisional": self.provisional,
            "accessions": len(accessions),
            "accessions_sha256": hashlib.sha256("\n".join(accessions).encode()).hexdigest(),
        }


def classify_anchor(
    quarter: date,
    session: date,
    unit_values: Sequence[float],
    close: float | None,
    *,
    as_of: date,
    accessions: Sequence[str] = (),
) -> Anchor:
    """Status for one (instrument, quarter).

    ``unit_values`` holds one value per distinct filer: the median value / shares over that filer's rows (finite
    shares > 0, finite value > 0, no put/call) in its latest accession for the quarter. ``close`` is the stored
    close on exactly ``session``.
    """
    if any(not math.isfinite(v) or v <= 0 for v in unit_values):
        raise ValueError(f"{quarter}: a filer unit value is not finite and positive")
    provisional = as_of <= quarter + timedelta(days=FILING_DUE_DAYS)
    common: dict[str, Any] = {
        "quarter": quarter,
        "session": session,
        "filers": len(unit_values),
        "provisional": provisional,
        "accessions": tuple(accessions),
    }
    if close is None or not math.isfinite(close) or close <= 0:
        return Anchor(status=AnchorStatus.NO_CLOSE, **common)
    if len(unit_values) < MIN_FILERS:
        return Anchor(status=AnchorStatus.THIN, **common)
    median = statistics.median(unit_values)
    coherence = sum(magnitude(v / median) <= COHERENCE_BAND for v in unit_values) / len(unit_values)
    f_hat = median / close
    # Each side is one division, so a band edge and its reciprocal classify alike.
    spread = max(median / close, close / median)
    if coherence < COHERENCE_SHARE:
        status = AnchorStatus.INCOHERENT
    elif SNAP_BAND < spread < SPLIT_BAND:
        status = AnchorStatus.AMBIGUOUS
    else:
        status = AnchorStatus.OK
    return Anchor(status=status, coherence=coherence, f_hat=f_hat, spread=spread, **common)


def observed(anchors: Sequence[Anchor]) -> tuple[Anchor, ...]:
    """The anchors with trailing provisional ``thin`` quarters removed.

    A quarter whose 13F due date has not passed is still filling; before then ``thin`` means "not yet observed", not
    "observed and missing". Without this, every count whose window reaches the latest quarter would be refused for 45
    days after each quarter end. Only ``thin`` is filings not yet arrived: a provisional ``no_close``, ``incoherent``
    or ``anchor_ambiguous`` quarter is an observed discrepancy and stays, so rule 2 refuses through it. A provisional
    ``ok`` quarter is used, and the daily rebuild re-reads it as filings land. This is what gives the two-quarter
    trailing limit (rule 4) its meaning.
    """
    end = len(anchors)
    while end and anchors[end - 1].provisional and anchors[end - 1].status is AnchorStatus.THIN:
        end -= 1
    return tuple(anchors[:end])


@dataclass(frozen=True)
class Basis:
    """The window factor for one count: F, or a refusal, with the window W it was decided on."""

    factor: float | None
    refusal: Refusal | None
    window: tuple[Anchor, ...]
    bracketed: bool


def window_factor(b_lo: date, b_hi: date, anchors: Sequence[Anchor]) -> Basis:
    """Rules 1-4 of spec "The factor for one count" over the basis interval [b_lo, b_hi].

    ``anchors`` holds every quarter end from ``FIRST_QUARTER`` to the latest on or before the build date, ascending;
    the last ``TRAILING_QUARTERS`` of them are "the most recent quarter ends" for rule 4. Comparisons use s(q).
    """
    seen = observed(anchors)
    left = [i for i, anchor in enumerate(seen) if anchor.session <= b_lo]
    if not left:
        return Basis(None, Refusal.NO_LEFT_ANCHOR, (), False)
    start = left[-1]
    right = next((i for i in range(start, len(seen)) if seen[i].session >= b_hi), None)
    window = seen[start : len(seen) if right is None else right + 1]
    bracketed = right is not None
    # Rule 1: the left edge.
    if window[0].status is not AnchorStatus.OK:
        return Basis(None, Refusal.NO_LEFT_ANCHOR, window, bracketed)
    # Rule 2: no bridging over a quarter that is not ok.
    if any(anchor.status is not AnchorStatus.OK for anchor in window):
        return Basis(None, Refusal.BASIS_UNRESOLVED, window, bracketed)
    phis = [anchor.phi for anchor in window]
    assert all(phi is not None for phi in phis)
    values = [float(phi) for phi in phis if phi is not None]
    # Rule 3: agreement as max / min.
    if max(values) / min(values) > AGREEMENT_RATIO:
        return Basis(None, Refusal.BASIS_UNRESOLVED, window, bracketed)
    # Rule 4: an unbracketed window must end recently and show no net re-basing since.
    if not bracketed:
        recent = {anchor.quarter for anchor in anchors[-TRAILING_QUARTERS:]}
        if window[-1].quarter not in recent or any(value != 1.0 for value in values):
            return Basis(None, Refusal.BASIS_UNRESOLVED, window, bracketed)
    return Basis(values[0], None, window, bracketed)


# --------------------------------------------------------------------------- selection


@dataclass(frozen=True)
class ShareFact:
    """One share-count fact. ``context`` is the instant (cover) or the period end (balance sheet);
    ``accepted_at`` is the EDGAR acceptance from ``submissions.zip``, timezone-aware."""

    concept: Concept
    accession: str
    form: str
    context: date
    accepted_at: datetime
    value: Decimal

    @property
    def accepted(self) -> date:
        """The New York civil date of acceptance; the count is public from the first session after it."""
        return self.accepted_at.astimezone(NEW_YORK).date()


def eligible(fact: ShareFact, session: date) -> bool:
    """Admitted before ``session``, in scope by form, and within the #3609 age predicate."""
    return (
        form_family(fact.form) in IN_SCOPE_FAMILIES
        and fact.accepted < session
        and add_months(session, -MAX_SHARES_AGE_MONTHS) <= fact.context
    )


@dataclass(frozen=True)
class Selection:
    """The count selected at a session. ``shares`` is None when the candidates disagree (``values`` holds them)."""

    concept: Concept
    accession: str
    context: date
    accepted_at: datetime
    shares: Decimal | None
    values: tuple[Decimal, ...]

    @property
    def basis_interval(self) -> tuple[date, date]:
        """[context, acceptance]: cover per #3609 check 1; balance sheet per SAB Topic 4C, acceptance as issuance."""
        return self.context, self.accepted_at.astimezone(NEW_YORK).date()


def select(facts: Sequence[ShareFact], session: date) -> Selection | None:
    """The cover count with the latest context, then the latest acceptance; the balance-sheet count only when no
    cover count is eligible. Distinct values at the chosen key are ambiguous, with no fallback past them."""
    for concept in (Concept.COVER, Concept.BALANCE_SHEET):
        candidates = [f for f in facts if f.concept is concept and eligible(f, session)]
        if not candidates:
            continue
        key = max((f.context, f.accepted_at) for f in candidates)
        tied = [f for f in candidates if (f.context, f.accepted_at) == key]
        values = tuple(sorted({f.value for f in tied}))
        return Selection(
            concept=concept,
            accession=max(f.accession for f in tied),
            context=key[0],
            accepted_at=key[1],
            shares=values[0] if len(values) == 1 else None,
            values=values,
        )
    return None


def accession_sheet_count(facts: Sequence[ShareFact], accession: str) -> Decimal | None:
    """Check 2's comparator: the accession's own balance-sheet count at its latest instant, when it holds one value."""
    sheet = [f for f in facts if f.concept is Concept.BALANCE_SHEET and f.accession == accession]
    if not sheet:
        return None
    latest = max(f.context for f in sheet)
    values = {f.value for f in sheet if f.context == latest}
    return values.pop() if len(values) == 1 else None


# --------------------------------------------------------------------------- scale fallback (Amendment 3, #3925)

# The EPS-identity operands (ASC 260-10-45-10; DQC_0227's numerator, then the stored stand-in) and their units.
WEIGHTED_AVERAGE: Final = "WeightedAverageNumberOfSharesOutstandingBasic"
EPS_BASIC: Final = "EarningsPerShareBasic"
INCOME_AVAILABLE: Final = "NetIncomeLossAvailableToCommonStockholdersBasic"
NET_INCOME: Final = "NetIncomeLoss"
IDENTITY_UNITS: Final = {
    WEIGHTED_AVERAGE: "shares",
    EPS_BASIC: "USD/shares",
    INCOME_AVAILABLE: "USD",
    NET_INCOME: "USD",
}
# DQC_0227 (2026): ``$perShareItemTolerance``, and the period minimums under which it suppresses an EPS failure.
PER_SHARE_TOLERANCE: Final = Decimal("0.01")
DQC_SHORT_PERIOD_DAYS: Final = {"FY": 310, "Q2": 120, "Q3": 200}
SCALE_FALLBACK_VERSION: Final = 1


class PeriodStatus(StrEnum):
    """Rule 4: a period is consistent, or unused with the first condition it fails."""

    CONSISTENT = "consistent"
    FOCUS_UNKNOWN = "focus_unknown"
    DQC_SHORT_PERIOD = "dqc_short_period"
    OPERAND_COUNT = "operand_count"
    OPERAND_INVALID = "operand_invalid"
    IDENTITY_DISAGREES = "identity_disagrees"


@dataclass(frozen=True)
class IdentityFact:
    """One EPS-identity operand of an accession: an undimensioned duration fact (rule 2)."""

    concept: str
    start: date
    end: date
    value: Decimal
    fiscal_period: str | None


def fiscal_focus(facts: Sequence[IdentityFact]) -> str | None:
    """The accession's fiscal period focus: its one distinct non-blank ``fiscal_period``; None when unknown."""
    found = {f.fiscal_period for f in facts if f.fiscal_period}
    return found.pop() if len(found) == 1 else None


def _finite_decimal(value: Decimal) -> bool:
    return value.is_finite() and math.isfinite(float(value))


def period_status(
    days: int, focus: str | None, operands: Mapping[str, set[Decimal]]
) -> tuple[PeriodStatus, Decimal | None, Decimal | None]:
    """Rule 4 for one period: (status, weighted average, numerator / EPS). The penny test is exact decimal arithmetic,
    |N - E x W| <= 0.01 x W, so a gap of exactly one cent passes; the 100x bound is in float, as the cover check."""
    if focus is None:
        return PeriodStatus.FOCUS_UNKNOWN, None, None
    if days < DQC_SHORT_PERIOD_DAYS.get(focus, 0):
        return PeriodStatus.DQC_SHORT_PERIOD, None, None
    weighted = operands.get(WEIGHTED_AVERAGE, set())
    eps = operands.get(EPS_BASIC, set())
    numerator = operands[INCOME_AVAILABLE] if INCOME_AVAILABLE in operands else operands.get(NET_INCOME, set())
    if len(weighted) != 1 or len(eps) != 1 or len(numerator) != 1:
        return PeriodStatus.OPERAND_COUNT, None, None
    (w,), (e,), (n,) = weighted, eps, numerator
    if not all(_finite_decimal(x) for x in (w, e, n)) or w <= 0 or e == 0 or n == 0 or (n > 0) != (e > 0):
        return PeriodStatus.OPERAND_INVALID, None, None
    with localcontext() as context:
        context.prec = 60  # wide enough that N - E x W is exact for any stored numeric(…, 6) operands
        gap = abs(n - e * w)
        implied = n / e
    if gap > PER_SHARE_TOLERANCE * w or not _finite_decimal(implied):
        return PeriodStatus.IDENTITY_DISAGREES, w, implied
    if not _within_tolerance(float(w) / float(implied)):
        return PeriodStatus.IDENTITY_DISAGREES, w, implied
    return PeriodStatus.CONSISTENT, w, implied


def weighted_average_scale(
    cover: Decimal, accepted: date, facts: Sequence[IdentityFact]
) -> tuple[Check, dict[str, Any]]:
    """Rules 3-5: the scale outcome of an eligible cover count against its accession's consistent periods, and the
    evidence. ``accepted`` is the accession's New York acceptance date; ``facts`` are the accession's operand rows."""
    evidence: dict[str, Any] = {
        "focus": fiscal_focus(facts),
        "latest_end": None,
        "accepted": accepted.isoformat(),
        "no_period": None,
        "periods": [],
    }
    ends = [f.end for f in facts if f.concept == WEIGHTED_AVERAGE]
    if not ends:
        evidence["no_period"] = "no_weighted_average"
        return Check.UNTESTED, evidence
    latest = max(ends)
    evidence["latest_end"] = latest.isoformat()
    if latest > accepted:
        evidence["no_period"] = "end_after_acceptance"
        return Check.UNTESTED, evidence
    by_start: dict[date, dict[str, set[Decimal]]] = {}
    for fact in facts:
        if fact.end == latest:
            by_start.setdefault(fact.start, {}).setdefault(fact.concept, set()).add(fact.value)
    outcomes: list[bool] = []
    for start, operands in sorted(by_start.items()):
        if WEIGHTED_AVERAGE not in operands:
            continue
        days = (latest - start).days + 1
        status, weighted, implied = period_status(days, evidence["focus"], operands)
        entry: dict[str, Any] = {
            "start": start.isoformat(),
            "end": latest.isoformat(),
            "days": days,
            "values": {concept: sorted(str(v) for v in values) for concept, values in sorted(operands.items())},
            "numerator": INCOME_AVAILABLE if INCOME_AVAILABLE in operands else NET_INCOME,
            "implied": _str(implied),
            "status": str(status),
        }
        if status is PeriodStatus.CONSISTENT:
            assert weighted is not None and implied is not None
            versus = {
                "weighted_average": _within_tolerance(float(cover) / float(weighted)),
                "implied": _within_tolerance(float(cover) / float(implied)),
            }
            entry["cover_vs"] = {k: str(Check.PASS if ok else Check.FAIL) for k, ok in versus.items()}
            outcomes.extend(versus.values())
        evidence["periods"].append(entry)
    if not outcomes:
        return Check.UNTESTED, evidence
    return (Check.PASS if all(outcomes) else Check.FAIL), evidence


def _sheet_unavailable(facts: Sequence[ShareFact], accession: str, sheet: Decimal | None) -> str:
    """Rule 1's reason the balance-sheet comparator is unusable: a single non-positive value, several, or none."""
    if sheet is not None:
        return "invalid"
    if any(f.concept is Concept.BALANCE_SHEET and f.accession == accession for f in facts):
        return "ambiguous"
    return "none"


def _within_tolerance(ratio: float) -> bool:
    tolerance = float(SCALE_TOLERANCE)
    return 1 / tolerance <= ratio <= tolerance


def _finite_positive(value: float | Decimal | None) -> bool:
    """Judged on the float the arithmetic will use: a Decimal beyond float range reads inf, and one below it 0.0."""
    if value is None:
        return False
    as_float = float(value)
    return math.isfinite(as_float) and as_float > 0


# --------------------------------------------------------------------------- steps


@dataclass(frozen=True)
class Step:
    """A maximal run of sessions with one selected result. ``valid_to`` is exclusive: the first session not in it, or
    the day after the last session built."""

    valid_from: date
    valid_to: date
    selection: Selection
    split_factor: float | None
    refusal: Refusal | None
    window: tuple[date, ...]
    evidence: Mapping[str, Any]

    def covers(self, session: date) -> bool:
        return self.valid_from <= session < self.valid_to

    def market_cap(self, close: float | None) -> float | None:
        """close x shares_reported x split_factor, as the view computes it; None when refused or not computable."""
        if self.refusal is not None or self.selection.shares is None or self.split_factor is None:
            return None
        if not _finite_positive(close):
            return None
        assert close is not None
        value = close * float(self.selection.shares) * self.split_factor
        return value if math.isfinite(value) else None


def _selection_evidence(selection: Selection) -> dict[str, Any]:
    return {
        "concept": str(selection.concept),
        "accession": selection.accession,
        "context": selection.context.isoformat(),
        "accepted_at": selection.accepted_at.isoformat(),
        "values": [str(v) for v in selection.values],
    }


def build_steps(
    facts: Sequence[ShareFact],
    anchors: Sequence[Anchor],
    sessions: Sequence[date],
    *,
    closes: Mapping[date, float],
    dollar_volume: Mapping[date, float],
    multiclass: bool,
    identity: Mapping[str, Sequence[IdentityFact]] | None = None,
) -> tuple[Step, ...]:
    """Steps for one instrument over ``sessions`` (ascending NYSE sessions to the build date).

    ``identity`` holds each accession's EPS-identity operand rows, the scale check's comparator when the accession
    has no usable balance-sheet count (spec Amendment 3).

    ``closes`` and ``dollar_volume`` (the mean 21-session dollar volume ending at the session) are on the stored
    basis. Every check is evaluated where its operands exist; the first failure, in the order below, is the refusal.
    A session with no eligible count has no step.
    """
    identity = identity or {}
    bases: dict[tuple[date, date], Basis] = {}
    fallbacks: dict[str, tuple[Check, dict[str, Any]]] = {}
    steps: list[Step] = []
    open_key: tuple[Any, ...] | None = None
    open_step: Step | None = None

    def close_open(end: date) -> None:
        nonlocal open_step, open_key
        if open_step is not None:
            steps.append(replace(open_step, valid_to=end))
        open_step, open_key = None, None

    for session in sessions:
        selection = select(facts, session)
        if selection is None:
            close_open(session)
            continue
        interval = selection.basis_interval
        if interval not in bases:
            bases[interval] = window_factor(*interval, anchors)
        basis = bases[interval]
        shares = selection.shares
        factor = basis.factor
        # ``float(Decimal)`` goes through the decimal string: a count beyond float range becomes inf, never an
        # OverflowError, and refuses ``invalid_input`` below (pinned by a test).
        product = (
            float(shares) * factor
            if shares is not None and factor is not None and _finite_positive(shares) and _finite_positive(factor)
            else None
        )
        # Every ratio below divides by one of these, so each must be finite and positive, not merely computed.
        common_basis = product if _finite_positive(product) else None
        checks: dict[str, Check] = {}

        sheet = accession_sheet_count(facts, selection.accession) if selection.concept is Concept.COVER else None
        scale: dict[str, Any] = {"cover": _str(shares), "sheet": _str(sheet), "comparator": None}
        if selection.concept is not Concept.COVER or shares is None or not _finite_positive(shares):
            checks["scale"] = Check.UNTESTED
        elif sheet is not None and _finite_positive(sheet):
            # Float, not Decimal, division: a Decimal quotient beyond the context's exponent range would trap.
            checks["scale"] = Check.PASS if _within_tolerance(float(shares) / float(sheet)) else Check.FAIL
            scale["comparator"] = "balance_sheet"
        else:
            # Amendment 3: no usable balance-sheet count, so the accession's EPS-consistent periods are the comparator.
            if selection.accession not in fallbacks:
                accepted = selection.accepted_at.astimezone(NEW_YORK).date()
                fallbacks[selection.accession] = weighted_average_scale(
                    shares, accepted, identity.get(selection.accession, ())
                )
            checks["scale"], detail = fallbacks[selection.accession]
            scale["sheet_unavailable"] = _sheet_unavailable(facts, selection.accession, sheet)
            scale["comparator"] = None if checks["scale"] is Check.UNTESTED else "weighted_average_basic"
            scale.update(detail)

        close = closes.get(session)
        volume = dollar_volume.get(session)
        raw_cap = None if common_basis is None or close is None or not _finite_positive(close) else common_basis * close
        cap = raw_cap if _finite_positive(raw_cap) else None
        # A zero mean dollar volume is an operand (the ratio is 0 and passes), not a missing one.
        if cap is None or volume is None or not math.isfinite(volume) or volume < 0:
            checks["turnover"] = Check.UNTESTED
        else:
            checks["turnover"] = Check.FAIL if volume / cap > DOLLAR_VOLUME_ME_CEILING else Check.PASS

        # The reference is a DIFFERENT count: a run of one count split by a per-session check compares against what
        # preceded the count, not against itself.
        history: list[Step] = [*steps] if open_step is None else [*steps, open_step]
        reference = next(
            (
                step
                for step in reversed(history)
                if step.refusal is None and step.selection.concept is selection.concept and step.selection != selection
            ),
            None,
        )
        reference_basis = (
            float(reference.selection.shares) * reference.split_factor
            if reference is not None and reference.selection.shares is not None and reference.split_factor is not None
            else None
        )
        if common_basis is None or reference_basis is None:
            checks["discontinuity"] = Check.UNTESTED
        else:
            checks["discontinuity"] = Check.PASS if _within_tolerance(common_basis / reference_basis) else Check.FAIL

        refusal: Refusal | None
        if multiclass:
            refusal = Refusal.MULTICLASS_UNAVAILABLE
        elif shares is None:
            refusal = Refusal.SHARES_AMBIGUOUS
        elif not _finite_positive(shares):
            refusal = Refusal.INVALID_INPUT
        elif basis.refusal is not None:
            refusal = basis.refusal
        elif common_basis is None or (raw_cap is not None and cap is None):
            refusal = Refusal.INVALID_INPUT
        elif checks["scale"] is Check.FAIL:
            refusal = Refusal.SHARES_SCALE_CONFLICT
        elif checks["turnover"] is Check.FAIL:
            refusal = Refusal.SHARES_TURNOVER_IMPLAUSIBLE
        elif checks["discontinuity"] is Check.FAIL:
            refusal = Refusal.SHARES_DISCONTINUITY
        else:
            refusal = None
        split_factor = factor if refusal is None else None

        key = (selection, split_factor, refusal, tuple(sorted(checks.items())))
        if key == open_key:
            continue
        close_open(session)
        evidence = {
            "selection": _selection_evidence(selection),
            "basis": {
                "interval": [interval[0].isoformat(), interval[1].isoformat()],
                "bracketed": basis.bracketed,
                "factor": basis.factor,
                "refusal": None if basis.refusal is None else str(basis.refusal),
                "window": [anchor.evidence() for anchor in basis.window],
            },
            "checks": {
                "scale": {"outcome": str(checks["scale"]), **scale},
                "turnover": {
                    "outcome": str(checks["turnover"]),
                    "session": session.isoformat(),
                    "dollar_volume": volume,
                    "cap": cap,
                },
                "discontinuity": {
                    "outcome": str(checks["discontinuity"]),
                    "reference": None
                    if reference is None
                    else {
                        "accession": reference.selection.accession,
                        "context": reference.selection.context.isoformat(),
                        "shares": _str(reference.selection.shares),
                        "split_factor": reference.split_factor,
                    },
                },
            },
            "multiclass": multiclass,
        }
        open_step = Step(
            valid_from=session,
            valid_to=session,
            selection=selection,
            split_factor=split_factor,
            refusal=refusal,
            window=tuple(anchor.quarter for anchor in basis.window),
            evidence=evidence,
        )
        open_key = key
    if sessions:
        close_open(sessions[-1] + timedelta(days=1))
    return tuple(steps)


def _str(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


# --------------------------------------------------------------------------- gate 1


@dataclass(frozen=True)
class Trade:
    """A Form 4 non-derivative P or S row on common stock, deduplicated, with a finite price > 0."""

    day: date
    price: float


@dataclass(frozen=True)
class Gate1Quarter:
    quarter: date
    outcome: GateOutcome
    trades: int
    dates: int
    median_ratio: float | None


def trade_factor(day: date, anchors: Sequence[Anchor]) -> tuple[date, float] | None:
    """(q_k, Phi(q_k)) for a trade on ``day``: s(q_k) <= day < s(q_k+1), both ok and agreeing; else None."""
    for left, right in zip(anchors, anchors[1:], strict=False):
        if left.session <= day < right.session:
            phi_left, phi_right = left.phi, right.phi
            if phi_left is None or phi_right is None:
                return None
            if max(phi_left, phi_right) / min(phi_left, phi_right) > AGREEMENT_RATIO:
                return None
            return left.quarter, phi_left
    return None


def gate1(
    trades: Sequence[Trade], closes: Mapping[date, float], anchors: Sequence[Anchor]
) -> tuple[dict[date, Gate1Quarter], int]:
    """Per q_k: the median of price / (close(d) x Phi(d)) over tested trades. Returns the quarters and the count of
    untested trades (no bracketing pair, a pair not ok or disagreeing, or no close on d)."""
    ratios: dict[date, list[tuple[date, float]]] = {}
    untested = 0
    for trade in trades:
        factor = trade_factor(trade.day, anchors)
        close = closes.get(trade.day)
        expected = None if factor is None or close is None or not _finite_positive(close) else close * factor[1]
        if factor is None or expected is None or not _finite_positive(expected) or not _finite_positive(trade.price):
            untested += 1
            continue
        ratios.setdefault(factor[0], []).append((trade.day, trade.price / expected))
    results: dict[date, Gate1Quarter] = {}
    for quarter, rows in sorted(ratios.items()):
        dates = len({day for day, _ in rows})
        if len(rows) < GATE1_MIN_TRADES or dates < GATE1_MIN_DATES:
            results[quarter] = Gate1Quarter(quarter, GateOutcome.UNTESTED, len(rows), dates, None)
            continue
        median = statistics.median(ratio for _, ratio in rows)
        low, high = GATE1_BAND
        outcome = GateOutcome.PASS if low <= median <= high else GateOutcome.FAIL
        results[quarter] = Gate1Quarter(quarter, outcome, len(rows), dates, median)
    return results, untested


# --------------------------------------------------------------------------- gate 2


@dataclass(frozen=True)
class FloatFact:
    """``dei:EntityPublicFloat`` in USD from the latest accession for its context date, same CIK."""

    context: date
    value: float


@dataclass(frozen=True)
class Gate2Result:
    context: date
    session: date | None
    outcome: GateOutcome
    cap: float | None
    public_float: float


def gate2(
    floats: Sequence[FloatFact], steps: Sequence[Step], closes: Mapping[date, float], sessions: Sequence[date]
) -> tuple[Gate2Result, ...]:
    """One-sided: fail when cap < 0.9 x public float on the last session on or before the float's context date."""
    out: list[Gate2Result] = []
    for fact in floats:
        session = last_session_on_or_before(sessions, fact.context)
        step = None if session is None else next((s for s in steps if s.covers(session)), None)
        cap = None if step is None or session is None else step.market_cap(closes.get(session))
        if cap is None or not _finite_positive(fact.value):
            out.append(Gate2Result(fact.context, session, GateOutcome.UNCOMPARABLE, cap, fact.value))
            continue
        outcome = GateOutcome.FAIL if cap < GATE2_FLOAT_RATIO * fact.value else GateOutcome.PASS
        out.append(Gate2Result(fact.context, session, outcome, cap, fact.value))
    return tuple(out)


# --------------------------------------------------------------------------- applying the gates


def _refuse(steps: Sequence[Step], hit: Callable[[Step], bool], refusal: Refusal, note: str) -> tuple[Step, ...]:
    """Refuse every published step ``hit`` selects; a step already refused keeps its first refusal."""
    out: list[Step] = []
    for step in steps:
        if step.refusal is None and hit(step):
            evidence = {**step.evidence, "gates": {**step.evidence.get("gates", {}), str(refusal): note}}
            step = replace(step, split_factor=None, refusal=refusal, evidence=evidence)
        out.append(step)
    return tuple(out)


def apply_gate1(steps: Sequence[Step], quarters: Mapping[date, Gate1Quarter]) -> tuple[Step, ...]:
    failed = {q for q, result in quarters.items() if result.outcome is GateOutcome.FAIL}
    return _refuse(
        steps,
        lambda step: bool(failed & set(step.window)),
        Refusal.ANCHOR_CONTRADICTED,
        "Form 4 median price/(close x Phi) outside the band in a window quarter",
    )


def apply_gate2(steps: Sequence[Step], results: Sequence[Gate2Result]) -> tuple[Step, ...]:
    failed = [r.session for r in results if r.outcome is GateOutcome.FAIL and r.session is not None]
    return _refuse(
        steps,
        lambda step: any(step.covers(session) for session in failed),
        Refusal.FLOAT_BOUND,
        "cap below 0.9 x dei:EntityPublicFloat",
    )
