"""The tax-year view: the capital-gains computation, coverage flags and the budget reserve's arithmetic.

#3616 slice 3d-4a (``docs/specs/metrics/2026-10-15-3616-slice3d4a-tax-year-view.md``), Designs 2–5 and 7. Pure:
every function here takes rows already read from one effective matching run and returns figures; reading the run
and the endpoints are part B.

Every amount is computed exactly: a ``decimal`` context with ``Inexact`` and ``Rounded`` trapped raises rather than
round. Rounding happens only at the named boundaries (display, and ``to_usd_ceiling`` for the reserve).
"""

from __future__ import annotations

import decimal
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final, Literal

from app.services.tax_assumptions import assumption_ids
from app.services.tax_ledger import (
    _CGT_RATE_PERIODS,
    _MIN_SUPPORTED_TAX_YEAR_START,
    ANNUAL_EXEMPT,
    _compute_tax_year,
)

Scenario = Literal["basic", "higher"]
GainCharacter = Literal["capital_loss", "nil", "offshore_income_gain", "oig_or_capital_unresolved"]

_ZERO: Final = Decimal(0)
_CENT: Final = Decimal("0.01")


class YearUnsupported(ValueError):
    """A disposal falls in a year outside ``_CGT_RATE_PERIODS``; the reserve reports it as unavailable."""

    def __init__(self, years: Sequence[str]) -> None:
        self.years = tuple(sorted(set(years)))
        super().__init__(f"tax years outside the CGT rate table: {', '.join(self.years)}")


@contextmanager
def _exact() -> Iterator[None]:
    with decimal.localcontext() as ctx:
        ctx.prec = 80
        ctx.traps[decimal.Inexact] = True
        ctx.traps[decimal.Rounded] = True
        yield


# ---------------------------------------------------------------------------
# Rows read from the effective run (part B fills these)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ShareDisposal:
    """A ``tax_statutory_disposals`` row: one security on one UK day (s105(1)(a))."""

    tax_security_id: int
    uk_date: date
    tax_year: str
    status: Literal["computed", "amount_missing"]
    gain_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class FundDisposal:
    """A ``tax_fund_disposals`` row: one disposal of a non-reporting fund (reg 42)."""

    tax_security_id: int
    seq: int
    lot_key: str
    uk_date: date
    tax_year: str
    status: Literal["computed", "amount_missing"]
    gain_character: GainCharacter | None
    basic_gain_gbp: Decimal | None
    offshore_income_gain_gbp: Decimal | None
    chargeable_gain_gbp: Decimal | None
    missing_lot_keys: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class WithheldSecurity:
    tax_security_id: int
    symbols: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class EvidenceSubject:
    """An evidence subject outside identification: ``unresolved`` or ``excluded_non_share_product`` (3c)."""

    subject: str
    outcome: Literal["unresolved", "excluded_non_share_product"]
    product: str | None
    reasons: tuple[str, ...]
    first_uk_date: date | None
    last_uk_date: date | None


@dataclass(frozen=True)
class FundSecurity:
    """A security whose character is ``non_reporting_fund``."""

    tax_security_id: int
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class RunRows:
    share_disposals: tuple[ShareDisposal, ...]
    fund_disposals: tuple[FundDisposal, ...]
    withheld: tuple[WithheldSecurity, ...]
    subjects: tuple[EvidenceSubject, ...]
    non_reporting_funds: tuple[FundSecurity, ...]


# ---------------------------------------------------------------------------
# Years
# ---------------------------------------------------------------------------


def _start_year(tax_year: str) -> int:
    return int(tax_year.split("/")[0])


def supported_tax_years(current_year: str) -> tuple[str, ...]:
    """Every year from the rate table's first to ``current_year``, newest first (Design 6 selector)."""
    return tuple(
        _compute_tax_year(date(start, 4, 6))
        for start in range(_start_year(current_year), _MIN_SUPPORTED_TAX_YEAR_START - 1, -1)
    )


def _rate_period(uk_date: date) -> int:
    for index, (from_d, to_d, _basic, _higher) in enumerate(_CGT_RATE_PERIODS):
        if from_d <= uk_date <= to_d:
            return index
    raise YearUnsupported([_compute_tax_year(uk_date)])


def _rate(period: int, scenario: Scenario) -> Decimal:
    _from_d, _to_d, basic, higher = _CGT_RATE_PERIODS[period]
    return basic if scenario == "basic" else higher


# ---------------------------------------------------------------------------
# Design 2: the capital-gains computation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Member:
    """One statutory or fund disposal's signed gain in the computation."""

    amount_gbp: Decimal
    uk_date: date


@dataclass(frozen=True)
class Reading:
    gains: Decimal
    losses: Decimal
    net_current: Decimal
    current_losses_carried_forward: Decimal
    aea: Decimal
    aea_used: Decimal
    aea_remaining: Decimal
    brought_forward_opening: Decimal
    brought_forward_used: Decimal
    brought_forward_remaining: Decimal
    losses_carried_forward: Decimal
    taxable: Decimal
    tax_basic: Decimal
    tax_higher: Decimal


def _scenario_tax(gains_by_period: dict[int, Decimal], deduction: Decimal, scenario: Scenario) -> Decimal:
    """Deduct from the highest-rate gains first (s1F, s1K(5); assumption L6), then charge what remains."""
    tax = _ZERO
    left = deduction
    for period in sorted(gains_by_period, key=lambda p: (_rate(p, scenario), p), reverse=True):
        taken = min(left, gains_by_period[period])
        left -= taken
        tax += (gains_by_period[period] - taken) * _rate(period, scenario)
    return tax


def compute_reading(members: Iterable[Member], brought_forward_opening: Decimal = _ZERO) -> Reading:
    """CG21500 and TCGA s1K(4): current losses in full, then the AEA, then brought-forward losses down to it."""
    if brought_forward_opening < 0:
        raise ValueError("brought_forward_opening must be >= 0")
    with _exact():
        gains_by_period: dict[int, Decimal] = {}
        gains = losses = _ZERO
        for member in members:
            if member.amount_gbp > 0:
                period = _rate_period(member.uk_date)
                gains_by_period[period] = gains_by_period.get(period, _ZERO) + member.amount_gbp
                gains += member.amount_gbp
            elif member.amount_gbp < 0:
                losses -= member.amount_gbp
        net_current = max(gains - losses, _ZERO)
        current_carried = max(losses - gains, _ZERO)
        aea = ANNUAL_EXEMPT
        aea_used = min(aea, net_current)
        bf_used = min(brought_forward_opening, net_current - aea_used)
        bf_remaining = brought_forward_opening - bf_used
        taxable = net_current - aea_used - bf_used
        deduction = min(losses, gains) + aea_used + bf_used
        return Reading(
            gains=gains,
            losses=losses,
            net_current=net_current,
            current_losses_carried_forward=current_carried,
            aea=aea,
            aea_used=aea_used,
            aea_remaining=aea - aea_used,
            brought_forward_opening=brought_forward_opening,
            brought_forward_used=bf_used,
            brought_forward_remaining=bf_remaining,
            losses_carried_forward=current_carried + bf_remaining,
            taxable=taxable,
            tax_basic=_scenario_tax(gains_by_period, deduction, "basic"),
            tax_higher=_scenario_tax(gains_by_period, deduction, "higher"),
        )


# ---------------------------------------------------------------------------
# Design 4: coverage flags
# ---------------------------------------------------------------------------

FlagCode = Literal[
    "securities_withheld",
    "amounts_missing",
    "subjects_unresolved",
    "products_excluded",
    "dividends_not_computed",
    "fund_income_not_computed",
    "income_tax_not_computed",
    "account_only",
]


@dataclass(frozen=True)
class FlagRow:
    ref: str
    detail: tuple[str, ...]
    # The tax years the row can affect; None means every year (its effect cannot be dated).
    years: tuple[str, ...] | None


@dataclass(frozen=True)
class Flag:
    code: FlagCode
    omission: bool
    rows: tuple[FlagRow, ...]

    @property
    def count(self) -> int:
        return len(self.rows)


def _subject_years(subject: EvidenceSubject, supported: tuple[str, ...]) -> tuple[str, ...] | None:
    """Design 4's year scope of a subject, by the UK dates of its first and last events."""
    first = _start_year(_compute_tax_year(subject.first_uk_date)) if subject.first_uk_date else None
    last = _start_year(_compute_tax_year(subject.last_uk_date)) if subject.last_uk_date else None
    if first is None and last is None:
        return None
    lo = first if first is not None else -1
    hi = last if last is not None else 10**6
    return tuple(year for year in supported if lo <= _start_year(year) <= hi)


def _ref(tax_security_id: int, symbols: tuple[str, ...]) -> str:
    return f"{tax_security_id}:{'/'.join(symbols)}"


def run_flags(rows: RunRows, current_year: str) -> tuple[Flag, ...]:
    """Every flag of the run, each row carrying its year scope. Flags with no rows are omitted, except the
    always-raised ones."""
    supported = supported_tax_years(current_year)
    flags: list[Flag] = []
    if rows.withheld:
        flags.append(
            Flag(
                "securities_withheld",
                True,
                tuple(FlagRow(_ref(w.tax_security_id, w.symbols), w.reasons, None) for w in rows.withheld),
            )
        )
    missing = [
        FlagRow(f"{d.tax_security_id}:{d.uk_date.isoformat()}", d.missing_lot_keys, (d.tax_year,))
        for d in rows.share_disposals
        if d.status == "amount_missing"
    ] + [
        FlagRow(f"{d.tax_security_id}:{d.seq}", d.missing_lot_keys, (d.tax_year,))
        for d in rows.fund_disposals
        if d.status == "amount_missing"
    ]
    if missing:
        flags.append(Flag("amounts_missing", True, tuple(missing)))
    subject_codes: tuple[tuple[FlagCode, str], ...] = (
        ("subjects_unresolved", "unresolved"),
        ("products_excluded", "excluded_non_share_product"),
    )
    for code, outcome in subject_codes:
        subject_rows = tuple(
            FlagRow(s.subject, (s.product or "", *s.reasons), _subject_years(s, supported))
            for s in rows.subjects
            if s.outcome == outcome
        )
        if subject_rows:
            flags.append(Flag(code, True, subject_rows))
    flags.append(Flag("dividends_not_computed", True, ()))
    if rows.non_reporting_funds:
        flags.append(
            Flag(
                "fund_income_not_computed",
                True,
                tuple(FlagRow(_ref(f.tax_security_id, f.symbols), (), None) for f in rows.non_reporting_funds),
            )
        )
    income = tuple(
        FlagRow(f"{d.tax_security_id}:{d.seq}", (d.gain_character or "",), (d.tax_year,))
        for d in rows.fund_disposals
        if d.gain_character in ("offshore_income_gain", "oig_or_capital_unresolved")
    )
    if income:
        flags.append(Flag("income_tax_not_computed", True, income))
    flags.append(Flag("account_only", False, ()))
    return tuple(flags)


_ALWAYS_RAISED: Final = frozenset({"dividends_not_computed", "account_only"})


def flags_for_year(flags: Iterable[Flag], tax_year: str) -> tuple[Flag, ...]:
    """The run's flags whose rows can affect ``tax_year``."""
    out: list[Flag] = []
    for flag in flags:
        if flag.code in _ALWAYS_RAISED:
            out.append(flag)
            continue
        rows = tuple(r for r in flag.rows if r.years is None or tax_year in r.years)
        if rows:
            out.append(Flag(flag.code, flag.omission, rows))
    return tuple(out)


def is_complete(flags: Iterable[Flag]) -> bool:
    return not any(flag.omission for flag in flags)


# ---------------------------------------------------------------------------
# Designs 2, 3 and 5: one year's view
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class YearView:
    tax_year: str
    # The reading without any unresolved gain; the only reading when the year has none.
    income_reading: Reading
    # With every unresolved gain as a chargeable gain; None when the year has no unresolved disposal.
    capital_reading: Reading | None
    offshore_income_gains: Decimal
    unresolved_gains: Decimal
    flags: tuple[Flag, ...]
    complete: bool
    # Every figure rests on open assumptions L1–L7, so it is provisional (Design 5).
    figure_class: Literal["provisional"]
    reasons: tuple[str, ...]
    assumption_ids: tuple[str, ...]


def _share_members(rows: RunRows, tax_year: str) -> list[Member]:
    return [
        Member(d.gain_gbp, d.uk_date)
        for d in rows.share_disposals
        if d.tax_year == tax_year and d.status == "computed" and d.gain_gbp is not None
    ]


def _fund_capital_members(rows: RunRows, tax_year: str) -> list[Member]:
    return [
        Member(d.chargeable_gain_gbp, d.uk_date)
        for d in rows.fund_disposals
        if d.tax_year == tax_year
        and d.status == "computed"
        and d.gain_character == "capital_loss"
        and d.chargeable_gain_gbp is not None
    ]


def _fund_sum(rows: RunRows, tax_year: str, character: GainCharacter) -> tuple[Decimal, list[Member]]:
    picked = [
        d
        for d in rows.fund_disposals
        if d.tax_year == tax_year and d.status == "computed" and d.gain_character == character
    ]
    with _exact():
        if character == "offshore_income_gain":
            amounts = [d.offshore_income_gain_gbp for d in picked]
        else:
            amounts = [d.basic_gain_gbp for d in picked]
        if any(a is None for a in amounts):
            raise ValueError(f"a computed {character} disposal has no amount")
        total = sum((a for a in amounts if a is not None), _ZERO)
    members = [Member(d.basic_gain_gbp, d.uk_date) for d in picked if d.basic_gain_gbp is not None]
    return total, members


def year_view(
    rows: RunRows,
    tax_year: str,
    current_year: str,
    brought_forward_opening: Decimal = _ZERO,
) -> YearView:
    capital = _share_members(rows, tax_year) + _fund_capital_members(rows, tax_year)
    oig, _ = _fund_sum(rows, tax_year, "offshore_income_gain")
    unresolved, unresolved_members = _fund_sum(rows, tax_year, "oig_or_capital_unresolved")
    income_reading = compute_reading(capital, brought_forward_opening)
    capital_reading = (
        compute_reading(capital + unresolved_members, brought_forward_opening) if unresolved_members else None
    )
    flags = flags_for_year(run_flags(rows, current_year), tax_year)
    reasons = sorted(
        {r for d in rows.share_disposals if d.tax_year == tax_year for r in d.reasons}
        | {r for d in rows.fund_disposals if d.tax_year == tax_year for r in d.reasons}
    )
    return YearView(
        tax_year=tax_year,
        income_reading=income_reading,
        capital_reading=capital_reading,
        offshore_income_gains=oig,
        unresolved_gains=unresolved,
        flags=flags,
        complete=is_complete(flags),
        figure_class="provisional",
        reasons=tuple(reasons),
        assumption_ids=assumption_ids(reasons),
    )


# ---------------------------------------------------------------------------
# Design 7: the reserve's arithmetic
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReserveYear:
    tax_year: str
    # The scenario's CGT under the income reading.
    cgt_gbp: Decimal
    # Offshore income gains and unresolved gains, reserved whole: a provision, not a computed income tax.
    income_buffer_gbp: Decimal
    total_gbp: Decimal
    complete: bool


@dataclass(frozen=True)
class ReserveFigures:
    status: Literal["available", "incomplete"]
    scenario: Scenario
    years: tuple[ReserveYear, ...]
    total_gbp: Decimal
    # Run-level: every flag of the run with its year scope, so an omission outside a reserved year is carried.
    flags: tuple[Flag, ...]
    figure_class: Literal["provisional"]
    reasons: tuple[str, ...]
    assumption_ids: tuple[str, ...]


def reserved_years(rows: RunRows) -> tuple[str, ...]:
    """Every year with a disposal in the run; no year is released on a payment deadline (Design 7)."""
    return tuple(sorted({d.tax_year for d in rows.share_disposals} | {d.tax_year for d in rows.fund_disposals}))


def reserve_figures(rows: RunRows, scenario: Scenario, current_year: str) -> ReserveFigures:
    """Raises ``YearUnsupported`` naming every reserved year outside the rate table."""
    years = reserved_years(rows)
    supported = set(supported_tax_years(current_year))
    outside = [y for y in years if y not in supported]
    if outside:
        raise YearUnsupported(outside)
    run_level = run_flags(rows, current_year)
    out: list[ReserveYear] = []
    for tax_year in years:
        view = year_view(rows, tax_year, current_year)
        with _exact():
            cgt = view.income_reading.tax_basic if scenario == "basic" else view.income_reading.tax_higher
            buffer = view.offshore_income_gains + view.unresolved_gains
            out.append(ReserveYear(tax_year, cgt, buffer, cgt + buffer, view.complete))
    with _exact():
        total = sum((y.total_gbp for y in out), _ZERO)
    reasons = sorted(
        {r for d in rows.share_disposals for r in d.reasons} | {r for d in rows.fund_disposals for r in d.reasons}
    )
    # Unreachable today: `dividends_not_computed` is always an omission (spec Design 7, "Under the current policies the
    # status is always `incomplete`"). `available` exists so that evidence alone can make it reachable.
    return ReserveFigures(
        status="available" if is_complete(run_level) else "incomplete",
        scenario=scenario,
        years=tuple(out),
        total_gbp=total,
        flags=run_level,
        figure_class="provisional",
        reasons=tuple(reasons),
        assumption_ids=assumption_ids(reasons),
    )


def to_usd_ceiling(gbp: Decimal, gbp_usd_rate: Decimal) -> Decimal:
    """The reserve in USD, rounded up to the cent so rounding never understates it (Design 7)."""
    with decimal.localcontext() as ctx:
        ctx.prec = 80
        ctx.traps[decimal.Inexact] = True
        product = gbp * gbp_usd_rate
        ctx.traps[decimal.Inexact] = False
        return product.quantize(_CENT, rounding=decimal.ROUND_CEILING)
