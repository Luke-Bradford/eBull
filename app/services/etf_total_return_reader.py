"""#3619 slice 2b — monthly ETF total return: Intrader, then N-PORT Item B.5.a from 2022-01.

Spec: ``docs/proposals/etl/2026-10-04-3619-total-return-splice.md`` §"Slice 2b". ETFs are outside the
stock reader's ``survivorship_free`` selection, so this is its own reader with its own version; it reuses
that reader's month-end and return rules unchanged.

Source rule: Form N-PORT Item B.5.a monthly total returns per class (Item B.5.b), computed under Form N-1A
Item 26(b)(1) — NAV-based, distributions reinvested — stored by ``scripts/load_3619_nport_returns.py``.
Intrader's ETF ``adj_close`` loses distributions from 2022 as its stocks do (HYG carries 12 stamps a year
to 2021 and 7 in 2022), so a fund with an N-PORT class switches at the stock splice's ``SWITCH_MONTH``. From
then the basis is the fund's NAV, not its market close: the two differ by the change in premium/discount
every month, not only at the join. UITs and commodity pools file no N-PORT:
SPY and QQQ read a registered fund tracking the same index (declared proxies), and the declared commodity
pools (no distributions to lose) keep Intrader through 2024-08 and then read eToro ``price_daily`` closes,
price return only, and only when the caller opts in. Every other
rule here (the proxies, the pool list, the identity gate, the ambiguity refusals) is fixed by construction
and frozen by this module's source hash.

⚠ Returns chain, levels never splice: Intrader's last month ends at its own month-end and the extension
source's first month starts from its own. The join is checked, not assumed.
"""

from __future__ import annotations

import hashlib
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

import psycopg

from app.services.cik_coverage_audit import _MF_DIRECTORY_FRESHNESS_DAYS as MF_DIRECTORY_FRESHNESS_DAYS
from app.services.price_masked_bars import load_masked_bars
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.price_segments import load_unresolved_breaks
from app.services.total_return_reader import (
    IDENTITY_MIN_MONTHS,
    INTRADER_VENDOR,
    LAST_SURVIVORSHIP_FREE_MONTH,
    SWITCH_MONTH,
    TOTAL_RETURN_SPLICE_VERSION,
    IdentityCheck,
    Month,
    MonthEnd,
    PeriodReturn,
    add_months,
    load_month_ends,
    month_of,
    monthly_returns,
)

_RULE_ID: Final = "etf-total-return-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


ETF_TOTAL_RETURN_VERSION: Final[str] = f"{_RULE_ID}+{_code_hash()}"

NPORT_SOURCE: Final = "sec_nport"
ETORO_SOURCE: Final = "etoro_price_daily"

#: First month eToro supplies a declared pool; every earlier month is Intrader's.
FIRST_POOL_EXTENSION_MONTH: Final[Month] = add_months(LAST_SURVIVORSHIP_FREE_MONTH, 1)

#: Symbols whose fund files no N-PORT, read from a registered fund tracking the same index. Slice 1's
#: references: SPY is a UIT (S&P 500 → IVV); QQQ filed none before its 2025 conversion (Nasdaq-100 → QQQM),
#: and the proxy is kept after it so one series never switches fund mid-panel. The result is a synthetic
#: history: fees and tracking differ from the symbol's own.
PROXIES: Final[Mapping[str, str]] = {"SPY": "IVV", "QQQ": "QQQM"}

#: Commodity pools: trusts or partnerships registered under the 1933 Act, not 1940-Act funds, so no N-PORT.
#: Only these may extend on eToro closes; a symbol merely absent from the SEC fund directory is unresolved,
#: not a non-filer (an absent directory row can be a transient refresh miss, sql/149).
PRICE_RETURN_POOLS: Final[frozenset[str]] = frozenset({"GLD", "IAU", "SLV", "USO", "DBC"})

#: Identity gate between the Intrader series and its extension source: at least ``IDENTITY_MIN_MONTHS`` paired
#: months (the stock splice's floor) and a median |monthly return difference| below the source's threshold.
#: N-PORT is compared with Intrader ``adj_close`` before ``SWITCH_MONTH``, where Intrader still carries
#: distributions; eToro with Intrader ``close`` up to 2024-08 on returns over the same bars. Market close vs
#: NAV differs by the monthly premium change, so N-PORT's threshold is wider (spec §"Why these thresholds").
NPORT_IDENTITY_MAX_MEDIAN: Final = 0.0050
ETORO_IDENTITY_MAX_MEDIAN: Final = 0.0010


class EtfVerdict(StrEnum):
    """One per symbol, assigned in this precedence order."""

    NO_INTRADER_SERIES = "no_intrader_series"
    #: No N-PORT class, not a declared pool: Intrader months only.
    UNRESOLVED_REFERENCE = "unresolved_reference"
    #: More than one N-PORT class, or more than one eToro instrument, under the symbol: Intrader months only.
    AMBIGUOUS_REFERENCE = "ambiguous_reference"
    #: The extension source fails the identity gate on the overlap: Intrader months only.
    REFUSED_IDENTITY = "refused_identity"
    #: Intrader's August month-end is not its last August bar, or the eToro August anchor is on another day.
    JOIN_MISALIGNED = "join_misaligned"
    NPORT = "nport"
    NPORT_PROXY = "nport_proxy"
    #: A declared pool, extended on eToro closes: distributions excluded.
    PRICE_RETURN_ONLY = "price_return_only"
    #: A declared pool the caller did not opt into price return for: Intrader months only.
    PRICE_RETURN_EXCLUDED = "price_return_excluded"


@dataclass(frozen=True, slots=True)
class EtfMonthlyReturn:
    symbol: str
    #: First day of the calendar month the return is assigned to.
    month: date
    #: Decimal simple return: a filed N-PORT 2.5 (percent) is 0.025 here.
    total_return: float
    source: str
    #: Intrader ``series_id``, N-PORT ``class_id`` or eToro ``instrument_id``, as text.
    source_key: str
    #: The fund whose N-PORT return is used, when it is not ``symbol`` itself.
    reference_symbol: str | None
    #: The bars the return runs between; ``None`` for N-PORT, whose months are calendar months by rule.
    start_bar: date | None
    end_bar: date | None
    #: The N-PORT filing the value comes from.
    accession_number: str | None
    price_return_only: bool
    #: An Intrader month from ``SWITCH_MONTH`` on for a symbol that is not a declared pool: its distributions
    #: may be missing.
    dividend_capture_degraded: bool


@dataclass(frozen=True, slots=True)
class NportFiling:
    month: Month
    return_pct: Decimal
    filing_date: date
    accession_number: str


@dataclass(frozen=True, slots=True)
class NportMonth:
    #: Decimal simple return.
    value: float
    accession_number: str


@dataclass(frozen=True)
class EtfTotalReturnPanel:
    #: Treatment versions: this module, the stock reader whose helpers it reuses, the quarantine rule set.
    version: str
    #: Data state read: N-PORT row count and latest filing date. Links and prices can still move beneath it.
    nport_snapshot: str
    price_return_included: bool
    rows: tuple[EtfMonthlyReturn, ...]
    verdicts: Mapping[str, EtfVerdict]
    identity: Mapping[str, IdentityCheck]


# ---------------------------------------------------------------------------
# Pure rules
# ---------------------------------------------------------------------------


def resolve_nport_months(filings: Iterable[NportFiling]) -> dict[Month, NportMonth]:
    """One decimal return per month for one class: the value from its latest filing date.

    Filing date, not accession order, decides: accession numbers carry no cross-filer chronology. A month
    whose latest filing date carries two different values (two accessions that day, or one accession that
    repeats the class) is absent: no rule picks one. Blank cells were never stored, so a later filing that
    leaves a month blank does not erase an earlier value: none of those cases is an amendment, so a blank is an
    omission, not a retraction (census: ``scripts/report_3619_etf_total_return``; spec §"Resolution").
    """
    by_month: dict[Month, list[NportFiling]] = {}
    for f in filings:
        by_month.setdefault(f.month, []).append(f)
    out: dict[Month, NportMonth] = {}
    for month, rows in by_month.items():
        latest = max(r.filing_date for r in rows)
        winners = [r for r in rows if r.filing_date == latest]
        if len({r.return_pct for r in winners}) == 1:
            best = max(winners, key=lambda r: r.accession_number)
            out[month] = NportMonth(float(best.return_pct) / 100.0, best.accession_number)
    return out


def identity_gate(
    intrader: Mapping[Month, PeriodReturn], other: Mapping[Month, float], *, max_median: float
) -> IdentityCheck:
    """Do the Intrader series and the extension source describe the same fund, on the months both cover?

    The caller restricts ``intrader`` to the comparison window; every month present in both is paired.
    """
    diffs = [abs(ret.value - other[month]) for month, ret in intrader.items() if month in other]
    if len(diffs) < IDENTITY_MIN_MONTHS:
        return IdentityCheck(paired_months=len(diffs), median_abs_diff=None, passed=False)
    median = statistics.median(diffs)
    return IdentityCheck(paired_months=len(diffs), median_abs_diff=median, passed=median < max_median)


@dataclass(frozen=True)
class SymbolInputs:
    symbol: str
    intrader_series_id: int | None
    intrader: Mapping[Month, MonthEnd]
    #: The Intrader series' last raw bar (quarantined or not) per month, for the join months.
    intrader_last_raw_bars: Mapping[Month, date]
    reference_symbol: str | None
    nport_classes: Sequence[str]
    nport: Mapping[Month, NportMonth]
    etoro_instrument_ids: Sequence[int]
    etoro: Mapping[Month, MonthEnd]
    #: Unresolved ``price_series_break`` dates on the eToro instrument: no return may span one.
    etoro_breaks: Sequence[date] = ()


def _intrader_row(inputs: SymbolInputs, month: Month, ret: PeriodReturn) -> EtfMonthlyReturn:
    return EtfMonthlyReturn(
        symbol=inputs.symbol,
        month=date(*month, 1),
        total_return=ret.value,
        source=INTRADER_VENDOR,
        source_key=str(inputs.intrader_series_id),
        reference_symbol=None,
        start_bar=ret.start_bar,
        end_bar=ret.end_bar,
        accession_number=None,
        price_return_only=False,
        dividend_capture_degraded=month >= SWITCH_MONTH and inputs.symbol not in PRICE_RETURN_POOLS,
    )


def _joined(inputs: SymbolInputs, intrader: Mapping[Month, PeriodReturn], last_month: Month) -> bool:
    """Intrader's last month ends on its last raw bar of that month, so no day falls between the sources."""
    ret = intrader.get(last_month)
    return ret is not None and ret.end_bar == inputs.intrader_last_raw_bars.get(last_month)


def assemble_symbol(
    inputs: SymbolInputs, *, include_price_return: bool
) -> tuple[EtfVerdict, IdentityCheck | None, list[EtfMonthlyReturn]]:
    """Every month of one ETF from at most one source (spec §"Reader contract").

    A symbol that does not extend keeps every Intrader month through 2024-08.
    """
    if inputs.intrader_series_id is None:
        return EtfVerdict.NO_INTRADER_SERIES, None, []
    intrader = monthly_returns(inputs.intrader, field="adj_close", through=LAST_SURVIVORSHIP_FREE_MONTH)
    intrader_only = [_intrader_row(inputs, m, r) for m, r in sorted(intrader.items())]
    is_pool = inputs.symbol in PRICE_RETURN_POOLS
    if len(inputs.nport_classes) > 1 or (is_pool and len(inputs.etoro_instrument_ids) > 1):
        return EtfVerdict.AMBIGUOUS_REFERENCE, None, intrader_only
    if inputs.nport_classes:
        return _assemble_nport(inputs, intrader, intrader_only)
    if not (is_pool and inputs.etoro_instrument_ids):
        return EtfVerdict.UNRESOLVED_REFERENCE, None, intrader_only
    return _assemble_pool(inputs, intrader, intrader_only, include_price_return=include_price_return)


def _assemble_nport(
    inputs: SymbolInputs, intrader: Mapping[Month, PeriodReturn], intrader_only: list[EtfMonthlyReturn]
) -> tuple[EtfVerdict, IdentityCheck | None, list[EtfMonthlyReturn]]:
    """Intrader before ``SWITCH_MONTH``, N-PORT from it. A month N-PORT lacks is absent, never filled."""
    (class_id,) = inputs.nport_classes
    before = {m: r for m, r in intrader.items() if m < SWITCH_MONTH}
    nport = {m: v.value for m, v in inputs.nport.items()}
    identity = identity_gate(before, nport, max_median=NPORT_IDENTITY_MAX_MEDIAN)
    if not identity.passed:
        return EtfVerdict.REFUSED_IDENTITY, identity, intrader_only
    if not _joined(inputs, intrader, add_months(SWITCH_MONTH, -1)):
        return EtfVerdict.JOIN_MISALIGNED, identity, intrader_only
    rows = [_intrader_row(inputs, m, r) for m, r in sorted(before.items())]
    for month in sorted(m for m in inputs.nport if m >= SWITCH_MONTH):
        filed = inputs.nport[month]
        rows.append(
            EtfMonthlyReturn(
                symbol=inputs.symbol,
                month=date(*month, 1),
                total_return=filed.value,
                source=NPORT_SOURCE,
                source_key=class_id,
                reference_symbol=inputs.reference_symbol,
                start_bar=None,
                end_bar=None,
                accession_number=filed.accession_number,
                price_return_only=False,
                dividend_capture_degraded=False,
            )
        )
    verdict = EtfVerdict.NPORT if inputs.reference_symbol is None else EtfVerdict.NPORT_PROXY
    return verdict, identity, rows


def _last_weekday(month: Month) -> date:
    nxt = add_months(month, 1)
    day = date.fromordinal(date(nxt[0], nxt[1], 1).toordinal() - 1)
    while day.weekday() >= 5:
        day = date.fromordinal(day.toordinal() - 1)
    return day


def _last_complete_month(month_ends: Mapping[Month, MonthEnd]) -> Month:
    """The latest month whose last bar reaches the month's last weekday; the month after a feed's last bar is unread.

    A month ending on a holiday on its last weekday reads as partial and is dropped: an absent row, the safe error.
    """
    if not month_ends:
        return LAST_SURVIVORSHIP_FREE_MONTH
    last = max(month_ends)
    return last if month_ends[last].bar_date >= _last_weekday(last) else add_months(last, -1)


def _assemble_pool(
    inputs: SymbolInputs,
    intrader: Mapping[Month, PeriodReturn],
    intrader_only: list[EtfMonthlyReturn],
    *,
    include_price_return: bool,
) -> tuple[EtfVerdict, IdentityCheck | None, list[EtfMonthlyReturn]]:
    """Intrader through 2024-08, eToro closes after, for a declared pool on one eToro instrument."""
    (instrument_id,) = inputs.etoro_instrument_ids
    through = _last_complete_month(inputs.etoro)
    etoro = {
        m: r
        for m, r in monthly_returns(inputs.etoro, field="close", through=through).items()
        if not any(r.start_bar < b <= r.end_bar for b in inputs.etoro_breaks)
    }
    price_only = monthly_returns(inputs.intrader, field="close", through=LAST_SURVIVORSHIP_FREE_MONTH)
    same_bars = {
        m: r
        for m, r in price_only.items()
        if m in etoro and (etoro[m].start_bar, etoro[m].end_bar) == (r.start_bar, r.end_bar)
    }
    identity = identity_gate(same_bars, {m: r.value for m, r in etoro.items()}, max_median=ETORO_IDENTITY_MAX_MEDIAN)
    if not identity.passed:
        return EtfVerdict.REFUSED_IDENTITY, identity, intrader_only
    august = intrader.get(LAST_SURVIVORSHIP_FREE_MONTH)
    anchor = inputs.etoro.get(LAST_SURVIVORSHIP_FREE_MONTH)
    if (
        not _joined(inputs, intrader, LAST_SURVIVORSHIP_FREE_MONTH)
        or august is None
        or anchor is None
        or anchor.bar_date != august.end_bar
    ):
        return EtfVerdict.JOIN_MISALIGNED, identity, intrader_only
    if not include_price_return:
        return EtfVerdict.PRICE_RETURN_EXCLUDED, identity, intrader_only
    rows = list(intrader_only)
    for month, ret in sorted(etoro.items()):
        if month < FIRST_POOL_EXTENSION_MONTH:
            continue
        rows.append(
            EtfMonthlyReturn(
                symbol=inputs.symbol,
                month=date(*month, 1),
                total_return=ret.value,
                source=ETORO_SOURCE,
                source_key=str(instrument_id),
                reference_symbol=None,
                start_bar=ret.start_bar,
                end_bar=ret.end_bar,
                accession_number=None,
                price_return_only=True,
                dividend_capture_degraded=False,
            )
        )
    return EtfVerdict.PRICE_RETURN_ONLY, identity, rows


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_INTRADER_SERIES_SQL = """
SELECT vendor_symbol, series_id
FROM research_price_series
WHERE vendor = %(vendor)s AND vendor_symbol = ANY(%(symbols)s::text[])
"""

#: Every bar counts, quarantined or not: the join check asks whether the usable month-end IS the last bar.
_LAST_RAW_BARS_SQL = """
SELECT series_id, date_trunc('month', bar_date)::date, max(bar_date)
FROM research_price_daily
WHERE series_id = ANY(%(series_ids)s::bigint[])
  AND date_trunc('month', bar_date)::date = ANY(%(months)s::date[])
GROUP BY 1, 2
"""

#: ``cik_refresh_mf_directory`` keeps every class ever observed; ``daily_cik_refresh`` advances ``last_seen`` for
#: the classes still listed. A class not seen within the audit's freshness window of the latest refresh has been
#: dropped by SEC (``cik_coverage_audit``). Anchored on the latest refresh, not the wall clock, so a refresh
#: outage does not empty the map.
_CLASS_SQL = """
SELECT symbol, class_id
FROM cik_refresh_mf_directory
WHERE symbol = ANY(%(symbols)s::text[])
  AND last_seen >= (SELECT max(last_seen) FROM cik_refresh_mf_directory) - make_interval(days => %(fresh_days)s)
"""

_NPORT_SQL = """
SELECT class_id, month, return_pct, filing_date, accession_number
FROM sec_nport_monthly_returns
WHERE class_id = ANY(%(class_ids)s::text[])
"""

_NPORT_SNAPSHOT_SQL = "SELECT count(*), max(filing_date) FROM sec_nport_monthly_returns"

_INSTRUMENT_SQL = "SELECT symbol, instrument_id FROM instruments WHERE symbol = ANY(%(symbols)s::text[])"


def etoro_month_ends(conn: psycopg.Connection[Any], instrument_id: int) -> dict[Month, MonthEnd]:
    """Last usable close of each calendar month on eToro's ``price_daily``.

    ``price_daily`` is the execution-venue view — Bid-derived and unadjusted (market-data skill) — so it is read
    through ``price_masked_bars``: fail-closed outside the instrument's quarantine coverage, close masked where
    the bar's return is unusable. Unresolved scale breaks are the caller's (``SymbolInputs.etoro_breaks``).
    """
    masked = load_masked_bars(conn, instrument_id)
    out: dict[Month, MonthEnd] = {}
    for bar_date, row in zip(masked.series.dates, masked.series.rows, strict=True):
        close = row["close"]
        if close is not None and close > 0:
            out[month_of(bar_date)] = MonthEnd(bar_date, float(close), float(close))
    return out


def _group(pairs: Iterable[tuple[Any, Any]]) -> dict[str, list[Any]]:
    out: dict[str, list[Any]] = {}
    for key, value in pairs:
        out.setdefault(str(key), []).append(value)
    return out


def load_etf_total_return_panel(
    conn: psycopg.Connection[Any], symbols: Sequence[str], *, include_price_return: bool = False
) -> EtfTotalReturnPanel:
    """Monthly total returns for ``symbols``. Price-return extensions only when ``include_price_return``."""
    wanted = sorted(set(symbols))
    intrader = {
        str(s): int(sid)
        for s, sid in conn.execute(_INTRADER_SERIES_SQL, {"vendor": INTRADER_VENDOR, "symbols": wanted}).fetchall()
    }
    join_months = [date(*add_months(SWITCH_MONTH, -1), 1), date(*LAST_SURVIVORSHIP_FREE_MONTH, 1)]
    last_raw: dict[int, dict[Month, date]] = {}
    for sid, month, bar in conn.execute(
        _LAST_RAW_BARS_SQL, {"series_ids": list(intrader.values()), "months": join_months}
    ).fetchall():
        last_raw.setdefault(int(sid), {})[month_of(month)] = bar
    references = {s: PROXIES.get(s, s) for s in wanted}
    classes = _group(
        conn.execute(
            _CLASS_SQL,
            {"symbols": sorted(set(references.values())), "fresh_days": MF_DIRECTORY_FRESHNESS_DAYS},
        ).fetchall()
    )
    pools = sorted(s for s in wanted if s in PRICE_RETURN_POOLS)
    instruments = _group(conn.execute(_INSTRUMENT_SQL, {"symbols": pools}).fetchall())

    class_ids = sorted({c for cs in classes.values() for c in cs})
    filings = _group(
        (class_id, NportFiling(month_of(month), pct, filing_date, accession))
        for class_id, month, pct, filing_date, accession in conn.execute(
            _NPORT_SQL, {"class_ids": class_ids}
        ).fetchall()
    )
    count, latest_filing = conn.execute(_NPORT_SNAPSHOT_SQL).fetchone() or (0, None)

    instrument_ids = sorted({int(i) for ids in instruments.values() for i in ids})
    etoro = {instrument_id: etoro_month_ends(conn, instrument_id) for instrument_id in instrument_ids}
    breaks = load_unresolved_breaks(conn, instrument_ids)

    month_ends = load_month_ends(conn, list(intrader.values()))
    rows: list[EtfMonthlyReturn] = []
    verdicts: dict[str, EtfVerdict] = {}
    identity: dict[str, IdentityCheck] = {}
    for symbol in wanted:
        series_id = intrader.get(symbol)
        symbol_classes = classes.get(references[symbol], [])
        symbol_instruments = [int(i) for i in instruments.get(symbol, [])]
        inputs = SymbolInputs(
            symbol=symbol,
            intrader_series_id=series_id,
            intrader=month_ends.get(series_id, {}) if series_id is not None else {},
            intrader_last_raw_bars=last_raw.get(series_id, {}) if series_id is not None else {},
            reference_symbol=references[symbol] if references[symbol] != symbol else None,
            nport_classes=symbol_classes,
            nport=resolve_nport_months(filings.get(symbol_classes[0], [])) if len(symbol_classes) == 1 else {},
            etoro_instrument_ids=symbol_instruments,
            etoro=etoro.get(symbol_instruments[0], {}) if len(symbol_instruments) == 1 else {},
            etoro_breaks=breaks.get(symbol_instruments[0], ()) if len(symbol_instruments) == 1 else (),
        )
        verdict, check, symbol_rows = assemble_symbol(inputs, include_price_return=include_price_return)
        verdicts[symbol] = verdict
        if check is not None:
            identity[symbol] = check
        rows.extend(symbol_rows)
    return EtfTotalReturnPanel(
        version=f"{ETF_TOTAL_RETURN_VERSION}|{TOTAL_RETURN_SPLICE_VERSION}|{QUARANTINE_RULE_SET_VERSION}",
        nport_snapshot=f"rows={count};latest_filing={latest_filing}",
        price_return_included=include_price_return,
        rows=tuple(rows),
        verdicts=verdicts,
        identity=identity,
    )


__all__ = [
    "ETF_TOTAL_RETURN_VERSION",
    "ETORO_IDENTITY_MAX_MEDIAN",
    "ETORO_SOURCE",
    "FIRST_POOL_EXTENSION_MONTH",
    "NPORT_IDENTITY_MAX_MEDIAN",
    "NPORT_SOURCE",
    "PRICE_RETURN_POOLS",
    "PROXIES",
    "EtfMonthlyReturn",
    "EtfTotalReturnPanel",
    "EtfVerdict",
    "NportFiling",
    "NportMonth",
    "SymbolInputs",
    "assemble_symbol",
    "etoro_month_ends",
    "identity_gate",
    "load_etf_total_return_panel",
    "resolve_nport_months",
]
