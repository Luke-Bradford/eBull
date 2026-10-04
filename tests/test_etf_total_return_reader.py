"""#3619 slice 2b — the ETF total-return rules, pure."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.etf_total_return_reader import (
    ETORO_SOURCE,
    NPORT_SOURCE,
    EtfVerdict,
    NportFiling,
    NportMonth,
    SymbolInputs,
    assemble_symbol,
    identity_gate,
    resolve_nport_months,
)
from app.services.total_return_reader import INTRADER_VENDOR, Month, MonthEnd, PeriodReturn, add_months


def _month_end_bar(month: Month) -> date:
    """A fixed weekday-agnostic stand-in: the 28th is a bar in every month."""
    return date(month[0], month[1], 28)


def _levels(first: Month, last: Month, growth: float = 0.01) -> dict[Month, MonthEnd]:
    """Month-ends compounding at ``growth`` on adj_close and ``growth / 2`` on close (half is distributions)."""
    out: dict[Month, MonthEnd] = {}
    adj = close = 100.0
    month = first
    while month <= last:
        out[month] = MonthEnd(_month_end_bar(month), adj, close)
        adj *= 1 + growth
        close *= 1 + growth / 2
        month = add_months(month, 1)
    return out


def _nport(first: Month, last: Month, value: float = 0.01) -> dict[Month, NportMonth]:
    out: dict[Month, NportMonth] = {}
    month = first
    while month <= last:
        out[month] = NportMonth(value, f"0000000000-{month[0] % 100:02d}-{month[1]:06d}")
        month = add_months(month, 1)
    return out


def _inputs(symbol: str = "TLT", **overrides: object) -> SymbolInputs:
    base: dict[str, object] = {
        "symbol": symbol,
        "intrader_series_id": 7,
        "intrader": _levels((2019, 1), (2024, 9)),
        "intrader_last_raw_bars": {(2021, 12): date(2021, 12, 28), (2024, 8): date(2024, 8, 28)},
        "reference_symbol": None,
        "nport_classes": ["C000012090"],
        "nport": _nport((2019, 10), (2026, 3)),
        "etoro_instrument_ids": [],
        "etoro": {},
    }
    base.update(overrides)
    return SymbolInputs(**base)  # type: ignore[arg-type]


def _filing(month: Month, pct: str, filed: date, accession: str) -> NportFiling:
    return NportFiling(month, Decimal(pct), filed, accession)


# --- resolve_nport_months ---------------------------------------------------------------------------


def test_a_filed_percent_becomes_a_decimal_return_once() -> None:
    resolved = resolve_nport_months([_filing((2024, 9), "2.5", date(2024, 11, 20), "0000000001-24-000001")])
    assert resolved[(2024, 9)].value == 0.025


def test_the_latest_filing_date_wins_whatever_the_accession_order() -> None:
    rows = [
        _filing((2024, 9), "1.0", date(2024, 11, 20), "0009999999-24-999999"),
        _filing((2024, 9), "1.2", date(2025, 1, 10), "0000000001-25-000001"),
    ]
    assert resolve_nport_months(rows)[(2024, 9)] == NportMonth(0.012, "0000000001-25-000001")
    assert resolve_nport_months(reversed(rows)) == resolve_nport_months(rows)


def test_two_values_on_the_latest_filing_date_leave_the_month_absent() -> None:
    day = date(2024, 11, 20)
    conflicting = [
        _filing((2024, 9), "1.0", day, "0000000001-24-000001"),
        _filing((2024, 9), "1.1", day, "0000000002-24-000001"),
        _filing((2024, 9), "0.5", date(2024, 10, 1), "0000000003-24-000001"),
    ]
    assert resolve_nport_months(conflicting) == {}
    agreeing = [
        _filing((2024, 9), "1.0", day, "0000000001-24-000001"),
        _filing((2024, 9), "1.00", day, "0000000002-24-000001"),
    ]
    assert resolve_nport_months(agreeing)[(2024, 9)].value == 0.01


# --- identity_gate ---------------------------------------------------------------------------------


def _returns(months: int, value: float) -> dict[Month, PeriodReturn]:
    return {(2020, m + 1): PeriodReturn(value, date(2020, 1, 1), date(2020, 1, 2)) for m in range(months)}


def test_identity_needs_twelve_paired_months_and_a_median_below_the_threshold() -> None:
    other = {(2020, m + 1): 0.01 for m in range(12)}
    too_few = identity_gate(_returns(11, 0.01), other, max_median=0.005)
    assert (too_few.paired_months, too_few.median_abs_diff, too_few.passed) == (11, None, False)
    assert identity_gate(_returns(12, 0.012), other, max_median=0.005).passed
    assert not identity_gate(_returns(12, 0.02), other, max_median=0.005).passed


# --- assemble_symbol: N-PORT ----------------------------------------------------------------------


def test_nport_supplies_every_month_from_2022_and_intrader_every_month_before() -> None:
    verdict, identity, rows = assemble_symbol(_inputs(), include_price_return=False)
    assert verdict is EtfVerdict.NPORT
    assert identity is not None and identity.passed
    sources = {(r.month < date(2022, 1, 1), r.source) for r in rows}
    assert sources == {(True, INTRADER_VENDOR), (False, NPORT_SOURCE)}
    assert max(r.month for r in rows if r.source == INTRADER_VENDOR) == date(2021, 12, 1)
    assert min(r.month for r in rows if r.source == NPORT_SOURCE) == date(2022, 1, 1)
    assert not any(r.dividend_capture_degraded or r.price_return_only for r in rows)


def test_identity_is_measured_before_the_switch_where_intrader_still_carries_distributions() -> None:
    # From 2022 Intrader's adj_close drops half the return (its distributions); before, it matches N-PORT.
    intrader = _levels((2019, 1), (2021, 12))
    adj = intrader[(2021, 12)].adj_close
    for k in range(33):  # 2022-01 .. 2024-09
        month = add_months((2022, 1), k)
        adj *= 1.005
        intrader[month] = MonthEnd(_month_end_bar(month), adj, adj)
    verdict, identity, _rows = assemble_symbol(_inputs(intrader=intrader), include_price_return=False)
    assert verdict is EtfVerdict.NPORT
    assert identity is not None and identity.median_abs_diff is not None and identity.median_abs_diff < 1e-12


def test_a_different_fund_is_refused_and_keeps_intrader_with_degraded_flags() -> None:
    verdict, _identity, rows = assemble_symbol(
        _inputs(nport=_nport((2019, 10), (2026, 3), 0.03)), include_price_return=False
    )
    assert verdict is EtfVerdict.REFUSED_IDENTITY
    assert {r.source for r in rows} == {INTRADER_VENDOR}
    assert max(r.month for r in rows) == date(2024, 8, 1)
    assert all(r.dividend_capture_degraded == (r.month >= date(2022, 1, 1)) for r in rows)


def test_a_december_month_end_short_of_the_last_raw_bar_is_a_misaligned_join() -> None:
    verdict, _identity, rows = assemble_symbol(
        _inputs(intrader_last_raw_bars={(2021, 12): date(2021, 12, 31)}), include_price_return=False
    )
    assert verdict is EtfVerdict.JOIN_MISALIGNED
    assert {r.source for r in rows} == {INTRADER_VENDOR}


def test_a_proxy_is_labelled_on_every_nport_row() -> None:
    verdict, _identity, rows = assemble_symbol(
        _inputs(symbol="SPY", reference_symbol="IVV"), include_price_return=False
    )
    assert verdict is EtfVerdict.NPORT_PROXY
    assert {r.reference_symbol for r in rows if r.source == NPORT_SOURCE} == {"IVV"}


def test_two_classes_under_one_symbol_extend_nothing() -> None:
    verdict, _identity, rows = assemble_symbol(_inputs(nport_classes=["C1", "C2"]), include_price_return=False)
    assert verdict is EtfVerdict.AMBIGUOUS_REFERENCE
    assert {r.source for r in rows} == {INTRADER_VENDOR}


def test_no_class_and_not_a_declared_pool_is_unresolved_not_price_return() -> None:
    verdict, _identity, rows = assemble_symbol(
        _inputs(nport_classes=[], nport={}, etoro_instrument_ids=[3], etoro=_levels((2022, 6), (2026, 9))),
        include_price_return=True,
    )
    assert verdict is EtfVerdict.UNRESOLVED_REFERENCE
    assert {r.source for r in rows} == {INTRADER_VENDOR}


# --- assemble_symbol: declared pools on eToro -----------------------------------------------------


def _pool(**overrides: object) -> SymbolInputs:
    intrader = {m: MonthEnd(e.bar_date, e.close, e.close) for m, e in _levels((2019, 1), (2024, 9)).items()}
    etoro = {m: MonthEnd(e.bar_date, e.close, e.close) for m, e in _levels((2022, 6), (2026, 10)).items()}
    base: dict[str, object] = {
        "symbol": "GLD",
        "intrader": intrader,
        "nport_classes": [],
        "nport": {},
        "etoro_instrument_ids": [3025],
        "etoro": etoro,
    }
    base.update(overrides)
    return _inputs(**base)  # type: ignore[arg-type]


def test_a_pool_extends_on_etoro_only_when_the_caller_opts_in() -> None:
    verdict, _identity, rows = assemble_symbol(_pool(), include_price_return=False)
    assert verdict is EtfVerdict.PRICE_RETURN_EXCLUDED
    assert max(r.month for r in rows) == date(2024, 8, 1)

    verdict, identity, rows = assemble_symbol(_pool(), include_price_return=True)
    assert verdict is EtfVerdict.PRICE_RETURN_ONLY
    assert identity is not None and identity.passed
    extension = [r for r in rows if r.source == ETORO_SOURCE]
    assert min(r.month for r in extension) == date(2024, 9, 1)
    assert all(r.price_return_only for r in extension)
    assert not any(r.dividend_capture_degraded for r in rows)


def test_the_instruments_final_month_is_partial_and_dropped() -> None:
    _verdict, _identity, rows = assemble_symbol(_pool(), include_price_return=True)
    assert max(r.month for r in rows) == date(2026, 9, 1)  # eToro's last bar is in 2026-10


def test_a_final_month_that_reaches_its_last_weekday_is_kept() -> None:
    etoro = dict(_pool().etoro)
    del etoro[(2026, 10)]
    etoro[(2026, 9)] = MonthEnd(date(2026, 9, 30), etoro[(2026, 9)].adj_close, etoro[(2026, 9)].close)  # a Wednesday
    _verdict, _identity, rows = assemble_symbol(_pool(etoro=etoro), include_price_return=True)
    assert max(r.month for r in rows) == date(2026, 9, 1)


def test_an_etoro_august_anchor_on_another_day_is_a_misaligned_join() -> None:
    inputs = _pool()
    etoro = dict(inputs.etoro)
    etoro[(2024, 8)] = MonthEnd(date(2024, 8, 27), etoro[(2024, 8)].adj_close, etoro[(2024, 8)].close)
    verdict, _identity, _rows = assemble_symbol(_pool(etoro=etoro), include_price_return=True)
    assert verdict is EtfVerdict.JOIN_MISALIGNED


def test_no_etoro_return_spans_an_unresolved_scale_break() -> None:
    _verdict, _identity, rows = assemble_symbol(_pool(etoro_breaks=(date(2025, 3, 10),)), include_price_return=True)
    months = {r.month for r in rows if r.source == ETORO_SOURCE}
    assert date(2025, 3, 1) not in months  # (2025-02-28, 2025-03-28] holds the break
    assert {date(2025, 2, 1), date(2025, 4, 1)} <= months


def test_without_an_intrader_series_there_is_nothing() -> None:
    assert assemble_symbol(_inputs(intrader_series_id=None), include_price_return=True)[0] is (
        EtfVerdict.NO_INTRADER_SERIES
    )
