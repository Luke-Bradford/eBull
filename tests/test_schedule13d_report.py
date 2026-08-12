from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal

from scripts.evaluate_2582_schedule13d_outcomes import Initial13GSourceEvent, PriceWindow, SourceEvent
from scripts.schedule13d_report import build_historical_falsification_report

_ENTRY_DATES = (
    date(2025, 2, 3),
    date(2025, 3, 3),
    date(2025, 8, 4),
    date(2025, 9, 2),
    date(2026, 2, 2),
    date(2026, 3, 2),
)


def _source(index: int) -> SourceEvent:
    entry = _ENTRY_DATES[index]
    return SourceEvent(
        accession_number=f"t-{index}",
        issuer_cik=f"issuer-{index}",
        instrument_id=100 + index,
        public_filing_date=entry - timedelta(days=1),
        maximum_percent_of_class=Decimal(5 + index),
        prior_active=False,
        prior_passive=False,
        same_public_date_peer=False,
        reporter_identity_complete=True,
        current_security_eligible=True,
        series_ids=(1000 + index,),
        series_adjustment_bases=("split_adjusted",),
    )


def _challenger(index: int, rule: str) -> Initial13GSourceEvent:
    entry = _ENTRY_DATES[index]
    return Initial13GSourceEvent(
        accession_number=f"c-{rule}-{index}",
        issuer_cik=f"challenger-{rule}-{index}",
        instrument_id=200 + index,
        public_filing_date=entry - timedelta(days=1),
        rule=rule,  # type: ignore[arg-type]
        raw_document_count=1,
        current_security_eligible=True,
        series_ids=(2000 + index,),
        series_adjustment_bases=("split_adjusted",),
    )


def _window(
    event: SourceEvent | Initial13GSourceEvent,
    net_return_pct: str,
    population: str,
) -> PriceWindow:
    entry = next(
        item
        for item in _ENTRY_DATES
        if item.month == event.public_filing_date.month and item.year == event.public_filing_date.year
    )
    exit_close = Decimal("10") * (Decimal(1) + (Decimal(net_return_pct) + Decimal("0.5")) / Decimal(100))
    return PriceWindow(
        event=event,
        entry_date=entry,
        exit_date=entry + timedelta(days=11),
        stock_bars_present=70,
        market_bars_present=70,
        positive_ohlcv_bars=70,
        positive_adjustment_bars=70,
        quarantine_covered_bars=70,
        return_usable=True,
        entry_open=Decimal("10"),
        entry_close=Decimal("10"),
        entry_adj_close=Decimal("10"),
        exit_close=exit_close,
        exit_adj_close=exit_close,
        trailing_median_dollar_volume=Decimal("25000000"),
        prior_20_stock_return_pct=Decimal("1"),
        prior_20_market_return_pct=Decimal("1"),
        holding_market_return_pct=Decimal("0.5"),
        population=population,  # type: ignore[arg-type]
    )


def test_report_matches_every_frozen_control_and_remains_inconclusive_when_underpowered() -> None:
    sources = tuple(_source(index) for index in range(6))
    treatment_returns = ("1.7", "1.9", "2.1", "2.3", "2.0", "2.2")
    primary = tuple(_window(event, value, "primary") for event, value in zip(sources, treatment_returns, strict=True))
    unfiltered = tuple(
        _window(event, value, "unfiltered") for event, value in zip(sources, treatment_returns, strict=True)
    )
    random = tuple(_window(event, "-0.2", "random") for event in sources)
    challenger_sources = tuple(
        _challenger(index, rule) for rule in ("1b", "1c", "both", "unknown") for index in range(6)
    )
    challenger_windows = tuple(_window(event, "-0.3", "13g") for event in challenger_sources)

    report = build_historical_falsification_report(
        source_events=sources,
        initial_13g_sources=challenger_sources,
        primary_windows=primary,
        unfiltered_windows=unfiltered,
        random_windows=random,
        initial_13g_windows=challenger_windows,
        sector_by_instrument={
            event.instrument_id: f"sector-{index}"
            for index, event in enumerate(sources)
            if event.instrument_id is not None
        },
    )

    assert report.decision == "inconclusive", [item for item in report.gates if item.state == "fail"]
    assert report.primary_eligible_count == 6
    assert [item.name for item in report.comparisons] == [
        "random_time",
        "initial_13g_1b",
        "initial_13g_1c",
        "initial_13g_both",
        "initial_13g_unknown",
    ]
    assert all(item.matched_count == 6 for item in report.comparisons)
    assert all(item.holm_adjusted_one_sided_p_value is not None for item in report.comparisons[:3])
    assert next(item for item in report.gates if item.name == "effective_sample_size_gte_785").state == ("inconclusive")
    assert report.as_dict()["decision"] == "inconclusive"
    json.dumps(report.as_dict(), allow_nan=False)


def test_report_fails_on_measured_adverse_result_even_when_power_is_insufficient() -> None:
    sources = tuple(_source(index) for index in range(6))
    primary = tuple(
        _window(event, value, "primary")
        for event, value in zip(sources, ("-2", "-1", "-3", "-2", "-1", "-3"), strict=True)
    )

    report = build_historical_falsification_report(
        source_events=sources,
        initial_13g_sources=(),
        primary_windows=primary,
        unfiltered_windows=(),
        random_windows=(),
        initial_13g_windows=(),
    )

    assert report.decision == "fail"
    assert (
        next(item for item in report.gates if item.name == "adverse_cost_clustered_lower_bound_gt_zero").state == "fail"
    )
