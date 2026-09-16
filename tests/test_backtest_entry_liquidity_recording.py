"""#3104 slice 7a — how ``_absorb`` gives each realised leg ONE liquidity verdict.

Separate from ``test_strategy_entry_liquidity.py`` because these exercise the
recording path in ``backtest_run`` rather than the pure arithmetic, and separate
from the existing ``backtest_run`` suites because those are DB-marked — the
``db`` marker is per MODULE, so one DB test in a file evicts every test there
from the fast tier.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.backtest_run import (
    _entry_liquidity_summary,
    _NamespaceBook,
    _record_entry_liquidity,
)
from app.services.indicator_series import BarSeries
from app.services.strategy_entry_liquidity import LOOKBACK_SESSIONS, archive_policy_for

START = date(2020, 1, 1)


def _series(count: int) -> BarSeries:
    dates = tuple(START + timedelta(days=offset) for offset in range(count))
    rows = tuple({"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 100} for _ in dates)
    return BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]


def _measured(series: BarSeries) -> tuple[tuple[float, ...], tuple[str | None, ...]]:
    """Every bar measured, so the test isolates the recording rule."""
    return tuple(1000.0 for _ in series.dates), tuple(None for _ in series.dates)


def _record(**overrides: object) -> _NamespaceBook:
    series = overrides.pop("series", None) or _series(LOOKBACK_SESSIONS + 1)
    assert isinstance(series, BarSeries)
    means, reasons = _measured(series)
    book = _NamespaceBook()
    kwargs: dict[str, object] = {
        "series": series,
        "signal_date": series.dates[-1],
        "exit_date": series.dates[-1],
        "name_key": 42,
        "eligible": True,
        "means": means,
        "reasons": reasons,
        "unresolved_breaks": (),
    }
    kwargs.update(overrides)
    _record_entry_liquidity(book, **kwargs)  # type: ignore[arg-type]
    return book


def test_an_eligible_measured_leg_is_recorded() -> None:
    book = _record()
    assert book.entry_close_volume == [1000.0]
    assert book.entry_liquidity_name_keys == [42]
    assert book.entry_liquidity_excluded == {}


def test_an_ineligible_basis_excludes_every_leg_and_does_not_raise() -> None:
    """``sql/305`` refuses a split-adjusted level for dollar-volume attribution.
    ⚠ Withholding must NOT raise: ``_ledger_evidence``'s ``ValueError`` path
    aborts the whole run, which is right for a broken ledger and wrong for a
    corpus whose basis simply does not support a diagnostic."""
    book = _record(eligible=False)
    assert book.entry_close_volume == []
    assert book.entry_liquidity_excluded == {"basis_ineligible": 1}


def test_a_negative_name_key_is_measured_and_flagged_not_excluded() -> None:
    """⚠⚠ ``-series_id`` is a survivorship-free series admitted without a live
    link (#2721 step 3), and ``price_series_break.instrument_id REFERENCES
    instruments`` — so that table describes the LIVE corpus and can hold no row
    for it. ``backtest_run`` already passes ``()`` for those keys everywhere
    else, which is how the leg came to be OPENED; excluding it here would apply
    a stricter standard than the position construction the diagnostic
    describes, on 17,707 of 22,879 survivorship-free series. Carried as a
    caveat instead."""
    book = _record(name_key=-77)
    assert book.entry_close_volume == [1000.0]
    assert book.entry_liquidity_excluded == {}

    book.returns.append(1.5)  # the realised leg this verdict belongs to
    summary = _entry_liquidity_summary(book, liquidity_policy=None)
    assert summary is not None
    assert summary.unlinked_series_leg_count == 1
    assert summary.measured_leg_count == 1


def test_a_spanned_break_excludes_and_outranks_a_measured_window() -> None:
    series = _series(LOOKBACK_SESSIONS + 1)
    inside = series.dates[-3]
    book = _record(series=series, unresolved_breaks=(inside,))
    assert book.entry_liquidity_excluded == {"scale_break_spanned": 1}


def test_window_short_outranks_the_break_check() -> None:
    """Frozen precedence: ``window_short`` is reason 2 and ``scale_break_spanned``
    is 3, so a short window is never reported as a break problem."""
    series = _series(LOOKBACK_SESSIONS + 1)
    means, _ = _measured(series)
    short = tuple("window_short" for _ in series.dates)
    book = _NamespaceBook()
    _record_entry_liquidity(
        book,
        series=series,
        signal_date=series.dates[-1],
        exit_date=series.dates[-1],
        name_key=-1,
        eligible=True,
        means=means,
        reasons=short,  # type: ignore[arg-type]
        unresolved_breaks=(series.dates[-3],),
    )
    assert book.entry_liquidity_excluded == {"window_short": 1}


def test_every_leg_gets_exactly_one_verdict() -> None:
    series = _series(LOOKBACK_SESSIONS + 1)
    means, reasons = _measured(series)
    book = _NamespaceBook()
    for index, name_key in enumerate((1, -2, 3, -4, 5)):
        _record_entry_liquidity(
            book,
            series=series,
            signal_date=series.dates[-1],
            exit_date=series.dates[-1],
            name_key=name_key,
            eligible=index != 4,
            means=means,
            reasons=reasons,
            unresolved_breaks=(),
        )
    measured = len(book.entry_close_volume)
    excluded = sum(book.entry_liquidity_excluded.values())
    assert measured + excluded == 5
    assert book.entry_liquidity_excluded == {"basis_ineligible": 1}


def test_an_ineligible_basis_is_still_REPORTED_not_erased() -> None:
    """⚠ Codex ckpt-2 P2. A ``survivor_only`` run resolves and verifies
    ``split_adjusted``; reporting ``None`` for it made that indistinguishable
    from a run whose provenance could not be resolved at all, since both also
    carry ``basis_ineligible``. Eligibility decides whether observations are
    MEASURED, not whether known provenance is REPORTED."""
    ineligible = archive_policy_for("paperswithbacktest/Stocks-Daily-Price")
    assert ineligible is not None and not ineligible.eligible

    book = _NamespaceBook()
    book.returns.append(1.0)
    book.entry_liquidity_excluded["basis_ineligible"] += 1

    known = _entry_liquidity_summary(book, liquidity_policy=ineligible)
    assert known is not None
    assert known.adjustment_basis == "split_adjusted", "a verified basis must survive being ineligible"

    unresolved = _entry_liquidity_summary(book, liquidity_policy=None)
    assert unresolved is not None
    assert unresolved.adjustment_basis is None
    assert known.adjustment_basis != unresolved.adjustment_basis, "the two states must stay distinguishable"


def test_a_book_that_never_recorded_summarises_to_none() -> None:
    """A hand-built harness book appends to ``returns`` without going through
    ``_absorb``, so it holds no verdicts. ⚠ The escape is narrow BY
    CONSTRUCTION: it needs the recorder to have run ZERO times against a
    non-empty ledger, which no production path can do — ``_absorb`` records in
    the same branch that appends the return."""
    book = _NamespaceBook()
    book.returns.append(1.5)
    assert _entry_liquidity_summary(book, liquidity_policy=None) is None


def test_a_partially_recorded_book_still_raises() -> None:
    """⚠ Some verdicts but not one per realised leg is a REAL BUG — a path
    appending a realised return without recording its verdict — and a partial
    accounting read as a complete one is what the invariant exists to catch."""
    book = _NamespaceBook()
    book.returns.extend([1.5, 2.5, 3.5])
    book.entry_liquidity_excluded["basis_ineligible"] += 1
    with pytest.raises(ValueError, match="every leg carries exactly one verdict"):
        _entry_liquidity_summary(book, liquidity_policy=None)


def test_the_recording_touches_no_existing_population() -> None:
    """⚠ The claim a reviewer most needs proved: an excluded window removes the
    leg's LIQUIDITY OBSERVATION and never the leg. Asserted directly rather than
    inferred from where the call sits."""
    book = _NamespaceBook()
    book.returns.append(1.5)
    book.entry_dates.append(START)
    book.exit_dates.append(START)
    before = (list(book.returns), list(book.entry_dates), list(book.exit_dates), book.positions)
    _record_entry_liquidity(
        book,
        series=_series(LOOKBACK_SESSIONS + 1),
        signal_date=START + timedelta(days=LOOKBACK_SESSIONS),
        exit_date=START + timedelta(days=LOOKBACK_SESSIONS),
        name_key=-9,
        eligible=True,
        means=tuple(1000.0 for _ in range(LOOKBACK_SESSIONS + 1)),
        reasons=tuple(None for _ in range(LOOKBACK_SESSIONS + 1)),
        unresolved_breaks=(),
    )
    after = (list(book.returns), list(book.entry_dates), list(book.exit_dates), book.positions)
    assert before == after
