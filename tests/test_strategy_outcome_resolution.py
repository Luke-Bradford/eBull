"""Forward outcome resolution: mature once, never persist an immature poll."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import psycopg
import pytest

from app.services.indicator_series import BarSeries
from app.services.outcome_ledger import PendingFill
from app.services.outcome_resolver import ExitLevels
from app.services.strategy_manifest import StrategyEntry
from app.services.strategy_outcome_resolution import _read_cursor, _resolve_fill, _select_round_robin, _write_cursor


def _series(*ranges: tuple[Decimal | None, Decimal | None]) -> BarSeries:
    dates = tuple(date(2026, 8, 1) + timedelta(days=index) for index in range(len(ranges)))
    rows = tuple(
        {
            "open": Decimal("100"),
            "high": high,
            "low": low,
            "close": Decimal("100"),
            "volume": Decimal("1000"),
        }
        for high, low in ranges
    )
    return BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]


#: The raw corpus says the date is GONE. ⚠ That is the only evidence that
#: distinguishes a deleted bar from one this process cannot see: the loader is
#: fail-closed, so its silence proves nothing (#3189 finding 10).
def _BAR_DELETED(_day: date) -> bool:  # noqa: N802 - reads as a constant at call sites
    return False


def _BAR_PRESENT(_day: date) -> bool:  # noqa: N802 - reads as a constant at call sites
    """The raw corpus still holds it, so the loader's silence is about
    VISIBILITY — an unevaluated instrument, a stale quarantine version — and
    nothing may be recorded."""
    return True


def _entry(*, max_hold_bars: int = 2) -> StrategyEntry:
    levels = ExitLevels(
        take_profit=Decimal("110"),
        stop_loss=Decimal("90"),
        max_hold_bars=max_hold_bars,
    )
    return cast(
        StrategyEntry,
        SimpleNamespace(
            strategy_id="test-level",
            exit_levels=lambda _series, *, signal_index, entry_price, universe: levels,
        ),
    )


def _fill(series: BarSeries) -> PendingFill:
    return PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=series.dates[0],
        fill_bar_date=series.dates[1],
        fill_price=Decimal("100"),
        universe="survivor_only",
    )


def test_an_immature_window_is_left_pending_without_a_row() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("105"), Decimal("95")))
    assert _resolve_fill(_entry(), _fill(series), series=series, unresolved_breaks=()) is None


def test_a_decisive_target_is_stored_as_a_booked_outcome() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("111"), Decimal("95")))
    row = _resolve_fill(_entry(), _fill(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.exit_bar_date, row.gross_return_pct) == (
        "tp_hit",
        series.dates[1],
        Decimal("0.1"),
    )


def test_same_bar_both_touch_remains_ambiguous_and_unbooked() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("111"), Decimal("89")))
    row = _resolve_fill(_entry(), _fill(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.exit_bar_date, row.gross_return_pct) == (
        "ambiguous",
        series.dates[1],
        None,
    )


def test_a_scale_break_is_terminal_but_never_priced_across_scales() -> None:
    series = _series(
        (Decimal("105"), Decimal("95")),
        (Decimal("105"), Decimal("95")),
        (Decimal("111"), Decimal("89")),
    )
    row = _resolve_fill(
        _entry(),
        _fill(series),
        series=series,
        unresolved_breaks=(series.dates[2],),
    )
    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "series_break", None)


def test_a_break_between_signal_and_fill_is_terminal_before_levels_are_mixed() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("150"), Decimal("100")))
    row = _resolve_fill(
        _entry(),
        _fill(series),
        series=series,
        unresolved_breaks=(series.dates[1],),
    )
    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "series_break", None)


def test_a_masked_required_field_is_counted_not_silently_dropped() -> None:
    series = _series((Decimal("105"), Decimal("95")), (None, Decimal("95")))
    row = _resolve_fill(_entry(), _fill(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "quarantined_bar", None)


def test_unorderable_exit_levels_are_terminal_without_aborting_the_batch() -> None:
    series = _series((Decimal("1"), Decimal("0.5")), (Decimal("1"), Decimal("0.5")))
    entry = cast(
        StrategyEntry,
        SimpleNamespace(
            strategy_id="test-level",
            exit_levels=lambda _series, *, signal_index, entry_price, universe: "unorderable_exit_levels",
        ),
    )

    row = _resolve_fill(entry, _fill(series), series=series, unresolved_breaks=())

    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == (
        "unresolved",
        "unorderable_exit_levels",
        None,
    )


def test_a_fill_date_the_corpus_lost_is_terminal_without_aborting_the_batch() -> None:
    """#3189 finding 10 — `locate_fill_index` raised, and the raise escaped three
    nested loops to abort the strategy BEFORE its cursor was written. The guard's
    intent survives: the row NAMES the disagreement instead of re-reading
    whatever bar now sits at that position."""
    series = _gapped_series()
    fill = PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=series.dates[0],
        # Inside the loaded span, and gone from it — the corpus moved.
        fill_bar_date=date(2026, 8, 2),
        fill_price=Decimal("100"),
        universe="survivor_only",
    )

    row = _resolve_fill(_entry(), fill, series=series, unresolved_breaks=(), raw_bar_exists=_BAR_DELETED)

    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "fill_bar_absent", None)


def test_a_signal_date_the_corpus_lost_is_terminal_without_aborting_the_batch() -> None:
    """The other locator, and a DISTINCT reason: which half of the stored pair
    disagreed with the corpus is the whole operator-facing content of the row."""
    series = _gapped_series()
    fill = PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=date(2026, 8, 2),  # inside the loaded span, gone from it
        fill_bar_date=series.dates[1],
        fill_price=Decimal("100"),
        universe="survivor_only",
    )

    row = _resolve_fill(_entry(), fill, series=series, unresolved_breaks=(), raw_bar_exists=_BAR_DELETED)

    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "signal_bar_absent", None)


@pytest.mark.parametrize("field", ["fill_bar_date", "signal_bar_date"])
def test_a_bar_the_raw_corpus_still_holds_stays_pending(field: str) -> None:
    """What the new terminal rows must NOT claim (Codex checkpoint 2, P1).

    ``load_masked_bars`` is fail-closed at the INSTRUMENT level: no
    ``price_quarantine_coverage`` row at the current ``rule_set_version``
    returns ZERO bars. Recording there would be far worse than the wedge it
    replaces — outcomes are immutable and the selection anti-joins on the
    version pair, so a corpus resolved during an incomplete quarantine refresh
    would be permanently mislabelled at exactly the version pair the refresh
    was producing. The raw bar still exists, so nothing is written and the fill
    is retried.
    """
    series = _series((Decimal("105"), Decimal("95")), (Decimal("105"), Decimal("95")))
    dates = {"signal_bar_date": series.dates[0], "fill_bar_date": series.dates[1]}
    dates[field] = date(2099, 1, 1)  # beyond anything the loader returned
    fill = PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=dates["signal_bar_date"],
        fill_bar_date=dates["fill_bar_date"],
        fill_price=Decimal("100"),
        universe="survivor_only",
    )

    assert _resolve_fill(_entry(), fill, series=series, unresolved_breaks=(), raw_bar_exists=_BAR_PRESENT) is None


def test_a_deleted_boundary_bar_is_recorded_not_deferred() -> None:
    """Codex checkpoint 2, rounds 3-4 — the case every PROXY gets wrong.

    When a rebuild deletes the FIRST bar, the returned span starts after the
    stored date; and once the quarantine refresh recomputes its bounds from the
    shortened series, the coverage window does the same. Both proxies then read
    a deleted bar as an invisible one and defer it for ever, consuming a
    round-robin slot on every tick. Asking the raw corpus has no boundary: the
    bar is gone, so the disagreement is recorded.
    """
    series = _gapped_series()  # 08-01, 08-03, 08-04
    fill = PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=date(2026, 7, 15),  # before the first returned bar
        fill_bar_date=series.dates[1],
        fill_price=Decimal("100"),
        universe="survivor_only",
    )

    row = _resolve_fill(_entry(), fill, series=series, unresolved_breaks=(), raw_bar_exists=_BAR_DELETED)

    assert row is not None
    assert row.reason == "signal_bar_absent"


def test_a_caller_that_cannot_answer_never_records() -> None:
    """``raw_bar_exists=None`` means nobody asked the corpus, and the default is
    therefore to write NOTHING — fail-closed, because the expensive mistake is
    the one that records an immutable row at the version pair a refresh was
    still producing."""
    series = _gapped_series()
    fill = PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=series.dates[0],
        fill_bar_date=date(2026, 8, 2),
        fill_price=Decimal("100"),
        universe="survivor_only",
    )

    assert _resolve_fill(_entry(), fill, series=series, unresolved_breaks=(), raw_bar_exists=None) is None


def _gapped_series() -> BarSeries:
    """Three bars with 2026-08-02 missing from the middle.

    The gap is the point: a date the loader SPANS but does not hold is the only
    shape that proves the corpus itself moved, as opposed to a corpus this
    process cannot currently see.
    """
    dates = (date(2026, 8, 1), date(2026, 8, 3), date(2026, 8, 4))
    rows = tuple(
        {
            "open": Decimal("100"),
            "high": Decimal("105"),
            "low": Decimal("95"),
            "close": Decimal("100"),
            "volume": Decimal("1000"),
        }
        for _ in dates
    )
    return BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]


def test_a_fill_bar_whose_open_no_longer_loads_is_terminal() -> None:
    """The third raise: the date survives, the open does not.

    `load_masked_bars` nulls an open that is NULL or <= 0, and
    `fill_price_is_superseded` deliberately returns False for it ("None is not
    evidence the bar moved"), so the flow used to reach `resolve_outcome` and
    raise "bar N has no open, so it cannot be a fill bar" — correct, and a
    batch-aborting way to say it.

    Built by hand rather than through `_series`, which hardcodes a positive open
    on every bar; that is exactly why no existing test covered this.
    """
    dates = (date(2026, 8, 1), date(2026, 8, 2), date(2026, 8, 3))
    rows = (
        {"open": Decimal("100"), "high": Decimal("105"), "low": Decimal("95"), "close": Decimal("100")},
        {"open": None, "high": Decimal("105"), "low": Decimal("95"), "close": Decimal("100")},
        {"open": Decimal("100"), "high": Decimal("105"), "low": Decimal("95"), "close": Decimal("100")},
    )
    series = BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]
    fill = PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=dates[0],
        fill_bar_date=dates[1],
        fill_price=Decimal("100"),
        universe="survivor_only",
    )

    row = _resolve_fill(_entry(), fill, series=series, unresolved_breaks=())

    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "fill_bar_open_absent", None)


def test_an_unorderable_bracket_outranks_a_masked_fill_open() -> None:
    """Precedence, pinned (#3189 finding 10, Codex checkpoint 2).

    A fill whose open is masked AND whose bracket cannot be reconstructed
    records `unorderable_exit_levels` TODAY — it is a resolved row, not a crash.
    The new `fill_bar_open_absent` check therefore has to sit after the levels
    branch: placed earlier it relabels that row, which breaks the invariant the
    whole change rests on (only rows that previously CRASHED may move).
    """
    dates = (date(2026, 8, 1), date(2026, 8, 2), date(2026, 8, 3))
    rows = (
        {"open": Decimal("100"), "high": Decimal("105"), "low": Decimal("95"), "close": Decimal("100")},
        {"open": None, "high": Decimal("105"), "low": Decimal("95"), "close": Decimal("100")},
        {"open": Decimal("100"), "high": Decimal("105"), "low": Decimal("95"), "close": Decimal("100")},
    )
    series = BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]
    entry = cast(
        StrategyEntry,
        SimpleNamespace(
            strategy_id="test-level",
            exit_levels=lambda _series, *, signal_index, entry_price, universe: "unorderable_exit_levels",
        ),
    )
    fill = PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=dates[0],
        fill_bar_date=dates[1],
        fill_price=Decimal("100"),
        universe="survivor_only",
    )

    row = _resolve_fill(entry, fill, series=series, unresolved_breaks=())

    assert row is not None
    assert row.reason == "unorderable_exit_levels"


def test_a_genuine_contract_breach_still_raises() -> None:
    """What the new containment must NOT swallow.

    The `except ValueError` is deliberately narrow — around each locator only.
    `resolve_outcome`'s own ValueErrors mean a bracket that cannot be traded or
    an entry price that divides by zero: deploy-time breaches that are identical
    for every row of that strategy, so a recorded per-row refusal would write
    thousands of junk rows and hide a bug that should be loud.
    """
    series = _series((Decimal("105"), Decimal("95")), (Decimal("105"), Decimal("95")))
    entry = cast(
        StrategyEntry,
        SimpleNamespace(
            strategy_id="test-level",
            exit_levels=lambda *_args, **_kwargs: ExitLevels(
                take_profit=Decimal("110"),
                # Orderable as a bracket (ExitLevels refuses stop >= target on
                # construction) but not against the entry of 100, which is what
                # `resolve_outcome` checks.
                stop_loss=Decimal("105"),
                max_hold_bars=2,
            ),
        ),
    )

    with pytest.raises(ValueError, match="is not below the entry"):
        _resolve_fill(entry, _fill(series), series=series, unresolved_breaks=())


def _superseded_fill(series: BarSeries) -> PendingFill:
    """The stored fill, priced from a bar that has since been rewritten.

    ``_series`` builds every bar with ``open = 100``; a 2:1 split rewrites that
    history to 50 while the ledger keeps its copy of 100. This is the measured
    shape — #2414 found all 221 real disagreements to be exact corporate-action
    ratios, 2:1 the most common.
    """
    return PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=series.dates[0],
        fill_bar_date=series.dates[1],
        fill_price=Decimal("200"),
        universe="survivor_only",
    )


def test_a_superseded_fill_price_is_terminal_without_aborting_the_batch() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("105"), Decimal("95")))

    row = _resolve_fill(_entry(), _superseded_fill(series), series=series, unresolved_breaks=())

    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "fill_price_superseded", None)


def test_a_superseded_fill_price_never_reaches_the_exit_levels_factory() -> None:
    """The factory derives a bracket from the stored entry AND the reloaded
    series, so on a rescaled series it can hand ``resolve_outcome`` levels that
    are unorderable against the entry — a raise, not a refusal. The guard must
    sit in front of it, not behind."""
    series = _series((Decimal("105"), Decimal("95")), (Decimal("105"), Decimal("95")))
    entry = cast(
        StrategyEntry,
        SimpleNamespace(
            strategy_id="test-level",
            exit_levels=lambda *_args, **_kwargs: pytest.fail("the levels factory saw a superseded fill"),
        ),
    )

    row = _resolve_fill(entry, _superseded_fill(series), series=series, unresolved_breaks=())

    assert row is not None
    assert row.reason == "fill_price_superseded"


def test_a_series_break_still_outranks_a_superseded_fill_price() -> None:
    """Placement is AFTER the break test on purpose: no row that resolves today
    may be relabelled by this change."""
    series = _series((Decimal("105"), Decimal("95")), (Decimal("105"), Decimal("95")))

    row = _resolve_fill(
        _entry(),
        _superseded_fill(series),
        series=series,
        unresolved_breaks=(series.dates[1],),
    )

    assert row is not None
    assert row.reason == "series_break"


def test_a_masked_open_is_not_reported_as_a_superseded_fill_price() -> None:
    """``None`` is not evidence the bar moved, so the masked case must not
    borrow ``fill_price_superseded``.

    ⚠ NARROWED, not flipped (#3189 finding 10). This asserted
    ``pytest.raises(ValueError, match="cannot be a fill bar")`` — it pinned the
    subject (which refusal applies) to the MECHANISM of the day (a raise), and
    that mechanism is the batch wedge: the raise escapes three nested loops and
    aborts the strategy before its cursor is written. The subject is unchanged
    and still asserted: the reason is its own, and specifically not the
    supersession code."""
    series = BarSeries(
        dates=(date(2026, 8, 1), date(2026, 8, 2)),
        rows=(
            {
                "open": Decimal("100"),
                "high": Decimal("105"),
                "low": Decimal("95"),
                "close": Decimal("100"),
                "volume": Decimal("1000"),
            },
            {
                "open": None,
                "high": Decimal("105"),
                "low": Decimal("95"),
                "close": Decimal("100"),
                "volume": Decimal("1000"),
            },
        ),  # type: ignore[arg-type]
    )

    row = _resolve_fill(_entry(), _superseded_fill(series), series=series, unresolved_breaks=())

    assert row is not None
    assert row.reason == "fill_bar_open_absent"
    assert row.reason != "fill_price_superseded"


def test_precomputed_masked_reasons_are_reused_for_an_instrument(monkeypatch: pytest.MonkeyPatch) -> None:
    series = _series((Decimal("105"), Decimal("95")), (None, Decimal("95")))
    monkeypatch.setattr(
        "app.services.strategy_outcome_resolution._masked_reasons",
        lambda _rows: pytest.fail("masked reasons were recomputed per fill"),
    )

    row = _resolve_fill(
        _entry(),
        _fill(series),
        series=series,
        unresolved_breaks=(),
        masked_bar_reasons={1: "quarantined_bar"},
    )

    assert row is not None
    assert (row.outcome, row.reason) == ("unresolved", "quarantined_bar")


def test_round_robin_wraps_after_the_cursor_without_repeating(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[int, int | None, int]] = []

    def select(_conn: object, **kwargs: object) -> list[PendingFill]:
        after = cast(int, kwargs["after_signal_id"])
        ceiling = cast(int | None, kwargs.get("at_or_before_signal_id"))
        limit = cast(int, kwargs["limit"])
        seen.append((after, ceiling, limit))
        ids = [8] if after == 7 else [2, 3]
        return [
            PendingFill(
                signal_id=signal_id,
                instrument_id=42,
                signal_bar_date=date(2026, 8, 1),
                fill_bar_date=date(2026, 8, 2),
                fill_price=Decimal("100"),
                universe="survivor_only",
            )
            for signal_id in ids[:limit]
        ]

    monkeypatch.setattr("app.services.strategy_outcome_resolution.select_pending_fills", select)
    fills = _select_round_robin(
        cast(psycopg.Connection[object], object()),
        strategy_id="test",
        strategy_version="v1",
        cursor=7,
        limit=3,
    )
    assert [fill.signal_id for fill in fills] == [8, 2, 3]
    assert seen == [(7, None, 3), (0, 7, 2)]


def test_cursor_is_one_mutable_row_per_version_pair(ebull_test_conn: psycopg.Connection[object]) -> None:
    assert _read_cursor(ebull_test_conn, strategy_id="test", strategy_version="v1") == 0
    _write_cursor(ebull_test_conn, strategy_id="test", strategy_version="v1", last_signal_id=10)
    _write_cursor(ebull_test_conn, strategy_id="test", strategy_version="v1", last_signal_id=3)
    assert _read_cursor(ebull_test_conn, strategy_id="test", strategy_version="v1") == 3
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_outcome_cursor").fetchone() == (1,)
