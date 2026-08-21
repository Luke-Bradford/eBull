"""The scan card's freshness bar is the frontier a scan would choose (#2809).

Pure (no Postgres). The defect: ``scan.status`` compared the stored scan frontier
against ``MAX(last_bar) FROM research_price_series`` — a different table, a
different statistic and a different population — and reported all 10 strategies
``stale`` while every one of them sat exactly on the frontier.

Measured on dev at the fix: the frontier a scan would choose was 2026-08-19
(modal 5,819 of 6,584 loadable); ``MAX(research_price_series.last_bar)`` was
2026-08-20, held by ONE row — CBOE VIX, series_id 7728 — while the next-newest
series in that archive ended 2026-07-08. ``max(price_daily.price_date)`` was also
2026-08-20, held by 1,563 rows of an in-flight ``daily_candle_refresh`` against
~11,000 on each completed day: the refresh-in-flight case the modal rule exists
to survive.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from collections import Counter
from datetime import date, timedelta

from app.api.strategies import _CORPUS_FRESHNESS_WINDOW, _corpus_frontier, get_strategy_overview
from app.services.price_masked_bars import _LAST_BAR_SQL, _RECENT_LAST_BAR_COUNTS_SQL
from app.services.strategy_signal_scan import choose_frontier, modal_bar_date


def _calls_in(func: object) -> set[str]:
    """Every plain function name called in ``func``'s body."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))  # type: ignore[arg-type]
    return {
        node.func.id for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


class TestOneTieBreakRule:
    """``modal_bar_date`` is the rule; ``choose_frontier`` is one of its callers."""

    def test_the_card_and_the_scanner_break_ties_identically(self) -> None:
        """The whole point of extracting it: two copies is how #2809 happened.

        A tie must resolve to the LATER date on both sides, or a corpus split
        evenly between two sessions makes the card disagree with the scan that
        wrote the row it is describing.
        """
        last_bars = {1: date(2026, 8, 18), 2: date(2026, 8, 19)}
        frontier = choose_frontier(last_bars)
        assert frontier is not None
        assert modal_bar_date(Counter(last_bars.values())) == (frontier.bar_date, frontier.modal_count)
        assert frontier.bar_date == date(2026, 8, 19)

    def test_modal_not_max_on_the_distribution_the_card_reads(self) -> None:
        """The refresh-in-flight shape, as counts rather than per-instrument.

        The live numbers at the fix: 5,819 instruments last traded 2026-08-19 and
        1,563 carried a partial 2026-08-20 written by a refresh that was still
        running. A ``max`` calls the corpus fresh at 08-20 and every scan stale;
        the mode calls it 08-19, which is what a scan run then would evaluate.
        """
        assert modal_bar_date({date(2026, 8, 19): 5819, date(2026, 8, 20): 1563}) == (date(2026, 8, 19), 5819)

    def test_empty_distribution_is_none_not_an_exception(self) -> None:
        assert modal_bar_date({}) is None


class TestTheFreshnessBarIsTheScannersOwnStatistic:
    """Structural: both sides of the comparison must route through one rule."""

    def test_the_card_reads_the_scan_population_through_the_scan_rule(self) -> None:
        """``_corpus_frontier`` must use the scanner's universe AND its tie-break.

        Asserted structurally rather than by value because the failure is silent:
        a freshness bar computed off a different population still returns a
        plausible date, and the card renders it without complaint. That is
        exactly how ``MAX(last_bar) FROM research_price_series`` survived — it
        answered, and the answer was about a corpus nothing on this card reads.
        """
        calls = _calls_in(_corpus_frontier)
        assert "load_validated_universe" in calls, (
            "the freshness bar must be measured over the population the scan is scored on"
        )
        assert "load_recent_last_bar_counts" in calls, (
            "the freshness bar must read price_daily through the masked predicate, not another corpus"
        )
        assert "modal_bar_date" in calls, (
            "the freshness bar must break ties the way the scanner does — a second copy is #2809"
        )

    def test_the_overview_no_longer_reads_a_max_over_the_research_archive(self) -> None:
        """Retarget this guard rather than deleting it if the source moves."""
        source = inspect.getsource(get_strategy_overview)
        assert "research_price_series" not in source, (
            "the scan card's freshness bar must not come from the backtest archive (#2809)"
        )
        assert "_corpus_frontier(" in source, "get_strategy_overview no longer computes a freshness bar"


class TestTheBoundedQueryKeepsTheMaskedPopulation:
    """The bound may shorten the window; it must not widen the population.

    ``price_masked_bars``' own docstring makes the mirror the discipline —
    *"the SQL is mirrored field for field rather than shared — and the mirror is
    checkable"*. Checking it is what stops the cheap query from quietly counting
    bars the fail-closed loader never returns, which would be defect (3) of #2809
    (wrong population) reintroduced by the fix for defects (1) and (2).
    """

    def test_it_carries_the_same_coverage_predicate_as_the_span_query(self) -> None:
        for clause in (
            "JOIN price_quarantine_coverage cov",
            "cov.instrument_id = d.instrument_id",
            "cov.rule_set_version = %(quarantine_version)s",
            "d.price_date BETWEEN cov.first_bar AND cov.last_bar",
            "d.instrument_id = ANY(%(instrument_ids)s)",
        ):
            assert clause in _LAST_BAR_SQL, f"{clause!r} left the span query — retarget this mirror"
            assert clause in _RECENT_LAST_BAR_COUNTS_SQL, (
                f"{clause!r} is missing from the freshness query, which would count bars the "
                "masked loader never returns (#2809)"
            )

    def test_only_the_freshness_query_is_bounded(self) -> None:
        """The bound is sound for a freshness bar and wrong for the scan's own
        eligibility: an instrument with no bar in the window cannot be the
        freshest date the corpus reached, but it is still a universe member the
        scan must account for."""
        assert "%(since)s" in _RECENT_LAST_BAR_COUNTS_SQL
        assert "%(since)s" not in _LAST_BAR_SQL


class TestTheWindowIsWideEnoughForAClosedMarket:
    """The bound is by construction, so its construction is the assertion."""

    def test_the_window_outlasts_the_longest_exchange_closure(self) -> None:
        """Four sessions (9/11) plus the weekends either side is ~9 days.

        A window that a holiday week can empty turns a shut market into a stale
        corpus, which is the false-alarm direction — and a card that cries wolf
        is how the real #2803 staleness signal would be dismissed.
        """
        longest_closure = timedelta(days=4 + 2 + 2 + 1)
        assert _CORPUS_FRESHNESS_WINDOW >= longest_closure

    def test_the_window_is_short_enough_that_a_dead_refresh_still_reads_stale(self) -> None:
        """The other side of the same choice: a month of silence must not pass."""
        assert _CORPUS_FRESHNESS_WINDOW < timedelta(days=30)
