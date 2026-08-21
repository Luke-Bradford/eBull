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
from app.services.strategy_signal_scan import choose_frontier, modal_bar_date, window_decides_the_mode


def _calls_in(func: object) -> set[str]:
    """Every plain function name called in ``func``'s body."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))  # type: ignore[arg-type]
    return {node.func.id for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}


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


class TestTheWindowedReadKnowsWhenItCannotAnswer:
    """Codex checkpoint 2 on #2809: the optimisation can change the answer.

    Bounding the last-bar query drops every instrument whose last bar predates
    the window, and dropped instruments do not vote. With most of the universe
    stale and a minority freshly refreshed, the windowed mode is the minority's
    date while a scan run now would choose the stale majority's — the card would
    then call a scan sitting exactly on the frontier ``stale``, which is #2809
    reintroduced by its own fix.
    """

    def test_the_live_shape_is_decisive(self) -> None:
        """6,773 in the universe, 5,819 on the mode: the dropped set cannot win."""
        assert window_decides_the_mode(modal_count=5819, seen=6584, universe_size=6773)

    def test_a_fresh_minority_over_a_stale_majority_is_not_decisive(self) -> None:
        """The case Codex named, and the one the old code answered wrongly."""
        assert not window_decides_the_mode(modal_count=800, seen=1000, universe_size=6773)

    def test_the_boundary_is_a_tie_and_a_tie_goes_to_the_later_date(self) -> None:
        """``<=`` not ``<``.

        With the dropped set exactly the size of the mode, the worst case is a
        tie — and ``modal_bar_date`` breaks ties on the later date, which is the
        in-window one. So the windowed answer still stands.
        """
        assert window_decides_the_mode(modal_count=100, seen=900, universe_size=1000)
        assert not window_decides_the_mode(modal_count=100, seen=899, universe_size=1000)
        assert modal_bar_date({date(2026, 8, 19): 100, date(2026, 8, 18): 100}) == (date(2026, 8, 19), 100)

    def test_seeing_nothing_is_never_decisive(self) -> None:
        assert not window_decides_the_mode(modal_count=0, seen=0, universe_size=6773)

    def test_the_endpoint_falls_back_rather_than_answering_from_the_window(self) -> None:
        """Structural: an undecisive window must reach the full distribution.

        The failure is silent — a windowed mode is a plausible date whether or
        not it is the right one — so the fallback is asserted rather than
        reasoned about.
        """
        calls = _calls_in(_corpus_frontier)
        assert "window_decides_the_mode" in calls, "the windowed read must check that it is decisive"
        assert {"load_bar_spans", "choose_frontier"} <= calls, (
            "an undecisive window must fall back to the scanner's own unbounded distribution"
        )


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
