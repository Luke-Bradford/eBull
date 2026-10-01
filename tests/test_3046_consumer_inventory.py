"""#3046 residual 4 — keep the raw-``price_daily`` consumer inventory honest.

Pure-logic: no database, no fixtures. The measurement script's value rests
entirely on its inventory covering every raw reader, and an inventory nobody
checks goes stale the first time somebody adds a query.

⚠ WHAT THIS DOES NOT PROVE, restated here because the test name reads stronger
than the guard is: it binds per-file OCCURRENCE COUNTS. Swapping one reader for
another inside a file passes, a new consumer of an EXISTING helper adds no
occurrence, and ``frontend/`` is out of scope entirely. It is a drift detector on
the file set.
"""

from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path

from scripts.verify_3046_consumer_exposure import (
    _FROM_PRICE_DAILY,
    INVENTORY,
    KINDS,
    SLICES,
    SPECS,
    Anchors,
    guard_inventory,
    scan_occurrences,
    unmeasured_exposed_occurrences,
    window_start,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_inventory_covers_every_raw_price_daily_reader() -> None:
    """A new ``FROM price_daily`` under ``app/`` must be classified, not ignored."""
    assert guard_inventory(REPO_ROOT) == []


def test_inventory_declares_the_same_files_the_scanner_finds() -> None:
    declared = {occ.path for occ in INVENTORY}
    found = set(scan_occurrences(REPO_ROOT))
    assert declared == found


#: Entries whose ``line`` had already drifted when the line check below landed (#2842 slice 4b-i review): 38 of 84
#: on 2026-10-01. Frozen as debt, not re-mapped by guesswork — a file with several occurrences cannot be re-mapped
#: without re-reading which query each label means. Remove an entry when you correct its line; never add one.
_STALE_LINES: frozenset[tuple[str, int]] = frozenset(
    {
        ("app/api/copy_trading.py", 288),
        ("app/api/copy_trading.py", 409),
        ("app/api/instruments.py", 1232),
        ("app/api/instruments.py", 1242),
        ("app/api/portfolio.py", 575),
        ("app/api/portfolio.py", 757),
        ("app/api/portfolio.py", 764),
        ("app/api/portfolio.py", 995),
        ("app/services/ai_trial_pack_reader.py", 177),
        ("app/services/market_data.py", 1042),
        ("app/services/market_data.py", 1114),
        ("app/services/market_data.py", 1235),
        ("app/services/market_data.py", 1520),
        ("app/services/market_data.py", 1583),
        ("app/services/market_data.py", 1611),
        ("app/services/portfolio.py", 248),
        ("app/services/portfolio.py", 337),
        ("app/services/portfolio_eod.py", 337),
        ("app/services/portfolio_eod.py", 372),
        ("app/services/price_quarantine_store.py", 50),
        ("app/services/price_quarantine_store.py", 59),
        ("app/services/price_quarantine_store.py", 421),
        ("app/services/price_window_verdict.py", 251),
        ("app/services/price_window_verdict.py", 266),
        ("app/services/price_window_verdict.py", 269),
        ("app/services/ranking_ablation_reader.py", 258),
        ("app/services/scoring.py", 1261),
        ("app/services/scoring.py", 1378),
        ("app/services/scoring.py", 1622),
        ("app/services/scoring.py", 1733),
        ("app/services/scoring.py", 2277),
        ("app/services/strategies/s2_cross_sectional_momentum.py", 206),
        ("app/services/strategies/s2_cross_sectional_momentum.py", 300),
        ("app/services/strategy_monitoring.py", 690),
        ("app/workers/scheduler.py", 419),
        ("app/workers/scheduler.py", 664),
        ("app/workers/scheduler.py", 3175),
        ("app/workers/scheduler.py", 3418),
    }
)


def test_inventory_lines_point_at_their_occurrence() -> None:
    """A declared ``line`` must hold the ``FROM price_daily`` it classifies, so an edit above it fails here instead
    of silently pointing the reader at the wrong query (the guard above binds counts only)."""
    drifted = []
    for occ in INVENTORY:
        lines = (REPO_ROOT / occ.path).read_text().splitlines()
        ok = 0 < occ.line <= len(lines) and _FROM_PRICE_DAILY.search(lines[occ.line - 1]) is not None
        if not ok and (occ.path, occ.line) not in _STALE_LINES:
            drifted.append(f"{occ.path}:{occ.line} ({occ.label})")
    assert drifted == []


def test_the_stale_line_debt_only_shrinks() -> None:
    declared = {(occ.path, occ.line) for occ in INVENTORY}
    assert _STALE_LINES <= declared, "a _STALE_LINES entry no longer names a declared occurrence: remove it"


def test_every_occurrence_carries_a_known_kind() -> None:
    unknown = {occ.kind for occ in INVENTORY} - set(KINDS)
    assert unknown == set()


def test_every_exposed_consumer_reaches_a_measurement() -> None:
    """⚠ An occurrence can be classified exposed and then left out of the report.

    Codex checkpoint 2 found ``research_comparator_snapshot`` and
    ``fair_value_band._OWN_HISTORY_SQL`` inventoried as WINDOWED with no
    ``WindowSpec`` attached — the occurrence-count guard passed and the table
    silently omitted them.
    """
    assert unmeasured_exposed_occurrences() == []


def test_no_duplicate_occurrence_entries() -> None:
    """Two entries for the same (file, line) would inflate a per-file count and
    hide a reader that actually vanished."""
    dupes = [k for k, n in Counter((o.path, o.line) for o in INVENTORY).items() if n > 1]
    assert dupes == []


def test_every_spec_has_exactly_one_window_start_rule() -> None:
    """``window_start`` dispatches on the first set field, so two at once would
    silently measure the wrong window rather than raise."""
    for spec in SPECS:
        rules = [
            spec.rank is not None,
            spec.rank_or_oldest is not None,
            spec.whole_slice,
            spec.calendar_days is not None,
            spec.oldest_after is not None,
            spec.trailing_from_as_of is not None,
        ]
        assert sum(rules) == 1, f"{spec.name} declares {sum(rules)} window-start rules"


def test_every_spec_names_a_slice_that_exists() -> None:
    for spec in SPECS:
        assert spec.slice_ in SLICES, f"{spec.name} names unknown slice {spec.slice_!r}"


def _short_series(depth: int) -> Anchors:
    return Anchors(
        win_end=date(2026, 9, 14),
        oldest=date(2026, 9, 1),
        depth=depth,
        by_rank={2: date(2026, 9, 11)},
    )


def test_a_rank_the_series_is_too_short_for_excludes_the_metric() -> None:
    """Warm-up must EXCLUDE, never fall back to the oldest bar.

    A metric the producer never wrote cannot be exposed, and substituting the
    oldest available bar would silently widen the window and over-report.
    """
    sma_200 = next(s for s in SPECS if s.name == "sma_200")
    assert window_start(sma_200, _short_series(depth=2)) is None


def test_the_computability_floor_excludes_a_whole_slice_metric() -> None:
    """⚠ The regression Codex checkpoint 2 found, pinned.

    ``ema_12`` reads its whole supplied slice, so before ``min_bars`` existed a
    two-bar series produced a window — and any transition inside it counted as an
    exposed pair, while ``compute_indicators`` returns NULL for that instrument.
    The operand span and the computability floor are different numbers.
    """
    ema_12 = next(s for s in SPECS if s.name == "ema_12")
    assert ema_12.whole_slice and ema_12.min_bars == 12
    assert window_start(ema_12, _short_series(depth=2)) is None
    assert window_start(ema_12, _short_series(depth=12)) == date(2026, 9, 1)


def test_stochastic_k_reads_a_shorter_span_than_its_own_floor() -> None:
    """%K's operands are the last 14 bars; only %D spans all 16. Both need 16 to exist."""
    k = next(s for s in SPECS if s.name == "stoch_k")
    d = next(s for s in SPECS if s.name == "stoch_d")
    assert (k.rank, k.min_bars) == (14, 16)
    assert d.rank == 16


def test_volatility_falls_back_to_the_oldest_bar_rather_than_demanding_31() -> None:
    """``prices[-31:]`` is UP TO 31 with a 5-return floor, not a 31-bar requirement."""
    vol = next(s for s in SPECS if s.name == "volatility_30d")
    assert (vol.rank_or_oldest, vol.min_bars) == (31, 6)
    assert window_start(vol, _short_series(depth=10)) == date(2026, 9, 1)
    assert window_start(vol, _short_series(depth=5)) is None
