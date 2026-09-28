"""#1822 route F slice 1a: the ablation's pure construction (spec v5, slice-1 test list)."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services import ranking_ablation as ra
from app.services.hunt_evaluator import BookSeries, SeriesPrices
from app.services.hunt_inference import StatRefused
from app.services.price_quarantine import TransitionVerdict

W = ra.weights()


def _row(name: int, *, p: float = 0.0, r: float = 0.0, **families: float) -> ra.ScoreRow:
    scores = {family: 0.5 for family in ra.FAMILY_ORDER} | families
    return ra.ScoreRow(instrument_id=name, families=scores, deductions=p, additions=r)


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("family", ra.FAMILY_ORDER)
@pytest.mark.parametrize("p,r", [(0.0, 0.0), (0.35, 0.0), (0.0, 0.03), (0.9, 0.0), (0.0, 0.9)])
def test_adding_the_term_back_before_the_clip_reproduces_key_full(family: str, p: float, r: float) -> None:
    row = _row(1, p=p, r=r, quality=0.9, value=0.1, momentum=0.7, sentiment=0.2)
    add_back = ra.pre_clip_total(row, W, drop=family) + W[family] * row.families[family]
    assert ra.rebuilt_key(row, W) == pytest.approx(min(1.0, max(0.0, add_back)), abs=1e-15)


def test_weights_are_the_live_v15_weights_and_sum_to_one() -> None:
    assert set(W) == set(ra.FAMILY_ORDER)
    assert math.fsum(W.values()) == pytest.approx(1.0)


def test_no_renormalisation_orders_names_differently_from_a_renormalised_key() -> None:
    # Name-specific P − R: dropping quality's term and rescaling the rest by 1/(1 − w_q) would
    # move the two names by different amounts relative to their penalties.
    # With kept weight K and kept sums S_a − S_b = 0.1, any P_a in (0.1, 0.1 / K) flips the order.
    a = _row(1, p=0.12, quality=0.0, value=0.9, turnaround=0.9, momentum=0.9, sentiment=0.9, confidence=0.9)
    b = _row(
        2, p=0.0, quality=0.0, value=0.7666, turnaround=0.7666, momentum=0.7666, sentiment=0.7666, confidence=0.7666
    )

    def renormalised(row: ra.ScoreRow) -> float:
        kept = [f for f in ra.FAMILY_ORDER if f != "quality"]
        scale = math.fsum(W[f] for f in kept)
        return math.fsum(W[f] * row.families[f] for f in kept) / scale - row.deductions + row.additions

    ours = ra.rebuilt_key(a, W, drop="quality") > ra.rebuilt_key(b, W, drop="quality")
    theirs = renormalised(a) > renormalised(b)
    assert ours != theirs


def test_clipped_keys_tie_and_every_tie_at_the_cut_is_selected() -> None:
    rows = [_row(n, r=0.9, quality=0.99, value=0.99) for n in range(1, 4)] + [_row(n) for n in range(4, 11)]
    keys = {row.instrument_id: ra.rebuilt_key(row, W) for row in rows}
    assert [keys[n] for n in (1, 2, 3)] == [1.0, 1.0, 1.0]
    cohort = ra.formation_cohort(keys, frozenset(keys))
    assert cohort is not None
    # ⌈0.2 · 10⌉ = 2, but three names tie at the clip.
    assert cohort.arm == frozenset({1, 2, 3})


def test_the_arm_is_selected_inside_the_control_only() -> None:
    keys = {1: 0.9, 2: 0.8, 3: 0.1, 4: 0.2, 5: 0.3}
    cohort = ra.formation_cohort(keys, frozenset({3, 4, 5}))
    assert cohort is not None and cohort.arm == frozenset({5})
    assert ra.formation_cohort(keys, frozenset()) is None


# ---------------------------------------------------------------------------
# Row validity: an excluded row leaves BOTH keys (the caller never builds either)
# ---------------------------------------------------------------------------


def _scores(**override: object) -> dict[str, object]:
    return {family: Decimal("0.5") for family in ra.FAMILY_ORDER} | override


def test_a_valid_row_parses_penalties_and_rewards() -> None:
    parsed = ra.parse_score_row(
        7,
        rank=3,
        family_scores=_scores(),
        raw_total=Decimal("0.5"),
        penalties_json=[
            {"kind": "penalty", "name": "stale_thesis", "deduction": 0.15, "reason": ""},
            {"kind": "penalty", "name": "low_confidence", "deduction": 0.10, "reason": ""},
            {"kind": "reward", "name": "strong_calmar", "addition": 0.03, "reason": ""},
        ],
    )
    assert isinstance(parsed, ra.ScoreRow)
    assert parsed.deductions == pytest.approx(0.25)
    assert parsed.additions == pytest.approx(0.03)
    assert ra.parse_score_row(7, rank=3, family_scores=_scores(), raw_total=0.5, penalties_json=[]) == ra.ScoreRow(
        7, {f: 0.5 for f in ra.FAMILY_ORDER}, 0.0, 0.0
    )


@pytest.mark.parametrize(
    "rank,scores,raw,penalties,reason",
    [
        (None, _scores(), 0.5, [], "null_rank"),
        (1, _scores(), None, [], "non_finite_raw_total"),
        (1, _scores(), math.inf, [], "non_finite_raw_total"),
        (1, _scores(), 0.5, None, "unparseable_penalties_json"),
        (1, _scores(value=None), 0.5, [], "non_finite_family_score"),
        (1, _scores(momentum=Decimal("NaN")), 0.5, [], "non_finite_family_score"),
        (1, _scores(), 0.5, "not json", "unparseable_penalties_json"),
        (1, _scores(), 0.5, [{"kind": "penalty", "addition": 0.1}], "unparseable_penalties_json"),
        (1, _scores(), 0.5, [{"kind": "bonus", "addition": 0.1}], "unparseable_penalties_json"),
        (1, _scores(), 0.5, {"kind": "penalty"}, "unparseable_penalties_json"),
    ],
)
def test_an_invalid_row_names_its_exclusion_reason(
    rank: int | None, scores: dict[str, object], raw: object, penalties: object, reason: str
) -> None:
    assert ra.parse_score_row(1, rank=rank, family_scores=scores, raw_total=raw, penalties_json=penalties) == reason


# ---------------------------------------------------------------------------
# Timeline
# ---------------------------------------------------------------------------

SESSIONS = ra.nyse_sessions(date(2026, 6, 29), date(2026, 7, 14))


def _ny(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ra._NEW_YORK)


def test_the_calendar_skips_the_weekend_and_independence_day() -> None:
    assert date(2026, 7, 3) not in SESSIONS  # Friday 3 July 2026: Independence Day observed
    assert date(2026, 7, 4) not in SESSIONS and date(2026, 7, 5) not in SESSIONS
    assert date(2026, 7, 2) in SESSIONS and date(2026, 7, 6) in SESSIONS


@pytest.mark.parametrize(
    "known,entry",
    [
        (_ny(date(2026, 6, 30), 9, 0), date(2026, 6, 30)),  # before the open: that session
        (_ny(date(2026, 6, 30), 9, 30), date(2026, 7, 1)),  # at the open: strictly after is required
        (_ny(date(2026, 6, 30), 10, 0), date(2026, 7, 1)),  # in session: the next open
        (_ny(date(2026, 7, 2), 11, 0), date(2026, 7, 6)),  # across the holiday and the weekend
        (_ny(date(2026, 7, 4), 12, 0), date(2026, 7, 6)),  # a weekend run
    ],
)
def test_entry_is_the_first_open_strictly_after_the_known_time(known: datetime, entry: date) -> None:
    assert ra.entry_session(known, SESSIONS) == entry
    assert ra.entry_session(known.astimezone(UTC), SESSIONS) == entry


def test_two_runs_per_entry_session_keep_the_latest_known_and_count_the_other() -> None:
    early = ra.Run(scored_at=_ny(date(2026, 7, 6), 10), known_at=_ny(date(2026, 7, 6), 10, 1))
    late = ra.Run(scored_at=_ny(date(2026, 7, 6), 14), known_at=_ny(date(2026, 7, 6), 14, 1))
    tie_a = ra.Run(scored_at=_ny(date(2026, 7, 8), 10), known_at=_ny(date(2026, 7, 8), 11))
    tie_b = ra.Run(scored_at=_ny(date(2026, 7, 8), 10, 30), known_at=_ny(date(2026, 7, 8), 11))
    beyond = ra.Run(scored_at=_ny(date(2026, 7, 14), 12), known_at=_ny(date(2026, 7, 14), 12))
    mapped = ra.map_runs([late, early, tie_a, tie_b, beyond], SESSIONS)
    assert mapped.entries == {date(2026, 7, 7): late, date(2026, 7, 9): tie_b}
    assert set(mapped.superseded) == {early, tie_a}
    assert mapped.beyond_calendar == (beyond,)


def test_a_naive_timestamp_is_refused() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ra.Run(scored_at=datetime(2026, 7, 6, 10), known_at=_ny(date(2026, 7, 6), 10))


# ---------------------------------------------------------------------------
# Control bar rule and t3_excluded
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "volume,canonical,verbatim",
    [
        (None, True, False),
        (Decimal(1000), True, True),
        (Decimal(0), False, False),
        (Decimal(-5), False, False),
        (math.nan, False, False),
        (math.inf, False, False),
    ],
)
def test_a_null_volume_passes_only_the_canonical_rule(volume: object, canonical: bool, verbatim: bool) -> None:
    bar = (Decimal("10"), Decimal("11"), Decimal("9"), Decimal("10.5"))
    assert ra.bar_valid(*bar, volume) is canonical  # type: ignore[arg-type]
    assert ra.bar_valid(*bar, volume, verbatim=True) is verbatim  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "bar",
    [
        (None, 11, 9, 10),  # a masked open
        (10, 11, 10.5, 10),  # L > min(O, C)
        (10, 10.2, 9, 10.5),  # H < max(O, C)
        (10, 10, 10, 10),  # H = L
    ],
)
def test_every_other_harness_clause_still_applies_with_a_null_volume(
    bar: tuple[float | None, float, float, float],
) -> None:
    open_, high, low, close = bar
    assert ra.bar_valid(open_, high, low, close, None) is False


def _transition(day: date, rules: tuple[str, ...], corroboration: str = "unclassifiable") -> TransitionVerdict:
    return TransitionVerdict(
        price_date=day,
        prior_date=day - timedelta(days=1),
        observed_ratio=Decimal(8),
        provisional=False,
        corroboration=corroboration,
        rules=rules,
    )


def test_t3_excluded_takes_t3_inside_the_span_and_nothing_else() -> None:
    span = {"start": date(2026, 7, 20), "end": date(2026, 9, 23)}
    verdicts = {
        1: [_transition(date(2026, 8, 11), ("T3",))],
        2: [_transition(date(2026, 8, 11), ("T1",)), _transition(date(2026, 8, 20), ("T2",))],
        3: [_transition(date(2026, 8, 10), (), corroboration="spike")],
        4: [_transition(date(2026, 9, 24), ("T3",))],  # after c_k
        5: [_transition(date(2026, 7, 20), ("T3",))],  # dated by its later bar, on the first formation
    }
    assert ra.t3_excluded(verdicts, **span) == frozenset({1, 5})


# ---------------------------------------------------------------------------
# Grid, books and Δ
# ---------------------------------------------------------------------------

GRID_SESSIONS = ra.nyse_sessions(date(2026, 7, 20), date(2026, 10, 30))


def test_the_grid_admits_entries_through_g_k_and_reports_through_g_k() -> None:
    cutoff = GRID_SESSIONS[60]
    grid = ra.reporting_grid(GRID_SESSIONS, first_formation=GRID_SESSIONS[0], cutoff=cutoff)
    assert not isinstance(grid, StatRefused)
    g_k = 60 - (ra.H - 1)
    assert (grid.first, grid.last) == (1, g_k)
    assert grid.formations[-1] + ra.LAG == g_k  # the last admitted entry is G_k
    assert grid.formations[-1] + ra.LAG + ra.H - 1 == 60  # and it exits at c_k


def test_a_cutoff_too_early_for_one_cohort_is_an_empty_grid() -> None:
    refused = ra.reporting_grid(GRID_SESSIONS, first_formation=GRID_SESSIONS[0], cutoff=GRID_SESSIONS[10])
    assert isinstance(refused, StatRefused) and refused.reason == "empty_grid"


def _flat(value: float, count: int) -> dict[int, float]:
    return dict.fromkeys(range(count), value)


def _prices(paths: dict[int, dict[int, float]], opens: dict[int, dict[int, float]]) -> dict[int, SeriesPrices]:
    return {
        n: SeriesPrices(opens=opens.get(n, p), closes=p, dividends={}, terminal_ordinal=None) for n, p in paths.items()
    }


def _books_fixture(
    paths: dict[int, dict[int, float]],
    keys: dict[str | None, dict[int, float]],
    *,
    cutoff: int,
    opens: dict[int, dict[int, float]] | None = None,
) -> ra.AblationBooks | StatRefused:
    """One run per session: every formation carries the whole control and the same keys."""
    grid = ra.reporting_grid(GRID_SESSIONS, first_formation=GRID_SESSIONS[0], cutoff=GRID_SESSIONS[cutoff])
    assert not isinstance(grid, StatRefused)
    names = frozenset(paths)
    control = dict.fromkeys(grid.formations, names)
    per_run = {drop: keys.get(drop, keys[None]) for drop in (None, *ra.FAMILY_ORDER)}
    key_map = dict.fromkeys(grid.formations, per_run)
    return ra.ablation_books(
        grid,
        control,
        key_map,
        _prices(paths, opens or {}),
        half_spread=lambda name, e, book: 0.00725,
        terminal_fractions={},
    )


def test_identical_keys_give_a_zero_delta_and_a_refusal_not_a_t() -> None:
    count = 70
    paths = {n: {d: 10.0 * (1 + 0.01 * n) ** d for d in range(count)} for n in range(1, 6)}
    books = _books_fixture(paths, {None: {n: float(n) for n in paths}}, cutoff=60)
    assert isinstance(books, ra.AblationBooks)
    ablated = books.ablated["value"]
    assert isinstance(ablated, BookSeries)
    delta = ra.paired_delta(books.full, ablated)
    assert all(value == 0.0 for value in delta)
    refused = ra.delta_statistics(delta, lag=3)
    assert isinstance(refused, StatRefused) and refused.reason == "degenerate_variance"


def test_a_uniform_rescale_of_one_series_leaves_every_delta_unchanged() -> None:
    count = 70
    paths = {n: {d: 5.0 + n + 0.3 * math.sin(d + n) for d in range(count)} for n in range(1, 6)}
    keys: dict[str | None, dict[int, float]] = {
        None: {1: 5.0, 2: 1, 3: 2, 4: 3, 5: 4},
        "value": {1: 0, 2: 1, 3: 2, 4: 3, 5: 4},
    }
    base = _books_fixture(paths, keys, cutoff=60)
    rescaled = _books_fixture(paths | {1: {d: 150.0 * p for d, p in paths[1].items()}}, keys, cutoff=60)
    assert isinstance(base, ra.AblationBooks) and isinstance(rescaled, ra.AblationBooks)
    for books in (base, rescaled):
        assert isinstance(books.ablated["value"], BookSeries)
    before = ra.paired_delta(base.full, base.ablated["value"])  # type: ignore[arg-type]
    after = ra.paired_delta(rescaled.full, rescaled.ablated["value"])  # type: ignore[arg-type]
    assert any(value != 0.0 for value in before)
    assert after == pytest.approx(before, abs=1e-12)


def test_ruin_of_one_ablated_arm_refuses_that_family_only() -> None:
    count = 70
    paths = {n: _flat(10.0, count) for n in range(1, 6)}
    paths[5] = {d: (10.0 if d < 30 else 1e-12) for d in range(count)}
    crash_open = {5: {d: (10.0 if d <= 30 else 1e-12) for d in range(count)}}
    keys: dict[str | None, dict[int, float]] = {
        None: {1: 5.0, 2: 1, 3: 2, 4: 3, 5: 4},
        "momentum": {1: 0, 2: 1, 3: 2, 4: 3, 5: 9},
    }
    books = _books_fixture(paths, keys, cutoff=60, opens=crash_open)
    assert isinstance(books, ra.AblationBooks)
    momentum = books.ablated["momentum"]
    assert isinstance(momentum, StatRefused) and momentum.reason == "book_ruin"
    assert all(isinstance(books.ablated[f], BookSeries) for f in ra.FAMILY_ORDER if f != "momentum")


def test_ruin_of_the_full_arm_refuses_the_whole_cell() -> None:
    count = 70
    paths = {n: _flat(10.0, count) for n in range(1, 6)}
    paths[5] = {d: (10.0 if d < 30 else 1e-12) for d in range(count)}
    crash_open = {5: {d: (10.0 if d <= 30 else 1e-12) for d in range(count)}}
    refused = _books_fixture(paths, {None: {1: 0, 2: 1, 3: 2, 4: 3, 5: 9}}, cutoff=60, opens=crash_open)
    assert isinstance(refused, StatRefused) and refused.reason == "book_ruin"


def test_a_collapse_after_g_k_is_not_ruin_on_the_reported_grid() -> None:
    count = 70
    cutoff = 60
    g_k = cutoff - (ra.H - 1)
    paths = {n: _flat(10.0, count) for n in range(1, 6)}
    paths[5] = {d: (10.0 if d <= g_k + 3 else 1e-12) for d in range(count)}
    crash_open = {5: {d: (10.0 if d <= g_k + 4 else 1e-12) for d in range(count)}}
    books = _books_fixture(paths, {None: {1: 0, 2: 1, 3: 2, 4: 3, 5: 9}}, cutoff=cutoff, opens=crash_open)
    assert isinstance(books, ra.AblationBooks)
    assert books.full.sessions == range(1, g_k + 1)


def test_delta_statistics_annualise_and_carry_the_mde() -> None:
    delta = [0.001 * math.sin(i) + 0.0002 for i in range(60)]
    stats = ra.delta_statistics(delta, lag=3)
    assert isinstance(stats, ra.DeltaStatistics)
    assert stats.mean_annual == pytest.approx(math.fsum(delta) / 60 * 252)
    assert stats.mde_annual == pytest.approx((1.959963984540054 + 0.8416212335729143) * stats.se_annual)
    assert stats.nonzero_sessions == 60 and stats.lag == 3


def test_a_grid_whose_every_control_is_empty_is_refused_not_all_cash() -> None:
    grid = ra.reporting_grid(GRID_SESSIONS, first_formation=GRID_SESSIONS[0], cutoff=GRID_SESSIONS[60])
    assert not isinstance(grid, StatRefused)
    refused = ra.ablation_books(grid, {}, {}, {}, half_spread=lambda name, e, book: 0.00725, terminal_fractions={})
    assert isinstance(refused, StatRefused) and refused.reason == "empty_grid"
